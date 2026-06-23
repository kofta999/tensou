use crate::MAX_CONCURRENT_STREAMS;
use crate::net::recv::PendingTransfer;
use crate::{discovery, protocol::DaemonEvent};
use mdns_sd::ServiceDaemon;
use quinn::{Endpoint, ServerConfig};
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use std::{
    net::SocketAddr,
    path::PathBuf,
    sync::{Arc, Mutex},
};
use tokio::sync::broadcast;

mod recv;
mod send;
pub use recv::Receiver;
pub use send::Sender;

// Server listener
pub struct AppDaemon {
    endpoint: quinn::Endpoint,
    event_tx: Option<broadcast::Sender<DaemonEvent>>,
    _discovery_daemon: ServiceDaemon,
}

impl AppDaemon {
    pub fn new(
        bind_addr: SocketAddr,
        event_tx: Option<broadcast::Sender<DaemonEvent>>,
    ) -> anyhow::Result<Self> {
        let server_config = Self::configure_server()?;

        let endpoint = Endpoint::server(server_config, bind_addr)?;
        let actual_port = endpoint.local_addr()?.port();

        let _discovery_daemon = discovery::register_service(actual_port)?;

        Ok(Self {
            endpoint,
            event_tx,
            _discovery_daemon,
        })
    }

    // TODO: target_dir will be replaced by a Config struct later
    pub async fn run(&self, target_dir: PathBuf) {
        // Waiting for connections, similar to HTTP servers
        while let Some(incoming) = self.endpoint.accept().await {
            let target_dir_clone = target_dir.clone();
            let event_tx_clone = self.event_tx.clone();
            let transfer_id = rand::random::<u32>();

            tokio::spawn(async move {
                // Get the actual connection
                let connection = incoming.await?;
                let peer = connection.remote_address();

                if let Ok(pending) = PendingTransfer::read_manifest(connection).await {
                    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();

                    if let Some(ref tx) = event_tx_clone {
                        tx.send(DaemonEvent::ConsentRequested {
                            peer,
                            job_name: pending.manifest.job_name.to_string(),
                            reply_tx: Arc::new(Mutex::new(Some(reply_tx))),
                        })?;
                    }

                    let is_accepted = reply_rx.await.unwrap_or(false);

                    if is_accepted {
                        let transfer_job = pending.accept(&target_dir_clone).await?;

                        if let Some(ref tx) = event_tx_clone {
                            let _ = tx.send(DaemonEvent::TransferStarted {
                                transfer_id,
                                peer,
                                total_bytes: transfer_job.total_size,
                                job_name: transfer_job.job_name.to_string(),
                            });
                        }

                        transfer_job
                            .process_chunks(event_tx_clone, transfer_id)
                            .await?;
                    } else {
                        println!("Transfer from {} was rejected.", peer);
                        pending.reject().await?;
                    }
                } else {
                    eprintln!("Handshake failed!");
                };

                anyhow::Ok(())
            });
        }
    }

    fn configure_server() -> anyhow::Result<ServerConfig> {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()])?;
        let cert_der = CertificateDer::from(cert.cert);
        let priv_key = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());

        let mut server_config =
            ServerConfig::with_single_cert(vec![cert_der.clone()], priv_key.into())?;
        let transport_config = Arc::get_mut(&mut server_config.transport)
            .ok_or(anyhow::anyhow!("Couldn't access transport config"))?;

        transport_config.max_concurrent_uni_streams(MAX_CONCURRENT_STREAMS.into());

        Ok(server_config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::Rng;
    use tempfile::tempdir;
    use tokio::time::Duration;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_full_network_transfer() -> anyhow::Result<()> {
        rustls::crypto::ring::default_provider()
            .install_default()
            .expect("Failed to install crypto provider");

        // 1. Setup: Create a temporary directory
        let source_dir = tempdir()?;
        let dest_dir = tempdir()?;
        let source_path = source_dir.path().join("source.bin");
        let received_dir = dest_dir.path().to_path_buf();

        // 2. Mock Data: Generate a 10MB test file at `source_path`
        let mut buffer = vec![0u8; 10 * 1024 * 1024];
        rand::rng().fill_bytes(&mut buffer);
        std::fs::write(&source_path, &buffer)?;

        // 3. Server Setup: Bind to port 0 (OS assigns a random free port)
        let app_daemon = AppDaemon::new("127.0.0.1:0".parse()?, None)?;

        // Grab the actual port the OS assigned us so the client knows where to dial
        let bound_server_addr = app_daemon.endpoint.local_addr()?;

        // 4. Start the Server Daemon in the background
        let target_path_clone = received_dir.clone();
        let server_handle = tokio::spawn(async move {
            app_daemon.run(target_path_clone).await;
        });

        // Give the server 50ms to boot up and start listening
        tokio::time::sleep(Duration::from_millis(50)).await;

        let client = Sender::connect(bound_server_addr, &source_path).await?;
        client.process_chunks(None).await?;

        // Give the server a tiny moment to flush the final commit() to disk
        tokio::time::sleep(Duration::from_millis(100)).await;

        // 7. Verification: Diff the files
        assert!(file_diff::diff(
            source_path.to_str().unwrap(),
            received_dir.join("source.bin").to_str().unwrap()
        ));

        // Clean up the background server task
        server_handle.abort();

        Ok(())
    }
}
