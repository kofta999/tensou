use crate::discovery;
use crate::net::recv::PendingTransfer;
use crate::protocol::TransferObserver;
use crate::{MAX_CONCURRENT_STREAMS, QUIC_RECEIVE_WINDOW, QUIC_STREAM_RECEIVE_WINDOW};
use async_trait::async_trait;
use mdns_sd::ServiceDaemon;
use quinn::{Endpoint, ServerConfig, TransportConfig};
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use std::{net::SocketAddr, path::PathBuf, sync::Arc};
use tokio_util::sync::CancellationToken;

mod recv;
mod send;
pub use recv::Receiver;
pub use send::Sender;

#[async_trait]
pub trait TransferConsentHandler: Send + Sync {
    async fn request_consent(&self, peer: SocketAddr, job_name: &str) -> bool;
}

// Server listener
pub struct AppDaemon {
    endpoint: quinn::Endpoint,
    _discovery_daemon: ServiceDaemon,
}

impl AppDaemon {
    pub fn new(bind_addr: SocketAddr) -> anyhow::Result<Self> {
        let server_config = Self::configure_server()?;

        let endpoint = Endpoint::server(server_config, bind_addr)?;
        let actual_port = endpoint.local_addr()?.port();

        let _discovery_daemon = discovery::register_service(actual_port)?;

        Ok(Self {
            endpoint,
            _discovery_daemon,
        })
    }

    pub fn local_addr(&self) -> anyhow::Result<SocketAddr> {
        Ok(self.endpoint.local_addr()?)
    }

    // TODO: target_dir will be replaced by a Config struct later
    pub async fn run(
        &self,
        target_dir: PathBuf,
        consent: Arc<dyn TransferConsentHandler>,
        observer: Arc<dyn TransferObserver>,
        cancel_token: CancellationToken,
    ) {
        use tokio::task::JoinSet;
        use tokio::time::{Duration, timeout};

        let mut active_transfers = JoinSet::new();

        loop {
            tokio::select! {
                _ = cancel_token.cancelled() =>  {
                    break;
                }

            incoming = self.endpoint.accept() => {
                let Some(incoming) = incoming else {break};

                let target_dir_clone = target_dir.clone();
                let observer_clone = observer.clone();
                let transfer_id = rand::random::<u32>();
                let consent_clone = consent.clone();
                let cancel_clone = cancel_token.clone();

                while active_transfers.try_join_next().is_some() {}

                active_transfers.spawn(async move {
                    let connection = match timeout(Duration::from_secs(5), incoming).await {
                        Ok(Ok(conn)) => conn,
                        Ok(Err(e)) => {
                            eprintln!("Quinn connection establishment failed: {e}");
                            return anyhow::Ok(());
                        }
                        Err(_) => {
                            eprintln!("Connection handshake timed out.");
                            return anyhow::Ok(());
                        }
                    };

                    let peer = connection.remote_address();

                    match timeout(
                        Duration::from_secs(10),
                        PendingTransfer::read_manifest(connection.clone()),
                    )
                    .await
                    {
                        Ok(Ok(pending)) => {
                            let is_accepted = consent_clone
                                .request_consent(peer, &pending.manifest.job_name)
                                .await;

                            if is_accepted {
                                let receiver = pending
                                    .accept(
                                        &target_dir_clone,
                                        observer_clone.clone(),
                                        transfer_id,
                                        cancel_clone.clone(),
                                        false
                                        // self.conoverwrite
                                    )
                                    .await?;

                                observer_clone.on_transfer_started(
                                    transfer_id,
                                    peer,
                                    receiver.total_size,
                                    &receiver.job_name,
                                );

                                tokio::select! {
                                    _ = cancel_clone.cancelled() =>  {
                                        connection.close(0u32.into(),b"Cancelled by receiver");
                                    }

                                    res = receiver.process_chunks(cancel_clone.clone()) => {
                                        res?;
                                        observer_clone.on_transfer_complete(transfer_id);
                                    }
                                };

                            } else {
                                println!("Transfer from {} was rejected.", peer);
                                pending.reject().await?;
                            }
                        }
                        Ok(Err(e)) => {
                            eprintln!("Handshake manifest read failed! {e}");
                        }
                        Err(_) => {
                            eprintln!("Timed out waiting for manifest from {peer}");
                        }
                    };

                    anyhow::Ok(())
                });
            }}
        }

        println!("Waiting for active transfers to safely flush to disk...");
        while let Some(_) = active_transfers.join_next().await {}
    }

    fn configure_server() -> anyhow::Result<ServerConfig> {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()])?;
        let cert_der = CertificateDer::from(cert.cert);
        let priv_key = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());

        let mut transport_config = TransportConfig::default();
        transport_config.max_concurrent_uni_streams(MAX_CONCURRENT_STREAMS.into());
        transport_config.stream_receive_window(QUIC_STREAM_RECEIVE_WINDOW.into());
        transport_config.receive_window(QUIC_RECEIVE_WINDOW.into());

        let mut server_config =
            ServerConfig::with_single_cert(vec![cert_der.clone()], priv_key.into())?;
        server_config.transport = Arc::new(transport_config);

        Ok(server_config)
    }
}

#[cfg(test)]
mod tests {
    use crate::SERVER_PORT;

    use super::*;
    use rand::Rng;
    use tempfile::tempdir;
    use tokio::time::Duration;

    struct AutoAccept;

    #[async_trait]
    impl TransferConsentHandler for AutoAccept {
        async fn request_consent(&self, _peer: SocketAddr, _job_name: &str) -> bool {
            true
        }
    }

    struct TestObserver;
    impl TransferObserver for TestObserver {}

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_full_network_transfer() -> anyhow::Result<()> {
        let _ = rustls::crypto::ring::default_provider().install_default();

        let source_dir = tempdir()?;
        let dest_dir = tempdir()?;
        let source_path = source_dir.path().join("source.bin");
        let received_dir = dest_dir.path().to_path_buf();

        let mut buffer = vec![0u8; 10 * 1024 * 1024];
        rand::rng().fill_bytes(&mut buffer);
        std::fs::write(&source_path, &buffer)?;

        let app_daemon = AppDaemon::new(format!("127.0.0.1:{}", SERVER_PORT).parse()?)?;
        let bound_server_addr = app_daemon.endpoint.local_addr()?;

        let target_path_clone = received_dir.clone();
        let server_handle = tokio::spawn(async move {
            app_daemon
                .run(
                    target_path_clone,
                    Arc::new(AutoAccept),
                    Arc::new(TestObserver),
                    CancellationToken::new(),
                )
                .await;
        });

        // Give the server 50ms to boot up and start listening
        tokio::time::sleep(Duration::from_millis(50)).await;

        let client = Sender::connect(bound_server_addr, &source_path).await?;
        client.process_chunks(Arc::new(TestObserver {})).await?;

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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_unique_naming_transfer() -> anyhow::Result<()> {
        let _ = rustls::crypto::ring::default_provider().install_default();

        let source_dir = tempdir()?;
        let dest_dir = tempdir()?;
        let source_path_1 = source_dir.path().join("source.bin");
        let source_path_2 = source_dir.path().join("source_different.bin");
        let received_dir = dest_dir.path().to_path_buf();

        let mut buffer_1 = vec![0u8; 1 * 1024 * 1024];
        rand::rng().fill_bytes(&mut buffer_1);
        std::fs::write(&source_path_1, &buffer_1)?;

        let mut buffer_2 = vec![0u8; 1 * 1024 * 1024];
        rand::rng().fill_bytes(&mut buffer_2);
        std::fs::write(&source_path_2, &buffer_2)?;

        let source_dir_2 = tempdir()?;
        let source_path_2_named_same = source_dir_2.path().join("source.bin");
        std::fs::copy(&source_path_2, &source_path_2_named_same)?;

        let app_daemon = AppDaemon::new("127.0.0.1:0".parse()?)?;
        let bound_server_addr = app_daemon.endpoint.local_addr()?;

        let target_path_clone = received_dir.clone();
        let server_handle = tokio::spawn(async move {
            app_daemon
                .run(
                    target_path_clone,
                    Arc::new(AutoAccept),
                    Arc::new(TestObserver),
                    CancellationToken::new(),
                )
                .await;
        });

        tokio::time::sleep(Duration::from_millis(50)).await;

        let client_1 = Sender::connect(bound_server_addr, &source_path_1).await?;
        client_1.process_chunks(Arc::new(TestObserver {})).await?;
        tokio::time::sleep(Duration::from_millis(100)).await;

        let client_2 = Sender::connect(bound_server_addr, &source_path_2_named_same).await?;
        client_2.process_chunks(Arc::new(TestObserver {})).await?;
        tokio::time::sleep(Duration::from_millis(100)).await;

        assert!(file_diff::diff(
            source_path_1.to_str().unwrap(),
            received_dir.join("source.bin").to_str().unwrap()
        ));
        assert!(file_diff::diff(
            source_path_2_named_same.to_str().unwrap(),
            received_dir.join("source (1).bin").to_str().unwrap()
        ));

        server_handle.abort();
        Ok(())
    }
}
