use crate::{
    crypto::SkipServerVerification,
    disk::{ReceiveSession, SendSession},
    protocol::{ChunkPacket, Metadata, State},
};
use quinn::{ClientConfig, Endpoint, ServerConfig, crypto::rustls::QuicClientConfig};
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use std::{
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

// Server listener
pub struct AppDaemon {
    endpoint: quinn::Endpoint,
}

impl AppDaemon {
    pub fn new(port: u16) -> anyhow::Result<Self> {
        let server_config = Self::configure_server()?;
        let bind_addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), port));
        let endpoint = Endpoint::server(server_config, bind_addr)?;

        Ok(Self { endpoint })
    }

    // TODO: target_dir will be replaced by a Config struct later
    pub async fn run(&self, target_dir: PathBuf) {
        // Waiting for connections, similar to HTTP servers
        while let Some(incoming) = self.endpoint.accept().await {
            let target_dir_clone = target_dir.clone();
            tokio::spawn(async move {
                // Get the actual connection
                let connection = incoming.await?;

                if let Ok(transfer_job) =
                    TransferJob::handle_handshake(connection, &target_dir_clone).await
                {
                    println!("Handshake successful, starting transfer...");

                    transfer_job.process_chunks().await?;
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

        transport_config.max_concurrent_uni_streams(255_u8.into());

        Ok(server_config)
    }
}

// Server Receiver
struct TransferJob {
    connection: quinn::Connection,
    session: Arc<Mutex<ReceiveSession>>,
}

impl TransferJob {
    pub async fn handle_handshake(
        connection: quinn::Connection,
        target_dir: &Path,
    ) -> anyhow::Result<Self> {
        let (mut send, mut recv) = connection.accept_bi().await?;

        let max_size = 64 * 1024; // 64KB
        let buf = recv.read_to_end(max_size).await?;
        let metadata: Metadata = rmp_serde::from_slice(&buf)?;

        let receive_session = ReceiveSession::new(metadata, target_dir)?;
        let state_buf = rmp_serde::to_vec(&receive_session.get_state())?;

        send.write_all(&state_buf).await?;
        send.finish()?;

        Ok(Self {
            connection,
            session: Arc::new(Mutex::new(receive_session)),
        })
    }

    pub async fn process_chunks(self) -> anyhow::Result<()> {
        while let Ok(mut chunk_stream) = self.connection.accept_uni().await {
            let conn_clone = self.connection.clone();
            let session_clone = self.session.clone();
            const MAX_CHUNK_SIZE: usize = 5 * 1024 * 1024; // 5MB

            tokio::spawn(async move {
                if let Err(e) = async {
                    // As it contains raw 4mb data + messagepack headers + hash + index
                    let buf = chunk_stream.read_to_end(MAX_CHUNK_SIZE).await?;
                    let chunk: ChunkPacket = rmp_serde::from_slice(&buf)?;

                    let mut session = session_clone
                        .lock()
                        .map_err(|e| anyhow::anyhow!("Session mutex poisoned: {}", e))?;

                    session.write_chunk(chunk)?;

                    if session.is_complete() {
                        session.commit()?;
                        conn_clone.close(0u32.into(), b"Transfer Complete");
                    }

                    anyhow::Ok(())
                }
                .await
                {
                    eprintln!("Error processing chunk: {:?}", e);
                };
            });
        }

        anyhow::Ok(())
    }
}

pub struct TransferClient {
    connection: quinn::Connection,
    session: Arc<SendSession>,
    remote_state: State,
}

impl TransferClient {
    pub async fn connect(
        server_addr: SocketAddr,
        session: Arc<SendSession>,
    ) -> anyhow::Result<Self> {
        let client_cfg = Self::configure_client()?;
        let bind_addr: SocketAddr = "0.0.0.0:0".parse()?;
        let mut endpoint = Endpoint::client(bind_addr)?;
        endpoint.set_default_client_config(client_cfg);

        println!("Connecting to {}...", server_addr);
        let connection = endpoint.connect(server_addr, "localhost")?.await?;

        let (mut send, mut recv) = connection.open_bi().await?;
        let buf = rmp_serde::to_vec(&session.get_metadata())?;

        send.write_all(&buf).await?;
        send.finish()?;

        const MAX_METADATA_SIZE: usize = 64 * 1024;
        let buf = recv.read_to_end(MAX_METADATA_SIZE).await?;
        let remote_state: State = rmp_serde::from_slice(&buf)?;

        Ok(Self {
            connection,
            session,
            remote_state,
        })
    }

    pub async fn process_chunks(self) -> anyhow::Result<()> {
        let chunk_count = self.session.get_total_chunks();
        let mut client_tasks = Vec::new();

        for i in 0..chunk_count {
            if self.remote_state.0.get(i).is_some_and(|v| v == true) {
                continue;
            }

            let conn_clone = self.connection.clone();
            let session = self.session.clone();

            let task = tokio::spawn(async move {
                let mut stream = conn_clone.clone().open_uni().await?;

                let chunk = session.get_chunk(i as u64)?;
                let buf = rmp_serde::to_vec(&chunk)?;

                stream.write_all(&buf).await?;
                stream.finish()?;

                anyhow::Ok(())
            });

            client_tasks.push(task);
        }

        for task in client_tasks {
            task.await??;
        }

        self.connection.closed().await;

        Ok(())
    }

    fn configure_client() -> anyhow::Result<ClientConfig> {
        let rustls_config = rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(SkipServerVerification::new())
            .with_no_client_auth();

        Ok(ClientConfig::new(Arc::new(QuicClientConfig::try_from(
            rustls_config,
        )?)))
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
        let app_daemon = AppDaemon::new(0)?;

        // Grab the actual port the OS assigned us so the client knows where to dial
        let bound_server_addr = app_daemon.endpoint.local_addr()?;

        // 4. Start the Server Daemon in the background
        let target_path_clone = received_dir.clone();
        let server_handle = tokio::spawn(async move {
            app_daemon.run(target_path_clone).await;
        });

        // Give the server 50ms to boot up and start listening
        tokio::time::sleep(Duration::from_millis(50)).await;

        // 5. Client Setup: Initialize the SendSession
        let send_session = Arc::new(SendSession::new(&source_path, 4 * 1024 * 1024)?);

        // 6. Execute the Transfer
        let client = TransferClient::connect(bound_server_addr, send_session).await?;
        client.process_chunks().await?;

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
