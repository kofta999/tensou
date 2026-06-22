use crate::{
    ChunkIndex, FileId, MAX_CONCURRENT_STREAMS, MAX_QUIC_CHUNK_SIZE,
    crypto::SkipServerVerification,
    discovery,
    disk::{ReceiveSession, SendSession},
    protocol::{ChunkPacket, Manifest, ManifestManager, State},
};
use mdns_sd::ServiceDaemon;
use quinn::{ClientConfig, Endpoint, ServerConfig, crypto::rustls::QuicClientConfig};
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use std::{
    collections::HashMap,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
};

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

        transport_config.max_concurrent_uni_streams(MAX_CONCURRENT_STREAMS.into());

        Ok(server_config)
    }
}

// Server Receiver
struct TransferJob {
    connection: quinn::Connection,
    sessions: Arc<HashMap<FileId, Arc<tokio::sync::Mutex<ReceiveSession>>>>,
}

impl TransferJob {
    pub async fn handle_handshake(
        connection: quinn::Connection,
        target_dir: &Path,
    ) -> anyhow::Result<Self> {
        let (mut send, mut recv) = connection.accept_bi().await?;

        let max_size = 1 * 1024 * 1024; // 1 MB
        let buf = recv.read_to_end(max_size).await?;
        let manifest: Manifest = rmp_serde::from_slice(&buf)?;

        let target_path = target_dir.join(&manifest.job_name);
        let (states, sessions) = ManifestManager::parse(manifest, &target_path)?;

        let state_buf = rmp_serde::to_vec(&states)?;

        send.write_all(&state_buf).await?;
        send.finish()?;

        Ok(Self {
            connection,
            sessions: Arc::new(sessions),
        })
    }

    pub async fn process_chunks(self) -> anyhow::Result<()> {
        let mut join_set = tokio::task::JoinSet::new();

        let mut recv = self.connection.accept_uni().await?;
        let buf = recv.read_to_end(64).await?;
        let chunk_count: usize = rmp_serde::from_slice(&buf)?;

        for _ in 0..chunk_count {
            let mut chunk_stream = self.connection.accept_uni().await?;
            let sessions_clone = self.sessions.clone();

            join_set.spawn(async move {
                if let Err(e) = async {
                    let buf = chunk_stream.read_to_end(MAX_QUIC_CHUNK_SIZE).await?;

                    let chunk: ChunkPacket = rmp_serde::from_slice(&buf)?;

                    let mut session = sessions_clone
                        .get(&chunk.file_id)
                        .ok_or_else(|| anyhow::anyhow!("Invalid file_id from client"))?
                        .lock()
                        .await;

                    session.write_chunk(chunk).await?;

                    if session.is_complete() {
                        session.commit()?;
                    }

                    anyhow::Ok(())
                }
                .await
                {
                    eprintln!("Error processing chunk: {:?}", e);
                };
            });
        }

        while let Some(res) = join_set.join_next().await {
            if let Err(e) = res {
                eprintln!("Chunk processing error: {:?}", e);
            }
        }

        self.connection.close(0u32.into(), b"Transfer Complete");

        println!("Transfer complete");
        anyhow::Ok(())
    }
}

pub struct TransferClient {
    connection: quinn::Connection,
    sessions: HashMap<FileId, Arc<SendSession>>,
    remote_states: Vec<State>,
}

impl TransferClient {
    pub async fn connect(server_addr: SocketAddr, path: &Path) -> anyhow::Result<Self> {
        let (manifest, sessions) = ManifestManager::build(path)?;

        let client_cfg = Self::configure_client()?;
        let bind_addr: SocketAddr = "0.0.0.0:0".parse()?;
        let mut endpoint = Endpoint::client(bind_addr)?;
        endpoint.set_default_client_config(client_cfg);

        println!("Connecting to {}...", server_addr);
        let connection = endpoint.connect(server_addr, "localhost")?.await?;

        let (mut send, mut recv) = connection.open_bi().await?;
        let buf = rmp_serde::to_vec(&manifest)?;

        send.write_all(&buf).await?;
        send.finish()?;

        const MAX_METADATA_SIZE: usize = 64 * 1024;
        let buf = recv.read_to_end(MAX_METADATA_SIZE).await?;
        let remote_states: Vec<State> = rmp_serde::from_slice(&buf)?;

        Ok(Self {
            connection,
            sessions,
            remote_states,
        })
    }

    pub async fn process_chunks(self) -> anyhow::Result<()> {
        let task_list = self.flatten();
        let chunk_count = task_list.len();

        let mut send = self.connection.open_uni().await?;
        send.write_all(&rmp_serde::to_vec(&chunk_count)?).await?;
        send.finish()?;

        let semaphore = Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_STREAMS.into()));
        let mut join_set = tokio::task::JoinSet::new();

        for (file_id, chunk_id) in task_list {
            let permit = semaphore.clone().acquire_owned().await?;
            let session = self.sessions.get(&file_id).expect("shouldn't happen");
            let session_clone = session.clone();
            let conn_clone = self.connection.clone();

            join_set.spawn(async move {
                let mut stream = conn_clone.clone().open_uni().await?;

                // To capture ownership and drop on task finish (to decrease semaphore count)
                let _permit = permit;

                let chunk = session_clone.get_chunk(chunk_id).await?;
                let buf = rmp_serde::to_vec(&chunk)?;

                stream.write_all(&buf).await?;
                stream.finish()?;

                anyhow::Ok(())
            });
        }

        while let Some(res) = join_set.join_next().await {
            res??;
        }

        self.connection.closed().await;

        Ok(())
    }

    fn flatten(&self) -> Vec<(FileId, ChunkIndex)> {
        let mut arr = Vec::new();

        for (file_id, session) in &self.sessions {
            for chunk_idx in 0..session.get_total_chunks() {
                if self
                    .remote_states
                    .get(*file_id)
                    .and_then(|s| s.0.get(chunk_idx))
                    .is_some_and(|v| v == true)
                {
                    continue;
                }

                arr.push((*file_id, chunk_idx as u64));
            }
        }

        arr
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
        let app_daemon = AppDaemon::new("127.0.0.1:0".parse()?)?;

        // Grab the actual port the OS assigned us so the client knows where to dial
        let bound_server_addr = app_daemon.endpoint.local_addr()?;

        // 4. Start the Server Daemon in the background
        let target_path_clone = received_dir.clone();
        let server_handle = tokio::spawn(async move {
            app_daemon.run(target_path_clone).await;
        });

        // Give the server 50ms to boot up and start listening
        tokio::time::sleep(Duration::from_millis(50)).await;

        let client = TransferClient::connect(bound_server_addr, &source_path).await?;
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
