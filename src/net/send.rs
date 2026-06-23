use crate::{
    ChunkIndex, FileId, MAX_CONCURRENT_STREAMS, MAX_METADATA_SIZE,
    crypto::SkipServerVerification,
    disk::SendSession,
    protocol::{ManifestManager, State},
};
use quinn::{ClientConfig, Endpoint, crypto::rustls::QuicClientConfig};
use std::{collections::HashMap, net::SocketAddr, path::Path, sync::Arc};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::mpsc,
};

pub struct Sender {
    connection: quinn::Connection,
    sessions: HashMap<FileId, Arc<SendSession>>,
    remote_states: Vec<State>,
}

impl Sender {
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

        let is_accepted = recv.read_u8().await?;
        if is_accepted == 0 {
            anyhow::bail!("The receiver rejected your transfer request.");
        }

        let buf = recv.read_to_end(MAX_METADATA_SIZE as usize).await?;
        let remote_states: Vec<State> = rmp_serde::from_slice(&buf)?;

        Ok(Self {
            connection,
            sessions,
            remote_states,
        })
    }

    pub fn get_remaining_bytes(&self) -> u64 {
        let mut total = 0;
        for (file_id, chunk_idx) in self.flatten() {
            if let Some(session) = self.sessions.get(&file_id) {
                total += session.get_chunk_size(chunk_idx);
            }
        }
        total
    }

    pub async fn process_chunks(
        self,
        progress_tx: Option<mpsc::Sender<u64>>,
    ) -> anyhow::Result<()> {
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
            let tx_clone = progress_tx.clone();

            join_set.spawn(async move {
                let mut stream = conn_clone.clone().open_uni().await?;

                // To capture ownership and drop on task finish (to decrease semaphore count)
                let _permit = permit;

                let (header, buf) = session_clone.get_chunk(chunk_id).await?;
                let buf_len = buf.len() as u64;

                // Very lightweight
                let header_bytes = rmp_serde::to_vec(&header)?;
                let header_len = header_bytes.len() as u32;

                stream.write_u32(header_len).await?;
                stream.write_all(&header_bytes).await?;

                stream.write_all(&buf).await?;
                drop(buf);

                stream.finish()?;

                if let Some(tx) = tx_clone {
                    // No need to throw errors on progress reports
                    let _ = tx.send(buf_len).await;
                }

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
