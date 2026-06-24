use crate::{
    CHUNK_SIZE, FileId, MAX_CONCURRENT_STREAMS, MAX_METADATA_SIZE,
    disk::{IgnitionPayload, ReceiveSession},
    protocol::{ChunkHeader, ChunkPacket, Manifest, ManifestManager, TransferObserver},
};
use std::{collections::HashMap, path::Path, sync::Arc};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub(super) struct PendingTransfer {
    pub(super) connection: quinn::Connection,
    pub(super) send_stream: quinn::SendStream,
    pub(super) manifest: Manifest,
}

impl PendingTransfer {
    pub async fn read_manifest(connection: quinn::Connection) -> anyhow::Result<Self> {
        let (send_stream, mut recv_stream) = connection.accept_bi().await?;

        let buf = recv_stream.read_to_end(MAX_METADATA_SIZE as usize).await?;
        let manifest: Manifest = rmp_serde::from_slice(&buf)?;

        Ok(Self {
            connection,
            manifest,
            send_stream,
        })
    }

    pub async fn accept(
        mut self,
        target_dir: &Path,
        observer: Arc<dyn TransferObserver>,
        transfer_id: u32,
    ) -> anyhow::Result<Receiver> {
        self.send_stream.write_u8(1).await?;

        let job_name = self.manifest.job_name.clone();
        let target_path = target_dir.join(&self.manifest.job_name);
        let target_path_clone = target_path.clone();

        let instructions = tokio::task::spawn_blocking(move || {
            ManifestManager::parse(self.manifest, &target_path_clone)
        })
        .await??;

        let (total_remaining_size, states) =
            instructions
                .iter()
                .fold((0, Vec::new()), |(total, mut states), ins| {
                    states.push(&ins.state);
                    (total + ins.remaining_bytes, states)
                });

        let state_buf = rmp_serde::to_vec(&states)?;

        self.send_stream.write_all(&state_buf).await?;
        self.send_stream.finish()?;

        let mut sessions = HashMap::new();

        for ins in instructions.into_iter() {
            // TODO: change that 2
            let (tx, rx) = tokio::sync::mpsc::channel::<ChunkPacket>(2);
            let file_id = ins.metadata.file_id;

            let payload = IgnitionPayload {
                ins,
                observer: observer.clone(),
                target_path: target_path.clone(),
                rx,
                transfer_id,
            };

            sessions.insert(file_id, Arc::new(ReceiveSession::new(tx, payload)));
        }

        Ok(Receiver {
            connection: self.connection,
            total_size: total_remaining_size,
            job_name,
            sessions: Arc::new(sessions),
        })
    }

    pub async fn reject(mut self) -> anyhow::Result<()> {
        self.send_stream.write_u8(0).await?;
        self.send_stream.finish()?;
        self.connection
            .close(0u32.into(), b"Transfer rejected by user");
        Ok(())
    }
}

pub struct Receiver {
    pub(super) connection: quinn::Connection,
    pub(super) total_size: u64,
    pub(super) job_name: String,
    pub(super) sessions: Arc<HashMap<FileId, Arc<ReceiveSession>>>,
}

impl Receiver {
    pub async fn process_chunks(self) -> anyhow::Result<()> {
        let mut join_set = tokio::task::JoinSet::new();
        let semaphore = Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_STREAMS.into()));

        let mut recv = self.connection.accept_uni().await?;
        let buf = recv.read_to_end(64).await?;
        let chunk_count: usize = rmp_serde::from_slice(&buf)?;

        for _ in 0..chunk_count {
            let mut chunk_stream = self.connection.accept_uni().await?;
            let sessions_clone = self.sessions.clone();
            let permit = semaphore.clone().acquire_owned().await?;

            join_set.spawn(async move {
                if let Err(e) = async {
                    // To capture ownership and drop on task finish (to decrease semaphore count)
                    let _permit = permit;

                    let header_len = chunk_stream.read_u32().await?;
                    let mut header_buf = vec![0u8; header_len as usize];
                    chunk_stream.read_exact(&mut header_buf).await?;
                    let header: ChunkHeader = rmp_serde::from_slice(&header_buf)?;

                    let data_buf = chunk_stream.read_to_end(CHUNK_SIZE as usize).await?;

                    let session = sessions_clone
                        .get(&header.file_id)
                        .ok_or_else(|| anyhow::anyhow!("Invalid file_id from client"))?;

                    session.write_chunk(header, data_buf).await?;

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

        anyhow::Ok(())
    }
}
