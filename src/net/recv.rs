use crate::{
    FileId, MAX_METADATA_SIZE, MAX_QUIC_CHUNK_SIZE,
    disk::ReceiveSession,
    protocol::{ChunkPacket, DaemonEvent, Manifest, ManifestManager},
};
use std::{collections::HashMap, path::Path, sync::Arc};
use tokio::{io::AsyncWriteExt, sync::broadcast};

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

    pub async fn accept(mut self, target_dir: &Path) -> anyhow::Result<Receiver> {
        self.send_stream.write_u8(1).await?;

        let job_name = self.manifest.job_name.clone();
        let target_path = target_dir.join(&self.manifest.job_name);
        let (states, sessions, total_size) = tokio::task::spawn_blocking(move || {
            ManifestManager::parse(self.manifest, &target_path)
        })
        .await??;

        let state_buf = rmp_serde::to_vec(&states)?;

        self.send_stream.write_all(&state_buf).await?;
        self.send_stream.finish()?;

        Ok(Receiver {
            connection: self.connection,
            total_size,
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
    pub(super) sessions: Arc<HashMap<FileId, Arc<tokio::sync::Mutex<ReceiveSession>>>>,
}

impl Receiver {
    pub async fn process_chunks(
        self,
        progress_tx: Option<broadcast::Sender<DaemonEvent>>,
        transfer_id: u32,
    ) -> anyhow::Result<()> {
        let mut join_set = tokio::task::JoinSet::new();

        let mut recv = self.connection.accept_uni().await?;
        let buf = recv.read_to_end(64).await?;
        let chunk_count: usize = rmp_serde::from_slice(&buf)?;

        for _ in 0..chunk_count {
            let mut chunk_stream = self.connection.accept_uni().await?;
            let sessions_clone = self.sessions.clone();
            let tx_clone = progress_tx.clone();

            join_set.spawn(async move {
                if let Err(e) = async {
                    let start = std::time::Instant::now();
                    let buf = chunk_stream.read_to_end(MAX_QUIC_CHUNK_SIZE).await?;
                    let net_time = start.elapsed();

                    let chunk: ChunkPacket = rmp_serde::from_slice(&buf)?;
                    let size = chunk.bytes.len();
                    let idx = chunk.index;

                    let start_lock = std::time::Instant::now();
                    let mut session = sessions_clone
                        .get(&chunk.file_id)
                        .ok_or_else(|| anyhow::anyhow!("Invalid file_id from client"))?
                        .lock()
                        .await;
                    let lock_wait_time = start_lock.elapsed();

                    let start_write = std::time::Instant::now();
                    session.write_chunk(chunk).await?;
                    let write_time = start_write.elapsed();

                    if session.is_complete() {
                        session.commit()?;
                    }

                    if lock_wait_time > std::time::Duration::from_millis(50) {
                        println!(
                            "Chunk {} statistics: Net read: {:?}, Lock wait: {:?}, Disk write: {:?}",
                            idx, net_time, lock_wait_time, write_time
                        );
                    }


                    if let Some(tx) = tx_clone {
                        let _ = tx.send(DaemonEvent::ChunkReceived {
                            transfer_id,
                            bytes: size as u64,
                        })?;
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

        if let Some(tx) = progress_tx {
            let _ = tx.send(DaemonEvent::TransferComplete { transfer_id });
        }

        anyhow::Ok(())
    }
}
