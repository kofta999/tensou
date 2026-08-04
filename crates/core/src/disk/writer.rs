use crate::{
    disk::TransferStaging,
    protocol::{
        ChunkHeader, ChunkPacket, ChunkPacketReceiver, ChunkPacketSender, JobInstruction, Metadata,
        State, TransferObserver,
    },
};
use std::{
    fs,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};
use tokio::{
    fs::File,
    io::{AsyncSeekExt, AsyncWriteExt},
    task,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

pub struct SmallFilePayload {
    pub relative_path: String,
    pub modified: u64,
    pub data: Vec<u8>,
}

pub fn write_small_files_to_disk(
    rx: std::sync::mpsc::Receiver<SmallFilePayload>,
    staging: Arc<TransferStaging>,
    observer: Arc<dyn TransferObserver>,
    transfer_id: Uuid,
) -> anyhow::Result<()> {
    while let Ok(payload) = rx.recv() {
        let data_len = payload.data.len() as u64;
        let final_path = staging.final_path(&payload.relative_path);
        staging.create_file_destination_dir(&payload.relative_path)?;
        std::fs::write(&final_path, &payload.data)?;

        if let Ok(f) = std::fs::File::open(&final_path) {
            let _ = f.set_modified(SystemTime::UNIX_EPOCH + Duration::from_secs(payload.modified));
        }

        observer.on_chunk_transferred(transfer_id, data_len);
    }
    Ok(())
}

struct ReceiveWriterWorker {
    metadata: Metadata,
    state: State,
    staging: Arc<TransferStaging>,
    is_resumed: bool,
    file: Option<File>,
    transfer_id: Uuid,
    rx: ChunkPacketReceiver,
    observer: Arc<dyn TransferObserver>,
    cancel_token: CancellationToken,
}

impl ReceiveWriterWorker {
    async fn run(&mut self) -> anyhow::Result<()> {
        let mut chunks_since_save: u32 = 0;
        const SAVE_INTERVAL: u32 = 16;

        loop {
            tokio::select! {
                _ = self.cancel_token.cancelled() => {
                    if !self.is_complete() && self.is_resumed {
                        self.save_state().await?;
                    }
                    return Ok(());
                }

                maybe_packet = self.rx.recv() => {
                    match maybe_packet {
                        Some(packet) => {
                            let size = packet.bytes.len() as u64;

                            if !self.write_chunk(packet).await? {
                                self.cancel_token.cancel();
                                anyhow::bail!("Chunk integrity check failed — retry the transfer");
                            } else {
                                chunks_since_save += 1;

                                self.observer
                                    .on_chunk_transferred(self.transfer_id, size);

                                if self.is_complete() {
                                    self.commit()?;
                                    break;
                                }

                                if chunks_since_save >= SAVE_INTERVAL {
                                    self.save_state().await?;
                                    chunks_since_save = 0;
                                }
                            }
                        }
                        None => break,
                    }
                }
            }
        }

        if !self.is_complete() && chunks_since_save > 0 {
            self.save_state().await?;
        }

        Ok(())
    }

    async fn save_state(&self) -> anyhow::Result<()> {
        if self.metadata.size <= self.metadata.chunk_size {
            return Ok(());
        }
        tokio::fs::write(
            &self.staging.state_path(&self.metadata.relative_path),
            self.state.0.as_raw_slice(),
        )
        .await?;
        Ok(())
    }

    async fn write_chunk(&mut self, packet: ChunkPacket) -> anyhow::Result<bool> {
        if packet.header.hash == ChunkHeader::hash_chunk(&packet.bytes) {
            let offset = packet.header.index * self.metadata.chunk_size;
            let part_path = self.staging.part_path(&self.metadata.relative_path);
            let final_path = self.staging.final_path(&self.metadata.relative_path);
            let is_small = self.metadata.size <= self.metadata.chunk_size;

            let write_path = if is_small { &final_path } else { &part_path };

            if self.file.is_none() {
                if is_small {
                    self.staging
                        .create_file_destination_dir(&self.metadata.relative_path)?;
                } else {
                    self.staging
                        .create_file_staging_dir(&self.metadata.relative_path)?;
                }
            }

            if self.file.is_none() {
                let file = tokio::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(false)
                    .open(write_path)
                    .await?;

                if !self.is_resumed {
                    if !is_small {
                        file.set_len(self.metadata.size).await?;
                    }
                    self.is_resumed = true;
                }

                self.file = Some(file);
            }

            let file_fd = self.file.as_mut().unwrap();

            file_fd.seek(std::io::SeekFrom::Start(offset)).await?;
            file_fd.write_all(&packet.bytes).await?;

            self.state.0.set(packet.header.index as usize, true);

            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn is_complete(&self) -> bool {
        self.state.0.all()
    }

    fn commit(&mut self) -> anyhow::Result<()> {
        self.file = None;

        let state_path = self.staging.state_path(&self.metadata.relative_path);
        if state_path.exists() {
            std::fs::remove_file(state_path)?;
        }

        let is_small = self.metadata.size <= self.metadata.chunk_size;
        let final_path = self.staging.final_path(&self.metadata.relative_path);
        if !is_small {
            self.staging
                .create_file_destination_dir(&self.metadata.relative_path)?;

            let part_path = self.staging.part_path(&self.metadata.relative_path);
            log::debug!(
                "Renaming completed file from {:?} to {:?}",
                part_path,
                final_path
            );
            std::fs::rename(part_path, &final_path)?;
        } else {
            log::debug!(
                "File {:?} is small, already written to final path",
                final_path
            );
        }

        if let Ok(f) = fs::File::open(&final_path)
            && let Err(e) =
                f.set_modified(SystemTime::UNIX_EPOCH + Duration::from_secs(self.metadata.modified))
        {
            log::warn!("Failed to set mtime for {:?}: {}", final_path, e);
        }

        Ok(())
    }
}

pub struct ReceiveSession {
    tx: ChunkPacketSender,
    pub(crate) writer_handle: Mutex<Option<task::JoinHandle<anyhow::Result<()>>>>,
}

impl ReceiveSession {
    pub fn new(
        ins: JobInstruction,
        transfer_id: Uuid,
        observer: Arc<dyn TransferObserver>,
        cancel_token: CancellationToken,
        staging: Arc<TransferStaging>,
    ) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel::<ChunkPacket>(crate::MAX_CONCURRENT_STREAMS.into());
        let mut worker = ReceiveWriterWorker {
            metadata: ins.metadata,
            state: ins.state,
            staging,
            is_resumed: ins.is_resumed,
            file: None,
            transfer_id,
            rx,
            observer,
            cancel_token,
        };

        let handle = tokio::spawn(async move { worker.run().await });

        Self {
            tx,
            writer_handle: Mutex::new(Some(handle)),
        }
    }

    pub async fn write_chunk(&self, header: ChunkHeader, bytes: Vec<u8>) -> anyhow::Result<()> {
        if self.tx.send(ChunkPacket { header, bytes }).await.is_err() {
            return Ok(());
        }
        Ok(())
    }

    /// Wait for the background writing task to finish and commit.
    pub async fn join_writer(&self) -> anyhow::Result<()> {
        let handle = self.writer_handle.lock().unwrap().take();
        if let Some(h) = handle {
            h.await??;
        }
        Ok(())
    }
}
