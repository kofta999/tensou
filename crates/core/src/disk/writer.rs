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

use std::sync::atomic::{AtomicBool, Ordering};

pub struct ReceiveSession {
    metadata: Metadata,
    state: Arc<Mutex<State>>,
    staging: Arc<TransferStaging>,
    is_resumed: Arc<AtomicBool>,
    file: Arc<tokio::sync::Mutex<Option<File>>>,
    transfer_id: Uuid,
    tx: ChunkPacketSender,
    rx: Mutex<Option<ChunkPacketReceiver>>,
    observer: Arc<dyn TransferObserver>,
    cancel_token: CancellationToken,
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
        Self {
            metadata: ins.metadata,
            state: Arc::new(Mutex::new(ins.state)),
            staging,
            is_resumed: Arc::new(AtomicBool::new(ins.is_resumed)),
            file: Arc::new(tokio::sync::Mutex::new(None)),
            transfer_id,
            tx,
            rx: Mutex::new(Some(rx)),
            observer,
            cancel_token,
            writer_handle: Mutex::new(None),
        }
    }

    pub async fn write_chunk(&self, header: ChunkHeader, bytes: Vec<u8>) -> anyhow::Result<()> {
        let maybe_rx = self.rx.lock().unwrap().take();
        if let Some(rx) = maybe_rx {
            let metadata = self.metadata.clone();
            let state = self.state.clone();
            let staging = self.staging.clone();
            let is_resumed = self.is_resumed.clone();
            let file = self.file.clone();
            let transfer_id = self.transfer_id;
            let observer = self.observer.clone();
            let cancel_token = self.cancel_token.clone();

            let handle = tokio::spawn(async move {
                Self::run_writer_loop(
                    rx,
                    metadata,
                    state,
                    staging,
                    is_resumed,
                    file,
                    transfer_id,
                    observer,
                    cancel_token,
                )
                .await
            });
            *self.writer_handle.lock().unwrap() = Some(handle);
        }

        if self.tx.send(ChunkPacket { header, bytes }).await.is_err() {
            return Ok(());
        }
        Ok(())
    }

    async fn run_writer_loop(
        mut rx: ChunkPacketReceiver,
        metadata: Metadata,
        state: Arc<Mutex<State>>,
        staging: Arc<TransferStaging>,
        is_resumed: Arc<AtomicBool>,
        file: Arc<tokio::sync::Mutex<Option<File>>>,
        transfer_id: Uuid,
        observer: Arc<dyn TransferObserver>,
        cancel_token: CancellationToken,
    ) -> anyhow::Result<()> {
        let mut chunks_since_save: u32 = 0;
        const SAVE_INTERVAL: u32 = 16;

        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    if !state.lock().unwrap().0.all() && is_resumed.load(Ordering::Relaxed) {
                        Self::save_state(&metadata, &state, &staging).await?;
                    }
                    return Ok(());
                }

                maybe_packet = rx.recv() => {
                    match maybe_packet {
                        Some(packet) => {
                            let size = packet.bytes.len() as u64;

                            if !Self::write_chunk_internal(&packet, &metadata, &state, &staging, &is_resumed, &file).await? {
                                cancel_token.cancel();
                                anyhow::bail!("Chunk integrity check failed — retry the transfer");
                            } else {
                                chunks_since_save += 1;

                                observer.on_chunk_transferred(transfer_id, size);

                                if state.lock().unwrap().0.all() {
                                    Self::commit(&metadata, &staging, &file).await?;
                                    break;
                                }

                                if chunks_since_save >= SAVE_INTERVAL {
                                    Self::save_state(&metadata, &state, &staging).await?;
                                    chunks_since_save = 0;
                                }
                            }
                        }
                        None => break,
                    }
                }
            }
        }

        if !state.lock().unwrap().0.all() && chunks_since_save > 0 {
            Self::save_state(&metadata, &state, &staging).await?;
        }

        Ok(())
    }

    async fn save_state(
        metadata: &Metadata,
        state: &Arc<Mutex<State>>,
        staging: &Arc<TransferStaging>,
    ) -> anyhow::Result<()> {
        if metadata.size <= metadata.chunk_size {
            return Ok(());
        }
        let raw_slice = state.lock().unwrap().0.as_raw_slice().to_vec();
        tokio::fs::write(
            &staging.state_path(&metadata.relative_path),
            &raw_slice,
        )
        .await?;
        Ok(())
    }

    async fn write_chunk_internal(
        packet: &ChunkPacket,
        metadata: &Metadata,
        state: &Arc<Mutex<State>>,
        staging: &Arc<TransferStaging>,
        is_resumed: &Arc<AtomicBool>,
        file: &Arc<tokio::sync::Mutex<Option<File>>>,
    ) -> anyhow::Result<bool> {
        if packet.header.hash == ChunkHeader::hash_chunk(&packet.bytes) {
            let offset = packet.header.index * metadata.chunk_size;
            let part_path = staging.part_path(&metadata.relative_path);
            let final_path = staging.final_path(&metadata.relative_path);
            let is_small = metadata.size <= metadata.chunk_size;

            let write_path = if is_small { &final_path } else { &part_path };

            let mut file_guard = file.lock().await;

            if file_guard.is_none() {
                if is_small {
                    staging.create_file_destination_dir(&metadata.relative_path)?;
                } else {
                    staging.create_file_staging_dir(&metadata.relative_path)?;
                }

                let f = tokio::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(false)
                    .open(write_path)
                    .await?;

                if !is_resumed.load(Ordering::Relaxed) {
                    if !is_small {
                        f.set_len(metadata.size).await?;
                    }
                    is_resumed.store(true, Ordering::Relaxed);
                }

                *file_guard = Some(f);
            }

            let file_fd = file_guard.as_mut().unwrap();

            file_fd.seek(std::io::SeekFrom::Start(offset)).await?;
            file_fd.write_all(&packet.bytes).await?;

            state.lock().unwrap().0.set(packet.header.index as usize, true);

            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn commit(
        metadata: &Metadata,
        staging: &Arc<TransferStaging>,
        file: &Arc<tokio::sync::Mutex<Option<File>>>,
    ) -> anyhow::Result<()> {
        *file.lock().await = None;

        let state_path = staging.state_path(&metadata.relative_path);
        if state_path.exists() {
            std::fs::remove_file(state_path)?;
        }

        let is_small = metadata.size <= metadata.chunk_size;
        let final_path = staging.final_path(&metadata.relative_path);
        if !is_small {
            staging.create_file_destination_dir(&metadata.relative_path)?;

            let part_path = staging.part_path(&metadata.relative_path);
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
                f.set_modified(SystemTime::UNIX_EPOCH + Duration::from_secs(metadata.modified))
        {
            log::warn!("Failed to set mtime for {:?}: {}", final_path, e);
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
