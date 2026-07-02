use crate::protocol::{
    ChunkHeader, ChunkPacket, ChunkPacketReceiver, ChunkPacketSender, JobInstruction, Metadata,
    State, TransferObserver,
};
use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    task,
};
use tokio_util::sync::CancellationToken;

pub struct SendSession {
    metadata: Metadata,
    total_chunks: usize,
    full_path: PathBuf,
}

impl SendSession {
    pub fn new(metadata: Metadata, full_path: &Path) -> anyhow::Result<Self> {
        let total_chunks: usize =
            ((metadata.size + metadata.chunk_size - 1) / metadata.chunk_size).try_into()?;

        Ok(Self {
            metadata,
            total_chunks,
            full_path: full_path.to_path_buf(),
        })
    }

    pub fn get_metadata(&self) -> Metadata {
        self.metadata.clone()
    }

    pub fn get_total_chunks(&self) -> usize {
        self.total_chunks
    }

    pub fn get_chunk_size(&self, index: u64) -> u64 {
        self.metadata.get_chunk_size(index)
    }

    pub async fn get_chunk(&self, index: u64) -> anyhow::Result<(ChunkHeader, Vec<u8>)> {
        let offset = index * self.metadata.chunk_size;
        let mut buf = self.get_read_buffer(index);

        let mut fd = File::open(&self.full_path).await?;
        fd.seek(std::io::SeekFrom::Start(offset)).await?;
        fd.read_exact(&mut buf).await?;

        Ok((
            ChunkHeader {
                file_id: self.metadata.file_id,
                index,
                hash: ChunkHeader::hash_chunk(&buf),
            },
            buf,
        ))
    }

    fn get_read_buffer(&self, index: u64) -> Vec<u8> {
        vec![0u8; self.metadata.get_chunk_size(index) as usize]
    }
}

pub struct DiskWriter {
    metadata: Metadata,
    state: State,
    staging: Arc<TransferStaging>,
    is_resumed: bool,
    file: Option<File>,
    transfer_id: u32,
    rx: ChunkPacketReceiver,
    observer: Arc<dyn TransferObserver>,
    cancel_token: CancellationToken,
}

impl DiskWriter {
    pub fn new(
        IgnitionPayload {
            rx,
            ins,
            transfer_id,
            observer,
            cancel_token,
            staging,
        }: IgnitionPayload,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            state: ins.state,
            metadata: ins.metadata,
            is_resumed: ins.is_resumed,
            staging,
            file: None,
            transfer_id,
            rx,
            observer,
            cancel_token,
        })
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
        let mut chunks_since_save: u32 = 0;
        const SAVE_INTERVAL: u32 = 16;

        loop {
            tokio::select! {
                _ = self.cancel_token.cancelled() =>  {
                    self.save_state().await?;
                    return Ok(())
                }

                maybe_packet = self.rx.recv() => {
                    match maybe_packet {
                        Some(packet) => {
                            let size = packet.bytes.len() as u64;

                            self.write_chunk(packet).await?;
                            chunks_since_save += 1;

                            self.observer
                                .on_chunk_transferred(Some(self.transfer_id), size);

                            if self.is_complete() {
                                self.commit()?;
                                break;
                            }

                            if chunks_since_save >= SAVE_INTERVAL {
                                self.save_state().await?;
                                chunks_since_save = 0;
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

    async fn allocate_sparse_file(path: &PathBuf, size: u64) -> anyhow::Result<()> {
        let file = tokio::fs::File::create(&path).await?;
        file.set_len(size).await?;
        Ok(())
    }

    async fn save_state(&self) -> anyhow::Result<()> {
        tokio::fs::write(
            &self.staging.state_path(&self.metadata.relative_path),
            self.state.0.as_raw_slice(),
        )
        .await?;
        Ok(())
    }

    pub async fn write_chunk(&mut self, packet: ChunkPacket) -> anyhow::Result<bool> {
        if packet.header.hash == ChunkHeader::hash_chunk(&packet.bytes) {
            let offset = packet.header.index * self.metadata.chunk_size;
            let part_path = self.staging.part_path(&self.metadata.relative_path);

            if self.file.is_none() {
                self.staging
                    .create_file_staging_dir(&self.metadata.relative_path)?;
            }

            if !self.is_resumed {
                Self::allocate_sparse_file(&part_path, self.metadata.size).await?;
                self.is_resumed = true;
            }

            if self.file.is_none() {
                let file = tokio::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&part_path)
                    .await?;
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
        // Drop the open file handle so it is closed and we can rename/remove it
        self.file = None;

        let state_path = self.staging.state_path(&self.metadata.relative_path);
        if state_path.exists() {
            std::fs::remove_file(state_path)?;
        }

        self.staging
            .create_file_destination_dir(&self.metadata.relative_path)?;

        let part_path = self.staging.part_path(&self.metadata.relative_path);
        let final_path = self.staging.final_path(&self.metadata.relative_path);
        std::fs::rename(part_path, final_path)?;

        Ok(())
    }
}

pub struct IgnitionPayload {
    pub rx: ChunkPacketReceiver,
    pub ins: JobInstruction,
    pub transfer_id: u32,
    pub observer: Arc<dyn TransferObserver>,
    pub staging: Arc<TransferStaging>,
    pub cancel_token: CancellationToken,
}

pub struct ReceiveSession {
    tx: ChunkPacketSender,
    ignition: Mutex<Option<IgnitionPayload>>,
    pub writer_handle: Mutex<Option<task::JoinHandle<anyhow::Result<()>>>>,
}

impl ReceiveSession {
    pub fn new(tx: ChunkPacketSender, ignition: IgnitionPayload) -> Self {
        Self {
            tx,
            ignition: Mutex::new(Some(ignition)),
            writer_handle: Mutex::new(None),
        }
    }

    pub async fn write_chunk(&self, header: ChunkHeader, bytes: Vec<u8>) -> anyhow::Result<()> {
        if let Some(ign) = self.ignition.lock().unwrap().take() {
            let handle = tokio::spawn(async move {
                let mut writer = DiskWriter::new(ign)?;
                writer.run().await
            });

            *self.writer_handle.lock().unwrap() = Some(handle);
        }

        // We've resumes, so it's no problem if we couldn't send a packet once
        if self.tx.send(ChunkPacket { header, bytes }).await.is_err() {
            return anyhow::Ok(());
        };

        Ok(())
    }
}

pub struct TransferStaging {
    /// Final user-visible destination directory (e.g. `Downloads/MyTransfer/`)
    pub dest_dir: PathBuf,
    /// Hidden staging directory on the same partition (e.g. `Downloads/MyTransfer/.tensou/`)
    pub staging_dir: PathBuf,
    /// Either directory name or file name if it's a single-file operation (e.g. `MyPhotos (1)`)
    job_name: String,
    is_single_file: bool,
}

impl TransferStaging {
    pub fn new(
        downloads_dir: PathBuf,
        job_name: &str,
        overwrite: bool,
        is_single_file: bool,
    ) -> Self {
        let base_path = downloads_dir.join(&job_name);
        let unique_path = if overwrite {
            base_path
        } else {
            crate::protocol::find_unique_path(&base_path)
        };

        let resolved_job_name = unique_path
            .file_name()
            .map(|v| v.to_string_lossy().into_owned())
            .unwrap_or_else(|| job_name.to_string());

        let dest_dir = if is_single_file {
            downloads_dir
        } else {
            unique_path
        };

        Self {
            staging_dir: dest_dir.join(".tensou"),
            dest_dir,
            job_name: resolved_job_name,
            is_single_file,
        }
    }

    /// Resolve where a partial download should go (e.g. `.tensou/subfolder/file.part`)
    pub fn part_path(&self, relative_path: &str) -> PathBuf {
        let path_name = if self.is_single_file && relative_path.is_empty() {
            &self.job_name
        } else {
            relative_path
        };

        self.staging_dir
            .join(path_name)
            .with_added_extension("part")
    }

    /// Resolve where a transfer state file should go (e.g. `.tensou/subfolder/file.state`)
    pub fn state_path(&self, relative_path: &str) -> PathBuf {
        let path_name = if self.is_single_file && relative_path.is_empty() {
            &self.job_name
        } else {
            relative_path
        };

        self.staging_dir
            .join(path_name)
            .with_added_extension("state")
    }

    /// Resolve the final destination path (e.g. `MyTransfer/subfolder/file`)
    pub fn final_path(&self, relative_path: &str) -> PathBuf {
        let path_name = if self.is_single_file && relative_path.is_empty() {
            &self.job_name
        } else {
            relative_path
        };

        self.dest_dir.join(path_name)
    }

    pub fn prepare(&self) -> std::io::Result<()> {
        std::fs::create_dir_all(&self.staging_dir)
    }

    /// Safely creates the parent directory hierarchy for a staging file
    /// (e.g., creating `.tensou/nested_folder/` so we can write the .part file)
    pub fn create_file_staging_dir(&self, relative_path: &str) -> std::io::Result<()> {
        let part_path = self.part_path(relative_path);
        if let Some(parent) = part_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        Ok(())
    }

    /// Safely creates the parent directory hierarchy in the final destination folder
    /// (e.g., creating `MyPhotos/nested_folder/` right before renaming the file out of staging)
    pub fn create_file_destination_dir(&self, relative_path: &str) -> std::io::Result<()> {
        let final_path = self.final_path(relative_path);
        if let Some(parent) = final_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        Ok(())
    }

    /// Clean up the staging directory recursively
    pub fn cleanup(&self) -> std::io::Result<()> {
        if self.staging_dir.exists() {
            return std::fs::remove_dir_all(&self.staging_dir);
        }
        Ok(())
    }

    /// Accessor for the active job name (e.g. for notifications or UI tracking)
    pub fn job_name(&self) -> &str {
        &self.job_name
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CHUNK_SIZE, protocol::JobInstruction};
    use rand::Rng;
    use std::{fs, time::Duration};
    use tempfile::tempdir;
    use tokio::sync::mpsc;

    struct TestObserver;
    impl TransferObserver for TestObserver {}

    #[tokio::test]
    async fn test_full_local_transfer() -> anyhow::Result<()> {
        let source_dir = tempdir()?;
        let dest_dir = tempdir()?;
        let source_path = source_dir.path().join("source.bin");
        let received_dir = dest_dir.path();

        let mut buffer = vec![0u8; 10 * 1024 * 1024];
        rand::rng().fill_bytes(&mut buffer);
        fs::write(&source_path, &buffer)?;

        let metadata = Metadata {
            file_id: 0,
            relative_path: "source.bin".to_string(),
            size: 10 * 1024 * 1024,
            chunk_size: CHUNK_SIZE as u64,
        };
        let send_session = SendSession::new(metadata, &source_path)?;

        let (tx, rx) = mpsc::channel::<ChunkPacket>(10);
        let instruction = JobInstruction::new(send_session.get_metadata());

        let staging = Arc::new(TransferStaging::new(
            received_dir.to_path_buf(),
            "source.bin",
            true,
            true,
        ));
        staging.prepare()?;

        let ignition = IgnitionPayload {
            ins: instruction,
            rx,
            transfer_id: 0,
            observer: Arc::new(TestObserver {}),
            cancel_token: CancellationToken::new(),
            staging,
        };
        let receive_session = ReceiveSession::new(tx, ignition);

        for i in 0..send_session.get_total_chunks() {
            let (header, bytes) = send_session.get_chunk(i as u64).await?;
            receive_session.write_chunk(header, bytes).await?;
        }

        // Wait until disk finishes
        tokio::time::sleep(Duration::from_secs(2)).await;

        assert!(file_diff::diff(
            source_path.to_str().unwrap(),
            received_dir.join("source.bin").to_str().unwrap()
        ));

        Ok(())
    }
}
