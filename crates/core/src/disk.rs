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

#[derive(Debug)]
pub struct SendSession {
    metadata: Metadata,
    total_chunks: usize,
    full_path: PathBuf,
}

impl SendSession {
    pub fn new(metadata: Metadata, full_path: &Path) -> anyhow::Result<Self> {
        let total_chunks: usize = metadata.size.div_ceil(metadata.chunk_size).try_into()?;

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
    pub(crate) writer_handle: Mutex<Option<task::JoinHandle<anyhow::Result<()>>>>,
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
        let base_path = downloads_dir.join(job_name);
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
    use std::fs;
    use tempfile::tempdir;
    use tokio::sync::mpsc;

    struct TestObserver;
    impl TransferObserver for TestObserver {}

    fn make_staging(dir: &std::path::Path, job: &str, single: bool) -> Arc<TransferStaging> {
        let s = Arc::new(TransferStaging::new(dir.to_path_buf(), job, true, single));
        s.prepare().unwrap();
        s
    }

    /// Verifies staging paths calculation for a folder-based transfer.
    #[test]
    fn staging_folder_transfer_paths() {
        let dir = tempdir().unwrap();
        let staging = make_staging(dir.path(), "MyPhotos", false);

        assert_eq!(
            staging.staging_dir,
            dir.path().join("MyPhotos").join(".tensou")
        );
        assert_eq!(
            staging.part_path("nested/img.jpg"),
            dir.path()
                .join("MyPhotos")
                .join(".tensou")
                .join("nested/img.jpg.part")
        );
        assert_eq!(
            staging.state_path("nested/img.jpg"),
            dir.path()
                .join("MyPhotos")
                .join(".tensou")
                .join("nested/img.jpg.state")
        );
        assert_eq!(
            staging.final_path("nested/img.jpg"),
            dir.path().join("MyPhotos").join("nested/img.jpg")
        );
    }

    /// Verifies staging paths calculation for a single-file transfer.
    #[test]
    fn staging_single_file_transfer_paths() {
        let dir = tempdir().unwrap();
        let staging = make_staging(dir.path(), "photo.jpg", true);

        assert_eq!(staging.staging_dir, dir.path().join(".tensou"));
        assert_eq!(
            staging.part_path(""),
            dir.path().join(".tensou").join("photo.jpg.part")
        );
        assert_eq!(
            staging.state_path(""),
            dir.path().join(".tensou").join("photo.jpg.state")
        );
        assert_eq!(staging.final_path(""), dir.path().join("photo.jpg"));
    }

    /// Verifies that job name is deduplicated by finding a unique path if overwrite is disabled.
    #[test]
    fn staging_overwrite_false_deduplicates_job_name() {
        let dir = tempdir().unwrap();
        fs::create_dir(dir.path().join("MyPhotos")).unwrap();

        let staging = Arc::new(TransferStaging::new(
            dir.path().to_path_buf(),
            "MyPhotos",
            false,
            false,
        ));

        assert_eq!(staging.job_name(), "MyPhotos (1)");
        assert_eq!(staging.dest_dir, dir.path().join("MyPhotos (1)"));
    }

    /// Verifies that prepare() correctly creates the hidden staging directory on disk.
    #[test]
    fn staging_prepare_creates_staging_dir() {
        let dir = tempdir().unwrap();
        let staging = Arc::new(TransferStaging::new(
            dir.path().to_path_buf(),
            "job",
            true,
            false,
        ));
        assert!(!staging.staging_dir.exists());
        staging.prepare().unwrap();
        assert!(staging.staging_dir.exists());
    }

    /// Verifies that staging cleanup removes the hidden staging directory completely.
    #[test]
    fn staging_cleanup_removes_dir() {
        let dir = tempdir().unwrap();
        let staging = make_staging(dir.path(), "job", false);
        assert!(staging.staging_dir.exists());
        staging.cleanup().unwrap();
        assert!(!staging.staging_dir.exists());
    }

    /// Verifies that create_file_staging_dir creates nested folder structures inside the staging folder.
    #[test]
    fn staging_create_file_staging_dir_nested() {
        let dir = tempdir().unwrap();
        let staging = make_staging(dir.path(), "job", false);
        staging.create_file_staging_dir("a/b/c.txt").unwrap();
        assert!(staging.staging_dir.join("a/b").is_dir());
    }

    /// Verifies that create_file_destination_dir creates nested destination directory hierarchies.
    #[test]
    fn staging_create_file_destination_dir_nested() {
        let dir = tempdir().unwrap();
        let staging = make_staging(dir.path(), "job", false);
        staging.create_file_destination_dir("a/b/c.txt").unwrap();
        assert!(staging.dest_dir.join("a/b").is_dir());
    }

    /// Verifies a complete and byte-perfect local transfer of a single file.
    #[tokio::test]
    async fn test_full_local_transfer() -> anyhow::Result<()> {
        let source_dir = tempdir()?;
        let dest_dir = tempdir()?;
        let source_path = source_dir.path().join("source.bin");

        let mut buffer = vec![0u8; 10 * 1024 * 1024];
        rand::rng().fill_bytes(&mut buffer);
        fs::write(&source_path, &buffer)?;

        let metadata = crate::protocol::Metadata {
            file_id: 0,
            relative_path: "source.bin".to_string(),
            size: 10 * 1024 * 1024,
            chunk_size: CHUNK_SIZE as u64,
        };
        let send_session = SendSession::new(metadata, &source_path)?;

        let (tx, rx) = mpsc::channel::<ChunkPacket>(16);
        let instruction = JobInstruction::new(send_session.get_metadata());

        let staging = make_staging(dest_dir.path(), "source.bin", true);

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

        let handle = receive_session.writer_handle.lock().unwrap().take();
        drop(receive_session);
        if let Some(h) = handle {
            h.await??;
        }

        assert!(file_diff::diff(
            source_path.to_str().unwrap(),
            dest_dir.path().join("source.bin").to_str().unwrap()
        ));

        Ok(())
    }

    struct ChunkSignalObserver {
        tx: mpsc::Sender<()>,
    }
    impl crate::protocol::TransferObserver for ChunkSignalObserver {
        fn on_chunk_transferred(&self, _transfer_id: Option<u32>, _bytes: u64) {
            let tx = self.tx.clone();
            tokio::spawn(async move {
                let _ = tx.send(()).await;
            });
        }
    }

    /// Verifies that cancellation mid-transfer leaves the partial .part file intact.
    #[tokio::test]
    async fn test_cancel_preserves_partial_files() -> anyhow::Result<()> {
        let source_dir = tempdir()?;
        let dest_dir = tempdir()?;
        let source_path = source_dir.path().join("big.bin");

        let file_size = 3 * CHUNK_SIZE as u64;
        fs::write(&source_path, vec![0xABu8; file_size as usize])?;

        let metadata = crate::protocol::Metadata {
            file_id: 0,
            relative_path: "big.bin".to_string(),
            size: file_size,
            chunk_size: CHUNK_SIZE as u64,
        };
        let send_session = SendSession::new(metadata, &source_path)?;

        let (tx, rx) = mpsc::channel::<ChunkPacket>(16);
        let instruction = JobInstruction::new(send_session.get_metadata());
        let cancel_token = CancellationToken::new();

        let staging = make_staging(dest_dir.path(), "big.bin", true);
        let part_path = staging.part_path("");

        let (obs_tx, mut obs_rx) = mpsc::channel(1);
        let observer = Arc::new(ChunkSignalObserver { tx: obs_tx });

        let ignition = IgnitionPayload {
            ins: instruction,
            rx,
            transfer_id: 0,
            observer,
            cancel_token: cancel_token.clone(),
            staging,
        };
        let receive_session = ReceiveSession::new(tx, ignition);

        let (header, bytes) = send_session.get_chunk(0).await?;
        receive_session.write_chunk(header, bytes).await?;

        let _ = obs_rx.recv().await;

        cancel_token.cancel();

        let handle = receive_session.writer_handle.lock().unwrap().take();
        drop(receive_session);
        if let Some(h) = handle {
            let _ = h.await;
        }

        assert!(
            part_path.exists(),
            ".part file should be kept after cancellation"
        );

        Ok(())
    }
}
