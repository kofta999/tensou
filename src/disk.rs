use std::{
    fs::{self},
    path::{Path, PathBuf},
    sync::Mutex,
};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::{broadcast, mpsc},
};

use crate::protocol::{ChunkHeader, ChunkPacket, JobInstruction, Metadata, State, TransferEvent};

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
        let offset = index * self.metadata.chunk_size;
        let diff = self.metadata.size - offset;
        if diff < self.metadata.chunk_size {
            diff
        } else {
            self.metadata.chunk_size
        }
    }

    pub async fn get_chunk(&self, index: u64) -> anyhow::Result<(ChunkHeader, Vec<u8>)> {
        let offset = index * self.metadata.chunk_size;
        let mut buf = self.get_read_buffer(offset);

        let mut fd = File::open(&self.full_path).await?;
        fd.seek(std::io::SeekFrom::Start(offset)).await?;
        fd.read_exact(&mut buf).await?;

        Ok((
            ChunkHeader {
                file_id: self.metadata.file_id,
                index,
                hash: Self::hash_chunk(&buf),
            },
            buf,
        ))
    }

    fn get_read_buffer(&self, offset: u64) -> Vec<u8> {
        let diff = self.metadata.size - offset;

        let size = if diff < self.metadata.chunk_size {
            diff
        } else {
            self.metadata.chunk_size
        };

        vec![0u8; size as usize]
    }

    fn hash_chunk(chunk: &[u8]) -> [u8; 32] {
        blake3::hash(chunk).into()
    }
}

pub struct DiskWriter {
    metadata: Metadata,
    state: State,
    state_file_path: PathBuf,
    part_file_path: PathBuf,
    target_path: PathBuf,
    is_resumed: bool,
    file: Option<File>,
    transfer_id: u32,
    rx: mpsc::Receiver<ChunkPacket>,
    event_tx: Option<broadcast::Sender<TransferEvent>>,
}

impl DiskWriter {
    /// Creates sparse file, loads state if exists
    pub fn new(
        IgnitionPayload {
            rx,
            ins,
            target_path,
            transfer_id,
            event_tx,
        }: IgnitionPayload,
    ) -> anyhow::Result<Self> {
        let base_path = if ins.metadata.relative_path.is_empty() {
            target_path.to_path_buf()
        } else {
            target_path.join(Path::new(&ins.metadata.relative_path))
        };

        let mut state_file_path = base_path.clone();
        state_file_path.add_extension("state");
        let mut part_file_path = base_path;
        part_file_path.add_extension("part");

        Ok(Self {
            state: ins.state,
            state_file_path,
            part_file_path,
            metadata: ins.metadata,
            is_resumed: ins.is_resumed,
            target_path: target_path.into(),
            file: None,
            transfer_id,
            rx,
            event_tx,
        })
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
        let mut chunks_since_save: u32 = 0;
        const SAVE_INTERVAL: u32 = 16;

        while let Some(packet) = self.rx.recv().await {
            let size = packet.bytes.len() as u64;

            self.write_chunk(packet).await?;
            chunks_since_save += 1;

            if let Some(ref tx) = self.event_tx {
                let _ = tx.send(TransferEvent::ChunkReceived {
                    transfer_id: self.transfer_id,
                    bytes: size,
                });
            }

            if self.is_complete() {
                self.commit()?;
                break;
            }

            if chunks_since_save >= SAVE_INTERVAL {
                self.save_state().await?;
                chunks_since_save = 0;
            }
        }

        if !self.is_complete() && chunks_since_save > 0 {
            self.save_state().await?;
        }

        Ok(())
    }

    fn allocate_sparse_file(path: &PathBuf, size: u64) -> anyhow::Result<()> {
        let file = fs::File::create(&path)?;
        file.set_len(size as u64)?;
        Ok(())
    }

    async fn save_state(&self) -> anyhow::Result<()> {
        tokio::fs::write(&self.state_file_path, self.state.0.as_raw_slice()).await?;
        Ok(())
    }

    pub async fn write_chunk(&mut self, packet: ChunkPacket) -> anyhow::Result<bool> {
        if packet.hash == Self::hash_chunk(&packet.bytes) {
            let offset = packet.index * self.metadata.chunk_size;

            if !self.is_resumed {
                Self::allocate_sparse_file(&self.part_file_path, self.metadata.size)?;
                self.is_resumed = true;
            }

            if self.file.is_none() {
                let file = tokio::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&self.part_file_path)
                    .await?;
                self.file = Some(file);
            }

            let file_fd = self.file.as_mut().unwrap();

            file_fd.seek(std::io::SeekFrom::Start(offset)).await?;
            file_fd.write_all(&packet.bytes).await?;

            self.state.0.set(packet.index as usize, true);

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

        fs::remove_file(&self.state_file_path)?;

        let dest_path = if self.metadata.relative_path.is_empty() {
            self.target_path.clone()
        } else {
            self.target_path.join(&self.metadata.relative_path)
        };

        fs::rename(&self.part_file_path, dest_path)?;

        Ok(())
    }

    fn hash_chunk(chunk: &[u8]) -> [u8; 32] {
        blake3::hash(chunk).into()
    }
}

pub struct IgnitionPayload {
    pub rx: mpsc::Receiver<ChunkPacket>,
    pub ins: JobInstruction,
    pub target_path: PathBuf,
    pub transfer_id: u32,
    pub event_tx: Option<tokio::sync::broadcast::Sender<TransferEvent>>,
}

pub struct ReceiveSession {
    tx: mpsc::Sender<ChunkPacket>,
    ignition: Mutex<Option<IgnitionPayload>>,
}

impl ReceiveSession {
    pub fn new(tx: mpsc::Sender<ChunkPacket>, ignition: IgnitionPayload) -> Self {
        Self {
            tx,
            ignition: Mutex::new(Some(ignition)),
        }
    }

    pub async fn write_chunk(&self, header: ChunkHeader, bytes: Vec<u8>) -> anyhow::Result<()> {
        if let Some(ign) = self.ignition.lock().unwrap().take() {
            tokio::spawn(async move {
                if let Err(e) = async {
                    let mut writer = DiskWriter::new(ign)?;
                    writer.run().await
                }
                .await
                {
                    eprintln!("Disk I/O Error: {:?}", e);
                }
            });
        }

        self.tx
            .send(ChunkPacket {
                file_id: header.file_id,
                index: header.index,
                hash: header.hash,
                bytes,
            })
            .await?;
        Ok(())
    }
}

// #[cfg(test)]
// mod tests {
//     use crate::{CHUNK_SIZE, protocol::JobInstruction};

//     use super::*;
//     use rand::Rng;
//     use tempfile::tempdir;

//     #[tokio::test]
//     async fn test_full_local_transfer() -> anyhow::Result<()> {
//         // 1. Setup: Create a temporary directory
//         let source_dir = tempdir()?;
//         let dest_dir = tempdir()?;
//         let source_path = source_dir.path().join("source.bin");
//         let received_dir = dest_dir.path();

//         // 2. Mock Data: Create a file with exactly 10MB of random data
//         // (Write logic to fill `source_path` with 10MB of bytes)
//         let mut buffer = vec![0u8; 10 * 1024 * 1024];
//         rand::rng().fill_bytes(&mut buffer);
//         fs::write(&source_path, &buffer)?;

//         let metadata = Metadata {
//             file_id: 0,
//             relative_path: "source.bin".to_string(),
//             size: 10 * 1024 * 1024,
//             chunk_size: CHUNK_SIZE,
//         };
//         let send_session = SendSession::new(metadata, &source_path)?;

//         // 4. Initialize: Create your ReceiveSession and DiskWriter using the sender's metadata
//         let (tx, rx) = mpsc::channel::<ChunkPacket>(10);
//         let receive_session = ReceiveSession::new(tx);

//         let instruction = JobInstruction::new(send_session.get_metadata(), &received_dir)?;

//         let mut writer = DiskWriter::new(
//             instruction.state,
//             instruction.metadata,
//             &received_dir,
//             instruction.is_resumed,
//             0,
//             rx,
//             None,
//         )?;

//         let writer_handle = tokio::spawn(async move { writer.run().await });

//         // 5. The Loop:
//         // Iterate through the total number of chunks.
//         // For each chunk: get_chunk from sender -> write_chunk to receiver.

//         for i in 0..send_session.get_total_chunks() {
//             let chunk = send_session.get_chunk(i as u64).await?;
//             receive_session.write_chunk(chunk).await?;
//         }

//         // 6. The Commit:
//         // Drop the receive session to close the channel, and await the disk writer to finish writing.
//         drop(receive_session);
//         writer_handle.await??;

//         // 7. The Final Verification:
//         // Read `source.bin` and the final received file into memory.
//         // Assert that they are exactly equal.

//         assert!(file_diff::diff(
//             source_path.to_str().unwrap(),
//             received_dir.join("source.bin").to_str().unwrap()
//         ));

//         Ok(())
//     }
// }
