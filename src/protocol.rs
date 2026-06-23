use crate::{CHUNK_SIZE, disk::SendSession};
use crate::{FileId, is_safe_relative_path};
use anyhow::bail;
use bitvec::{bitvec, order::Lsb0, vec::BitVec};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fs,
    net::SocketAddr,
    path::Path,
    sync::{Arc, Mutex},
};
use tokio::sync::oneshot;
use walkdir::WalkDir;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Manifest {
    /// Root folder name
    pub job_name: String,
    pub files: Vec<Metadata>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Metadata {
    pub file_id: FileId,
    pub relative_path: String,
    pub size: u64,
    pub chunk_size: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChunkHeader {
    pub file_id: FileId,
    pub index: u64,
    #[serde(with = "serde_bytes")]
    pub hash: [u8; 32],
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChunkPacket {
    pub header: ChunkHeader,
    pub bytes: Vec<u8>,
}

pub type ChunkPacketSender = tokio::sync::mpsc::Sender<ChunkPacket>;
pub type ChunkPacketReceiver = tokio::sync::mpsc::Receiver<ChunkPacket>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct State(pub BitVec<u8, Lsb0>);

pub struct JobInstruction {
    pub metadata: Metadata,
    pub is_resumed: bool,
    pub state: State,
    pub remaining_bytes: u64,
}

impl JobInstruction {
    pub(crate) fn new(metadata: Metadata, target_path: &Path) -> anyhow::Result<Self> {
        let total_chunks: usize =
            ((metadata.size + metadata.chunk_size - 1) / metadata.chunk_size).try_into()?;
        let mut is_resumed = false;

        let base_path = if metadata.relative_path.is_empty() {
            target_path.to_path_buf()
        } else {
            target_path.join(Path::new(&metadata.relative_path))
        };

        let mut state_file_path = base_path.clone();
        state_file_path.add_extension("state");

        if let Some(parent) = state_file_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let state = if state_file_path.exists() {
            let state_bytes = fs::read(&state_file_path)?;

            let mut bitvec: BitVec<u8, Lsb0> = BitVec::from_vec(state_bytes);
            bitvec.truncate(total_chunks);

            is_resumed = true;

            State(bitvec)
        } else {
            let state = State(bitvec![u8, Lsb0; 0; total_chunks]);
            fs::write(&state_file_path, state.0.as_raw_slice())?;

            state
        };

        Ok(Self {
            remaining_bytes: Self::get_remaining_size(&state, &metadata),
            is_resumed,
            metadata,
            state,
        })
    }

    fn get_remaining_size(state: &State, metadata: &Metadata) -> u64 {
        let mut total = 0;
        for idx in 0..state.0.len() {
            if let Some(val) = state.0.get(idx) {
                if !*val {
                    let offset = idx as u64 * metadata.chunk_size;
                    let diff = metadata.size - offset;
                    let size = if diff < metadata.chunk_size {
                        diff
                    } else {
                        metadata.chunk_size
                    };
                    total += size;
                }
            }
        }
        total
    }
}

pub struct ManifestManager;

impl ManifestManager {
    pub fn parse(manifest: Manifest, target_path: &Path) -> anyhow::Result<Vec<JobInstruction>> {
        let mut instructions = Vec::new();

        for metadata in manifest.files.into_iter() {
            if !is_safe_relative_path(Path::new(&metadata.relative_path)) {
                bail!("Invalid path")
            }

            let full_path = if metadata.relative_path.is_empty() {
                target_path.to_path_buf()
            } else {
                target_path.join(&metadata.relative_path)
            };

            if let Some(parent) = full_path.parent() {
                std::fs::create_dir_all(parent)?;
            }

            let instruction = JobInstruction::new(metadata, target_path)?;
            instructions.push(instruction);
        }

        Ok(instructions)
    }

    pub fn build(path: &Path) -> anyhow::Result<(Manifest, HashMap<FileId, Arc<SendSession>>)> {
        let mut files = Vec::new();
        let mut sessions = HashMap::new();

        for (i, entry) in WalkDir::new(path)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().is_file())
            .enumerate()
        {
            let metadata = Metadata {
                file_id: i,
                chunk_size: CHUNK_SIZE.into(),
                relative_path: entry
                    .path()
                    .strip_prefix(path)?
                    .to_string_lossy()
                    .into_owned(),
                size: entry.metadata()?.len(),
            };

            sessions.insert(
                i,
                Arc::new(SendSession::new(metadata.clone(), entry.path())?),
            );

            files.push(metadata);
        }

        Ok((
            Manifest {
                job_name: path
                    .file_name()
                    .map(|v| v.to_string_lossy().into_owned())
                    .ok_or(anyhow::anyhow!("Cannot get name of folder path"))?,
                files,
            },
            sessions,
        ))
    }
}

#[derive(Debug, Clone)]
pub enum TransferEvent {
    /// Fired when a sender connects and sends the Manifest
    ConsentRequested {
        peer: SocketAddr,
        job_name: String,
        // total_bytes: u64,
        // file_count: u64,
        // Escape hatch for non-clone types with multiple access points
        reply_tx: Arc<Mutex<Option<oneshot::Sender<bool>>>>,
    },
    /// Fired when the receiver accepts the transfer
    TransferStarted {
        transfer_id: u32,
        peer: SocketAddr,
        total_bytes: u64,
        job_name: String,
    },
    /// Fired every time a chunk is successfully written to disk
    ChunkReceived { transfer_id: u32, bytes: u64 },
    /// Fired when the final file is committed
    TransferComplete { transfer_id: u32 },
}

pub type TransferEventSender = tokio::sync::broadcast::Sender<TransferEvent>;
