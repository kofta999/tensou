use crate::disk::{SendSession, TransferStaging};
use crate::{CHUNK_SIZE, FileId, is_safe_relative_path};
use anyhow::bail;
use async_trait::async_trait;
use bitvec::{bitvec, order::Lsb0, vec::BitVec};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::{collections::HashMap, fs, net::SocketAddr, path::Path, sync::Arc};
use tokio_util::sync::CancellationToken;
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

impl Metadata {
    pub fn get_chunk_size(&self, index: u64) -> u64 {
        let offset = index * self.chunk_size;
        let diff = self.size - offset;
        if diff < self.chunk_size {
            diff
        } else {
            self.chunk_size
        }
    }
}

pub fn find_unique_path(path: &Path) -> PathBuf {
    let mut p = path.to_path_buf();
    if !p.exists() {
        return p;
    }
    let base_name = path.file_stem().and_then(|s| s.to_str());
    let extension = path.extension().and_then(|s| s.to_str());

    if let Some(base_name) = base_name {
        let mut counter = 1;
        loop {
            let candidate = match extension {
                Some(e) => format!("{base_name} ({counter}).{e}"),
                None => format!("{base_name} ({counter})"),
            };
            p.set_file_name(candidate);
            if !p.exists() {
                return p;
            }
            counter += 1;
        }
    }
    p
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChunkHeader {
    pub file_id: FileId,
    pub index: u64,
    #[serde(with = "serde_bytes")]
    pub hash: [u8; 32],
}

impl ChunkHeader {
    pub fn hash_chunk(chunk: &[u8]) -> [u8; 32] {
        blake3::hash(chunk).into()
    }
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
    pub fn new(metadata: Metadata) -> Self {
        let total_chunks =
            ((metadata.size + metadata.chunk_size - 1) / metadata.chunk_size) as usize;

        let state = State(bitvec![u8, Lsb0; 0; total_chunks]);

        Self {
            remaining_bytes: metadata.size,
            is_resumed: false,
            metadata,
            state,
        }
    }

    pub fn load_state_from_disk(&mut self, state_file_path: &Path) -> anyhow::Result<()> {
        if state_file_path.exists() {
            let state_bytes = fs::read(&state_file_path)?;
            let mut bitvec: BitVec<u8, Lsb0> = BitVec::from_vec(state_bytes);
            bitvec.truncate(self.state.0.len());

            self.is_resumed = true;
            self.state = State(bitvec);
            self.remaining_bytes = self.get_remaining_size();
        }

        Ok(())
    }

    fn get_remaining_size(&self) -> u64 {
        let mut total = 0;
        for idx in 0..self.state.0.len() {
            if let Some(val) = self.state.0.get(idx) {
                if !*val {
                    total += self.metadata.get_chunk_size(idx as u64);
                }
            }
        }
        total
    }
}

pub struct ManifestManager;

impl ManifestManager {
    pub fn parse(
        manifest: Manifest,
        staging: Arc<TransferStaging>,
    ) -> anyhow::Result<Vec<JobInstruction>> {
        let mut instructions = Vec::new();

        for metadata in manifest.files.into_iter() {
            if !is_safe_relative_path(Path::new(&metadata.relative_path)) {
                bail!("Invalid path")
            }

            let mut instruction = JobInstruction::new(metadata);
            let state_path = &staging.state_path(&instruction.metadata.relative_path);
            instruction.load_state_from_disk(&state_path)?;

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

pub trait TransferObserver: Send + Sync {
    fn on_transfer_started(
        &self,
        _transfer_id: u32,
        _peer: SocketAddr,
        _total_bytes: u64,
        _job_name: &str,
        _cancel_token: CancellationToken,
    ) {
    }
    fn on_chunk_transferred(&self, _transfer_id: Option<u32>, _bytes: u64) {}
    fn on_transfer_complete(&self, _transfer_id: u32) {}
    fn on_transfer_failed(&self, _transfer_id: u32, _error: &str) {}
}

#[async_trait]
pub trait TransferConsentHandler: Send + Sync {
    async fn request_consent(&self, peer: SocketAddr, job_name: &str) -> bool;
}
