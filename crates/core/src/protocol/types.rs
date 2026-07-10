use crate::FileId;
use async_trait::async_trait;
use bitvec::{bitvec, order::Lsb0, vec::BitVec};
use serde::{Deserialize, Serialize};
use std::{fs, net::SocketAddr, path::Path};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferRequest {
    pub payload: TransferPayload,
    pub sender: SenderInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TransferPayload {
    File(Manifest),
    Text(String),
}

impl TransferPayload {
    pub fn job_name(&self) -> &str {
        match self {
            TransferPayload::File(manifest) => &manifest.job_name,
            TransferPayload::Text { .. } => "Clipboard Text",
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Manifest {
    /// Purely cosmetic name for UI/Notifications (e.g. "Cargo.lock" or "export.zip and 4 other items")
    pub job_name: String,
    /// The top-level files or folders selected by the sender.
    /// E.g., `["document.pdf", "MyPhotos"]` or `["Photos", "Videos"]`
    pub top_level_targets: Vec<String>,
    pub files: Vec<Metadata>,
}

impl Manifest {
    pub fn get_receiving_folder(&self) -> Option<&str> {
        if self.top_level_targets.len() == 1 {
            let target = &self.top_level_targets[0];
            let prefix = format!("{}/", target);
            if self
                .files
                .iter()
                .any(|f| f.relative_path.starts_with(&prefix))
            {
                return Some(target);
            }
        }
        None
    }
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
        let total_chunks = metadata.size.div_ceil(metadata.chunk_size) as usize;

        let state = State(bitvec![u8, Lsb0; 0; total_chunks]);

        Self {
            remaining_bytes: metadata.size,
            is_resumed: false,
            metadata,
            state,
        }
    }

    pub fn load_state_from_disk(
        &mut self,
        state_file_path: &Path,
        final_file_path: &Path,
        overwrite: bool,
    ) -> anyhow::Result<()> {
        // Partial Transfer -> State file exists
        if state_file_path.exists() {
            let state_bytes = fs::read(state_file_path)?;
            let mut bitvec: BitVec<u8, Lsb0> = BitVec::from_vec(state_bytes);
            let expected_len = self.state.0.len();
            if bitvec.len() < expected_len {
                bitvec.resize(expected_len, false);
            } else {
                bitvec.truncate(expected_len);
            }

            self.is_resumed = true;
            self.state = State(bitvec);
            self.remaining_bytes = self.get_remaining_size();
        // File Already Transferred -> Assume state file is all 1s (only when overwrite is false)
        } else if !overwrite
            && let Ok(metadata) = fs::metadata(final_file_path)
            && metadata.len() == self.metadata.size
        {
            self.is_resumed = true;
            self.state.0.fill(true);
            self.remaining_bytes = 0;
        }

        // File does not exist -> new transfer
        Ok(())
    }

    fn get_remaining_size(&self) -> u64 {
        let mut total = 0;
        for idx in 0..self.state.0.len() {
            if let Some(val) = self.state.0.get(idx)
                && !*val
            {
                total += self.metadata.get_chunk_size(idx as u64);
            }
        }
        total
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SenderInfo {
    pub display_name: String,
    pub device_uuid: String,
    pub os_type: String,
}

impl From<&crate::config::Config> for SenderInfo {
    fn from(config: &crate::config::Config) -> Self {
        Self {
            display_name: config.display_name.clone(),
            device_uuid: config.device_uuid.clone(),
            os_type: config.os_type.clone(),
        }
    }
}

pub trait TransferObserver: Send + Sync {
    fn on_transfer_started(
        &self,
        _transfer_id: u32,
        _peer: SocketAddr,
        _total_bytes: u64,
        _bytes_done: u64,
        _job_name: &str,
        _cancel_token: CancellationToken,
    ) {
    }
    fn on_chunk_transferred(&self, _transfer_id: Option<u32>, _bytes: u64) {}
    fn on_transfer_complete(&self, _transfer_id: u32) {}
    fn on_transfer_failed(&self, _transfer_id: u32, _error: &str) {}
    /// Called when a text/clipboard sharing event is received and accepted.
    fn on_text_received(&self, _peer: SocketAddr, _job_name: String, _content: String) {}
}

#[async_trait]
pub trait TransferConsentHandler: Send + Sync {
    async fn request_consent(
        &self,
        peer: SocketAddr,
        sender_info: &SenderInfo,
        job_name: &str,
    ) -> bool;
}
