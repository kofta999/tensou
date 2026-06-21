use bitvec::{order::Lsb0, vec::BitVec};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Metadata {
    pub filename: String,
    pub size: u64,
    pub chunk_size: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChunkPacket {
    pub index: u64,
    // Optimizes serializing of u8 arrays (50% size reduction)
    #[serde(with = "serde_bytes")]
    pub hash: [u8; 32],
    #[serde(with = "serde_bytes")]
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct State(pub BitVec<u8, Lsb0>);
