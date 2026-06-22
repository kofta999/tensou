use std::path::{Component, Path};

pub const CHUNK_SIZE: u64 = 4 * 1024 * 1024;

// As it contains raw 4mb data + messagepack headers + hash + index
pub const MAX_QUIC_CHUNK_SIZE: usize = 5 * 1024 * 1024;
pub const MAX_CONCURRENT_STREAMS: u8 = 255;
pub const SERVICE_TYPE: &str = "_tensou._udp.local.";

pub type FileId = usize;
pub type ChunkIndex = u64;

pub mod crypto;
pub mod discovery;
pub mod disk;
pub mod net;
mod protocol;

pub fn is_safe_relative_path(path: &Path) -> bool {
    path.components().all(|c| matches!(c, Component::Normal(_)))
}
