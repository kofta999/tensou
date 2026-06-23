use std::path::{Component, Path};

pub const CHUNK_SIZE: u32 = 4 * 1024 * 1024;
pub const MAX_METADATA_SIZE: u64 = 64 * 1024 * 1024;
pub const MAX_CONCURRENT_STREAMS: u16 = 8;
pub const SERVICE_TYPE: &str = "_tensou._udp.local.";

pub type FileId = usize;
pub type ChunkIndex = u64;

pub mod cli;
pub mod crypto;
pub mod discovery;
pub mod disk;
pub mod net;
mod protocol;

pub fn is_safe_relative_path(path: &Path) -> bool {
    path.components().all(|c| matches!(c, Component::Normal(_)))
}
