use std::path::{Component, Path};

pub const SERVER_PORT: u16 = 6967;
pub const CHUNK_SIZE: u32 = 4 * 1024 * 1024;
pub const MAX_METADATA_SIZE: u64 = 64 * 1024 * 1024;
pub const MAX_CONCURRENT_STREAMS: u16 = 8;
pub const SERVICE_TYPE: &str = "_tensou._udp.local.";

// According to the robot:
// This tells the sender "you can have up to 8 MB of unacknowledged data in flight." On a LAN with ~0.1ms RTT, you only need:
// bandwidth × RTT = 125 MB/s × 0.0001s = 12.5 KB
// Even with some headroom, 8-16 MB is more than enough for any LAN.
pub const QUIC_RECEIVE_WINDOW: u32 = 8 * 1024 * 1024;

// 5 MB (CHUNK_SIZE + extra 1 MB for headers etc)
pub const QUIC_STREAM_RECEIVE_WINDOW: u32 = 5 * 1024 * 1024;

pub type FileId = usize;
pub type TransferId = u32;
pub type ChunkIndex = u64;

pub mod cli;
pub mod config;
pub mod crypto;
pub mod discovery;
pub mod disk;
pub mod gui;
pub mod net;
mod protocol;

pub fn is_safe_relative_path(path: &Path) -> bool {
    path.components().all(|c| matches!(c, Component::Normal(_)))
}
