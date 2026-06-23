use crate::{
    CHUNK_SIZE,
    disk::{ReceiveSession, SendSession},
};
use crate::{FileId, is_safe_relative_path};
use anyhow::bail;
use bitvec::{order::Lsb0, vec::BitVec};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
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
pub struct ChunkPacket {
    pub file_id: FileId,
    pub index: u64,
    // Optimizes serializing of u8 arrays (50% size reduction)
    #[serde(with = "serde_bytes")]
    pub hash: [u8; 32],
    #[serde(with = "serde_bytes")]
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct State(pub BitVec<u8, Lsb0>);

pub struct ManifestManager;

impl ManifestManager {
    pub fn parse(
        manifest: Manifest,
        target_path: &Path,
    ) -> anyhow::Result<(
        Vec<State>,
        HashMap<FileId, Arc<tokio::sync::Mutex<ReceiveSession>>>,
        u64,
    )> {
        let mut sessions = HashMap::new();
        let mut states = Vec::new();
        let mut remaining_bytes = 0;

        for (i, metadata) in manifest.files.into_iter().enumerate() {
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

            let session = ReceiveSession::new(metadata, target_path)?;
            states.push(session.get_state());
            remaining_bytes += session.get_remaining_size();
            sessions.insert(i, Arc::new(tokio::sync::Mutex::new(session)));
        }

        Ok((states, sessions, remaining_bytes))
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
                chunk_size: CHUNK_SIZE,
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
pub enum DaemonEvent {
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
