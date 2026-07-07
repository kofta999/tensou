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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TransferRequest {
    File(Manifest),
    Text { device_name: String, content: String },
}

impl TransferRequest {
    pub fn job_name(&self) -> &str {
        match self {
            TransferRequest::File(manifest) => &manifest.job_name,
            TransferRequest::Text { .. } => "Clipboard Text",
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

// Borrowed from https://chromium.googlesource.com/chromium/src/+/HEAD/base/files/file_path.cc
fn find_extension_start(file_name: &str) -> usize {
    let last_dot = match file_name.rfind('.') {
        Some(idx) if idx > 0 => idx,
        _ => return file_name.len(),
    };

    let penultimate_dot = match file_name[..last_dot].rfind('.') {
        Some(idx) => idx,
        None => return last_dot,
    };

    let common_suffixes = ["bz", "bz2", "gz", "lz", "lzma", "lzo", "xz", "z", "zst"];
    let final_ext = &file_name[last_dot + 1..].to_ascii_lowercase();

    if common_suffixes.contains(&final_ext.as_str()) {
        let middle_segment_len = last_dot - penultimate_dot;
        if middle_segment_len <= 5 && middle_segment_len > 1 {
            return penultimate_dot;
        }
    }

    last_dot
}

pub fn find_unique_path(path: &Path) -> PathBuf {
    if !path.exists() {
        return path.to_path_buf();
    }

    let file_name = path.file_name().unwrap().to_string_lossy().into_owned();
    let ext_start = find_extension_start(&file_name);

    let stem = &file_name[..ext_start];
    let ext = &file_name[ext_start..]; // includes the leading dot (e.g. ".tar.gz")

    let parent = path.parent().unwrap_or_else(|| Path::new(""));
    let mut counter = 1;

    loop {
        let new_file_name = if path.is_dir() {
            format!("{} ({})", file_name, counter)
        } else {
            format!("{} ({}){}", stem, counter, ext)
        };

        let new_path = parent.join(new_file_name);

        if !new_path.exists() {
            return new_path;
        }
        counter += 1;
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

pub struct ManifestManager;

impl ManifestManager {
    pub fn parse(
        manifest: Manifest,
        staging: Arc<TransferStaging>,
        overwrite: bool,
    ) -> anyhow::Result<Vec<JobInstruction>> {
        let mut instructions = Vec::new();
        let mut unique_mappings = HashMap::new(); // Map: Original Target -> Unique Target

        for target in manifest.top_level_targets {
            let original_path = staging.final_path(&target);
            let has_staging =
                staging.state_path(&target).exists() || staging.staging_dir.join(&target).is_dir();

            let unique_path = if overwrite || has_staging {
                original_path
            } else {
                crate::protocol::find_unique_path(&original_path)
            };

            let unique_name = unique_path
                .file_name()
                .unwrap()
                .to_string_lossy()
                .into_owned();
            unique_mappings.insert(target, unique_name);
        }

        for mut metadata in manifest.files.into_iter() {
            for (original_target, unique_target) in &unique_mappings {
                if metadata.relative_path == *original_target {
                    // Flat file match: rewrite directly
                    metadata.relative_path = unique_target.clone();
                    break;
                } else if metadata
                    .relative_path
                    .starts_with(&format!("{}/", original_target))
                {
                    // Nested folder file match: replace parent prefix
                    let subpath = &metadata.relative_path[original_target.len() + 1..];
                    metadata.relative_path = format!("{}/{}", unique_target, subpath);
                    break;
                }
            }

            if !is_safe_relative_path(Path::new(&metadata.relative_path)) {
                bail!("Invalid path")
            }

            let mut instruction = JobInstruction::new(metadata);
            let state_path = &staging.state_path(&instruction.metadata.relative_path);
            let final_path = &staging.final_path(&instruction.metadata.relative_path);
            instruction.load_state_from_disk(state_path, final_path, overwrite)?;

            // If the file is already complete (size 0) and does not exist at final destination,
            // create the parent folder structure and the empty file on the blocking thread.
            if instruction.state.0.all() && (overwrite || !final_path.exists()) {
                staging.create_file_destination_dir(&instruction.metadata.relative_path)?;
                std::fs::File::create(final_path)?;
            }

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
                relative_path: if path.is_dir() {
                    let parent = path.parent().unwrap_or(path);
                    entry
                        .path()
                        .strip_prefix(parent)?
                        .to_string_lossy()
                        .into_owned()
                } else {
                    entry.file_name().to_string_lossy().into_owned()
                },
                size: entry.metadata()?.len(),
            };

            sessions.insert(
                i,
                Arc::new(SendSession::new(metadata.clone(), entry.path())?),
            );

            files.push(metadata);
        }

        let name = path
            .file_name()
            .map(|v| v.to_string_lossy().into_owned())
            .ok_or(anyhow::anyhow!("Cannot get name of folder path"))?;
        Ok((
            Manifest {
                job_name: name.clone(),
                top_level_targets: vec![name],
                files,
            },
            sessions,
        ))
    }

    pub fn build_multiple(
        paths: &[PathBuf],
    ) -> anyhow::Result<(Manifest, HashMap<FileId, Arc<SendSession>>)> {
        let mut files = Vec::new();
        let mut sessions = HashMap::new();
        let mut file_id_counter = 0;
        let mut top_level_targets = Vec::new();

        for path in paths {
            if let Some(path) = path.file_name() {
                top_level_targets.push(path.to_string_lossy().into_owned());
            }

            let parent = path
                .parent()
                .ok_or_else(|| anyhow::anyhow!("No parent directory"))?;

            for entry in WalkDir::new(path)
                .into_iter()
                .filter_map(|e| e.ok())
                .filter(|e| e.file_type().is_file())
            {
                let metadata = Metadata {
                    file_id: file_id_counter,
                    chunk_size: CHUNK_SIZE.into(),
                    relative_path: entry
                        .path()
                        .strip_prefix(parent)?
                        .to_string_lossy()
                        .into_owned(),
                    size: entry.metadata()?.len(),
                };

                sessions.insert(
                    file_id_counter,
                    Arc::new(SendSession::new(metadata.clone(), entry.path())?),
                );

                files.push(metadata);
                file_id_counter += 1;
            }
        }

        // Generate descriptive job name (e.g. "document.pdf and 2 other items")
        let job_name = if paths.len() == 1 {
            paths[0]
                .file_name()
                .map(|v| v.to_string_lossy().into_owned())
                .unwrap_or_else(|| "Files".to_string())
        } else {
            format!(
                "{} and {} other items",
                paths[0]
                    .file_name()
                    .map(|v| v.to_string_lossy().into_owned())
                    .unwrap_or_default(),
                paths.len() - 1
            )
        };

        Ok((
            Manifest {
                job_name,
                top_level_targets,
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
    /// Called when a text/clipboard sharing event is received and accepted.
    fn on_text_received(&self, _peer: SocketAddr, _job_name: String, _content: String) {}
}

#[async_trait]
pub trait TransferConsentHandler: Send + Sync {
    async fn request_consent(&self, peer: SocketAddr, job_name: &str) -> bool;
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    /// Verifies that find_unique_path returns the original path unchanged if the file does not exist.
    #[test]
    fn unique_path_nonexistent_returns_as_is() {
        let dir = tempdir().unwrap();
        let target = dir.path().join("ghost.bin");
        assert_eq!(find_unique_path(&target), target);
    }

    /// Verifies that find_unique_path appends a counter suffix (e.g. `(1)`) if a file already exists.
    #[test]
    fn unique_path_existing_file_increments() {
        let dir = tempdir().unwrap();
        let target = dir.path().join("file.txt");
        std::fs::write(&target, b"x").unwrap();

        let unique = find_unique_path(&target);
        assert_eq!(unique, dir.path().join("file (1).txt"));
    }

    /// Verifies that find_unique_path increments the counter progressively if multiple naming collisions exist.
    #[test]
    fn unique_path_multiple_collisions() {
        let dir = tempdir().unwrap();
        let target = dir.path().join("archive.tar.gz");
        std::fs::write(&target, b"x").unwrap();
        std::fs::write(dir.path().join("archive (1).tar.gz"), b"x").unwrap();
        std::fs::write(dir.path().join("archive (2).tar.gz"), b"x").unwrap();

        let unique = find_unique_path(&target);
        assert_eq!(unique, dir.path().join("archive (3).tar.gz"));
    }

    /// Verifies that find_unique_path appends the suffix correctly for files without extensions (e.g., `Makefile`).
    #[test]
    fn unique_path_no_extension() {
        let dir = tempdir().unwrap();
        let target = dir.path().join("Makefile");
        std::fs::write(&target, b"x").unwrap();

        let unique = find_unique_path(&target);
        assert_eq!(unique, dir.path().join("Makefile (1)"));
    }

    /// Verifies that find_unique_path handles dotfiles (e.g. `.bashrc`) correctly.
    #[test]
    fn unique_path_dotfile() {
        let dir = tempdir().unwrap();
        let target = dir.path().join(".bashrc");
        std::fs::write(&target, b"x").unwrap();

        let unique = find_unique_path(&target);
        assert_eq!(unique, dir.path().join(".bashrc (1)"));
    }

    /// Verifies that Metadata::get_chunk_size returns the standard chunk size for intermediate chunks.
    #[test]
    fn chunk_size_full_chunks() {
        let m = Metadata {
            file_id: 0,
            relative_path: "".into(),
            size: 12,
            chunk_size: 4,
        };
        assert_eq!(m.get_chunk_size(0), 4);
        assert_eq!(m.get_chunk_size(1), 4);
        assert_eq!(m.get_chunk_size(2), 4);
    }

    /// Verifies that Metadata::get_chunk_size returns the correct remaining bytes for the final partial chunk.
    #[test]
    fn chunk_size_last_partial_chunk() {
        let m = Metadata {
            file_id: 0,
            relative_path: "".into(),
            size: 10,
            chunk_size: 4,
        };
        assert_eq!(m.get_chunk_size(0), 4);
        assert_eq!(m.get_chunk_size(1), 4);
        assert_eq!(m.get_chunk_size(2), 2);
    }

    /// Verifies that Metadata::get_chunk_size returns the entire file size if it is smaller than the chunk size limit.
    #[test]
    fn chunk_size_single_chunk_smaller_than_max() {
        let m = Metadata {
            file_id: 0,
            relative_path: "".into(),
            size: 100,
            chunk_size: 4 * 1024 * 1024,
        };
        assert_eq!(m.get_chunk_size(0), 100);
    }

    /// Verifies that constructing a JobInstruction is a pure in-memory operation without disk side-effects.
    #[test]
    fn job_instruction_new_is_memory_only() {
        let dir = tempdir().unwrap();
        let m = Metadata {
            file_id: 0,
            relative_path: "sub/file.txt".into(),
            size: 8,
            chunk_size: 4,
        };
        let ins = JobInstruction::new(m);

        assert!(!dir.path().join("sub").exists());
        assert!(!ins.is_resumed);
        assert_eq!(ins.remaining_bytes, 8);
        assert!(ins.state.0.not_any());
    }

    /// Verifies that load_state_from_disk restores bitvec states and sizes for resuming.
    #[test]
    fn job_instruction_load_state_resumes_correctly() {
        let dir = tempdir().unwrap();
        let m = Metadata {
            file_id: 0,
            relative_path: "".into(),
            size: 8,
            chunk_size: 4,
        };
        let mut ins = JobInstruction::new(m);

        let mut bv = bitvec![u8, Lsb0; 0; 2];
        bv.set(0, true);
        let state_file = dir.path().join("resume.state");
        std::fs::write(&state_file, bv.as_raw_slice()).unwrap();

        let final_file = dir.path().join("resume.final");
        ins.load_state_from_disk(&state_file, &final_file, false)
            .unwrap();

        assert!(ins.is_resumed);
        assert!(ins.state.0[0]);
        assert!(!ins.state.0[1]);
        assert_eq!(ins.remaining_bytes, 4);
    }

    /// Verifies that load_state_from_disk handles empty/truncated state files without panicking.
    #[test]
    fn job_instruction_load_state_empty_or_truncated() {
        let dir = tempdir().unwrap();
        let m = Metadata {
            file_id: 0,
            relative_path: "".into(),
            size: 16,
            chunk_size: 4,
        }; // Requires 4 chunks

        // Test 1: Empty state file (0 bytes)
        let mut ins = JobInstruction::new(m.clone());
        let state_file = dir.path().join("empty.state");
        std::fs::write(&state_file, &[]).unwrap();
        let final_file = dir.path().join("empty.final");
        ins.load_state_from_disk(&state_file, &final_file, false)
            .unwrap();
        assert!(ins.is_resumed);
        assert_eq!(ins.state.0.len(), 4);
        assert!(ins.state.0.not_any());

        // Test 2: Truncated state file (contains only 1 byte, but we expect 4 chunks)
        let mut ins = JobInstruction::new(m);
        let state_file = dir.path().join("truncated.state");
        std::fs::write(&state_file, &[0b0000_0001]).unwrap(); // Lsb0 order: bit 0 is true, others false
        let final_file = dir.path().join("truncated.final");
        ins.load_state_from_disk(&state_file, &final_file, false)
            .unwrap();
        assert!(ins.is_resumed);
        assert_eq!(ins.state.0.len(), 4);
        assert!(ins.state.0[0]);
        assert!(!ins.state.0[1]);
    }

    /// Verifies that load_state_from_disk behaves as a no-op if the state file does not exist.
    #[test]
    fn job_instruction_load_state_nonexistent_is_noop() {
        let m = Metadata {
            file_id: 0,
            relative_path: "".into(),
            size: 8,
            chunk_size: 4,
        };
        let mut ins = JobInstruction::new(m);
        ins.load_state_from_disk(
            std::path::Path::new("/tmp/does_not_exist_xyz.state"),
            std::path::Path::new("/tmp/does_not_exist_xyz.final"),
            false,
        )
        .unwrap();

        assert!(!ins.is_resumed);
        assert_eq!(ins.remaining_bytes, 8);
    }

    /// Verifies that load_state_from_disk marks the instruction as fully completed if the final file exists and matches size.
    #[test]
    fn job_instruction_load_state_completed_file() {
        let dir = tempdir().unwrap();
        let m = Metadata {
            file_id: 0,
            relative_path: "already_done.txt".into(),
            size: 12,
            chunk_size: 4,
        };
        let mut ins = JobInstruction::new(m);

        let final_file = dir.path().join("already_done.txt");
        std::fs::write(&final_file, b"hello world\n").unwrap();

        let state_file = dir.path().join("already_done.state");

        ins.load_state_from_disk(&state_file, &final_file, false)
            .unwrap();

        assert!(ins.is_resumed);
        assert!(ins.state.0.all());
        assert_eq!(ins.remaining_bytes, 0);
    }

    /// Verifies that load_state_from_disk does NOT mark the instruction as completed if the final file exists but overwrite is true.
    #[test]
    fn job_instruction_load_state_completed_file_overwrite() {
        let dir = tempdir().unwrap();
        let m = Metadata {
            file_id: 0,
            relative_path: "already_done.txt".into(),
            size: 12,
            chunk_size: 4,
        };
        let mut ins = JobInstruction::new(m);

        let final_file = dir.path().join("already_done.txt");
        std::fs::write(&final_file, b"hello world\n").unwrap();

        let state_file = dir.path().join("already_done.state");

        ins.load_state_from_disk(&state_file, &final_file, true)
            .unwrap();

        assert!(!ins.is_resumed);
        assert!(!ins.state.0.all());
        assert_eq!(ins.remaining_bytes, 12);
    }

    /// Verifies that ManifestManager::build correctly constructs a manifest for a single file.
    #[test]
    fn manifest_build_single_file() {
        let dir = tempdir().unwrap();
        let file = dir.path().join("hello.txt");
        std::fs::write(&file, b"hello world").unwrap();

        let (manifest, sessions) = ManifestManager::build(&file).unwrap();

        assert_eq!(manifest.job_name, "hello.txt");
        assert_eq!(manifest.files.len(), 1);
        assert_eq!(manifest.files[0].relative_path, "hello.txt");
        assert_eq!(manifest.files[0].size, 11);
        assert_eq!(sessions.len(), 1);
    }

    /// Verifies that ManifestManager::build preserves relative directory layouts for folder transfers.
    #[test]
    fn manifest_build_nested_directory() {
        let dir = tempdir().unwrap();
        std::fs::create_dir(dir.path().join("sub")).unwrap();
        std::fs::write(dir.path().join("a.txt"), b"aaa").unwrap();
        std::fs::write(dir.path().join("sub/b.txt"), b"bbbbb").unwrap();

        let (manifest, sessions) = ManifestManager::build(dir.path()).unwrap();

        assert_eq!(manifest.files.len(), 2);
        assert_eq!(sessions.len(), 2);

        let dir_name = dir.path().file_name().unwrap().to_str().unwrap();
        let expected_a = format!("{}/a.txt", dir_name);
        let expected_b = format!("{}/sub/b.txt", dir_name);

        let paths: Vec<&str> = manifest
            .files
            .iter()
            .map(|m| m.relative_path.as_str())
            .collect();
        assert!(paths.contains(&expected_a.as_str()));
        assert!(paths.contains(&expected_b.as_str()));
    }

    /// Verifies that paths containing traversal components like `..` are explicitly rejected.
    #[test]
    fn manifest_build_path_traversal_is_rejected() {
        let staging_dir = tempdir().unwrap();
        let staging = std::sync::Arc::new(crate::disk::TransferStaging::new(
            staging_dir.path().to_path_buf(),
            1,
        ));
        staging.prepare().unwrap();

        let manifest = Manifest {
            job_name: "job".into(),
            top_level_targets: vec![],
            files: vec![Metadata {
                file_id: 0,
                relative_path: "../escape.txt".into(),
                size: 4,
                chunk_size: 4,
            }],
        };

        let result = ManifestManager::parse(manifest, staging, false);
        assert!(result.is_err(), "Path traversal should be rejected");
    }

    /// Verifies that ManifestManager::parse correctly renames top-level targets to resolve collisions.
    #[test]
    fn manifest_parse_resolves_unique_targets() {
        let downloads_dir = tempdir().unwrap();

        // Pre-create an existing file and folder to trigger collisions
        std::fs::write(downloads_dir.path().join("a.txt"), b"existing file").unwrap();
        std::fs::create_dir(downloads_dir.path().join("MyPhotos")).unwrap();
        std::fs::write(
            downloads_dir.path().join("MyPhotos/pic1.jpg"),
            b"existing pic",
        )
        .unwrap();

        let staging = std::sync::Arc::new(crate::disk::TransferStaging::new(
            downloads_dir.path().to_path_buf(),
            1,
        ));
        staging.prepare().unwrap();

        // Manifest containing the colliding targets
        let manifest = Manifest {
            job_name: "multi_transfer".into(),
            top_level_targets: vec!["a.txt".into(), "MyPhotos".into()],
            files: vec![
                Metadata {
                    file_id: 0,
                    relative_path: "a.txt".into(),
                    size: 5,
                    chunk_size: 4,
                },
                Metadata {
                    file_id: 1,
                    relative_path: "MyPhotos/pic1.jpg".into(),
                    size: 5,
                    chunk_size: 4,
                },
            ],
        };

        let instructions = ManifestManager::parse(manifest, staging, false).unwrap();
        assert_eq!(instructions.len(), 2);

        // Check that paths are correctly updated to unique names
        assert_eq!(instructions[0].metadata.relative_path, "a (1).txt");
        assert_eq!(
            instructions[1].metadata.relative_path,
            "MyPhotos (1)/pic1.jpg"
        );
    }
}
