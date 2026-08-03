use crate::disk::{SendSession, TransferStaging};
use crate::protocol::{JobInstruction, Manifest, State};
use crate::protocol::{Metadata, TransferMode};
use crate::util::{generate_job_name, is_safe_relative_path};
use crate::{CHUNK_SIZE, FileId, SMALL_FILE_THRESHOLD, STAGING_DIR_NAME};
use anyhow::bail;
use bitvec::order::Lsb0;
use bitvec::vec::BitVec;
use std::path::PathBuf;
use std::{collections::HashMap, path::Path, sync::Arc};
use walkdir::WalkDir;

fn get_unique_paths(
    manifest: &Manifest,
    downloads_dir: &Path,
    transfer_mode: TransferMode,
) -> HashMap<String, String> {
    let mut unique_mappings = HashMap::new(); // Map: Original Target -> Unique Target

    for target in &manifest.top_level_targets {
        let original_path = downloads_dir.join(target);
        let has_staging = {
            // If target is a folder: downloads_dir/target/.tensou exists
            let folder_staging = original_path.join(STAGING_DIR_NAME);
            // If target is a file: downloads_dir/.tensou/target.state exists
            let file_state = downloads_dir
                .join(STAGING_DIR_NAME)
                .join(format!("{}.state", target));
            folder_staging.exists() || file_state.exists()
        };

        let unique_path = if transfer_mode != TransferMode::Unique || has_staging {
            original_path
        } else {
            crate::util::find_unique_path(&original_path)
        };

        let unique_name = unique_path
            .file_name()
            .unwrap()
            .to_string_lossy()
            .into_owned();
        unique_mappings.insert(target.clone(), unique_name);
    }
    unique_mappings
}

pub fn get_top_level_prefix<'a>(
    manifest: &Manifest,
    unique_mappings: &'a HashMap<String, String>,
) -> Option<&'a str> {
    if manifest.top_level_targets.len() == 1 {
        let target = &manifest.top_level_targets[0];
        let unique_target = unique_mappings.get(target).unwrap();
        let prefix = format!("{}/", target);
        if manifest
            .large_files
            .iter()
            .chain(manifest.small_files.iter())
            .any(|f| f.relative_path.starts_with(&prefix))
        {
            Some(unique_target.as_str())
        } else {
            None
        }
    } else {
        None
    }
}

fn rewrite_relative_path(metadata: &mut Metadata, unique_mappings: &HashMap<String, String>) {
    for (original_target, unique_target) in unique_mappings {
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
}

pub fn parse(
    manifest: Manifest,
    downloads_dir: &Path,
    transfer_mode: TransferMode,
) -> anyhow::Result<(Vec<JobInstruction>, Arc<TransferStaging>, State)> {
    let mut instructions = Vec::new();
    let unique_mappings = get_unique_paths(&manifest, downloads_dir, transfer_mode);
    let top_level_prefix = get_top_level_prefix(&manifest, &unique_mappings);
    let mut small_files_state = BitVec::<u8, Lsb0>::with_capacity(manifest.small_files.len());

    let staging = Arc::new(TransferStaging::new(
        downloads_dir.to_path_buf(),
        top_level_prefix,
    ));

    for mut metadata in manifest.small_files.into_iter() {
        rewrite_relative_path(&mut metadata, &unique_mappings);

        if !is_safe_relative_path(Path::new(&metadata.relative_path)) {
            bail!("Invalid path")
        }

        let final_path = &staging.final_path(&metadata.relative_path);

        let should_skip = match transfer_mode {
            TransferMode::Unique => final_path.exists(),
            TransferMode::Overwrite => false,
            TransferMode::Sync => std::fs::metadata(final_path).map_or(false, |local_meta| {
                let local_mtime = local_meta
                    .modified()
                    .ok()
                    .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                    .map(|d| d.as_secs())
                    .unwrap_or(0);
                local_meta.len() == metadata.size && local_mtime == metadata.modified
            }),
        };

        if metadata.size == 0 && !should_skip {
            staging.create_file_destination_dir(&metadata.relative_path)?;
            std::fs::File::create(final_path)?;
            small_files_state.push(true);
            continue;
        }

        small_files_state.push(should_skip);
    }

    for mut metadata in manifest.large_files.into_iter() {
        rewrite_relative_path(&mut metadata, &unique_mappings);

        if !is_safe_relative_path(Path::new(&metadata.relative_path)) {
            bail!("Invalid path")
        }

        let final_path = &staging.final_path(&metadata.relative_path);

        log::info!(
            "Parsing file '{:?}' with transfer_mode {:?}",
            metadata.relative_path,
            transfer_mode
        );

        match transfer_mode {
            TransferMode::Unique => {
                let mut instruction = JobInstruction::new(metadata);
                let state_path = &staging.state_path(&instruction.metadata.relative_path);
                log::debug!("Unique mode: loading state from disk for {:?}", final_path);
                instruction.load_state_from_disk(state_path, final_path, false)?;

                // If the file is already complete (size 0) and does not exist at final destination,
                // create the parent folder structure and the empty file on the blocking thread.
                if instruction.state.0.all() && !final_path.exists() {
                    log::info!(
                        "Unique mode: creating empty file for 0-byte transfer at {:?}",
                        final_path
                    );
                    staging.create_file_destination_dir(&instruction.metadata.relative_path)?;
                    std::fs::File::create(final_path)?;
                }

                instructions.push(instruction);
            }
            TransferMode::Overwrite => {
                let mut instruction = JobInstruction::new(metadata);
                let state_path = &staging.state_path(&instruction.metadata.relative_path);
                log::debug!(
                    "Overwrite mode: loading state from disk (overwrite=true) for {:?}",
                    final_path
                );
                instruction.load_state_from_disk(state_path, final_path, true)?;
                if instruction.state.0.all() {
                    log::info!(
                        "Overwrite mode: creating empty file for 0-byte transfer at {:?}",
                        final_path
                    );
                    staging.create_file_destination_dir(&instruction.metadata.relative_path)?;
                    std::fs::File::create(final_path)?;
                }
                instructions.push(instruction);
            }
            TransferMode::Sync => {
                // Sync: skip files with matching size + mtime
                if let Ok(local_meta) = std::fs::metadata(final_path) {
                    let local_mtime = local_meta
                        .modified()
                        .ok()
                        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                        .map(|d| d.as_secs())
                        .unwrap_or(0);

                    log::info!(
                        "Sync comparison for {:?}: local_size={}, local_mtime={}; incoming_size={}, incoming_mtime={}",
                        final_path,
                        local_meta.len(),
                        local_mtime,
                        metadata.size,
                        metadata.modified
                    );

                    if local_meta.len() == metadata.size && local_mtime == metadata.modified {
                        log::info!(
                            "Sync: File {:?} matches local file exactly. Skipping transfer.",
                            final_path
                        );
                        let mut instruction = JobInstruction::new(metadata);
                        instruction.state.0.fill(true);
                        instruction.remaining_bytes = 0;
                        instruction.is_resumed = true;
                        instructions.push(instruction);
                        continue;
                    } else {
                        log::info!(
                            "Sync: File {:?} differs (size or mtime mismatch). Proceeding with transfer.",
                            final_path
                        );
                    }
                } else {
                    log::info!(
                        "Sync: File {:?} does not exist locally. Proceeding with transfer.",
                        final_path
                    );
                }

                let mut instruction = JobInstruction::new(metadata);
                let state_path = &staging.state_path(&instruction.metadata.relative_path);
                instruction.load_state_from_disk(state_path, final_path, true)?;
                if instruction.state.0.all() {
                    log::info!(
                        "Sync mode: creating empty file for 0-byte transfer at {:?}",
                        final_path
                    );
                    staging.create_file_destination_dir(&instruction.metadata.relative_path)?;
                    std::fs::File::create(final_path)?;
                }
                instructions.push(instruction);
            }
        }
    }

    Ok((instructions, staging, State(small_files_state)))
}

pub fn build(
    paths: &[PathBuf],
) -> anyhow::Result<(
    Manifest,
    HashMap<FileId, Arc<SendSession>>,
    HashMap<FileId, PathBuf>,
)> {
    let mut large_files = Vec::new();
    let mut small_files = Vec::new();
    let mut sessions = HashMap::new();
    let mut small_files_paths = HashMap::new();
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
            let fs_metadata = entry.metadata()?;
            let metadata = Metadata {
                file_id: file_id_counter,
                chunk_size: CHUNK_SIZE.into(),
                relative_path: entry
                    .path()
                    .strip_prefix(parent)?
                    .to_string_lossy()
                    .into_owned(),
                size: fs_metadata.len(),
                modified: fs_metadata
                    .modified()?
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
            };

            if metadata.size <= SMALL_FILE_THRESHOLD {
                small_files_paths.insert(file_id_counter, entry.path().to_path_buf());
                small_files.push(metadata);
            } else {
                sessions.insert(
                    file_id_counter,
                    Arc::new(SendSession::new(metadata.clone(), entry.path())?),
                );

                large_files.push(metadata);
            }

            file_id_counter += 1;
        }
    }

    let job_name = generate_job_name(paths);

    Ok((
        Manifest {
            job_name,
            top_level_targets,
            large_files,
            small_files,
        },
        sessions,
        small_files_paths,
    ))
}
