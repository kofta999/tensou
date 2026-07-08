use crate::disk::{SendSession, TransferStaging};
use crate::protocol::Metadata;
use crate::protocol::{JobInstruction, Manifest};
use crate::util::is_safe_relative_path;
use crate::{CHUNK_SIZE, FileId};
use anyhow::bail;
use std::path::PathBuf;
use std::{collections::HashMap, path::Path, sync::Arc};
use walkdir::WalkDir;

pub fn parse(
    manifest: Manifest,
    downloads_dir: &Path,
    overwrite: bool,
) -> anyhow::Result<(Vec<JobInstruction>, Arc<TransferStaging>)> {
    use crate::STAGING_DIR_NAME;
    let mut instructions = Vec::new();
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

        let unique_path = if overwrite || has_staging {
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

    // Determine top_level_prefix
    let top_level_prefix = if manifest.top_level_targets.len() == 1 {
        let target = &manifest.top_level_targets[0];
        let unique_target = unique_mappings.get(target).unwrap();
        let prefix = format!("{}/", target);
        if manifest
            .files
            .iter()
            .any(|f| f.relative_path.starts_with(&prefix))
        {
            Some(unique_target.as_str())
        } else {
            None
        }
    } else {
        None
    };

    let staging = Arc::new(TransferStaging::new(
        downloads_dir.to_path_buf(),
        top_level_prefix,
    ));

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

    Ok((instructions, staging))
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
