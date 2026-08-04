use crate::{STAGING_DIR_NAME, protocol::JobInstruction};
use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    sync::Arc,
};

pub struct TransferStaging {
    /// Final user-visible destination directory (e.g. `Downloads/MyTransfer/`)
    pub dest_dir: PathBuf,
    /// Hidden staging directory on the same partition (e.g. `Downloads/MyTransfer/.tensou/`)
    pub staging_dir: PathBuf,
    /// The prefix of the receiving folder to strip, if any
    pub top_level_prefix: Option<String>,
}

impl TransferStaging {
    pub fn new(downloads_dir: PathBuf, top_level_prefix: Option<&str>) -> Self {
        let staging_dir = if let Some(prefix) = top_level_prefix {
            downloads_dir.join(prefix).join(STAGING_DIR_NAME)
        } else {
            downloads_dir.join(STAGING_DIR_NAME)
        };
        Self {
            dest_dir: downloads_dir,
            staging_dir,
            top_level_prefix: top_level_prefix.map(|s| s.to_string()),
        }
    }

    fn get_staging_relative_path(&self, relative_path: &str) -> PathBuf {
        let path = Path::new(relative_path);
        if self.top_level_prefix.is_some()
            && let Some(first_component) = path.components().next()
            && let Ok(stripped) = path.strip_prefix(first_component)
        {
            return stripped.to_path_buf();
        }
        path.to_path_buf()
    }

    /// Resolve where a partial download should go (e.g. `.tensou/subfolder/file.part`)
    pub fn part_path(&self, relative_path: &str) -> PathBuf {
        self.staging_dir
            .join(self.get_staging_relative_path(relative_path))
            .with_added_extension("part")
    }

    /// Resolve where a transfer state file should go (e.g. `.tensou/subfolder/file.state`)
    pub fn state_path(&self, relative_path: &str) -> PathBuf {
        self.staging_dir
            .join(self.get_staging_relative_path(relative_path))
            .with_added_extension("state")
    }

    /// Resolve the final destination path (e.g. `MyTransfer/subfolder/file`)
    pub fn final_path(&self, relative_path: &str) -> PathBuf {
        self.dest_dir.join(relative_path)
    }

    pub fn prepare(&self) -> std::io::Result<()> {
        std::fs::create_dir_all(&self.staging_dir)
    }

    /// Safely creates the parent directory hierarchy for a staging file
    /// (e.g., creating `.tensou/nested_folder/` so we can write the .part file)
    pub fn create_file_staging_dir(&self, relative_path: &str) -> std::io::Result<()> {
        let part_path = self.part_path(relative_path);
        if let Some(parent) = part_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        Ok(())
    }

    /// Safely creates the parent directory hierarchy in the final destination folder
    /// (e.g., creating `MyPhotos/nested_folder/` right before renaming the file out of staging)
    pub fn create_file_destination_dir(&self, relative_path: &str) -> std::io::Result<()> {
        let final_path = self.final_path(relative_path);
        if let Some(parent) = final_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        Ok(())
    }

    /// Clean up the staging directory recursively
    pub fn cleanup(&self) -> std::io::Result<()> {
        if self.staging_dir.exists() {
            std::fs::remove_dir_all(&self.staging_dir)?;

            // Try to delete the parent `.tensou` folder to prevent clutter.
            // This safely fails (and does nothing) if other concurrent transfers are still active in it!
            if let Some(parent) = self.staging_dir.parent() {
                let _ = std::fs::remove_dir(parent);
            }
        }
        Ok(())
    }

    /// Pre-create all unique destination and staging directories
    /// before any data flows. This collapses the per-file create_dir_all
    /// calls (each causing ~22 statx syscalls) into a single upfront pass.
    pub async fn prepare_destination(
        self: Arc<Self>,
        instructions: &[JobInstruction],
    ) -> anyhow::Result<()> {
        let unique_dirs: HashSet<std::path::PathBuf> = instructions
            .iter()
            .flat_map(|ins| {
                let is_small = ins.metadata.size <= ins.metadata.chunk_size;
                let mut dirs = Vec::with_capacity(2);
                if is_small {
                    if let Some(p) = self.final_path(&ins.metadata.relative_path).parent() {
                        dirs.push(p.to_path_buf());
                    }
                } else {
                    if let Some(p) = self.part_path(&ins.metadata.relative_path).parent() {
                        dirs.push(p.to_path_buf());
                    }
                    if let Some(p) = self.final_path(&ins.metadata.relative_path).parent() {
                        dirs.push(p.to_path_buf());
                    }
                }
                dirs
            })
            .collect();
        let staging_clone = self.clone();
        tokio::task::spawn_blocking(move || {
            staging_clone.prepare()?;
            for dir in unique_dirs {
                std::fs::create_dir_all(dir)?;
            }
            std::io::Result::Ok(())
        })
        .await??;

        Ok(())
    }
}
