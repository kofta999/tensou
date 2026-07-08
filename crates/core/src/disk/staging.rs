use crate::STAGING_DIR_NAME;
use std::path::{Path, PathBuf};

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
        if self.top_level_prefix.is_some() {
            if let Some(first_component) = path.components().next() {
                if let Ok(stripped) = path.strip_prefix(first_component) {
                    return stripped.to_path_buf();
                }
            }
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
}
