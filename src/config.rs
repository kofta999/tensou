use std::path::PathBuf;

#[derive(Clone)]
pub struct Config {
    /// Should receiver overwrite target file / dir if it exists
    pub overwrite_dest: bool,
    /// Base target directory for receiving files
    pub target_dir: PathBuf,
}
