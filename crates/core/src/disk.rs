mod reader;
pub mod staging;
mod writer;

pub use reader::*;
pub use staging::TransferStaging;
pub use writer::*;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::STAGING_DIR_NAME;
    use std::sync::Arc;
    use tempfile::tempdir;

    fn make_staging(dir: &std::path::Path, top_level_prefix: Option<&str>) -> Arc<TransferStaging> {
        let s = Arc::new(TransferStaging::new(dir.to_path_buf(), top_level_prefix));
        s.prepare().unwrap();
        s
    }

    /// Verifies staging paths calculation for a folder-based transfer.
    #[test]
    fn staging_folder_transfer_paths() {
        let dir = tempdir().unwrap();
        let staging = make_staging(dir.path(), Some("MyPhotos"));

        assert_eq!(
            staging.staging_dir,
            dir.path().join("MyPhotos").join(STAGING_DIR_NAME)
        );
        assert_eq!(
            staging.part_path("MyPhotos/nested/img.jpg"),
            dir.path()
                .join("MyPhotos")
                .join(STAGING_DIR_NAME)
                .join("nested/img.jpg.part")
        );
        assert_eq!(
            staging.state_path("MyPhotos/nested/img.jpg"),
            dir.path()
                .join("MyPhotos")
                .join(STAGING_DIR_NAME)
                .join("nested/img.jpg.state")
        );
        assert_eq!(
            staging.final_path("MyPhotos/nested/img.jpg"),
            dir.path().join("MyPhotos/nested/img.jpg")
        );
    }

    /// Verifies staging paths calculation for a single-file transfer.
    #[test]
    fn staging_single_file_transfer_paths() {
        let dir = tempdir().unwrap();
        let staging = make_staging(dir.path(), None);

        assert_eq!(staging.staging_dir, dir.path().join(STAGING_DIR_NAME));
        assert_eq!(
            staging.part_path("photo.jpg"),
            dir.path().join(STAGING_DIR_NAME).join("photo.jpg.part")
        );
        assert_eq!(
            staging.state_path("photo.jpg"),
            dir.path().join(STAGING_DIR_NAME).join("photo.jpg.state")
        );
        assert_eq!(
            staging.final_path("photo.jpg"),
            dir.path().join("photo.jpg")
        );
    }

    /// Verifies that prepare() correctly creates the hidden staging directory on disk.
    #[test]
    fn staging_prepare_creates_staging_dir() {
        let dir = tempdir().unwrap();
        let staging = Arc::new(TransferStaging::new(dir.path().to_path_buf(), None));
        assert!(!staging.staging_dir.exists());
        staging.prepare().unwrap();
        assert!(staging.staging_dir.exists());
    }

    /// Verifies that staging cleanup removes the hidden staging directory completely.
    #[test]
    fn staging_cleanup_removes_dir() {
        let dir = tempdir().unwrap();
        let staging = make_staging(dir.path(), None);
        assert!(staging.staging_dir.exists());
        staging.cleanup().unwrap();
        assert!(!staging.staging_dir.exists());
    }

    /// Verifies that create_file_staging_dir creates nested folder structures inside the staging folder.
    #[test]
    fn staging_create_file_staging_dir_nested() {
        let dir = tempdir().unwrap();
        let staging = make_staging(dir.path(), None);
        staging.create_file_staging_dir("a/b/c.txt").unwrap();
        assert!(staging.staging_dir.join("a/b").is_dir());
    }

    /// Verifies that create_file_destination_dir creates nested destination directory hierarchies.
    #[test]
    fn staging_create_file_destination_dir_nested() {
        let dir = tempdir().unwrap();
        let staging = make_staging(dir.path(), None);
        staging.create_file_destination_dir("a/b/c.txt").unwrap();
        assert!(staging.dest_dir.join("a/b").is_dir());
    }
}
