pub mod manifest;
mod types;
pub use types::*;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::find_unique_path;
    use bitvec::{bitvec, order::Lsb0};
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
        std::fs::write(&state_file, []).unwrap();
        let final_file = dir.path().join("empty.final");
        ins.load_state_from_disk(&state_file, &final_file, false)
            .unwrap();
        assert!(ins.is_resumed);
        assert_eq!(ins.state.0.len(), 4);
        assert!(ins.state.0.not_any());

        // Test 2: Truncated state file (contains only 1 byte, but we expect 4 chunks)
        let mut ins = JobInstruction::new(m);
        let state_file = dir.path().join("truncated.state");
        std::fs::write(&state_file, [0b0000_0001]).unwrap(); // Lsb0 order: bit 0 is true, others false
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

        let (manifest, sessions) = manifest::build(&file).unwrap();

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

        let (manifest, sessions) = manifest::build(dir.path()).unwrap();

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

        let result = manifest::parse(manifest, staging_dir.path(), false);
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

        let (instructions, _staging) =
            manifest::parse(manifest, downloads_dir.path(), false).unwrap();
        assert_eq!(instructions.len(), 2);

        // Check that paths are correctly updated to unique names
        assert_eq!(instructions[0].metadata.relative_path, "a (1).txt");
        assert_eq!(
            instructions[1].metadata.relative_path,
            "MyPhotos (1)/pic1.jpg"
        );
    }
}
