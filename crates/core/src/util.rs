use std::path::Component;
use std::path::Path;
use std::path::PathBuf;

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

pub fn is_safe_relative_path(path: &Path) -> bool {
    path.components().all(|c| matches!(c, Component::Normal(_)))
}
