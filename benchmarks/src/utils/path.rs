use std::path::{Path, PathBuf};

use anyhow::{bail, Result};
use walkdir::WalkDir;

/// Walks the directory referenced by `dir` recursively up to a depth of 5.
fn walk_dirs_for(p: &Path, dir: &Path) -> Option<PathBuf> {
    let walk = WalkDir::new(dir)
        .follow_links(true)
        .max_depth(5)
        .into_iter()
        .filter_map(|e| {
            if let Ok(d) = e {
                if d.file_type().is_dir() {
                    return Some(d);
                }
            }

            None
        });

    for entry in walk {
        let mut possible_path = entry.into_path();
        possible_path.push(&p);

        if possible_path.exists() {
            return Some(possible_path);
        }
    }

    None
}

/// A path that is either:
///   1. A valid absolute or relative path from the current working directory.
///   2. A valid path postfix in the benchmark directory.
///
/// This will allow us to reference files independent of their absolute paths or the
/// directory the benchmark script is run from.
pub fn benchmark_path(p: &Path) -> Result<PathBuf> {
    if p.is_absolute() || p.exists() {
        return Ok(p.to_owned());
    }

    // If we cannot find the path, check if we can find it in the benchmarks directory.
    // Iterate over all benchmark directories recursively and check if appending `p` to the
    // path yields a file.
    if let Ok(dir) = std::env::var("CARGO_MANIFEST_DIR") {
        if let Some(path) = walk_dirs_for(p, dir.as_ref()) {
            return Ok(path);
        }
    }

    // Search the current directory as well.
    if let Ok(dir) = std::env::current_dir() {
        if let Some(path) = walk_dirs_for(p, &dir) {
            return Ok(path);
        }
    }

    bail!("Could not find the file: {:?}", p);
}
