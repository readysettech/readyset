//! Remove from the target directory things that shouldn't be cached.
// Based on the GitHub Action https://github.com/Swatinem/rust-cache
use std::collections::HashSet;
use std::convert::TryFrom;
use std::fs;
use std::path::Path;
use std::time::{Duration, SystemTime};

use anyhow::{bail, Result};
use camino::{Utf8Path, Utf8PathBuf};

const ONE_WEEK: Duration = Duration::from_secs(7 * 24 * 3600);

fn rm_rf<P: AsRef<Utf8Path>>(path: P) -> Result<()> {
    let path = path.as_ref();
    if path.is_file() {
        fs::remove_file(path)?;
    } else if path.is_dir() {
        fs::remove_dir_all(path)?;
    } else {
        bail!("Don't know how to remove {}", path)
    }
    Ok(())
}

fn clean_dir_keeping_prefixes<P: AsRef<Path>>(path: P, prefixes: &HashSet<String>) -> Result<()> {
    let now = SystemTime::now();
    for entry in path.as_ref().read_dir()? {
        let entry = entry?;
        let path = Utf8PathBuf::try_from(entry.path())?;
        let modified = path.metadata()?.modified()?;
        let delta = now.duration_since(modified)?;
        if delta > ONE_WEEK {
            rm_rf(path)?;
            continue;
        }
        if let Some((package_name, _)) = path.file_name().unwrap().rsplit_once('-') {
            if !prefixes.contains(package_name) {
                rm_rf(path)?;
                continue;
            }
        }
    }
    Ok(())
}

pub(crate) fn run() -> Result<()> {
    let cmd = cargo_metadata::MetadataCommand::new();
    let metadata = cmd.exec()?;
    let workspace_root = metadata.workspace_root;
    let target_directory = metadata.target_directory;
    let packages = metadata.packages;

    let target_debug_directory = target_directory.join("debug");

    std::fs::remove_file(target_directory.join(".rustc_info.json"))?;
    std::fs::remove_dir_all(target_debug_directory.join("examples"))?;
    std::fs::remove_dir_all(target_debug_directory.join("incremental"))?;

    for entry in target_debug_directory.read_dir()? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() {
            std::fs::remove_file(path)?;
        }
    }

    let keep_packages: HashSet<_> = packages
        .iter()
        .filter(|p| !p.manifest_path.starts_with(&workspace_root))
        .map(|p| p.name.clone())
        .collect();

    clean_dir_keeping_prefixes(target_debug_directory.join("build"), &keep_packages)?;
    clean_dir_keeping_prefixes(target_debug_directory.join(".fingerprint"), &keep_packages)?;

    let keep_deps: HashSet<_> = packages
        .iter()
        .flat_map(|p| {
            let mut names: HashSet<String> = HashSet::new();
            let name_for_package = p.name.replace('-', "_");
            names.insert(format!("lib{}", name_for_package));
            for target in p.targets.iter().filter(|t| t.kind[0] == "lib") {
                let name_for_target = target.name.replace('-', "_");
                names.insert(format!("lib{}", name_for_target));
            }
            names
        })
        .collect();

    clean_dir_keeping_prefixes(target_debug_directory.join("deps"), &keep_deps)?;
    Ok(())
}
