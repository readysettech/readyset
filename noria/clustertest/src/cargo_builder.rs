//! Wrappers for building a noria server binary from a provided
//! rust project root. This file provides the `CargoBuilder`
//! utility to build arbitrary rust binaries with a given path,
//! bin, and arguments. The primary noria_server build function
//! is `build_noria_server` which supports building both release
//! and debug builds.

use anyhow::anyhow;
use std::path::{Path, PathBuf};
use std::process::Command;

/// The name of the noria-server binary as built using
/// cargo build --bin <noria_server_bin>
const NORIA_SERVER_BIN: &str = "noria-server";

/// Spawns a process to build a rust binary using cargo.
pub struct CargoBuilder {
    /// The path that `cargo build` will be called from.
    path: PathBuf,
    /// The binary to be built using `cargo build --bin <bin>`.
    bin: String,
    /// Arguments to be passed through to the `cargo build` command.
    args: Vec<String>,
}

impl CargoBuilder {
    /// Running `build` spawns a child proecss that builds the
    /// binary using cargo build --bin, called in the directory
    /// referenced by `self.path`.
    pub fn build(&self) -> anyhow::Result<()> {
        let build_output = Command::new("cargo")
            .arg("build")
            .arg("--bin")
            .arg(&self.bin)
            .args(&self.args)
            .current_dir(&self.path)
            .output()
            .expect("Failed to build");

        if !build_output.status.success() {
            Err(anyhow!(
                "Failure while building, stderr: {}",
                std::str::from_utf8(&build_output.stderr).unwrap()
            ))
        } else {
            Ok(())
        }
    }
}

/// This uses the CargoBuilder to build noria-server from the rust
/// project's root: `root_project_dir`.
///
/// `target_dir`: The target build directory.
/// `release`: If we are building a release binary, passing --release as
///            an arg to cargo builder.
/// `rebuild`: If we should rebuild the binary if it already exists.
///
/// A successful result contains the path to the built binary.
pub fn build_noria_server(
    root_project_dir: &Path,
    target_dir: &Path,
    release: bool,
    rebuild: bool,
) -> anyhow::Result<PathBuf> {
    let mut args = Vec::new();
    if release {
        args.push("--release".to_string());
    }

    // Construct the start of the output path, if the user passed in
    // a valid `target_dir`.
    args.push("--target-dir".to_string());
    args.push(target_dir.to_str().unwrap().to_string());
    let mut result_path = target_dir.to_owned();

    if release {
        result_path.push("release")
    } else {
        result_path.push("debug")
    }
    result_path.push(NORIA_SERVER_BIN);

    // Early return if the binary already exists and we are not rebuilding.
    if !rebuild && result_path.exists() {
        return Ok(result_path);
    }

    let c = CargoBuilder {
        path: root_project_dir.into(),
        bin: NORIA_SERVER_BIN.to_string(),
        args,
    };

    c.build()?;

    Ok(result_path)
}

#[cfg(test)]
mod test {
    use super::*;
    use std::{env, fs};

    // This test builds noria-server, and as a result, can take a long time to
    // run. #[ignore] is specified, limiting this test to only be run when requested.
    // This test only verifies that a binary exists where we expect it to after
    // calling `build_noria_server`.
    #[test]
    #[ignore]
    fn build_noria_server_test() {
        // The project root directory is CARGO_MANIFEST_DIR/../../. Pop
        // the project_root twice to get the root.
        let mut project_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        project_root.pop();
        project_root.pop();

        // Store test binaries in <root>/test_target.
        let target_dir = project_root.join("test_target");

        // Remove any existing noria-server binaries from target_dir. Forcing
        // `build_noria_server` to rebuild them.
        let bin_path = &target_dir.join("debug/".to_string() + NORIA_SERVER_BIN);
        if bin_path.exists() {
            assert!(!fs::remove_file(&bin_path).is_err());
            assert!(!bin_path.exists());
        }

        let result = build_noria_server(&project_root, &target_dir, false, true);
        assert!(
            !result.is_err(),
            "Error building noria: {}",
            result.err().unwrap()
        );

        assert!(bin_path.exists());
        assert_eq!(bin_path, &result.unwrap());
    }
}
