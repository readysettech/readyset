//! Wrappers for building a noria server binary from a provided
//! rust project root. This file provides the `CargoBuilder`
//! utility to build arbitrary rust binaries with a given path,
//! bin, and arguments. The primary noria_server build function
//! is `build_noria` which supports building both release
//! and debug builds.

use crate::NoriaBinaryPath;
use anyhow::anyhow;
use std::path::{Path, PathBuf};
use std::process::Command;

/// The name of the noria-server binary as built using
/// cargo build --bin <noria_server_bin>
const NORIA_SERVER_BIN: &str = "noria-server";
const NORIA_MYSQL_BIN: &str = "noria-mysql";

/// Spawns a process to build a rust binary using cargo.
pub struct CargoBuilder {
    /// The path that `cargo build` will be called from.
    path: PathBuf,
    /// The set of binaries to be built using
    /// `cargo build --bin <bin1> --bin <bin2>`.
    bins: Vec<String>,
    /// Arguments to be passed through to the `cargo build` command.
    args: Vec<String>,
}

impl CargoBuilder {
    /// Running `build` spawns a child proecss that builds the
    /// binary using cargo build --bin, called in the directory
    /// referenced by `self.path`.
    pub fn build(&self) -> anyhow::Result<()> {
        let mut build_cmd = Command::new("cargo");
        build_cmd.arg("build").args(&self.args);
        for bin in &self.bins {
            build_cmd.arg("--bin").arg(bin);
        }

        let build_output = build_cmd
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
pub fn build_noria(
    root_project_dir: &Path,
    target_dir: &Path,
    release: bool,
    rebuild: bool,
    mysql_adapter: bool,
) -> anyhow::Result<NoriaBinaryPath> {
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

    let binary_paths = NoriaBinaryPath {
        noria_server: result_path.join(NORIA_SERVER_BIN),
        noria_mysql: if mysql_adapter {
            Some(result_path.join(NORIA_MYSQL_BIN))
        } else {
            None
        },
    };

    // Early return if the binary already exists and we are not rebuilding.
    if !rebuild && binary_paths.exists() {
        return Ok(binary_paths);
    }

    let mut bins = vec![NORIA_SERVER_BIN.to_string()];
    if mysql_adapter {
        bins.push(NORIA_MYSQL_BIN.to_string());
    }

    let c = CargoBuilder {
        path: root_project_dir.into(),
        bins,
        args,
    };

    c.build()?;

    Ok(binary_paths)
}

#[cfg(test)]
mod test {
    use super::*;
    use std::{env, fs};

    // This test builds noria-server, and as a result, can take a long time to
    // run. #[ignore] is specified, limiting this test to only be run when requested.
    // This test only verifies that a binary exists where we expect it to after
    // calling `build_noria`.
    #[test]
    #[ignore]
    fn build_noria_test() {
        // The project root directory is CARGO_MANIFEST_DIR/../../. Pop
        // the project_root twice to get the root.
        let mut project_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        project_root.pop();
        project_root.pop();

        // Store test binaries in <root>/test_target.
        let target_dir = project_root.join("test_target");

        // Remove any existing noria-server binaries from target_dir. Forcing
        // `build_noria` to rebuild them.
        let bin_path = &target_dir.join("debug/".to_string() + NORIA_SERVER_BIN);
        if bin_path.exists() {
            assert!(!fs::remove_file(&bin_path).is_err());
            assert!(!bin_path.exists());
        }

        let result = build_noria(&project_root, &target_dir, false, true, true);
        assert!(
            !result.is_err(),
            "Error building noria: {}",
            result.err().unwrap()
        );

        assert!(bin_path.exists());
        assert_eq!(bin_path, &result.unwrap().noria_server);
    }
}
