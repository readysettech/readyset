use std::path::PathBuf;
use std::{env, fs};

/// Configuration for test discovery.
#[derive(Debug)]
pub struct TestDiscoveryConfig {
    /// Directory containing the test `.rs` files (relative to the crate root).
    pub tests_dir: PathBuf,
    /// File stems to skip (e.g. helper modules that aren't standalone test files).
    pub skip_stems: Vec<String>,
}

impl Default for TestDiscoveryConfig {
    fn default() -> Self {
        Self {
            tests_dir: PathBuf::from("tests"),
            skip_stems: Vec::new(),
        }
    }
}

/// Scan `config.tests_dir` for `.rs` files and generate a manifest that
/// includes each one as a `mod` with an absolute `#[path]` attribute.
///
/// The generated file is written to `$OUT_DIR/tests.rs` and can be pulled in
/// from a crate-level `tests.rs` via:
///
/// ```ignore
/// include!(concat!(env!("OUT_DIR"), "/tests.rs"));
/// ```
///
/// Also emits `cargo:rerun-if-changed` directives for the tests directory
/// and each discovered test file.
///
/// # Migration Notes
///
/// When test files are consolidated from separate binaries into modules of
/// a single binary, `crate::` references in those files change meaning.
/// Each test file was previously a crate root; now it is a module inside
/// the entry-point crate. Any `crate::` references must be updated to
/// `self::` or `super::` as appropriate, and `$crate::` in macro
/// definitions may require `#[macro_use]` at the entry-point level.
///
/// # Panics
///
/// Panics if the tests directory cannot be read or the output file cannot be
/// written.  This is intentional – build scripts should fail loudly.
pub fn generate_test_manifest(config: TestDiscoveryConfig) {
    let manifest_dir =
        PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set"));
    let tests_dir = manifest_dir.join(&config.tests_dir);
    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR not set"));

    // Emit rerun-if-changed for the tests directory itself so new/removed files
    // trigger a rebuild.
    println!("cargo:rerun-if-changed={}", tests_dir.display());

    let mut entries: Vec<(String, PathBuf)> = Vec::new();

    let read_dir = fs::read_dir(&tests_dir).unwrap_or_else(|e| {
        panic!(
            "failed to read tests directory `{}`: {e}",
            tests_dir.display()
        )
    });

    for entry in read_dir {
        let entry = entry.unwrap_or_else(|e| panic!("failed to read directory entry: {e}"));
        let path = entry.path();

        // Skip directories (e.g. `tests/common/`).
        if path.is_dir() {
            continue;
        }

        // Only consider `.rs` files.
        if path.extension().and_then(|e| e.to_str()) != Some("rs") {
            continue;
        }

        let stem = path
            .file_stem()
            .and_then(|s| s.to_str())
            .expect("file stem should be valid UTF-8")
            .to_string();

        if config.skip_stems.contains(&stem) {
            continue;
        }

        // Emit rerun-if-changed for each individual test file.
        println!("cargo:rerun-if-changed={}", path.display());

        entries.push((stem, path));
    }

    // Sort for deterministic output.
    entries.sort_unstable();

    let mut output = String::new();
    for (stem, path) in &entries {
        // Use forward slashes even on Windows for `#[path]` attributes.
        let path_str = path.display().to_string().replace('\\', "/");
        output.push_str(&format!("#[path = \"{path_str}\"]\nmod {stem};\n\n"));
    }

    let out_file = out_dir.join("tests.rs");
    fs::write(&out_file, output)
        .unwrap_or_else(|e| panic!("failed to write `{}`: {e}", out_file.display()));
}
