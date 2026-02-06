use std::path::{Path, PathBuf};
use std::{env, fs, process};

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

/// Validate that the consuming crate's `Cargo.toml` is configured correctly for
/// test-discovery: `autotests` must be explicitly set to `false` and at least one
/// `[[test]]` entry must be present.
fn validate_cargo_toml(manifest_dir: &Path) -> Result<(), String> {
    let cargo_toml_path = manifest_dir.join("Cargo.toml");
    let contents = fs::read_to_string(&cargo_toml_path).unwrap_or_else(|e| {
        panic!(
            "test-discovery: failed to read `{}`: {e}",
            cargo_toml_path.display()
        )
    });

    let manifest: toml::Value = toml::from_str(&contents).unwrap_or_else(|e| {
        panic!(
            "test-discovery: failed to parse `{}`: {e}",
            cargo_toml_path.display()
        )
    });

    let path = cargo_toml_path.display();

    // Check that `package.autotests` is explicitly set to `false`.
    match manifest.get("package").and_then(|p| p.get("autotests")) {
        Some(v) if v.as_bool() == Some(false) => {} // correct
        Some(_) => {
            return Err(format!(
                "`package.autotests` is not set to `false` in {path}; \
                 set `autotests = false` under `[package]`"
            ));
        }
        None => {
            return Err(format!(
                "`package.autotests` is missing in {path}; \
                 add `autotests = false` under `[package]`"
            ));
        }
    }

    // Check that at least one `[[test]]` entry exists.
    match manifest.get("test") {
        Some(v) if v.as_array().is_some_and(|a| !a.is_empty()) => {} // correct
        _ => {
            return Err(format!(
                "no `[[test]]` entries found in {path}; \
                 add a `[[test]]` section with `name` and `path` entries"
            ));
        }
    }

    Ok(())
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
/// written.  Emits a `cargo::error` diagnostic and exits if `Cargo.toml` is
/// misconfigured (missing `autotests = false` or `[[test]]` entries).
pub fn generate_test_manifest(config: TestDiscoveryConfig) {
    let manifest_dir =
        PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set"));

    println!("cargo:rerun-if-changed=Cargo.toml");

    if let Err(msg) = validate_cargo_toml(&manifest_dir) {
        println!("cargo::error={msg}");
        process::exit(0);
    }

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

#[cfg(test)]
mod tests {
    use super::*;

    fn write_cargo_toml(dir: &Path, contents: &str) {
        fs::write(dir.join("Cargo.toml"), contents).expect("failed to write test Cargo.toml");
    }

    #[test]
    fn valid_manifest_passes() {
        let dir = tempfile::tempdir().expect("failed to create tempdir");
        write_cargo_toml(
            dir.path(),
            r#"
[package]
name = "fake"
autotests = false

[[test]]
name = "tests"
path = "tests.rs"
"#,
        );
        validate_cargo_toml(dir.path()).expect("validation should pass");
    }

    #[test]
    fn autotests_true_errors() {
        let dir = tempfile::tempdir().expect("failed to create tempdir");
        write_cargo_toml(
            dir.path(),
            r#"
[package]
name = "fake"
autotests = true

[[test]]
name = "tests"
path = "tests.rs"
"#,
        );
        let err = validate_cargo_toml(dir.path()).unwrap_err();
        assert!(err.contains("not set to `false`"), "{err}");
        assert!(err.contains("Cargo.toml"), "{err}");
    }

    #[test]
    fn autotests_missing_errors() {
        let dir = tempfile::tempdir().expect("failed to create tempdir");
        write_cargo_toml(
            dir.path(),
            r#"
[package]
name = "fake"

[[test]]
name = "tests"
path = "tests.rs"
"#,
        );
        let err = validate_cargo_toml(dir.path()).unwrap_err();
        assert!(err.contains("is missing"), "{err}");
        assert!(err.contains("Cargo.toml"), "{err}");
    }

    #[test]
    fn no_test_entries_errors() {
        let dir = tempfile::tempdir().expect("failed to create tempdir");
        write_cargo_toml(
            dir.path(),
            r#"
[package]
name = "fake"
autotests = false
"#,
        );
        let err = validate_cargo_toml(dir.path()).unwrap_err();
        assert!(err.contains("no `[[test]]` entries found"), "{err}");
        assert!(err.contains("Cargo.toml"), "{err}");
    }
}
