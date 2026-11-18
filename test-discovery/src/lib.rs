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

/// Scan `config.tests_dir` for `.rs` files and generate a manifest that declares each one as a
/// `mod` below the [`WATERMARK`] line of `main.rs`, which is appended when missing.
///
/// A deprecation warning for each change to `main.rs` is written to `$OUT_DIR/tests.rs` and can be
/// pulled in from `main.rs` via:
///
/// ```ignore
/// include!(concat!(env!("OUT_DIR"), "/tests.rs"));
/// ```
///
/// Also emits `cargo:rerun-if-changed` directives for the tests directory and each discovered test
/// file.
///
/// # Migration Notes
///
/// When test files are consolidated from separate binaries into modules of a single binary,
/// `crate::` references in those files change meaning. Each test file was previously a crate root;
/// now it is a module inside the entry-point crate. Any `crate::` references must be updated to
/// `self::` or `super::` as appropriate, and `$crate::` in macro definitions may require
/// `#[macro_use]` at the entry-point level.
///
/// # Panics
///
/// Panics if the tests directory cannot be read or the output file cannot be written.  Emits a
/// `cargo::error` diagnostic and exits if `Cargo.toml` is misconfigured (missing `autotests =
/// false` or `[[test]]` entries).
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

    let root = tests_dir.join("main.rs");
    let contents = fs::read_to_string(&root)
        .unwrap_or_else(|e| panic!("failed to read `{}`: {e}", root.display()));
    let (generated, changes) = generated_root(&contents, &entries);

    let mut output = String::new();
    if generated != contents {
        fs::write(&root, &generated)
            .unwrap_or_else(|e| panic!("failed to write `{}`: {e}", root.display()));
        let path = display_path(&root);
        let path = path.display();
        let message = format!("{path} was regenerated.  Commit the updated file.");
        let changes = changes.join("\n  ");
        let note = format!(
            "\n\n{path} was regenerated with the following changes:\n\n  {changes}\n\n\
             Commit the updated file.\n"
        );
        // If you are reading this comment because you grepped for TEST_MANIFEST_REGENERATED, you
        // may need to build locally and commit generated changes made by the test-discovery crate.
        output = format!(
            "#[deprecated(note = {note:?})]\n\
             const TEST_MANIFEST_REGENERATED: () = ();\n\
             const _: () = TEST_MANIFEST_REGENERATED; // {message}\n"
        );
    }

    let out_file = out_dir.join("tests.rs");
    if fs::read_to_string(&out_file).ok() != Some(output.clone()) {
        fs::write(&out_file, output)
            .unwrap_or_else(|e| panic!("failed to write `{}`: {e}", out_file.display()));
    }
}

/// The line of `main.rs` after which every line is generated.
pub const WATERMARK: &str = "// Generated by the test-discovery crate.";

/// `contents` of `main.rs` with the lines after [`WATERMARK`] replaced by a `mod stem;` line for
/// each test file in `entries` that the lines above it do not declare.  Without a [`WATERMARK`]
/// line, every line is above it, and it is appended after a blank line.
///
/// Also return the lines added and removed, prefixed with `+ ` and `- `, in order of changes.
fn generated_root(contents: &str, entries: &[(String, PathBuf)]) -> (String, Vec<String>) {
    let mut changes = Vec::new();
    let mut hand_written: Vec<&str> = contents.lines().collect();
    let replaced = match hand_written.iter().position(|line| *line == WATERMARK) {
        Some(watermark) => hand_written.split_off(watermark + 1),
        None => {
            if hand_written
                .last()
                .is_some_and(|line| !line.trim().is_empty())
            {
                hand_written.push("");
            }
            hand_written.push(WATERMARK);
            changes.push(format!("+ {WATERMARK}"));
            Vec::new()
        }
    };
    let declared: Vec<&str> = hand_written
        .iter()
        .filter_map(|line| line.trim().strip_prefix("mod ")?.strip_suffix(';'))
        .collect();
    let written: Vec<String> = entries
        .iter()
        .filter(|(stem, _)| stem != "main" && !declared.contains(&stem.as_str()))
        .map(|(stem, _)| format!("mod {stem};"))
        .collect();

    // Walk the replaced and written lines side by side, reporting the diff.
    let (mut i, mut j) = (0, 0);
    while i < replaced.len() || j < written.len() {
        if let Some(line) = replaced
            .get(i)
            .filter(|line| !written.iter().any(|w| w == *line))
        {
            if !line.trim().is_empty() {
                changes.push(format!("- {line}"));
            }
            i += 1;
        } else if let Some(line) = written
            .get(j)
            .filter(|line| !replaced.contains(&line.as_str()))
        {
            changes.push(format!("+ {line}"));
            j += 1;
        } else {
            (i, j) = (i + 1, j + 1);
        }
    }

    let mut generated = hand_written.join("\n");
    generated.push('\n');
    for line in &written {
        generated.push_str(line);
        generated.push('\n');
    }
    (generated, changes)
}

/// `path` relative to the directory in which cargo was invoked, which the shell exports as `PWD`,
/// or `path` itself when that directory is unknown or shares only the filesystem root with `path`.
fn display_path(path: &Path) -> PathBuf {
    let relative = env::var_os("PWD").and_then(|cwd| {
        let cwd = fs::canonicalize(cwd).ok()?;
        relative_path(&cwd, &fs::canonicalize(path).ok()?)
    });
    relative.unwrap_or_else(|| path.to_path_buf())
}

/// `path` relative to `base`, where both are absolute, or `None` when they share only the
/// filesystem root.
fn relative_path(base: &Path, path: &Path) -> Option<PathBuf> {
    let common = base
        .ancestors()
        .find(|ancestor| path.starts_with(ancestor))?;
    common.parent()?;
    let mut relative: PathBuf = base
        .strip_prefix(common)
        .ok()?
        .components()
        .map(|_| "..")
        .collect();
    relative.push(path.strip_prefix(common).ok()?);
    Some(relative)
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

    #[test]
    fn lines_after_watermark_declare_undeclared_files() {
        let entries: Vec<(String, PathBuf)> = ["a", "common", "main", "z"]
            .iter()
            .map(|stem| (stem.to_string(), PathBuf::from(format!("{stem}.rs"))))
            .collect();
        let root = format!("mod common;\n{WATERMARK}\nmod stale;\n");
        assert_eq!(
            generated_root(&root, &entries),
            (
                format!("mod common;\n{WATERMARK}\nmod a;\nmod z;\n"),
                vec!["- mod stale;".into(), "+ mod a;".into(), "+ mod z;".into()]
            )
        );
        assert_eq!(
            generated_root("mod common;\n", &entries).0,
            format!("mod common;\n\n{WATERMARK}\nmod a;\nmod z;\n")
        );
    }

    #[test]
    fn relative_path_from_ancestor_sibling_and_unrelated_directory() {
        let path = Path::new("/repo/public/crate/tests/main.rs");
        let relative = |base| relative_path(Path::new(base), path);
        assert_eq!(relative("/repo/public"), Some("crate/tests/main.rs".into()));
        assert_eq!(
            relative("/repo/public/other"),
            Some("../crate/tests/main.rs".into())
        );
        assert_eq!(relative("/elsewhere"), None);
    }
}
