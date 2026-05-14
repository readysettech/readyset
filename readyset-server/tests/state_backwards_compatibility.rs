//! Verifies the current build can still decode every `ControllerState`
//! snapshot under `tests/state_snapshots/`.
//!
//! Each `.bin` is the `rmp_serde::to_vec` output of a `ControllerState`
//! whose shape matches a deployed Authority's persisted state at a specific
//! historical point. The test runs each snapshot through
//! `readyset_server::snapshot_compat::verify_snapshot` — which calls
//! `rmp_serde::from_slice` and asserts the sentinel fields that define the
//! snapshot — failing loudly with policy guidance if anything mismatches.

use std::ffi::OsStr;

use include_dir::{include_dir, Dir};
use readyset_server::snapshot_compat;

const SNAPSHOTS: Dir = include_dir!("$CARGO_MANIFEST_DIR/tests/state_snapshots");

const FAILURE_GUIDANCE: &str = "
SNAPSHOT MISMATCH. This snapshot represents deployed Authority state that
this build must still decode. You have two valid responses:

  1. Add a #[serde(default)] #[deprecated] compat shim that restores the
     wire shape. See Config.sharding for the recipe.
  2. If certain no Authority holds this state, DELETE the snapshot file
     and justify in the commit message.

DO NOT REGENERATE the snapshot to make this test pass. Snapshots are
immutable artifacts of deployed state. See
public/readyset-server/tests/state_snapshots/README.md.
";

#[test]
fn snapshots_decode() {
    let mut bin_files: Vec<_> = SNAPSHOTS
        .files()
        .filter(|f| f.path().extension() == Some(OsStr::new("bin")))
        .collect();
    bin_files.sort_by_key(|f| f.path());

    assert!(
        !bin_files.is_empty(),
        "no `.bin` files in tests/state_snapshots/; at minimum, v1.bin \
         must exist to anchor backwards-compat coverage",
    );

    // Every name registered in SNAPSHOT_NAMES must have a corresponding
    // `.bin` on disk. Catches the inverse-drift case where someone adds
    // a snapshot name without committing the generated fixture.
    for name in snapshot_compat::SNAPSHOT_NAMES {
        assert!(
            bin_files
                .iter()
                .any(|f| f.path().file_stem().and_then(OsStr::to_str) == Some(*name)),
            "SNAPSHOT_NAMES contains `{name}` but `{name}.bin` is missing from \
             tests/state_snapshots/ — run `cargo run --bin make_state_snapshot \
             -- --snapshot-name {name}` and commit the result, or remove the \
             name from SNAPSHOT_NAMES.",
        );
    }

    let mut failures = Vec::new();
    for file in &bin_files {
        let name = file
            .path()
            .file_stem()
            .expect("snapshot file has a stem")
            .to_str()
            .expect("snapshot file name is UTF-8");
        if let Err(e) = snapshot_compat::verify_snapshot(name, file.contents()) {
            failures.push(format!("  {name}.bin: {e:#}"));
        }
    }

    if !failures.is_empty() {
        panic!(
            "{} snapshot(s) failed to decode/verify:\n{}\n{}",
            failures.len(),
            failures.join("\n"),
            FAILURE_GUIDANCE,
        );
    }
}

/// Sanity: this build's `build_snapshot_bytes` and `verify_snapshot` for
/// every registered snapshot name are mutually consistent. Catches the case
/// where someone edits one without updating the other, before bad bytes get
/// committed to a fixture file.
#[test]
fn build_and_verify_round_trip() {
    for name in snapshot_compat::SNAPSHOT_NAMES {
        let bytes = snapshot_compat::build_snapshot_bytes(name)
            .unwrap_or_else(|e| panic!("build_snapshot_bytes({name}) failed: {e:#}"));
        snapshot_compat::verify_snapshot(name, &bytes).unwrap_or_else(|e| {
            panic!("build/verify disagree for `{name}`: {e:#}");
        });
    }
}
