//! Generate a new named snapshot of `ControllerState` for the
//! state-backwards-compatibility test.
//!
//! Snapshots are immutable artifacts of deployed Authority state. This tool
//! refuses to overwrite an existing file; if you genuinely need to
//! regenerate one, hand-delete it on disk first and justify the change in
//! the commit message.
//!
//! Usage:
//! ```text
//! cargo run --bin make_state_snapshot -- --snapshot-name v2
//! ```
//!
//! The snapshot name must already be registered in
//! `readyset_server::snapshot_compat::SNAPSHOT_NAMES` and have matching
//! `build_<name>_state` and `verify_<name>` implementations. Adding a
//! name there is the deliberate step that authorises this tool to emit a
//! new fixture.

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use anyhow::{Context, Result, bail};
use clap::Parser;

#[derive(Parser)]
struct Args {
    /// Name of the snapshot to generate. Must match an entry in
    /// `readyset_server::snapshot_compat::SNAPSHOT_NAMES`.
    #[arg(long)]
    snapshot_name: String,
}

fn main() -> ExitCode {
    match run() {
        Ok(path) => {
            println!("wrote {}", path.display());
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: {e:#}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<PathBuf> {
    let args = Args::parse();

    if !readyset_server::snapshot_compat::SNAPSHOT_NAMES.contains(&args.snapshot_name.as_str()) {
        bail!(
            "snapshot name `{}` is not registered in snapshot_compat::SNAPSHOT_NAMES. \
             Add it there first, along with build/verify branches, then re-run.",
            args.snapshot_name,
        );
    }

    let out_path = snapshots_dir().join(format!("{}.bin", args.snapshot_name));
    if out_path.exists() {
        bail!(
            "{} already exists. Snapshots are immutable; never regenerate an existing \
             snapshot (generate a new snapshot instead). If you truly need to overwrite, \
             hand-delete the file and document the reason.",
            out_path.display(),
        );
    }

    let bytes = readyset_server::snapshot_compat::build_snapshot_bytes(&args.snapshot_name)
        .with_context(|| format!("building snapshot bytes for `{}`", args.snapshot_name))?;

    // Round-trip the bytes through verify to catch any build/verify
    // disagreement before we commit garbage to disk.
    readyset_server::snapshot_compat::verify_snapshot(&args.snapshot_name, &bytes).with_context(
        || {
            format!(
                "self-verify failed for `{}`; the build/verify pair is inconsistent",
                args.snapshot_name,
            )
        },
    )?;

    std::fs::write(&out_path, &bytes).with_context(|| format!("writing {}", out_path.display()))?;

    Ok(out_path)
}

fn snapshots_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("readyset-tools is a workspace member; parent must exist")
        .join("readyset-server/tests/state_snapshots")
}
