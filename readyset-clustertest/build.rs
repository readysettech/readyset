use anyhow::{anyhow, Result};
use std::env;
use std::path::Path;

fn main() -> Result<()> {
    if env::var("CLIPPY_ARGS").is_ok() {
        // running via cargo clippy; don't bother compiling test dependencies
        return Ok(());
    }
    env::set_current_dir(Path::new(".."))?;
    let status = std::process::Command::new("cargo")
        .args([
            "--locked",
            "build",
            "--target-dir",
            "target/clustertest",
            "--release",
            "--bin",
            "readyset",
            "--bin",
            "readyset-server",
            "--features",
            "failure_injection",
        ])
        .status()?;
    if !status.success() {
        return Err(anyhow!("cargo exited with nonzero status: {status}"));
    }
    Ok(())
}
