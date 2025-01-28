use anyhow::{anyhow, Result};
use jobserver::Client;
use std::env;
use std::path::Path;

fn main() -> Result<()> {
    if env::var("CLIPPY_ARGS").is_ok()
        || env::var("SKIP_CLUSTERTEST_BUILD").is_ok_and(|val| val.to_lowercase().as_str() == "true")
    {
        // running via cargo clippy or something else that isn't actually going to run clustertests,
        // so don't bother compiling test dependencies
        return Ok(());
    }
    env::set_current_dir(Path::new(".."))?;
    let client = unsafe { Client::from_env_ext(false).client }?;
    let mut cmd = std::process::Command::new("cargo");
    cmd.args([
        "--locked",
        "build",
        "--target-dir",
        "target/clustertest",
        "--bin",
        "readyset",
        "--bin",
        "readyset-server",
        "--features",
        "failure_injection",
    ]);
    client.configure(&mut cmd);
    let status = cmd.status()?;
    if !status.success() {
        return Err(anyhow!("cargo exited with nonzero status: {status}"));
    }
    Ok(())
}
