use anyhow::{bail, Result};
use tokio::process::Command;

pub(crate) async fn run_command(command: &mut Command) -> Result<()> {
    let status = command.status().await?;
    if !status.success() {
        bail!("Command exited with {}", status);
    }
    Ok(())
}
