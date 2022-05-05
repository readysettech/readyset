use std::ffi::OsStr;
use std::io;

use anyhow::{bail, Result};
use tokio::process::Command;

pub async fn check_command_installed(description: &str, command: &mut Command) -> Result<()> {
    match command.output().await {
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            bail!("Please install {} before continuing.", description);
        }
        Err(e) => Err(e.into()),
        Ok(output) if !output.status.success() => {
            eprintln!(
                "Error checking for {}\nstdout:\n{}\n\nstderr:\n{}",
                description,
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            bail!("Error running docker");
        }
        Ok(_) => Ok(()),
    }
}

pub async fn run_docker_compose<I, S>(args: I) -> Result<()>
where
    I: IntoIterator<Item = S> + Clone,
    S: AsRef<OsStr>,
{
    match Command::new("docker-compose")
        .args(args.clone())
        .status()
        .await
    {
        Ok(status) if status.success() => Ok(()),
        Ok(status) => bail!("Command exited with {}", status),
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            let status = Command::new("docker")
                .arg("compose")
                .args(args)
                .status()
                .await?;
            if !status.success() {
                bail!("Command exited with {}", status);
            }
            Ok(())
        }
        Err(e) => Err(e.into()),
    }
}
