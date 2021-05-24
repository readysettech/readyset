use anyhow::{anyhow, Result};
use std::path::{Path, PathBuf};
use std::process::{Child, Command};

/// Wrapper for a single server process.
pub struct ServerProcessHandle {
    /// Child process handle running the server.
    process: Child,
}

impl ServerProcessHandle {
    pub fn kill(&mut self) -> Result<()> {
        self.process.kill()?;
        Ok(())
    }
}

/// Manages running a noria-server binary with the correct arguments.
pub struct NoriaServerRunner {
    /// Path to the noria-server binary.
    binary: PathBuf,

    /// The arguments to pass to the noria-server process on startup.
    args: Vec<String>,
}

impl NoriaServerRunner {
    pub fn new(binary: &Path) -> Self {
        Self {
            binary: binary.to_owned(),
            args: vec!["--no-reuse".to_string()],
        }
    }

    pub fn start(&self) -> anyhow::Result<ServerProcessHandle> {
        Ok(ServerProcessHandle {
            process: Command::new(&self.binary)
                .args(&self.args)
                .spawn()
                .map_err(|e| anyhow!(e.to_string()))?,
        })
    }

    pub fn set_region(&mut self, region: &str) {
        self.args.push("--region".to_string());
        self.args.push(region.to_string());
    }

    pub fn set_primary_region(&mut self, primary_region: &str) {
        self.args.push("--primary-region".to_string());
        self.args.push(primary_region.to_string());
    }

    pub fn set_zookeeper(&mut self, zookeeper_addr: &str) {
        self.args.push("-z".to_string());
        self.args.push(zookeeper_addr.to_string());
    }

    pub fn set_deployment(&mut self, deployment: &str) {
        self.args.push("--deployment".to_string());
        self.args.push(deployment.to_string());
    }

    pub fn set_shards(&mut self, shards: usize) {
        self.args.push("--shards".to_string());
        self.args.push(shards.to_string());
    }

    pub fn set_external_port(&mut self, external_port: u16) {
        self.args.push("--external-port".to_string());
        self.args.push(external_port.to_string());
    }

    pub fn set_log_dir(&mut self, path: &Path) {
        self.args.push("--log-dir".to_string());
        self.args.push(path.to_str().unwrap().to_string());
    }
}
