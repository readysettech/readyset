use anyhow::{anyhow, Result};
use std::path::{Path, PathBuf};
use std::process::{Child, Command};

/// Wrapper for a single process.
pub struct ProcessHandle {
    /// Child process handle running the server or mysql instance.
    process: Child,
}

impl ProcessHandle {
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
            args: vec!["--noria-metrics".into()],
        }
    }

    pub fn start(&self) -> anyhow::Result<ProcessHandle> {
        Ok(ProcessHandle {
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

    pub fn set_volume_id(&mut self, id: &str) {
        self.args.push("--volume-id".to_string());
        self.args.push(id.to_string());
    }

    pub fn set_authority_addr(&mut self, authority_addr: &str) {
        self.args.push("--authority-address".to_string());
        self.args.push(authority_addr.to_string());
    }

    pub fn set_authority(&mut self, authority: &str) {
        self.args.push("--authority".to_string());
        self.args.push(authority.to_string());
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

    pub fn set_mysql(&mut self, addr: &str) {
        self.args.push("--replication-url".to_string());
        self.args.push(addr.to_string());
    }
}

/// Manages running a noria-mysql binary with the correct arguments.
pub struct NoriaMySQLRunner {
    /// Path to the noria-mysql binary.
    binary: PathBuf,

    /// The arguments to pass to the noria-mysql process on startup.
    args: Vec<String>,
}

impl NoriaMySQLRunner {
    pub fn new(binary: &Path) -> Self {
        Self {
            binary: binary.to_owned(),
            args: vec!["--allow-unauthenticated-connections".to_string()],
        }
    }

    pub fn start(&self) -> anyhow::Result<ProcessHandle> {
        Ok(ProcessHandle {
            process: Command::new(&self.binary)
                .args(&self.args)
                .spawn()
                .map_err(|e| anyhow!(e.to_string()))?,
        })
    }

    pub fn set_authority_addr(&mut self, authority_addr: &str) {
        self.args.push("--authority-address".to_string());
        self.args.push(authority_addr.to_string());
    }

    pub fn set_authority(&mut self, authority: &str) {
        self.args.push("--authority".to_string());
        self.args.push(authority.to_string());
    }

    pub fn set_deployment(&mut self, deployment: &str) {
        self.args.push("--deployment".to_string());
        self.args.push(deployment.to_string());
    }

    pub fn set_port(&mut self, port: u16) {
        self.args.push("-a".to_string());
        self.args.push(format!("127.0.0.1:{}", port.to_string()));
    }

    pub fn set_mysql(&mut self, addr: &str) {
        self.args.push("--upstream-db-url".to_string());
        self.args.push(addr.to_string());
    }

    pub fn set_live_qca(&mut self) {
        self.args.push("--live-qca".to_string());
    }
}
