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

    pub fn is_alive(&mut self) -> bool {
        self.process.try_wait().unwrap().is_none()
    }
}

/// Manages running a noria-server binary with the correct arguments.
pub struct NoriaServerBuilder {
    /// Path to the noria-server binary.
    binary: PathBuf,

    /// The arguments to pass to the noria-server process on startup.
    args: Vec<String>,
}

impl NoriaServerBuilder {
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

    fn push_arg(mut self, arg_name: &str, arg_value: &str) -> Self {
        self.args.push(arg_name.to_string());
        self.args.push(arg_value.to_string());
        self
    }

    pub fn region(self, region: &str) -> Self {
        self.push_arg("--region", region)
    }

    pub fn primary_region(self, primary_region: &str) -> Self {
        self.push_arg("--primary-region", primary_region)
    }

    pub fn volume_id(self, id: &str) -> Self {
        self.push_arg("--volume-id", id)
    }

    pub fn authority_addr(self, authority_addr: &str) -> Self {
        self.push_arg("--authority-address", authority_addr)
    }

    pub fn authority(self, authority: &str) -> Self {
        self.push_arg("--authority", authority)
    }

    pub fn deployment(self, deployment: &str) -> Self {
        self.push_arg("--deployment", deployment)
    }

    pub fn shards(self, shards: usize) -> Self {
        self.push_arg("--shards", &shards.to_string())
    }

    pub fn quorum(self, quorum: usize) -> Self {
        self.push_arg("--quorum", &quorum.to_string())
    }

    pub fn external_port(self, external_port: u16) -> Self {
        self.push_arg("--external-port", &external_port.to_string())
    }

    pub fn mysql(self, addr: &str) -> Self {
        self.push_arg("--replication-url", addr)
    }
}

/// Manages running a noria-mysql binary with the correct arguments.
pub struct AdapterBuilder {
    /// Path to the noria-mysql binary.
    binary: PathBuf,

    /// The arguments to pass to the noria-mysql process on startup.
    args: Vec<String>,
}

impl AdapterBuilder {
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

    fn push_arg(mut self, arg_name: &str, arg_value: &str) -> Self {
        self.args.push(arg_name.to_string());
        self.args.push(arg_value.to_string());
        self
    }

    pub fn authority_addr(self, authority_addr: &str) -> Self {
        self.push_arg("--authority-address", authority_addr)
    }

    pub fn authority(self, authority: &str) -> Self {
        self.push_arg("--authority", authority)
    }

    pub fn deployment(self, deployment: &str) -> Self {
        self.push_arg("--deployment", deployment)
    }

    pub fn port(self, port: u16) -> Self {
        self.push_arg("-a", &format!("127.0.0.1:{}", port.to_string()))
    }

    pub fn metrics_port(self, port: u16) -> Self {
        self.push_arg(
            "--metrics-address",
            &format!("0.0.0.0:{}", port.to_string()),
        )
    }

    pub fn mysql(self, addr: &str) -> Self {
        self.push_arg("--upstream-db-url", addr)
    }

    pub fn async_migrations(mut self, migration_task_interval: u64) -> Self {
        self.args.push("--async-migrations".to_string());
        self.push_arg(
            "--migration-task-interval",
            &migration_task_interval.to_string(),
        )
    }
}
