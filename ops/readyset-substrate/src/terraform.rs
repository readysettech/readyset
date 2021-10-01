//! Interface to the various Terraform commands that need to be run with no connection to Substrate
//! or anything else. Runs the commands in a standard way along with some help to get information
//! out from Terraform.

use std::convert::{TryFrom, TryInto};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::{anyhow, bail, Result};
use serde::de::DeserializeOwned;
use tracing::{event, Level};

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum PlanStatus {
    NoChanges,
    HasDiff,
}

impl TryFrom<ExitStatus> for PlanStatus {
    type Error = anyhow::Error;

    fn try_from(exit_status: ExitStatus) -> Result<Self, Self::Error> {
        match exit_status.code() {
            Some(0) => Ok(PlanStatus::NoChanges),
            Some(2) => Ok(PlanStatus::HasDiff),
            Some(code) => bail!("`terraform plan` failed with exit code {:?}", code),
            None => bail!("`terraform plan` terminated by signal"),
        }
    }
}

pub(crate) struct Plan {
    status: PlanStatus,
    module_dir: PathBuf,
}

impl Plan {
    fn terraform_command(&self) -> Command {
        let mut cmd = Command::new("terraform");
        cmd.arg(format!("-chdir={}", self.module_dir.display()));
        cmd
    }

    pub(crate) fn has_diff(&self) -> bool {
        self.status == PlanStatus::HasDiff
    }

    pub(crate) fn describe(&self) -> Result<String> {
        Ok(String::from_utf8_lossy(
            &self
                .terraform_command()
                .arg("show")
                .arg("-no-color")
                .arg(self.path())
                .output()?
                .stdout,
        )
        .into_owned())
    }

    pub(crate) fn path(&self) -> PathBuf {
        self.module_dir.join(".terraform").join("terraform.tfplan")
    }

    #[allow(dead_code)]
    pub(crate) fn to_json<T>(&self) -> Result<T>
    where
        T: DeserializeOwned,
    {
        let json = self
            .terraform_command()
            .arg("show")
            .arg("-json")
            .arg(self.path())
            .output()?
            .stdout;
        Ok(serde_json::from_slice(&json)?)
    }
}

pub(crate) fn run_init(chdir: &Path) -> Result<()> {
    event!(Level::DEBUG, "Running terraform init");
    let chdir = chdir
        .to_str()
        .take()
        .ok_or_else(|| anyhow!("Could not convert chdir path to string"))?;
    let exit_status = Command::new("terraform")
        .arg(format!("-chdir={}", chdir))
        .arg("init")
        .arg("-input=false")
        .status()?;
    if exit_status.success() {
        Ok(())
    } else {
        bail!("`terraform init` failed with exit status {:?}", exit_status)
    }
}

// TOOD: Use this to update all of the Terraform lockfiles
#[allow(dead_code)]
pub(crate) fn run_init_upgrade(chdir: &Path) -> Result<()> {
    event!(Level::DEBUG, "Running terraform init -upgrade");
    let chdir = chdir
        .to_str()
        .take()
        .ok_or_else(|| anyhow!("Could not convert chdir path to string"))?;
    let exit_status = Command::new("terraform")
        .arg(format!("-chdir={}", chdir))
        .arg("init")
        .arg("-input=false")
        .arg("-upgrade")
        .status()?;
    if exit_status.success() {
        Ok(())
    } else {
        bail!("`terraform init` failed with exit status {:?}", exit_status)
    }
}

pub(crate) fn run_validate(chdir: &Path) -> Result<()> {
    event!(Level::DEBUG, "Running terraform validate");
    let chdir = chdir
        .to_str()
        .take()
        .ok_or_else(|| anyhow!("Could not convert chdir path to string"))?;
    let exit_status = Command::new("terraform")
        .arg(format!("-chdir={}", chdir))
        .arg("validate")
        .status()?;
    if exit_status.success() {
        Ok(())
    } else {
        bail!(
            "`terraform validate` failed with exit status {:?}",
            exit_status
        )
    }
}

pub(crate) fn run_plan(chdir: &Path, lock: bool) -> Result<Plan> {
    event!(Level::DEBUG, "Running terraform plan");

    let module_dir = chdir.to_owned();
    let chdir = chdir
        .to_str()
        .take()
        .ok_or_else(|| anyhow!("Could not convert chdir path to string"))?;

    let mut command = Command::new("terraform");
    command
        .arg(format!("-chdir={}", chdir))
        .arg("plan")
        .arg("-input=false")
        .arg("-detailed-exitcode")
        .arg("-out=.terraform/terraform.tfplan");
    if !lock {
        // TODO consider removing this if we do apply from buildkite (but then we'll have to solve locks
        // getting left around from canceled builds)
        command.arg("-lock=false");
    }
    let mut child = command.spawn()?;

    let term = Arc::new(AtomicBool::new(false));
    signal_hook::flag::register(signal_hook::consts::SIGTERM, Arc::clone(&term))?;
    let mut exit_status: Option<ExitStatus> = None;
    while !term.load(Ordering::Relaxed) {
        match child.try_wait() {
            Ok(Some(status)) => {
                exit_status = Some(status);
                break;
            }
            Ok(None) => {
                std::hint::spin_loop();
                continue;
            }
            Err(e) => bail!("`terraform plan` failed to wait {:?}", e),
        }
    }

    if let Some(exit_status) = exit_status {
        Ok(Plan {
            status: exit_status.try_into()?,
            module_dir,
        })
    } else {
        nix::sys::signal::kill(
            nix::unistd::Pid::from_raw(child.id() as nix::libc::pid_t),
            nix::sys::signal::SIGTERM,
        )?;
        child.wait()?;
        bail!("`terraform plan` was canceled by SIGTERM")
    }
}

// TOOD: Use this to create refresh plans to reflect changes into Terraform State that happened outside of Terraform
#[allow(dead_code)]
pub(crate) fn run_plan_refresh_only(chdir: &Path) -> Result<Plan> {
    event!(Level::DEBUG, "Running terraform plan -refresh-only");

    let module_dir = chdir.to_owned();
    let chdir = chdir
        .to_str()
        .take()
        .ok_or_else(|| anyhow!("Could not convert chdir path to string"))?;

    let exit_status = Command::new("terraform")
        .arg(format!("-chdir={}", chdir))
        .arg("plan")
        .arg("-input=false")
        .arg("-detailed-exitcode")
        .arg("-refresh-only")
        .arg("-out=.terraform/terraform.tfplan")
        .status()?;

    Ok(Plan {
        status: exit_status.try_into()?,
        module_dir,
    })
}

pub(crate) fn run_apply(chdir: &Path) -> Result<()> {
    event!(Level::DEBUG, "Running terraform apply");
    let chdir = chdir
        .to_str()
        .take()
        .ok_or_else(|| anyhow!("Could not convert chdir path to string"))?;
    let exit_status = Command::new("terraform")
        .arg(format!("-chdir={}", chdir))
        .arg("apply")
        .arg("-input=false")
        .arg(".terraform/terraform.tfplan")
        .status()?;
    // TODO: Delete plan on Success? Failure? Both?
    if exit_status.success() {
        Ok(())
    } else {
        bail!(
            "`terraform apply` failed with exit status {:?}",
            exit_status
        )
    }
}

// TOOD: Use this to apply refresh plans to reflect changes into Terraform state that happened outside of Terraform
#[allow(dead_code)]
pub(crate) fn run_apply_refresh_only(chdir: &Path) -> Result<()> {
    event!(Level::DEBUG, "Running terraform apply");
    let chdir = chdir
        .to_str()
        .take()
        .ok_or_else(|| anyhow!("Could not convert chdir path to string"))?;
    let exit_status = Command::new("terraform")
        .arg(format!("-chdir={}", chdir))
        .arg("apply")
        .arg("-input=false")
        .arg("-refresh-only")
        .arg(".terraform/terraform.tfplan")
        .status()?;
    // TODO: Delete plan on Success? Failure? Both?
    if exit_status.success() {
        Ok(())
    } else {
        bail!(
            "`terraform apply` failed with exit status {:?}",
            exit_status
        )
    }
}
