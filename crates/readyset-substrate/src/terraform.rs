/// Interface to the various Terraform commands that need to be run with no connection to Substrate
/// or anything else. Runs the commands in a standard way along with some help to get information
/// out from Terraform.
use std::{path::Path, process::Command};

use anyhow::{anyhow, bail, Result};
use tracing::{event, Level};

pub(crate) enum PlanStatus {
    NoChanges,
    HasDiff,
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

pub(crate) fn run_plan(chdir: &Path) -> Result<PlanStatus> {
    event!(Level::DEBUG, "Running terraform plan");
    let chdir = chdir
        .to_str()
        .take()
        .ok_or_else(|| anyhow!("Could not convert chdir path to string"))?;
    let exit_status = Command::new("terraform")
        .arg(format!("-chdir={}", chdir))
        .arg("plan")
        .arg("-input=false")
        .arg("-detailed-exitcode")
        .arg("-out=.terraform/terraform.tfplan")
        .status()?;
    match exit_status.code() {
        Some(0) => Ok(PlanStatus::NoChanges),
        Some(2) => Ok(PlanStatus::HasDiff),
        Some(code) => bail!("`terraform plan` failed with exit code {:?}", code),
        None => bail!("`terraform plan` terminated by signal"),
    }
}

// TOOD: Use this to create refresh plans to reflect changes into Terraform State that happened outside of Terraform
#[allow(dead_code)]
pub(crate) fn run_plan_refresh_only(chdir: &Path) -> Result<PlanStatus> {
    event!(Level::DEBUG, "Running terraform plan -refresh-only");
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
    match exit_status.code() {
        Some(0) => Ok(PlanStatus::NoChanges),
        Some(2) => Ok(PlanStatus::HasDiff),
        Some(code) => bail!("`terraform plan` failed with exit code {:?}", code),
        None => bail!("`terraform plan` terminated by signal"),
    }
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
