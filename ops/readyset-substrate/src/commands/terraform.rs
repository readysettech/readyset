/// Commands that just interact with Terraform on disk and do not assume we are going through
/// Buildkite. These commands would be used by admins hopefully only in emergencies and otherwise
/// rely upon our CI tools to run these.
use crate::substrate::ModuleLocator;
use crate::terraform;
use anyhow::Result;
use std::path::PathBuf;

pub(crate) fn validate<T: ModuleLocator>(module_locator: &T) -> Result<()> {
    let terraform_path: PathBuf = module_locator.to_terraform_path()?;
    terraform::run_init(&terraform_path)?;
    terraform::run_validate(&terraform_path)?;
    Ok(())
}

pub(crate) fn plan<T: ModuleLocator>(module_locator: &T) -> Result<()> {
    let terraform_path: PathBuf = module_locator.to_terraform_path()?;
    terraform::run_init(&terraform_path)?;
    terraform::run_plan(&terraform_path)?;
    Ok(())
}

pub(crate) fn apply<T: ModuleLocator>(module_locator: &T) -> Result<()> {
    let terraform_path: PathBuf = module_locator.to_terraform_path()?;
    terraform::run_init(&terraform_path)?;
    terraform::run_apply(&terraform_path)?;
    Ok(())
}
