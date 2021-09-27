/// Commands that just interact with Terraform on disk and do not assume we are going through
/// Buildkite. These commands would be used by admins hopefully only in emergencies and otherwise
/// rely upon our CI tools to run these.
use crate::{substrate, terraform};
use anyhow::Result;

pub(crate) fn validate(root_module: &substrate::RootModule) -> Result<()> {
    let terraform_path = root_module.to_terraform_path()?;
    terraform::run_init(&terraform_path)?;
    terraform::run_validate(&terraform_path)?;
    Ok(())
}

pub(crate) fn plan(root_module: &substrate::RootModule) -> Result<()> {
    let terraform_path = root_module.to_terraform_path()?;
    terraform::run_init(&terraform_path)?;
    terraform::run_plan(&terraform_path)?;
    Ok(())
}

pub(crate) fn apply(root_module: &substrate::RootModule) -> Result<()> {
    let terraform_path = root_module.to_terraform_path()?;
    terraform::run_init(&terraform_path)?;
    terraform::run_apply(&terraform_path)?;
    Ok(())
}
