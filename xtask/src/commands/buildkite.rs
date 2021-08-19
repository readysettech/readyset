//! Buildkite related commands to upload pipelines and to wrap the other commands with things that
//! interact with Buildkite.

// pub(crate) is used for things that are commands.
use anyhow::{anyhow, bail, Result};
use handlebars::Handlebars;
use std::{
    io::{ErrorKind, Write},
    path::{Path, PathBuf},
    process::{Command, Stdio},
};
use tracing::{event, Level};

use serde::Serialize;

use crate::substrate::{find_all_admin_module_locators, ModuleLocator};
use crate::terraform;

#[derive(Serialize)]
struct ModuleTemplateData {
    module_descriptor: String,
    module_locator_args: Vec<String>,
    buildkite_validate_command: String,
}

#[derive(Serialize)]
struct TerraformValidateAllPipelineTemplateData {
    modules: Vec<ModuleTemplateData>,
    plan_only: bool,
}

// TODO: Replace templating with YAML serialization with serde.
static TERRAFORM_VALIDATE_ALL_PIPELINE_TEMPLATE: &str =
    include_str!("../templates/terraform_validate_all_pipeline.yml.hbs");

fn upload_plan_artifact(terraform_path: &Path) -> Result<()> {
    let plan_path = terraform_path.join(".terraform").join("terraform.tfplan");
    if !plan_path.exists() {
        bail!("Could not upload plan artifact since it does not exist")
    }
    // TODO: Use our own S3 bucket for storing plan artifacts
    let exit_status = Command::new("buildkite-agent")
        .arg("artifact")
        .arg("upload")
        .arg(plan_path)
        .status()?;
    if exit_status.success() {
        Ok(())
    } else {
        bail!(
            "`buildkite artifact upload` failed with exit status {:?}",
            exit_status
        )
    }
}

fn generate_validate_all_pipeline() -> Result<String> {
    let all_admin_module_locators = find_all_admin_module_locators()?;
    let all_module_template_data: Vec<ModuleTemplateData> = all_admin_module_locators
        .iter()
        .map(|locator| ModuleTemplateData {
            module_descriptor: locator.to_description(),
            module_locator_args: locator.to_args(),
            buildkite_validate_command: String::from("buildkite-terraform-admin-validate"),
        })
        .collect();

    let template_data = TerraformValidateAllPipelineTemplateData {
        modules: all_module_template_data,
        plan_only: true,
    };

    // TODO: Extract out the handlebars registry into a single function to grab the whole thing
    // loaded and configured correctly.
    let mut handlebars = Handlebars::new();
    handlebars.set_strict_mode(true);
    handlebars.register_escape_fn(handlebars::no_escape);
    let pipeline_defintion =
        handlebars.render_template(TERRAFORM_VALIDATE_ALL_PIPELINE_TEMPLATE, &template_data)?;
    Ok(pipeline_defintion)
}

pub(crate) fn terraform_validate<T: ModuleLocator>(module_locator: &T) -> Result<()> {
    let terraform_path: PathBuf = module_locator.to_terraform_path()?;
    terraform::run_init(&terraform_path)?;
    terraform::run_validate(&terraform_path)?;
    Ok(())
}

pub(crate) fn terraform_plan<T: ModuleLocator>(module_locator: &T) -> Result<()> {
    let terraform_path: PathBuf = module_locator.to_terraform_path()?;
    terraform::run_init(&terraform_path)?;
    let status = terraform::run_plan(&terraform_path)?;
    match status {
        terraform::PlanStatus::HasDiff => {
            event!(Level::DEBUG, "Plan has changes so upload plan artifact");
            upload_plan_artifact(&terraform_path)
            // TODO: Upload pipeline for apply if we are in that mode.
        }
        terraform::PlanStatus::NoChanges => {
            event!(Level::DEBUG, "No changes in plan so nothing more to do");
            Ok(())
        }
    }
}

pub(crate) fn terraform_upload_validate_all_pipeline() -> Result<()> {
    let pipeline_definition = generate_validate_all_pipeline()?;

    let mut upload_command_child = match Command::new("buildkite-agent")
        .arg("pipeline")
        .arg("upload")
        .stdin(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(err) => {
            if err.kind() == ErrorKind::NotFound {
                event!(
                    Level::ERROR,
                    pipeline_definition = %pipeline_definition.as_str(),
                    "Could not upload Buildkite pipeline definition",
                );
                bail!("Could not find buildkite-agent.")
            }
            bail!("Could not spawn child: {}", err)
        }
    };

    let mut stdin = upload_command_child
        .stdin
        .take()
        .ok_or_else(|| anyhow!("Could not get child stdin handle"))?;

    event!(
        Level::DEBUG,
        pipeline_definition = %pipeline_definition.as_str(),
        "Uploading pipeline defintion"
    );
    {
        std::thread::spawn(move || stdin.write_all(pipeline_definition.as_bytes()));
    }

    let exit_status = upload_command_child.wait()?;
    if exit_status.success() {
        Ok(())
    } else {
        bail!(
            "`buildkite-agent pipeline upload` failed with exit status {:?}",
            exit_status
        )
    }
}
