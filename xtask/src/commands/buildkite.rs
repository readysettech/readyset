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

use crate::substrate::{
    find_all_admin_module_locators, find_all_service_module_locators, ModuleLocator,
};
use crate::terraform;

#[derive(Serialize)]
struct ModuleTemplateData {
    module_descriptor: String,
    module_locator_args: Vec<String>,
    buildkite_validate_command: String,
    buildkite_plan_command: String,
}

#[derive(Serialize)]
struct TerraformValidateAllPipelineTemplateData {
    modules: Vec<ModuleTemplateData>,
    plan_only: bool,
}
#[derive(Serialize)]
struct TerraformPlanAllPipelineTemplateData {
    modules: Vec<ModuleTemplateData>,
}

// TODO: Replace templating with YAML serialization with serde.
static TERRAFORM_VALIDATE_ALL_PIPELINE_TEMPLATE: &str =
    include_str!("../templates/terraform_validate_all_pipeline.yml.hbs");
static TERRAFORM_PLAN_ALL_PIPELINE_TEMPLATE: &str =
    include_str!("../templates/terraform_plan_all_pipeline.yml.hbs");

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

fn template_registry() -> Result<Handlebars<'static>> {
    let mut handlebars = Handlebars::new();
    handlebars.set_strict_mode(true);
    handlebars.register_escape_fn(handlebars::no_escape);
    handlebars
        .register_template_string("validate_all", TERRAFORM_VALIDATE_ALL_PIPELINE_TEMPLATE)?;
    handlebars.register_template_string("plan_all", TERRAFORM_PLAN_ALL_PIPELINE_TEMPLATE)?;
    Ok(handlebars)
}

fn module_template_data_for_all() -> Result<Vec<ModuleTemplateData>> {
    let all_admin_module_locators = find_all_admin_module_locators()?;
    let all_service_module_locators = find_all_service_module_locators()?;
    let admin_template_data_iter =
        all_admin_module_locators
            .iter()
            .map(|locator| ModuleTemplateData {
                module_descriptor: locator.to_description(),
                module_locator_args: locator.to_args(),
                buildkite_validate_command: String::from("buildkite-terraform-admin-validate"),
                buildkite_plan_command: String::from("buildkite-terraform-admin-plan"),
            });
    let service_template_data_iter =
        all_service_module_locators
            .iter()
            .map(|locator| ModuleTemplateData {
                module_descriptor: locator.to_description(),
                module_locator_args: locator.to_args(),
                buildkite_validate_command: String::from("buildkite-terraform-service-validate"),
                buildkite_plan_command: String::from("buildkite-terraform-service-plan"),
            });
    Ok(admin_template_data_iter
        .chain(service_template_data_iter)
        .collect())
}

fn generate_plan_all_pipeline() -> Result<String> {
    let modules: Vec<ModuleTemplateData> = module_template_data_for_all()?;

    let template_data = TerraformPlanAllPipelineTemplateData { modules };

    let handlebars = template_registry()?;
    let pipeline_defintion = handlebars.render("plan_all", &template_data)?;

    Ok(pipeline_defintion)
}

fn generate_validate_all_pipeline() -> Result<String> {
    let modules: Vec<ModuleTemplateData> = module_template_data_for_all()?;

    let template_data = TerraformValidateAllPipelineTemplateData {
        modules,
        plan_only: true,
    };

    let handlebars = template_registry()?;
    let pipeline_defintion = handlebars.render("validate_all", &template_data)?;

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

fn buildkite_pipeline_upload(pipeline_definition: String) -> Result<()> {
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

pub(crate) fn terraform_upload_validate_all_pipeline() -> Result<()> {
    let pipeline_definition = generate_validate_all_pipeline()?;
    buildkite_pipeline_upload(pipeline_definition)
}

pub(crate) fn terraform_upload_plan_all_pipeline() -> Result<()> {
    let pipeline_definition = generate_plan_all_pipeline()?;
    buildkite_pipeline_upload(pipeline_definition)
}
