//! Buildkite related commands to upload pipelines and to wrap the other commands with things that
//! interact with Buildkite.

// pub(crate) is used for things that are commands.
use std::io::{ErrorKind, Write};
use std::process::{Command, Stdio};

use anyhow::{anyhow, bail, Result};
use handlebars::Handlebars;
use serde::Serialize;
use tracing::{event, Level};

use crate::gerrit;
use crate::substrate::{self, RootModule};
use crate::terraform;

#[derive(Serialize)]
struct TerraformValidateAllPipelineTemplateData {
    root_modules: Vec<RootModule>,
    plan_only: bool,
}
#[derive(Serialize)]
struct TerraformPlanAllPipelineTemplateData {
    root_modules: Vec<RootModule>,
}

// TODO: Replace templating with YAML serialization with serde.
static TERRAFORM_VALIDATE_ALL_PIPELINE_TEMPLATE: &str =
    include_str!("../templates/terraform_validate_all_pipeline.yml.hbs");
static TERRAFORM_PLAN_ALL_PIPELINE_TEMPLATE: &str =
    include_str!("../templates/terraform_plan_all_pipeline.yml.hbs");

fn upload_plan_artifact(plan: &terraform::Plan) -> Result<()> {
    // TODO: Use our own S3 bucket for storing plan artifacts
    let exit_status = Command::new("buildkite-agent")
        .arg("artifact")
        .arg("upload")
        .arg(plan.path())
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

fn generate_plan_all_pipeline() -> Result<String> {
    let root_modules: Vec<RootModule> = substrate::root_modules()?;

    let template_data = TerraformPlanAllPipelineTemplateData { root_modules };

    let handlebars = template_registry()?;
    let pipeline_defintion = handlebars.render("plan_all", &template_data)?;

    Ok(pipeline_defintion)
}

fn generate_validate_all_pipeline() -> Result<String> {
    let root_modules: Vec<RootModule> = substrate::root_modules()?;

    let template_data = TerraformValidateAllPipelineTemplateData {
        root_modules,
        plan_only: true,
    };

    let handlebars = template_registry()?;
    let pipeline_defintion = handlebars.render("validate_all", &template_data)?;

    Ok(pipeline_defintion)
}

fn run_validate_all() -> Result<()> {
    let root_modules: Vec<RootModule> = substrate::root_modules()?;

    let mut errors: Vec<anyhow::Error> = Vec::new();

    for root_module in root_modules {
        println!("--- :terraform: Validating {}", root_module);
        match terraform_validate(&root_module) {
            Ok(_) => {}
            Err(e) => {
                println!("^^^ +++");
                println!("Error: {}", &e);
                errors.push(e);
            }
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        bail!("{} workspaces had validation errors", errors.len())
    }
}

fn run_plan_all() -> Result<()> {
    let root_modules: Vec<RootModule> = substrate::root_modules()?;

    let mut errors: Vec<anyhow::Error> = Vec::new();

    for root_module in root_modules {
        println!("--- :terraform: Planning {}", root_module);
        match terraform_plan(&root_module) {
            Ok(plan) => {
                if plan.has_diff() {
                    println!("^^^ +++");
                }
            }
            Err(e) => {
                println!("^^^ +++");
                println!("Error: {}", &e);
                errors.push(e);
            }
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        bail!("{} workspaces could not be planned", errors.len())
    }
}

pub(crate) fn terraform_validate(root_module: &substrate::RootModule) -> Result<()> {
    let terraform_path = root_module.to_terraform_path()?;
    terraform::run_init(&terraform_path)?;
    terraform::run_validate(&terraform_path)?;
    Ok(())
}

pub(crate) fn terraform_plan(root_module: &substrate::RootModule) -> Result<terraform::Plan> {
    let terraform_path = root_module.to_terraform_path()?;
    terraform::run_init(&terraform_path)?;
    let plan = terraform::run_plan(&terraform_path, false)?;
    if plan.has_diff() {
        event!(Level::DEBUG, "Plan has changes so upload plan artifact");
        upload_plan_artifact(&plan)?;
        gerrit::post_terraform_plan(root_module, &plan)?;
        // TODO: Upload pipeline for apply if we are in that mode.
    } else {
        event!(Level::DEBUG, "No changes in plan so nothing more to do");
    }
    Ok(plan)
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

pub(crate) fn terraform_run_validate_all() -> Result<()> {
    run_validate_all()?;
    Ok(())
}

pub(crate) fn terraform_generate_validate_all_pipeline() -> Result<()> {
    let pipeline_definition = generate_validate_all_pipeline()?;
    println!("{}", pipeline_definition);
    Ok(())
}

pub(crate) fn terraform_upload_validate_all_pipeline() -> Result<()> {
    let pipeline_definition = generate_validate_all_pipeline()?;
    buildkite_pipeline_upload(pipeline_definition)
}

pub(crate) fn terraform_run_plan_all() -> Result<()> {
    run_plan_all()?;
    Ok(())
}

pub(crate) fn terraform_generate_plan_all_pipeline() -> Result<()> {
    let pipeline_definition = generate_plan_all_pipeline()?;
    println!("{}", pipeline_definition);
    Ok(())
}

pub(crate) fn terraform_upload_plan_all_pipeline() -> Result<()> {
    let pipeline_definition = generate_plan_all_pipeline()?;
    buildkite_pipeline_upload(pipeline_definition)
}
