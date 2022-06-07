//! Buildkite related commands to upload pipelines and to wrap the other commands with things that
//! interact with Buildkite.

// pub(crate) is used for things that are commands.

use std::process::Command;

use anyhow::{bail, Result};
use tracing::error;

use crate::substrate::{self, RootModule};
use crate::{gerrit, terraform};

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

fn run_validate_and_plan_all(include_network: bool) -> Result<()> {
    let root_modules: Vec<RootModule> = substrate::root_modules(include_network)?;

    let mut init_errors: Vec<anyhow::Error> = Vec::new();
    let mut validate_errors: Vec<anyhow::Error> = Vec::new();
    let mut plan_errors: Vec<anyhow::Error> = Vec::new();

    for root_module in root_modules {
        println!("--- :terraform: Initializing {}", root_module);
        let terraform_path = root_module.to_terraform_path()?;

        match terraform::run_init(&terraform_path) {
            Ok(_) => {}
            Err(e) => {
                println!("^^^ +++");
                println!("Error: {}", &e);
                init_errors.push(e);
                continue;
            }
        }

        println!("--- :terraform: Validating {}", root_module);
        match terraform::run_validate(&terraform_path) {
            Ok(_) => {}
            Err(e) => {
                println!("^^^ +++");
                println!("Error: {}", &e);
                validate_errors.push(e);
                continue;
            }
        }

        println!("--- :terraform: Planning {}", root_module);
        match terraform::run_plan(&terraform_path, false) {
            Ok(plan) => {
                if plan.has_diff() {
                    println!("^^^ +++");
                    upload_plan_artifact(&plan)?;
                    gerrit::post_terraform_plan(&root_module, &plan)?;
                }
            }
            Err(e) => {
                println!("^^^ +++");
                println!("Error: {}", &e);
                plan_errors.push(e);
                continue;
            }
        }
    }

    if init_errors.is_empty() && validate_errors.is_empty() && plan_errors.is_empty() {
        Ok(())
    } else {
        error!(
            "{} working directories had initialization errors",
            init_errors.len()
        );
        error!(
            "{} working directories had validation errors",
            validate_errors.len()
        );
        error!("{} working directories had plan errors", plan_errors.len());
        bail!("Errors found while running all plans")
    }
}

pub(crate) fn terraform_run_validate_and_plan_all(include_network: bool) -> Result<()> {
    run_validate_and_plan_all(include_network)?;
    Ok(())
}
