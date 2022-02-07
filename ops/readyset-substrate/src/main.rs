use anyhow::Result;
use clap::{AppSettings, Parser};
use substrate::RootModule;
use tracing_subscriber::EnvFilter;
mod commands;
mod gerrit;
mod substrate;
mod terraform;

#[derive(Parser, Debug)]
enum Subcommand {
    TerraformValidate { root_module: RootModule },
    TerraformPlan { root_module: RootModule },
    TerraformApply { root_module: RootModule },
    BuildkiteTerraformValidate { root_module: RootModule },
    BuildkiteTerraformPlan { root_module: RootModule },
    // BuildkiteSubstrateTerraformApply {
    //    root_module: String
    // }
    BuildkiteTerraformGenerateValidateAllPipeline,
    BuildkiteTerraformUploadValidateAllPipeline,
    BuildkiteTerraformRunValidateAll,
    BuildkiteTerraformGeneratePlanAllPipeline,
    BuildkiteTerraformUploadPlanAllPipeline,
    BuildkiteTerraformRunPlanAll,
}

#[derive(Parser, Debug)]
#[clap(setting=AppSettings::SubcommandRequired)]
struct Opts {
    #[clap(subcommand)]
    subcommand: Subcommand,
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .pretty()
        .without_time()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let opts = Opts::parse();
    match opts.subcommand {
        Subcommand::TerraformValidate { root_module } => {
            commands::terraform::validate(&root_module)
        }
        Subcommand::TerraformPlan { root_module } => commands::terraform::plan(&root_module),
        Subcommand::TerraformApply { root_module } => commands::terraform::apply(&root_module),
        Subcommand::BuildkiteTerraformValidate { root_module } => {
            commands::buildkite::terraform_validate(&root_module)
        }
        Subcommand::BuildkiteTerraformPlan { root_module } => {
            commands::buildkite::terraform_plan(&root_module).map(|_| ())
        }
        Subcommand::BuildkiteTerraformGenerateValidateAllPipeline => {
            commands::buildkite::terraform_generate_validate_all_pipeline()
        }
        Subcommand::BuildkiteTerraformUploadValidateAllPipeline => {
            commands::buildkite::terraform_upload_validate_all_pipeline()
        }
        Subcommand::BuildkiteTerraformRunValidateAll => {
            commands::buildkite::terraform_run_validate_all()
        }
        Subcommand::BuildkiteTerraformGeneratePlanAllPipeline => {
            commands::buildkite::terraform_generate_plan_all_pipeline()
        }
        Subcommand::BuildkiteTerraformUploadPlanAllPipeline => {
            commands::buildkite::terraform_upload_plan_all_pipeline()
        }
        Subcommand::BuildkiteTerraformRunPlanAll => commands::buildkite::terraform_run_plan_all(),
    }
}
