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
    TerraformValidate {
        root_module: RootModule,
    },
    TerraformPlan {
        root_module: RootModule,
    },
    TerraformApply {
        root_module: RootModule,
    },
    // TODO: These commands should exist
    // BuildkiteTerraformValidateAndPlan {
    //   root_module: RootModule,
    // },
    // BuildkiteSubstrateTerraformApply {
    //    root_module: String
    // }
    BuildkiteTerraformRunValidateAndPlanAll {
        #[clap(long)]
        /// Should we run the validate and plan on the network working
        /// directories
        include_network: bool,
    },
    // TODO: This command should exist
    // BuildkiteSubstrateTerraformApplyAll {
    //    include_network: bool
    // }
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
        // TODO: This command should exist
        // Subcommand::BuildkiteTerraformValidateAndPlan { root_module } => {
        //     commands::buildkite::terraform_validate_and_plan(&root_module)
        // }
        Subcommand::BuildkiteTerraformRunValidateAndPlanAll { include_network } => {
            commands::buildkite::terraform_run_validate_and_plan_all(include_network)
        }
    }
}
