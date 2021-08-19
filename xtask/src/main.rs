use anyhow::Result;
use clap::{AppSettings, Clap};
use tracing_subscriber::EnvFilter;

mod commands;
mod substrate;
mod terraform;

// Some draft commands have been added to this struct to give some guidance on what needs to be implemented.
// We should have commands that run outside Buildkite just in case Buildkite falls over and we need to run
// without Buildkite in case of emergency.
#[derive(Clap, Debug)]
enum Subcommand {
    TerraformServiceValidate {
        #[clap(flatten)]
        service_module_locator: substrate::ServiceModuleLocator,
    },
    TerraformServicePlan {
        #[clap(flatten)]
        service_module_locator: substrate::ServiceModuleLocator,
    },
    TerraformServiceApply {
        #[clap(flatten)]
        service_module_locator: substrate::ServiceModuleLocator,
    },
    TerraformAdminValidate {
        #[clap(flatten)]
        admin_module_locator: substrate::AdminModuleLocator,
    },
    TerraformAdminPlan {
        #[clap(flatten)]
        admin_module_locator: substrate::AdminModuleLocator,
    },
    TerraformAdminApply {
        #[clap(flatten)]
        admin_module_locator: substrate::AdminModuleLocator,
    },
    BuildkiteTerraformServiceValidate {
        #[clap(flatten)]
        service_module_locator: substrate::ServiceModuleLocator,
    },
    BuildkiteTerraformServicePlan {
        #[clap(flatten)]
        service_module_locator: substrate::ServiceModuleLocator,
    },
    // BuildkiteTerraformServiceApply,
    BuildkiteTerraformAdminValidate {
        #[clap(flatten)]
        admin_module_locator: substrate::AdminModuleLocator,
    },
    BuildkiteTerraformAdminPlan {
        #[clap(flatten)]
        admin_module_locator: substrate::AdminModuleLocator,
    },
    // BuildkiteTerraformServiceApply,
    BuildkiteTerraformUploadValidateAllPipeline,
}

#[derive(Clap, Debug)]
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
        Subcommand::TerraformServiceValidate {
            service_module_locator,
        } => commands::terraform::validate(&service_module_locator),
        Subcommand::TerraformServicePlan {
            service_module_locator,
        } => commands::terraform::plan(&service_module_locator),
        Subcommand::TerraformServiceApply {
            service_module_locator,
        } => commands::terraform::apply(&service_module_locator),
        Subcommand::TerraformAdminValidate {
            admin_module_locator,
        } => commands::terraform::validate(&admin_module_locator),
        Subcommand::TerraformAdminPlan {
            admin_module_locator,
        } => commands::terraform::plan(&admin_module_locator),
        Subcommand::TerraformAdminApply {
            admin_module_locator,
        } => commands::terraform::apply(&admin_module_locator),
        Subcommand::BuildkiteTerraformServiceValidate {
            service_module_locator,
        } => commands::buildkite::terraform_validate(&service_module_locator),
        Subcommand::BuildkiteTerraformServicePlan {
            service_module_locator,
        } => commands::buildkite::terraform_plan(&service_module_locator),
        Subcommand::BuildkiteTerraformAdminValidate {
            admin_module_locator,
        } => commands::buildkite::terraform_validate(&admin_module_locator),
        Subcommand::BuildkiteTerraformAdminPlan {
            admin_module_locator,
        } => commands::buildkite::terraform_plan(&admin_module_locator),
        Subcommand::BuildkiteTerraformUploadValidateAllPipeline => {
            commands::buildkite::terraform_upload_validate_all_pipeline()
        }
    }
}
