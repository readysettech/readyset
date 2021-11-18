use std::{
    env,
    path::{Path, PathBuf},
};

use anyhow::Result;
use clap::{AppSettings, Parser};
use tracing_subscriber::EnvFilter;

mod commands;

#[derive(Parser, Debug)]
enum Subcommand {
    PreCache,
    InstallDockerCredentialECRLogin,
    InstallCommitMsgHook,
    ValidateBuildkiteCommon,
    PresignedDockerImageUrls {
        #[clap(flatten)]
        opts: commands::presigned_docker_image_urls::Opts,
    },
    Bisect(Box<commands::bisect::BisectCommand>),
    MockPrometheusPushGateway,
}

#[derive(Parser, Debug)]
#[clap(setting=AppSettings::SubcommandRequired)]
struct Opts {
    #[clap(subcommand)]
    subcommand: Subcommand,
}

fn project_root_path() -> PathBuf {
    // This tries to pull the `CARGO_MANIFEST_DIR` from the environment. But we might also compile an
    // xtask to be used as say a pre-commit hook and `env!` will compile the location of the project
    // into the binary as `env!` is resolved at compile time.
    Path::new(
        &env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| env!("CARGO_MANIFEST_DIR").to_owned()),
    )
    .ancestors()
    .nth(1)
    .unwrap()
    .to_path_buf()
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .pretty()
        .without_time()
        .with_env_filter(EnvFilter::from_default_env().add_directive("xtask=debug".parse()?))
        .init();

    let opts = Opts::parse();
    match opts.subcommand {
        Subcommand::InstallCommitMsgHook => commands::install_commit_msg_hook::run(),
        Subcommand::InstallDockerCredentialECRLogin => {
            commands::install_docker_credential_ecr_login::run()
        }
        Subcommand::ValidateBuildkiteCommon => commands::validate_buildkite_common::run(),
        Subcommand::PresignedDockerImageUrls { opts } => {
            commands::presigned_docker_image_urls::run(opts)
        }
        Subcommand::PreCache => commands::pre_cache::run(),
        Subcommand::Bisect(opts) => commands::bisect::run(*opts),
        Subcommand::MockPrometheusPushGateway => Ok(commands::mock_prometheus_push_gateway::run()?),
    }
}
