#![feature(box_patterns)]

use std::borrow::Cow;
use std::path::{Path, PathBuf};

use ::console::style;
use anyhow::{anyhow, bail, Context, Result};
use clap::Parser;
use deployment::{Deployment, DeploymentData};
use directories::ProjectDirs;
use readyset_telemetry_reporter::{Error as TelemetryError, TelemetryReporter};
use tokio::fs::DirBuilder;

use crate::console::{password, select};

#[macro_use]
mod console;
mod aws;
mod compose;
mod constants;
mod deployment;
mod telemetry;

use crate::aws::AwsInstaller;
use crate::compose::ComposeInstaller;
use crate::deployment::DeploymentStatus;

/// Teaar down an existing deployment
#[derive(Parser)]
struct TearDown {
    /// Name of the deployment to tear down. If not specified, will prompt from a list of existing
    /// deployments
    deployment_name: Option<String>,
}

#[derive(Parser)]
enum Subcommand {
    TearDown(TearDown),
    Version,
}

/// Install and configure a ReadySet cluster in AWS
#[derive(Parser)]
pub struct Options {
    /// Directory to store state between runs. Defaults to `$XDG_STATE_HOME/readyset`.
    state_directory: Option<PathBuf>,

    /// Whether to use the full installer or not. Defaults to false. In the case that the full
    /// installer is not used, we default to docker-compose.
    #[clap(short, long)]
    full: bool,

    #[clap(subcommand)]
    subcommand: Option<Subcommand>,

    /// Supply the ReadySet API key. If not provided, will interactively prompt for the key
    #[clap(long, env = "RS_API_KEY")]
    api_key: Option<String>,
}

impl Options {
    fn state_directory(&self) -> Result<Cow<Path>> {
        if let Some(state_directory) = &self.state_directory {
            return Ok(Cow::Borrowed(state_directory));
        }

        let project_dirs = ProjectDirs::from("io", "readyset", "ReadySet").ok_or_else(|| {
            anyhow!("Could not determine HOME directory, and --state-directory not passed")
        })?;
        Ok(Cow::Owned(project_dirs.data_dir().to_owned()))
    }
}

struct Installer {
    options: Options,
    deployment: Deployment,
    telemetry: TelemetryReporter,
}

impl Installer {
    fn new(options: Options, deployment: Deployment, telemetry: TelemetryReporter) -> Self {
        Self {
            options,
            deployment,
            telemetry,
        }
    }

    /// Save this installer's deployment to the configured state directory
    pub async fn save(&self) -> Result<()> {
        self.deployment
            .save_to_directory(self.options.state_directory()?)
            .await
    }

    /// Run the install process, picking up where the user left off if necessary.
    pub async fn run(&mut self) -> Result<()> {
        if self.deployment.is_complete() {
            println!("This deployment is already running");
            self.deployment.print_connection_information()?;

            println!();
            match select()
                .with_prompt("What would you like to do with this deployment?")
                .items(&[
                    "Upgrade to the latest version of ReadySet",
                    "Tear down the deployment",
                ])
                .interact()?
            {
                0 => self.upgrade().await?,
                1 => self.tear_down().await?,
                _ => unreachable!(),
            }

            return Ok(());
        } else if self.deployment.is_tearing_down() {
            println!(
                "Continuing tear down of deployment {}",
                style(&self.deployment.name).bold()
            );
            self.tear_down().await?;

            return Ok(());
        }

        self.install().await
    }

    async fn install(&mut self) -> Result<()> {
        let _ = self
            .telemetry
            .send_payload(&telemetry::Payload::DeploymentStarted)
            .await;

        match self.deployment.inner {
            DeploymentData::Cloudformation(_) => {
                let mut aws = AwsInstaller::new(&mut self.options, &mut self.deployment);
                aws.run().await?;
            }
            DeploymentData::Compose(_) => {
                ComposeInstaller::new(&mut self.options, &mut self.deployment)
                    .install()
                    .await?;
            }
        }

        let _ = self
            .telemetry
            .send_payload(&telemetry::Payload::DeploymentFinished)
            .await;

        Ok(())
    }

    /// Upgrade an existing deployment in-place
    pub(crate) async fn upgrade(&mut self) -> Result<()> {
        match self.deployment.inner {
            DeploymentData::Cloudformation(_) => {
                bail!("Sorry, upgrading isn't supported for cloudformation deployments yet")
            }
            DeploymentData::Compose(_) => {
                let mut compose = ComposeInstaller::new(&mut self.options, &mut self.deployment);
                compose.upgrade().await?
            }
        }

        success!(
            "Deployment {} successfully upgraded",
            self.deployment.name()
        );

        Ok(())
    }

    /// Tear down all resources for an already-created deployment
    pub async fn tear_down(&mut self) -> Result<()> {
        self.deployment.status = DeploymentStatus::TearingDown;
        self.save().await?;

        match self.deployment.inner {
            DeploymentData::Cloudformation(_) => {
                let mut aws = AwsInstaller::new(&mut self.options, &mut self.deployment);
                aws.tear_down().await?
            }
            DeploymentData::Compose(_) => {
                let compose = ComposeInstaller::new(&mut self.options, &mut self.deployment);
                compose.tear_down().await?
            }
        }

        Deployment::delete(self.options.state_directory()?, self.deployment.name()).await?;

        success!(
            "Deployment {} successfully torn down",
            self.deployment.name()
        );

        let _ = self
            .telemetry
            .send_payload(&telemetry::Payload::DeploymentTornDown)
            .await;

        Ok(())
    }
}

async fn prompt_for_and_validate_api_key(options: &Options) -> Result<TelemetryReporter> {
    if let Some(api_key) = &options.api_key {
        let telemetry = TelemetryReporter::new(api_key)?;
        telemetry.authenticate().await?;
        Ok(telemetry)
    } else {
        loop {
            let api_key = password().with_prompt("API key").interact()?;
            let telemetry = TelemetryReporter::new(api_key)?;
            match telemetry.authenticate().await {
                Ok(_) => return Ok(telemetry),
                Err(TelemetryError::InvalidAPIKeyHeader(_) | TelemetryError::Unauthorized) => {
                    println!("Invalid API key. Let's try again.")
                }
                Err(e) => return Err(e).context("Validating API token"),
            };
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut options = Options::parse();
    println!("Welcome to the ReadySet orchestrator.\n");
    let telemetry = prompt_for_and_validate_api_key(&options).await?;

    let _ = telemetry
        .send_payload(&telemetry::Payload::InstallerRun)
        .await;

    DirBuilder::new()
        .recursive(true)
        .create(options.state_directory()?)
        .await?;

    match &options.subcommand {
        Some(Subcommand::TearDown(tear_down)) => {
            let deployment = match &tear_down.deployment_name {
                Some(deployment_name) => {
                    if !options
                        .state_directory()?
                        .as_ref()
                        .join(deployment_name)
                        .exists()
                    {
                        bail!("No deployment with name {} exists.", deployment_name);
                    }
                    Deployment::load(options.state_directory()?, deployment_name).await?
                }
                None => {
                    println!("Which deployment would you like to tear down?");
                    deployment::prompt_for_existing_deployment(options.state_directory()?).await?
                }
            };

            let mut installer = Installer::new(options, deployment, telemetry.clone());
            installer.tear_down().await?;
        }
        Some(Subcommand::Version) => {
            println!("{}", *compose::template::DOCKER_TAG);
        }
        None => {
            println!("Welcome to the ReadySet orchestrator.\n");
            DirBuilder::new()
                .recursive(true)
                .create(options.state_directory()?)
                .await?;
            let deployment = deployment::create_or_load_existing(&mut options).await?;
            let mut installer = Installer::new(options, deployment, telemetry.clone());

            installer.run().await?;
        }
    }

    let _ = telemetry
        .send_payload(&telemetry::Payload::InstallerFinished)
        .await;

    Ok(())
}
