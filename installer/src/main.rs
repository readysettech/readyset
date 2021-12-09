use std::borrow::Cow;
use std::collections::HashSet;
use std::env;
use std::fmt::Debug;
use std::path::{Path, PathBuf};

use ::console::{style, Emoji};
use anyhow::{anyhow, bail, Result};
use aws_sdk_ec2 as ec2;
use aws_types::credentials::future::ProvideCredentials as ProvideCredentialsFut;
use aws_types::credentials::{CredentialsError, ProvideCredentials};
use aws_types::region::Region;
use aws_types::Credentials;
use clap::Parser;
use ec2::model::Filter;
use futures::stream::{FuturesUnordered, StreamExt};
use indicatif::{MultiProgress, ProgressBar};
use ipnet::Ipv4Net;
use rusoto_credential::ProfileProvider;
use tokio::fs::DirBuilder;

mod aws;
mod deployment;
#[macro_use]
mod console;

use crate::console::{confirm, input, select, SPINNER_STYLE};
pub use crate::deployment::Deployment;
use crate::deployment::{CreateNew, Existing, MaybeExisting};

/// List of regions where we deploy AMIs.
///
/// Should match `destination_regions` in `//ops/image-deploy/locals.pkr.hcl`
const REGIONS: &[&str] = &["us-east-1", "us-east-2", "us-west-2"];

/// Minimum number of availability zones in which we can deploy a cluster
const MIN_AVAILABILITY_ZONES: usize = 3;

fn filter<K, V>(key: K, value: V) -> Filter
where
    K: Into<String>,
    V: Into<String>,
{
    Filter::builder().name(key).values(value).build()
}

/// Install and configure a ReadySet cluster in AWS
#[derive(Parser)]
struct Options {
    /// Directory to store state between runs. Defaults to `$XDG_STATE_HOME/readyset`.
    state_directory: Option<PathBuf>,
}

impl Options {
    fn state_directory(&self) -> Result<Cow<Path>> {
        if let Some(state_directory) = &self.state_directory {
            return Ok(Cow::Borrowed(state_directory));
        }

        let mut state_home = match env::var("XDG_STATE_HOME") {
            Ok(state_home) => PathBuf::from(state_home),
            Err(_) => {
                let home = env::var("HOME")
                    .map_err(|_| anyhow!("HOME not set, and --state-directory not passed"))?;
                let mut path = PathBuf::from(home);
                path.push(".local");
                path.push("state");
                path
            }
        };

        state_home.push("readyset");

        Ok(Cow::Owned(state_home))
    }
}

#[derive(Debug)]
struct RusotoAWSWrapper<T>(T);

impl<T> ProvideCredentials for RusotoAWSWrapper<T>
where
    T: rusoto_credential::ProvideAwsCredentials + Send + Sync + Debug,
{
    fn provide_credentials<'a>(&'a self) -> ProvideCredentialsFut<'a>
    where
        Self: 'a,
    {
        ProvideCredentialsFut::new(async {
            let creds = rusoto_credential::ProvideAwsCredentials::credentials(&self.0)
                .await
                .map_err(|e| CredentialsError::provider_error(Box::new(e)))?;
            Ok(Credentials::new(
                creds.aws_access_key_id(),
                creds.aws_secret_access_key(),
                creds.token().clone(),
                creds.expires_at().as_ref().map(|t| (*t).into()),
                "profile",
            ))
        })
    }
}

struct Installer {
    options: Options,
    deployment: Deployment,

    // Runtime state
    aws_config: Option<aws_config::Config>,
    ec2_client: Option<ec2::Client>,
}

impl Installer {
    fn new(options: Options, deployment: Deployment) -> Self {
        Self {
            options,
            deployment,
            aws_config: None,
            ec2_client: None,
        }
    }

    /// Save this installer's deployment to the configured state directory
    pub async fn save(&self) -> Result<()> {
        self.deployment
            .save_to_directory(self.options.state_directory()?)
            .await
    }

    /// Run the install process, picking up where the user left off if necessary
    pub async fn run(&mut self) -> Result<()> {
        self.save().await?;

        self.load_aws_config().await?;
        self.save().await?;

        if let Some(vpc_id) = &self.deployment.vpc_id {
            match vpc_id {
                Existing(vpc_id) => println!("Using existing AWS VPC: {}", style(vpc_id).bold()),
                CreateNew => println!("Deploying to a {} AWS VPC", style("new").bold()),
            }
        } else {
            self.prompt_for_vpc().await?;
        };
        self.save().await?;

        if let Existing(vpc_id) = self.deployment.vpc_id.clone().unwrap() {
            self.validate_vpc(vpc_id).await?;
        }

        Ok(())
    }

    async fn validate_vpc(&mut self, mut vpc_id: String) -> Result<()> {
        // First, check that the VPC actually exists
        let vpcs = self
            .ec2_client()
            .await?
            .describe_vpcs()
            .filters(filter("vpc-id", &vpc_id))
            .send()
            .await?;
        let vpc_cidr: Ipv4Net = if let Some(vpc) = vpcs.vpcs.unwrap_or_default().first() {
            vpc.cidr_block()
                .ok_or_else(|| anyhow!("VPC with id {} doesn't have any CIDR blocks", vpc_id))?
                .parse()?
        } else {
            println!(
                "The previously configured VPC {} no longer exists, or you're missing the required \
                 permissions to view it",
                style(&vpc_id).bold()
            );
            if confirm()
                .with_prompt("Would you like to change the configured VPC?")
                .interact()?
            {
                self.prompt_for_vpc().await?;
                self.save().await?;

                match self.deployment.vpc_id.as_ref().unwrap() {
                    CreateNew => return Ok(()),
                    Existing(new_vpc_id) => {
                        vpc_id = new_vpc_id.clone();
                        self.ec2_client()
                            .await?
                            .describe_vpcs()
                            .filters(filter("vpc-id", &vpc_id))
                            .send()
                            .await?
                            .vpcs
                            .unwrap()
                            .first()
                            .unwrap()
                            .cidr_block()
                            .ok_or_else(|| {
                                anyhow!("VPC with id {} doesn't have any CIDR blocks!", vpc_id)
                            })?
                            .parse()?
                    }
                }
            } else {
                bail!("Could not successfully access the configured VPC");
            }
        };

        let subnets = self
            .ec2_client()
            .await?
            .describe_subnets()
            .filters(filter("vpc-id", &vpc_id))
            .send()
            .await?
            .subnets
            .unwrap_or_default();

        let subnet_azs = &subnets
            .iter()
            .filter_map(|subnet| subnet.availability_zone())
            .collect::<HashSet<_>>();
        if subnet_azs.len() < MIN_AVAILABILITY_ZONES {
            let other_azs = self
                .ec2_client()
                .await?
                .describe_availability_zones()
                .filters(filter(
                    "region-name",
                    self.deployment.aws_region.as_ref().unwrap(),
                ))
                .send()
                .await?
                .availability_zones
                .unwrap_or_default()
                .into_iter()
                .filter_map(|az| az.zone_name)
                .filter(|zone_name| !subnet_azs.contains(zone_name.as_str()))
                .take(MIN_AVAILABILITY_ZONES - subnet_azs.len());

            println!(
                "ReadySet needs to deploy into at least 3 subnets in different availability zones"
            );
            println!(
                "The selected VPC {} only has the following subnets:",
                style(&vpc_id).bold(),
            );
            for subnet in &subnets {
                println!(
                    " • {}, in {}",
                    style(subnet.cidr_block().unwrap()).blue(),
                    style(subnet.availability_zone().unwrap()).bold()
                );
            }

            let new_cidrs = aws::subnets::subnet_cidrs(
                vpc_cidr,
                subnets
                    .iter()
                    .map(|subnet| {
                        Ok(subnet
                            .cidr_block()
                            .ok_or_else(|| {
                                anyhow!(
                                    "Subnet {} is missing a CIDR block!",
                                    subnet.subnet_id().unwrap()
                                )
                            })?
                            .parse()?)
                    })
                    .collect::<Result<Vec<Ipv4Net>>>()?,
                MIN_AVAILABILITY_ZONES,
            )?;

            let subnet_az_cidrs = other_azs.zip(new_cidrs).collect::<Vec<_>>();

            println!("If you like, I can automatically create the following new subnets:",);
            for (az, cidr) in &subnet_az_cidrs {
                println!(" • {}, in {}", style(cidr).blue(), style(az).bold());
            }

            if confirm()
                .with_prompt("Create subnets?")
                .default(true)
                .interact()?
            {
                self.create_subnets(vpc_id, subnet_az_cidrs).await?;
            } else {
                bail!(
                    "Please create subnets in the missing availability zones in your VPC, then \
                     re-run this installer to continue"
                );
            }
        } else {
            success!(
                "VPC has subnets in at least {} availability zones",
                MIN_AVAILABILITY_ZONES
            );
        }

        Ok(())
    }

    async fn create_subnets(
        &mut self,
        vpc_id: String,
        subnet_az_cidrs: Vec<(String, Ipv4Net)>,
    ) -> Result<()> {
        let multi_bar = MultiProgress::new();
        let ec2_client = self.ec2_client().await?;
        let mut futures = FuturesUnordered::new();
        for (az, cidr) in subnet_az_cidrs {
            let bar = multi_bar.add(
                ProgressBar::new_spinner()
                    .with_style(SPINNER_STYLE.clone())
                    .with_message(format!("Creating subnet {} in {}", cidr, az)),
            );
            bar.enable_steady_tick(50);
            let ec2 = ec2_client.clone();
            let vpc_id = vpc_id.to_owned();
            futures.push(async move {
                let res = ec2
                    .create_subnet()
                    .vpc_id(vpc_id)
                    .availability_zone(az)
                    .cidr_block(cidr.to_string())
                    .send()
                    .await;
                match &res {
                    Ok(subnet) => bar.finish_with_message(format!(
                        "{}Created {}",
                        style(Emoji("✔ ", "")).green(),
                        style(subnet.subnet().unwrap().subnet_id().unwrap()).bold()
                    )),
                    Err(e) => bar.finish_with_message(format!(
                        "{}Failed: {}",
                        style(Emoji("✘ ", "")).red(),
                        e
                    )),
                }
                res
            })
        }

        let mut err = None;
        while let Some(res) = futures.next().await {
            if let Err(e) = res {
                err = Some(e);
            }
        }

        multi_bar.join()?;

        if let Some(e) = err {
            Err(e.into())
        } else {
            Ok(())
        }
    }

    async fn prompt_for_vpc(&mut self) -> Result<MaybeExisting<&str>> {
        let vpc_id = if confirm()
            .with_prompt("Would you like to deploy to an existing AWS VPC?")
            .default(false)
            .interact()?
        {
            let vpcs = self.ec2_client().await?.describe_vpcs().send().await?;
            let vpcs = vpcs
                .vpcs()
                .into_iter()
                .flatten()
                .flat_map(|vpc| {
                    let vpc_id = vpc.vpc_id.as_ref()?;
                    let vpc_name = vpc
                        .tags()
                        .into_iter()
                        .flatten()
                        .find_map(|tag| {
                            if tag.key() == Some("Name") {
                                tag.value()
                            } else {
                                None
                            }
                        })
                        .unwrap_or(vpc_id);
                    Some((vpc_id, vpc_name))
                })
                .collect::<Vec<_>>();

            let vpc_names = vpcs.iter().map(|(_, name)| name).collect::<Vec<_>>();

            println!(
                "Found {} VPCs in {}",
                style(vpcs.len()).bold(),
                style(self.deployment.aws_region.as_ref().unwrap()).bold()
            );
            let idx = select()
                .with_prompt("Which VPC should we deploy to?")
                .items(&vpc_names)
                .interact()?;

            Existing(vpcs[idx].0.to_owned())
        } else {
            println!("OK, we'll create a new AWS VPC as part of the deployment process");
            CreateNew
        };

        Ok(self.deployment.vpc_id.insert(vpc_id).as_deref())
    }

    async fn ec2_client(&mut self) -> Result<&ec2::Client> {
        if self.ec2_client.is_none() {
            self.init_ec2_client().await?;
        }
        Ok(self.ec2_client.as_ref().unwrap())
    }

    async fn init_ec2_client(&mut self) -> Result<&ec2::Client> {
        let ec2_client = ec2::Client::new(self.aws_config().await?);
        Ok(self.ec2_client.insert(ec2_client))
    }

    async fn aws_config(&mut self) -> Result<&aws_config::Config> {
        if self.aws_config.is_none() {
            self.load_aws_config().await?;
        }
        Ok(self.aws_config.as_ref().unwrap())
    }

    async fn load_aws_config(&mut self) -> Result<&aws_config::Config> {
        let mut loader = aws_config::from_env();

        let profile =
            if let Some(aws_credentials_profile) = &self.deployment.aws_credentials_profile {
                println!(
                    "Using AWS profile: {}",
                    style(aws_credentials_profile).bold()
                );
                aws_credentials_profile
            } else {
                self.prompt_for_aws_credentials_profile()?
            };

        loader = loader.credentials_provider(RusotoAWSWrapper(
            ProfileProvider::with_default_credentials(profile)?,
        ));

        let region = if let Some(aws_region) = &self.deployment.aws_region {
            println!("Using AWS region: {}", style(aws_region).bold());
            aws_region
        } else {
            self.prompt_for_aws_region()?
        };

        loader = loader.region(Region::new(region.to_owned()));

        let config = loader.load().await;

        success!("Loaded AWS credentials");

        Ok(self.aws_config.insert(config))
    }

    fn prompt_for_aws_credentials_profile(&mut self) -> Result<&str> {
        println!(
            "We'll use your AWS credentials (stored in {}) throughout the install process.",
            style("~/.aws/credentials").bold()
        );

        let profile = input()
            .with_prompt("Which AWS profile should we use?")
            .default("default".to_owned())
            .validate_with(|input: &String| {
                match ProfileProvider::with_default_credentials(input) {
                    Ok(_) => Ok(()),
                    Err(e) => Err(e.message),
                }
            })
            .interact_text()?;
        println!();

        Ok(self.deployment.aws_credentials_profile.insert(profile))
    }

    fn prompt_for_aws_region(&mut self) -> Result<&str> {
        let mut prompt = select();
        prompt.with_prompt("Which AWS region should we deploy to?");
        prompt.items(REGIONS);

        if let Ok(default_region) = env::var("AWS_DEFAULT_REGION") {
            if let Some(idx) = REGIONS.iter().position(|r| r == &default_region) {
                prompt.default(idx);
            }
        }

        let idx = prompt.interact()?;

        Ok(self.deployment.aws_region.insert(REGIONS[idx].to_owned()))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let options = Options::parse();

    println!("\n{}\n", style("Welcome to the ReadySet Installer!").bold());

    DirBuilder::new()
        .recursive(true)
        .create(options.state_directory()?)
        .await?;

    let deployment = deployment::create_or_load_existing(options.state_directory()?).await?;
    let mut installer = Installer::new(options, deployment);

    installer.run().await
}
