use std::borrow::Cow;
use std::env;
use std::fmt::{Debug, Display};
use std::path::{Path, PathBuf};
use std::str::FromStr;

use anyhow::{anyhow, Result};
use aws_sdk_ec2 as ec2;
use aws_types::credentials::future::ProvideCredentials as ProvideCredentialsFut;
use aws_types::credentials::{CredentialsError, ProvideCredentials};
use aws_types::region::Region;
use aws_types::Credentials;
use clap::Parser;
use console::{style, Emoji};
use dialoguer::theme::ColorfulTheme;
use dialoguer::{Confirm, Input, Select};
use lazy_static::lazy_static;
use rusoto_credential::ProfileProvider;
use serde::{Deserialize, Serialize};
use tokio::fs::{read_dir, DirBuilder, File};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

lazy_static! {
    static ref DIALOG_THEME: ColorfulTheme = ColorfulTheme::default();
}

/// List of regions where we deploy AMIs.
///
/// Should match `destination_regions` in `//ops/image-deploy/locals.pkr.hcl`
const REGIONS: &[&str] = &["us-east-1", "us-east-2", "us-west-2"];

fn confirm() -> Confirm<'static> {
    let mut confirm = Confirm::with_theme(&*DIALOG_THEME);
    confirm.wait_for_newline(true);
    confirm
}

fn input<T>() -> Input<'static, T>
where
    T: Clone + FromStr + Display,
    T::Err: Display + Debug,
{
    Input::with_theme(&*DIALOG_THEME)
}

fn select() -> Select<'static> {
    Select::with_theme(&*DIALOG_THEME)
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

/// `#[serde(with = "...")]`-compatible module for serializing an option as either the string
/// "unset", or a value, rather than using `null` for [`None`].
///
/// In the fields of [`Deployment`], this allows explicitly differentiating between unset values and
/// set-to-null values.
mod maybe_set {
    use std::fmt;
    use std::marker::PhantomData;

    use serde::de::value::BorrowedStrDeserializer;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<T, S>(val: &Option<T>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        T: Serialize,
    {
        match val {
            Some(val) => val.serialize(serializer),
            None => serializer.serialize_str("unset"),
        }
    }

    pub fn deserialize<'de, T, D>(deserializer: D) -> Result<Option<T>, D::Error>
    where
        D: Deserializer<'de>,
        T: Deserialize<'de>,
    {
        struct UnsetVisitor<T> {
            _marker: PhantomData<T>,
        }

        impl<'de, T> serde::de::Visitor<'de> for UnsetVisitor<T>
        where
            T: Deserialize<'de>,
        {
            type Value = Option<T>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                write!(formatter, "the string \"unset\", or a value")
            }

            fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: Deserializer<'de>,
            {
                T::deserialize(deserializer).map(Some)
            }

            fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                if v == "unset" {
                    return Ok(None);
                }
                T::deserialize(BorrowedStrDeserializer::new(v)).map(Some)
            }
        }

        deserializer.deserialize_any(UnsetVisitor {
            _marker: PhantomData,
        })
    }
}

/// A (potentially partially-completed) deployment of a readyset cluster
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct Deployment {
    name: String,

    #[serde(with = "maybe_set", default)]
    aws_credentials_profile: Option<String>,

    #[serde(with = "maybe_set", default)]
    aws_region: Option<String>,

    /// VPC to deploy to. If Some(None), will create a new VPC as part of the deployment process.
    #[serde(with = "maybe_set", default)]
    vpc_id: Option<Option<String>>,
}

impl Deployment {
    /// Create a new Deployment with the given name
    pub fn new<S>(name: S) -> Self
    where
        S: Into<String>,
    {
        Deployment {
            name: name.into(),
            ..Default::default()
        }
    }

    /// Returns a reference to the name of the given deployment
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Save this deployment to the path in the given state directory
    pub async fn save_to_directory<P>(&self, dir: P) -> Result<()>
    where
        P: AsRef<Path>,
    {
        let path = dir.as_ref().join(self.name());
        let mut file = File::create(path).await?;
        file.write_all(&serde_json::to_vec(self)?).await?;
        Ok(())
    }

    /// Load the Deployment with the given name from the given state directory
    pub async fn load<P, N>(state_dir: P, name: N) -> Result<Self>
    where
        P: AsRef<Path>,
        N: AsRef<str>,
    {
        let path = state_dir.as_ref().join(name.as_ref());
        let mut file = File::open(path).await?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf).await?;
        Ok(serde_json::from_slice::<Self>(&buf)?)
    }

    /// Returns a list of all available deployments in the given state directory
    pub async fn list<P>(dir: P) -> Result<Vec<String>>
    where
        P: AsRef<Path>,
    {
        let mut dir = read_dir(dir).await?;
        let mut res = Vec::new();
        while let Some(entry) = dir.next_entry().await? {
            if entry.file_type().await?.is_file() {
                if let Ok(name) = entry.file_name().into_string() {
                    res.push(name);
                }
            }
        }
        Ok(res)
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

        let _vpc_id = if let Some(vpc_id) = &self.deployment.vpc_id {
            match vpc_id {
                Some(vpc_id) => println!("Using existing AWS VPC: {}", style(vpc_id).bold()),
                None => println!("Deploying to a {} AWS VPC", style("new").bold()),
            }
            vpc_id.as_deref()
        } else {
            self.prompt_for_vpc().await?
        };
        self.save().await?;

        Ok(())
    }

    async fn prompt_for_vpc(&mut self) -> Result<Option<&str>> {
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

            Some(vpcs[idx].0.to_owned())
        } else {
            println!("OK, we'll create a new AWS VPC as part of the deployment process");
            None
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

        println!(
            "\n{} {}\n",
            style(Emoji("✔", "")).green(),
            style("Loaded AWS credentials").bold(),
        );

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

fn prompt_for_and_create_deployment() -> Result<Deployment> {
    let deployment_name: String = input().with_prompt("Deployment name").interact_text()?;
    Ok(Deployment::new(deployment_name))
}

async fn select_deployment<P>(
    state_dir: P,
    mut deployments: Vec<String>,
) -> Result<Option<Deployment>>
where
    P: AsRef<Path>,
{
    let index = match select()
        .with_prompt("Select a deployment")
        .items(&deployments)
        .interact_opt()?
    {
        Some(idx) => idx,
        None => return Ok(None),
    };

    Ok(Some(
        Deployment::load(state_dir, deployments.remove(index)).await?,
    ))
}

async fn create_or_load_existing_deployment<P>(state_dir: P) -> Result<Deployment>
where
    P: AsRef<Path>,
{
    let deployments = Deployment::list(state_dir.as_ref()).await?;
    let deployment = match deployments.as_slice() {
        [] => {
            println!("To get started, enter a name for your ReadySet deployment.");
            println!(
                "We'll use this name to save your progress during the ReadySet cluster setup process.\n"
            );
            Some(prompt_for_and_create_deployment()?)
        }
        [deployment] => {
            println!(
                "I found an existing deployment named {}\n",
                style(&deployment).bold()
            );
            if confirm()
                .with_prompt("Would you like to continue where you left off?")
                .default(true)
                .wait_for_newline(true)
                .interact()?
            {
                Some(Deployment::load(state_dir.as_ref(), deployment).await?)
            } else {
                None
            }
        }
        _ => {
            println!("I found multiple existing deployments.");
            println!(
                "Would you like to continue where we left off with one of those deployments?\n"
            );
            select_deployment(state_dir, deployments).await?
        }
    };

    if let Some(deployment) = deployment {
        println!();
        Ok(deployment)
    } else {
        println!("\nOk, we'll create a new deployment\n");
        prompt_for_and_create_deployment()
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

    let deployment = create_or_load_existing_deployment(options.state_directory()?).await?;
    let mut installer = Installer::new(options, deployment);

    installer.run().await
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    #[tokio::test]
    async fn deployment_save_and_load() {
        let state_dir = TempDir::new().unwrap();
        let deployment = Deployment::new("deployment_save_and_load");
        deployment
            .save_to_directory(state_dir.path())
            .await
            .unwrap();
        let res = Deployment::load(state_dir.path(), "deployment_save_and_load")
            .await
            .unwrap();
        assert_eq!(res, deployment);
    }

    #[tokio::test]
    async fn list_deployments() {
        let state_dir = TempDir::new().unwrap();
        assert!(Deployment::list(state_dir.path()).await.unwrap().is_empty());

        Deployment::new("list_deployments_1")
            .save_to_directory(state_dir.path())
            .await
            .unwrap();
        assert_eq!(
            Deployment::list(state_dir.path()).await.unwrap(),
            vec!["list_deployments_1"]
        );

        Deployment::new("list_deployments_2")
            .save_to_directory(state_dir.path())
            .await
            .unwrap();
        assert_eq!(
            Deployment::list(state_dir.path()).await.unwrap(),
            vec!["list_deployments_1", "list_deployments_2"]
        );
    }
}
