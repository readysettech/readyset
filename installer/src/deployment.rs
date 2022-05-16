use std::collections::HashMap;
use std::fmt::{self, Display};
use std::ops::Deref;
use std::path::{Path, PathBuf};

use ::console::style;
use anyhow::{anyhow, bail, Result};
use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;
use tokio::fs::{read_dir, remove_file, File};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
pub(crate) use MaybeExisting::{CreateNew, Existing};

use crate::compose_installer::ComposeInstaller;
use crate::console::{confirm, input, password, select};
use crate::constants::CFN_VERSION;
use crate::Options;

/// Used in build to match this installer a set of CFN templates.
const PAIRED_VERSION: Option<&str> = option_env!("READYSET_CFN_PREFIX");

const S3_PREFIX: &str = "https://readysettech-cfn-public-us-east-2.s3.amazonaws.com/";
const TEMPLATE_DIR: &str = "/readyset/templates/";

/// An enum encapsulating the user selection to either use an existing entity (represented by the
/// `T` type argument) or create a new one
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub(crate) enum MaybeExisting<T> {
    /// Create a new entity
    CreateNew,

    /// Use an existing entity
    Existing(T),
}

impl<T> From<Option<T>> for MaybeExisting<T> {
    fn from(opt: Option<T>) -> Self {
        match opt {
            Some(val) => Self::Existing(val),
            None => Self::CreateNew,
        }
    }
}

impl<T> From<MaybeExisting<T>> for Option<T> {
    fn from(val: MaybeExisting<T>) -> Self {
        match val {
            CreateNew => None,
            Existing(x) => Some(x),
        }
    }
}

impl<T> Default for MaybeExisting<T> {
    fn default() -> Self {
        Self::CreateNew
    }
}

impl<T> MaybeExisting<T> {
    /// Converts from `MaybeExisting<T>` (or `&MaybeExisting<T>`) to `MaybeExisting<&T::Target>`.
    ///
    /// Equivalent to [`Option::as_deref`]
    pub(crate) fn as_deref(&self) -> MaybeExisting<&T::Target>
    where
        T: Deref,
    {
        match self {
            CreateNew => CreateNew,
            Existing(x) => Existing(&*x),
        }
    }

    /// Returns `true` if this value is [`CreateNew`].
    ///
    /// [`CreateNew`]: MaybeExisting::CreateNew
    pub(crate) fn is_create_new(&self) -> bool {
        matches!(self, Self::CreateNew)
    }

    /// Returns `true` if this value is [`Existing`].
    ///
    /// [`Existing`]: MaybeExisting::Existing
    pub(crate) fn is_existing(&self) -> bool {
        matches!(self, Self::Existing(..))
    }

    pub(crate) fn as_existing(&self) -> Option<&T> {
        if let Self::Existing(v) = self {
            Some(v)
        } else {
            None
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, Arbitrary)]
pub enum Engine {
    MySQL,
    PostgreSQL,
}

impl Display for Engine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Engine::MySQL => f.write_str("MySQL"),
            Engine::PostgreSQL => f.write_str("PostgreSQL"),
        }
    }
}

impl Engine {
    pub(crate) fn select<S>(prompt: S) -> Result<Self>
    where
        S: Into<String>,
    {
        const ENGINES: &[Engine] = &[Engine::MySQL, Engine::PostgreSQL];
        let idx = select().with_prompt(prompt).items(ENGINES).interact()?;
        Ok(ENGINES[idx])
    }

    pub(crate) fn root_conn_string(
        &self,
        db_pass: &str,
        host: &str,
        port: u16,
        db_name: &str,
    ) -> String {
        let suffix = format!("{}@{}:{}/{}", db_pass, host, port, db_name);
        match self {
            Engine::MySQL => format!("mysql://root:{}", suffix),
            Engine::PostgreSQL => format!("postgresql://postgres:{}", suffix),
        }
    }

    pub(crate) fn from_aws_engine<S>(name: S) -> Result<Self>
    where
        S: AsRef<str>,
    {
        match name.as_ref() {
            "mysql" | "mariadb" => Ok(Self::MySQL),
            // TODO: check if we support aurora-postgresql out of the box
            "postgres" => Ok(Self::PostgreSQL),
            engine => bail!(
                "Unsupported database engine {}; ReadySet only supports mysql/mariadb or postgres",
                engine
            ),
        }
    }
}

/// Information about the RDS database to deploy in front of
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Arbitrary)]
pub(crate) struct RdsDb {
    pub(crate) db_id: MaybeExisting<String>,
    pub(crate) db_name: String,
    pub(crate) engine: Engine,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Arbitrary)]
pub(crate) struct DatabasePasswordParameter {
    pub(crate) kms_arn: String,
    pub(crate) ssm_path: String,
}

/// Credentials to use for ReadySet to connect to the RDS database
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Arbitrary)]
pub(crate) struct DatabaseCredentials {
    pub(crate) username: String,
    pub(crate) password: DatabasePasswordParameter,
}

/// The status of a deployment
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, Arbitrary)]
pub enum DeploymentStatus {
    /// The deployment is in-progress
    InProgress,
    /// The deployment has been completed, meaning the ReadySet cluster is running.
    Complete,
    /// The deployment is in the process of being torn down
    TearingDown,
}

impl Default for DeploymentStatus {
    fn default() -> Self {
        Self::InProgress
    }
}

/// A (potentially partially-completed) deployment of a readyset cluster.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct Deployment {
    pub(crate) name: String,
    pub(crate) db_type: Engine,
    pub(crate) inner: DeploymentData,
    pub(crate) status: DeploymentStatus,
}

/// Represents the different ways in which we may choose to deploy ReadySet.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Arbitrary)]
pub enum DeploymentData {
    Cloudformation(Box<CloudformationDeployment>),
    Compose(DockerComposeDeployment),
}

/// A (potentially partially-completed) deployment of a local readyset cluster.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct DockerComposeDeployment {
    pub(crate) name: Option<String>,

    pub(crate) migration_mode: Option<MigrationMode>,

    pub(crate) db_connection_string: Option<String>,

    pub(crate) mysql_db_name: Option<String>,

    pub(crate) mysql_db_root_pass: Option<String>,

    pub(crate) adapter_port: Option<u16>,
}

/// Whether the user wants to use the async migration feature, or the explicit migration feature.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Arbitrary)]
pub enum MigrationMode {
    Async,
    Explicit,
}

/// A (potentially partially-completed) deployment of a readyset CFN cluster.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct CloudformationDeployment {
    #[serde(default)]
    pub(crate) aws_credentials_profile: Option<String>,

    #[serde(default)]
    pub(crate) aws_region: Option<String>,

    /// VPC to deploy to
    #[serde(default)]
    pub(crate) vpc_id: Option<MaybeExisting<String>>,

    /// Subnet IDs to use for instances
    #[serde(default)]
    pub(crate) subnet_ids: Option<Vec<String>>,

    /// RDS database to deploy in front of
    #[serde(default)]
    pub(crate) rds_db: Option<RdsDb>,

    /// EC2 key pair name to use to allow the user to SSH into instances
    #[serde(default)]
    pub(crate) key_pair_name: Option<String>,

    /// Database credentials to use for ReadySet to connect to the RDS database
    #[serde(default)]
    pub(crate) database_credentials: Option<DatabaseCredentials>,

    /// Cloudformation stack outputs for the VPC supplemental stack
    #[serde(default)]
    pub(crate) vpc_supplemental_stack_outputs: Option<HashMap<String, String>>,

    /// Cloudformation stack outputs for the Consul stack
    #[serde(default)]
    pub(crate) consul_stack_outputs: Option<HashMap<String, String>>,

    /// Cloudformation stack outputs for the ReadySet stack
    #[serde(default)]
    pub(crate) readyset_stack_outputs: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TemplateType {
    Consul,
    VpcSupplemental,
    RdsMysql,
    RdsPostgres,
    Mysql,
    Postgres,
}

impl CloudformationDeployment {
    /// Create a new Deployment with the given name
    pub(crate) fn new_deployment<S>(name: S, db_type: Engine) -> Deployment
    where
        S: Into<String>,
    {
        Deployment {
            name: name.into(),
            db_type,
            inner: DeploymentData::Cloudformation(Box::new(CloudformationDeployment {
                ..Default::default()
            })),
            status: DeploymentStatus::InProgress,
        }
    }

    pub fn cloudformation_template_url(&self, t: TemplateType) -> String {
        let version_to_use = PAIRED_VERSION.unwrap_or(CFN_VERSION);
        let template_file = match t {
            TemplateType::VpcSupplemental => "readyset-vpc-supplemental-template.yaml",
            TemplateType::Consul => "readyset-authority-consul-template.yaml",
            TemplateType::RdsMysql => "readyset-rds-mysql-template.yaml",
            TemplateType::RdsPostgres => "readyset-rds-postgresql-template.yaml",
            TemplateType::Mysql => "readyset-mysql-template.yaml",
            TemplateType::Postgres => "readyset-postgresql-template.yaml",
        };
        format!(
            "{}{}{}{}",
            S3_PREFIX, version_to_use, TEMPLATE_DIR, template_file
        )
    }
}

impl DockerComposeDeployment {
    /// Create a new Deployment with the given name
    pub(crate) fn new_deployment<S>(name: S, db_type: Engine) -> Deployment
    where
        S: Into<String>,
    {
        Deployment {
            name: name.into(),
            db_type,
            inner: DeploymentData::Compose(DockerComposeDeployment {
                ..Default::default()
            }),
            status: DeploymentStatus::InProgress,
        }
    }

    /// Sets the underlying database name to the supplied name String.
    pub fn set_db_name(&mut self, name: String) -> Result<&mut DockerComposeDeployment> {
        if self.mysql_db_name.is_some() {
            return Ok(self);
        }
        self.mysql_db_name = Some(name);
        Ok(self)
    }

    pub fn set_db_connection_string(
        &mut self,
        db_type: Engine,
    ) -> Result<&mut DockerComposeDeployment> {
        if confirm()
            .with_prompt(format!("Create a new backing {db_type} database now? Selecting 'no' will provide an opportunity to supply an existing backing database instead"))
            .default(true)
            .wait_for_newline(true)
            .interact()?
        {
            return Ok(self);
        }
        println!("Gathering information needed to access an existing backing database");
        println!("Notes:");
        println!("* For compatibility, the backing DB might require certain configuraiton options to be set, or to be of certain versions");
        println!(
            "* During ReadySet initialization, expect a spike in read activity on the backing DB"
        );

        // Get credentials for upstream db
        let address: String = input()
            .with_prompt(format!(
                "Backing {db_type} address (use `host.docker.internal` for the Docker host)"
            ))
            .interact_text()?;
        let username: String = input()
            .with_prompt(format!("Backing {db_type} username"))
            .interact_text()?;
        let password = password()
            .with_prompt(format!("Backing {db_type} password"))
            .with_confirmation("Confirm password", "Passwords mismatching")
            .interact()?;

        let protocol = db_type.to_string().to_lowercase();
        let db_name = self.mysql_db_name.as_ref().ok_or_else(|| {
            anyhow!("Database name unset when attempting to create connection string")
        })?;
        self.db_connection_string = Some(format!(
            "{protocol}://{username}:{password}@{address}/{db_name}",
        ));

        Ok(self)
    }

    pub fn set_db_password(&mut self) -> Result<&mut DockerComposeDeployment> {
        if self.mysql_db_root_pass.is_some() {
            return Ok(self);
        }
        self.mysql_db_root_pass = Some(
            password()
                .with_prompt("Deployment password")
                .with_confirmation("Confirm password", "Passwords mismatching")
                .interact()?,
        );
        Ok(self)
    }

    pub fn set_migration_mode(
        &mut self,
        mode: MigrationMode,
    ) -> Result<&mut DockerComposeDeployment> {
        if self.migration_mode.is_some() {
            return Ok(self);
        }
        self.migration_mode = Some(mode);
        Ok(self)
    }

    pub(crate) fn set_adapter_port(
        &mut self,
        db_type: Engine,
    ) -> Result<&mut DockerComposeDeployment> {
        if self.adapter_port.is_some() {
            return Ok(self);
        }
        println!("Which port should ReadySet listen on?");
        self.adapter_port = Some(
            input()
                .with_prompt("ReadySet port")
                .default(match db_type {
                    Engine::MySQL => 3307,
                    Engine::PostgreSQL => 5433,
                })
                .interact_text()?,
        );
        Ok(self)
    }
}

impl Deployment {
    /// Returns a reference to the name of the given deployment
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns `true` if this deployment is [complete]
    ///
    /// [complete]: DeploymentStatus::Complete
    pub fn is_complete(&self) -> bool {
        self.status == DeploymentStatus::Complete
    }

    /// Returns `true` if this deployment is [being torn down]
    ///
    /// [being torn down]: DeploymentStatus::TearingDown
    pub(crate) fn is_tearing_down(&self) -> bool {
        self.status == DeploymentStatus::TearingDown
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

    /// Remove all data about this deployment from the given state directory
    pub async fn delete<P>(state_dir: P, name: &str) -> Result<()>
    where
        P: AsRef<Path>,
    {
        let path = state_dir.as_ref().join(name);
        if path.exists() {
            remove_file(path).await?;
        }

        Ok(())
    }

    pub(crate) fn print_connection_information(&self) -> Result<()> {
        match &self.inner {
            DeploymentData::Cloudformation(box CloudformationDeployment {
                readyset_stack_outputs: Some(outputs),
                ..
            }) => {
                println!(
                    "Use the following URL to connect to your ReadySet cluster:\n\n    {}",
                    style(&outputs["ReadySetAdapterNLBDNSName"]).bold()
                );
            }
            DeploymentData::Compose(DockerComposeDeployment {
                mysql_db_name: Some(db_name),
                mysql_db_root_pass: Some(db_pass),
                adapter_port: Some(db_port),
                ..
            }) => {
                let conn_string =
                    self.db_type
                        .root_conn_string(db_pass, "127.0.0.1", *db_port, db_name);
                let conn_cmd = match self.db_type {
                    Engine::MySQL => {
                        format!(
                            "Run the following command to connect to ReadySet via the MySQL client:\n\n    $ mysql -h127.0.0.1 -uroot -p{} -P{} --database={}\n\nTo connect to ReadySet using an application, use the following ReadySet connection string:\n\n    {}",
                            db_pass, db_port, db_name, conn_string
                        )
                    }
                    Engine::PostgreSQL => {
                        format!(
                            "Run the following command to connect to ReadySet via the PostgreSQL client:\n\n    $ psql {}\n\nTo connect to ReadySet using an application, use the following ReadySet connection string:\n\n    {}",
                            &conn_string, &conn_string
                        )
                    }
                };

                println!("{}", style(conn_cmd).bold());

                let dashboard_str = "\nAccess the ReadySet dashboard at 127.0.0.1:4000";
                println!("{}", style(dashboard_str).bold());
            }
            _ => bail!("Deployment missing required fields"),
        }

        Ok(())
    }

    fn cloudformation_stack_name<P>(&self, stack: P) -> String
    where
        P: Display,
    {
        format!("{}-{}", self.name(), stack)
    }

    pub(crate) fn readyset_stack_name(&self) -> String {
        self.cloudformation_stack_name("readyset")
    }

    pub(crate) fn rds_stack_name(&self) -> String {
        self.cloudformation_stack_name("rds")
    }

    pub(crate) fn consul_stack_name(&self) -> String {
        self.cloudformation_stack_name("consul")
    }

    pub(crate) fn vpc_supplemental_stack_name(&self) -> String {
        self.cloudformation_stack_name("vpc-supplemental")
    }

    pub(crate) fn vpc_stack_name(&self) -> String {
        self.cloudformation_stack_name("vpc")
    }

    pub(crate) fn compose_path<P>(state_dir: P, name: &str) -> PathBuf
    where
        P: AsRef<Path>,
    {
        state_dir
            .as_ref()
            .join("compose")
            .join(format!("{}.yml", name))
    }
}

fn prompt_for_and_create_deployment(full: bool) -> Result<Deployment> {
    let db_type = Engine::select(
        "ReadySet supports both the MySQL or Postgres wire protocols, select which one to use.\nChoose backing database:",
    )?;

    let deployment_name: String = input()
        .with_prompt("ReadySet deployment name")
        .interact_text()?;
    if full {
        match select()
            .with_prompt("How would you like to deploy ReadySet?")
            .items(&["[local] Docker Compose", "[remote] AWS Cloudformation"])
            .interact()?
        {
            0 => Ok(DockerComposeDeployment::new_deployment(
                deployment_name,
                db_type,
            )),
            1 => Ok(CloudformationDeployment::new_deployment(
                deployment_name,
                db_type,
            )),
            _ => bail!("Must choose a destination to deploy ReadySet to"),
        }
    } else {
        Ok(DockerComposeDeployment::new_deployment(
            deployment_name,
            db_type,
        ))
    }
}

async fn select_deployment<P>(
    state_dir: P,
    mut deployments: Vec<String>,
) -> Result<Option<Deployment>>
where
    P: AsRef<Path>,
{
    let index = match select()
        .with_prompt("Select a previous deployment:")
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

pub(crate) async fn prompt_for_existing_deployment<P>(state_dir: P) -> Result<Deployment>
where
    P: AsRef<Path>,
{
    let deployments = Deployment::list(state_dir.as_ref()).await?;
    select_deployment(state_dir, deployments)
        .await?
        .ok_or_else(|| anyhow!("No deployment selected"))
}

async fn tear_down_compose_deployments<P: AsRef<Path>>(
    state_dir: P,
    deployments: Vec<String>,
    exception: Option<&str>,
) -> Result<()> {
    if !confirm()
        .with_prompt("Tear down other deployments now?")
        .default(true)
        .wait_for_newline(true)
        .interact()?
    {
        bail!("Exiting. The ReadySet installer currently only supports one active deployment at a time.");
    }

    let tear_down_deployments = if let Some(deployment) = exception {
        deployments
            .into_iter()
            .filter(|d| d != deployment)
            .collect()
    } else {
        deployments
    };
    for deployment in tear_down_deployments {
        println!("Tearing down deployment {}", &deployment);
        ComposeInstaller::tear_down(state_dir.as_ref(), &deployment).await?;
        Deployment::delete(state_dir.as_ref(), &deployment).await?;
    }
    Ok(())
}

/// Creates a new deployment, or allows a customer to load an existing deployment. In the case that
/// they create a new deployment, it tears down all deployments. In the case that they load an
/// existing deployment, it tears down all deployments except for the selected deployment.
pub(crate) async fn create_or_load_existing(options: &Options) -> Result<Deployment> {
    let (state_dir, full) = (options.state_directory()?, options.full);
    let mut tear_down_all = false;
    let deployments = Deployment::list(state_dir.as_ref()).await?;
    let deployment = match deployments.as_slice() {
        [] => {
            println!("Enter a name for your ReadySet deployment.");
            Some(prompt_for_and_create_deployment(options.full)?)
        }
        [deployment] => {
            println!(
                "I found an existing deployment named {}. We can continue with this deployment, or \
                 create a new one.\n",
                style(&deployment).bold()
            );
            if confirm()
                .with_prompt("Would you like to continue with the existing deployment?")
                .default(true)
                .wait_for_newline(true)
                .interact()?
            {
                Some(Deployment::load(state_dir.as_ref(), deployment).await?)
            } else {
                tear_down_all = true;
                None
            }
        }
        _ => {
            tear_down_all = true;
            println!(
                "There are multiple existing deployments. We can continue with an existing \
                 deployment, or create a new one.\n "
            );
            if confirm()
                .with_prompt("Continue with an existing deployment?")
                .default(true)
                .wait_for_newline(true)
                .interact()?
            {
                select_deployment(state_dir.clone(), deployments.clone()).await?
            } else {
                None
            }
        }
    };
    if !full && tear_down_all {
        println!("Before proceeding we need to tear down all other deployments.");
        tear_down_compose_deployments(
            state_dir,
            deployments,
            deployment.as_ref().map(|d| d.name.as_str()),
        )
        .await?;
    }

    if let Some(deployment) = deployment {
        println!();
        Ok(deployment)
    } else {
        println!("\nOk, we'll create a new deployment\n");
        prompt_for_and_create_deployment(full)
    }
}

#[cfg(test)]
mod tests {
    use std::env;

    use tempfile::TempDir;
    use test_strategy::proptest;

    use super::*;

    #[tokio::test]
    async fn save_and_load() {
        let state_dir = TempDir::new().unwrap();
        let deployment = CloudformationDeployment::new_deployment("save_and_load", Engine::MySQL);
        deployment
            .save_to_directory(state_dir.path())
            .await
            .unwrap();
        let res = Deployment::load(state_dir.path(), "save_and_load")
            .await
            .unwrap();
        assert_eq!(res, deployment);
    }

    #[tokio::test]
    async fn list_deployments() {
        let state_dir = TempDir::new().unwrap();
        assert!(Deployment::list(state_dir.path()).await.unwrap().is_empty());

        CloudformationDeployment::new_deployment("list_deployments_1", Engine::MySQL)
            .save_to_directory(state_dir.path())
            .await
            .unwrap();
        assert_eq!(
            Deployment::list(state_dir.path()).await.unwrap(),
            vec!["list_deployments_1"]
        );

        CloudformationDeployment::new_deployment("list_deployments_2", Engine::MySQL)
            .save_to_directory(state_dir.path())
            .await
            .unwrap();
        let mut list = Deployment::list(state_dir.path()).await.unwrap();
        list.sort();
        assert_eq!(list, vec!["list_deployments_1", "list_deployments_2"]);
    }

    #[proptest]
    fn serialize_round_trip(deployment: CloudformationDeployment) {
        let serialized = serde_json::to_string(&deployment).unwrap();
        eprintln!("JSON: {}", serialized);
        let rt = serde_json::from_str::<CloudformationDeployment>(&serialized).unwrap();
        assert_eq!(rt, deployment);
    }

    #[test]
    fn cloudformation_template_url_fallback() {
        env::set_var("READYSET_CFN_PREFIX", "");
        let expected = format!("https://readysettech-cfn-public-us-east-2.s3.amazonaws.com/{}/readyset/templates/readyset-authority-consul-template.yaml", CFN_VERSION);
        let deployment =
            CloudformationDeployment::new_deployment("cloudformation_template_url", Engine::MySQL);
        let actual = if let DeploymentData::Cloudformation(cfn) = deployment.inner {
            cfn.cloudformation_template_url(TemplateType::Consul)
        } else {
            panic!("We are trying to generate a cloudformation template URL with an inner deployment type that is not DeploymentData::Cloudformation");
        };
        assert_eq!(expected, actual)
    }

    #[test]
    fn cloudformation_template_url_release() {
        env::set_var(
            "READYSET_CFN_PREFIX",
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz1234567890",
        ); // valid hash characters
        let expected = format!("https://readysettech-cfn-public-us-east-2.s3.amazonaws.com/{}/readyset/templates/readyset-authority-consul-template.yaml", CFN_VERSION);
        let deployment =
            CloudformationDeployment::new_deployment("cloudformation_template_url", Engine::MySQL);
        let actual = if let DeploymentData::Cloudformation(cfn) = deployment.inner {
            cfn.cloudformation_template_url(TemplateType::Consul)
        } else {
            panic!("We are trying to generate a cloudformation template URL with an inner deployment type that is not DeploymentData::Cloudformation");
        };
        assert_eq!(expected, actual)
    }
}
