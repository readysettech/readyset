use std::collections::HashMap;
use std::fmt::{self, Display};
use std::ops::Deref;
use std::path::Path;

use ::console::style;
use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;
use tokio::fs::{read_dir, File};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
pub(crate) use MaybeExisting::{CreateNew, Existing};

use crate::console::{confirm, input, select};

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
pub(crate) enum Engine {
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

/// A (potentially partially-completed) deployment of a readyset cluster
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct Deployment {
    pub(crate) name: String,

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

pub(crate) async fn create_or_load_existing<P>(state_dir: P) -> Result<Deployment>
where
    P: AsRef<Path>,
{
    let deployments = Deployment::list(state_dir.as_ref()).await?;
    let deployment = match deployments.as_slice() {
        [] => {
            println!("Enter a name for your ReadySet deployment.");
            Some(prompt_for_and_create_deployment()?)
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
                None
            }
        }
        _ => {
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
                select_deployment(state_dir, deployments).await?
            } else {
                None
            }
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

#[cfg(test)]
mod tests {
    use tempfile::TempDir;
    use test_strategy::proptest;

    use super::*;

    #[tokio::test]
    async fn save_and_load() {
        let state_dir = TempDir::new().unwrap();
        let deployment = Deployment::new("save_and_load");
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
        let mut list = Deployment::list(state_dir.path()).await.unwrap();
        list.sort();
        assert_eq!(list, vec!["list_deployments_1", "list_deployments_2"]);
    }

    #[proptest]
    fn serialize_round_trip(deployment: Deployment) {
        let serialized = serde_json::to_string(&deployment).unwrap();
        eprintln!("JSON: {}", serialized);
        let rt = serde_json::from_str::<Deployment>(&serialized).unwrap();
        assert_eq!(rt, deployment);
    }
}
