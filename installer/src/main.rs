use std::borrow::Cow;
use std::env;
use std::fmt::{Debug, Display};
use std::path::{Path, PathBuf};
use std::str::FromStr;

use anyhow::{anyhow, Result};
use clap::Parser;
use console::style;
use dialoguer::theme::ColorfulTheme;
use dialoguer::{Confirm, Input, Select};
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use tokio::fs::{read_dir, DirBuilder, File};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

lazy_static! {
    static ref DIALOG_THEME: ColorfulTheme = ColorfulTheme::default();
}

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

/// A (potentially partially-completed) deployment of a readyset cluster
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Deployment {
    name: String,
}

impl Deployment {
    /// Create a new Deployment with the given name
    pub fn new<S>(name: S) -> Self
    where
        S: Into<String>,
    {
        Deployment { name: name.into() }
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
}

impl Installer {
    pub async fn save(&self) -> Result<()> {
        self.deployment
            .save_to_directory(self.options.state_directory()?)
            .await
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
    let installer = Installer {
        options,
        deployment,
    };
    installer.save().await?;
    Ok(())
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
