//! Defintions of things specific to how Substrate organizes Terraform modules.

use std::fmt;
use std::path::PathBuf;
use std::process::Command;
use std::str::FromStr;
use std::{fmt::Display, io::BufRead};

use anyhow::{bail, Result};
use serde::Serialize;
use tracing::error;

/// Reference to a root module
#[derive(Serialize, Debug)]
pub(crate) struct RootModule {
    path: PathBuf,
}

impl Display for RootModule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.path.display())
    }
}

impl RootModule {
    pub(crate) fn to_terraform_path(&self) -> Result<PathBuf> {
        let root_modules_path = root_modules_path()?;
        let terraform_path = root_modules_path.join(&self.path);
        if terraform_path.is_dir() {
            Ok(terraform_path)
        } else {
            bail!("root module {:?} does not exist", &self.path)
        }
    }

    // The output of the `substrate root-modules` command includes the prefix
    // `root-modules`. Since we also want to use it as input, remove the
    // `root-modules` prefix.
    fn from_substrate_output(s: &str) -> Self {
        let path = PathBuf::from(s)
            .strip_prefix("root-modules")
            .unwrap()
            .to_path_buf();
        Self { path }
    }
}

impl FromStr for RootModule {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let path = root_modules_path()?.join(PathBuf::from(s));
        if path.is_dir() {
            Ok(RootModule { path })
        } else {
            bail!("Could not find substrate root module {}", s)
        }
    }
}

fn root_path() -> Result<PathBuf> {
    let mut path: PathBuf;
    if let Ok(substrate_root) = std::env::var("SUBSTRATE_ROOT") {
        path = PathBuf::from(&substrate_root);
        if !path.is_absolute() {
            path = std::env::current_dir()?.join(path)
        }
    } else {
        path = std::env::current_dir()?.join("ops/substrate")
    }
    if !path.is_dir() {
        bail!("could not find substrate root")
    }
    Ok(path)
}

fn root_modules_path() -> Result<PathBuf> {
    let path = root_path()?.join("root-modules");
    if !path.is_dir() {
        bail!("could not find root-modules in substrate root");
    }
    Ok(path)
}

pub(crate) fn root_modules() -> Result<Vec<RootModule>> {
    let root = root_path()?;
    let output = Command::new("substrate")
        .arg("root-modules")
        .current_dir(&root)
        .output()?;
    if !output.status.success() {
        error!(
            stderr = %std::str::from_utf8(&output.stderr).unwrap(),
            stdout = %std::str::from_utf8(&output.stdout).unwrap(),
        );
        bail!(
            "Could not get root-modules from substrate (status {})",
            output.status.code().unwrap_or(0)
        )
    }
    let result: Result<Vec<String>, _> = output.stdout.lines().collect();
    match result {
        Ok(root_modules) => Ok(root_modules
            .iter()
            .map(|s| RootModule::from_substrate_output(s))
            // TODO(harleyk): Remove these filters once they are fixed up.
            // network/sandbox is using modules in Github which don't work in CI.
            // readyset/prod is in a weird half existing state.
            .filter(|r| {
                !(r.path.starts_with("network/sandbox") || r.path.starts_with("readyset/prod"))
            })
            .collect()),
        Err(_) => bail!("Could not parse substrate root-modules output"),
    }
}
