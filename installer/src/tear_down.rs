use anyhow::{bail, Result};

use crate::deployment::{DeploymentData, DeploymentStatus};
use crate::Installer;

impl Installer {
    /// Tear down all resources for an already-created deployment
    pub async fn tear_down(&mut self) -> Result<()> {
        self.deployment.status = DeploymentStatus::TearingDown;
        self.save().await?;

        match self.deployment.inner {
            DeploymentData::Cloudformation(_) => self.tear_down_cfn().await,
            DeploymentData::Compose(_) => self.tear_down_compose().await,
        }
    }

    async fn tear_down_cfn(&self) -> Result<()> {
        bail!("Tearing down cloudformation deployments is not currently supported");
    }

    async fn tear_down_compose(&self) -> Result<()> {
        bail!("Tearing down docker-compose deployments is not currently supported");
    }
}
