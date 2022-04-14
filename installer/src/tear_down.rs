use anyhow::{anyhow, bail, Result};
use tokio::fs::remove_file;
use tokio::process::Command;

use crate::aws::cloudformation::{delete_stack, stack_exists};
use crate::console::confirm;
use crate::deployment::{DeploymentData, DeploymentStatus};
use crate::Installer;

impl Installer {
    /// Tear down all resources for an already-created deployment
    pub async fn tear_down(&mut self) -> Result<()> {
        self.deployment.status = DeploymentStatus::TearingDown;
        self.save().await?;

        match self.deployment.inner {
            DeploymentData::Cloudformation(_) => self.tear_down_cfn().await?,
            DeploymentData::Compose(_) => self.tear_down_compose().await?,
        }

        self.deployment
            .delete(self.options.state_directory()?)
            .await?;

        success!(
            "Deployment {} successfully torn down",
            self.deployment.name()
        );

        Ok(())
    }

    async fn tear_down_cfn(&mut self) -> Result<()> {
        for stack in [
            self.deployment.readyset_stack_name(),
            self.deployment.rds_stack_name(),
            self.deployment.consul_stack_name(),
            self.deployment.vpc_supplemental_stack_name(),
            self.deployment.vpc_stack_name(),
        ] {
            if stack_exists(self.cfn_client().await?, &stack).await? {
                if !confirm()
                    .with_prompt(format!("Delete stack {}?", stack))
                    .interact()?
                {
                    bail!("Exiting as requested")
                }

                delete_stack(self.cfn_client().await?, &stack).await?;
            }
        }

        Ok(())
    }

    async fn tear_down_compose(&self) -> Result<()> {
        if !confirm()
            .with_prompt("Tear down docker-compose deployment?")
            .interact()?
        {
            bail!("Exiting as requested");
        }

        let path = self
            .deployment
            .compose_path(self.options.state_directory()?);
        let path_str = path
            .to_str()
            .ok_or_else(|| anyhow!("Path does not contain valid unicode characters"))?;
        let status = Command::new("docker-compose")
            .args(["-f", path_str, "down", "-v", "--rmi", "all"])
            .status()
            .await?;
        if !status.success() {
            bail!("Command exited with {}", status);
        }

        remove_file(path).await?;

        Ok(())
    }
}
