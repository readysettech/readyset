use std::path::Path;
use std::process::Stdio;

use ::console::style;
use anyhow::{anyhow, bail, Result};
use tokio::fs::{remove_file, File};
use tokio::io::AsyncWriteExt;
use tokio::join;
use tokio::process::Command;

use crate::console::spinner;
use crate::constants::{
    READYSET_MYSQL_ADAPTER_FILE_PREFIX, READYSET_PSQL_ADAPTER_FILE_PREFIX,
    READYSET_SERVER_FILE_PREFIX, READYSET_TAG, READYSET_URL_PREFIX,
};
use crate::deployment::{
    Deployment, DeploymentData, DeploymentStatus, DockerComposeDeployment, Engine, MigrationMode,
};
use crate::docker_compose::Compose;
use crate::template::{mysql_adapter_img, postgres_adapter_img, server_img};
use crate::utils::{check_command_installed, run_docker_compose};
use crate::Options;

pub struct ComposeInstaller<'a> {
    options: &'a mut Options,
    deployment: &'a mut Deployment,
}

impl<'a> ComposeInstaller<'a> {
    pub fn new(options: &'a mut Options, deployment: &'a mut Deployment) -> Self {
        Self {
            options,
            deployment,
        }
    }

    /// Save this installer's deployment to the configured state directory
    async fn save(&self) -> Result<()> {
        self.deployment
            .save_to_directory(self.options.state_directory()?)
            .await
    }

    pub async fn tear_down<P: AsRef<Path>>(state_directory: P, name: &str) -> Result<()> {
        let path = Deployment::compose_path(state_directory, name);
        if !path.exists() {
            // File doesn't exist so there's nothing to tear down here.
            return Ok(());
        }
        let path_str = path
            .to_str()
            .ok_or_else(|| anyhow!("Path does not contain valid unicode characters"))?;
        run_docker_compose(["-f", path_str, "down", "-v", "--rmi", "all"]).await?;

        remove_file(path).await?;

        Ok(())
    }

    async fn write_config_file<P>(&mut self, base_yml: &str, destination_path: P) -> Result<()>
    where
        P: AsRef<Path>,
    {
        let absolute_path = self
            .options
            .state_directory()?
            .join(destination_path.as_ref());

        tokio::fs::create_dir_all(&absolute_path.parent().unwrap()).await?;

        let mut file = File::create(&absolute_path).await?;
        file.write_all(base_yml.as_bytes()).await?;

        Ok(())
    }

    pub async fn check_docker_installed_and_running(&mut self) -> Result<()> {
        check_command_installed("Docker", Command::new("docker").args(["--version"])).await?;

        let running_output = Command::new("docker").args(["ps"]).output().await?;

        if !running_output.status.success() {
            bail!("Please start Docker before continuing.");
        }

        Ok(())
    }

    pub async fn check_docker_compose_installed(&mut self) -> Result<()> {
        match check_command_installed(
            "Docker Compose",
            Command::new("docker-compose").args(["--version"]),
        )
        .await
        {
            Ok(()) => Ok(()),
            Err(_) => {
                check_command_installed(
                    "Docker Compose",
                    Command::new("docker").args(["compose", "--version"]),
                )
                .await?;
                Ok(())
            }
        }
    }

    async fn create_prometheus_configs(&mut self) -> Result<()> {
        self.write_config_file(
            include_str!("./templates/base_prometheus.yml"),
            "compose/prometheus/prometheus.yml",
        )
        .await
    }

    async fn create_grafana_configs(&mut self) -> Result<()> {
        let datasources_yml = include_str!("./templates/grafana_datasources.yml");

        self.write_config_file(
            include_str!("./templates/grafana_config.ini"),
            "compose/grafana/config/grafana.ini",
        )
        .await?;
        self.write_config_file(
            include_str!("./templates/grafana_dashboards.yml"),
            "compose/grafana/provisioning/dashboards/default.yaml",
        )
        .await?;

        let grafana_provisioning_datasources_dir = self
            .options
            .state_directory()?
            .join("compose")
            .join("grafana")
            .join("provisioning")
            .join("datasources");
        tokio::fs::create_dir_all(&grafana_provisioning_datasources_dir).await?;

        let datasources_provisioning_path =
            grafana_provisioning_datasources_dir.join("default.yaml");
        let mut file = File::create(&datasources_provisioning_path).await?;

        // replace database credentials to configure datasource correctly
        let datasources_yml = &datasources_yml.replace("$db-name", self.deployment.name());

        let adapter_port = self
            .compose_deployment()?
            .adapter_port
            .as_ref()
            .unwrap()
            .to_string();
        let datasources_yml = &datasources_yml.replace("$adapter-port", &adapter_port);

        let db_pass = self
            .compose_deployment()?
            .mysql_db_root_pass
            .as_ref()
            .unwrap();
        let datasources_yml = &datasources_yml.replace("$password", db_pass);

        file.write_all(datasources_yml.as_bytes()).await?;

        Ok(())
    }

    async fn create_grafana_dashboards(&mut self) -> Result<()> {
        self.write_config_file(
            include_str!("./templates/grafana_connected.json"),
            "compose/grafana/dashboards/connected.json",
        )
        .await?;
        self.write_config_file(
            include_str!("./templates/grafana_overview.json"),
            "compose/grafana/dashboards/query_overview.json",
        )
        .await?;
        self.write_config_file(
            include_str!("./templates/grafana_specific.json"),
            "compose/grafana/dashboards/query_specific.json",
        )
        .await?;

        Ok(())
    }

    async fn create_vector_configs(&mut self) -> Result<()> {
        self.write_config_file(
            include_str!("./templates/vector_agent.toml"),
            "compose/vector/agent.toml",
        )
        .await?;

        let aggregator_file_path = self
            .options
            .state_directory()?
            .join("compose")
            .join("vector")
            .join("aggregator.toml");

        let mut file = File::create(&aggregator_file_path).await?;

        let aggregator_yml = include_str!("./templates/vector_aggregator.toml");
        let deployment_name = self
            .compose_deployment()?
            .mysql_db_name
            .as_ref()
            .unwrap()
            .to_string();

        let aggregator_yml = &aggregator_yml.replace("$deployment", &deployment_name);
        file.write_all(aggregator_yml.as_bytes()).await?;

        Ok(())
    }

    /// Downloads necessary docker images needed for local deployment.
    async fn download_and_load_docker_images(&mut self, engine: Engine) -> Result<()> {
        let download_spinner =
            spinner().with_message(format!("{}", style("Downloading Docker images").bold()));
        let server_fut = reqwest::get(readyset_server_url());
        let adapter_fut = match engine {
            Engine::MySQL => reqwest::get(readyset_mysql_adapter_url()),
            Engine::PostgreSQL => reqwest::get(readyset_psql_adapter_url()),
        };
        // TODO(peter): Consider chunking these downloads, as they may be large.
        let (server_res, adapter_res) = join!(server_fut, adapter_fut);
        let server_contents = server_res?.bytes().await?;
        let adapter_contents = adapter_res?.bytes().await?;
        download_spinner.finish_with_message(format!(
            "{}",
            style("Finished downloading Docker images").bold()
        ));

        let (saved_adapter_img_name, new_adapter_img_name) = match engine {
            Engine::MySQL => (
                format!("readyset-mysql:{}", READYSET_TAG),
                mysql_adapter_img(),
            ),
            Engine::PostgreSQL => (
                format!("readyset-psql:{}", READYSET_TAG),
                postgres_adapter_img(),
            ),
        };

        let load_spinner =
            spinner().with_message(format!("{}", style("Loading Docker images").bold()));

        load_and_tag(
            server_contents.as_ref(),
            &format!("readyset-server:{}", READYSET_TAG),
            &server_img(),
        )
        .await?;

        load_and_tag(
            adapter_contents.as_ref(),
            &saved_adapter_img_name,
            &new_adapter_img_name,
        )
        .await?;

        load_spinner.finish_with_message(format!(
            "{}",
            style("Finished loading Docker images").bold()
        ));

        Ok(())
    }

    /// Run the install process for deploying locally using docker-compose, picking up where the
    /// user left off if necessary.
    pub async fn run(&mut self) -> Result<()> {
        self.check_docker_installed_and_running().await?;
        self.check_docker_compose_installed().await?;

        let res = self._compose_user_input();
        self.save().await?;
        res?;

        self.download_and_load_docker_images(self.deployment.db_type)
            .await?;

        let compose = Compose::try_from(&*self.deployment)?;

        let path =
            Deployment::compose_path(self.options.state_directory()?, self.deployment.name());
        tokio::fs::create_dir_all(&path.parent().unwrap()).await?;
        let path_str = path
            .to_str()
            .ok_or_else(|| anyhow!("Path does not contain valid unicode characters"))?;

        let mut file = File::create(&path).await?;
        file.write_all(&serde_yaml::to_vec(&compose)?).await?;
        let dest_text = format!("Docker Compose file was saved to {}", &path_str);
        println!("{}", style(dest_text).bold());

        self.create_prometheus_configs().await?;
        self.create_vector_configs().await?;
        self.create_grafana_configs().await?;
        self.create_grafana_dashboards().await?;

        println!("Deploying with Docker Compose now");
        run_docker_compose([
            "-f",
            path_str,
            "up",
            "-d",
            "--renew-anon-volumes",
            "--remove-orphans",
        ])
        .await?;

        self.deployment.status = DeploymentStatus::Complete;
        self.save().await?;

        println!("ReadySet should be available in a few seconds.");
        self.deployment.print_connection_information()?;

        Ok(())
    }

    /// Exists so if any step fails we save before returning error.
    fn _compose_user_input(&mut self) -> Result<()> {
        let deployment_name = self.deployment.name().to_owned();
        let db_type = self.deployment.db_type;
        self.compose_deployment()?
            .set_db_name(deployment_name)?
            .set_db_password()?
            .set_migration_mode(MigrationMode::Explicit)?
            .set_adapter_port(db_type)?;
        Ok(())
    }

    /// Returns a DockerComposeDeployment if the inner deployment type matches, otherwise returns an
    /// error. This should only be used if you know for sure the deployment type is already a
    /// DockerComposeDeployment.
    fn compose_deployment(&mut self) -> Result<&mut DockerComposeDeployment> {
        match self.deployment.inner {
            DeploymentData::Compose(ref mut c) => Ok(c),
            _ => {
                // This should be unreachable in practice.
                bail!("Should not have run Docker Compose functionality unless our deployment type was Docker Compose.")
            }
        }
    }
}

// TODO(peter): Consider switching over to bollard over shelling out to docker directly.
/// Loads the docker image and re-tags it based on the provided pre and post tags.
async fn load_and_tag(container: &[u8], old_name: &str, new_name: &str) -> Result<()> {
    let mut process = Command::new("docker")
        .args(["load"])
        .stdin(Stdio::piped())
        .spawn()?;

    let mut stdin = process.stdin.take().unwrap();
    stdin.write_all(container).await?;
    drop(stdin);

    let out = process.wait().await?;

    if !out.success() {
        bail!("Failed to load Docker image {}", old_name);
    }

    let out = Command::new("docker")
        .args(["tag", old_name, new_name])
        .output()
        .await?;

    if !out.status.success() {
        bail!("Failed to retag {}", old_name);
    }

    Ok(())
}

fn readyset_server_file() -> String {
    format!("{}-{}.tar.gz", READYSET_SERVER_FILE_PREFIX, READYSET_TAG)
}

fn readyset_mysql_adapter_file() -> String {
    format!(
        "{}-{}.tar.gz",
        READYSET_MYSQL_ADAPTER_FILE_PREFIX, READYSET_TAG
    )
}

fn readyset_psql_adapter_file() -> String {
    format!(
        "{}-{}.tar.gz",
        READYSET_PSQL_ADAPTER_FILE_PREFIX, READYSET_TAG
    )
}

fn readyset_server_url() -> String {
    format!("{}{}", READYSET_URL_PREFIX, readyset_server_file(),)
}

fn readyset_mysql_adapter_url() -> String {
    format!("{}{}", READYSET_URL_PREFIX, readyset_mysql_adapter_file(),)
}

fn readyset_psql_adapter_url() -> String {
    format!("{}{}", READYSET_URL_PREFIX, readyset_psql_adapter_file(),)
}
