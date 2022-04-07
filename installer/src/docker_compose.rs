use std::collections::HashMap;

use anyhow::bail;
use derive_builder::*;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

use crate::deployment::{Deployment, DeploymentData, Engine, MigrationMode};
use crate::template::generate_base_template;

#[skip_serializing_none]
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Compose {
    version: Option<String>,
    pub services: Option<Services>,
    pub volumes: Option<TopLevelVolumes>,
    pub networks: Option<ComposeNetworks>,
    pub service: Option<Service>,
}

impl TryFrom<&Deployment> for Compose {
    type Error = anyhow::Error;
    fn try_from(value: &Deployment) -> Result<Self, Self::Error> {
        let (name, compose) = match &value.inner {
            DeploymentData::Compose(c) => (&value.name, c),
            _ => {
                bail!("Tried to convert a Deployment whose inner type was not a DeploymentType::Compose to a docker Compose type");
            }
        };
        match (
            &compose.mysql_db_name,
            &compose.mysql_db_root_pass,
            &compose.adapter_port,
            &compose.migration_mode,
        ) {
            (Some(db_name), Some(db_pass), Some(adapter_port), Some(migration_mode)) => {
                let mut default = generate_base_template(&value.db_type);
                default.fill_deployment(name);
                default.fill_credentials(&value.db_type, db_name, db_pass);
                default.fill_adapter_port(*adapter_port);
                default.fill_migration_mode(migration_mode);
                Ok(default)
            }
            _ => {
                bail!("Tried to convert to a Compose type, but missing one of mysql_db_name, mysql_db_root_pass, adapter_port, or migration_mode");
            }
        }
    }
}

impl Compose {
    pub fn fill_deployment(&mut self, deployment: &str) {
        if let Some(ref mut services) = self.services {
            services.set_service_env_var("readyset-server", "NORIA_DEPLOYMENT", deployment);
            services.set_service_env_var("readyset-adapter", "NORIA_DEPLOYMENT", deployment);
        }
    }

    /// Fills database name, root password, and replication urls for mysql upstream database.
    pub(crate) fn fill_credentials(&mut self, db_type: &Engine, db_name: &str, pass: &str) {
        if let Some(ref mut services) = self.services {
            let url = match db_type {
                Engine::MySQL => {
                    services.set_service_env_var("mysql", "MYSQL_DATABASE", db_name);
                    services.set_service_env_var("mysql", "MYSQL_ROOT_PASSWORD", pass);
                    format!("mysql://root:{}@mysql/{}", pass, db_name)
                }
                Engine::PostgreSQL => {
                    services.set_service_env_var("postgres", "POSTGRES_DB", db_name);
                    services.set_service_env_var("postgres", "POSTGRES_PASSWORD", pass);
                    format!("postgresql://postgres:{}@postgres/{}", pass, db_name)
                }
            };

            services.set_service_env_var("readyset-adapter", "ALLOWED_USERNAME", "root");
            services.set_service_env_var("readyset-adapter", "ALLOWED_PASSWORD", pass);

            services.set_service_env_var("readyset-server", "REPLICATION_URL", &url);
            services.set_service_env_var("readyset-adapter", "UPSTREAM_DB_URL", &url);
        }
    }

    /// Fills in the port an end user would like to host the ReadySet adapter on.
    pub fn fill_adapter_port(&mut self, port: u16) {
        if let Some(ref mut services) = self.services {
            let addr = format!("0.0.0.0:{}", port);
            services.set_service_env_var("readyset-adapter", "LISTEN_ADDRESS", &addr);

            if let Some(Some(ref mut service)) = services.0.get_mut("readyset-adapter") {
                let ports = format!("{}:{}", port, port);
                if let Some(ref mut list) = service.ports {
                    list.push(ports);
                } else {
                    service.ports = Some(vec![ports]);
                }
            }
        }
    }

    /// Sets the desired migration mode for the ReadySet adapter.
    pub fn fill_migration_mode(&mut self, mode: &MigrationMode) {
        if let Some(ref mut services) = self.services {
            match mode {
                MigrationMode::Async => {
                    services.set_service_env_var("readyset-adapter", "ASYNC_MIGRATIONS", "1");
                }
                MigrationMode::Explicit => {
                    services.set_service_env_var("readyset-adapter", "EXPLICIT_MIGRATIONS", "1");
                }
            }
        }
    }
}

#[skip_serializing_none]
#[derive(Builder, Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
#[builder(setter(into), default)]
pub struct Service {
    pub hostname: Option<String>,
    pub extra_hosts: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub privileged: bool,
    pub healthcheck: Option<Healthcheck>,
    pub deploy: Option<Deploy>,
    pub image: Option<String>,
    pub container_name: Option<String>,
    #[serde(rename = "build")]
    pub build_: Option<BuildStep>,
    pub pid: Option<String>,
    pub ports: Option<Vec<String>>,
    pub environment: Option<HashMap<String, String>>,
    pub network_mode: Option<String>,
    pub devices: Option<Vec<String>>,
    pub restart: Option<String>,
    pub labels: Option<Labels>,
    pub ulimits: Option<Ulimits>,
    pub volumes: Option<Volumes>,
    pub networks: Option<Networks>,
    pub cap_add: Option<Vec<String>>,
    pub depends_on: Option<DependsOnOptions>,
    pub command: Option<Command>,
    pub entrypoint: Option<String>,
    pub env_file: Option<EnvFile>,
    pub stop_grace_period: Option<String>,
    pub profiles: Option<Vec<String>>,
    pub links: Option<Vec<String>>,
    pub dns: Option<Vec<String>>,
    pub ipc: Option<String>,
    pub net: Option<String>,
    pub stop_signal: Option<String>,
    pub user: Option<String>,
    pub working_dir: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub expose: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub volumes_from: Vec<String>,
    pub extends: Option<HashMap<String, String>>,
    pub logging: Option<LoggingParameters>,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub scale: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum EnvFile {
    Simple(String),
    List(Vec<String>),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DependsOnOptions {
    Simple(Vec<String>),
    Conditional(HashMap<String, DependsCondition>),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DependsCondition {
    pub condition: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LoggingParameters {
    pub driver: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub options: Option<LoggingParameterOptions>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LoggingParameterOptions {
    #[serde(rename = "max-size")]
    pub max_size: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum EnvTypes {
    String(String),
    Number(serde_yaml::Number),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Services(pub HashMap<String, Option<Service>>);

impl Services {
    /// Sets the image for the service, if the service exists, and no-ops if the service does not
    /// exist.
    pub fn set_service_img(&mut self, name: &str, img: String) {
        if let Some(Some(ref mut service)) = self.0.get_mut(name) {
            service.image = Some(img)
        }
    }

    /// Adds the env var for the service, if the service exists, and no-ops if the service does not
    /// exist. If the service exists and the env var already exists, will overwrite the old value.
    pub fn set_service_env_var(&mut self, name: &str, key: &str, val: &str) {
        if let Some(Some(ref mut service)) = self.0.get_mut(name) {
            if let Some(ref mut map) = service.environment {
                map.insert(key.to_string(), val.to_string());
            } else {
                let mut map = HashMap::new();
                map.insert(key.to_string(), val.to_string());
                service.environment = Some(map);
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Labels(pub HashMap<String, String>);

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Ulimits {
    pub nofile: Nofile,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Nofile {
    pub soft: i64,
    pub hard: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Networks {
    Simple(Vec<String>),
    Advanced(AdvancedNetworks),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum BuildStep {
    Simple(String),
    Advanced(AdvancedBuildStep),
}

#[skip_serializing_none]
#[derive(Builder, Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
#[builder(setter(into), default)]
pub struct AdvancedBuildStep {
    pub context: String,
    pub dockerfile: Option<String>,
    pub args: Option<BuildArgs>,
    shm_size: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum BuildArgs {
    Simple(String),
    List(Vec<String>),
    KvPair(HashMap<String, String>),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AdvancedNetworks(pub HashMap<String, Option<AdvancedNetworkSettings>>);

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdvancedNetworkSettings {
    pub ipv4_address: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ComposeVolumes(pub HashMap<String, Option<HashMap<String, String>>>);

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum TopLevelVolumes {
    CV(ComposeVolumes),
    Labelled(LabelledComposeVolumes),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LabelledComposeVolumes(pub HashMap<String, VolumeLabels>);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VolumeLabels {
    labels: HashMap<String, String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ComposeNetworks(pub HashMap<String, NetworkSettingsOptions>);

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum NetworkSettingsOptions {
    Settings(NetworkSettings),
    Empty(HashMap<(), ()>),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ComposeNetwork {
    Detailed(ComposeNetworkSettingDetails),
    Bool(bool),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ComposeNetworkSettingDetails {
    pub name: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExternalNetworkSettingBool(bool);

#[skip_serializing_none]
#[derive(Clone, Default, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NetworkSettings {
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub attachable: bool,
    pub driver: Option<String>,
    pub internal: Option<ComposeNetwork>,
    pub external: Option<ComposeNetwork>,
    pub ipam: Option<Ipam>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Ipam {
    pub config: Vec<IpamConfig>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IpamConfig {
    pub subnet: String,
}

#[skip_serializing_none]
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Deploy {
    pub mode: Option<String>,
    pub replicas: i64,
    pub labels: Option<Vec<String>>,
    pub update_config: Option<UpdateConfig>,
    pub resources: Resources,
    pub restart_policy: RestartPolicy,
    pub placement: Option<Placement>,
}

fn is_zero(val: &i64) -> bool {
    *val == 0
}

#[skip_serializing_none]
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Healthcheck {
    pub test: Option<HealthcheckTest>,
    pub interval: Option<String>,
    pub start_period: Option<String>,
    pub timeout: Option<String>,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub retries: i64,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub disable: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum HealthcheckTest {
    Single(String),
    Multiple(Vec<String>),
}

#[skip_serializing_none]
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Limits {
    pub cpus: Option<String>,
    pub memory: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Placement {
    pub constraints: Vec<String>,
    pub preferences: Vec<Preferences>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Preferences {
    pub spread: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Resources {
    pub limits: Limits,
    pub reservations: Limits,
}

#[skip_serializing_none]
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RestartPolicy {
    pub condition: String,
    pub delay: Option<String>,
    pub max_attempts: i64,
    pub window: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateConfig {
    pub parallelism: i64,
    pub delay: String,
    pub failure_action: String,
    pub monitor: String,
    pub max_failure_ratio: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Volumes {
    Simple(Vec<String>),
    Advanced(Vec<AdvancedVolumes>),
}

#[skip_serializing_none]
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdvancedVolumes {
    pub source: Option<String>,
    pub target: String,
    #[serde(rename = "type")]
    pub _type: String,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub read_only: bool,
    pub volume: Option<Volume>,
}

#[derive(Default, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Volume {
    pub nocopy: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Command {
    Simple(String),
    Args(Vec<String>),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn volumes() {
        let v = r#"
volumes:
  - source: /host/path
    target: /container/path
    type: bind
    read_only: true
  - source: foobar
    type: volume
    target: /container/volumepath
  - type: volume
    target: /anonymous
  - type: volume
    source: foobar
    target: /container/volumepath2
    volume:
      nocopy: true
"#;

        #[derive(Deserialize)]
        #[allow(dead_code)]
        struct Container {
            volumes: Volumes,
        }
        let _parsed: Container = serde_yaml::from_str(v).unwrap();
    }
}
