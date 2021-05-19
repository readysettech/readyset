mod cargo_builder;
mod docker;

use docker::{kill_zookeeper, start_zookeeper};
use std::path::PathBuf;

/// Source of the noria-server binary.
pub enum NoriaServerSource {
    /// Use a prebuilt binary specified by the path.
    Existing(PathBuf),
    /// Build the binary based on `BuildParams`.
    Build(BuildParams),
}

/// Parameters required to build noria-server.
pub struct BuildParams {
    /// The path to the root project to build noria-server from.
    root_project_path: PathBuf,
    /// The target directory to store the noria-server binary.
    target_dir: PathBuf,
    /// Whether we build the release version of the binary.
    release: bool,
    /// Rebuild if the binary already exists.
    rebuild: bool,
}

/// Set of parameters defining an entire cluster's topology.
pub struct DeploymentParams {
    /// Name of the cluster, cluster resources will be prefixed
    /// with this name.
    name: String,

    /// Source of the noria-server binary. `start_multi_process`
    /// may be required to do more work based on this value.
    noria_server_source: NoriaServerSource,
    // TODO(justin): Add parameters for the servers in the
    // deployment.
}

impl DeploymentParams {
    pub fn new(name: &str, noria_server_source: NoriaServerSource) -> Self {
        Self {
            name: name.to_string(),
            noria_server_source,
        }
    }
}

/// A handle to a deployment created with `start_multi_process`.
pub struct DeploymentHandle {
    /// The name of the deployment, cluster resources are prefixed
    /// by `name`.
    name: String,
}

impl DeploymentHandle {
    /// Tears down any resources associated with the deployment.
    pub async fn teardown(&mut self) -> anyhow::Result<()> {
        kill_zookeeper(&self.name).await?;
        Ok(())
    }
}

/// Used to create a multi_process test deployment. This deployment
/// consists of a docker container running zookeeper for cluster management,
/// and a set of noria-servers. `params` can be used to setup the topology
/// of the deployment for testing.
///
/// Currently this sets up a single zookeeper node on the local machines
/// docker daemon.
/// TODO(justin): Build and start noria server instances.
pub async fn start_multi_process(params: DeploymentParams) -> anyhow::Result<DeploymentHandle> {
    // Kill and remove any containers with the same name to prevent
    // container conflicts errors.
    kill_zookeeper(&params.name).await?;
    start_zookeeper(&params.name).await?;

    let _noria_server_path = match params.noria_server_source {
        // TODO(justin): Make building the noria container start in a seperate
        // thread to parallelize zookeeper startup and binary building.
        NoriaServerSource::Build(build_params) => cargo_builder::build_noria_server(
            &build_params.root_project_path,
            &build_params.target_dir,
            build_params.release,
            build_params.rebuild,
        )?,
        NoriaServerSource::Existing(path) => path,
    };

    Ok(DeploymentHandle {
        name: params.name.clone(),
    })
}

// These tests currently require that a docker daemon is already setup
// and accessible by the user calling cargo test. As these tests interact
// with a stateful external component, the docker daemon, each test is
// responsible for cleaning up its own external state.
#[cfg(test)]
mod tests {
    use super::*;
    use docker::prefix_to_zookeeper_container;
    use serial_test::serial;
    // Verifies that the wrappers that create and teardown the deployment
    // correctly setup zookeeper containers.
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn deployment_handle_startup_teardown() {
        let cluster_name = "handle_cleanup_test";
        let zookeeper_container_name = prefix_to_zookeeper_container(cluster_name);
        let handle = start_multi_process(DeploymentParams::new(
            cluster_name,
            NoriaServerSource::Existing(PathBuf::from("/")),
        ))
        .await;
        assert!(
            !handle.is_err(),
            "Error starting deployment: {}",
            handle.err().unwrap()
        );
        assert!(docker::zookeeper_container_running(&zookeeper_container_name).await);

        let res = handle.unwrap().teardown().await;
        assert!(
            !res.is_err(),
            "Error tearing down deployment: {}",
            res.err().unwrap()
        );
        assert!(!docker::zookeeper_container_exists(&zookeeper_container_name).await);
    }
}
