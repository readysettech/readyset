use anyhow::Result;
use shiplift::{ContainerOptions, Docker, RmContainerOptions};

/// Set of parameters defining an entire cluster's topology.
pub struct DeploymentParams {
    /// Name of the cluster, cluster resources will be prefixed
    /// with this name.
    name: String,
    // TODO(justin): Add parameters for the servers in the
    // deployment.
}

impl DeploymentParams {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
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

    Ok(DeploymentHandle {
        name: params.name.clone(),
    })
}

fn prefix_to_zookeeper_container(prefix: &str) -> String {
    prefix.to_string() + "-zookeeper"
}

/// Creates and starts a new zookeeper container, this does not
/// check for container conflicts and will error if a container
/// with the same name '`name_prefix`-zookeeper' already exist.
async fn start_zookeeper(name_prefix: &str) -> Result<()> {
    let docker = Docker::new();
    let image = "zookeeper";
    let container_name = prefix_to_zookeeper_container(name_prefix);

    docker
        .containers()
        .create(
            &ContainerOptions::builder(image)
                .name(&container_name)
                .restart_policy("always", 10)
                .expose(2181, "tcp", 2181)
                .build(),
        )
        .await?;

    docker.containers().get(container_name).start().await?;
    Ok(())
}

/// Stops an existing zookeeper instance and removes the container.
/// This function returns an `Ok` result if the container does not
/// exist or was not running.
async fn kill_zookeeper(name_prefix: &str) -> Result<()> {
    let docker = Docker::new();
    let container_name = prefix_to_zookeeper_container(name_prefix);
    let container = docker.containers().get(container_name);
    if let Ok(d) = container.inspect().await {
        if d.state.running {
            container.stop(None).await?;
        }

        container
            .remove(RmContainerOptions::builder().force(true).build())
            .await?;
    }
    Ok(())
}

// These tests currently require that a docker daemon is already setup
// and accessible by the user calling cargo test. As these tests interact
// with a stateful external component, the docker daemon, each test is
// responsible for cleaning up its own external state.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::prefix_to_zookeeper_container;
    use serial_test::serial;

    // Creates a new zookeeper client that checks if a container, `name`,
    // is running.
    async fn zookeeper_container_running(name: &str) -> bool {
        let docker = Docker::new();
        match docker.containers().get(name).inspect().await {
            Err(e) => {
                eprintln!("Error inspecting container: {}", e);
                false
            }
            Ok(d) => d.state.running,
        }
    }

    // Checks if the container already exists.
    async fn zookeeper_container_exists(name: &str) -> bool {
        let docker = Docker::new();
        // Inspects a container, this will return a 404 error
        // if the container does not exist.
        !docker.containers().get(name).inspect().await.is_err()
    }

    // This test verifies that we can create and teardown a zookeeper
    // docker container. It does not verify that the container is
    // set up properly.
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn zookeeper_operations() {
        // Create a zookeeper container, verify that it is running.
        let container_prefix = "start_zookeeper_test";
        let name = prefix_to_zookeeper_container(container_prefix);
        let res = start_zookeeper(container_prefix).await;
        assert!(
            !res.is_err(),
            "Error starting zookeeper: {}",
            res.err().unwrap()
        );
        // This only verifies that the zookeeper container is running, it
        // does not verify connectivity or that zookeeper is running
        // successfully in the container.
        assert!(zookeeper_container_running(&name).await);

        // Kill and remove the zookeeper container,
        let res = kill_zookeeper(container_prefix).await;
        assert!(
            !res.is_err(),
            "Error killing zookeeper: {}",
            res.err().unwrap()
        );
        assert!(!zookeeper_container_exists(&name).await);
    }

    // Verifies that the wrappers that create and teardown the deployment
    // correctly setup zookeeper containers.
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn deployment_handle_startup_teardown() {
        let cluster_name = "handle_cleanup_test";
        let zookeeper_container_name = prefix_to_zookeeper_container(cluster_name);
        let handle = start_multi_process(DeploymentParams::new(cluster_name)).await;
        assert!(
            !handle.is_err(),
            "Error starting deployment: {}",
            handle.err().unwrap()
        );
        assert!(zookeeper_container_running(&zookeeper_container_name).await);

        let res = handle.unwrap().teardown().await;
        assert!(
            !res.is_err(),
            "Error tearing down deployment: {}",
            res.err().unwrap()
        );
        assert!(!zookeeper_container_exists(&zookeeper_container_name).await);
    }
}
