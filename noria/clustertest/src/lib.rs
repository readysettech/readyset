mod cargo_builder;
mod docker;
mod server;

use anyhow::{anyhow, Result};
use docker::{kill_zookeeper, start_zookeeper};
use futures::executor;
use noria::consensus::ZookeeperAuthority;
use noria::metrics::client::MetricsClient;
use noria::ControllerHandle;
use server::{NoriaServerRunner, ServerHandle};
use std::{
    path::PathBuf,
    thread::sleep,
    time::{Duration, Instant},
};

#[cfg(test)]
use std::env;

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

/// Parameters for a single noria-server instance.
pub struct ServerParams {
    /// A server's region string, passed in via --region.
    region: Option<String>,
}

/// Set of parameters defining an entire cluster's topology.
pub struct DeploymentParams {
    /// Name of the cluster, cluster resources will be prefixed
    /// with this name.
    name: String,
    /// Source of the noria-server binary. `start_multi_process`
    /// may be required to do more work based on this value.
    noria_server_source: NoriaServerSource,
    /// Number of shards for dataflow nodes.
    sharding: Option<usize>,
    /// The primary region of the noria cluster.
    primary_region: Option<String>,
    /// Parameters for the set of noria-server instances in the deployment.
    servers: Vec<ServerParams>,
}

impl DeploymentParams {
    // TODO(justin): Convert to a builder pattern to make this cleaner.
    pub fn new(name: &str, noria_server_source: NoriaServerSource) -> Self {
        Self {
            name: name.to_string(),
            noria_server_source,
            sharding: None,
            primary_region: None,
            servers: vec![],
        }
    }

    pub fn set_sharding(&mut self, shards: usize) {
        self.sharding = Some(shards);
    }

    pub fn set_primary_region(&mut self, region: &str) {
        self.primary_region = Some(region.to_string());
    }

    pub fn add_server(&mut self, server: ServerParams) {
        self.servers.push(server);
    }
}

/// A handle to a deployment created with `start_multi_process`.
pub struct DeploymentHandle {
    /// A handle to the current controller of the deployment.
    pub handle: ControllerHandle<ZookeeperAuthority>,
    /// Metrics client for aggregating metrics across the deployment.
    pub metrics: MetricsClient<ZookeeperAuthority>,
    /// The name of the deployment, cluster resources are prefixed
    /// by `name`.
    name: String,
    /// A handle to each noria server in the deployment.
    // TODO(justin): Add API function to support querying status
    // and killing specific servers.
    noria_server_handles: Vec<ServerHandle>,
    /// True if this deployment has already been torn down.
    shutdown: bool,
}

impl DeploymentHandle {
    /// Tears down any resources associated with the deployment.
    pub async fn teardown(&mut self) -> anyhow::Result<()> {
        if self.shutdown {
            return Ok(());
        }

        for h in &mut self.noria_server_handles {
            // Drop any errors on failure to kill so we complete
            // cleanup.
            let _ = h.kill();
        }
        kill_zookeeper(&self.name).await?;
        self.shutdown = true;
        Ok(())
    }
}

impl Drop for DeploymentHandle {
    // Attempt to clean up any resources used by the DeploymentHandle. Drop
    // will be called on test panics allowing resources to be cleaned up.
    // TODO(justin): This does not always work if a test does not cleanup
    // with teardown explicitly, leading to noria-server instances living.
    #[allow(unused_must_use)]
    fn drop(&mut self) {
        executor::block_on(self.teardown());
    }
}

// Queries the number of workers every half second until `max_wait`.
pub async fn wait_until_workers_alive(
    handle: &mut ControllerHandle<ZookeeperAuthority>,
    max_wait: Duration,
    num_workers: usize,
) -> Result<()> {
    let start = Instant::now();
    loop {
        let workers = handle.workers().await.unwrap().len();
        if workers == num_workers {
            return Ok(());
        }

        let now = Instant::now();
        if (now - start) > max_wait {
            break;
        }

        sleep(Duration::from_millis(500));
    }

    Err(anyhow!(
        "Exceeded maximum waiting time for workers to be alive"
    ))
}

/// Finds the next available port after `port` (if supplied).
/// Otherwise, it returns a random available port in the range of 20000-60000.
fn get_next_good_port(port: Option<u16>) -> u16 {
    let mut port = port.map(|p| p + 1).unwrap_or_else(|| {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        rng.gen_range(20000, 60000)
    });
    while !port_scanner::local_port_available(port) {
        port += 1;
    }
    port
}

/// Used to create a multi_process test deployment. This deployment
/// consists of a docker container running zookeeper for cluster management,
/// and a set of noria-servers. `params` can be used to setup the topology
/// of the deployment for testing.
///
/// Currently this sets up a single zookeeper node on the local machines
/// docker daemon.
// TODO(justin): Add support for multiple concurrent multi-process clusters by
// dynamically assigning port ranges.
pub async fn start_multi_process(params: DeploymentParams) -> anyhow::Result<DeploymentHandle> {
    let mut port = get_next_good_port(None);

    // Kill and remove any containers with the same name to prevent
    // container conflicts errors.
    let zookeeper_addr = "127.0.0.1:".to_string() + &port.to_string();
    kill_zookeeper(&params.name).await?;
    start_zookeeper(&params.name, port).await?;

    let noria_server_path = match params.noria_server_source {
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

    // Create the noria-server instances.
    let mut handles = Vec::new();

    for server in &params.servers {
        port = get_next_good_port(Some(port));
        let mut runner = NoriaServerRunner::new(&noria_server_path);
        runner.set_zookeeper(&zookeeper_addr);
        runner.set_deployment(&params.name);
        runner.set_external_port(port);
        if let Some(shard) = &params.sharding {
            runner.set_shards(*shard);
        }
        if let Some(region) = server.region.as_ref() {
            runner.set_region(&region);
        }
        if let Some(region) = params.primary_region.as_ref() {
            runner.set_primary_region(region);
        }

        handles.push(runner.start()?);
    }

    let zookeeper_path = zookeeper_addr + "/" + &params.name;
    let authority = ZookeeperAuthority::new(&zookeeper_path)?;
    let mut handle = ControllerHandle::new(authority).await?;
    wait_until_workers_alive(&mut handle, Duration::from_secs(15), params.servers.len()).await?;

    // Duplicate the authority and handle creation as the metrics client
    // owns its own handle.
    let metrics_authority = ZookeeperAuthority::new(&zookeeper_path)?;
    let metrics_handle = ControllerHandle::new(metrics_authority).await?;
    let metrics = MetricsClient::new(metrics_handle).unwrap();

    Ok(DeploymentHandle {
        handle,
        metrics,
        name: params.name.clone(),
        noria_server_handles: handles,
        shutdown: false,
    })
}

#[cfg(test)]
pub fn get_project_root() -> PathBuf {
    let mut project_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    project_root.pop();
    project_root.pop();

    project_root
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
    async fn clustertest_startup_teardown_test() {
        let cluster_name = "ct_startup_teardown";
        let project_root = get_project_root();
        let build_dir = project_root.join("test_target");
        let zookeeper_container_name = prefix_to_zookeeper_container(cluster_name);

        let mut deployment = DeploymentParams::new(
            cluster_name,
            NoriaServerSource::Build(BuildParams {
                root_project_path: project_root,
                target_dir: build_dir,
                release: true,
                rebuild: false,
            }),
        );
        deployment.add_server(ServerParams { region: None });
        deployment.add_server(ServerParams { region: None });

        let deployment = start_multi_process(deployment).await;
        assert!(
            !deployment.is_err(),
            "Error starting deployment: {}",
            deployment.err().unwrap()
        );
        assert!(docker::zookeeper_container_running(&zookeeper_container_name).await);

        let mut deployment = deployment.unwrap();

        // Check we received a metrics dump from each client.
        let metrics = deployment.metrics.get_metrics().await.unwrap();
        assert_eq!(metrics.len(), 2);

        // Check that the controller can respond to an rpc.
        let workers = deployment.handle.workers().await.unwrap();
        assert_eq!(workers.len(), 2);

        let res = deployment.teardown().await;
        assert!(
            !res.is_err(),
            "Error tearing down deployment: {}",
            res.err().unwrap()
        );
        assert!(!docker::zookeeper_container_exists(&zookeeper_container_name).await);
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn clustertest_minimal() {
        let cluster_name = "ct_minimal";
        let mut deployment = DeploymentParams::new(
            cluster_name,
            NoriaServerSource::Build(BuildParams {
                root_project_path: get_project_root(),
                target_dir: get_project_root().join("test_target"),
                release: true,
                rebuild: false,
            }),
        );
        deployment.add_server(ServerParams { region: None });
        deployment.add_server(ServerParams { region: None });

        let mut deployment = start_multi_process(deployment).await.unwrap();
        deployment.teardown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn clustertest_multiregion() {
        let cluster_name = "ct_multiregion";
        let mut deployment = DeploymentParams::new(
            cluster_name,
            NoriaServerSource::Build(BuildParams {
                root_project_path: get_project_root(),
                target_dir: get_project_root().join("test_target"),
                release: true,
                rebuild: false,
            }),
        );
        deployment.set_primary_region("r1");
        deployment.add_server(ServerParams {
            region: Some("r1".to_string()),
        });
        deployment.add_server(ServerParams {
            region: Some("r2".to_string()),
        });

        let mut deployment = start_multi_process(deployment).await.unwrap();
        deployment.teardown().await.unwrap();
    }
}
