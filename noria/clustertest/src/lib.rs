mod cargo_builder;
mod docker;
mod server;

use anyhow::{anyhow, Result};
use docker::{kill_zookeeper, start_zookeeper};
use futures::executor;
use noria::consensus::ZookeeperAuthority;
use noria::metrics::client::MetricsClient;
use noria::ControllerHandle;
use server::{NoriaServerRunner, ServerProcessHandle};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::thread::sleep;
use std::time::{Duration, Instant};
use url::Url;

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

impl ServerParams {
    pub fn default() -> Self {
        Self { region: None }
    }

    pub fn with_region(mut self, region: &str) -> Self {
        self.region = Some(region.to_string());
        self
    }
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

/// A handle to a single server in the deployment.
pub struct ServerHandle {
    /// The external address of the server.
    pub addr: Url,
    /// The local process the server is running in.
    pub process: ServerProcessHandle,
}

/// A handle to a deployment created with `start_multi_process`.
pub struct DeploymentHandle {
    /// A handle to the current controller of the deployment.
    pub handle: ControllerHandle<ZookeeperAuthority>,
    /// Metrics client for aggregating metrics across the deployment.
    pub metrics: MetricsClient<ZookeeperAuthority>,
    /// Map from a noria server's address to a handle to the server.
    noria_server_handles: HashMap<Url, ServerHandle>,
    /// The name of the deployment, cluster resources are prefixed
    /// by `name`.
    name: String,
    /// The zookeeper connect string for the deployment.
    zookeeper_addr: String,
    /// A handle to each noria server in the deployment.
    /// True if this deployment has already been torn down.
    shutdown: bool,
    /// The source of the noria-server binary. Stored here to allow
    /// new servers to be started up.
    noria_server_path: PathBuf,
    /// Dataflow sharding for new servers.
    sharding: Option<usize>,
    /// The primary region of the deployment.
    primary_region: Option<String>,
    /// Next new server port.
    port: u16,
}

impl DeploymentHandle {
    /// Start a new noria-server instance in the deployment.
    pub async fn start_server(&mut self, params: ServerParams) -> anyhow::Result<Url> {
        let port = get_next_good_port(Some(self.port));
        self.port = port;
        let handle = start_server(
            &params,
            &self.noria_server_path,
            &self.name,
            self.sharding,
            self.primary_region.as_ref(),
            &self.zookeeper_addr,
            port,
        )?;
        let server_addr = handle.addr.clone();
        self.noria_server_handles
            .insert(server_addr.clone(), handle);

        // Wait until the worker has been created and is visible over rpc.
        wait_until_worker_count(
            &mut self.handle,
            Duration::from_secs(15),
            self.noria_server_handles.len(),
        )
        .await?;
        Ok(server_addr)
    }

    /// Kill an existing noria-server instance in the deployment referenced
    /// by `ServerHandle`.
    pub async fn kill_server(&mut self, server_addr: Url) -> anyhow::Result<()> {
        if !self.noria_server_handles.contains_key(&server_addr) {
            return Err(anyhow!("Server handle does not exist in deployment"));
        }

        let mut handle = self.noria_server_handles.remove(&server_addr).unwrap();
        handle.process.kill()?;

        // Wait until the server is no longer visible in the deployment.
        // This must be at least the value of the deployment's state.config.healthcheck_every,
        // the interval where a worker's liveliness status changes.
        wait_until_worker_count(
            &mut self.handle,
            Duration::from_secs(15),
            self.noria_server_handles.len(),
        )
        .await?;

        Ok(())
    }

    /// Tears down any resources associated with the deployment.
    pub async fn teardown(&mut self) -> anyhow::Result<()> {
        if self.shutdown {
            return Ok(());
        }

        for h in &mut self.noria_server_handles {
            // Drop any errors on failure to kill so we complete
            // cleanup.
            let _ = h.1.process.kill();
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
async fn wait_until_worker_count(
    handle: &mut ControllerHandle<ZookeeperAuthority>,
    max_wait: Duration,
    num_workers: usize,
) -> Result<()> {
    let start = Instant::now();
    loop {
        let workers = handle.healthy_workers().await.unwrap().len();
        if workers == num_workers {
            return Ok(());
        }

        let now = Instant::now();
        if (now - start) > max_wait {
            break;
        }

        sleep(Duration::from_millis(500));
    }

    Err(anyhow!("Exceeded maximum time to wait for workers"))
}

fn start_server(
    server_params: &ServerParams,
    noria_server_path: &Path,
    deployment_name: &str,
    sharding: Option<usize>,
    primary_region: Option<&String>,
    zookeeper_addr: &str,
    port: u16,
) -> Result<ServerHandle> {
    let mut runner = NoriaServerRunner::new(noria_server_path);
    runner.set_deployment(deployment_name);
    runner.set_external_port(port);
    runner.set_zookeeper(zookeeper_addr);
    if let Some(shard) = sharding {
        runner.set_shards(shard);
    }
    if let Some(region) = server_params.region.as_ref() {
        runner.set_region(&region);
    }
    if let Some(region) = primary_region.as_ref() {
        runner.set_primary_region(region);
    }

    let addr = Url::parse(&format!("http://127.0.0.1:{}", port)).unwrap();
    Ok(ServerHandle {
        addr,
        process: runner.start()?,
    })
}

/// Checks the set of deployment params for invalid configurations
pub fn check_deployment_params(params: &DeploymentParams) -> anyhow::Result<()> {
    match &params.primary_region {
        Some(pr) => {
            // If the primary region is set, at least one server should match that
            // region.
            if params
                .servers
                .iter()
                .all(|s| s.region.as_ref().filter(|region| region == &pr).is_none())
            {
                return Err(anyhow!(
                    "Primary region specified, but no servers match
                    the region."
                ));
            }
        }
        None => {
            // If the primary region is not set, servers should not include a `region`
            // parameter. Otherwise, a controller will not be elected.
            if params.servers.iter().any(|s| s.region.is_some()) {
                return Err(anyhow!(
                    "Servers have region without a deployment primary region"
                ));
            }
        }
    }
    Ok(())
}

/// Finds the next available port after `port` (if supplied).
/// Otherwise, it returns a random available port in the range of 20000-60000.
fn get_next_good_port(port: Option<u16>) -> u16 {
    let mut port = port.map(|p| p + 1).unwrap_or_else(|| {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        rng.gen_range(20000..60000)
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
    check_deployment_params(&params)?;
    let mut port = get_next_good_port(None);

    // Kill and remove any containers with the same name to prevent
    // container conflicts errors.
    let zookeeper_addr = format!("127.0.0.1:{}", &port);
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
    let mut handles = HashMap::new();
    for server in &params.servers {
        port = get_next_good_port(Some(port));
        let handle = start_server(
            server,
            &noria_server_path,
            &params.name,
            params.sharding,
            params.primary_region.as_ref(),
            &zookeeper_addr,
            port,
        )?;

        handles.insert(handle.addr.clone(), handle);
    }

    let zookeeper_connect_str = format!("{}/{}", &zookeeper_addr, &params.name);
    let authority = ZookeeperAuthority::new(zookeeper_connect_str.as_str())?;
    let mut handle = ControllerHandle::new(authority).await?;
    wait_until_worker_count(&mut handle, Duration::from_secs(15), params.servers.len()).await?;

    // Duplicate the authority and handle creation as the metrics client
    // owns its own handle.
    let metrics_authority = ZookeeperAuthority::new(zookeeper_connect_str.as_str())?;
    let metrics_handle = ControllerHandle::new(metrics_authority).await?;
    let metrics = MetricsClient::new(metrics_handle).unwrap();

    Ok(DeploymentHandle {
        handle,
        metrics,
        name: params.name.clone(),
        zookeeper_addr,
        noria_server_handles: handles,
        shutdown: false,
        noria_server_path,
        sharding: params.sharding,
        primary_region: params.primary_region,
        port,
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
        deployment.add_server(ServerParams::default());
        deployment.add_server(ServerParams::default());

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
        let workers = deployment.handle.healthy_workers().await.unwrap();
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
        deployment.add_server(ServerParams::default());
        deployment.add_server(ServerParams::default());

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
        deployment.add_server(ServerParams::default().with_region("r1"));
        deployment.add_server(ServerParams::default().with_region("r2"));

        let mut deployment = start_multi_process(deployment).await.unwrap();
        deployment.teardown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn clustertest_server_management() {
        let cluster_name = "ct_server_management";
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
        deployment.add_server(ServerParams::default().with_region("r1"));
        deployment.add_server(ServerParams::default().with_region("r2"));

        let mut deployment = start_multi_process(deployment).await.unwrap();

        // Check that we currently have two workers.
        assert_eq!(deployment.handle.healthy_workers().await.unwrap().len(), 2);

        // Start up a new server.
        let server_handle = deployment
            .start_server(ServerParams::default().with_region("r3"))
            .await
            .unwrap();
        assert_eq!(deployment.handle.healthy_workers().await.unwrap().len(), 3);

        // Now kill that server we started up.
        deployment.kill_server(server_handle).await.unwrap();
        assert_eq!(deployment.handle.healthy_workers().await.unwrap().len(), 2);

        deployment.teardown().await.unwrap();
    }

    #[tokio::test]
    async fn clustertest_no_server_in_primary_region_test() {
        let mut deployment =
            DeploymentParams::new("fake_cluster", NoriaServerSource::Existing("/".into()));
        deployment.set_primary_region("r1");
        deployment.add_server(ServerParams::default().with_region("r2"));
        deployment.add_server(ServerParams::default().with_region("r3"));
        assert!(start_multi_process(deployment).await.is_err());
    }

    #[tokio::test]
    async fn clustertest_server_region_without_primary_region() {
        let mut deployment =
            DeploymentParams::new("fake_cluster", NoriaServerSource::Existing("/".into()));
        deployment.add_server(ServerParams::default().with_region("r1"));
        deployment.add_server(ServerParams::default().with_region("r2"));

        assert!(start_multi_process(deployment).await.is_err());
    }
}
