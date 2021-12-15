mod server;

#[cfg(test)]
mod readyset;
#[cfg(test)]
mod readyset_mysql;
#[cfg(test)]
mod utils;

use anyhow::{anyhow, Result};
use futures::executor;
use mysql_async::prelude::Queryable;
use noria::consensus::AuthorityType;
use noria::metrics::client::MetricsClient;
use noria::ControllerHandle;
use rand::Rng;
use serde::Deserialize;
use server::{AdapterBuilder, NoriaServerBuilder, ProcessHandle};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use url::Url;

/// The set of environment variables that need to be set for the
/// tests to run. Each variable is the upper case of their respective,
/// struct variable name, i.e. AUTHORITY_ADDRESS.
#[derive(Deserialize, Debug)]
struct Env {
    #[serde(default = "default_authority_address")]
    authority_address: String,
    #[serde(default = "default_authority")]
    authority: String,
    #[serde(default = "default_binary_path")]
    binary_path: PathBuf,
    #[serde(default = "default_mysql_host")]
    mysql_host: String,
    #[serde(default = "default_root_password")]
    mysql_root_password: String,
}

fn default_authority_address() -> String {
    "127.0.0.1:8500".to_string()
}

fn default_mysql_host() -> String {
    "127.0.0.1".to_string()
}

fn default_authority() -> String {
    "consul".to_string()
}

fn default_binary_path() -> PathBuf {
    // Convert from <dir>/noria/clustertest to <dir>/target/debug.
    let mut path: PathBuf = std::env::var("CARGO_MANIFEST_DIR").unwrap().into();
    path.pop();
    path.pop();
    path.push("target/debug");
    path
}

fn default_root_password() -> String {
    "noria".to_string()
}

/// Source of the noria binaries.
pub struct NoriaBinarySource {
    /// Path to a built noria-server on the local machine.
    pub noria_server: PathBuf,
    /// Optional path to noria-mysql on the local machine. noria-mysql
    /// may not be included in the build.
    pub noria_mysql: Option<PathBuf>,
}

/// Parameters for a single noria-server instance.
#[derive(Clone)]
pub struct ServerParams {
    /// A server's region string, passed in via --region.
    region: Option<String>,
    /// THe volume id of the server, passed in via --volume-id.
    volume_id: Option<String>,
}

impl ServerParams {
    pub fn default() -> Self {
        Self {
            region: None,
            volume_id: None,
        }
    }

    pub fn with_region(mut self, region: &str) -> Self {
        self.region = Some(region.to_string());
        self
    }

    pub fn with_volume(mut self, volume: &str) -> Self {
        self.volume_id = Some(volume.to_string());
        self
    }
}

/// Set of parameters defining an entire cluster's topology.
pub struct DeploymentBuilder {
    /// Name of the cluster, cluster resources will be prefixed
    /// with this name.
    name: String,
    /// Source of the binaries.
    noria_binaries: NoriaBinarySource,
    /// Number of shards for dataflow nodes.
    shards: Option<usize>,
    /// Number of workers to wait for before starting.
    quorum: usize,
    /// The primary region of the noria cluster.
    primary_region: Option<String>,
    /// Parameters for the set of noria-server instances in the deployment.
    servers: Vec<ServerParams>,
    /// Deploy the mysql adapter.
    mysql_adapter: bool,
    /// Deploy mysql and use binlog replication.
    mysql: bool,
    /// The type of authority to use for cluster management.
    authority: AuthorityType,
    /// The address of the authority.
    authority_address: String,
    /// The address of the mysql host.
    mysql_host: String,
    /// The root password for the mysql db.
    mysql_root_password: String,
    /// Are async migrations enabled on the adapter.
    async_migration_interval: Option<u64>,
}

impl DeploymentBuilder {
    pub fn new(name: &str) -> Self {
        let env = envy::from_env::<Env>().unwrap();

        let mut noria_server_path = env.binary_path.clone();
        noria_server_path.push("noria-server");

        let mut noria_mysql_path = env.binary_path;
        noria_mysql_path.push("noria-mysql");

        // Append the deployment name with a random number to prevent state collisions
        // on test repeats with failed teardowns.
        let mut rng = rand::thread_rng();
        let name = name.to_string() + &rng.gen::<u32>().to_string();

        Self {
            name,
            noria_binaries: NoriaBinarySource {
                noria_server: noria_server_path,
                noria_mysql: Some(noria_mysql_path),
            },
            shards: None,
            quorum: 1,
            primary_region: None,
            servers: vec![],
            mysql_adapter: false,
            mysql: false,
            authority: AuthorityType::from_str(&env.authority).unwrap(),
            authority_address: env.authority_address,
            mysql_host: env.mysql_host,
            mysql_root_password: env.mysql_root_password,
            async_migration_interval: None,
        }
    }

    pub fn shards(mut self, shards: usize) -> Self {
        self.shards = Some(shards);
        self
    }

    pub fn quorum(mut self, quorum: usize) -> Self {
        self.quorum = quorum;
        self
    }

    pub fn primary_region(mut self, region: &str) -> Self {
        self.primary_region = Some(region.to_string());
        self
    }

    pub fn add_server(mut self, server: ServerParams) -> Self {
        self.servers.push(server);
        self
    }

    pub fn deploy_mysql_adapter(mut self) -> Self {
        self.mysql_adapter = true;
        self
    }

    pub fn deploy_mysql(mut self) -> Self {
        self.mysql = true;
        self
    }

    pub fn async_migrations(mut self, interval_ms: u64) -> Self {
        self.async_migration_interval = Some(interval_ms);
        self
    }

    /// Checks the set of deployment params for invalid configurations
    pub fn check_deployment_params(&self) -> anyhow::Result<()> {
        match &self.primary_region {
            Some(pr) => {
                // If the primary region is set, at least one server should match that
                // region.
                if self
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
                if self.servers.iter().any(|s| s.region.is_some()) {
                    return Err(anyhow!(
                        "Servers have region without a deployment primary region"
                    ));
                }
            }
        }
        Ok(())
    }

    /// Used to create a multi_process test deployment. This deployment
    /// connects to an authority for cluster management, and deploys a
    /// set of noria-servers. `params` can be used to setup the topology
    /// of the deployment for testing.
    pub async fn start(self) -> anyhow::Result<DeploymentHandle> {
        self.check_deployment_params()?;
        let mut port = get_next_good_port(None);
        // If this deployment includes binlog replication and a mysql instance.
        let mut mysql_addr = None;
        if self.mysql {
            // TODO(justin): Parameterize port.
            let addr = format!(
                "mysql://root:{}@{}:3306",
                &self.mysql_root_password, &self.mysql_host
            );
            let opts = mysql_async::Opts::from_url(&addr).unwrap();
            let mut conn = mysql_async::Conn::new(opts).await.unwrap();
            let _ = conn
                .query_drop(format!("CREATE DATABASE {};", &self.name))
                .await
                .unwrap();
            mysql_addr = Some(format!("{}/{}", &addr, &self.name));
        }

        // Create the noria-server instances.
        let mut handles = HashMap::new();
        for server in &self.servers {
            port = get_next_good_port(Some(port));
            let handle = start_server(
                server,
                &self.noria_binaries.noria_server,
                &self.name,
                self.shards,
                self.quorum,
                self.primary_region.as_ref(),
                &self.authority_address,
                &self.authority.to_string(),
                port,
                mysql_addr.as_ref(),
            )?;

            handles.insert(handle.addr.clone(), handle);
        }

        let authority = self
            .authority
            .to_authority(&self.authority_address, &self.name)
            .await;
        let mut handle = ControllerHandle::new(authority).await;
        wait_until_worker_count(&mut handle, Duration::from_secs(15), self.servers.len()).await?;

        // Duplicate the authority and handle creation as the metrics client
        // owns its own handle.
        let metrics_authority = self
            .authority
            .to_authority(&self.authority_address, &self.name)
            .await;
        let metrics_handle = ControllerHandle::new(metrics_authority).await;
        let metrics = MetricsClient::new(metrics_handle).unwrap();

        // Start a MySQL adapter instance.
        let mysql_adapter_handle = if self.mysql_adapter || self.mysql {
            // TODO(justin): Turn this into a stateful object.
            port = get_next_good_port(Some(port));
            let metrics_port = get_next_good_port(Some(port));
            let process = start_mysql_adapter(
                self.noria_binaries.noria_mysql.as_ref().unwrap(),
                &self.name,
                &self.authority_address,
                &self.authority.to_string(),
                port,
                metrics_port,
                mysql_addr.as_ref(),
                self.async_migration_interval,
            )?;
            // Sleep to give the adapter time to startup.
            sleep(Duration::from_millis(2000)).await;
            Some(MySQLAdapterHandle {
                conn_str: format!("mysql://127.0.0.1:{}", port),
                process,
            })
        } else {
            None
        };

        Ok(DeploymentHandle {
            handle,
            metrics,
            name: self.name.clone(),
            authority_addr: self.authority_address,
            authority: self.authority,
            mysql_addr,
            noria_server_handles: handles,
            shutdown: false,
            noria_binaries: self.noria_binaries,
            shards: self.shards,
            quorum: self.quorum,
            primary_region: self.primary_region,
            port,
            mysql_adapter: mysql_adapter_handle,
        })
    }
}

/// A handle to a single server in the deployment.
pub struct ServerHandle {
    /// The external address of the server.
    pub addr: Url,
    /// The parameters used to create the server.
    pub params: ServerParams,
    /// The local process the server is running in.
    pub process: ProcessHandle,
}

/// A handle to a mysql-adapter instance in the deployment.
pub struct MySQLAdapterHandle {
    /// The mysql connection string of the adapter.
    pub conn_str: String,
    /// The local process the adapter is running in.
    pub process: ProcessHandle,
}

/// A handle to a deployment created with `start_multi_process`.
pub struct DeploymentHandle {
    /// A handle to the current controller of the deployment.
    pub handle: ControllerHandle,
    /// Metrics client for aggregating metrics across the deployment.
    pub metrics: MetricsClient,
    /// Map from a noria server's address to a handle to the server.
    pub noria_server_handles: HashMap<Url, ServerHandle>,
    /// The name of the deployment, cluster resources are prefixed
    /// by `name`.
    name: String,
    /// The authority connect string for the deployment.
    authority_addr: String,
    /// The authority type for the deployment.
    authority: AuthorityType,
    /// The MySql connect string for the deployment.
    mysql_addr: Option<String>,
    /// A handle to each noria server in the deployment.
    /// True if this deployment has already been torn down.
    shutdown: bool,
    /// The paths to the binaries for the deployment.
    noria_binaries: NoriaBinarySource,
    /// Dataflow shards for new servers.
    shards: Option<usize>,
    /// Number of workers to wait for before starting.
    quorum: usize,
    /// The primary region of the deployment.
    primary_region: Option<String>,
    /// Next new server port.
    port: u16,
    /// Holds a handle to the mysql adapter if this deployment includes
    /// a mysql adapter.
    mysql_adapter: Option<MySQLAdapterHandle>,
}

impl DeploymentHandle {
    /// Start a new noria-server instance in the deployment.
    pub async fn start_server(&mut self, params: ServerParams) -> anyhow::Result<Url> {
        let port = get_next_good_port(Some(self.port));
        self.port = port;
        let handle = start_server(
            &params,
            &self.noria_binaries.noria_server,
            &self.name,
            self.shards,
            self.quorum,
            self.primary_region.as_ref(),
            &self.authority_addr,
            &self.authority.to_string(),
            port,
            self.mysql_addr.as_ref(),
        )?;
        let server_addr = handle.addr.clone();
        self.noria_server_handles
            .insert(server_addr.clone(), handle);

        // Wait until the worker has been created and is visible over rpc.
        wait_until_worker_count(
            &mut self.handle,
            Duration::from_secs(90),
            self.noria_server_handles.len(),
        )
        .await?;
        Ok(server_addr)
    }

    /// Kill an existing noria-server instance in the deployment referenced
    /// by `ServerHandle`.
    pub async fn kill_server(&mut self, server_addr: &Url) -> anyhow::Result<()> {
        if !self.noria_server_handles.contains_key(server_addr) {
            return Err(anyhow!("Server handle does not exist in deployment"));
        }

        let mut handle = self.noria_server_handles.remove(server_addr).unwrap();
        handle.process.kill()?;

        // Wait until the server is no longer visible in the deployment.
        wait_until_worker_count(
            &mut self.handle,
            Duration::from_secs(90),
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

        // Clean up the existing mysql state.
        if let Some(mysql_addr) = &self.mysql_addr {
            let opts = mysql_async::Opts::from_url(mysql_addr).unwrap();
            let mut conn = mysql_async::Conn::new(opts).await.unwrap();
            conn.query_drop(format!("DROP DATABASE {};", &self.name))
                .await?;
        }

        // Drop any errors on failure to kill so we complete
        // cleanup.
        for h in &mut self.noria_server_handles {
            let _ = h.1.process.kill();
        }
        if let Some(adapter_handle) = &mut self.mysql_adapter {
            let _ = adapter_handle.process.kill();
        }
        std::fs::remove_dir_all(&get_log_path(&self.name))?;

        self.shutdown = true;
        Ok(())
    }

    pub fn server_addrs(&self) -> Vec<Url> {
        self.noria_server_handles.keys().cloned().collect()
    }

    pub fn server_handles(&mut self) -> &mut HashMap<Url, ServerHandle> {
        &mut self.noria_server_handles
    }

    pub fn server_handle(&self, url: &Url) -> Option<&ServerHandle> {
        self.noria_server_handles.get(url)
    }

    pub fn mysql_connection_str(&self) -> Option<String> {
        self.mysql_adapter.as_ref().map(|h| h.conn_str.clone())
    }

    pub fn mysql_db_str(&self) -> Option<String> {
        self.mysql_addr.clone()
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
    handle: &mut ControllerHandle,
    max_wait: Duration,
    num_workers: usize,
) -> Result<()> {
    if num_workers == 0 {
        return Ok(());
    }

    let start = Instant::now();
    loop {
        let now = Instant::now();
        if (now - start) > max_wait {
            break;
        }

        // Use a timeout so if the leader died we retry quickly before the `max_wait`
        // duration.
        if let Ok(Ok(workers)) =
            tokio::time::timeout(Duration::from_secs(1), handle.healthy_workers()).await
        {
            if workers.len() == num_workers {
                return Ok(());
            }
        }

        sleep(Duration::from_millis(500)).await;
    }

    Err(anyhow!("Exceeded maximum time to wait for workers"))
}

/// Returns the path `temp_dir()`/deployment_name.
fn get_log_path(deployment_name: &str) -> PathBuf {
    std::env::temp_dir().join(deployment_name)
}

#[allow(clippy::too_many_arguments)]
fn start_server(
    server_params: &ServerParams,
    noria_server_path: &Path,
    deployment_name: &str,
    shards: Option<usize>,
    quorum: usize,
    primary_region: Option<&String>,
    authority_addr: &str,
    authority: &str,
    port: u16,
    mysql: Option<&String>,
) -> Result<ServerHandle> {
    let log_path = get_log_path(deployment_name).join(port.to_string());
    std::fs::create_dir_all(&log_path)?;

    let mut builder = NoriaServerBuilder::new(noria_server_path)
        .deployment(deployment_name)
        .external_port(port)
        .authority_addr(authority_addr)
        .authority(authority)
        .quorum(quorum)
        .log_dir(&log_path);

    if let Some(shard) = shards {
        builder = builder.shards(shard);
    }

    let region = server_params.region.as_ref();
    if let Some(region) = region {
        builder = builder.region(region);
    }
    if let Some(region) = primary_region.as_ref() {
        builder = builder.primary_region(region);
    }
    if let Some(volume) = server_params.volume_id.as_ref() {
        builder = builder.volume_id(volume);
    }
    if let Some(mysql) = mysql {
        builder = builder.mysql(mysql);
    }
    let addr = Url::parse(&format!("http://127.0.0.1:{}", port)).unwrap();
    Ok(ServerHandle {
        addr,
        process: builder.start()?,
        params: server_params.clone(),
    })
}

// TODO(justin): Wrap these parameters.
#[allow(clippy::too_many_arguments)]
fn start_mysql_adapter(
    noria_mysql_path: &Path,
    deployment_name: &str,
    authority_addr: &str,
    authority: &str,
    port: u16,
    metrics_port: u16,
    mysql: Option<&String>,
    async_migration_interval: Option<u64>,
) -> Result<ProcessHandle> {
    let mut builder = AdapterBuilder::new(noria_mysql_path)
        .deployment(deployment_name)
        .port(port)
        .metrics_port(metrics_port)
        .authority_addr(authority_addr)
        .authority(authority);

    if let Some(interval) = async_migration_interval {
        builder = builder.async_migrations(interval);
    }

    if let Some(mysql) = mysql {
        builder = builder.mysql(mysql);
    }

    builder.start()
}

/// Finds the next available port after `port` (if supplied).
/// Otherwise, it returns a random available port in the range of 20000-60000.
fn get_next_good_port(port: Option<u16>) -> u16 {
    let mut port = port.map(|p| p + 1).unwrap_or_else(|| {
        let mut rng = rand::thread_rng();
        rng.gen_range(20000..60000)
    });
    while !port_scanner::local_port_available(port) {
        port += 1;
    }
    port
}

// These tests currently require that a docker daemon is already setup
// and accessible by the user calling cargo test. As these tests interact
// with a stateful external component, the docker daemon, each test is
// responsible for cleaning up its own external state.
#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;
    // Verifies that the wrappers that create and teardown the deployment.
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn clustertest_startup_teardown_test() {
        let deployment = DeploymentBuilder::new("ct_startup_teardown")
            .add_server(ServerParams::default())
            .add_server(ServerParams::default())
            .start()
            .await;
        assert!(
            !deployment.is_err(),
            "Error starting deployment: {}",
            deployment.err().unwrap()
        );

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
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn clustertest_minimal() {
        let mut deployment = DeploymentBuilder::new("ct_minimal")
            .add_server(ServerParams::default())
            .add_server(ServerParams::default())
            .start()
            .await
            .unwrap();
        deployment.teardown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn clustertest_multiregion() {
        let mut deployment = DeploymentBuilder::new("ct_multiregion")
            .primary_region("r1")
            .add_server(ServerParams::default().with_region("r1"))
            .add_server(ServerParams::default().with_region("r2"))
            .start()
            .await
            .unwrap();
        deployment.teardown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn clustertest_server_management() {
        let mut deployment = DeploymentBuilder::new("ct_server_management")
            .primary_region("r1")
            .add_server(ServerParams::default().with_region("r1"))
            .add_server(ServerParams::default().with_region("r2"))
            .start()
            .await
            .unwrap();

        // Check that we currently have two workers.
        assert_eq!(deployment.handle.healthy_workers().await.unwrap().len(), 2);

        // Start up a new server.
        let server_handle = deployment
            .start_server(ServerParams::default().with_region("r3"))
            .await
            .unwrap();
        assert_eq!(deployment.handle.healthy_workers().await.unwrap().len(), 3);

        // Now kill that server we started up.
        deployment.kill_server(&server_handle).await.unwrap();
        assert_eq!(deployment.handle.healthy_workers().await.unwrap().len(), 2);

        deployment.teardown().await.unwrap();
    }

    #[tokio::test]
    async fn clustertest_no_server_in_primary_region_test() {
        assert!(DeploymentBuilder::new("fake_cluster")
            .primary_region("r1")
            .add_server(ServerParams::default().with_region("r2"))
            .add_server(ServerParams::default().with_region("r3"))
            .start()
            .await
            .is_err());
    }

    #[tokio::test]
    async fn clustertest_server_region_without_primary_region() {
        assert!(DeploymentBuilder::new("fake_cluster_2")
            .add_server(ServerParams::default().with_region("r1"))
            .add_server(ServerParams::default().with_region("r2"))
            .start()
            .await
            .is_err());
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn clustertest_with_binlog() {
        let mut deployment = DeploymentBuilder::new("ct_with_binlog")
            .add_server(ServerParams::default())
            .add_server(ServerParams::default())
            .deploy_mysql()
            .start()
            .await
            .unwrap();

        // Check that we currently have two workers.
        assert_eq!(deployment.handle.healthy_workers().await.unwrap().len(), 2);
        deployment.teardown().await.unwrap();
    }
}
