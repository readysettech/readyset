//! A local multi-process deployment test framework for ReadySet. It enables
//! local blackbox testing on multi-server deployments with support for
//! programatically modifying the deployment, i.e. introducing faults,
//! adding new servers, replicating readers.
//!
//! This makes this framework well suited for:
//!   * Testing fault tolerance and failure recovery behavior. Clustertests can easily introduce
//!     faults via [`kill_server`](DeploymentHandle::kill_server) and mimic failure recovery via
//!     [`start_server`](DeploymentHandle::start_server).
//!   * Testing behavior that is localized to specific workers. Each worker in a deployment can be
//!     queried for metrics separately via [`MetricsClient`] which can be checked in clustertests.
//!
//! # Preparing to run clustertests Clustertests require external resources in order to run: a MySQL
//! server, an authority (consul) and binaries for readyset-server and readyset-mysql. Clustertest
//! defaults are set to match the developer docker-compose in `//readyset` and the default flags for
//! readyset-server and readyset-mysql.
//!
//! ```bash
//! # Build the binaries with the failure injection feature for most clustertests. Binaries built
//! to //target/debug are used by default.
//! cargo build --bin readyset-server --bin readyset-mysql --features failure_injection
//!
//! # Spin up the developer docker stack to start MySQL and a consul authority.
//! cd //readyset
//! cp docker-compose.override.yml.example docker-compose.override.yml
//! docker-compose up -d
//! ```
//!
//! See [Docs/Running ReadySet](http://docs/running-readyset.html#consul--mysql-docker-stack)
//! for more information on the docker stack.
//!
//! # Running clustertests
//!
//! Clustertests are run through `cargo test`.
//! ```bash
//! # Running clustertests with the default arguments.
//! cargo test -p clustertest
//!
//! # Modifying clustertests via environment arguments.
//! MYSQL_PORT=3310 cargo test -p clustertest
//!
//! # Configuring readyset-server logging via logging environment variables.
//! LOG_LEVEL=debug cargo test -p clustertest
//! ```
//!
//! Clustertests can be configured via environment variables. Any environment variables are also
//! passed to the child readyset-server and readyset-mysql processes, as a result, these processes
//! can be futher configured through environment variables. This is helpful for configuring logging
//! environment variables, such as `LOG_LEVEL`. See
//! [Configuring Logging](http://docs/running-readyset.html#configuring-logging) for more
//! information.
//!
//! * `AUTHORITY_ADDRESS`: The address of an authority, defaults to `127.0.0.1:8500`
//!
//! * `AUTHORITY`: The type of authority, defaults to `consul`.
//!
//! * `BINARY_PATH`: The path to a directory with the readyset-server and
//! readyset-mysql binaries, defaults to `$CARGO_MANIFEST_DIR/../../target/debug`,
//! `readyset/target/debug`.
//!
//! * `MYSQL_PORT`: The host of the MySQL database to use as upstream, defaults to
//! `127.0.0.1`.
//!
//! * `MYSQL_PORT`: The port of the MySQL database to use as upstream, defaults to
//! `3306`.
//!
//! * `MYSQL_ROOT_PASSWORD`: The password to use for the upstream MySQL database,
//! defaults to `noria`.
//!
//! * `RUN_SLOW_TESTS`: Enables running certain tests that are slow.
//!
//! # Example Clustertest
//!
//! Creating a two server deployment, creating a third server, and then killing a
//! server.
//!
//! ```rust
//! use readyset_clustertest::*;
//! use readyset_clustertest_macros::clustertest;
//!
//! #[clustertest]
//! async fn example_clustertest() {
//!     // DeploymentBuilder is a builder used to create the local
//!     // deployment.
//!     let mut deployment = DeploymentBuilder::new("ct_example")
//!         .with_servers(2, ServerParams::default())
//!         .start()
//!         .await
//!         .unwrap();
//!
//!     // Check that we currently have two workers.
//!     assert_eq!(
//!         deployment
//!             .leader_handle()
//!             .healthy_workers()
//!             .await
//!             .unwrap()
//!             .len(),
//!         2
//!     );
//!
//!     // Start up a new server.
//!     let server_handle = deployment
//!         .start_server(ServerParams::default())
//!         .await
//!         .unwrap();
//!     assert_eq!(
//!         deployment
//!             .leader_handle()
//!             .healthy_workers()
//!             .await
//!             .unwrap()
//!             .len(),
//!         3
//!     );
//!
//!     // Now kill that server we started up.
//!     deployment.kill_server(&server_handle).await.unwrap();
//!     assert_eq!(
//!         deployment
//!             .leader_handle()
//!             .healthy_workers()
//!             .await
//!             .unwrap()
//!             .len(),
//!         2
//!     );
//!
//!     // Clustertests must cleanup their state via deployment.teardown().
//!     deployment.teardown().await.unwrap();
//! }
//! ```
//!
//! # Anatomy of a clustertest
//!
//! ### `#[clustertest]`
//!
//! Clustertests begin with the clustertest attribute: `#[clustertest]`. This
//! creates clustertests as a multi-threaded tokio test, that is run serially
//! with other tests (`#[serial]`).
//!
//! ### [`DeploymentBuilder`]
//!
//! The [`DeploymentBuilder`] is used to specify parameters for a deployment,
//! for example:
//!   * the number of readyset-server instances to create,
//!     [`with_servers`](DeploymentBuilder::with_servers).
//!   * whether to use an upstream database, [`deploy_mysql`](DeploymentBuilder::deploy_mysql).
//!   * whether to deploy an adapter,
//!     [`deploy_mysql_adapter`](DeploymentBuilder::deploy_mysql_adapter).
//!   * The parameters used to build a server, [`ServerParams`].
//!
//! Once all parameters are specified, creating the resources for the deployment
//! is done with [`DeploymentBuilder::start`].
//!
//! ```rust
//! use readyset_clustertest::*;
//! // Deploy a three server deployment with different volume IDs for each server, a mysql adapter,
//! // and upstream database.
//! async fn build_deployment() {
//!     let mut deployment = DeploymentBuilder::new("ct_example")
//!         .add_server(ServerParams::default().with_volume("v1"))
//!         .add_server(ServerParams::default().with_volume("v2"))
//!         .add_server(ServerParams::default().with_volume("v3"))
//!         .quorum(3)
//!         .deploy_mysql()
//!         .deploy_mysql_adapter()
//!         .start()
//!         .await
//!         .unwrap();
//! }
//! ```
//!
//! Calling [`DeploymentBuilder::start`] creates a [`DeploymentHandle`] that
//! can be used to modify a deployment
//!
//! ### [`DeploymentHandle`]
//!
//! The [`DeploymentHandle`] allows clustertests writers to programatically
//! modify the deployment and check controller / metrics properties. It
//! primarily facilitates four operations:
//!   1. Adding servers to the deployment. Clustertest can create any number
//!      of server processes during the test via
//!      [`DeploymentHandle::start_server`].
//!   2. Killing server in the deployment.
//!      [`DeploymentHandle::kill_server`] may be called to
//!      remove a server from the deployment, this is done by sending the kill
//!      command to the process running the server.
//!   3. Sending controller RPCs via the [`ControllerHandle`] returned by
//!      [`DeploymentHandle::leader_handle`].
//!   4. Querying metrics via the [`MetricsClient`] returned by
//!      [`DeploymentHandle::metrics`].
//!
//! It also provides helper functions to create connections to a ReadySet adapter
//! and an upstream database, if present. See [`DeploymentHandle::adapter`] and
//! [`DeploymentHandle::upstream`], both of which return [`mysql_async::Conn`] to
//! their respective database endpoints.
//!
//! ### [`DeploymentHandle::teardown`]
//!
//! All tests should end in a call to [`DeploymentHandle::teardown`]. To
//! kill the deployment proceses and remove locally created files.

mod server;

#[cfg(test)]
mod readyset;
#[cfg(test)]
mod readyset_mysql;
#[cfg(test)]
mod utils;

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::str::FromStr;
use std::time::{Duration, Instant};

use ::readyset::consensus::AuthorityType;
use ::readyset::metrics::client::MetricsClient;
use ::readyset::{ControllerHandle, ReadySetResult};
use anyhow::{anyhow, Result};
use futures::executor;
use hyper::Client;
use mysql_async::prelude::Queryable;
use rand::Rng;
#[cfg(test)]
use readyset_clustertest_macros::clustertest;
use serde::Deserialize;
use server::{AdapterBuilder, ProcessHandle, ReadysetServerBuilder};
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
    #[serde(default = "default_mysql_port")]
    mysql_port: String,
    #[serde(default = "default_root_password")]
    mysql_root_password: String,
}

fn default_authority_address() -> String {
    "127.0.0.1:8500".to_string()
}

fn default_mysql_host() -> String {
    "127.0.0.1".to_string()
}

fn default_mysql_port() -> String {
    "3306".to_string()
}

fn default_authority() -> String {
    "consul".to_string()
}

fn default_binary_path() -> PathBuf {
    // Convert from <dir>/noria/clustertest to <dir>/target/debug.
    let mut path: PathBuf = std::env::var("CARGO_MANIFEST_DIR").unwrap().into();
    path.pop();
    path.push("target/debug");
    path
}

fn default_root_password() -> String {
    "noria".to_string()
}

/// Source of the readyset binaries.
pub(crate) struct ReadySetBinarySource {
    /// Path to a built readyset-server on the local machine.
    pub readyset_server: PathBuf,
    /// Optional path to readyset-mysql on the local machine. readyset-mysql
    /// may not be included in the build.
    pub readyset_mysql: Option<PathBuf>,
}

/// Parameters for a single readyset-server instance.
#[must_use]
#[derive(Clone, Default)]
pub struct ServerParams {
    /// The volume id of the server, passed in via `--volume-id`.
    volume_id: Option<String>,
    /// Prevent this server from running domains containing readers. Corresponds to the
    /// `--no-readers` flag to the readyset server binary
    no_readers: bool,
    /// Only allow domains containing readers to run on this server. Corresponds to the
    /// `--reader-only` flag to the readyset server binary
    reader_only: bool,
}

impl ServerParams {
    /// Sets a server's --volume-id string, passed in via --volume-id.
    pub fn with_volume(mut self, volume: &str) -> Self {
        self.volume_id = Some(volume.to_string());
        self
    }

    /// Configure this server to never run domains containing reader nodes
    pub fn no_readers(mut self) -> Self {
        self.no_readers = true;
        self
    }

    /// Configure this server to only run domains containing reader nodes
    pub fn reader_only(mut self) -> Self {
        self.reader_only = true;
        self
    }
}

#[must_use]
#[derive(Clone)]
pub struct ServerStartParams {
    /// Absolute path to the readyset-server binary
    readyset_server_path: PathBuf,
    /// Name of the deployment.
    deployment_name: String,
    /// Number of shards for dataflow nodes.
    shards: Option<usize>,
    /// Number of workers to wait for before starting.
    quorum: usize,
    /// The authority connect string the server is configured to use.
    authority_address: String,
    /// The authority type the server is configured to use.
    authority_type: String,
    /// Replicator restart timeout in seconds.
    replicator_restart_timeout_secs: Option<u64>,
    /// Number of times to replicate reader domains.
    reader_replicas: Option<usize>,
    /// Whether or not to auto restart the server process.
    auto_restart: bool,
}

/// Set of parameters defining an entire cluster's topology.
#[must_use]
pub struct DeploymentBuilder {
    /// Name of the cluster, cluster resources will be prefixed
    /// with this name.
    name: String,
    /// Source of the binaries.
    readyset_binaries: ReadySetBinarySource,
    /// Number of shards for dataflow nodes.
    shards: Option<usize>,
    /// Number of workers to wait for before starting.
    quorum: usize,
    /// Parameters for the set of readyset-server instances in the deployment.
    servers: Vec<ServerParams>,
    /// How many readyset-mysql adapter instances to deploy
    mysql_adapters: usize,
    /// Deploy mysql and use binlog replication.
    mysql: bool,
    /// The type of authority to use for cluster management.
    authority: AuthorityType,
    /// The address of the authority.
    authority_address: String,
    /// The host of the mysql db.
    mysql_host: String,
    /// The port of the mysql db.
    mysql_port: String,
    /// The root password for the mysql db.
    mysql_root_password: String,
    /// Are async migrations enabled on the adapter.
    async_migration_interval: Option<u64>,
    /// Enables explicit migrations, and passes in an interval for running dry run migrations that
    /// determine whether queries that weren't explicitly migrated would be supported by ReadySet.
    /// Exposed via the `SHOW PROXIED QUERIES` command.
    dry_run_migration_interval: Option<u64>,
    /// The max time in seconds that a query may continuously fail until we enter a recovery
    /// period. None if not enabled.
    query_max_failure_seconds: Option<u64>,
    /// The period in seconds that we enter into a fallback recovery mode for a given query, if
    /// that query has continously failed for query_max_failure_seconds.
    /// None if not enabled.
    fallback_recovery_seconds: Option<u64>,
    /// Specifies the polling interval for the adapter to request views from the Leader.
    ///
    /// Corresponds to [`readyset_client_adapter::Options::views_polling_interval`]
    views_polling_interval: Duration,
    /// Optional username for the MySQL user.
    mysql_user: Option<String>,
    /// Optional password for the MySQL user.
    mysql_pass: Option<String>,
    /// Replicator restart timeout in seconds.
    replicator_restart_timeout_secs: Option<u64>,
    /// Number of times to replicate reader domains
    reader_replicas: Option<usize>,
    /// If true, will automatically restart the server/adapter processes
    auto_restart: bool,
}

impl DeploymentBuilder {
    pub fn new(name: &str) -> Self {
        let env = envy::from_env::<Env>().unwrap();

        let mut readyset_server_path = env.binary_path.clone();
        readyset_server_path.push("readyset-server");

        let mut readyset_mysql_path = env.binary_path;
        readyset_mysql_path.push("readyset-mysql");

        // Append the deployment name with a random number to prevent state collisions
        // on test repeats with failed teardowns.
        let mut rng = rand::thread_rng();
        let name = name.to_string() + &rng.gen::<u32>().to_string();

        Self {
            name,
            readyset_binaries: ReadySetBinarySource {
                readyset_server: readyset_server_path,
                readyset_mysql: Some(readyset_mysql_path),
            },
            shards: None,
            quorum: 1,
            servers: vec![],
            mysql_adapters: 0,
            mysql: false,
            authority: AuthorityType::from_str(&env.authority).unwrap(),
            authority_address: env.authority_address,
            mysql_host: env.mysql_host,
            mysql_port: env.mysql_port,
            mysql_root_password: env.mysql_root_password,
            async_migration_interval: None,
            dry_run_migration_interval: None,
            query_max_failure_seconds: None,
            fallback_recovery_seconds: None,
            views_polling_interval: Duration::from_secs(300),
            mysql_user: None,
            mysql_pass: None,
            replicator_restart_timeout_secs: None,
            reader_replicas: None,
            auto_restart: false,
        }
    }

    /// The number of shards in the graph, `shards` <= 1 disables sharding.
    pub fn shards(mut self, shards: usize) -> Self {
        self.shards = Some(shards);
        self
    }

    /// The number of healthy servers required in the system before we begin
    /// accepting queries and performing migrations.
    pub fn quorum(mut self, quorum: usize) -> Self {
        self.quorum = quorum;
        self
    }

    /// Adds `count` servers to the deployment, each created with the specified
    /// [`ServerParams`].
    pub fn with_servers(mut self, count: u32, server: ServerParams) -> Self {
        for _ in 0..count {
            self.servers.push(server.clone());
        }
        self
    }

    /// Adds a single server to the deployment, created with the specified
    /// [`ServerParams`].
    pub fn add_server(mut self, server: ServerParams) -> Self {
        self.servers.push(server);
        self
    }

    /// Deploys a single adapter server as part of this deployment.
    ///
    /// This will overwrite any previous call to [`Self::set_mysql_adapters`]
    pub fn deploy_mysql_adapter(mut self) -> Self {
        self.mysql_adapters = 1;
        self
    }

    /// Sets the number of mysql adapter servers to deploy as part of this deployment.
    ///
    /// This will overwrite any previous call to [`Self::deploy_mysql_adapter`]
    pub fn with_mysql_adapters(mut self, num_adapters: usize) -> Self {
        self.mysql_adapters = num_adapters;
        self
    }

    /// Whether to use an upstream database as part of this deployment. This
    /// will populate [`DeploymentHandle::upstream_connection_str`] with the upstream
    /// databases's connection string.
    pub fn deploy_mysql(mut self) -> Self {
        self.mysql = true;
        self
    }

    /// Whether to enable the async migrations feature in the adapter. Requires
    /// [`Self::deploy_mysql_adapter`] to be set on this deployment.
    pub fn async_migrations(mut self, interval_ms: u64) -> Self {
        self.async_migration_interval = Some(interval_ms);
        self
    }

    /// Whether to enable the explicit migrations feature in the adapter. Requires
    /// [`Self::deploy_mysql_adapter`] to be set on this deployment.
    /// Must supply an interval for the dry run loop.
    pub fn explicit_migrations(mut self, interval_ms: u64) -> Self {
        self.dry_run_migration_interval = Some(interval_ms);
        self
    }

    /// Overrides the maximum time a query may continuously fail in the adapter.
    /// Requires [`Self::deploy_mysql_adapter`] to be set on this deployment.
    pub fn query_max_failure_seconds(mut self, secs: u64) -> Self {
        self.query_max_failure_seconds = Some(secs);
        self
    }

    /// Overrides the fallback recovery period in the adapter that we enter when a query has
    /// repeatedly failed for query_max_failure_seconds.
    /// Requires [`Self::deploy_mysql_adapter`] to be set on this deployment.
    pub fn fallback_recovery_seconds(mut self, secs: u64) -> Self {
        self.fallback_recovery_seconds = Some(secs);
        self
    }

    /// Overrides the interval at which to poll for updated view status.
    /// Requires [`Self::deploy_mysql_adapter`] to be set on this deployment.
    pub fn views_polling_interval(mut self, interval: Duration) -> Self {
        self.views_polling_interval = interval;
        self
    }

    /// Sets the value of the MySQL user and password to use for the upstream database in the
    /// adapter and the server. The upstream connection returned will always be the root connection
    /// to allow making changes without worrying about permissions to the upstream database.
    pub fn with_user(mut self, user: &str, pass: &str) -> Self {
        self.mysql_user = Some(user.to_string());
        self.mysql_pass = Some(pass.to_string());
        self
    }

    /// Sets the amount of time that the replicator should wait before restarting in seconds.
    pub fn replicator_restart_timeout(mut self, secs: u64) -> Self {
        self.replicator_restart_timeout_secs = Some(secs);
        self
    }

    /// Sets the number of times to replicate reader domains
    pub fn reader_replicas(mut self, replicas: usize) -> Self {
        self.reader_replicas = Some(replicas);
        self
    }

    /// Sets whether or not to restart the adapter/server processes
    pub fn auto_restart(mut self, auto_restart: bool) -> Self {
        self.auto_restart = auto_restart;
        self
    }

    pub fn adapter_start_params(&self) -> AdapterStartParams {
        AdapterStartParams {
            deployment_name: self.name.clone(),
            readyset_mysql_path: self.readyset_binaries.readyset_mysql.clone().unwrap(),
            authority_address: self.authority_address.clone(),
            authority_type: self.authority.to_string(),
            async_migration_interval: self.async_migration_interval,
            dry_run_migration_interval: self.dry_run_migration_interval,
            query_max_failure_seconds: self.query_max_failure_seconds,
            fallback_recovery_seconds: self.fallback_recovery_seconds,
            auto_restart: self.auto_restart,
            views_polling_interval: self.views_polling_interval,
        }
    }

    pub fn server_start_params(&self) -> ServerStartParams {
        ServerStartParams {
            readyset_server_path: self.readyset_binaries.readyset_server.clone(),
            deployment_name: self.name.clone(),
            shards: self.shards,
            quorum: self.quorum,
            authority_address: self.authority_address.clone(),
            authority_type: self.authority.to_string(),
            replicator_restart_timeout_secs: self.replicator_restart_timeout_secs,
            reader_replicas: self.reader_replicas,
            auto_restart: self.auto_restart,
        }
    }

    /// Starts the local multi-process deployment after running a set of commands in the
    /// upstream database. This can be useful for checking snapshotting properties. This also
    /// includes the `leader_timeout` parameter, how long to wait for the leader to be ready,
    /// since certain configurations may make it so that the leader is *never* ready.
    pub async fn start_with_seed<'a, Q>(
        self,
        cmds: &[Q],
        leader_timeout: Duration,
    ) -> anyhow::Result<DeploymentHandle>
    where
        Q: AsRef<str> + Send + Sync + 'a,
    {
        let mut port = get_next_good_port(None);
        // If this deployment includes binlog replication and a mysql instance.
        let mut upstream_mysql_addr = None;
        let server_upstream = if self.mysql {
            let root_addr = format!(
                "mysql://root:{}@{}:{}",
                &self.mysql_root_password, &self.mysql_host, &self.mysql_port
            );
            upstream_mysql_addr = Some(format!("{}/{}", &root_addr, &self.name));
            let opts = mysql_async::Opts::from_url(&root_addr).unwrap();
            let mut conn = mysql_async::Conn::new(opts).await.unwrap();
            conn.query_drop(format!(
                "CREATE DATABASE {}; USE {}",
                &self.name, &self.name
            ))
            .await
            .unwrap();

            for c in cmds {
                conn.query_drop(&c).await?;
            }

            let user = self
                .mysql_user
                .clone()
                .unwrap_or_else(|| "root".to_string());
            let pass = self
                .mysql_pass
                .clone()
                .unwrap_or_else(|| self.mysql_root_password.clone());

            let user_addr = format!(
                "mysql://{}:{}@{}:{}",
                &user, &pass, &self.mysql_host, &self.mysql_port
            );

            Some(format!("{}/{}", &user_addr, &self.name))
        } else {
            None
        };

        // Create the readyset-server instances.
        let mut handles = HashMap::new();
        for server in &self.servers {
            port = get_next_good_port(Some(port));
            let handle = start_server(
                port,
                server_upstream.as_ref(),
                server,
                &self.server_start_params(),
            )
            .await?;

            handles.insert(handle.addr.clone(), handle);
        }

        let authority = self
            .authority
            .to_authority(&self.authority_address, &self.name)
            .await;
        let handle = ControllerHandle::new(authority).await;

        // Duplicate the authority and handle creation as the metrics client
        // owns its own handle.
        let metrics_authority = self
            .authority
            .to_authority(&self.authority_address, &self.name)
            .await;
        let metrics_handle = ControllerHandle::new(metrics_authority).await;
        let metrics = MetricsClient::new(metrics_handle).unwrap();

        // Start `self.mysql_adapters` MySQL adapter instances.
        let mut mysql_adapters = Vec::with_capacity(self.mysql_adapters);
        for _ in 0..self.mysql_adapters {
            port = get_next_good_port(Some(port));
            let metrics_port = get_next_good_port(Some(port));
            let process = start_mysql_adapter(
                server_upstream.clone(),
                port,
                metrics_port,
                &self.adapter_start_params(),
            )
            .await?;

            // Sleep to give the adapter time to startup.
            let handle = AdapterHandle {
                conn_str: format!("mysql://127.0.0.1:{}", port),
                process,
            };

            wait_for_adapter_startup(metrics_port, Duration::from_millis(2000)).await?;

            mysql_adapters.push(handle);
        }

        let mut handle = DeploymentHandle {
            handle,
            metrics,
            name: self.name.clone(),
            adapter_start_params: self.adapter_start_params(),
            server_start_params: self.server_start_params(),
            upstream_mysql_addr,
            readyset_server_handles: handles,
            shutdown: false,
            port,
            mysql_adapters,
        };

        handle.wait_for_workers(Duration::from_secs(90)).await?;
        handle.backend_ready(leader_timeout).await?;

        Ok(handle)
    }

    /// Creates the local multi-process deployment from the set of parameters
    /// specified in the builder.
    pub async fn start(self) -> anyhow::Result<DeploymentHandle> {
        self.start_with_seed::<String>(&[], Duration::from_secs(90))
            .await
    }
}

/// Waits for AdapterHandle to be healthy, up to the provided timeout.
async fn wait_for_adapter_startup(metrics_port: u16, timeout: Duration) -> anyhow::Result<()> {
    let health_check_url = format!("http://127.0.0.1:{}/health", metrics_port);
    let poll_interval = timeout.checked_div(10).expect("timeout must be valid");

    // Polls the health_check_url continuously until it returns OK.
    async fn health_check_poller(
        health_check_url: String,
        poll_interval: Duration,
    ) -> anyhow::Result<()> {
        loop {
            let r = hyper::Request::get(&health_check_url)
                .body(hyper::Body::from(String::default()))
                .unwrap();

            let client = Client::new();
            // If this http requests returns an error, the adapter http server may not be ready yet.
            // If it returns something that isn't StatusCode::Ok, it is unhealthy and may eventually
            // become healthy.
            if let Ok(r) = client.request(r).await {
                if r.status() == hyper::StatusCode::OK {
                    break Ok(());
                }
            }
            sleep(poll_interval).await;
        }
    }

    tokio::time::timeout(
        timeout,
        health_check_poller(health_check_url, poll_interval),
    )
    .await?
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

impl ServerHandle {
    pub async fn check_alive(&mut self) -> bool {
        self.process.check_alive().await
    }

    pub async fn set_failpoint(&mut self, name: &str, action: &str) {
        if !self.check_alive().await {
            return;
        }

        let data = bincode::serialize(&(name, action)).unwrap();
        let string_url = self.addr.to_string() + "failpoint";
        let r = hyper::Request::get(string_url)
            .body(hyper::Body::from(data))
            .unwrap();

        let client = Client::new();
        // If this http requests returns an error, we probably killed off the
        // server. If it returns something that isn't StatusCode::Ok, there
        // is probably a problem with the controller HTTP logic.
        if let Ok(r) = client.request(r).await {
            let status = r.status();
            assert!(status == hyper::StatusCode::OK);
        }
    }
}

/// A handle to a mysql-adapter instance in the deployment.
pub struct AdapterHandle {
    /// The mysql connection string of the adapter.
    pub conn_str: String,
    /// The local process the adapter is running in.
    pub process: ProcessHandle,
}

/// A handle to a deployment created with `start_multi_process`.
pub struct DeploymentHandle {
    /// A handle to the current controller of the deployment.
    handle: ControllerHandle,
    /// Metrics client for aggregating metrics across the deployment.
    metrics: MetricsClient,
    /// Map from a readyset server's address to a handle to the server.
    readyset_server_handles: HashMap<Url, ServerHandle>,
    /// Holds a list of handles to the mysql adapters for this deployment, if any
    mysql_adapters: Vec<AdapterHandle>,
    /// The name of the deployment, cluster resources are prefixed
    /// by `name`.
    name: String,
    /// The configured parameters with which to start new adapters: in the deployment.
    adapter_start_params: AdapterStartParams,
    /// The configured parameters with which to start new servers in the deployment.
    server_start_params: ServerStartParams,
    /// The connection string of the upstream mysql database for the deployment.
    upstream_mysql_addr: Option<String>,
    /// A handle to each readyset server in the deployment.
    /// True if this deployment has already been torn down.
    shutdown: bool,
    /// Next new server port.
    port: u16,
}

impl DeploymentHandle {
    /// Returns the [`ServerStartParams`] for the deployment
    pub fn server_start_params(&mut self) -> &mut ServerStartParams {
        &mut self.server_start_params
    }

    /// Returns the [`AdapterStartParams`] for the deployment
    pub fn adapter_start_params(&mut self) -> &mut AdapterStartParams {
        &mut self.adapter_start_params
    }

    /// Returns a [`ControllerHandle`] that enables sending RPCs to the leader
    /// of the deployment.
    pub fn leader_handle(&mut self) -> &mut ControllerHandle {
        &mut self.handle
    }

    /// Returns a [`MetricsClient`] for the deployment. Tests can query for
    /// metrics for each server via [`MetricsClient::get_metrics_for_server`]
    /// with the URL from `server_addrs` or `server_handles`.
    pub fn metrics(&mut self) -> &mut MetricsClient {
        &mut self.metrics
    }

    /// Creates a [`mysql_async::Conn`] to the first MySQL adapter in the deployment.
    ///
    /// # Panics
    ///
    /// Panics if the adapter does not exist or a connection can not be made.
    pub async fn first_adapter(&self) -> mysql_async::Conn {
        self.adapter(0).await
    }

    /// Creates a [`mysql_async::Conn`] to the MySQL adapter at the given index in the deployment.
    ///
    /// # Panics
    ///
    /// Panics if the adapter does not exist or a connection can not be made.
    pub async fn adapter(&self, idx: usize) -> mysql_async::Conn {
        let addr = &self.mysql_adapters[idx].conn_str;
        let opts = mysql_async::Opts::from_url(addr).unwrap();
        mysql_async::Conn::new(opts.clone()).await.unwrap()
    }

    /// Creates a [`mysql_async::Conn`] to the upstream database in the deployment.
    /// Otherwise panics if the upstream database does not exist or a connection
    /// can not be made.
    pub async fn upstream(&self) -> mysql_async::Conn {
        let addr = self.upstream_mysql_addr.as_ref().unwrap();
        let opts = mysql_async::Opts::from_url(addr).unwrap();
        mysql_async::Conn::new(opts.clone()).await.unwrap()
    }

    /// Returns the expected number of workers alive within the deployment based
    /// on the liveness of the server processes.
    pub async fn expected_workers(&mut self) -> HashSet<Url> {
        let mut alive = HashSet::new();
        for s in self.server_handles().values_mut() {
            if s.check_alive().await {
                alive.insert(s.addr.clone());
            }
        }
        alive
    }

    /// Queries the controller in the deployment for the number of workers and
    /// loops until the number of healthy workers in the deployment matches the
    /// expected number of worker processes that are expected. If a worker is
    /// not found by `max_wait`, an error is returned.
    pub async fn wait_for_workers(&mut self, max_wait: Duration) -> Result<()> {
        if self.expected_workers().await.is_empty() {
            return Ok(());
        }

        let start = Instant::now();
        loop {
            let now = Instant::now();
            if (now - start) > max_wait {
                break;
            }

            let expected_workers = self.expected_workers().await;

            // Use a timeout so if the leader died we retry quickly before the `max_wait`
            // duration.
            if let Ok(Ok(workers)) =
                tokio::time::timeout(Duration::from_secs(1), self.handle.healthy_workers()).await
            {
                if workers.len() == expected_workers.len()
                    && workers.iter().all(|w| expected_workers.contains(w))
                {
                    return Ok(());
                }
            }

            sleep(Duration::from_millis(500)).await;
        }

        Err(anyhow!("Exceeded maximum time to wait for workers"))
    }

    /// Start a new readyset-server instance in the deployment using the provided
    /// [`ServerParams`]. Any deployment-wide configuration parameters, such as
    /// sharing, quorum, authority, are passed to the new server.
    pub async fn start_server(
        &mut self,
        params: ServerParams,
        wait_for_startup: bool,
    ) -> anyhow::Result<Url> {
        let port = get_next_good_port(Some(self.port));
        self.port = port;
        let handle = start_server(
            port,
            self.upstream_mysql_addr.clone().as_ref(),
            &params,
            self.server_start_params(),
        )
        .await?;
        let server_addr = handle.addr.clone();
        self.readyset_server_handles
            .insert(server_addr.clone(), handle);

        if wait_for_startup {
            self.wait_for_workers(Duration::from_secs(90)).await?;
        }

        Ok(server_addr)
    }

    /// Start a new readyset-mysql adapter instance in the deployment
    pub async fn start_mysql_adapter(&mut self, wait_for_startup: bool) -> anyhow::Result<()> {
        let port = get_next_good_port(Some(self.port));
        let metrics_port = get_next_good_port(Some(port));
        self.port = port;
        let process = start_mysql_adapter(
            self.upstream_mysql_addr.clone(),
            port,
            metrics_port,
            self.adapter_start_params(),
        )
        .await?;

        if wait_for_startup {
            wait_for_adapter_startup(metrics_port, Duration::from_millis(2000)).await?;
        }

        self.mysql_adapters.push(AdapterHandle {
            conn_str: format!("mysql://127.0.0.1:{}", port),
            process,
        });

        Ok(())
    }

    /// Waits for the back-end to return that it is ready to process queries.
    pub async fn backend_ready(&mut self, timeout: Duration) -> ReadySetResult<()> {
        let mut e = None;
        let start = Instant::now();
        let check_leader_loop = async {
            loop {
                let remaining = std::cmp::min(Duration::from_secs(5), timeout - start.elapsed());
                let res = tokio::time::timeout(remaining, self.handle.leader_ready()).await;
                match res {
                    Ok(Ok(true)) => return,
                    Ok(Err(rpc_err)) => e = Some(rpc_err),
                    Ok(_) => {
                        // Any other Ok case means we didn't hit the tokio timeout, so we should
                        // still sleep to ensure we aren't spamming RPCs.
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                    Err(_) => {
                        // We hit the timeout, which is pretty long, so we can just try again right
                        // away.
                        continue;
                    }
                }
            }
        };
        if tokio::time::timeout(timeout, check_leader_loop)
            .await
            .is_err()
        {
            if let Some(e) = e {
                return Err(e);
            }
        }

        Ok(())
    }

    /// Kills an existing readyset-server instance in the deployment referenced
    /// by `Url`.
    pub async fn kill_server(
        &mut self,
        server_addr: &Url,
        wait_for_removal: bool,
    ) -> anyhow::Result<()> {
        if !self.readyset_server_handles.contains_key(server_addr) {
            return Err(anyhow!("Server handle does not exist in deployment"));
        }

        let mut handle = self.readyset_server_handles.remove(server_addr).unwrap();
        handle.process.kill().await?;

        if wait_for_removal {
            self.wait_for_workers(Duration::from_secs(90)).await?;
        }

        Ok(())
    }

    /// Tears down any resources associated with the deployment.
    pub async fn teardown(&mut self) -> anyhow::Result<()> {
        if self.shutdown {
            return Ok(());
        }

        // Drop any errors on failure to kill so we complete
        // cleanup.
        for h in &mut self.readyset_server_handles {
            // Ignore the result: If the kill() errors, we still want to attempt cleaning up the
            // resources below
            let _ = h.1.process.kill().await;
        }
        for adapter_handle in &mut self.mysql_adapters {
            // Ignore the result: If the kill() errors, we still want to attempt cleaning up the
            // resources below
            let _ = adapter_handle.process.kill().await;
        }

        // Clean up the existing mysql state.
        if let Some(upstream_mysql_addr) = &self.upstream_mysql_addr {
            let opts = mysql_async::Opts::from_url(upstream_mysql_addr).unwrap();
            let mut conn = mysql_async::Conn::new(opts).await.unwrap();
            conn.query_drop(format!("DROP DATABASE {};", &self.name))
                .await?;
        }

        self.shutdown = true;
        Ok(())
    }

    /// Returns a vector of readyset-server controller addresses.
    pub fn server_addrs(&self) -> Vec<Url> {
        self.readyset_server_handles.keys().cloned().collect()
    }

    /// Returns a mutable map from readyset-server addresses to their
    /// [`ServerHandle`].
    pub fn server_handles(&mut self) -> &mut HashMap<Url, ServerHandle> {
        &mut self.readyset_server_handles
    }

    /// Returns the [`ServerHandle`] for a readyset-server if `url` is in the
    /// deployment. Otherwise, `None` is returned.
    pub fn server_handle(&mut self, url: &Url) -> Option<&mut ServerHandle> {
        self.readyset_server_handles.get_mut(url)
    }

    /// Returns a mutable reference to the [`AdapterHandle`] for the first readyset-adapter if one
    /// exists Otherwise, `None` is returned.
    pub fn first_adapter_handle(&mut self) -> Option<&mut AdapterHandle> {
        self.adapter_handle(0)
    }

    /// Returns a mutable reference to the list of [`AdapterHandle`]s for the deployment.
    /// index. May be empty if no adapters are running
    pub fn adapter_handles(&mut self) -> &mut Vec<AdapterHandle> {
        self.mysql_adapters.as_mut()
    }

    /// Returns a mutable reference to the [`AdapterHandle`] for the readyset-adapter at the given
    /// index. Returns `None` if the index is out-of-bounds
    pub fn adapter_handle(&mut self, idx: usize) -> Option<&mut AdapterHandle> {
        self.mysql_adapters.get_mut(idx)
    }

    /// Returns a reference to the name of the deployment.
    ///
    /// This is used to prefix cluster resources, and as the name of the database created in the
    /// upstream
    pub fn name(&self) -> &str {
        self.name.as_ref()
    }
}

impl Drop for DeploymentHandle {
    // Attempt to clean up any resources used by the DeploymentHandle. Drop
    // will be called on test panics allowing resources to be cleaned up.
    // TODO(justin): This does not always work if a test does not cleanup
    // with teardown explicitly, leading to readyset-server instances living.
    #[allow(unused_must_use)]
    fn drop(&mut self) {
        executor::block_on(self.teardown());
    }
}

pub struct AdapterStartParams {
    /// Absolute path to the readyset-mysql binary.
    readyset_mysql_path: PathBuf,
    /// Name of the deployment.
    deployment_name: String,
    /// The authority connect string the adapter is configured to use.
    authority_address: String,
    /// The authority type the adapter is configured to use.
    authority_type: String,
    /// The interval at which to perform async migrations, if Some, else None indicates that async
    /// migrations are not enabled.
    async_migration_interval: Option<u64>,
    /// Enables explicit migrations, and passes in an interval for running dry run migrations that
    /// determine whether queries that weren't explicitly migrated would be supported by ReadySet.
    /// Exposed via the `SHOW PROXIED QUERIES` command.
    dry_run_migration_interval: Option<u64>,
    /// The max time in seconds that a query may continuously fail until we enter a recovery
    /// period. None if not enabled.
    query_max_failure_seconds: Option<u64>,
    /// The period in seconds that we enter into a fallback recovery mode for a given query, if
    /// that query has continously failed for query_max_failure_seconds.
    /// None if not enabled.
    fallback_recovery_seconds: Option<u64>,
    /// Whether or not to automatically restart the adapter process.
    auto_restart: bool,
    /// Specifies the polling interval for the adapter to request views from the Leader.
    ///
    /// Corresponds to [`readyset_client_adapter::Options::views_polling_interval`]
    views_polling_interval: Duration,
}

async fn start_server(
    port: u16,
    mysql: Option<&String>,
    server_params: &ServerParams,
    server_start_params: &ServerStartParams,
) -> Result<ServerHandle> {
    let mut builder = ReadysetServerBuilder::new(server_start_params.readyset_server_path.as_ref())
        .deployment(server_start_params.deployment_name.as_str())
        .external_port(port)
        .authority_addr(&server_start_params.authority_address)
        .authority(&server_start_params.authority_type)
        .quorum(server_start_params.quorum)
        .auto_restart(server_start_params.auto_restart);

    if let Some(shard) = server_start_params.shards {
        builder = builder.shards(shard);
    }

    if let Some(volume) = server_params.volume_id.as_ref() {
        builder = builder.volume_id(volume);
    }
    if server_params.reader_only {
        builder = builder.reader_only();
    }
    if server_params.no_readers {
        builder = builder.no_readers();
    }
    if let Some(mysql) = mysql {
        builder = builder.mysql(mysql);
    }
    if let Some(t) = server_start_params.replicator_restart_timeout_secs {
        builder = builder.replicator_restart_timeout(t);
    }
    if let Some(rs) = server_start_params.reader_replicas {
        builder = builder.reader_replicas(rs);
    }
    let addr = Url::parse(&format!("http://127.0.0.1:{}", port)).unwrap();
    Ok(ServerHandle {
        addr,
        process: builder.start().await?,
        params: server_params.clone(),
    })
}

async fn start_mysql_adapter(
    server_upstream: Option<String>,
    port: u16,
    metrics_port: u16,
    params: &AdapterStartParams,
) -> Result<ProcessHandle> {
    let mut builder = AdapterBuilder::new(params.readyset_mysql_path.as_ref())
        .deployment(&params.deployment_name)
        .port(port)
        .metrics_port(metrics_port)
        .authority_addr(params.authority_address.as_str())
        .authority(params.authority_type.as_str())
        .auto_restart(params.auto_restart)
        .views_polling_interval(params.views_polling_interval);

    if let Some(interval) = params.async_migration_interval {
        builder = builder.async_migrations(interval);
    }

    if let Some(interval) = params.dry_run_migration_interval {
        builder = builder.explicit_migrations(interval);
    }

    if let Some(secs) = params.query_max_failure_seconds {
        builder = builder.query_max_failure_seconds(secs);
    }

    if let Some(secs) = params.fallback_recovery_seconds {
        builder = builder.fallback_recovery_seconds(secs);
    }

    if let Some(ref mysql) = server_upstream {
        builder = builder.mysql(mysql);
    }

    builder.start().await
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
    use serial_test::serial;

    use super::*;
    // Verifies that the wrappers that create and teardown the deployment.
    #[clustertest]
    async fn clustertest_startup_teardown_test() {
        let deployment = DeploymentBuilder::new("ct_startup_teardown")
            .with_servers(2, ServerParams::default())
            .start()
            .await;
        assert!(
            deployment.is_ok(),
            "Error starting deployment: {}",
            deployment.err().unwrap()
        );

        let mut deployment = deployment.unwrap();

        // Check we received a metrics dump from each client.
        let metrics = deployment.metrics().get_metrics().await.unwrap();
        assert_eq!(metrics.len(), 2);

        // Check that the controller can respond to an rpc.
        let workers = deployment.leader_handle().healthy_workers().await.unwrap();
        assert_eq!(workers.len(), 2);

        let res = deployment.teardown().await;
        assert!(
            res.is_ok(),
            "Error tearing down deployment: {}",
            res.err().unwrap()
        );
    }

    #[clustertest]
    async fn clustertest_minimal() {
        let mut deployment = DeploymentBuilder::new("ct_minimal")
            .with_servers(2, ServerParams::default())
            .start()
            .await
            .unwrap();
        deployment.teardown().await.unwrap();
    }

    #[clustertest]
    async fn clustertest_with_binlog() {
        let mut deployment = DeploymentBuilder::new("ct_with_binlog")
            .with_servers(2, ServerParams::default())
            .deploy_mysql()
            .start()
            .await
            .unwrap();

        // Check that we currently have two workers.
        assert_eq!(
            deployment
                .leader_handle()
                .healthy_workers()
                .await
                .unwrap()
                .len(),
            2
        );
        deployment.teardown().await.unwrap();
    }

    /// Test that setting a failpoint triggers a panic on RPC.
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn clustertest_with_set_failpoint() {
        let cluster_name = "ct_with_set_failpoint";
        let mut deployment = DeploymentBuilder::new(cluster_name)
            .add_server(ServerParams::default())
            .start()
            .await
            .unwrap();

        // Verify we can issue an RPC.
        assert_eq!(deployment.handle.healthy_workers().await.unwrap().len(), 1);

        let controller_uri = deployment.handle.controller_uri().await.unwrap();
        let server_handle = deployment.server_handle(&controller_uri).unwrap();
        server_handle
            .set_failpoint("controller-request", "panic")
            .await;

        // Request times out because the server panics.
        assert!(tokio::time::timeout(
            Duration::from_millis(300),
            deployment.handle.healthy_workers()
        )
        .await
        .is_err());
        deployment.teardown().await.unwrap();
    }

    /// Test that setting a failpoint triggers a panic on RPC.
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn start_adapter_running_deployment() {
        let cluster_name = "ct_start_adapter_running_deployment";
        let mut deployment = DeploymentBuilder::new(cluster_name)
            .add_server(ServerParams::default())
            .start()
            .await
            .unwrap();

        assert!(deployment.adapter_handles().is_empty());

        deployment
            .start_mysql_adapter(false)
            .await
            .expect("failed to start adapter");

        assert_eq!(1, deployment.adapter_handles().len());
        deployment.teardown().await.unwrap();
    }
}
