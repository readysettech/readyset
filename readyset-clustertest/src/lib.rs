//! A local multi-process deployment test framework for ReadySet. It enables
//! local blackbox testing on multi-server deployments with support for
//! programmatically modifying the deployment, i.e. introducing faults,
//! adding new servers, replicating readers.
//!
//! This makes this framework well suited for:
//!   * Testing fault tolerance and failure recovery behavior. Clustertests can easily introduce
//!     faults via [`kill_server`](DeploymentHandle::kill_server) and mimic failure recovery via
//!     [`start_server`](DeploymentHandle::start_server).
//!   * Testing behavior that is localized to specific workers. Each worker in a deployment can be
//!     queried for metrics separately via [`MetricsClient`] which can be checked in clustertests.
//!
//! # Preparing to run clustertests Clustertests require external resources in order to run: a
//! MySQL and PostgreSQL server, an authority (consul) and binaries for readyset-server and
//! readyset. Clustertest defaults are set to match the developer docker-compose in `//readyset`
//! and the default flags for readyset-server and readyset.
//!
//! ```bash
//! # Build the binaries with the failure injection feature for most clustertests. Binaries built
//! to //target/debug are used by default.
//! cargo build --bin readyset-server --bin readyset --features failure_injection
//!
//! # Spin up the developer docker stack to start MySQL, PostgreSQL, and a consul authority.
//! cd //readyset/public
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
//! cargo test -p readyset-clustertest
//!
//! # Modifying clustertests via environment arguments.
//! PORT=3310 cargo test -p readyset-clustertest
//!
//! # Configuring readyset-server logging via logging environment variables.
//! LOG_LEVEL=debug cargo test -p readyset-clustertest
//! ```
//!
//! Clustertests can be configured via environment variables. Any environment variables are also
//! passed to the child readyset-server and readyset processes, as a result, these processes can be
//! further configured through environment variables. This is helpful for configuring logging
//! environment variables, such as `LOG_LEVEL`. See [Configuring
//! Logging](http://docs/running-readyset.html#configuring-logging) for more information.
//!
//! * `AUTHORITY_ADDRESS`: The address of an authority, defaults to `127.0.0.1:8500`
//!
//! * `AUTHORITY`: The type of authority, defaults to `consul`.
//!
//! * `BINARY_PATH`: The path to a directory with the readyset-server and readyset binaries,
//!   defaults to `$CARGO_MANIFEST_DIR/../../target/debug`, `readyset/target/debug`.
//!
//! * `PORT`: The host of the upstream database to use as upstream, defaults to
//! `127.0.0.1`.
//!
//! * `PORT`: The port of the upstream database to use as upstream, defaults to
//! `3306`.
//!
//! * `ROOT_PASSWORD`: The password to use for the upstream database,
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
//!     let mut deployment = DeploymentBuilder::new(DatabaseType::MySQL, "ct_example")
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
//!   * whether to use an upstream database, [`deploy_adapter`](DeploymentBuilder::deploy_adapter).
//!   * whether to deploy an adapter, [`deploy_adapter`](DeploymentBuilder::deploy_adapter).
//!   * The parameters used to build a server, [`ServerParams`].
//!
//! Once all parameters are specified, creating the resources for the deployment
//! is done with [`DeploymentBuilder::start`].
//!
//! ```rust
//! use readyset_clustertest::*;
//! // Deploy a three server deployment with different volume IDs for each server, a readyset adapter,
//! // and upstream database.
//! async fn build_deployment() {
//!     let mut deployment = DeploymentBuilder::new(DatabaseType::MySQL, "ct_example")
//!         .add_server(ServerParams::default().with_volume("v1"))
//!         .add_server(ServerParams::default().with_volume("v2"))
//!         .add_server(ServerParams::default().with_volume("v3"))
//!         .min_workers(3)
//!         .deploy_upstream()
//!         .deploy_adapter()
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
//! The [`DeploymentHandle`] allows clustertests writers to programmatically
//! modify the deployment and check controller / metrics properties. It
//! primarily facilitates four operations:
//!   1. Adding servers to the deployment. Clustertest can create any number
//!      of server processes during the test via
//!      [`DeploymentHandle::start_server`].
//!   2. Killing server in the deployment.
//!      [`DeploymentHandle::kill_server`] may be called to
//!      remove a server from the deployment, this is done by sending the kill
//!      command to the process running the server.
//!   3. Sending controller RPCs via the [`ReadySetHandle`] returned by
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
//! kill the deployment processes and remove locally created files.

mod server;

#[cfg(test)]
mod readyset;
#[cfg(test)]
mod readyset_mysql;
#[cfg(test)]
mod readyset_postgres;
#[cfg(test)]
mod utils;

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::net::TcpListener;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::{Duration, Instant};

use ::readyset_client::consensus::AuthorityType;
use ::readyset_client::metrics::client::MetricsClient;
use ::readyset_client::ReadySetHandle;
use anyhow::{anyhow, Result};
pub use database_utils::DatabaseType;
use database_utils::{DatabaseConnection, DatabaseURL};
use futures::executor;
use hyper::Client;
use mysql_async::prelude::Queryable;
use rand::Rng;
use readyset_adapter::DeploymentMode;
#[cfg(test)]
use readyset_clustertest_macros::clustertest;
use readyset_errors::ReadySetResult;
use serde::Deserialize;
use server::{AdapterBuilder, ProcessHandle, ReadysetServerBuilder};
use tokio::time::sleep;
use tokio_postgres::NoTls;
use tracing::{debug, warn};
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
    #[serde(default = "default_postgresql_host")]
    postgresql_host: String,
    #[serde(default = "default_mysql_port")]
    mysql_port: String,
    #[serde(default = "default_postgresql_port")]
    postgresql_port: String,
    #[serde(default = "default_pguser")]
    pguser: String,
    #[serde(default = "default_pgpassword")]
    pgpassword: String,
    #[serde(default = "default_mysql_user")]
    mysql_user: String,
    #[serde(default = "default_mysql_password")]
    mysql_password: String,
}

fn default_pguser() -> String {
    "postgres".to_string()
}

fn default_mysql_user() -> String {
    "root".to_string()
}

fn default_authority_address() -> String {
    "127.0.0.1:8500".to_string()
}

fn default_mysql_host() -> String {
    "127.0.0.1".to_string()
}

fn default_postgresql_host() -> String {
    "127.0.0.1".to_string()
}

fn default_mysql_port() -> String {
    "3306".to_string()
}

fn default_postgresql_port() -> String {
    "5432".to_string()
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

fn default_pgpassword() -> String {
    "noria".to_string()
}

fn default_mysql_password() -> String {
    "noria".to_string()
}

/// Source of the readyset binaries.
pub(crate) struct ReadySetBinarySource {
    /// Path to a built readyset-server on the local machine.
    pub readyset_server: PathBuf,
    /// Optional path to readyset on the local machine. readyset may not be included in the build.
    pub readyset: Option<PathBuf>,
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
    min_workers: usize,
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
    /// Whether the server should wait to receive a failpoint request before proceeding with the
    /// rest of it's startup.
    wait_for_failpoint: bool,
    /// Whether to allow full materialization nodes or not
    allow_full_materialization: bool,
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
    min_workers: usize,
    /// Parameters for the set of readyset-server instances in the deployment.
    servers: Vec<ServerParams>,
    /// How many ReadySet adapter instances to deploy
    adapters: usize,
    /// Deploy an upstream database and use replication.
    upstream: bool,
    /// The type of authority to use for cluster management.
    authority: AuthorityType,
    /// The address of the authority.
    authority_address: String,
    /// The user of the upstream db.
    upstream_user: String,
    /// The host of the upstream db.
    host: String,
    /// The port of the upstream db.
    port: String,
    /// The root password for the db.
    root_password: String,
    /// Are async migrations enabled on the adapter.
    async_migration_interval: Option<u64>,
    /// Enables explicit migrations, and passes in an interval for running dry run migrations
    /// that determine whether queries that weren't explicitly migrated would be
    /// supported by ReadySet. Exposed via the `SHOW PROXIED QUERIES` command.
    dry_run_migration_interval: Option<u64>,
    /// The max time in seconds that a query may continuously fail until we enter a recovery
    /// period. None if not enabled.
    query_max_failure_seconds: Option<u64>,
    /// The period in seconds that we enter into a fallback recovery mode for a given query, if
    /// that query has continuously failed for query_max_failure_seconds.
    /// None if not enabled.
    fallback_recovery_seconds: Option<u64>,
    /// Specifies the polling interval for the adapter to request views from the Leader.
    ///
    /// Corresponds to [`readyset_client_adapter::Options::views_polling_interval`]
    views_polling_interval: Duration,
    /// Optional username for the user.
    user: Option<String>,
    /// Optional password for the user.
    pass: Option<String>,
    /// Replicator restart timeout in seconds.
    replicator_restart_timeout_secs: Option<u64>,
    /// Number of times to replicate reader domains
    reader_replicas: Option<usize>,
    /// If true, will automatically restart the server/adapter processes
    auto_restart: bool,
    /// Sets whether the adapter, server, both, or neither should wait to receive a failpoint
    /// request before proceeding with the rest of the startup process.
    wait_for_failpoint: FailpointDestination,
    /// Type of the upstream database (mysql or postgresql)
    database_type: DatabaseType,
    /// If true, runs the adapter in cleanup mode, which cleans up deployment related assets for
    /// the given deployment name.
    cleanup: bool,
    /// Whether to automatically create inlined migrations for a query with unsupported
    /// placeholders.
    enable_experimental_placeholder_inlining: bool,
    /// Whether to allow fully materialized nodes or not
    allow_full_materialization: bool,
    /// Whether to allow prometheus metrics
    prometheus_metrics: bool,
    /// How to execute the adapter and server processes.
    deployment_mode: DeploymentMode,
}

pub enum FailpointDestination {
    Adapter,
    Server,
    Both,
    None,
}

impl DeploymentBuilder {
    pub fn new(database_type: DatabaseType, name: &str) -> Self {
        let env = envy::from_env::<Env>().unwrap();

        let mut readyset_server_path = env.binary_path.clone();
        readyset_server_path.push("readyset-server");

        let mut readyset_path = env.binary_path;
        readyset_path.push("readyset");

        // Append the deployment name with a random number to prevent state collisions
        // on test repeats with failed teardowns.
        let mut rng = rand::thread_rng();
        let name = name.to_string() + &rng.gen::<u32>().to_string();

        // Depending on what type of upstream we have, use the mysql or posgresql host/ports.
        let (user, pass, host, port) = match database_type {
            DatabaseType::MySQL => (
                env.mysql_user,
                env.mysql_password,
                env.mysql_host,
                env.mysql_port,
            ),
            DatabaseType::PostgreSQL => (
                env.pguser,
                env.pgpassword,
                env.postgresql_host,
                env.postgresql_port,
            ),
        };

        Self {
            name,
            readyset_binaries: ReadySetBinarySource {
                readyset_server: readyset_server_path,
                readyset: Some(readyset_path),
            },
            shards: None,
            min_workers: 1,
            servers: vec![],
            adapters: 0,
            upstream: false,
            authority: AuthorityType::from_str(&env.authority).unwrap(),
            authority_address: env.authority_address,
            upstream_user: user.clone(),
            host,
            port,
            root_password: pass.clone(),
            async_migration_interval: None,
            dry_run_migration_interval: None,
            query_max_failure_seconds: None,
            fallback_recovery_seconds: None,
            views_polling_interval: Duration::from_secs(5),
            user: Some(user),
            pass: Some(pass),
            replicator_restart_timeout_secs: None,
            reader_replicas: None,
            auto_restart: false,
            wait_for_failpoint: FailpointDestination::None,
            database_type,
            deployment_mode: DeploymentMode::Adapter,
            cleanup: false,
            enable_experimental_placeholder_inlining: false,
            allow_full_materialization: false,
            prometheus_metrics: false,
        }
    }
    /// The number of shards in the graph, `shards` <= 1 disables sharding.
    pub fn shards(mut self, shards: usize) -> Self {
        self.shards = Some(shards);
        self
    }

    /// The number of healthy servers required in the system before we begin
    /// accepting queries and performing migrations.
    pub fn min_workers(mut self, min_workers: usize) -> Self {
        self.min_workers = min_workers;
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
    /// This will overwrite any previous call to [`Self::set_adapters`]
    pub fn deploy_adapter(mut self) -> Self {
        self.adapters = 1;
        self
    }

    /// Sets the number of adapter servers to deploy as part of this deployment.
    ///
    /// This will overwrite any previous call to [`Self::deploy_adapter`]
    pub fn with_adapters(mut self, num_adapters: usize) -> Self {
        self.adapters = num_adapters;
        self
    }

    /// Run the adapter(s) in standalone mode with the default ServerStartParams
    pub fn standalone(mut self) -> Self {
        self.deployment_mode = DeploymentMode::Standalone;
        self
    }

    /// Whether to use an upstream database as part of this deployment. This
    /// will populate [`DeploymentHandle::upstream_connection_str`] with the upstream
    /// databases's connection string.
    pub fn deploy_upstream(mut self) -> Self {
        self.upstream = true;
        self
    }

    /// Whether to enable the async migrations feature in the adapter. Requires
    /// [`Self::deploy_adapter`] to be set on this deployment.
    pub fn async_migrations(mut self, interval_ms: u64) -> Self {
        self.async_migration_interval = Some(interval_ms);
        self
    }

    /// Whether to enable the explicit migrations feature in the adapter. Requires
    /// [`Self::deploy_adapter`] to be set on this deployment.
    /// Must supply an interval for the dry run loop.
    pub fn explicit_migrations(mut self, interval_ms: u64) -> Self {
        self.dry_run_migration_interval = Some(interval_ms);
        self
    }

    /// Overrides the maximum time a query may continuously fail in the adapter.
    /// Requires [`Self::deploy_adapter`] to be set on this deployment.
    pub fn query_max_failure_seconds(mut self, secs: u64) -> Self {
        self.query_max_failure_seconds = Some(secs);
        self
    }

    /// Overrides the fallback recovery period in the adapter that we enter when a query has
    /// repeatedly failed for query_max_failure_seconds.
    /// Requires [`Self::deploy_adapter`] to be set on this deployment.
    pub fn fallback_recovery_seconds(mut self, secs: u64) -> Self {
        self.fallback_recovery_seconds = Some(secs);
        self
    }

    /// Overrides the interval at which to poll for updated view status.
    /// Requires [`Self::deploy_adapter`] to be set on this deployment.
    pub fn views_polling_interval(mut self, interval: Duration) -> Self {
        self.views_polling_interval = interval;
        self
    }

    /// Sets the value of the user and password to use for the upstream database in the
    /// adapter and the server. The upstream connection returned will always be the root connection
    /// to allow making changes without worrying about permissions to the upstream database.
    pub fn with_user(mut self, user: &str, pass: &str) -> Self {
        self.user = Some(user.to_string());
        self.pass = Some(pass.to_string());
        self
    }

    pub fn with_database_type(mut self, database_type: DatabaseType) -> Self {
        self.database_type = database_type;
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

    /// Turns on cleanup mode, which cleans up deployment related assets for the given deployment
    /// name, and then exits.
    pub fn cleanup(mut self) -> Self {
        self.cleanup = true;
        self
    }

    /// Sets whether the adapter, server, both, or neither should wait for an incoming failpoint
    /// request before proceeding with the rest of the startup process.
    pub fn wait_for_failpoint(mut self, wait_for_failpoint: FailpointDestination) -> Self {
        match (&self.wait_for_failpoint, &wait_for_failpoint) {
            (&FailpointDestination::Adapter, &FailpointDestination::Server)
            | (&FailpointDestination::Server, &FailpointDestination::Adapter) => {
                self.wait_for_failpoint = FailpointDestination::Both
            }
            _ => self.wait_for_failpoint = wait_for_failpoint,
        };
        self
    }

    /// Allows fully materialized nodes
    pub fn allow_full_materialization(mut self) -> Self {
        self.allow_full_materialization = true;
        self
    }

    /// Sets whether or not to automatically create inlined caches for queries with unsupported
    /// placeholders
    pub fn enable_experimental_placeholder_inlining(mut self) -> Self {
        self.enable_experimental_placeholder_inlining = true;
        self
    }

    /// Sets whether to enable embedded readers mode in the adapter
    pub fn embedded_readers(mut self, embedded_readers: bool) -> Self {
        self.deployment_mode = match embedded_readers {
            true => DeploymentMode::EmbeddedReaders,
            false => DeploymentMode::Adapter,
        };
        self
    }

    /// Sets whether to enable prometheus metrics in the adapter
    pub fn prometheus_metrics(mut self, prometheus_metrics: bool) -> Self {
        self.prometheus_metrics = prometheus_metrics;
        self
    }

    pub fn adapter_start_params(&self) -> AdapterStartParams {
        let wait_for_failpoint = matches!(
            self.wait_for_failpoint,
            FailpointDestination::Adapter | FailpointDestination::Both
        );

        AdapterStartParams {
            deployment_name: self.name.clone(),
            readyset_path: self.readyset_binaries.readyset.clone().unwrap(),
            authority_address: self.authority_address.clone(),
            authority_type: self.authority.to_string(),
            async_migration_interval: self.async_migration_interval,
            dry_run_migration_interval: self.dry_run_migration_interval,
            query_max_failure_seconds: self.query_max_failure_seconds,
            fallback_recovery_seconds: self.fallback_recovery_seconds,
            auto_restart: self.auto_restart,
            views_polling_interval: self.views_polling_interval,
            wait_for_failpoint,
            cleanup: self.cleanup,
            enable_experimental_placeholder_inlining: self.enable_experimental_placeholder_inlining,
            deployment_mode: self.deployment_mode,
            allow_full_materialization: self.allow_full_materialization,
            prometheus_metrics: self.prometheus_metrics,
        }
    }

    pub fn server_start_params(&self) -> ServerStartParams {
        let wait_for_failpoint = matches!(
            self.wait_for_failpoint,
            FailpointDestination::Server | FailpointDestination::Both
        );
        ServerStartParams {
            readyset_server_path: self.readyset_binaries.readyset_server.clone(),
            deployment_name: self.name.clone(),
            shards: self.shards,
            min_workers: self.min_workers,
            authority_address: self.authority_address.clone(),
            authority_type: self.authority.to_string(),
            replicator_restart_timeout_secs: self.replicator_restart_timeout_secs,
            reader_replicas: self.reader_replicas,
            auto_restart: self.auto_restart,
            wait_for_failpoint,
            allow_full_materialization: self.allow_full_materialization,
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
        wait_for_adapters: bool,
    ) -> anyhow::Result<DeploymentHandle>
    where
        Q: AsRef<str> + Send + Sync + 'a,
    {
        readyset_tracing::init_test_logging();
        if self.deployment_mode.is_standalone() {
            debug_assert!(
                self.adapters == 1 && self.servers.is_empty(),
                "Standalone mode should only have 1 adapter and no servers for any current use case.
                 If this is no longer true, remove or adjust this assert."
            );
            // HACK: Since the clustertests spawn a process but also want a handle into the
            // controller, we use Consul as the authority even for standalone mode.
            debug_assert!(
                self.authority == AuthorityType::Consul,
                "Standalone mode is only supported with AuthorityType::Consul"
            );
        }

        let mut port_allocator = PortAllocator::new();
        // If this deployment includes binlog replication and a mysql instance.
        let mut upstream_addr = None;
        let server_upstream = if self.upstream {
            let root_addr = format!(
                "{}://{}:{}@{}:{}",
                self.database_type,
                &self.upstream_user,
                &self.root_password,
                &self.host,
                &self.port,
            );
            upstream_addr = Some(format!("{}/{}", &root_addr, &self.name));
            match self.database_type {
                DatabaseType::MySQL => {
                    let opts = mysql_async::Opts::from_url(&root_addr).unwrap();
                    let mut conn = mysql_async::Conn::new(opts).await.unwrap();
                    conn.query_drop(format!(
                        "CREATE DATABASE {}; USE {}",
                        &self.name, &self.name
                    ))
                    .await
                    .unwrap();

                    for c in cmds {
                        conn.query_drop(c.as_ref()).await?;
                    }
                }

                DatabaseType::PostgreSQL => {
                    let (client, connection) =
                        tokio_postgres::connect(&root_addr, NoTls).await.unwrap();
                    let handle = tokio::spawn(async move {
                        if let Err(e) = connection.await {
                            eprintln!("connection error: {}", e);
                        }
                    });

                    client
                        .query_opt(&format!("CREATE DATABASE {}", &self.name), &[])
                        .await
                        .unwrap();

                    // TODO(peter): Support running arbitrary commands for postgres, requires
                    // changing types and a bit of work to support.

                    drop(handle);
                }
            }

            let user = self.user.clone().unwrap_or_else(|| "root".to_string());
            let pass = self
                .pass
                .clone()
                .unwrap_or_else(|| self.root_password.clone());

            let user_addr = format!(
                "{}://{}:{}@{}:{}",
                self.database_type, &user, &pass, &self.host, &self.port
            );

            Some(format!("{}/{}", &user_addr, &self.name))
        } else {
            None
        };

        // Create the readyset-server instances.
        let mut handles = HashMap::new();
        for server in &self.servers {
            let handle = start_server(
                port_allocator.get_available_port(),
                server_upstream.as_ref(),
                server,
                &self.server_start_params(),
            )
            .await?;

            handles.insert(handle.addr.clone(), handle);
        }

        let authority = self
            .authority
            .to_authority(&self.authority_address, &self.name);
        let handle = ReadySetHandle::new(authority).await;

        let metrics = MetricsClient::new(handle.clone()).unwrap();

        // Start `self.adapters` adapter instances.
        let mut adapters = Vec::with_capacity(self.adapters);
        for _ in 0..self.adapters {
            // If we are running in standalone, we need to pass both adapter and server parameters.
            let standalone_params = match self.deployment_mode.is_standalone() {
                true => Some(StandaloneParams {
                    server_start_params: self.server_start_params().clone(),
                    server_params: ServerParams::default(),
                }),
                false => None,
            };
            let port = port_allocator.get_available_port();
            let metrics_port = port_allocator.get_available_port();
            let process = start_adapter(
                server_upstream.clone(),
                port,
                metrics_port,
                &self.adapter_start_params(),
                self.database_type,
                standalone_params,
            )
            .await?;

            // Sleep to give the adapter time to startup.
            let handle = AdapterHandle {
                metrics_port,
                conn_str: format!("{}://127.0.0.1:{}/{}", self.database_type, port, &self.name),
                process,
            };

            if wait_for_adapters {
                wait_for_adapter_startup(metrics_port, Duration::from_millis(2000)).await?;
            }

            adapters.push(handle);
        }

        let mut handle = DeploymentHandle {
            handle,
            metrics,
            name: self.name.clone(),
            adapter_start_params: self.adapter_start_params(),
            server_start_params: self.server_start_params(),
            upstream_addr,
            readyset_server_handles: handles,
            shutdown: false,
            port_allocator,
            adapters,
            database_type: self.database_type,
            deployment_mode: self.deployment_mode,
        };

        // Skip waiting for workers/backend if we don't have any servers as part of this deployment
        // yet.
        if !self.servers.is_empty() {
            handle.wait_for_workers(Duration::from_secs(90)).await?;
            handle.backend_ready(leader_timeout).await?;
        }

        Ok(handle)
    }

    /// Creates the local multi-process deployment from the set of parameters
    /// specified in the builder.
    pub async fn start(self) -> anyhow::Result<DeploymentHandle> {
        self.start_with_seed::<String>(&[], Duration::from_secs(90), true)
            .await
    }

    /// Creates the local multi-process deployment from the set of parameters
    /// specified in the builder. Does not wait for adapters metric server to come online.
    pub async fn start_without_waiting(self) -> anyhow::Result<DeploymentHandle> {
        self.start_with_seed::<String>(&[], Duration::from_secs(90), false)
            .await
    }
}

/// Waits for AdapterHandle to be healthy, up to the provided timeout.
async fn wait_for_adapter_startup(metrics_port: u16, timeout: Duration) -> anyhow::Result<()> {
    let poll_interval = timeout.checked_div(10).expect("timeout must be valid");
    wait_for_poller(
        endpoint_reports_healthy,
        metrics_port,
        timeout,
        poll_interval,
        "adapter",
    )
    .await
}

/// Waits for adapters http router to come up.
#[allow(dead_code)] // Used in tests.
async fn wait_for_adapter_router(
    metrics_port: u16,
    timeout: Duration,
    poll_interval: Duration,
) -> anyhow::Result<()> {
    let res = wait_for_poller(
        http_router_is_up,
        metrics_port,
        timeout,
        poll_interval,
        "adapter",
    )
    .await;
    match res {
        Ok(_) => debug!("Adapter http router is active"),
        Err(_) => warn!("Adapter http router failed to become active"),
    }

    res
}

/// Waits for servers http router to come up.
#[allow(dead_code)] // Used in tests.
async fn wait_for_server_router(
    port: u16,
    timeout: Duration,
    poll_interval: Duration,
) -> anyhow::Result<()> {
    let res = wait_for_poller(http_router_is_up, port, timeout, poll_interval, "server").await;
    match res {
        Ok(_) => debug!("Server http router is active"),
        Err(_) => warn!("Server http router failed to become active"),
    }

    res
}

/// Waits for server to come up as healthy.
#[allow(dead_code)] // Used in tests.
async fn wait_for_server_startup(port: u16, timeout: Duration) -> anyhow::Result<()> {
    let poll_interval = timeout.checked_div(10).expect("timeout must be valid");
    wait_for_poller(http_router_is_up, port, timeout, poll_interval, "server").await
}

async fn wait_for_poller<F, Fut>(
    port_poll: F,
    metrics_port: u16,
    timeout: Duration,
    poll_interval: Duration,
    destination: &str,
) -> anyhow::Result<()>
where
    F: Fn(u16) -> Fut,
    Fut: Future<Output = bool>,
{
    debug!(
        ?poll_interval,
        "Waiting for {destination} on port {metrics_port}"
    );

    // Polls the health_check_url continuously until it returns OK.
    let health_check_poller = |poll_interval: Duration| async move {
        loop {
            if port_poll(metrics_port).await {
                break Ok(());
            }

            debug!(
                "{destination} on port {metrics_port} not ready. Sleeping for {:?}",
                &poll_interval,
            );
            sleep(poll_interval).await;
        }
    };

    tokio::time::timeout(timeout, health_check_poller(poll_interval)).await?
}

async fn endpoint_reports_healthy(port: u16) -> bool {
    let health_check_url = format!("http://127.0.0.1:{}/health", port);
    let r = hyper::Request::get(&health_check_url)
        .body(hyper::Body::from(String::default()))
        .unwrap();

    let client = Client::new();
    // If this http requests returns an error, the adapter http server may not be ready yet.
    // If it returns something that isn't StatusCode::Ok, it is unhealthy and may eventually
    // become healthy.
    if let Ok(r) = client.request(r).await {
        if r.status() == hyper::StatusCode::OK {
            return true;
        }
    }

    false
}

async fn http_router_is_up(port: u16) -> bool {
    let health_check_url = format!("http://127.0.0.1:{}/health", port);
    let r = hyper::Request::get(&health_check_url)
        .body(hyper::Body::from(String::default()))
        .unwrap();

    let client = Client::new();
    // If we get an ok it means we got a response of some kind from the health endpoint, which
    // means the http router is up.
    client.request(r).await.is_ok()
}

/// A handle to a single server in the deployment.
#[derive(Clone)]
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
        if !http_router_is_up(self.addr.port().unwrap()).await {
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

/// A handle to an adapter instance in the deployment.
pub struct AdapterHandle {
    /// The adapter http server port
    pub metrics_port: u16,
    /// The connection string of the adapter.
    pub conn_str: String,
    /// The local process the adapter is running in.
    pub process: ProcessHandle,
}

impl AdapterHandle {
    pub async fn wait_for_startup(&self, timeout: Duration) -> anyhow::Result<()> {
        wait_for_adapter_startup(self.metrics_port, timeout).await
    }

    pub async fn set_failpoint(&mut self, name: &str, action: &str) {
        if !http_router_is_up(self.metrics_port).await {
            return;
        }
        let data = bincode::serialize(&(name, action)).unwrap();
        let failpoint_url = format!("http://127.0.0.1:{}/failpoint", self.metrics_port);
        let r = hyper::Request::get(failpoint_url)
            .body(hyper::Body::from(data))
            .unwrap();

        let client = Client::new();
        // This invariant should already be enforced by checking we get any kind of response from
        // the metrics health endpoint above, indicating that we have an http router ready to
        // receive fail point requests.
        if let Ok(r) = client.request(r).await {
            let status = r.status();
            assert!(status == hyper::StatusCode::OK);
        }
    }
}

/// A handle to a deployment created with `start_multi_process`.
pub struct DeploymentHandle {
    /// A handle to the current controller of the deployment.
    handle: ReadySetHandle,
    /// Metrics client for aggregating metrics across the deployment.
    metrics: MetricsClient,
    /// Map from a readyset server's address to a handle to the server.
    readyset_server_handles: HashMap<Url, ServerHandle>,
    /// Holds a list of handles to the adapters for this deployment, if any
    adapters: Vec<AdapterHandle>,
    /// The name of the deployment, cluster resources are prefixed
    /// by `name`.
    name: String,
    /// The configured parameters with which to start new adapters: in the deployment.
    adapter_start_params: AdapterStartParams,
    /// The configured parameters with which to start new servers in the deployment.
    server_start_params: ServerStartParams,
    /// The connection string of the upstream database for the deployment.
    upstream_addr: Option<String>,
    /// A handle to each readyset server in the deployment.
    /// True if this deployment has already been torn down.
    shutdown: bool,
    /// A tool that allows us to allocate ports
    port_allocator: PortAllocator,
    /// The type of the upstream (MySQL or PostgresSQL)
    database_type: DatabaseType,
    /// How to execute the adapter and server processes.
    deployment_mode: DeploymentMode,
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

    /// Returns a [`ReadySetHandle`] that enables sending RPCs to the leader
    /// of the deployment.
    pub fn leader_handle(&mut self) -> &mut ReadySetHandle {
        &mut self.handle
    }

    /// Returns a [`MetricsClient`] for the deployment. Tests can query for
    /// metrics for each server via [`MetricsClient::get_metrics_for_server`]
    /// with the URL from `server_addrs` or `server_handles`.
    pub fn metrics(&mut self) -> &mut MetricsClient {
        &mut self.metrics
    }

    /// Creates a [`DatabaseConnection`] to the first adapter in the deployment.
    ///
    /// # Panics
    ///
    /// Panics if the adapter does not exist or a connection can not be made.
    pub async fn first_adapter(&self) -> DatabaseConnection {
        self.adapter(0).await
    }

    /// Creates a [`DatabaseConnection`] to the adapter at the given index in the deployment.
    ///
    /// # Panics
    ///
    /// Panics if the adapter does not exist or a connection can not be made.
    pub async fn adapter(&self, idx: usize) -> DatabaseConnection {
        let addr = &self.adapters[idx].conn_str;

        DatabaseURL::from_str(addr)
            .unwrap()
            .connect(None)
            .await
            .unwrap()
    }

    /// Creates a [`DatabaseConnection`] to the upstream database in the deployment.
    /// Otherwise panics if the upstream database does not exist or a connection
    /// can not be made.
    pub async fn upstream(&self) -> DatabaseConnection {
        let addr = self.upstream_addr.as_ref().unwrap();
        DatabaseURL::from_str(addr)
            .unwrap()
            .connect(None)
            .await
            .unwrap()
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
        debug!("Waiting {max_wait:?} for workers to be healthy");

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
    /// sharing, min_workers, authority, are passed to the new server.
    pub async fn start_server(
        &mut self,
        params: ServerParams,
        wait_for_startup: bool,
    ) -> anyhow::Result<Url> {
        let port = self.port_allocator.get_available_port();
        let handle = start_server(
            port,
            self.upstream_addr.clone().as_ref(),
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

    /// Start a new readyset adapter instance in the deployment
    pub async fn start_adapter(&mut self, wait_for_startup: bool) -> anyhow::Result<()> {
        let port = self.port_allocator.get_available_port();
        let metrics_port = self.port_allocator.get_available_port();
        let database_type = self.database_type;
        // If we are running in standalone, we need to pass both adapter and server parameters.
        let standalone_params = match self.deployment_mode.is_standalone() {
            true => Some(StandaloneParams {
                server_start_params: self.server_start_params().clone(),
                server_params: ServerParams::default(),
            }),
            false => None,
        };
        let process = start_adapter(
            self.upstream_addr.clone(),
            port,
            metrics_port,
            self.adapter_start_params(),
            database_type,
            standalone_params,
        )
        .await?;

        if wait_for_startup {
            wait_for_adapter_startup(metrics_port, Duration::from_millis(2000)).await?;
        }

        self.adapters.push(AdapterHandle {
            metrics_port,
            conn_str: format!("{}://127.0.0.1:{}/{}", database_type, port, &self.name),
            process,
        });

        Ok(())
    }

    /// Waits for the back-end to return that it is ready to process queries.
    pub async fn backend_ready(&mut self, timeout: Duration) -> ReadySetResult<()> {
        debug!("Waiting {timeout:?} for backend to be ready to process queries");
        let mut e = None;
        let start = Instant::now();
        let check_leader_loop = async {
            loop {
                let remaining = std::cmp::min(
                    Duration::from_secs(5),
                    timeout.saturating_sub(start.elapsed()),
                );
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

    /// Waits for adapters to die naturally.
    pub async fn wait_for_adapter_death(&mut self) {
        for adapter_handle in &mut self.adapters {
            while adapter_handle.process.check_alive().await {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
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
        for adapter_handle in &mut self.adapters {
            // Ignore the result: If the kill() errors, we still want to attempt cleaning up the
            // resources below
            let _ = adapter_handle.process.kill().await;
        }

        // Clean up the existing mysql state.
        if let (Some(upstream_mysql_addr), DatabaseType::MySQL) =
            (&self.upstream_addr, &self.database_type)
        {
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
        self.adapters.as_mut()
    }

    /// Returns a mutable reference to the [`AdapterHandle`] for the readyset-adapter at the given
    /// index. Returns `None` if the index is out-of-bounds
    pub fn adapter_handle(&mut self, idx: usize) -> Option<&mut AdapterHandle> {
        self.adapters.get_mut(idx)
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
    /// Absolute path to the readyset binary.
    readyset_path: PathBuf,
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
    /// that query has continuously failed for query_max_failure_seconds.
    /// None if not enabled.
    fallback_recovery_seconds: Option<u64>,
    /// Whether or not to automatically restart the adapter process.
    auto_restart: bool,
    /// Specifies the polling interval for the adapter to request views from the Leader.
    ///
    /// Corresponds to [`readyset_client_adapter::Options::views_polling_interval`]
    views_polling_interval: Duration,
    /// If true, will have the adapter wait for an incoming failpoint request before proceeding
    /// with the rest of the startup process. Useful for testing failpoints placed within the
    /// adapter startup flow.
    wait_for_failpoint: bool,
    /// If true, runs the adapter in cleanup mode, which cleans up deployment related assets for
    /// the given deployment name.
    cleanup: bool,
    /// Whether to automatically create inlined migrations for a query with unsupported
    /// placeholders.
    enable_experimental_placeholder_inlining: bool,
    /// How to execute the adapter
    deployment_mode: DeploymentMode,
    /// Whether or not to allow full materializations
    allow_full_materialization: bool,
    /// Whether to enable prometheus metrics
    prometheus_metrics: bool,
}

async fn start_server(
    port: u16,
    upstream_addr: Option<&String>,
    server_params: &ServerParams,
    server_start_params: &ServerStartParams,
) -> Result<ServerHandle> {
    let mut builder = ReadysetServerBuilder::new(server_start_params.readyset_server_path.as_ref())
        .deployment(server_start_params.deployment_name.as_str())
        .external_port(port)
        .authority_addr(&server_start_params.authority_address)
        .authority(&server_start_params.authority_type)
        .min_workers(server_start_params.min_workers)
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
    if let Some(upstream_addr) = upstream_addr {
        builder = builder.upstream_addr(upstream_addr);
    }
    if let Some(t) = server_start_params.replicator_restart_timeout_secs {
        builder = builder.replicator_restart_timeout(t);
    }
    if let Some(rs) = server_start_params.reader_replicas {
        builder = builder.reader_replicas(rs);
    }
    if server_start_params.wait_for_failpoint {
        builder = builder.wait_for_failpoint();
    }
    if server_start_params.allow_full_materialization {
        builder = builder.allow_full_materialization();
    }
    let addr = Url::parse(&format!("http://127.0.0.1:{}", port)).unwrap();
    Ok(ServerHandle {
        addr,
        process: builder.start().await?,
        params: server_params.clone(),
    })
}

struct StandaloneParams {
    server_start_params: ServerStartParams,
    server_params: ServerParams,
}

async fn start_adapter(
    server_upstream_addr: Option<String>,
    port: u16,
    metrics_port: u16,
    params: &AdapterStartParams,
    database_type: DatabaseType,
    standalone_params: Option<StandaloneParams>,
) -> Result<ProcessHandle> {
    let mut builder = AdapterBuilder::new(params.readyset_path.as_ref(), database_type)
        .deployment(&params.deployment_name)
        .port(port)
        .metrics_port(metrics_port)
        .authority_addr(params.authority_address.as_str())
        .authority(params.authority_type.as_str())
        .auto_restart(params.auto_restart)
        .views_polling_interval(params.views_polling_interval);

    if params.cleanup {
        builder = builder.cleanup();
    }

    // Standalone mode passes in server params as well
    builder = builder.deployment_mode(params.deployment_mode);
    if let Some(standalone_params) = standalone_params {
        let StandaloneParams {
            server_start_params,
            server_params,
        } = standalone_params;
        builder = builder.min_workers(server_start_params.min_workers);

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
        if let Some(t) = server_start_params.replicator_restart_timeout_secs {
            builder = builder.replicator_restart_timeout(t);
        }
        if let Some(rs) = server_start_params.reader_replicas {
            builder = builder.reader_replicas(rs);
        }
    }

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

    if let Some(upstream_addr) = server_upstream_addr {
        builder = builder.upstream_addr(&upstream_addr);
    }

    if params.wait_for_failpoint {
        builder = builder.wait_for_failpoint();
    }

    if params.enable_experimental_placeholder_inlining {
        builder = builder.enable_experimental_placeholder_inlining();
    }

    if params.allow_full_materialization {
        builder = builder.allow_full_materialization();
    }

    if params.prometheus_metrics {
        builder = builder.prometheus_metrics()
    }

    builder.start().await
}

/// A tool that allocates ports in a way that prevents the same port from being allocated more than
/// once, even if a port previously allocated by this tool is not yet in use.
struct PortAllocator(HashSet<u16>);

impl PortAllocator {
    fn new() -> Self {
        Self(HashSet::new())
    }

    /// Returns a random available port allocated by the operating system. This method keeps track
    /// of the ports that it has allocated, which means that you can call this method multiple
    /// times in succession without having to worry about the same port being returned more than
    /// once **even if one of the prior ports it has returned is not yet in use**.
    fn get_available_port(&mut self) -> u16 {
        loop {
            // We bind to localhost with a port of 0 here so the OS assigns us an unused port.
            // We only need the port (since we'll be passing it to a ReadySet server or adapter as
            // a CLI option) so we keep the unused port and drop the listener
            let port = TcpListener::bind(("127.0.0.1", 0))
                .unwrap()
                .local_addr()
                .unwrap()
                .port();

            // We need to check that the given port hasn't been allocated yet because it's possible
            // for this method to be called several times in succession without the ports being
            // used yet
            if !self.0.contains(&port) {
                return port;
            }
        }
    }
}

// These tests currently require that a docker daemon is already setup
// and accessible by the user calling cargo test. As these tests interact
// with a stateful external component, the docker daemon, each test is
// responsible for cleaning up its own external state.
#[cfg(test)]
#[cfg(not(feature = "postgres"))]
mod tests {
    use serial_test::serial;

    use super::*;
    // Verifies that the wrappers that create and teardown the deployment.
    #[clustertest]
    async fn clustertest_startup_teardown_test() {
        let deployment = DeploymentBuilder::new(DatabaseType::MySQL, "ct_startup_teardown")
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
    async fn clustertest_no_servers() {
        let mut deployment = DeploymentBuilder::new(DatabaseType::MySQL, "ct_empty")
            .start()
            .await
            .unwrap();
        deployment.teardown().await.unwrap();
    }

    #[clustertest]
    async fn clustertest_minimal() {
        let mut deployment = DeploymentBuilder::new(DatabaseType::MySQL, "ct_minimal")
            .with_servers(2, ServerParams::default())
            .start()
            .await
            .unwrap();
        deployment.teardown().await.unwrap();
    }

    #[clustertest]
    async fn clustertest_with_binlog() {
        let mut deployment = DeploymentBuilder::new(DatabaseType::MySQL, "ct_with_binlog")
            .with_servers(2, ServerParams::default())
            .deploy_adapter()
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
        let mut deployment = DeploymentBuilder::new(DatabaseType::MySQL, cluster_name)
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
        tokio::time::timeout(
            Duration::from_millis(300),
            deployment.handle.healthy_workers(),
        )
        .await
        .unwrap_err();
        deployment.teardown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn start_adapter_running_deployment() {
        let cluster_name = "ct_start_adapter_running_deployment";
        let mut deployment = DeploymentBuilder::new(DatabaseType::MySQL, cluster_name)
            .add_server(ServerParams::default())
            .start()
            .await
            .unwrap();

        assert!(deployment.adapter_handles().is_empty());

        deployment
            .start_adapter(false)
            .await
            .expect("failed to start adapter");

        assert_eq!(1, deployment.adapter_handles().len());
        deployment.teardown().await.unwrap();
    }
}
