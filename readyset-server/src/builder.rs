use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{self, Duration};

use database_utils::{ReplicationServerId, UpstreamConfig};
use dataflow::PersistenceParameters;
use readyset_client::consensus::{
    Authority, LocalAuthority, LocalAuthorityStore, NodeTypeSchedulingRestriction,
    WorkerSchedulingConfig,
};
use readyset_telemetry_reporter::TelemetrySender;
use readyset_util::shutdown::{self, ShutdownSender};

use crate::controller::replication::ReplicationStrategy;
use crate::handle::Handle;
use crate::{Config, FrontierStrategy, ReuseConfigType, VolumeId};

/// Used to construct a worker.
#[derive(Clone)]
pub struct Builder {
    config: Config,
    memory_limit: Option<usize>,
    memory_check_frequency: Option<time::Duration>,
    listen_addr: IpAddr,
    external_addr: SocketAddr,
    leader_eligible: bool,
    domain_scheduling_config: WorkerSchedulingConfig,
    /// The telelemetry sender
    pub telemetry: TelemetrySender,
    wait_for_failpoint: bool,
}

impl Default for Builder {
    fn default() -> Self {
        #[allow(clippy::unwrap_used)] // hardcoded literals
        Self {
            config: Config::default(),
            listen_addr: "127.0.0.1".parse().unwrap(),
            external_addr: "127.0.0.1:6033".parse().unwrap(),
            memory_limit: None,
            memory_check_frequency: None,
            leader_eligible: true,
            domain_scheduling_config: Default::default(),
            telemetry: TelemetrySender::new_no_op(),
            wait_for_failpoint: false,
        }
    }
}

impl Builder {
    /// Initialize a [`Builder`] from a set of command-line worker options and a deployment name.
    pub fn from_worker_options(
        opts: crate::WorkerOptions,
        deployment: &str,
        deployment_dir: PathBuf,
    ) -> Self {
        let mut builder = Self::default();
        if opts.memory_limit > 0 {
            builder.set_memory_limit(
                opts.memory_limit,
                Duration::from_secs(opts.memory_check_freq),
            );
        }
        builder.set_eviction_kind(opts.eviction_kind);

        builder.set_sharding(match opts.shards {
            0 | 1 => None,
            x => Some(x),
        });
        builder.set_min_workers(opts.min_workers);
        if opts.no_partial {
            builder.disable_partial();
        }
        if opts.feature_full_materialization {
            builder.enable_full_materialization();

            builder.set_materialization_persistence(opts.feature_materialization_persistence);
        }
        if opts.enable_packet_filters {
            builder.enable_packet_filters();
        }

        // TODO(fran): Reuse will be disabled until we refactor MIR to make it serializable.
        // See `noria/server/src/controller/sql/serde.rs` for details.
        builder.set_reuse(None);

        builder.set_topk(opts.feature_topk);
        builder.set_pagination(opts.feature_pagination);
        builder.set_mixed_comparisons(opts.feature_mixed_comparisons);
        builder.set_straddled_joins(opts.feature_straddled_joins);
        builder.set_post_lookup(opts.feature_post_lookup);
        builder.set_worker_timeout(Duration::from_secs(opts.worker_request_timeout_seconds));
        builder.set_background_recovery_interval(Duration::from_secs(
            opts.background_recovery_interval_seconds,
        ));

        builder.set_replication_strategy(opts.domain_replication_options.into());
        builder.set_verbose_domain_metrics(opts.verbose_domain_metrics);
        builder.set_frontier_strategy(opts.materialization_frontier);

        if let Some(volume_id) = opts.volume_id {
            builder.set_volume_id(volume_id);
        }

        let persistence_params = PersistenceParameters::new(
            opts.durability,
            Some(deployment.into()),
            Some(deployment_dir),
            opts.working_dir,
            builder
                .config
                .replicator_config
                .status_update_interval_secs
                .into(),
            opts.rocksdb_options,
        );
        builder.set_persistence(persistence_params);

        builder.set_replicator_config(opts.replicator_config);

        builder
    }

    /// Construct a new [`Builder`] with configuration setup for running tests
    pub fn for_tests() -> Self {
        let mut builder = Self::default();
        builder.set_abort_on_task_failure(false);
        builder.enable_full_materialization();
        builder.set_post_lookup(true);
        builder
    }

    /// Set the persistence parameters used by the system.
    pub fn set_persistence(&mut self, p: PersistenceParameters) {
        self.config.persistence = p;
    }

    pub fn get_persistence(&self) -> &PersistenceParameters {
        &self.config.persistence
    }

    /// Disable partial materialization for all subsequent migrations
    pub fn disable_partial(&mut self) {
        self.config.materialization_config.partial_enabled = false;
    }

    /// Enable the creation of [`PacketFilter`]s for egresses before readers
    ///
    /// [`PacketFilter`]: readyset_dataflow::node::special::PacketFilter
    pub fn enable_packet_filters(&mut self) {
        self.config.materialization_config.packet_filters_enabled = true;
    }

    /// Which nodes should be placed beyond the materialization frontier?
    pub fn set_frontier_strategy(&mut self, f: FrontierStrategy) {
        self.config.materialization_config.frontier_strategy = f;
    }

    /// Allow the creation of all fully materialized nodes.
    ///
    /// Unless this is called, any migrations that add fully materialized nodes will return
    /// [`ReadySetError::Unsupported`]
    pub fn enable_full_materialization(&mut self) {
        self.config
            .materialization_config
            .allow_full_materialization = true;
    }

    /// Forbid the creation of all fully materialized nodes.
    ///
    /// After this is called, any migrations that add fully materialized nodes will return
    /// [`ReadySetError::Unsupported`]
    pub fn forbid_full_materialization(&mut self) {
        self.config
            .materialization_config
            .allow_full_materialization = false;
    }

    /// Set sharding policy for all subsequent migrations; `None` or `Some(x)` where x <= 1 disables
    pub fn set_sharding(&mut self, shards: Option<usize>) {
        self.config.sharding = shards.filter(|s| *s > 1);
    }

    /// Set how many workers this worker should wait for before becoming a controller. More workers
    /// can join later, but they won't be assigned any of the initial domains.
    pub fn set_min_workers(&mut self, min_workers: usize) {
        assert_ne!(min_workers, 0);
        self.config.min_workers = min_workers;
    }

    /// Set the memory limit (target) and how often we check it (in millis).
    pub fn set_memory_limit(&mut self, limit: usize, check_freq: time::Duration) {
        assert_ne!(limit, 0);
        assert_ne!(check_freq, time::Duration::from_millis(0));
        self.memory_limit = Some(limit);
        self.memory_check_frequency = Some(check_freq);
    }

    /// Set the IP address that the worker should use for listening.
    pub fn set_listen_addr(&mut self, listen_addr: IpAddr) {
        self.listen_addr = listen_addr;
    }

    /// Set the external IP address and port that the worker should advertise to
    /// other noria instances
    pub fn set_external_addr(&mut self, external_addr: SocketAddr) {
        self.external_addr = external_addr;
    }

    /// Set the reuse policy for all subsequent migrations
    pub fn set_reuse(&mut self, reuse_type: Option<ReuseConfigType>) {
        self.config.reuse = reuse_type;
    }

    /// Set the value of [`controller::sql::Config::allow_topk`]
    pub fn set_topk(&mut self, allow_topk: bool) {
        self.config.mir_config.allow_topk = allow_topk;
    }

    /// Set the value of [`controller::sql::Config::allow_paginate`]
    pub fn set_pagination(&mut self, allow_paginate: bool) {
        self.config.mir_config.allow_paginate = allow_paginate;
    }

    /// Set the value of [`controller::sql::Config::allow_mixed_comparisons`]
    pub fn set_mixed_comparisons(&mut self, allow_mixed_comparisons: bool) {
        self.config.mir_config.allow_mixed_comparisons = allow_mixed_comparisons;
    }

    pub fn set_straddled_joins(&mut self, allow_straddled_joins: bool) {
        self.config.materialization_config.allow_straddled_joins = allow_straddled_joins;
    }

    pub fn set_post_lookup(&mut self, allow_post_lookup: bool) {
        self.config.mir_config.allow_post_lookup = allow_post_lookup;
    }

    /// Set the value of [`controller::sql::Config::worker_request_timeout`]
    pub fn set_worker_timeout(&mut self, worker_request_timeout: Duration) {
        self.config.worker_request_timeout = worker_request_timeout;
    }

    /// Set the value of [`Config::background_recovery_interval`]
    pub fn set_background_recovery_interval(&mut self, background_recovery_interval: Duration) {
        self.config.background_recovery_interval = background_recovery_interval;
    }

    /// Set the value of [`DomainConfig::aggressively_update_state_sizes`][0]. See the documentation
    /// of that field for more information
    ///
    /// [0]: readyset_dataflow::Config::aggressively_update_state_sizes.
    pub fn set_aggressively_update_state_sizes(&mut self, value: bool) {
        self.config.domain_config.aggressively_update_state_sizes = value;
    }

    /// Sets the URL for the database to replicate from
    pub fn set_replication_url(&mut self, url: String) {
        self.config.replicator_config.upstream_db_url = Some(url.clone().into());
        self.config.replicator_config.cdc_db_url = Some(url.into());
    }

    /// Sets configuration for the replicator thread
    pub fn set_replicator_config(&mut self, config: UpstreamConfig) {
        self.config.replicator_config = config;
    }

    /// Set the server ID for replication
    pub fn set_replicator_server_id(&mut self, server_id: ReplicationServerId) {
        self.config.replicator_config.replication_server_id = Some(server_id);
    }

    /// Sets the value of [`replicators::Config::disable_upstream_ssl_verification`]
    pub fn set_disable_upstream_ssl_verification(&mut self, value: bool) {
        self.config
            .replicator_config
            .disable_upstream_ssl_verification = value;
    }

    /// Sets the strategy to use to determine how many times to replicate domains
    pub fn set_replication_strategy(&mut self, replication_strategy: ReplicationStrategy) {
        self.config.replication_strategy = replication_strategy
    }

    /// Configures this ReadySet server to accept only domains that contain reader nodes.
    ///
    /// Overwrites any previous call to [`no_readers`]
    pub fn as_reader_only(&mut self) {
        self.domain_scheduling_config.reader_nodes =
            NodeTypeSchedulingRestriction::OnlyWithNodeType;
    }

    /// Configures this ReadySet server to never run domains that contain reader nodes
    ///
    /// Overwrites any previous call to [`as_reader_only`]
    pub fn no_readers(&mut self) {
        self.domain_scheduling_config.reader_nodes =
            NodeTypeSchedulingRestriction::NeverWithNodeType;
    }

    /// Configures this ReadySet server to be unable to become the leader
    pub fn cannot_become_leader(&mut self) {
        self.leader_eligible = false;
    }

    /// Configures the volume id associated with this server.
    pub fn set_volume_id(&mut self, volume_id: VolumeId) {
        self.domain_scheduling_config.volume_id = Some(volume_id);
    }

    /// Set the value of [`Config::abort_on_task_failure`]. See the documentation of that field for
    /// more information.
    pub fn set_abort_on_task_failure(&mut self, abort_on_task_failure: bool) {
        self.config.abort_on_task_failure = abort_on_task_failure;
    }

    /// Sets the value of [`Config::upquery_timeout`]. See documentation of that field for more
    /// information.
    pub fn set_upquery_timeout(&mut self, value: std::time::Duration) {
        self.config.upquery_timeout = value;
    }

    /// Sets the value of [`Config::domain_config::view_request_timeout`]. See documentation of
    /// that field for more information.
    pub fn set_view_request_timeout(&mut self, value: std::time::Duration) {
        self.config.domain_config.view_request_timeout = value;
    }

    /// Sets the value of [`Config::domain_config::verbose_metrics`]. See documentation of
    /// that field for more information.
    pub fn set_verbose_domain_metrics(&mut self, value: bool) {
        self.config.domain_config.verbose_metrics = value;
    }

    /// Sets the value of [`Config::domain_config::verbose_metrics`]. See documentation of
    /// that field for more information.
    pub fn set_materialization_persistence(&mut self, value: bool) {
        self.config.domain_config.materialization_persistence = value;
    }

    /// Sets the value of [`Config::domain_config::table_request_timeout`]. See documentation of
    /// that field for more information.
    pub fn set_table_request_timeout(&mut self, value: std::time::Duration) {
        self.config.domain_config.table_request_timeout = value;
    }

    /// Sets the value of [`Config::replicator_restart_timeout`]. See documentation of
    /// that field for more information.
    pub fn set_replicator_restart_timeout(&mut self, value: std::time::Duration) {
        self.config.replicator_config.replicator_restart_timeout = value;
    }

    /// Sets the value of [`Config::domain_config::eviction_kind`]. See documentation of
    /// that field for more information.
    pub fn set_eviction_kind(&mut self, value: dataflow::EvictionKind) {
        self.config.domain_config.eviction_kind = value;
    }

    /// Assigns a telemetry reporter to this ReadySet server
    pub fn set_telemetry_sender(&mut self, value: TelemetrySender) {
        self.telemetry = value;
    }

    /// Sets whether the server should wait to receive a failpoint request before proceeding it's
    /// startup.
    pub fn set_wait_for_failpoint(&mut self, value: bool) {
        self.wait_for_failpoint = value;
    }

    /// Sets whether to log statements in the replicator
    pub fn set_replicator_statement_logging(&mut self, value: bool) {
        self.config.replicator_statement_logging = value;
    }

    /// Start a server instance and return a handle to it. This method also returns a
    /// [`ShutdownSender`] that should be used to shut down the server when it is no longer needed.
    pub fn start(
        self,
        authority: Arc<Authority>,
    ) -> impl Future<Output = Result<(Handle, ShutdownSender), anyhow::Error>> {
        let Builder {
            listen_addr,
            external_addr,
            ref config,
            memory_limit,
            memory_check_frequency,
            domain_scheduling_config,
            leader_eligible,
            telemetry,
            wait_for_failpoint,
        } = self;

        let config = config.clone();

        crate::startup::start_instance(
            authority,
            listen_addr,
            external_addr,
            config,
            memory_limit,
            memory_check_frequency,
            domain_scheduling_config,
            leader_eligible,
            telemetry,
            wait_for_failpoint,
        )
    }

    /// Start a server instance with readers already created and return a handle to it. This method
    /// also returns a [`ShutdownSender`] that should be used to shut down the server when it is no
    /// longer needed.
    pub async fn start_with_readers(
        self,
        authority: Arc<Authority>,
        readers: dataflow::Readers,
        reader_addr: SocketAddr,
    ) -> Result<(Handle, ShutdownSender), anyhow::Error> {
        let Builder {
            listen_addr,
            external_addr,
            ref config,
            memory_limit,
            memory_check_frequency,
            domain_scheduling_config,
            leader_eligible,
            telemetry,
            wait_for_failpoint,
        } = self;

        let config = config.clone();
        let (shutdown_tx, shutdown_rx) = shutdown::channel();

        let controller = crate::startup::start_instance_inner(
            authority,
            listen_addr,
            external_addr,
            config,
            memory_limit,
            memory_check_frequency,
            domain_scheduling_config,
            leader_eligible,
            readers,
            reader_addr,
            telemetry,
            wait_for_failpoint,
            shutdown_rx,
        )
        .await?;

        Ok((controller, shutdown_tx))
    }

    /// Start a local-only worker, and return a handle to it. This method also returns a
    /// [`ShutdownSender`] that should be used to shut down the server when it is no longer needed.
    pub fn start_local(
        self,
    ) -> impl Future<Output = Result<(Handle, ShutdownSender), anyhow::Error>> {
        let store = Arc::new(LocalAuthorityStore::new());
        self.start_local_custom(Arc::new(Authority::from(LocalAuthority::new_with_store(
            store,
        ))))
    }

    /// Start a local-only worker using a custom authority, and return a handle to it. This method
    /// also returns a [`ShutdownSender`] that should be used to shut down the server when it is no
    /// longer needed.
    pub fn start_local_custom(
        self,
        authority: Arc<Authority>,
    ) -> impl Future<Output = Result<(Handle, ShutdownSender), anyhow::Error>> {
        let fut = self.start(authority);
        async move {
            #[allow(unused_mut)]
            let (mut wh, shutdown_tx) = fut.await?;
            #[cfg(test)]
            wh.backend_ready().await;
            Ok((wh, shutdown_tx))
        }
    }
}
