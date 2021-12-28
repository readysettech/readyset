use crate::handle::Handle;
use crate::{Config, FrontierStrategy, ReuseConfigType, VolumeId};
use dataflow::PersistenceParameters;
use noria::consensus::{Authority, LocalAuthority, LocalAuthorityStore};
use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time;

/// Used to construct a worker.
#[derive(Clone)]
pub struct Builder {
    config: Config,
    memory_limit: Option<usize>,
    memory_check_frequency: Option<time::Duration>,
    listen_addr: IpAddr,
    external_addr: SocketAddr,
    region: Option<String>,
    reader_only: bool,
    volume_id: Option<VolumeId>,
    should_reset_state: bool,
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
            region: None,
            reader_only: false,
            volume_id: None,
            should_reset_state: false,
        }
    }
}
impl Builder {
    /// Construct a new [`Builder`] with configuration setup for running tests
    pub fn for_tests() -> Self {
        let mut builder = Self::default();
        builder.set_abort_on_task_failure(false);
        builder
    }

    /// Set the maximum number of concurrent partial replay requests a domain can have outstanding
    /// at any given time.
    ///
    /// Note that this number *must* be greater than the width (in terms of number of ancestors) of
    /// the widest union in the graph, otherwise a deadlock will occur.
    pub fn set_max_concurrent_replay(&mut self, n: usize) {
        self.config.domain_config.concurrent_replays = n;
    }

    /// Set the persistence parameters used by the system.
    pub fn set_persistence(&mut self, p: PersistenceParameters) {
        self.config.persistence = p;
    }

    /// Disable partial materialization for all subsequent migrations
    pub fn disable_partial(&mut self) {
        self.config.materialization_config.partial_enabled = false;
    }

    /// Enable the creation of [`PacketFilter`]s for egresses before readers
    ///
    /// [`PacketFilter`]: noria_dataflow::node::special::PacketFilter
    pub fn enable_packet_filters(&mut self) {
        self.config.materialization_config.packet_filters_enabled = true;
    }

    /// Which nodes should be placed beyond the materialization frontier?
    pub fn set_frontier_strategy(&mut self, f: FrontierStrategy) {
        self.config.materialization_config.frontier_strategy = f;
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
    pub fn set_quorum(&mut self, quorum: usize) {
        assert_ne!(quorum, 0);
        self.config.quorum = quorum;
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
    pub fn set_allow_topk(&mut self, allow_topk: bool) {
        self.config.mir_config.allow_topk = allow_topk;
    }

    /// Set the value of [`controller::sql::Config::allow_range_queries`]
    pub fn set_allow_range_queries(&mut self, allow_range_queries: bool) {
        self.config.mir_config.allow_range_queries = allow_range_queries;
    }

    /// Set the value of [`DomainConfig::aggressively_update_state_sizes`][0]. See the documentation
    /// of that field for more information
    ///
    /// [0]: noria_dataflow::Config::aggressively_update_state_sizes.
    pub fn set_aggressively_update_state_sizes(&mut self, value: bool) {
        self.config.domain_config.aggressively_update_state_sizes = value;
    }

    /// Sets the region that the work is located in.
    pub fn set_region(&mut self, region: String) {
        self.region = Some(region);
    }

    /// Sets the primary region for Noria.
    pub fn set_primary_region(&mut self, region: String) {
        self.config.primary_region = Some(region);
    }

    /// Sets the primary MySQL/PostgreSQL server to replicate from.
    pub fn set_replicator_url(&mut self, url: String) {
        self.config.replication_url = Some(url);
    }

    /// Sets the server uuid to use when registering for a binlog replication slot.
    pub fn set_replication_server_id(&mut self, id: u32) {
        self.config.replication_server_id = Some(id);
    }

    /// Sets whether we should keep the chain of prior recipes when storing a new
    /// recipe. Setting this to false may have unexpected behavior and should be
    /// used with caution. It is currently only used in test environments.
    pub fn set_keep_prior_recipes(&mut self, value: bool) {
        self.config.keep_prior_recipes = value;
    }

    /// Configures this Noria server to accept only reader domains.
    pub fn as_reader_only(&mut self) {
        self.reader_only = true;
    }

    /// Configures the volume id associated with this server.
    pub fn set_volume_id(&mut self, volume_id: VolumeId) {
        self.volume_id = Some(volume_id);
    }

    /// Set the value of [`Config::abort_on_task_failure`]. See the documentation of that field for
    /// more information.
    pub fn set_abort_on_task_failure(&mut self, abort_on_task_failure: bool) {
        self.config.abort_on_task_failure = abort_on_task_failure;
    }

    /// Indicate if deployment state should be reset to DDL origin
    pub fn set_should_reset_state(&mut self, r: bool) {
        self.should_reset_state = r;
    }

    /// Start a server instance and return a handle to it.
    pub fn start(
        self,
        authority: Arc<Authority>,
    ) -> impl Future<Output = Result<Handle, anyhow::Error>> {
        let Builder {
            listen_addr,
            external_addr,
            ref config,
            memory_limit,
            memory_check_frequency,
            region,
            reader_only,
            volume_id,
            should_reset_state,
        } = self;

        let config = config.clone();

        crate::startup::start_instance(
            authority,
            listen_addr,
            external_addr,
            config,
            memory_limit,
            memory_check_frequency,
            region,
            reader_only,
            volume_id,
            should_reset_state,
        )
    }

    /// Start a local-only worker, and return a handle to it.
    pub fn start_local(self) -> impl Future<Output = Result<Handle, anyhow::Error>> {
        let store = Arc::new(LocalAuthorityStore::new());
        self.start_local_custom(Arc::new(Authority::from(LocalAuthority::new_with_store(
            store,
        ))))
    }

    /// Start a local-only worker using a custom authority, and return a handle to it.
    pub fn start_local_custom(
        self,
        authority: Arc<Authority>,
    ) -> impl Future<Output = Result<Handle, anyhow::Error>> {
        let fut = self.start(authority);
        async move {
            #[allow(unused_mut)]
            let mut wh = fut.await?;
            #[cfg(test)]
            wh.backend_ready().await;
            Ok(wh)
        }
    }
}
