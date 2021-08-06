use crate::handle::Handle;
use crate::{Config, FrontierStrategy, ReuseConfigType, VolumeId};
use dataflow::PersistenceParameters;
use noria::consensus::{Authority, LocalAuthority, LocalAuthorityStore};
use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time;
use std::time::Duration;

/// Used to construct a worker.
#[derive(Clone)]
pub struct Builder {
    config: Config,
    memory_limit: Option<usize>,
    memory_check_frequency: Option<time::Duration>,
    listen_addr: IpAddr,
    external_addr: SocketAddr,
    log: slog::Logger,
    region: Option<String>,
    replicator_url: Option<String>,
    reader_only: bool,
    volume_id: Option<VolumeId>,
}

impl Default for Builder {
    fn default() -> Self {
        #[allow(clippy::unwrap_used)] // hardcoded literals
        Self {
            config: Config::default(),
            listen_addr: "127.0.0.1".parse().unwrap(),
            external_addr: "127.0.0.1:6033".parse().unwrap(),
            log: slog::Logger::root(slog::Discard, o!()),
            memory_limit: None,
            memory_check_frequency: None,
            region: None,
            replicator_url: None,
            reader_only: false,
            volume_id: None,
        }
    }
}
impl Builder {
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
        self.config.partial_enabled = false;
    }

    /// Which nodes should be placed beyond the materialization frontier?
    pub fn set_frontier_strategy(&mut self, f: FrontierStrategy) {
        self.config.frontier_strategy = f;
    }

    /// Set sharding policy for all subsequent migrations; `None` disables
    pub fn set_sharding(&mut self, shards: Option<usize>) {
        self.config.sharding = shards;
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

    /// Set the logger that the derived worker should use. By default, it uses `slog::Discard`.
    pub fn log_with(&mut self, log: slog::Logger) {
        self.log = log;
    }

    /// Set the reuse policy for all subsequent migrations
    pub fn set_reuse(&mut self, reuse_type: Option<ReuseConfigType>) {
        self.config.reuse = reuse_type;
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
        self.replicator_url = Some(url);
    }

    /// Configures this Noria server to accept only reader domains.
    pub fn as_reader_only(&mut self) {
        self.reader_only = true;
    }

    /// Configures the interval at which the Workers send a heartbeat.
    pub fn set_heartbeat_interval(&mut self, duration: Duration) {
        self.config.heartbeat_every = duration;
    }

    /// Configures the interval at which a health check on the Workers must be run.
    pub fn set_healthcheck_interval(&mut self, duration: Duration) {
        self.config.healthcheck_every = duration;
    }

    /// Configures the volume id associated with this server.
    pub fn set_volume_id(&mut self, volume_id: VolumeId) {
        self.volume_id = Some(volume_id);
    }

    /// Start a server instance and return a handle to it.
    pub fn start<A: Authority + 'static>(
        self,
        authority: Arc<A>,
    ) -> impl Future<Output = Result<Handle<A>, anyhow::Error>> {
        let Builder {
            listen_addr,
            external_addr,
            ref config,
            memory_limit,
            memory_check_frequency,
            ref log,
            region,
            replicator_url,
            reader_only,
            volume_id,
        } = self;

        let config = config.clone();
        let log = log.clone();

        crate::startup::start_instance(
            authority,
            listen_addr,
            external_addr,
            config,
            memory_limit,
            memory_check_frequency,
            log,
            region,
            replicator_url,
            reader_only,
            volume_id,
        )
    }

    /// Start a local-only worker, and return a handle to it.
    pub fn start_local(
        self,
    ) -> impl Future<Output = Result<Handle<LocalAuthority>, anyhow::Error>> {
        let store = Arc::new(LocalAuthorityStore::new());
        self.start_local_custom(Arc::new(LocalAuthority::new_with_store(store)))
    }

    /// Start a local-only worker using a custom authority, and return a handle to it.
    pub fn start_local_custom(
        self,
        authority: Arc<LocalAuthority>,
    ) -> impl Future<Output = Result<Handle<LocalAuthority>, anyhow::Error>> {
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
