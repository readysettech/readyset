use consensus::{Authority, LocalAuthority};
use dataflow::PersistenceParameters;

use std::net::IpAddr;
use std::sync::Arc;
use std::time;

use slog;

use controller::handle::ControllerHandle;
use controller::sql::reuse::ReuseConfigType;
use controller::{self, ControllerConfig};

/// Used to construct a controller.
pub struct ControllerBuilder {
    config: ControllerConfig,
    nworker_threads: usize,
    nread_threads: usize,
    memory_limit: Option<usize>,
    listen_addr: IpAddr,
    log: slog::Logger,
}
impl Default for ControllerBuilder {
    fn default() -> Self {
        Self {
            config: ControllerConfig::default(),
            listen_addr: "127.0.0.1".parse().unwrap(),
            log: slog::Logger::root(slog::Discard, o!()),
            nworker_threads: 2,
            nread_threads: 1,
            memory_limit: None,
        }
    }
}
impl ControllerBuilder {
    /// Set the maximum number of concurrent partial replay requests a domain can have outstanding
    /// at any given time.
    ///
    /// Note that this number *must* be greater than the width (in terms of number of ancestors) of
    /// the widest union in the graph, otherwise a deadlock will occur.
    pub fn set_max_concurrent_replay(&mut self, n: usize) {
        self.config.domain_config.concurrent_replays = n;
    }

    /// Set the longest time a partial replay response can be delayed.
    pub fn set_partial_replay_batch_timeout(&mut self, t: time::Duration) {
        self.config.domain_config.replay_batch_timeout = t;
    }

    /// Set the persistence parameters used by the system.
    pub fn set_persistence(&mut self, p: PersistenceParameters) {
        self.config.persistence = p;
    }

    /// Disable partial materialization for all subsequent migrations
    pub fn disable_partial(&mut self) {
        self.config.partial_enabled = false;
    }

    /// Set sharding policy for all subsequent migrations; `None` disables
    pub fn set_sharding(&mut self, shards: Option<usize>) {
        self.config.sharding = shards;
    }

    /// Set how many workers the controller should wait for before starting. More workers can join
    /// later, but they won't be assigned any of the initial domains.
    pub fn set_quorum(&mut self, quorum: usize) {
        assert_ne!(quorum, 0);
        self.config.quorum = quorum;
    }

    /// Set the number of worker threads used by this instance.
    pub fn set_worker_threads(&mut self, threads: usize) {
        assert_ne!(threads, 0);
        self.nworker_threads = threads;
    }

    /// Set the number of read threads that should be run on this instance.
    pub fn set_read_threads(&mut self, threads: usize) {
        assert_ne!(threads, 0);
        self.nread_threads = threads;
    }

    /// Set the number of worker threads used by this instance.
    pub fn set_memory_limit(&mut self, limit: usize) {
        assert_ne!(limit, 0);
        self.memory_limit = Some(limit);
    }

    /// Set the IP address that the controller should use for listening.
    pub fn set_listen_addr(&mut self, listen_addr: IpAddr) {
        self.listen_addr = listen_addr;
    }

    /// Set the logger that the derived controller should use. By default, it uses `slog::Discard`.
    pub fn log_with(&mut self, log: slog::Logger) {
        self.log = log;
    }

    /// Set the reuse policy for all subsequent migrations
    pub fn set_reuse(&mut self, reuse_type: ReuseConfigType) {
        self.config.reuse = reuse_type;
    }

    /// Build a controller and return a handle to it.
    pub fn build<A: Authority + 'static>(self, authority: Arc<A>) -> ControllerHandle<A> {
        controller::start_instance(
            authority,
            self.listen_addr,
            self.config,
            self.nworker_threads,
            self.nread_threads,
            self.memory_limit,
            self.log,
        )
    }

    /// Build a local controller, and return a ControllerHandle to provide access to it.
    pub fn build_local(self) -> ControllerHandle<LocalAuthority> {
        self.build(Arc::new(LocalAuthority::new()))
    }
}
