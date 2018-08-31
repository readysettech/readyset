use consensus::{Authority, LocalAuthority};
use dataflow::PersistenceParameters;

use std::net::IpAddr;
use std::sync::Arc;
use std::time;

use failure;
use slog;

use controller::sql::reuse::ReuseConfigType;
use controller::{self, ControllerConfig, LocalControllerHandle};

/// Used to construct a controller.
pub struct ControllerBuilder {
    config: ControllerConfig,
    memory_limit: Option<usize>,
    memory_check_frequency: Option<time::Duration>,
    listen_addr: IpAddr,
    log: slog::Logger,
}
impl Default for ControllerBuilder {
    fn default() -> Self {
        Self {
            config: ControllerConfig::default(),
            listen_addr: "127.0.0.1".parse().unwrap(),
            log: slog::Logger::root(slog::Discard, o!()),
            memory_limit: None,
            memory_check_frequency: None,
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

    /// Set the memory limit (target) and how often we check it (in millis).
    pub fn set_memory_limit(&mut self, limit: usize, check_freq: time::Duration) {
        assert_ne!(limit, 0);
        assert_ne!(check_freq, time::Duration::from_millis(0));
        self.memory_limit = Some(limit);
        self.memory_check_frequency = Some(check_freq);
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
    pub fn build<A: Authority + 'static>(
        self,
        authority: Arc<A>,
    ) -> Result<LocalControllerHandle<A>, failure::Error> {
        controller::start_instance(
            authority,
            self.listen_addr,
            self.config,
            self.memory_limit,
            self.memory_check_frequency,
            self.log,
        )
    }

    /// Build a local controller, and return a ControllerHandle to provide access to it.
    pub fn build_local(self) -> Result<LocalControllerHandle<LocalAuthority>, failure::Error> {
        #[allow(unused_mut)]
        let mut lch = self.build(Arc::new(LocalAuthority::new()))?;
        #[cfg(test)]
        lch.wait_until_ready();
        Ok(lch)
    }
}
