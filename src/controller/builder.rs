use consensus::{Authority, LocalAuthority};
use dataflow::PersistenceParameters;

use std::time;
use std::net::IpAddr;

use slog;

use controller::handle::ControllerHandle;
use controller::{Controller, ControllerConfig};

/// Used to construct a controller.
pub struct ControllerBuilder {
    config: ControllerConfig,
    listen_addr: IpAddr,
    log: slog::Logger,
}
impl Default for ControllerBuilder {
    fn default() -> Self {
        Self {
            config: ControllerConfig::default(),
            listen_addr: "127.0.0.1".parse().unwrap(),
            log: slog::Logger::root(slog::Discard, o!()),
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

    /// Set the maximum number of partial replay responses that can be aggregated into a single
    /// replay batch.
    pub fn set_partial_replay_batch_size(&mut self, n: usize) {
        self.config.domain_config.replay_batch_size = n;
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
    pub fn set_nworkers(&mut self, workers: usize) {
        self.config.nworkers = workers;
    }

    /// Set how many threads should be set up when operating in local mode.
    pub fn set_local_read_threads(&mut self, n: usize) {
        self.config.nreaders = n;
    }

    /// Set the IP address that the controller should use for listening.
    pub fn set_listen_addr(&mut self, listen_addr: IpAddr) {
        self.listen_addr = listen_addr;
    }

    /// Set the number of worker threads to spin up in local mode (when nworkers == 0).
    pub fn set_local_workers(&mut self, workers: usize) {
        self.config.local_workers = workers;
    }

    #[cfg(test)]
    pub fn build_inner(self) -> ::controller::ControllerInner {
        use std::net::SocketAddr;
        use dataflow::checktable::service::CheckTableServer;
        use controller::{ControllerInner, ControllerState};

        let checktable_addr = CheckTableServer::start(SocketAddr::new(self.listen_addr, 0));
        let initial_state = ControllerState {
            config: self.config,
            recipe: (),
            epoch: LocalAuthority::get_epoch(),
        };
        ControllerInner::new(self.listen_addr, checktable_addr, self.log, initial_state)
    }

    /// Set the logger that the derived controller should use. By default, it uses `slog::Discard`.
    pub fn log_with(&mut self, log: slog::Logger) {
        self.log = log;
    }

    /// Build a controller and return a handle to it.
    pub fn build<A: Authority + 'static>(self, authority: A) -> ControllerHandle<A> {
        Controller::start(authority, self.listen_addr, self.config, self.log)
    }

    /// Build a local controller, and return a ControllerHandle to provide access to it.
    pub fn build_local(self) -> ControllerHandle<LocalAuthority> {
        self.build(LocalAuthority::new())
    }
}
