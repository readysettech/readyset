use crate::handle::Handle;
use crate::Config;
use crate::FrontierStrategy;
use crate::ReuseConfigType;
use dataflow::PersistenceParameters;
use noria::consensus::{Authority, LocalAuthority};
use std::future::Future;
use std::net::IpAddr;
use std::sync::Arc;
use std::time;

/// Used to construct a worker.
pub struct Builder {
    config: Config,
    memory_limit: Option<usize>,
    memory_check_frequency: Option<time::Duration>,
    listen_addr: IpAddr,
    log: slog::Logger,
}
impl Default for Builder {
    fn default() -> Self {
        Self {
            config: Config::default(),
            listen_addr: "127.0.0.1".parse().unwrap(),
            log: slog::Logger::root(slog::Discard, o!()),
            memory_limit: None,
            memory_check_frequency: None,
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

    /// Set the logger that the derived worker should use. By default, it uses `slog::Discard`.
    pub fn log_with(&mut self, log: slog::Logger) {
        self.log = log;
    }

    /// Set the reuse policy for all subsequent migrations
    pub fn set_reuse(&mut self, reuse_type: ReuseConfigType) {
        self.config.reuse = reuse_type;
    }

    /// Set the number of pool threads to use (default is #cores)
    pub fn set_threads(&mut self, threads: usize) {
        self.config.threads = Some(threads);
        unimplemented!();
    }

    /// Start a server instance and return a handle to it.
    ///
    /// The second item of the returned tuple is a future that resolves when Noria is idle.
    #[must_use]
    pub fn start<A: Authority + 'static>(
        &self,
        authority: Arc<A>,
    ) -> impl Future<Output = Result<(Handle<A>, impl Future<Output = ()> + Unpin + Send), failure::Error>>
    {
        let Builder {
            listen_addr,
            ref config,
            memory_limit,
            memory_check_frequency,
            ref log,
        } = *self;

        let config = config.clone();
        let log = log.clone();

        crate::startup::start_instance(
            authority,
            listen_addr,
            config,
            memory_limit,
            memory_check_frequency,
            log,
        )
    }

    /// Start a local-only worker, and return a handle to it.
    #[must_use]
    pub fn start_local(
        &self,
    ) -> impl Future<
        Output = Result<
            (
                Handle<LocalAuthority>,
                impl Future<Output = ()> + Unpin + Send,
            ),
            failure::Error,
        >,
    > {
        let fut = self.start(Arc::new(LocalAuthority::new()));
        async move {
            #[allow(unused_mut)]
            let (mut wh, done) = fut.await?;
            #[cfg(test)]
            wh.backend_ready().await;
            Ok((wh, done))
        }
    }
}
