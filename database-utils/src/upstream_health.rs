//! Probing upstream servers, and the health status the probes maintain.
//!
//! [`UpstreamHealthMonitor`] holds a connection to each server believed up and pings it every
//! interval, so a server that dies while idle is marked down before a session picks it. Servers
//! marked down are probed again once their backoff has elapsed.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use readyset_util::shutdown::ShutdownReceiver;
use tokio::sync::RwLock;
use tokio::time::Instant;
use tracing::{debug, error, info};

use crate::error::{DatabaseError, UpstreamFailure};
use crate::tls::ServerCertVerification;
use crate::upstream_registry::{PickedServer, SharedUpstreamRegistry, UpstreamServerRegistry};
use crate::upstream_servers::UpstreamServer;
use crate::{DatabaseConnection, UpstreamConfig};

/// Program name the health monitor's connections report to the upstream.
pub const HEALTH_MONITOR_PROGRAM_NAME: &str = "READYSET_HEALTH_MONITOR";

/// The outcome of probing one server.
pub enum ServerStatus {
    /// Connected, credentials accepted, configuration acceptable.
    Ok(DatabaseConnection),
    /// Could not reach the server.
    Unreachable(DatabaseError),
    /// The server can never be used as configured: bad credentials or a missing database.
    Rejected(String),
}

impl fmt::Debug for ServerStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ok(_) => f.write_str("Ok"),
            Self::Unreachable(error) => f.debug_tuple("Unreachable").field(error).finish(),
            Self::Rejected(reason) => f.debug_tuple("Rejected").field(reason).finish(),
        }
    }
}

/// Connect to `server`, bounded by `timeout`, and classify the outcome.
pub async fn probe_server(
    server: &UpstreamServer,
    verification: &ServerCertVerification,
    timeout: Duration,
    program_name: &str,
) -> ServerStatus {
    let mut url = server.url().clone();
    url.set_program_or_application_name(program_name.to_string());
    match url.connect_bounded(verification, timeout).await {
        Ok(connection) => ServerStatus::Ok(connection),
        Err(error) => connect_failure_status(error),
    }
}

fn connect_failure_status(error: DatabaseError) -> ServerStatus {
    match error.upstream_failure() {
        UpstreamFailure::Down => ServerStatus::Unreachable(error),
        UpstreamFailure::Auth => ServerStatus::Rejected(format!("credentials rejected: {error}")),
        UpstreamFailure::Semantic => ServerStatus::Rejected(error.to_string()),
    }
}

/// Most servers one monitor pass checks at once.
const MAX_CONCURRENT_CHECKS: usize = 16;

/// A monitor connection to a server believed up, counted against that server.
type HeldConnection = (DatabaseConnection, PickedServer);

/// Pings the servers believed up every interval, and probes down servers as their backoff runs
/// out.
pub struct UpstreamHealthMonitor {
    config: Arc<RwLock<UpstreamConfig>>,
    registry: Arc<SharedUpstreamRegistry>,
    interval: Duration,
    shutdown: ShutdownReceiver,
    /// Long-lived connections to servers believed up, keyed by `host:port`.
    connections: HashMap<String, HeldConnection>,
}

impl fmt::Debug for UpstreamHealthMonitor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UpstreamHealthMonitor")
            .field("interval", &self.interval)
            .field("connections", &self.connections.keys())
            .finish_non_exhaustive()
    }
}

impl UpstreamHealthMonitor {
    pub fn new(
        config: Arc<RwLock<UpstreamConfig>>,
        registry: Arc<SharedUpstreamRegistry>,
        interval: Duration,
        shutdown: ShutdownReceiver,
    ) -> Self {
        Self {
            config,
            registry,
            interval,
            shutdown,
            connections: HashMap::new(),
        }
    }

    pub async fn run(mut self) {
        let config = self.config.read().await.clone();
        let verification = match ServerCertVerification::from(&config).await {
            Ok(verification) => verification,
            Err(error) => {
                error!(%error, "Upstream health monitor cannot load the TLS configuration");
                return;
            }
        };
        info!(
            interval_secs = self.interval.as_secs(),
            "Starting upstream health monitor"
        );
        let timeout = config.upstream_connect_timeout;
        let mut next_ping = Instant::now();
        loop {
            let ping_up = Instant::now() >= next_ping;
            if ping_up {
                next_ping = Instant::now() + self.interval;
            }
            self.pass(&verification, timeout, ping_up).await;

            // Wake for the next ping, or sooner when a down server's probe is due.
            let probe_due = self.registry.current().and_then(|r| r.next_probe_due_in());
            let wake = probe_due.map_or(next_ping, |due| next_ping.min(Instant::now() + due));
            tokio::select! {
                _ = self.shutdown.recv() => break,
                _ = tokio::time::sleep_until(wake) => {}
            }
        }
    }

    /// Probe the down servers whose backoff has elapsed and, with `ping_up`, ping the servers
    /// believed up. Up to [`MAX_CONCURRENT_CHECKS`] servers are checked at once.
    async fn pass(
        &mut self,
        verification: &ServerCertVerification,
        timeout: Duration,
        ping_up: bool,
    ) {
        let Some(registry) = self.registry.current() else {
            self.connections.clear();
            return;
        };
        self.connections
            .retain(|name, _| registry.servers().position(name).is_some());

        let checks: Vec<_> = (0..registry.len())
            .filter(|&idx| {
                if registry.is_up(idx) {
                    ping_up
                } else {
                    registry.probe_due(idx)
                }
            })
            .filter_map(|idx| {
                let held = self.connections.remove(registry.get(idx)?.display());
                Some(ping_or_connect(&registry, idx, held, verification, timeout))
            })
            .collect();
        let mut checks = futures::stream::iter(checks).buffer_unordered(MAX_CONCURRENT_CHECKS);
        while let Some(kept) = checks.next().await {
            if let Some(held) = kept {
                self.connections
                    .insert(held.1.server().display().to_string(), held);
            }
        }
    }
}

/// Ping server `idx` over its held connection, or connect to it when there is none or the ping
/// fails. Returns the connection to keep, if any.
async fn ping_or_connect(
    registry: &Arc<UpstreamServerRegistry>,
    idx: usize,
    held: Option<HeldConnection>,
    verification: &ServerCertVerification,
    timeout: Duration,
) -> Option<HeldConnection> {
    if let Some((mut conn, picked)) = held {
        // Bound the ping so a server that never answers does not hold up the pass.
        let error = match tokio::time::timeout(timeout, conn.ping()).await {
            Ok(Ok(())) => {
                registry.mark_up(idx);
                return Some((conn, picked));
            }
            Ok(Err(error)) => error.to_string(),
            Err(_) => format!("no response within {timeout:?}"),
        };
        // The held connection may have been killed on its own, e.g. by KILL. Try reconnecting.
        debug!(server = %picked.server().display(), %error, "Health ping failed; reconnecting");
    }
    connect_and_record(registry, idx, verification, timeout).await
}

/// Connect to server `idx` and update its status. Returns the new connection when it is
/// reachable.
async fn connect_and_record(
    registry: &Arc<UpstreamServerRegistry>,
    idx: usize,
    verification: &ServerCertVerification,
    timeout: Duration,
) -> Option<HeldConnection> {
    let server = registry.get(idx)?;
    let was_down = !registry.is_up(idx);
    match probe_server(server, verification, timeout, HEALTH_MONITOR_PROGRAM_NAME).await {
        ServerStatus::Ok(conn) => {
            registry.mark_up(idx);
            return Some((conn, PickedServer::new(registry, idx)));
        }
        ServerStatus::Unreachable(error) => {
            if !registry.mark_down(idx, &error) {
                registry.record_probe_failure(idx);
                debug!(server = %server.display(), %error, "Upstream server still down");
            }
        }
        ServerStatus::Rejected(reason) => {
            // Back off so a down server that keeps rejecting is not probed in a tight loop.
            if was_down {
                registry.record_probe_failure(idx);
            }
            debug!(server = %server.display(), %reason, "Health probe could not open a connection");
        }
    }
    None
}
