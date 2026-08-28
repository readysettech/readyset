//! Probing upstream servers, one-shot or continuously, and the health status the probes maintain.
//!
//! [`validate_servers`] probes the servers once, at startup and on runtime list edits. An
//! unreachable server is reported and marked down; a rejected one causes the whole list to be
//! refused.
//!
//! [`UpstreamHealthMonitor`] holds a connection to each server believed up and pings it every
//! interval, so a server that dies while idle is marked down before a session picks it. Servers
//! marked down are probed again once their backoff has elapsed.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use futures::future::join_all;
use readyset_util::shutdown::ShutdownReceiver;
use thiserror::Error;
use tokio::sync::RwLock;
use tokio::time::Instant;
use tracing::{debug, error, info};

use crate::error::{DatabaseError, UpstreamFailure};
use crate::tls::ServerCertVerification;
use crate::upstream_registry::{PickedServer, SharedUpstreamRegistry, UpstreamServerRegistry};
use crate::upstream_servers::{UpstreamServer, UpstreamServers};
use crate::{DatabaseConnection, DatabaseType, QueryableConnection, UpstreamConfig};

/// Program name the health monitor's connections report to the upstream.
pub const HEALTH_MONITOR_PROGRAM_NAME: &str = "READYSET_HEALTH_MONITOR";

/// Program name validation connections report to the upstream.
pub const VALIDATION_PROGRAM_NAME: &str = "READYSET_VALIDATE";

/// The outcome of probing one server.
pub enum ServerStatus {
    /// Connected, credentials accepted, configuration acceptable.
    Ok(Box<DatabaseConnection>),
    /// Could not reach the server.
    Unreachable(DatabaseError),
    /// The server cannot be used as configured, e.g. bad credentials or a missing database.
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

/// Connect to `server`, bounded by `timeout`, and classify the outcome. With `check_failover`, a
/// MySQL server must also have `gtid_mode = ON`, so replication can fail over to it.
pub async fn probe_server(
    server: &UpstreamServer,
    verification: &ServerCertVerification,
    timeout: Duration,
    program_name: &str,
    check_failover: bool,
) -> ServerStatus {
    let mut url = server.url().clone();
    url.set_program_or_application_name(program_name.to_string());
    let mut connection = match url.connect_bounded(verification, timeout).await {
        Ok(connection) => connection,
        Err(error) => return failure_status(error),
    };
    if check_failover
        && url.database_type() == DatabaseType::MySQL
        && let Err(status) = check_gtid_mode(&mut connection).await
    {
        return status;
    }
    ServerStatus::Ok(Box::new(connection))
}

fn failure_status(error: DatabaseError) -> ServerStatus {
    match error.upstream_failure() {
        UpstreamFailure::Down => ServerStatus::Unreachable(error),
        UpstreamFailure::Auth => ServerStatus::Rejected(format!("credentials rejected: {error}")),
        UpstreamFailure::Semantic => ServerStatus::Rejected(error.to_string()),
    }
}

async fn read_gtid_mode(conn: &mut DatabaseConnection) -> Result<String, DatabaseError> {
    let row = conn
        .query("SELECT @@gtid_mode")
        .await?
        .into_iter()
        .get_single_row()?;
    row.get(0)
}

async fn check_gtid_mode(conn: &mut DatabaseConnection) -> Result<(), ServerStatus> {
    match read_gtid_mode(conn).await {
        Ok(mode) if mode.eq_ignore_ascii_case("ON") => Ok(()),
        Ok(mode) => Err(ServerStatus::Rejected(format!(
            "gtid_mode is {mode}; a list of more than one server requires gtid_mode = ON"
        ))),
        Err(error) => Err(failure_status(error)),
    }
}

/// Probe every server in `servers` concurrently.
async fn probe_all<'a>(
    servers: impl IntoIterator<Item = &'a UpstreamServer>,
    verification: &ServerCertVerification,
    timeout: Duration,
    check_failover: bool,
) -> Vec<(&'a UpstreamServer, ServerStatus)> {
    join_all(servers.into_iter().map(|server| async move {
        let status = probe_server(
            server,
            verification,
            timeout,
            VALIDATION_PROGRAM_NAME,
            check_failover,
        )
        .await;
        (server, status)
    }))
    .await
}

/// Why [`validate_servers`] refused the servers.
#[derive(Debug, Error)]
pub enum ValidationError {
    #[error("server{} cannot be used: {}", if .0.len() == 1 { "" } else { "s" }, .0.join(", "))]
    Rejected(Vec<String>),

    #[error("servers are all unreachable")]
    NoneReachable,

    #[error("server validation did not finish within {}s", .0.as_secs_f64())]
    TimedOut(Duration),
}

/// What [`validate_servers`] found when it accepted the servers.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ValidationReport {
    /// Servers already in the current list with the same URL; not re-validated.
    pub unchanged: Vec<UpstreamServer>,
    /// Servers that connected and passed every check.
    pub verified: Vec<UpstreamServer>,
    /// Servers that could not be reached, by `host:port`, with the error.
    pub unreachable: Vec<(String, String)>,
}

/// Validate `new` as the list that replaces `current`. Only servers not already in `current`
/// with the same URL are probed, all within twice `timeout`.
///
/// Any rejected server causes the list to be refused. Unreachable servers are reported, as long
/// as at least one server is reachable or retained, so an operator can still remove a dead one.
pub async fn validate_servers(
    current: Option<&UpstreamServers>,
    new: &UpstreamServers,
    verification: &ServerCertVerification,
    timeout: Duration,
) -> Result<ValidationReport, ValidationError> {
    let retained = |server: &UpstreamServer| {
        current.is_some_and(|c| c.iter().any(|s| s.raw_url() == server.raw_url()))
    };
    let (unchanged, added): (Vec<_>, Vec<_>) = new.iter().partition(|s| retained(s));

    // A server kept from a single-server list never had the multi-server checks (gtid_mode),
    // so growing the list checks it again.
    let grows_single_list = current.is_some_and(|c| c.is_single()) && !new.is_single();
    let to_validate: Vec<&UpstreamServer> = if grows_single_list {
        new.iter().collect()
    } else {
        added
    };

    let deadline = timeout.saturating_mul(2);
    let statuses = tokio::time::timeout(
        deadline,
        probe_all(to_validate, verification, timeout, !new.is_single()),
    )
    .await
    .map_err(|_| ValidationError::TimedOut(deadline))?;

    let mut outcome = ValidationReport {
        unchanged: unchanged.into_iter().cloned().collect(),
        ..Default::default()
    };
    let mut rejected = Vec::new();
    for (server, status) in statuses {
        let display = server.display().to_string();
        let is_retained = retained(server);
        match status {
            // A revalidated retained server keeps its "unchanged" row.
            ServerStatus::Ok(_) if is_retained => {}
            ServerStatus::Ok(_) => outcome.verified.push(server.clone()),
            // Growing the list needs the kept server's gtid_mode, so it must be reachable.
            ServerStatus::Unreachable(error) if is_retained => {
                rejected.push(format!("{display} (cannot verify gtid_mode: {error})"))
            }
            ServerStatus::Unreachable(error) => {
                outcome.unreachable.push((display, error.to_string()))
            }
            ServerStatus::Rejected(reason) => rejected.push(format!("{display} ({reason})")),
        }
    }
    if !rejected.is_empty() {
        return Err(ValidationError::Rejected(rejected));
    }
    if outcome.unchanged.is_empty() && outcome.verified.is_empty() {
        return Err(ValidationError::NoneReachable);
    }
    Ok(outcome)
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
    let status = probe_server(
        server,
        verification,
        timeout,
        HEALTH_MONITOR_PROGRAM_NAME,
        false,
    )
    .await;
    match status {
        ServerStatus::Ok(conn) => {
            registry.mark_up(idx);
            return Some((*conn, PickedServer::new(registry, idx)));
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
