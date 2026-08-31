//! Probing upstream servers, one-shot or continuously, and the health status the probes maintain.
//!
//! [`validate_servers`] probes the servers once, at startup and on runtime list edits. An
//! unreachable server is reported and marked down. A rejected server, or one whose system
//! properties differ from the rest, causes the whole list to be refused.
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
    Ok {
        connection: Box<DatabaseConnection>,
        /// Read for MySQL servers in a multi-server list, for the homogeneity check.
        properties: Option<ServerSystemProperties>,
    },
    /// Could not reach the server.
    Unreachable(DatabaseError),
    /// The server cannot be used as configured, e.g. bad credentials or a missing database.
    Rejected(String),
}

impl ServerStatus {
    /// The system properties read from a server that is `Ok`.
    pub fn properties(&self) -> Option<&ServerSystemProperties> {
        match self {
            Self::Ok { properties, .. } => properties.as_ref(),
            Self::Unreachable(_) | Self::Rejected(_) => None,
        }
    }
}

impl fmt::Debug for ServerStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ok { properties, .. } => f
                .debug_struct("Ok")
                .field("properties", properties)
                .finish_non_exhaustive(),
            Self::Unreachable(error) => f.debug_tuple("Unreachable").field(error).finish(),
            Self::Rejected(reason) => f.debug_tuple("Rejected").field(reason).finish(),
        }
    }
}

/// The system properties every server in a multi-server list must agree on, so a cached result
/// stays valid on any server. Compare with `system_property_mismatch`, which splits the fields
/// into fatal and advisory differences.
#[derive(Debug, Clone)]
pub struct ServerSystemProperties {
    /// `@@lower_case_table_names`. Differing values change identifier resolution.
    pub lower_case_table_names: String,
    /// `@@global.time_zone`, resolved through `@@system_time_zone` when it is `SYSTEM`.
    pub time_zone: String,
    /// `@@collation_server`. Differing values change comparisons and ordering.
    pub collation: String,
    /// `@@version`. Advisory: skew is normal during a rolling upgrade.
    pub version: String,
    /// `@@global.group_concat_max_len`. Advisory: changes where GROUP_CONCAT truncates.
    pub group_concat_max_len: String,
}

/// Compare `candidate` against `baseline`: a rejection reason, and warnings.
fn system_property_mismatch(
    baseline_display: &str,
    baseline: &ServerSystemProperties,
    candidate: &ServerSystemProperties,
) -> (Option<String>, Vec<String>) {
    type Field = fn(&ServerSystemProperties) -> &String;
    // Differences in the fatal fields change query results; the others only warn.
    let fields: [(&str, Field, bool); 5] = [
        (
            "lower_case_table_names",
            |p| &p.lower_case_table_names,
            true,
        ),
        ("time zone", |p| &p.time_zone, true),
        ("collation_server", |p| &p.collation, true),
        ("version", |p| &p.version, false),
        ("group_concat_max_len", |p| &p.group_concat_max_len, false),
    ];
    let mut fatal = Vec::new();
    let mut warnings = Vec::new();
    for (name, field, is_fatal) in fields {
        let (candidate, base) = (field(candidate), field(baseline));
        if candidate != base {
            let message = format!("{name} is {candidate}, but {baseline_display} has {base}");
            if is_fatal {
                fatal.push(message);
            } else {
                warnings.push(message);
            }
        }
    }
    ((!fatal.is_empty()).then(|| fatal.join("; ")), warnings)
}

/// Read `@@gtid_mode` and the [`ServerSystemProperties`] in one query.
async fn read_system_properties(
    conn: &mut DatabaseConnection,
) -> Result<(String, ServerSystemProperties), DatabaseError> {
    let row = conn
        .query(
            "SELECT CAST(@@lower_case_table_names AS CHAR), @@global.time_zone, \
             @@system_time_zone, @@collation_server, @@version, \
             CAST(@@global.group_concat_max_len AS CHAR), @@gtid_mode",
        )
        .await?
        .into_iter()
        .get_single_row()?;
    let time_zone: String = row.get(1)?;
    let time_zone = if time_zone.eq_ignore_ascii_case("SYSTEM") {
        row.get(2)?
    } else {
        time_zone
    };
    let properties = ServerSystemProperties {
        lower_case_table_names: row.get(0)?,
        time_zone,
        collation: row.get(3)?,
        version: row.get(4)?,
        group_concat_max_len: row.get(5)?,
    };
    Ok((row.get(6)?, properties))
}

/// Connect to `server`, bounded by `timeout`, and classify the outcome. With `check_failover`, a
/// MySQL server must also have `gtid_mode = ON`, so replication can fail over to it, and its
/// system properties are read.
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
    let mut properties = None;
    if check_failover && url.database_type() == DatabaseType::MySQL {
        let (gtid_mode, read) = match read_system_properties(&mut connection).await {
            Ok(read) => read,
            Err(error) => return failure_status(error),
        };
        if !gtid_mode.eq_ignore_ascii_case("ON") {
            return ServerStatus::Rejected(format!(
                "gtid_mode is {gtid_mode}; a list of more than one server requires gtid_mode = ON"
            ));
        }
        properties = Some(read);
    }
    ServerStatus::Ok {
        connection: Box::new(connection),
        properties,
    }
}

fn failure_status(error: DatabaseError) -> ServerStatus {
    match error.upstream_failure() {
        UpstreamFailure::Down => ServerStatus::Unreachable(error),
        UpstreamFailure::Auth => ServerStatus::Rejected(format!("credentials rejected: {error}")),
        UpstreamFailure::Semantic => ServerStatus::Rejected(error.to_string()),
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
    /// Advisory system-property differences on accepted servers, such as version skew.
    pub warnings: Vec<(String, String)>,
}

/// Validate `new` as the list that replaces `current`. Only servers not already in `current`
/// with the same URL are probed, plus one kept server as the baseline for the system-property
/// check, all within three times `timeout`.
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

    let grows_single_list = current.is_some_and(|c| c.is_single()) && !new.is_single();
    let needs_stored_baseline = !grows_single_list && !added.is_empty() && !new.is_single();
    let to_validate: Vec<&UpstreamServer> = if grows_single_list {
        new.iter().collect()
    } else {
        added
    };

    let deadline = timeout.saturating_mul(3);
    let (statuses, mut baseline) = tokio::time::timeout(deadline, async {
        let statuses = probe_all(to_validate, verification, timeout, !new.is_single()).await;
        let baseline = if needs_stored_baseline {
            retained_baseline(&unchanged, verification, timeout).await
        } else {
            None
        };
        (statuses, baseline)
    })
    .await
    .map_err(|_| ValidationError::TimedOut(deadline))?;

    if baseline.is_none() {
        baseline = statuses
            .iter()
            .filter(|(server, _)| retained(server))
            .find_map(|(server, status)| {
                Some((server.display().to_string(), status.properties()?.clone()))
            });
    }

    let mut outcome = ValidationReport {
        unchanged: unchanged.into_iter().cloned().collect(),
        ..Default::default()
    };
    let mut rejected = Vec::new();
    for (server, status) in statuses {
        let display = server.display().to_string();
        let is_retained = retained(server);
        match status {
            ServerStatus::Ok { properties, .. } => {
                if let Some(properties) = &properties {
                    match &baseline {
                        Some((base_display, base)) => {
                            let (fatal, warnings) =
                                system_property_mismatch(base_display, base, properties);
                            if let Some(reason) = fatal {
                                rejected.push(format!("{display} ({reason})"));
                            }
                            for warning in warnings {
                                outcome.warnings.push((display.clone(), warning));
                            }
                        }
                        None => baseline = Some((display.clone(), properties.clone())),
                    }
                }
                if !is_retained {
                    outcome.verified.push(server.clone());
                }
            }
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

/// The system properties of the first reachable retained MySQL server.
async fn retained_baseline(
    retained: &[&UpstreamServer],
    verification: &ServerCertVerification,
    timeout: Duration,
) -> Option<(String, ServerSystemProperties)> {
    for server in retained {
        if server.url().database_type() != DatabaseType::MySQL {
            return None;
        }
        let probe =
            probe_server(server, verification, timeout, VALIDATION_PROGRAM_NAME, true).await;
        if let ServerStatus::Ok {
            properties: Some(properties),
            ..
        } = probe
        {
            return Some((server.display().to_string(), properties));
        }
    }
    None
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
        ServerStatus::Ok {
            connection: conn, ..
        } => {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn props() -> ServerSystemProperties {
        ServerSystemProperties {
            lower_case_table_names: "0".to_string(),
            time_zone: "UTC".to_string(),
            collation: "utf8mb4_0900_ai_ci".to_string(),
            version: "8.4.8".to_string(),
            group_concat_max_len: "1024".to_string(),
        }
    }

    #[test]
    fn matching_properties_raise_nothing() {
        let (fatal, warnings) = system_property_mismatch("a:3306", &props(), &props());
        assert_eq!(fatal, None);
        assert!(warnings.is_empty());
    }

    #[test]
    fn result_affecting_differences_are_fatal() {
        let candidate = ServerSystemProperties {
            lower_case_table_names: "1".to_string(),
            time_zone: "America/Sao_Paulo".to_string(),
            collation: "latin1_swedish_ci".to_string(),
            ..props()
        };
        let (fatal, warnings) = system_property_mismatch("a:3306", &props(), &candidate);
        let fatal = fatal.expect("mismatch is fatal");
        assert!(fatal.contains("lower_case_table_names is 1"), "{fatal}");
        assert!(fatal.contains("time zone is America/Sao_Paulo"), "{fatal}");
        assert!(
            fatal.contains("collation_server is latin1_swedish_ci"),
            "{fatal}"
        );
        assert!(warnings.is_empty());
    }

    #[test]
    fn advisory_differences_only_warn() {
        let candidate = ServerSystemProperties {
            version: "8.0.39".to_string(),
            group_concat_max_len: "4096".to_string(),
            ..props()
        };
        let (fatal, warnings) = system_property_mismatch("a:3306", &props(), &candidate);
        assert_eq!(fatal, None);
        assert_eq!(warnings.len(), 2);
        assert!(warnings[0].contains("version is 8.0.39"), "{}", warnings[0]);
        assert!(
            warnings[1].contains("group_concat_max_len is 4096"),
            "{}",
            warnings[1]
        );
    }
}
