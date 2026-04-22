//! Background task that periodically queries the upstream database's current replication
//! position and compares it to ReadySet's position, computing replication lag metrics.
//!
//! The task uses connect-query-disconnect on each poll to avoid holding an idle connection
//! to the upstream database.
//!
//! When heartbeat is enabled, the task also writes a timestamp to a heartbeat table in the
//! upstream. The replicator intercepts the replicated row and records the timestamp in
//! the shared [`SharedReplicatorProgress`]. The lag reporter reads it back and computes
//! time-based staleness as `now() - heartbeat_timestamp`.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};

use metrics::{counter, gauge};
use readyset_client::metrics::recorded;
use readyset_client::status::ReplicationLagStatus;
use readyset_errors::{internal_err, ReadySetResult};
use replication_offset::mysql::MySqlPosition;
use replication_offset::mysql_gtid::GtidSet;
use replication_offset::postgres::{Lsn, PostgresPosition};
use replication_offset::ReplicationOffset;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};
use uuid::Uuid;

use mysql_async::prelude::*;

use crate::mysql_connector::get_mysql_version;

/// Shared state for replication lag, readable by vrels and other consumers.
pub type SharedLagStatus = Arc<RwLock<Option<ReplicationLagStatus>>>;

/// Internal state published by the main replication loop to the lag reporter. All fields
/// are private; writers go through [`SharedReplicatorProgress`] methods and the reporter
/// reads via [`SharedReplicatorProgress::snapshot`] to get a consistent view in one
/// critical section.
#[derive(Default, Debug)]
struct ReplicatorProgress {
    /// Last event consumed from the upstream stream, published by the main replication
    /// loop after each action is successfully applied.
    stream_position: Option<ReplicationOffset>,
    /// Max offset the replicator has successfully applied across all tables (the
    /// persist frontier). Published by the main replication loop alongside
    /// `stream_position` after each successfully applied action. This is the
    /// replicator's in-memory view of applied offsets and lags true on-disk durability
    /// by the depth of the base-table domain queue plus the RocksDB flush interval.
    persisted_offset: Option<ReplicationOffset>,
    /// Timestamp from the most recent heartbeat row intercepted in the replication
    /// stream. `None` when heartbeat is disabled or no heartbeat has been seen yet.
    /// Independent of [`stream_position`]; heartbeat rows are filtered before they
    /// reach the dataflow, so this timestamp is not tied to any particular offset.
    heartbeat_ts: Option<SystemTime>,
}

/// Point-in-time copy of [`ReplicatorProgress`] returned by
/// [`SharedReplicatorProgress::snapshot`]. All fields reflect a single consistent moment
/// because the snapshot is taken under one read lock.
#[derive(Debug, Default)]
pub struct ReplicatorProgressSnapshot {
    pub stream_position: Option<ReplicationOffset>,
    pub persisted_offset: Option<ReplicationOffset>,
    pub heartbeat_ts: Option<SystemTime>,
}

/// Handle to the replicator-progress state shared between the replicator and the lag
/// reporter. Cheaply cloneable (`Arc`-backed). Writers call the setter methods; the
/// reporter calls [`snapshot`](Self::snapshot) to read all fields atomically.
///
/// Uses [`parking_lot::RwLock`] because every critical section is a handful of field
/// assignments or clones — no I/O, no `.await`. An async lock would add future allocation
/// and task-wake overhead without solving any problem the sync lock doesn't.
///
/// Lock order: this lock is always innermost — callers must not acquire any other lock
/// while holding it.
#[derive(Clone, Debug, Default)]
pub struct SharedReplicatorProgress(Arc<parking_lot::RwLock<ReplicatorProgress>>);

impl SharedReplicatorProgress {
    /// Publish the latest consumed stream position.
    pub fn set_stream_position(&self, pos: ReplicationOffset) {
        self.0.write().stream_position = Some(pos);
    }

    /// Publish both the last consumed stream position and the current persist frontier
    /// in one critical section. Called after every successfully applied action so the
    /// lag reporter always sees `persisted_offset <= stream_position` (required for
    /// `persist_lag >= consume_lag`).
    pub fn publish_after_apply(
        &self,
        stream_position: ReplicationOffset,
        persisted_offset: Option<ReplicationOffset>,
    ) {
        let mut guard = self.0.write();
        guard.stream_position = Some(stream_position);
        guard.persisted_offset = persisted_offset;
    }

    /// Record the timestamp of the most recent heartbeat row intercepted on the
    /// replication stream.
    pub fn set_heartbeat_ts(&self, ts: SystemTime) {
        self.0.write().heartbeat_ts = Some(ts);
    }

    /// Take a consistent snapshot of all progress fields in one critical section.
    pub fn snapshot(&self) -> ReplicatorProgressSnapshot {
        let guard = self.0.read();
        ReplicatorProgressSnapshot {
            stream_position: guard.stream_position.clone(),
            persisted_offset: guard.persisted_offset.clone(),
            heartbeat_ts: guard.heartbeat_ts,
        }
    }
}

/// Heartbeat table name for Postgres (schema-qualified).
pub const PG_HEARTBEAT_TABLE: &str = "readyset._heartbeat";
/// Heartbeat schema for Postgres.
pub const PG_HEARTBEAT_SCHEMA: &str = "readyset";
/// Heartbeat table name (unqualified) for Postgres.
pub const PG_HEARTBEAT_TABLE_NAME: &str = "_heartbeat";

/// Heartbeat table name for MySQL.
pub const MYSQL_HEARTBEAT_TABLE: &str = "_readyset_heartbeat";

/// Default upper bound the lag reporter waits for an upstream TCP connect. The outer
/// per-poll timeout covers the rest of each poll, but the heartbeat-table setup at
/// startup runs outside that timeout — a connect timeout here is what bounds it. Sized
/// to be well above a normal handshake under load and well below any interval the user
/// can configure. Can be overridden at startup via the
/// [`LAG_CONNECT_TIMEOUT_ENV`] environment variable; the override exists primarily so
/// tests can lower the value well below the normal default.
const DEFAULT_LAG_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Environment variable override for [`DEFAULT_LAG_CONNECT_TIMEOUT`], specified in
/// milliseconds. Not a user-facing CLI flag — the only production use case is testing.
pub const LAG_CONNECT_TIMEOUT_ENV: &str = "READYSET_LAG_CONNECT_TIMEOUT_MS";

/// Resolve the current connect-timeout budget, honoring
/// [`LAG_CONNECT_TIMEOUT_ENV`] if set to a valid millisecond value.
fn lag_connect_timeout() -> Duration {
    std::env::var(LAG_CONNECT_TIMEOUT_ENV)
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(DEFAULT_LAG_CONNECT_TIMEOUT)
}

/// Budget for `setup_heartbeat_table`, which runs a CREATE TABLE IF NOT EXISTS + a
/// housekeeping DELETE after connecting. Gives the DDL a generous window on top of the
/// connect budget so a metadata-lock stall during the CREATE can't wedge the reporter
/// task at startup.
fn heartbeat_setup_timeout() -> Duration {
    lag_connect_timeout() * 3
}

/// Configuration for the upstream connection used by the lag reporter.
pub enum UpstreamLagConfig {
    Postgres {
        config: Box<tokio_postgres::Config>,
        tls: postgres_native_tls::MakeTlsConnector,
        heartbeat: bool,
    },
    MysqlFile {
        opts: mysql_async::Opts,
        heartbeat: bool,
    },
    MysqlGtid {
        opts: mysql_async::Opts,
        heartbeat: bool,
    },
}

impl UpstreamLagConfig {
    fn heartbeat_enabled(&self) -> bool {
        match self {
            UpstreamLagConfig::Postgres { heartbeat, .. } => *heartbeat,
            UpstreamLagConfig::MysqlFile { heartbeat, .. } => *heartbeat,
            UpstreamLagConfig::MysqlGtid { heartbeat, .. } => *heartbeat,
        }
    }

    /// Applies the current connect-timeout budget to the embedded Postgres config.
    /// Called once before the reporter task enters its poll loop, so every subsequent
    /// PG connect — including the heartbeat-table setup that runs outside the per-poll
    /// timeout — inherits a bounded handshake. This is a no-op for MySQL variants:
    /// `mysql_async::Opts` on the readyset fork doesn't expose a connect-timeout
    /// setter, so MySQL connects are bounded at the call sites via
    /// [`mysql_connect_bounded`] instead. The MySQL arms are spelled out explicitly
    /// so that adding a new upstream variant forces an audit.
    fn with_pg_connect_timeout(self) -> Self {
        match self {
            UpstreamLagConfig::Postgres {
                mut config,
                tls,
                heartbeat,
            } => {
                config.connect_timeout(lag_connect_timeout());
                UpstreamLagConfig::Postgres {
                    config,
                    tls,
                    heartbeat,
                }
            }
            UpstreamLagConfig::MysqlFile { .. } | UpstreamLagConfig::MysqlGtid { .. } => self,
        }
    }
}

/// Open a MySQL connection with a bounded handshake. Used by every MySQL connect the
/// lag reporter makes so neither the heartbeat setup (outside the per-poll timeout)
/// nor a per-poll connect can hang indefinitely on an unreachable upstream.
///
/// Errors are returned without a prefix so call sites add a single layer of context
/// (e.g. "Failed to connect for heartbeat setup"); wrapping here too would produce
/// double-layered messages.
async fn mysql_connect_bounded(opts: &mysql_async::Opts) -> ReadySetResult<mysql_async::Conn> {
    let budget = lag_connect_timeout();
    let connect = async {
        // Use fail::eval rather than set_failpoint! so the injected delay is async
        // and gets cancelled cleanly by the surrounding timeout. Configured via
        // `return(delay_ms)`, e.g. `"1*return(60000)"`.
        #[cfg(feature = "failure_injection")]
        if let Some(delay_ms) =
            fail::eval(readyset_util::failpoints::REPLICATION_LAG_CONNECT, |v| {
                v.and_then(|s| s.parse::<u64>().ok())
            })
            .flatten()
        {
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        }
        mysql_async::Conn::new(opts.clone()).await
    };
    match tokio::time::timeout(budget, connect).await {
        Ok(Ok(conn)) => Ok(conn),
        Ok(Err(e)) => Err(internal_err!("{e}")),
        Err(_) => Err(internal_err!(
            "connect timed out after {}ms",
            budget.as_millis()
        )),
    }
}

/// Reporter-level configuration (not upstream-specific).
struct ReporterConfig {
    poll_interval: Duration,
    instance_id: String,
}

/// Groups heartbeat-related parameters for `poll_lag`.
struct HeartbeatContext<'a> {
    active: bool,
    report_staleness: bool,
    reporter: &'a ReporterConfig,
}

/// Spawns the replication lag reporter as a background task.
///
/// `lag_status` is the shared state that the controller reads via RPC.
/// `server_uuid` is used as the heartbeat instance ID if set (from
/// `--replication-server-uuid`); otherwise a random UUID is generated.
/// Returns the shared replicator-progress handle (which the caller passes back to the
/// adapter so it can publish updates) and the heartbeat instance ID.
pub fn spawn_lag_reporter(
    upstream_config: UpstreamLagConfig,
    cancel: CancellationToken,
    lag_status: SharedLagStatus,
    poll_interval: Duration,
    server_uuid: Option<Uuid>,
) -> (SharedReplicatorProgress, String) {
    let progress = SharedReplicatorProgress::default();
    let instance_id = server_uuid.unwrap_or_else(Uuid::new_v4).to_string();

    let progress_clone = progress.clone();
    let reporter_config = ReporterConfig {
        poll_interval,
        instance_id: instance_id.clone(),
    };

    tokio::spawn(async move {
        lag_reporter_loop(
            upstream_config,
            lag_status,
            progress_clone,
            cancel,
            reporter_config,
        )
        .await;
    });

    (progress, instance_id)
}

async fn lag_reporter_loop(
    upstream_config: UpstreamLagConfig,
    lag_status: SharedLagStatus,
    progress: SharedReplicatorProgress,
    cancel: CancellationToken,
    reporter: ReporterConfig,
) {
    // Bound every upstream connect the reporter makes, including the heartbeat-table
    // setup below (which runs outside the per-poll timeout). Without this an
    // unreachable upstream at startup would hang the reporter task indefinitely.
    let upstream_config = upstream_config.with_pg_connect_timeout();

    // Attempt heartbeat table setup if enabled. The connect budget bounds the TCP
    // handshake; the outer timeout here additionally bounds any DDL that runs after
    // the connect completes, so a metadata-lock stall on CREATE TABLE can't wedge
    // the reporter at startup.
    let mut heartbeat_active = upstream_config.heartbeat_enabled();
    if heartbeat_active {
        let setup_budget = heartbeat_setup_timeout();
        let setup = setup_heartbeat_table(&upstream_config, &reporter.instance_id);
        let result = tokio::time::timeout(setup_budget, setup).await;
        match result {
            Ok(Ok(())) => debug!("Heartbeat table created/verified"),
            Ok(Err(e)) => {
                warn!(
                    error = %e,
                    "Failed to create heartbeat table — disabling heartbeat. \
                     Byte/transaction lag will still be reported."
                );
                heartbeat_active = false;
            }
            Err(_) => {
                warn!(
                    budget_ms = setup_budget.as_millis() as u64,
                    "Heartbeat table setup timed out — disabling heartbeat. \
                     Byte/transaction lag will still be reported."
                );
                heartbeat_active = false;
            }
        }
    }

    let mut interval = tokio::time::interval(reporter.poll_interval);
    // Prevent burst reconnection after a network hiccup: if the upstream was unreachable
    // for N*10s, Skip avoids N rapid back-to-back polls.
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // The entire poll (connect, query, disconnect, optional heartbeat write) must
    // complete within this budget. If the upstream is slow or unreachable, the poll
    // times out cleanly instead of hanging until TCP keepalive kills the connection.
    let poll_budget = reporter.poll_interval * 3 / 4;
    // Track how many heartbeat polls have completed. We need at least 2 polls
    // before staleness is meaningful: the first writes the heartbeat, the second
    // reads the replicated value. Without this, a restart shows stale data from
    // the previous session's heartbeat row.
    let mut heartbeat_polls: u32 = 0;

    loop {
        tokio::select! {
            _ = interval.tick() => {
                let report_staleness = heartbeat_active && heartbeat_polls >= 2;
                let hb_ctx = HeartbeatContext {
                    active: heartbeat_active,
                    report_staleness,
                    reporter: &reporter,
                };
                match tokio::time::timeout(poll_budget, poll_lag(
                    &upstream_config,
                    &lag_status,
                    &progress,
                    &hb_ctx,
                )).await {
                    Ok(Ok(())) => {
                        if heartbeat_active {
                            heartbeat_polls = heartbeat_polls.saturating_add(1);
                        }
                    }
                    Ok(Err(e)) => {
                        warn!(error = %e, "Failed to poll replication lag");
                        counter!(recorded::REPLICATOR_LAG_POLL_FAILURE).increment(1);
                    }
                    Err(_) => {
                        warn!(
                            budget_ms = poll_budget.as_millis() as u64,
                            "Replication lag poll timed out"
                        );
                        counter!(recorded::REPLICATOR_LAG_POLL_FAILURE).increment(1);
                    }
                }
            }
            _ = cancel.cancelled() => {
                debug!("Replication lag reporter shutting down");
                break;
            }
        }
    }
}

async fn setup_heartbeat_table(
    config: &UpstreamLagConfig,
    instance_id: &str,
) -> ReadySetResult<()> {
    match config {
        UpstreamLagConfig::Postgres { config, tls, .. } => {
            let (client, connection) = config.connect(tls.clone()).await.map_err(|e| {
                readyset_errors::internal_err!("Failed to connect for heartbeat setup: {e}")
            })?;
            let connection_handle = tokio::spawn(connection);

            client
                .batch_execute(&format!(
                    "CREATE SCHEMA IF NOT EXISTS {PG_HEARTBEAT_SCHEMA}; \
                     CREATE TABLE IF NOT EXISTS {PG_HEARTBEAT_TABLE} (\
                         id TEXT PRIMARY KEY, \
                         ts TIMESTAMPTZ NOT NULL DEFAULT now()\
                     ); \
                     DELETE FROM {PG_HEARTBEAT_TABLE} \
                         WHERE ts < now() - INTERVAL '7 days'"
                ))
                .await
                .map_err(|e| {
                    readyset_errors::internal_err!("Failed to create heartbeat table: {e}")
                })?;

            debug!(instance_id, "Heartbeat table ready (stale rows cleaned)");
            drop(client);
            connection_handle.abort();
            Ok(())
        }
        UpstreamLagConfig::MysqlFile { opts, .. } | UpstreamLagConfig::MysqlGtid { opts, .. } => {
            let mut conn = mysql_connect_bounded(opts).await.map_err(|e| {
                readyset_errors::internal_err!("Failed to connect for heartbeat setup: {e}")
            })?;

            conn.query_drop(format!(
                "CREATE TABLE IF NOT EXISTS {MYSQL_HEARTBEAT_TABLE} (\
                     id VARCHAR(36) PRIMARY KEY, \
                     ts TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)\
                 )"
            ))
            .await
            .map_err(|e| readyset_errors::internal_err!("Failed to create heartbeat table: {e}"))?;

            conn.query_drop(format!(
                "DELETE FROM {MYSQL_HEARTBEAT_TABLE} \
                 WHERE ts < NOW(6) - INTERVAL 7 DAY"
            ))
            .await
            .map_err(|e| {
                readyset_errors::internal_err!("Failed to clean stale heartbeat rows: {e}")
            })?;

            debug!(instance_id, "Heartbeat table ready (stale rows cleaned)");
            conn.disconnect().await.map_err(|e| {
                readyset_errors::internal_err!("Failed to disconnect after heartbeat setup: {e}")
            })?;
            Ok(())
        }
    }
}

async fn poll_lag(
    upstream_config: &UpstreamLagConfig,
    lag_status: &SharedLagStatus,
    progress: &SharedReplicatorProgress,
    hb: &HeartbeatContext<'_>,
) -> ReadySetResult<()> {
    let ReplicatorProgressSnapshot {
        stream_position: replicator_offset,
        persisted_offset,
        heartbeat_ts,
    } = progress.snapshot();

    let Some(replicator_offset) = replicator_offset else {
        debug!("Replicator position not yet available, skipping lag poll");
        return Ok(());
    };

    let persisted_offset_for_lag = persisted_offset.unwrap_or_else(|| replicator_offset.clone());

    let mut status = match upstream_config {
        UpstreamLagConfig::Postgres { config, tls, .. } => {
            poll_postgres_lag(
                config,
                tls,
                &replicator_offset,
                &persisted_offset_for_lag,
                hb.active,
                hb.reporter.instance_id.as_str(),
            )
            .await?
        }
        UpstreamLagConfig::MysqlFile { opts, .. } => {
            poll_mysql_file_lag(
                opts,
                &replicator_offset,
                &persisted_offset_for_lag,
                hb.active,
                hb.reporter.instance_id.as_str(),
            )
            .await?
        }
        UpstreamLagConfig::MysqlGtid { opts, .. } => {
            poll_mysql_gtid_lag(
                opts,
                &replicator_offset,
                &persisted_offset_for_lag,
                hb.active,
                hb.reporter.instance_id.as_str(),
            )
            .await?
        }
    };

    // Compute staleness from the snapshotted heartbeat timestamp.
    // Subtract the poll interval since the heartbeat was written one poll cycle ago
    // and replicated nearly instantly — the interval itself is measurement overhead,
    // not real lag. Floor at zero to handle timing jitter.
    // Only report after 2+ polls so we don't show stale data from a previous session.
    if hb.report_staleness {
        if let Some(ts) = heartbeat_ts {
            match ts.elapsed() {
                Ok(elapsed) => {
                    let adjusted = elapsed.saturating_sub(hb.reporter.poll_interval);
                    // Round to millisecond precision (3 decimal places)
                    status.staleness_seconds =
                        Some((adjusted.as_secs_f64() * 1000.0).round() / 1000.0);
                }
                Err(_) => {
                    warn!("Heartbeat timestamp is in the future — possible clock skew between upstream and ReadySet");
                }
            }
        }
    }

    // Update Prometheus gauges
    gauge!(
        recorded::REPLICATOR_REPLICATION_LAG,
        "mode" => status.mode.clone(),
        "kind" => "consume",
    )
    .set(status.consume_lag as f64);

    gauge!(
        recorded::REPLICATOR_REPLICATION_LAG,
        "mode" => status.mode.clone(),
        "kind" => "persist",
    )
    .set(status.persist_lag as f64);

    if let Some(staleness) = status.staleness_seconds {
        gauge!(recorded::REPLICATOR_REPLICATION_STALENESS).set(staleness);
    }

    debug!(
        mode = %status.mode,
        consume_lag = status.consume_lag,
        persist_lag = status.persist_lag,
        staleness_seconds = ?status.staleness_seconds,
        "Replication lag updated"
    );

    // Update shared state for vrel
    match lag_status.write() {
        Ok(mut guard) => *guard = Some(status),
        Err(e) => warn!(error = %e, "Failed to update lag status (lock poisoned)"),
    }

    Ok(())
}

async fn poll_postgres_lag(
    config: &tokio_postgres::Config,
    tls: &postgres_native_tls::MakeTlsConnector,
    replicator_offset: &ReplicationOffset,
    persisted_offset: &ReplicationOffset,
    heartbeat_active: bool,
    instance_id: &str,
) -> ReadySetResult<ReplicationLagStatus> {
    // Connect-query-disconnect
    let (client, connection) = config
        .connect(tls.clone())
        .await
        .map_err(|e| internal_err!("Failed to connect for lag query: {e}"))?;
    let connection_handle = tokio::spawn(connection);

    let row = client
        .query_one("SELECT pg_current_wal_flush_lsn()::text", &[])
        .await
        .map_err(|e| internal_err!("Failed to query pg_current_wal_flush_lsn(): {e}"))?;

    let lsn_str: String = row
        .try_get(0)
        .map_err(|e| internal_err!("Failed to get LSN from pg_current_wal_flush_lsn(): {e}"))?;

    // Write heartbeat if enabled
    if heartbeat_active {
        write_postgres_heartbeat(&client, instance_id).await;
    }

    drop(client);
    // Abort the connection driver task. We don't send a Terminate message since this is a
    // short-lived polling connection and the Postgres server handles the dropped TCP connection.
    connection_handle.abort();

    let upstream_lsn: Lsn = lsn_str.parse()?;
    // upstream_pos is only used for lag calculation (which uses the `lsn` field).
    // commit_lsn is set equal to lsn since there's no commit context for a standalone LSN query.
    let upstream_pos = PostgresPosition {
        commit_lsn: upstream_lsn.as_i64().into(),
        lsn: upstream_lsn,
    };

    let replicator_pos = PostgresPosition::try_from(replicator_offset.clone())?;
    let persisted_pos = PostgresPosition::try_from(persisted_offset.clone())?;

    Ok(ReplicationLagStatus {
        mode: <&str>::from(replicator_offset).to_string(),
        upstream_offset: upstream_pos.to_string(),
        replicator_offset: replicator_pos.to_string(),
        persisted_offset: persisted_pos.to_string(),
        consume_lag: replicator_pos.byte_lag_behind(&upstream_pos),
        persist_lag: persisted_pos.byte_lag_behind(&upstream_pos),
        staleness_seconds: None, // Filled in by poll_lag from shared heartbeat timestamp
    })
}

/// Writes a heartbeat timestamp to the Postgres upstream.
async fn write_postgres_heartbeat(client: &tokio_postgres::Client, instance_id: &str) {
    if let Err(e) = client
        .execute(
            &format!(
                "INSERT INTO {PG_HEARTBEAT_TABLE} (id, ts) VALUES ('{instance_id}', now()) \
                 ON CONFLICT (id) DO UPDATE SET ts = EXCLUDED.ts"
            ),
            &[],
        )
        .await
    {
        warn!(error = %e, "Failed to write heartbeat");
    }
}

/// Writes a heartbeat timestamp to the MySQL upstream.
async fn write_mysql_heartbeat(conn: &mut mysql_async::Conn, instance_id: &str) {
    if let Err(e) = conn
        .query_drop(format!(
            "INSERT INTO {MYSQL_HEARTBEAT_TABLE} (id, ts) VALUES ('{instance_id}', NOW(6)) \
             ON DUPLICATE KEY UPDATE ts = NOW(6)"
        ))
        .await
    {
        warn!(error = %e, "Failed to write heartbeat");
    }
}

/// Queries the upstream MySQL status and returns the result row.
///
/// Uses `SHOW BINARY LOG STATUS` on MySQL >= 8.4, `SHOW MASTER STATUS` on older versions.
async fn query_mysql_upstream_status(
    conn: &mut mysql_async::Conn,
) -> ReadySetResult<mysql_async::Row> {
    let version = get_mysql_version(conn)
        .await
        .map_err(|e| internal_err!("Failed to get MySQL version: {e}"))?;
    let status_query = if version >= 80400 {
        "SHOW BINARY LOG STATUS"
    } else {
        "SHOW MASTER STATUS"
    };

    conn.query_first(status_query)
        .await
        .map_err(|e| internal_err!("Failed to query {status_query}: {e}"))?
        .ok_or_else(|| internal_err!("Empty response for {status_query}"))
}

async fn poll_mysql_file_lag(
    opts: &mysql_async::Opts,
    replicator_offset: &ReplicationOffset,
    persisted_offset: &ReplicationOffset,
    heartbeat_active: bool,
    instance_id: &str,
) -> ReadySetResult<ReplicationLagStatus> {
    // Connect-query-disconnect
    let mut conn = mysql_connect_bounded(opts)
        .await
        .map_err(|e| internal_err!("Failed to connect for lag query: {e}"))?;

    let row = query_mysql_upstream_status(&mut conn).await?;

    let file: String = row
        .get(0)
        .ok_or_else(|| internal_err!("Missing binlog file name in upstream status response"))?;
    let position: u64 = row
        .get(1)
        .ok_or_else(|| internal_err!("Missing binlog position in upstream status response"))?;
    let upstream_pos = MySqlPosition::from_file_name_and_position(file, position)?;

    // Query binlog file sizes for cross-file lag calculation
    let binlog_file_sizes = query_binlog_file_sizes(&mut conn).await?;

    if heartbeat_active {
        write_mysql_heartbeat(&mut conn, instance_id).await;
    }

    conn.disconnect()
        .await
        .map_err(|e| internal_err!("Failed to disconnect: {e}"))?;

    let replicator_pos = replicator_offset.mysql_position()?;
    let persisted_pos = persisted_offset.mysql_position()?;

    let consume_lag = replicator_pos.try_byte_lag_behind(&upstream_pos, &binlog_file_sizes)?;
    let persist_lag = persisted_pos.try_byte_lag_behind(&upstream_pos, &binlog_file_sizes)?;

    Ok(ReplicationLagStatus {
        mode: <&str>::from(replicator_offset).to_string(),
        upstream_offset: upstream_pos.to_string(),
        replicator_offset: replicator_pos.to_string(),
        persisted_offset: persisted_pos.to_string(),
        consume_lag,
        persist_lag,
        staleness_seconds: None, // Filled in by poll_lag from shared heartbeat timestamp
    })
}

async fn poll_mysql_gtid_lag(
    opts: &mysql_async::Opts,
    replicator_offset: &ReplicationOffset,
    persisted_offset: &ReplicationOffset,
    heartbeat_active: bool,
    instance_id: &str,
) -> ReadySetResult<ReplicationLagStatus> {
    // Connect-query-disconnect
    let mut conn = mysql_connect_bounded(opts)
        .await
        .map_err(|e| internal_err!("Failed to connect for lag query: {e}"))?;

    let row = query_mysql_upstream_status(&mut conn).await?;

    let gtid_set_str: String = row
        .get(4)
        .ok_or_else(|| internal_err!("Executed_Gtid_Set missing"))?;

    if heartbeat_active {
        write_mysql_heartbeat(&mut conn, instance_id).await;
    }

    conn.disconnect()
        .await
        .map_err(|e| internal_err!("Failed to disconnect: {e}"))?;

    let upstream_gtid = GtidSet::parse(gtid_set_str.trim())?;

    let replicator_gtid = if let ReplicationOffset::Gtid(g) = replicator_offset {
        g.clone()
    } else {
        return Err(internal_err!("Expected GTID offset for replicator"));
    };

    let persisted_gtid = if let ReplicationOffset::Gtid(g) = persisted_offset {
        g.clone()
    } else {
        return Err(internal_err!("Expected GTID offset for persisted position"));
    };

    Ok(ReplicationLagStatus {
        mode: <&str>::from(replicator_offset).to_string(),
        upstream_offset: upstream_gtid.to_mysql_string(),
        replicator_offset: replicator_gtid.to_mysql_string(),
        persisted_offset: persisted_gtid.to_mysql_string(),
        consume_lag: replicator_gtid.transaction_lag(&upstream_gtid),
        persist_lag: persisted_gtid.transaction_lag(&upstream_gtid),
        staleness_seconds: None, // Filled in by poll_lag from shared heartbeat timestamp
    })
}

/// Query `SHOW BINARY LOGS` and return a map of file suffix -> file size.
async fn query_binlog_file_sizes(
    conn: &mut mysql_async::Conn,
) -> ReadySetResult<HashMap<u32, u64>> {
    let rows: Vec<mysql_async::Row> = conn
        .query("SHOW BINARY LOGS")
        .await
        .map_err(|e| internal_err!("Failed to query SHOW BINARY LOGS: {e}"))?;

    let mut sizes = HashMap::new();
    for row in rows {
        let file_name: String = row
            .get(0)
            .ok_or_else(|| internal_err!("Missing Log_name in SHOW BINARY LOGS row"))?;
        let file_size: u64 = row
            .get(1)
            .ok_or_else(|| internal_err!("Missing File_size in SHOW BINARY LOGS row"))?;
        let pos = MySqlPosition::from_file_name_and_position(file_name, 0)?;
        sizes.insert(pos.binlog_file_suffix, file_size);
    }

    Ok(sizes)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use postgres_native_tls::MakeTlsConnector;

    use super::*;

    fn pg_config() -> UpstreamLagConfig {
        let connector = native_tls::TlsConnector::builder().build().unwrap();
        UpstreamLagConfig::Postgres {
            config: Box::new(tokio_postgres::Config::new()),
            tls: MakeTlsConnector::new(connector),
            heartbeat: false,
        }
    }

    fn mysql_file_config() -> UpstreamLagConfig {
        UpstreamLagConfig::MysqlFile {
            opts: mysql_async::OptsBuilder::default().into(),
            heartbeat: false,
        }
    }

    fn mysql_gtid_config() -> UpstreamLagConfig {
        UpstreamLagConfig::MysqlGtid {
            opts: mysql_async::OptsBuilder::default().into(),
            heartbeat: false,
        }
    }

    #[test]
    fn pg_connect_timeout_applied() {
        let configured = pg_config().with_pg_connect_timeout();
        let UpstreamLagConfig::Postgres { config, .. } = configured else {
            panic!("expected Postgres variant");
        };
        assert_eq!(
            config.get_connect_timeout().copied(),
            Some(DEFAULT_LAG_CONNECT_TIMEOUT)
        );
    }

    #[test]
    fn mysql_variants_pass_through_pg_connect_timeout() {
        // MySQL connect-timeout is enforced at the call site via `mysql_connect_bounded`,
        // not on the Opts. `with_pg_connect_timeout` must leave the MySQL variants
        // untouched; this test guards against a future refactor that expects it to
        // apply a connect budget uniformly across all variants.
        assert!(matches!(
            mysql_file_config().with_pg_connect_timeout(),
            UpstreamLagConfig::MysqlFile { .. }
        ));
        assert!(matches!(
            mysql_gtid_config().with_pg_connect_timeout(),
            UpstreamLagConfig::MysqlGtid { .. }
        ));
    }

    // Env-var mutation tests share a process-wide lock so the two don't race with
    // each other (cargo test runs unit tests in parallel by default).
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn env_override_changes_connect_timeout() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        // SAFETY: ENV_LOCK serializes env-mutating tests in this module; no other
        // tests here touch LAG_CONNECT_TIMEOUT_ENV.
        unsafe { std::env::set_var(LAG_CONNECT_TIMEOUT_ENV, "250") };
        assert_eq!(lag_connect_timeout(), Duration::from_millis(250));
        assert_eq!(heartbeat_setup_timeout(), Duration::from_millis(250) * 3);
        // SAFETY: same as above — ENV_LOCK guarantees exclusive access.
        unsafe { std::env::remove_var(LAG_CONNECT_TIMEOUT_ENV) };
        assert_eq!(lag_connect_timeout(), DEFAULT_LAG_CONNECT_TIMEOUT);
    }

    #[test]
    fn env_override_invalid_falls_back_to_default() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        // SAFETY: ENV_LOCK serializes env-mutating tests in this module.
        unsafe { std::env::set_var(LAG_CONNECT_TIMEOUT_ENV, "not a number") };
        assert_eq!(lag_connect_timeout(), DEFAULT_LAG_CONNECT_TIMEOUT);
        // SAFETY: same as above.
        unsafe { std::env::remove_var(LAG_CONNECT_TIMEOUT_ENV) };
    }
}
