//! Background task that periodically queries the upstream database's current replication
//! position and compares it to ReadySet's position, computing replication lag metrics.
//!
//! The task uses connect-query-disconnect on each poll to avoid holding an idle connection
//! to the upstream database.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use metrics::{counter, gauge};
use readyset_client::metrics::recorded;
use readyset_client::status::ReplicationLagStatus;
use readyset_client::ReadySetHandle;
use readyset_errors::{internal_err, ReadySetResult};
use replication_offset::mysql::MySqlPosition;
use replication_offset::mysql_gtid::GtidSet;
use replication_offset::postgres::{Lsn, PostgresPosition};
use replication_offset::ReplicationOffset;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::mysql_connector::get_mysql_version;

/// Shared state for replication lag, readable by vrels and other consumers.
pub type SharedLagStatus = Arc<RwLock<Option<ReplicationLagStatus>>>;

/// Shared state for the replicator's current stream position.
/// Written by the main replication loop, read by the lag reporter.
pub type SharedReplicatorPosition = Arc<RwLock<Option<ReplicationOffset>>>;

/// Configuration for the upstream connection used by the lag reporter.
pub enum UpstreamLagConfig {
    Postgres {
        config: Box<tokio_postgres::Config>,
        tls: postgres_native_tls::MakeTlsConnector,
    },
    MysqlFile {
        opts: mysql_async::Opts,
    },
    MysqlGtid {
        opts: mysql_async::Opts,
    },
}

/// Spawns the replication lag reporter as a background task.
///
/// `lag_status` is the shared state that the controller reads via RPC.
/// Returns a tuple of (the same lag_status, shared replicator position for main_loop to write).
pub fn spawn_lag_reporter(
    upstream_config: UpstreamLagConfig,
    noria: ReadySetHandle,
    cancel: CancellationToken,
    lag_status: SharedLagStatus,
    poll_interval: Duration,
) -> (SharedLagStatus, SharedReplicatorPosition) {
    let replicator_position: SharedReplicatorPosition = Arc::new(RwLock::new(None));

    let lag_status_clone = Arc::clone(&lag_status);
    let replicator_pos_clone = Arc::clone(&replicator_position);

    tokio::spawn(async move {
        lag_reporter_loop(
            upstream_config,
            noria,
            lag_status_clone,
            replicator_pos_clone,
            cancel,
            poll_interval,
        )
        .await;
    });

    (lag_status, replicator_position)
}

async fn lag_reporter_loop(
    upstream_config: UpstreamLagConfig,
    mut noria: ReadySetHandle,
    lag_status: SharedLagStatus,
    replicator_position: SharedReplicatorPosition,
    cancel: CancellationToken,
    poll_interval: Duration,
) {
    let mut interval = tokio::time::interval(poll_interval);
    // Prevent burst reconnection after a network hiccup: if the upstream was unreachable
    // for N*10s, Skip avoids N rapid back-to-back polls.
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // The entire poll (connect, query, disconnect, optional heartbeat write) must
    // complete within this budget. If the upstream is slow or unreachable, the poll
    // times out cleanly instead of hanging until TCP keepalive kills the connection.
    let poll_budget = poll_interval * 3 / 4;

    loop {
        tokio::select! {
            _ = interval.tick() => {
                match tokio::time::timeout(poll_budget, poll_lag(
                    &upstream_config,
                    &mut noria,
                    &lag_status,
                    &replicator_position,
                )).await {
                    Ok(Ok(())) => {}
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

async fn poll_lag(
    upstream_config: &UpstreamLagConfig,
    noria: &mut ReadySetHandle,
    lag_status: &SharedLagStatus,
    replicator_position: &SharedReplicatorPosition,
) -> ReadySetResult<()> {
    let replicator_offset = {
        replicator_position
            .read()
            .map_err(|e| internal_err!("Failed to read replicator position: {e}"))?
            .clone()
    };

    let Some(replicator_offset) = replicator_offset else {
        debug!("Replicator position not yet available, skipping lag poll");
        return Ok(());
    };

    let offsets = noria.replication_offsets().await?;
    let persisted_offset_for_lag = match offsets.max_present_offset()? {
        Some(offset) => offset.clone(),
        // No tables have persisted yet — use replicator position as fallback.
        None => replicator_offset.clone(),
    };

    let status = match upstream_config {
        UpstreamLagConfig::Postgres { config, tls } => {
            poll_postgres_lag(config, tls, &replicator_offset, &persisted_offset_for_lag).await?
        }
        UpstreamLagConfig::MysqlFile { opts } => {
            poll_mysql_file_lag(opts, &replicator_offset, &persisted_offset_for_lag).await?
        }
        UpstreamLagConfig::MysqlGtid { opts } => {
            poll_mysql_gtid_lag(opts, &replicator_offset, &persisted_offset_for_lag).await?
        }
    };

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

    debug!(
        mode = %status.mode,
        consume_lag = status.consume_lag,
        persist_lag = status.persist_lag,
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
    })
}

/// Queries the upstream MySQL status and returns the result row.
///
/// Uses `SHOW BINARY LOG STATUS` on MySQL >= 8.4, `SHOW MASTER STATUS` on older versions.
async fn query_mysql_upstream_status(
    conn: &mut mysql_async::Conn,
) -> ReadySetResult<mysql_async::Row> {
    use mysql_async::prelude::*;

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
) -> ReadySetResult<ReplicationLagStatus> {
    // Connect-query-disconnect
    let mut conn = mysql_async::Conn::new(opts.clone())
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
    })
}

async fn poll_mysql_gtid_lag(
    opts: &mysql_async::Opts,
    replicator_offset: &ReplicationOffset,
    persisted_offset: &ReplicationOffset,
) -> ReadySetResult<ReplicationLagStatus> {
    // Connect-query-disconnect
    let mut conn = mysql_async::Conn::new(opts.clone())
        .await
        .map_err(|e| internal_err!("Failed to connect for lag query: {e}"))?;

    let row = query_mysql_upstream_status(&mut conn).await?;

    let gtid_set_str: String = row
        .get(4)
        .ok_or_else(|| internal_err!("Executed_Gtid_Set missing"))?;

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
    })
}

/// Query `SHOW BINARY LOGS` and return a map of file suffix -> file size.
async fn query_binlog_file_sizes(
    conn: &mut mysql_async::Conn,
) -> ReadySetResult<HashMap<u32, u64>> {
    use mysql_async::prelude::*;

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
