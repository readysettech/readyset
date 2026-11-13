//! System status information that can sent to a noria-client.
//!
//! When introducing new fields to the [`ReadySetControllerStatus`] type, be sure to
//! update support for converting the object to strings:
//!   * `ReadySetControllerStatus::try_from(_: Vec<(String, String)>)`
//!   * `Vec<(String, String)>::from(_: ReadySetControllerStatus)`
//!
//! These two conversions are used to convert the [`ReadySetControllerStatus`] structs to a format
//! that can be passed to various SQL clients.
use std::{
    borrow::Cow,
    fmt::{self, Display},
};

use serde::{Deserialize, Serialize};

use readyset_sql::ast::{CacheType, TrxCachePolicy};
use replication_offset::ReplicationOffset;

// Consts for variable names.

const STATUS_VARIABLE: &str = "Status";
const REPLICATION_STATUS_VARIABLE: &str = "Replication Status";
const CDC_SERVER_VARIABLE: &str = "CDC Server";
const MAX_REPLICATION_OFFSET: &str = "Maximum Replication Offset";
const MIN_REPLICATION_OFFSET: &str = "Minimum Replication Offset";

/// ReadySetControllerStatus holds information regarding the controller status of ReadySet, similar
/// to
///
/// [`SHOW STATUS`](https://dev.mysql.com/doc/refman/8.0/en/show-status.html) in MySQL.
///
/// Returned via the /status RPC and SHOW READYSET STATUS.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct ReadySetControllerStatus {
    /// The status of the current leader.
    pub current_status: CurrentStatus,
    /// The status of the replicator.
    pub replication_status: ReplicationStatus,
    /// The `host:port` of the CDC server replication is connected to, or `None` while
    /// replication is not running.
    pub cdc_server: Option<String>,
    /// The current maximum replication offset known by the leader.
    pub max_replication_offset: Option<ReplicationOffset>,
    /// The current minimum replication offset known by the leader.
    pub min_replication_offset: Option<ReplicationOffset>,
    /// The status of the replication channel of each catalog.
    #[serde(default)]
    pub channels: Vec<ReplicationChannelStatus>,
}

impl ReadySetControllerStatus {
    /// Build the status of the leader from the status of each replication channel. The
    /// replication status is the worst over the channels, the CDC server is the `None` catalog's,
    /// and the offsets are the minimum and maximum over the channels.
    pub fn new(current_status: CurrentStatus, channels: Vec<ReplicationChannelStatus>) -> Self {
        let replication_status = channels
            .iter()
            .map(|channel| channel.replication_status.clone())
            .max()
            .unwrap_or(ReplicationStatus::Disabled);
        let cdc_server = channels
            .iter()
            .find(|channel| channel.catalog.is_none())
            .and_then(|channel| channel.cdc_server.clone());
        let max_replication_offset = ReplicationOffset::try_max_all(
            channels.iter().map(|c| c.max_replication_offset.as_ref()),
        );
        let min_replication_offset = ReplicationOffset::try_min_all(
            channels
                .iter()
                .filter_map(|c| c.min_replication_offset.as_ref()),
        );
        Self {
            current_status,
            replication_status,
            cdc_server,
            max_replication_offset: max_replication_offset.ok().flatten().cloned(),
            min_replication_offset: min_replication_offset.ok().flatten().cloned(),
            channels,
        }
    }
}

/// The status of the replication channel of one catalog.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct ReplicationChannelStatus {
    /// The catalog this channel replicates.
    pub catalog: Option<String>,
    /// The status of the channel's replicator.
    pub replication_status: ReplicationStatus,
    /// The `host:port` of the CDC server the channel is connected to, or `None` while the
    /// channel is not running.
    pub cdc_server: Option<String>,
    /// The maximum replication offset of the channel's tables, if every table has one.
    pub max_replication_offset: Option<ReplicationOffset>,
    /// The minimum replication offset present in the channel's tables.
    pub min_replication_offset: Option<ReplicationOffset>,
}

impl From<ReadySetControllerStatus> for Vec<(String, String)> {
    fn from(status: ReadySetControllerStatus) -> Vec<(String, String)> {
        let mut res = vec![
            (
                STATUS_VARIABLE.to_string(),
                status.current_status.to_string(),
            ),
            (
                REPLICATION_STATUS_VARIABLE.to_string(),
                status.replication_status.to_string(),
            ),
        ];

        if let Some(cdc_server) = status.cdc_server {
            res.push((CDC_SERVER_VARIABLE.to_string(), cdc_server))
        }

        if let Some(replication_offset) = status.max_replication_offset {
            res.push((
                MAX_REPLICATION_OFFSET.to_string(),
                replication_offset.to_string(),
            ))
        }

        if let Some(replication_offset) = status.min_replication_offset {
            res.push((
                MIN_REPLICATION_OFFSET.to_string(),
                replication_offset.to_string(),
            ))
        }

        // The aggregate rows describe a single channel completely.
        if status.channels.len() > 1 {
            for channel in status.channels {
                let suffix = channel_suffix(channel.catalog.as_deref());
                res.push((
                    format!("{REPLICATION_STATUS_VARIABLE}{suffix}"),
                    channel.replication_status.to_string(),
                ));
                if let Some(cdc_server) = channel.cdc_server {
                    res.push((format!("{CDC_SERVER_VARIABLE}{suffix}"), cdc_server));
                }
                if let Some(offset) = channel.max_replication_offset {
                    res.push((
                        format!("{MAX_REPLICATION_OFFSET}{suffix}"),
                        offset.to_string(),
                    ));
                }
                if let Some(offset) = channel.min_replication_offset {
                    res.push((
                        format!("{MIN_REPLICATION_OFFSET}{suffix}"),
                        offset.to_string(),
                    ));
                }
            }
        }

        res
    }
}

/// Return the replication channel of `catalog`: `''` for `None`, otherwise its name.
pub fn replication_channel(catalog: Option<&str>) -> &str {
    catalog.unwrap_or("")
}

/// Return the suffix of the status variables of the replication channel of `catalog`.
pub fn channel_suffix(catalog: Option<&str>) -> String {
    format!(" (channel '{}')", replication_channel(catalog))
}

/// Whether or not snapshotting has completed.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub enum CurrentStatus {
    /// Snapshotting has not yet completed.
    SnapshotInProgress,
    /// Node is online and ready to serve traffic.
    Online,
    /// Node is in maintenance mode.
    MaintenanceMode,
}

impl Display for CurrentStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            CurrentStatus::SnapshotInProgress => "Snapshot In Progress",
            CurrentStatus::Online => "Online",
            CurrentStatus::MaintenanceMode => "Maintenance Mode",
        };
        write!(f, "{s}")
    }
}

/// The status of the replicator. The variants are ordered from best to worst.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub enum ReplicationStatus {
    /// Replication is active.
    Running,
    /// Replication has been explicitly stopped via ALTER READYSET STOP REPLICATION.
    Stopped,
    /// Replicator was not started (e.g., shallow caching mode).
    Disabled,
}

impl Display for ReplicationStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            ReplicationStatus::Running => "Running",
            ReplicationStatus::Stopped => "Stopped",
            ReplicationStatus::Disabled => "Disabled",
        };
        write!(f, "{s}")
    }
}

/// Snapshot of replication lag, computed by the background polling task.
///
/// For Postgres and MySQL file-based modes, lag values are in bytes.
/// For MySQL GTID mode, lag values are in transactions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationLagStatus {
    /// The replication mode name (e.g. "postgres", "mysql_file", "mysql_gtid").
    /// Derived from the `ReplicationOffset` variant via strum.
    pub mode: String,
    /// The upstream's current position, formatted for display.
    pub upstream_offset: String,
    /// The last event consumed from the replication stream, formatted for display.
    pub replicator_offset: String,
    /// The maximum persisted offset across all RocksDB tables, formatted for display.
    /// This is the persistence frontier — the furthest any table has durably stored.
    pub persisted_offset: String,
    /// Lag between upstream and replicator stream position (bytes or transactions).
    pub consume_lag: u64,
    /// Lag between upstream and max persisted offset (bytes or transactions).
    pub persist_lag: u64,
    /// Time-based staleness in fractional seconds, measured via pt-heartbeat.
    /// `None` when heartbeat is disabled.
    pub staleness_seconds: Option<f64>,
}

#[derive(Debug)]
pub struct CacheProperties {
    cache_type: CacheType,
    ttl_ms: Option<u64>,
    refresh_ms: Option<u64>,
    coalesce_ms: Option<u64>,
    trx_cache_policy: TrxCachePolicy,
    schedule: bool,
    adaptive: bool,
    topk_buffer_multiplier: Option<usize>,
    keeps_literals_inline: Option<bool>,
}

impl Display for CacheProperties {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // List the most important properties first.
        let mut properties = vec![Cow::Owned(format!("{}", self.cache_type))];
        if let Some(label) = self.trx_cache_policy.show_caches_label() {
            properties.push(Cow::Borrowed(label));
        }
        if let Some(ttl_ms) = self.ttl_ms {
            properties.push(Cow::Owned(format!("ttl {ttl_ms} ms")));
        }
        if let Some(refresh_ms) = self.refresh_ms {
            properties.push(Cow::Owned(if self.schedule {
                format!("refresh every {refresh_ms} ms")
            } else {
                format!("refresh {refresh_ms} ms")
            }));
        }
        if let Some(coalesce_ms) = self.coalesce_ms {
            properties.push(Cow::Owned(format!("coalesce {coalesce_ms} ms")));
        }
        if self.adaptive {
            properties.push(Cow::Borrowed("adaptive"));
        }
        if let Some(m) = self.topk_buffer_multiplier {
            properties.push(Cow::Owned(format!("topk buffer ×{m}")));
        }
        match self.keeps_literals_inline {
            Some(true) => properties.push(Cow::Borrowed("autoparam off")),
            Some(false) => properties.push(Cow::Borrowed("autoparam on")),
            // Not a property a shallow cache has.
            None => {}
        }
        write!(f, "{}", properties.join(", "))
    }
}

impl CacheProperties {
    pub fn new(cache_type: CacheType) -> Self {
        Self {
            cache_type,
            ttl_ms: None,
            refresh_ms: None,
            coalesce_ms: None,
            trx_cache_policy: TrxCachePolicy::default(),
            schedule: false,
            adaptive: false,
            topk_buffer_multiplier: None,
            keeps_literals_inline: None,
        }
    }

    pub fn set_trx_cache_policy(&mut self, trx_cache_policy: TrxCachePolicy) {
        self.trx_cache_policy = trx_cache_policy;
    }

    pub fn set_schedule(&mut self, schedule: bool) {
        self.schedule = schedule;
    }

    pub fn set_adaptive(&mut self, adaptive: bool) {
        self.adaptive = adaptive;
    }

    pub fn set_ttl_ms(&mut self, ttl_ms: u64) {
        self.ttl_ms = Some(ttl_ms);
    }

    pub fn set_refresh_ms(&mut self, refresh_ms: u64) {
        self.refresh_ms = Some(refresh_ms);
    }

    pub fn set_coalesce_ms(&mut self, coalesce_ms: u64) {
        self.coalesce_ms = Some(coalesce_ms);
    }

    pub fn set_topk_buffer_multiplier(&mut self, multiplier: usize) {
        self.topk_buffer_multiplier = Some(multiplier);
    }

    /// Say whether this cache keeps some of its author's literals inline. One that does is
    /// reached by a read carrying those same literals rather than by producing the cache's form.
    pub fn set_keeps_literals_inline(&mut self, keeps: bool) {
        self.keeps_literals_inline = Some(keeps);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn offset(s: &str) -> Option<ReplicationOffset> {
        Some(s.parse().unwrap())
    }

    fn channel(
        catalog: Option<&str>,
        replication_status: ReplicationStatus,
        cdc_server: Option<&str>,
        min: Option<ReplicationOffset>,
        max: Option<ReplicationOffset>,
    ) -> ReplicationChannelStatus {
        ReplicationChannelStatus {
            catalog: catalog.map(String::from),
            replication_status,
            cdc_server: cdc_server.map(String::from),
            max_replication_offset: max,
            min_replication_offset: min,
        }
    }

    #[test]
    fn aggregates_channels() {
        let status = ReadySetControllerStatus::new(
            CurrentStatus::Online,
            vec![
                channel(
                    None,
                    ReplicationStatus::Running,
                    Some("url:5432"),
                    offset("0/20"),
                    offset("0/30"),
                ),
                channel(
                    Some("db2"),
                    ReplicationStatus::Stopped,
                    Some("other:5432"),
                    offset("0/10"),
                    offset("0/40"),
                ),
            ],
        );
        assert_eq!(status.replication_status, ReplicationStatus::Stopped);
        assert_eq!(status.cdc_server.as_deref(), Some("url:5432"));
        assert_eq!(status.min_replication_offset, offset("0/10"));
        assert_eq!(status.max_replication_offset, offset("0/40"));
        assert_eq!(status.channels.len(), 2);
    }

    #[test]
    fn max_offset_needs_every_channel() {
        let status = ReadySetControllerStatus::new(
            CurrentStatus::Online,
            vec![
                channel(
                    None,
                    ReplicationStatus::Running,
                    None,
                    offset("0/20"),
                    offset("0/30"),
                ),
                channel(Some("db2"), ReplicationStatus::Running, None, None, None),
            ],
        );
        assert_eq!(status.replication_status, ReplicationStatus::Running);
        assert_eq!(status.cdc_server, None);
        assert_eq!(status.min_replication_offset, offset("0/20"));
        assert_eq!(status.max_replication_offset, None);
    }

    #[test]
    fn single_channel_has_only_aggregate_rows() {
        let status = ReadySetControllerStatus::new(
            CurrentStatus::Online,
            vec![channel(
                None,
                ReplicationStatus::Running,
                Some("url:5432"),
                offset("0/20"),
                offset("0/30"),
            )],
        );
        let rows = Vec::<(String, String)>::from(status);
        assert!(rows.iter().all(|(name, _)| !name.contains("(channel")));
    }

    #[test]
    fn channel_rows() {
        let status = ReadySetControllerStatus::new(
            CurrentStatus::Online,
            vec![
                channel(
                    None,
                    ReplicationStatus::Running,
                    Some("url:5432"),
                    offset("0/20"),
                    offset("0/30"),
                ),
                channel(Some("db2"), ReplicationStatus::Stopped, None, None, None),
            ],
        );
        let rows = Vec::<(String, String)>::from(status);
        let channel_rows: Vec<_> = rows
            .iter()
            .filter(|(name, _)| name.contains("(channel"))
            .map(|(name, value)| (name.as_str(), value.as_str()))
            .collect();
        assert_eq!(
            channel_rows,
            vec![
                ("Replication Status (channel '')", "Running"),
                ("CDC Server (channel '')", "url:5432"),
                ("Maximum Replication Offset (channel '')", "(0/30, 0/30)"),
                ("Minimum Replication Offset (channel '')", "(0/20, 0/20)"),
                ("Replication Status (channel 'db2')", "Stopped"),
            ]
        );
    }

    #[test]
    fn incomparable_offsets() {
        let status = ReadySetControllerStatus::new(
            CurrentStatus::Online,
            vec![
                channel(
                    None,
                    ReplicationStatus::Running,
                    None,
                    offset("0/20"),
                    offset("0/30"),
                ),
                channel(
                    Some("db2"),
                    ReplicationStatus::Running,
                    None,
                    offset("mysql-bin.000003:154"),
                    offset("mysql-bin.000003:154"),
                ),
            ],
        );
        assert_eq!(status.min_replication_offset, None);
        assert_eq!(status.max_replication_offset, None);
    }
}
