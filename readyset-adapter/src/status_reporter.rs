use std::net::SocketAddr;
use std::sync::Arc;

use crossbeam_skiplist::SkipSet;
use database_utils::UpstreamConfig;
use readyset_client::consensus::{Authority, AuthorityControl};
use readyset_client::debug::stats::PersistentStats;
use readyset_client::status::ReadySetControllerStatus;
use readyset_client::ReadySetHandle;
use readyset_data::TimestampTz;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tracing::warn;

use crate::backend::noria_connector::{MetaVariable, QueryResult};
use crate::upstream_database::LazyUpstream;
use crate::UpstreamDatabase;

#[derive(Debug, Serialize, Deserialize)]
pub struct ReadySetStatus {
    pub controller_status: Option<ReadySetControllerStatus>,
    pub connected: Option<bool>,
    pub connection_count: usize,
    pub persistent_stats: Option<PersistentStats>,
}

impl ReadySetStatus {
    pub(crate) fn into_json(self) -> String {
        serde_json::to_string(&self).unwrap_or_else(|_| "{}".to_string())
    }
}

impl ReadySetStatus {
    pub(crate) fn into_query_result(self) -> QueryResult<'static> {
        let mut status = Vec::new();
        status.push((
            "Database Connection".to_string(),
            match self.connected {
                Some(true) => "Connected".to_string(),
                None | Some(false) => "Unreachable".to_string(),
            },
        ));

        status.push((
            "Connection Count".to_string(),
            self.connection_count.to_string(),
        ));

        if let Some(controller_status) = self.controller_status {
            status.append(&mut Vec::<(String, String)>::from(controller_status));
        } else {
            status.push((
                "ReadySet Controller Status".to_string(),
                "Unavailable".to_string(),
            ));
        }

        if let Some(stats) = self.persistent_stats {
            status.push((
                "Last started Controller".to_string(),
                time_or_null(stats.last_controller_startup),
            ));
            status.push((
                "Last completed snapshot".to_string(),
                time_or_null(stats.last_completed_snapshot),
            ));
            status.push((
                "Last started replication".to_string(),
                time_or_null(stats.last_started_replication),
            ));
            if let Some(err) = stats.last_replicator_error {
                status.push(("Last replicator error".to_string(), err))
            }
        }

        QueryResult::MetaVariables(status.into_iter().map(MetaVariable::from).collect())
    }
}

pub struct ReadySetStatusReporter<U>
where
    U: UpstreamDatabase,
{
    // We use an Arc-Mutex here because UpstreamDatabase::is_connected() needs mutable state and
    // the outer Backend/http server structs need this to be Clone. There is no chance for
    // deadlocks because a call to the sql extension ShowReadySetStatus does not call the http
    // version, the http one does not call the SQL one, and neither call themselves
    // recursively.
    inner: Arc<Mutex<ReadySetStatusReporterInner<U>>>,
}

// Auto impl isn't doing the right thing here for some reason with the Arc Clone
impl<U> Clone for ReadySetStatusReporter<U>
where
    U: UpstreamDatabase,
{
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<U> ReadySetStatusReporter<U>
where
    U: UpstreamDatabase,
{
    pub fn new(
        upstream_config: UpstreamConfig,
        rs_handle: Option<ReadySetHandle>,
        connections: Arc<SkipSet<SocketAddr>>,
        authority: Arc<Authority>,
    ) -> Self {
        let inner = Arc::new(Mutex::new(ReadySetStatusReporterInner {
            upstream: upstream_config.into(),
            rs_handle,
            connections,
            authority,
        }));
        Self { inner }
    }

    /// Takes a write lock of [`ReadySetStatusReporterInner`] and generates a [`ReadySetStatus`]
    pub async fn report_status(&self) -> ReadySetStatus {
        let mut inner = self.inner.lock().await;
        inner.report_status().await
    }
}

/// [`ReadySetStatusReporterInner`] is responsible for aggregating status-related information from
/// various sources and generating a [`ReadySetStatus`].
struct ReadySetStatusReporterInner<U> {
    pub(crate) upstream: LazyUpstream<U>,
    /// A handle to the ReadySet controller, for making controller rpc calls to obtain
    /// a [`ReadySetControllerStatus`]
    pub(crate) rs_handle: Option<ReadySetHandle>,
    /// A set of current connections, shared with backends--used to report connection count
    pub(crate) connections: Arc<SkipSet<SocketAddr>>,
    /// A shared handle to the Authority, used for reading persistent_stats for /readyset_status
    pub(crate) authority: Arc<Authority>,
}

impl<U> ReadySetStatusReporterInner<U>
where
    U: UpstreamDatabase,
{
    async fn report_status(&mut self) -> ReadySetStatus {
        let controller_status = match self.rs_handle {
            None => None,
            Some(ref mut handle) => handle.status().await.ok(),
        };
        let persistent_stats = self.authority.persistent_stats().await.unwrap_or_default();

        ReadySetStatus {
            controller_status,
            connected: self.upstream_connected().await,
            connection_count: self.connections.len(),
            persistent_stats,
        }
    }

    async fn upstream_connected(&mut self) -> Option<bool> {
        if let Ok(is_connected) = self.upstream.is_connected().await {
            Some(is_connected)
        } else {
            warn!("Unable to determine Upstream connection status");
            None
        }
    }
}

// Helper function for formatting
fn time_or_null(time_ms: Option<u64>) -> String {
    if let Some(t) = time_ms {
        TimestampTz::from_unix_ms(t).to_string()
    } else {
        "NULL".to_string()
    }
}
