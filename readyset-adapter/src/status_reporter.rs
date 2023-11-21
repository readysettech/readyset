use std::net::SocketAddr;
use std::sync::Arc;

use crossbeam_skiplist::SkipSet;
use readyset_client::consensus::{Authority, AuthorityControl};
use readyset_client::debug::stats::PersistentStats;
use readyset_client::status::ReadySetControllerStatus;
use readyset_data::TimestampTz;

use crate::backend::noria_connector::{MetaVariable, QueryResult};
use crate::backend::NoriaConnector;
use crate::UpstreamDatabase;

#[derive(Debug)]
pub(crate) struct ReadySetStatus {
    pub(crate) controller_status: Option<ReadySetControllerStatus>,
    pub(crate) connected: bool,
    pub(crate) connection_count: usize,
    pub(crate) persistent_stats: Option<PersistentStats>,
}

impl ReadySetStatus {
    pub(crate) fn into_query_result(self) -> QueryResult<'static> {
        let mut status = Vec::new();
        status.push((
            "Database Connection".to_string(),
            if self.connected {
                "Connected".to_string()
            } else {
                "Unreachable".to_string()
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

pub(crate) struct ReadySetStatusReporter<'a, DB>
where
    DB: UpstreamDatabase,
{
    pub(crate) upstream: &'a mut Option<DB>,
    pub(crate) connector: &'a mut NoriaConnector,
    pub(crate) connections: &'a Option<Arc<SkipSet<SocketAddr>>>,
    pub(crate) authority: &'a Arc<Authority>,
}

impl<DB: UpstreamDatabase> ReadySetStatusReporter<'_, DB> {
    pub(crate) async fn report_status(self) -> ReadySetStatus {
        let controller_status = self.connector.readyset_status().await.ok();
        let connected = if let Some(upstream) = self.upstream.as_mut() {
            upstream.is_connected().await.unwrap_or_default()
        } else {
            false
        };
        let connection_count = self
            .connections
            .as_ref()
            .map(|c| c.len())
            .unwrap_or_default();
        let persistent_stats = self.authority.persistent_stats().await.unwrap_or_default();

        ReadySetStatus {
            controller_status,
            connected,
            connection_count,
            persistent_stats,
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
