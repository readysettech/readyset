use std::sync::Arc;

use readyset::query::MigrationState;
use readyset::ReadySetHandle;
use tokio::select;
use tracing::{debug, info, instrument, trace, warn};

use crate::query_status_cache::QueryStatusCache;

pub struct ViewsSynchronizer {
    /// The noria connector used to query
    controller: ReadySetHandle,
    /// The query status cache is updated according to which queries exist in noria
    query_status_cache: &'static QueryStatusCache,
    /// The interval between subsequent pollings of the Leader for migrated queries
    poll_interval: std::time::Duration,
    /// Receiver to return the shutdown signal on
    shutdown_recv: tokio::sync::broadcast::Receiver<()>,
}

impl ViewsSynchronizer {
    pub fn new(
        controller: ReadySetHandle,
        query_status_cache: &'static QueryStatusCache,
        poll_interval: std::time::Duration,
        shutdown_recv: tokio::sync::broadcast::Receiver<()>,
    ) -> Self {
        ViewsSynchronizer {
            controller,
            query_status_cache,
            poll_interval,
            shutdown_recv,
        }
    }

    //TODO(DAN): add metrics on views synchronizer performance (e.g., number of queries polled,
    //time spent processing)
    #[instrument(level = "info", name = "views_synchronizer", skip(self))]
    pub async fn run(&mut self) {
        let mut interval = tokio::time::interval(self.poll_interval);
        loop {
            select! {
                _ = interval.tick() => self.poll().await,
                _ = self.shutdown_recv.recv() => {
                    info!("Views Synchronizer shutting down after shut down signal received");
                    break;
                }
            }
        }
    }

    async fn poll(&mut self) {
        debug!("Views synchronizer polling");
        let queries = self
            .query_status_cache
            .pending_migration()
            .into_iter()
            .filter_map(|(q, _)| q.into_parsed().map(Arc::unwrap_or_clone))
            .collect::<Vec<_>>();

        match self.controller.view_statuses(queries.clone()).await {
            Ok(statuses) => {
                for (query, migrated) in queries.into_iter().zip(statuses) {
                    trace!(
                        query = %query.statement,
                        migrated,
                        "Loaded query status from controller"
                    );
                    if migrated {
                        self.query_status_cache
                            .update_query_migration_state(&query, MigrationState::Successful)
                    }
                }
            }
            Err(error) => warn!(%error, "Could not get view statuses from leader"),
        }
    }
}
