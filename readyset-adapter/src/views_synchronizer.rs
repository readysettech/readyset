use std::sync::Arc;

use dataflow_expression::Dialect;
use nom_sql::{DialectDisplay, Relation};
use readyset_client::query::MigrationState;
use readyset_client::{ReadySetHandle, ViewCreateRequest};
use readyset_util::shared_cache::LocalCache;
use readyset_util::shutdown::ShutdownReceiver;
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
    /// Dialect to pass to ReadySet to control the expression semantics used for all queries
    dialect: Dialect,
    /// Global and thread-local cache of view endpoints and prepared statements.
    view_name_cache: LocalCache<ViewCreateRequest, Relation>,
}

impl ViewsSynchronizer {
    pub fn new(
        controller: ReadySetHandle,
        query_status_cache: &'static QueryStatusCache,
        poll_interval: std::time::Duration,
        dialect: Dialect,
        view_name_cache: LocalCache<ViewCreateRequest, Relation>,
    ) -> Self {
        ViewsSynchronizer {
            controller,
            query_status_cache,
            poll_interval,
            dialect,
            view_name_cache,
        }
    }

    //TODO(DAN): add metrics on views synchronizer performance (e.g., number of queries polled,
    //time spent processing)
    #[instrument(level = "info", name = "views_synchronizer", skip(self))]
    pub async fn run(&mut self, mut shutdown_recv: ShutdownReceiver) {
        let mut interval = tokio::time::interval(self.poll_interval);

        let fut = async {
            loop {
                interval.tick().await;
                self.poll().await;
            }
        };
        select! {
            // We use `biased` here to ensure that our shutdown signal will be received and
            // acted upon even if the other branches in this `select!` are constantly in a
            // ready state (e.g. a stream that has many messages where very little time passes
            // between receipt of these messages). More information about this situation can
            // be found in the docs for `tokio::select`.
            biased;
            _ = shutdown_recv.recv() => {
                info!("Views Synchronizer shutting down after shut down signal received");
            }
            _ = fut => unreachable!(),
        }
    }

    async fn poll(&mut self) {
        debug!("Views synchronizer polling");
        let queries = self
            .query_status_cache
            .pending_migration()
            .into_iter()
            .filter_map(|(q, _)| {
                q.into_parsed()
                    // once arc_unwrap_or_clone is stabilized, we can use that cleaner syntax
                    .map(|p| Arc::try_unwrap(p).unwrap_or_else(|arc| (*arc).clone()))
            })
            .collect::<Vec<_>>();

        match self
            .controller
            .view_names(queries.clone(), self.dialect)
            .await
        {
            Ok(statuses) => {
                for (query, name) in queries.into_iter().zip(statuses) {
                    trace!(
                        // FIXME(REA-2168): Use correct dialect.
                        query = %query.statement.display(nom_sql::Dialect::MySQL),
                        name = ?name,
                        "Loaded query status from controller"
                    );
                    if let Some(name) = name {
                        self.view_name_cache.insert(query.clone(), name).await;
                        self.query_status_cache
                            .update_query_migration_state(&query, MigrationState::Successful)
                    }
                }
            }
            Err(error) => warn!(%error, "Could not get view statuses from leader"),
        }
    }
}
