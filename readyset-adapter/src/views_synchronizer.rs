use std::sync::Arc;

use dataflow_expression::Dialect;
use readyset_client::query::MigrationState;
use readyset_client::recipe::changelist::Change;
use readyset_client::recipe::ChangeList;
use readyset_client::ReadySetHandle;
use readyset_util::shutdown::ShutdownReceiver;
use tokio::select;
use tracing::{debug, info, instrument, trace, warn};

use crate::query_status_cache::QueryStatusCache;
use crate::utils;

pub struct ViewsSynchronizer {
    /// The noria connector used to query
    controller: ReadySetHandle,
    /// The query status cache is updated according to which queries exist in noria
    query_status_cache: &'static QueryStatusCache,
    /// The interval between subsequent pollings of the Leader for migrated queries
    poll_interval: std::time::Duration,
    /// Dialect to pass to ReadySet to control the expression semantics used for all queries
    dialect: Dialect,
    /// Receiver to return the shutdown signal on
    shutdown_recv: ShutdownReceiver,
}

impl ViewsSynchronizer {
    pub fn new(
        controller: ReadySetHandle,
        query_status_cache: &'static QueryStatusCache,
        poll_interval: std::time::Duration,
        dialect: Dialect,
        shutdown_recv: ShutdownReceiver,
    ) -> Self {
        ViewsSynchronizer {
            controller,
            query_status_cache,
            poll_interval,
            dialect,
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
                // We use `biased` here to ensure that our shutdown signal will be received and
                // acted upon even if the other branches in this `select!` are constantly in a
                // ready state (e.g. a stream that has many messages where very little time passes
                // between receipt of these messages). More information about this situation can
                // be found in the docs for `tokio::select`.
                biased;
                _ = self.shutdown_recv.recv() => {
                    info!("Views Synchronizer shutting down after shut down signal received");
                    break;
                }
                _ = interval.tick() => self.poll().await,
            }
        }
    }

    async fn poll(&mut self) {
        debug!("Views synchronizer polling");
        let queries = self
            .query_status_cache
            .pending_migration()
            .into_iter()
            .chain(self.query_status_cache.dry_run_succeeded().into_iter())
            .filter_map(|(q, _)| q.into_parsed().map(Arc::unwrap_or_clone))
            .collect::<Vec<_>>();

        match self
            .controller
            .view_statuses(queries.clone(), self.dialect)
            .await
        {
            Ok(statuses) => {
                for (query, migrated) in queries.into_iter().zip(statuses) {
                    trace!(
                        // FIXME(ENG-2499): Use correct dialect.
                        query = %query.statement.display(nom_sql::Dialect::MySQL),
                        migrated,
                        "Loaded query status from controller"
                    );
                    if migrated {
                        // Explicitly migrate the query.
                        // This adds the query hash as an alias for the existing cache, allowing
                        // it to be treated as a cache name e.g. for use in a `DROP CACHE`
                        // statement.
                        // We don't need to do any rewrites here, as they have already been done on
                        // the statements in the QueryStatusCache.
                        // The value for `always` is ignored, as any statements we cache here are
                        // guaranteed to alias to an existing cached statement.
                        match self
                            .controller
                            .extend_recipe(
                                ChangeList::from_change(
                                    Change::create_cache(
                                        // Rehashing the query here is simpler
                                        // than a reverse-lookup into
                                        // `self.query_status_cache.ids`.
                                        utils::generate_query_name(
                                            &query.statement,
                                            &query.schema_search_path,
                                        ),
                                        query.statement.clone(),
                                        false,
                                    ),
                                    self.dialect,
                                )
                                .with_schema_search_path(query.schema_search_path.clone()),
                            )
                            .await
                        {
                            Ok(_) => self
                                .query_status_cache
                                .update_query_migration_state(&query, MigrationState::Successful),
                            Err(_) => self
                                .query_status_cache
                                .update_query_migration_state(&query, MigrationState::Unsupported),
                        }
                    }
                }
            }
            Err(error) => warn!(%error, "Could not get view statuses from leader"),
        }
    }
}
