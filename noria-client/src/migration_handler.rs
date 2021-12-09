//! The MigrationHandler can be used to asynchronously execute migrations
//! in a separate task. It uses the QueryStatusCache to identify
//! the set of queries that require migration, and runs migrations for
//! these queries on an interval.
//!
//! The migration handler may change a queries state based on the
//! response from Noria.
use crate::backend::{noria_connector, NoriaConnector};
use crate::query_status_cache::{MigrationState, QueryStatusCache};
use crate::upstream_database::{IsFatalError, NoriaCompare};
use crate::UpstreamDatabase;
use metrics::counter;
use nom_sql::SelectStatement;
use noria::ReadySetResult;
use noria_client_metrics::recorded;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::select;
use tracing::{error, info, instrument, warn};

pub struct MigrationHandler<DB> {
    /// Connection used to issue prepare requests to Noria.
    noria: NoriaConnector,

    /// Connector used to issue prepares to the upstream db.
    upstream: DB,

    /// The query status cache is polled on a regular interval to
    /// determine which queries require processing.
    query_status_cache: Arc<QueryStatusCache>,

    /// Whether the queries should be validated against MySQL during
    /// migration.
    validate_queries: bool,

    /// The minimum interval between subsequent polls to the query
    /// status cache. In practice it may be longer if the queries
    /// that require processing take longer than `min_poll_interval`.
    min_poll_interval: std::time::Duration,

    /// The maximum amount of time the migration handler will retry a
    /// query for before marking it as Unsupported.
    max_retry: std::time::Duration,

    /// Reciever to return the broadcast signal on.
    shutdown_recv: tokio::sync::broadcast::Receiver<()>,

    /// The time that we began performing migrations on the query.
    /// Queries are removed when a migration yields success or unsupported
    /// and re-added when they are found in the pending migration list.
    start_time: HashMap<SelectStatement, Instant>,
}

impl<DB> MigrationHandler<DB>
where
    DB: UpstreamDatabase,
{
    pub fn new(
        noria: NoriaConnector,
        upstream: DB,
        query_status_cache: Arc<QueryStatusCache>,
        validate_queries: bool,
        min_poll_interval: std::time::Duration,
        max_retry: std::time::Duration,
        shutdown_recv: tokio::sync::broadcast::Receiver<()>,
    ) -> MigrationHandler<DB> {
        MigrationHandler {
            noria,
            upstream,
            query_status_cache,
            validate_queries,
            min_poll_interval,
            max_retry,
            shutdown_recv,
            start_time: HashMap::new(),
        }
    }

    #[instrument(level = "warn", name = "migration_handler", skip(self))]
    pub async fn run(&mut self) -> ReadySetResult<()> {
        let mut interval = tokio::time::interval(self.min_poll_interval);
        loop {
            select! {
                _ = interval.tick() => {
                    let to_process = self.query_status_cache.pending_migration().await;

                    for q in &to_process {
                        self.perform_migration(q).await
                    }

                    counter!(recorded::MIGRATION_HANDLER_PROCESSED, to_process.len() as u64);
                }
                _ = self.shutdown_recv.recv() => {
                    info!("Migration handler shutting down after shut down signal received");
                    break;
                }
            }
        }
        Ok(())
    }

    async fn perform_migration(&mut self, stmt: &SelectStatement) {
        // If this is the first migration we are performing, add the query to the
        // start_time map.
        if !self.start_time.contains_key(stmt) {
            self.start_time.insert(stmt.clone(), Instant::now());
        }

        let upstream_result = if self.validate_queries {
            let mut upstream_result = self.upstream.prepare(stmt.to_string()).await;

            // If we returned an error indicating the connection was closed, we will try to
            // reconnect. If that fails, we have an unrecoverable error and should wait until the
            // next reconciliation pass.
            match upstream_result {
                Err(e) if e.is_fatal() => {
                    if let Err(e) = self.upstream.reset().await {
                        error!(
                            error = %e,
                            query = %stmt,
                            "MigrationHandler dropped conn to Upstream and failed to reconnnect",
                        );
                        return;
                    } else {
                        // Succeeded on reconnecting. Retry prepare.
                        upstream_result = self.upstream.prepare(stmt.to_string()).await;
                    }
                }
                _ => {}
            };

            if let Err(e) = upstream_result {
                error!(
                    error = %e,
                    query = %stmt,
                    "Query failed to be prepared against upstream",
                );
                return;
            }

            Some(upstream_result)
        } else {
            None
        };

        // Check if we can successfully prepare against noria as well.
        match self.noria.prepare_select(stmt.clone(), 0, true).await {
            Ok(n) => {
                if self.validate_queries {
                    if let noria_connector::PrepareResult::Select {
                        ref schema,
                        ref params,
                        ..
                    } = n
                    {
                        // Upstream is an Ok value as we check for the error above.
                        #[allow(clippy::unwrap_used)]
                        if let Err(e) = upstream_result
                            .unwrap()
                            .unwrap()
                            .meta
                            .compare(schema, params)
                        {
                            warn!(error = %e, query = %stmt, "Query compare failed");
                            // TODO(justin): Fix setting migration state to unsupported with
                            // validate_queries.
                            /*self.query_status_cache
                            .update_query_migration_state(stmt, MigrationState::Unsupported)
                            .await;*/
                            return;
                        }
                    } else {
                        return;
                    }
                }
                counter!(recorded::MIGRATION_HANDLER_ALLOWED, 1);
                self.start_time.remove(stmt);
                self.query_status_cache
                    .update_query_migration_state(stmt, MigrationState::Successful)
                    .await;
            }
            Err(e) if e.caused_by_unsupported() => {
                error!(error = %e,
                        query = %stmt,
                        "Select query is unsupported in ReadySet");
                self.start_time.remove(stmt);
                self.query_status_cache
                    .update_query_migration_state(stmt, MigrationState::Unsupported)
                    .await;
            }
            // Errors that were not caused by unsupported may be transient, do nothing
            // so we may retry the migration on this query.
            Err(e) => {
                warn!(error = %e,
                      query = %stmt,
                      "Select query may have transiently failed");
                if Instant::now() - *self.start_time.get(stmt).unwrap() > self.max_retry {
                    // Query failed for long enough, it is unsupported.
                    self.query_status_cache
                        .update_query_migration_state(stmt, MigrationState::Unsupported)
                        .await;
                }
            }
        }
    }
}
