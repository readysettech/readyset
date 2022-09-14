//! The MigrationHandler can be used to asynchronously execute migrations
//! in a separate task. It uses the QueryStatusCache to identify
//! the set of queries that require migration, and runs migrations for
//! these queries on an interval.
//!
//! The migration handler may change a queries state based on the
//! response from ReadySet.
use std::collections::HashMap;
use std::time::Instant;

use launchpad::redacted::Sensitive;
use metrics::{counter, register_counter};
use readyset::recipe::changelist::{Change, ChangeList};
use readyset::{ControllerHandle, ReadySetResult, ViewCreateRequest};
use readyset_client_metrics::recorded;
use tokio::select;
use tracing::{error, info, instrument, warn};

use crate::backend::{noria_connector, NoriaConnector};
use crate::query_status_cache::{MigrationState, Query, QueryStatusCache};
use crate::upstream_database::{IsFatalError, NoriaCompare};
use crate::{utils, UpstreamDatabase};

pub struct MigrationHandler<DB> {
    /// Connection used to issue prepare requests to ReadySet.
    noria: NoriaConnector,

    /// The noria connector used to query if we are configured to run in dry_run mode.
    controller: Option<ControllerHandle>,

    /// Connector used to issue prepares to the upstream db.
    upstream: Option<DB>,

    /// The query status cache is polled on a regular interval to
    /// determine which queries require processing.
    query_status_cache: &'static QueryStatusCache,

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

    /// Receiver to return the broadcast signal on.
    shutdown_recv: tokio::sync::broadcast::Receiver<()>,

    /// The time that we began performing migrations on the query.
    /// Queries are removed when a migration yields success or unsupported
    /// and re-added when they are found in the pending migration list.
    start_time: HashMap<ViewCreateRequest, Instant>,
}

impl<DB> MigrationHandler<DB>
where
    DB: UpstreamDatabase,
{
    #[allow(clippy::too_many_arguments)] // Only one over. Designing away that for a single over arg seems like over-engineering.
    pub fn new(
        noria: NoriaConnector,
        upstream: Option<DB>,
        controller: Option<ControllerHandle>,
        query_status_cache: &'static QueryStatusCache,
        validate_queries: bool,
        min_poll_interval: std::time::Duration,
        max_retry: std::time::Duration,
        shutdown_recv: tokio::sync::broadcast::Receiver<()>,
    ) -> MigrationHandler<DB> {
        MigrationHandler {
            noria,
            upstream,
            controller,
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
        let success_counter = register_counter!(recorded::MIGRATION_HANDLER_SUCCESSES);
        let failure_counter = register_counter!(recorded::MIGRATION_HANDLER_FAILURES);

        loop {
            select! {
                _ = interval.tick() => {
                    let to_process = self.query_status_cache.pending_migration();
                    let has_controller = self.controller.is_some();
                    let mut successes = 0;
                    let mut failures = 0;
                    for q in to_process {
                        match &q.0 {
                            Query::Parsed(req) => {
                                if has_controller {
                                    self.perform_dry_run_migration(req).await
                                } else {
                                    self.perform_migration(req).await
                                }
                                successes += 1;
                            }
                            Query::ParseFailed(_) => {
                                error!("Should not be migrating query that failed to parse. Ignoring");
                                failures += 1;
                            },
                        }
                    }

                    success_counter.increment(successes);
                    failure_counter.increment(failures);
                }
                _ = self.shutdown_recv.recv() => {
                    info!("Migration handler shutting down after shut down signal received");
                    break;
                }
            }
        }
        Ok(())
    }

    async fn perform_migration(&mut self, view_request: &ViewCreateRequest) {
        // If this is the first migration we are performing, add the query to the
        // start_time map.
        if !self.start_time.contains_key(view_request) {
            self.start_time.insert(view_request.clone(), Instant::now());
        }

        let upstream_result = match self.upstream {
            Some(ref mut db) if self.validate_queries => {
                // TODO(grfn): Set search path here
                let mut upstream_result = db.prepare(view_request.statement.to_string()).await;

                // If we returned an error indicating the connection was closed, we will try to
                // reconnect. If that fails, we have an unrecoverable error and should wait until
                // the next reconciliation pass.
                match upstream_result {
                    Err(e) if e.is_fatal() => {
                        if let Err(e) = db.reset().await {
                            error!(
                                error = %e,
                                query = %Sensitive(&view_request.statement),
                                "MigrationHandler dropped conn to Upstream and failed to reconnnect",
                            );
                            return;
                        } else {
                            // Succeeded on reconnecting. Retry prepare.
                            upstream_result = db.prepare(view_request.statement.to_string()).await;
                        }
                    }
                    _ => {}
                };

                if let Err(e) = upstream_result {
                    error!(
                        error = %e,
                        query = %Sensitive(&view_request.statement),
                        "Query failed to be prepared against upstream",
                    );
                    return;
                }

                Some(upstream_result)
            }
            _ => None,
        };

        // Check if we can successfully prepare against noria as well.
        match self
            .noria
            .prepare_select(
                view_request.statement.clone(),
                0,
                true,
                Some(view_request.schema_search_path.clone()),
            )
            .await
        {
            Ok(n) => {
                if self.validate_queries {
                    if let noria_connector::PrepareResult::Select {
                        ref schema,
                        ref params,
                        ..
                    } = n
                    {
                        // Upstream is an Ok value as we check for the error above.
                        if let Err(e) = upstream_result
                            .unwrap()
                            .unwrap()
                            .meta
                            .compare(schema, params)
                        {
                            warn!(
                                error = %e,
                                query = %Sensitive(&view_request.statement),
                                "Query compare failed"
                            );
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
                self.start_time.remove(view_request);
                self.query_status_cache
                    .update_query_migration_state(view_request, MigrationState::Successful);
            }
            Err(e) if e.caused_by_unsupported() => {
                error!(
                    error = %e,
                    query = %Sensitive(&view_request.statement),
                    "Select query is unsupported in ReadySet"
                );

                self.start_time.remove(view_request);
                self.query_status_cache
                    .update_query_migration_state(view_request, MigrationState::Unsupported);
            }
            // Errors that were not caused by unsupported may be transient, do nothing
            // so we may retry the migration on this query.
            Err(e) => {
                warn!(
                    error = %e,
                    query = %Sensitive(&view_request.statement),
                    "Select query may have transiently failed"
                );
                if Instant::now() - *self.start_time.get(view_request).unwrap() > self.max_retry {
                    // Query failed for long enough, it is unsupported.
                    self.query_status_cache
                        .update_query_migration_state(view_request, MigrationState::Unsupported);
                }
            }
        }
    }

    async fn perform_dry_run_migration(&mut self, view_request: &ViewCreateRequest) {
        let controller = if let Some(ref mut c) = self.controller {
            c
        } else {
            return;
        };
        let start_time = self
            .start_time
            .entry(view_request.clone())
            .or_insert_with(Instant::now);
        if Instant::now() - *start_time > self.max_retry {
            // We've exceeded the max amount of times we'll try running dry runs with this query.
            // It's probably unsupported, but we'll allow a proper migration determine that.
            return;
        }
        let qname =
            utils::generate_query_name(&view_request.statement, &view_request.schema_search_path);
        let changelist = ChangeList::from_change(Change::create_cache(
            qname,
            view_request.statement.clone(),
            false,
        ))
        .with_schema_search_path(view_request.schema_search_path.clone());
        match controller.dry_run(changelist).await {
            Ok(_) => {
                self.start_time.remove(view_request);
                self.query_status_cache
                    .update_query_migration_state(view_request, MigrationState::DryRunSucceeded);
            }
            Err(e) if e.caused_by_unsupported() => {
                self.start_time.remove(view_request);
                self.query_status_cache
                    .update_query_migration_state(view_request, MigrationState::Unsupported);
            }
            _ => {} // Leave it as pending.
        }
    }
}
