//! The MigrationHandler can be used to asynchronously execute migrations
//! in a separate task. It uses the QueryStatusCache to identify
//! the set of queries that require migration, and runs migrations for
//! these queries on an interval.
//!
//! The migration handler may change a queries state based on the
//! response from ReadySet.
use std::collections::HashMap;
use std::time::Instant;

use dataflow_expression::Dialect;
use metrics::{counter, register_counter, Counter};
use nom_sql::Literal;
use readyset_client::query::{MigrationState, Query};
use readyset_client::recipe::changelist::{Change, ChangeList};
use readyset_client::{PlaceholderIdx, ReadySetHandle, ViewCreateRequest};
use readyset_client_metrics::recorded;
use readyset_data::DfValue;
use readyset_errors::{internal_err, ReadySetResult};
use readyset_sql_passes::InlineLiterals;
use readyset_util::redacted::Sensitive;
use readyset_util::shutdown::ShutdownReceiver;
use tokio::select;
use tracing::{debug, error, info, instrument};

use crate::backend::NoriaConnector;
use crate::query_status_cache::QueryStatusCache;
use crate::utils;

pub struct MigrationHandler {
    /// Connection used to issue prepare requests to ReadySet.
    noria: NoriaConnector,

    /// The noria connector used to query if we are configured to run in dry_run mode.
    controller: Option<ReadySetHandle>,

    /// SQL Dialect to pass to ReadySet as part of all migration requests
    dialect: Dialect,

    /// The query status cache is polled on a regular interval to
    /// determine which queries require processing.
    query_status_cache: &'static QueryStatusCache,

    /// The minimum interval between subsequent polls to the query
    /// status cache. In practice it may be longer if the queries
    /// that require processing take longer than `min_poll_interval`.
    min_poll_interval: std::time::Duration,

    /// The maximum amount of time the migration handler will retry a
    /// query for before marking it as Unsupported.
    max_retry: std::time::Duration,

    /// Receiver to listen for a shutdown signal
    shutdown_recv: ShutdownReceiver,

    /// The time that we began performing migrations on the query.
    /// Queries are removed when a migration yields success or unsupported
    /// and re-added when they are found in the pending migration list.
    start_time: HashMap<ViewCreateRequest, Instant>,
}

impl MigrationHandler {
    pub fn new(
        noria: NoriaConnector,
        controller: Option<ReadySetHandle>,
        query_status_cache: &'static QueryStatusCache,
        dialect: Dialect,
        min_poll_interval: std::time::Duration,
        max_retry: std::time::Duration,
        shutdown_recv: ShutdownReceiver,
    ) -> MigrationHandler {
        MigrationHandler {
            noria,
            controller,
            dialect,
            query_status_cache,
            min_poll_interval,
            max_retry,
            shutdown_recv,
            start_time: HashMap::new(),
        }
    }

    /// Migrate (or attempt a dry run migration) for each query marked as pending in the
    /// `QueryStatusCache`.
    async fn process_pending_migrations(
        &mut self,
        success_counter: &Counter,
        failure_counter: &Counter,
    ) {
        // First process pending queries
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
                }
            }
        }

        success_counter.increment(successes);
        failure_counter.increment(failures);
    }

    /// Migrate queries pending an inlined migration in the `QueryStatusCache`. A single query may
    /// have multiple sets of literals for which we attempt to run an inlined migration. After each
    /// inlined migration is completed, we migrate the original query. This allows the server to
    /// associate the original query with the inlined migrations that we ran.
    async fn process_inlined_migrations(&mut self) {
        let inlined_queries = self.query_status_cache.pending_inlined_migration();
        for query in inlined_queries {
            let mut successful_migrations = vec![];
            for literals in query.literals() {
                match self
                    .perform_inlined_migration(query.query(), query.placeholders(), literals)
                    .await
                {
                    Ok(()) => {
                        successful_migrations.push(literals);
                        // Remove the start time since we've successfully completed the migration.
                        self.start_time.remove(query.query());
                    }
                    Err(e) if e.caused_by_unsupported() => {
                        debug!(
                            error = %e,
                            // FIXME(REA-2169): Use correct dialect.
                            query = %Sensitive(&query.query().statement.display(nom_sql::Dialect::MySQL)),
                            "Select query is unsupported in ReadySet"
                        );
                        self.query_status_cache
                            .unsupported_inlined_migration(query.query());
                        self.start_time.remove(query.query());
                    }
                    // Errors that were not caused by unsupported may be transient, do nothing
                    // so we may retry the migration on this query.
                    Err(e) => {
                        debug!(
                            error = %e,
                            // FIXME(REA-2169): Use correct dialect.
                            query = %Sensitive(&query.query().statement.display(nom_sql::Dialect::MySQL)),
                            "Select query may have transiently failed"
                        );
                        if Instant::now() - *self.start_time.get(query.query()).unwrap()
                            > self.max_retry
                        {
                            // Query failed for long enough, it is unsupported.
                            self.query_status_cache
                                .unsupported_inlined_migration(query.query());
                            self.start_time.remove(query.query());
                        }
                    }
                }
            }

            // Perform a migration on the original query to update the cache
            // TODO(ENG-2820): Implement retry
            if !successful_migrations.is_empty() {
                let _ = self
                    .noria
                    .handle_create_cached_query(
                        None,
                        &query.query().statement,
                        Some(query.query().schema_search_path.clone()),
                        false,
                        false,
                    )
                    .await;
                // Inform the query status cache of completed migrations
                self.query_status_cache
                    .created_inlined_query(query.query(), successful_migrations);
            }
        }
    }

    #[instrument(level = "warn", name = "migration_handler", skip(self))]
    pub async fn run(&mut self) -> ReadySetResult<()> {
        let mut interval = tokio::time::interval(self.min_poll_interval);
        let success_counter = register_counter!(recorded::MIGRATION_HANDLER_SUCCESSES);
        let failure_counter = register_counter!(recorded::MIGRATION_HANDLER_FAILURES);

        loop {
            select! {
                // We use `biased` here to ensure that our shutdown signal will be received and
                // acted upon even if the other branches in this `select!` are constantly in a
                // ready state (e.g. a stream that has many messages where very little time passes
                // between receipt of these messages). More information about this situation can
                // be found in the docs for `tokio::select`.
                biased;
                _ = self.shutdown_recv.recv() => {
                    info!("Migration handler shutting down after shut down signal received");
                    break;
                }
                _ = interval.tick() => {
                    self.process_pending_migrations(&success_counter, &failure_counter).await;
                    self.process_inlined_migrations().await;
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

        // Check if we can successfully prepare against noria as well.
        match self
            .noria
            .prepare_select(
                view_request.statement.clone(),
                true,
                Some(view_request.schema_search_path.clone()),
            )
            .await
        {
            Ok(_) => {
                counter!(recorded::MIGRATION_HANDLER_ALLOWED, 1);
                self.start_time.remove(view_request);
                self.query_status_cache
                    .update_query_migration_state(view_request, MigrationState::Successful);
            }
            Err(e) if e.caused_by_unsupported() => {
                debug!(
                    error = %e,
                    // FIXME(REA-2168 + REA-2169): Use correct dialect.
                    query = %Sensitive(&view_request.statement.display(nom_sql::Dialect::MySQL)),
                    "Select query is unsupported in ReadySet"
                );

                self.start_time.remove(view_request);
                self.query_status_cache
                    .update_query_migration_state(view_request, MigrationState::Unsupported);
            }
            // Errors that were not caused by unsupported may be transient, do nothing
            // so we may retry the migration on this query.
            Err(e) => {
                debug!(
                    error = %e,
                    // FIXME(REA-2168 + REA-2169): Use correct dialect.
                    query = %Sensitive(&view_request.statement.display(nom_sql::Dialect::MySQL)),
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

    /// Inline the literals over the placeholders for the view_request.
    async fn perform_inlined_migration(
        &mut self,
        view_request: &ViewCreateRequest,
        placeholders: &[PlaceholderIdx],
        literals: &[DfValue],
    ) -> ReadySetResult<()> {
        // We maintain one entry for all inlined migrations for the same query. It's unlikely that a
        // query will be unsupported for only a subset of `DfValue` literals.
        if !self.start_time.contains_key(view_request) {
            self.start_time.insert(view_request.clone(), Instant::now());
        }

        let mapping = placeholders
            .iter()
            .map(|p| {
                literals
                    .get(*p - 1) // placeholders are 1-indexed
                    .and_then(|v| Literal::try_from(v.clone()).ok())
                    .map(|lit| (*p, lit))
                    .ok_or_else(|| internal_err!("Expected value for placeholder ${p}"))
            })
            .collect::<ReadySetResult<HashMap<_, _>>>()?;

        let inlined_query = view_request.statement.clone().inline_literals(&mapping);

        self.noria
            .handle_create_cached_query(
                None,
                &inlined_query,
                Some(view_request.schema_search_path.clone()),
                false,
                false,
            )
            .await?;
        Ok(())
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
            // We can't be certain its unsupported without attempting an actual migration, but
            // mark it unsupported anyway to avoid confusion.
            debug!(
                "Max retry time of {:?} exceeded for dry run migration. {:?} is probably unsupported",
                self.max_retry,
                view_request.to_anonymized_string()
            );
            self.query_status_cache
                .update_query_migration_state(view_request, MigrationState::Unsupported);
            return;
        }
        let qname =
            utils::generate_query_name(&view_request.statement, &view_request.schema_search_path);
        let changelist = ChangeList::from_change(
            Change::create_cache(qname, view_request.statement.clone(), false),
            self.dialect,
        )
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
