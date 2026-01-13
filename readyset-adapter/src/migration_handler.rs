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
use metrics::{Counter, counter};
use readyset_client::query::{MigrationState, Query, QueryId};
use readyset_client::recipe::changelist::{Change, ChangeList};
use readyset_client::{PlaceholderIdx, ReadySetHandle, ViewCreateRequest};
use readyset_client_metrics::recorded;
use readyset_data::DfValue;
use readyset_errors::{ReadySetResult, internal_err};
use readyset_sql::ast::CacheType;
use readyset_sql::ast::SqlIdentifier;
use readyset_sql::{DialectDisplay, ast::Literal};
use readyset_sql_passes::InlineLiterals;
use readyset_util::redacted::Sensitive;
use readyset_util::shutdown::ShutdownReceiver;
use schema_catalog::{RewriteContext, SchemaCatalogHandle};
use tokio::select;
use tracing::{debug, error, info, warn};

use crate::backend::NoriaConnector;
use crate::query_status_cache::QueryStatusCache;

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

    /// A handle to the SchemaCatalog used for rewrite contexts and invalidating dry run migrations
    /// if the schema is updated while they are in flight.
    schema_catalog_handle: SchemaCatalogHandle,

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
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        noria: NoriaConnector,
        controller: Option<ReadySetHandle>,
        dialect: Dialect,
        query_status_cache: &'static QueryStatusCache,
        schema_catalog_handle: SchemaCatalogHandle,
        min_poll_interval: std::time::Duration,
        max_retry: std::time::Duration,
        shutdown_recv: ShutdownReceiver,
    ) -> MigrationHandler {
        MigrationHandler {
            noria,
            controller,
            dialect,
            query_status_cache,
            schema_catalog_handle,
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
                Query::ShallowParsed(_) => {
                    // Shallow cache queries don't need migration, skip them
                    continue;
                }
                Query::ParseFailed(..) => {
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
                    Err(e) if e.is_transient() => {
                        debug!(
                            error = %e,
                            query = %Sensitive(&query.query().statement.display(self.dialect.into())),
                            "Transient failure during inline migration"
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
                    Err(e) => {
                        debug!(
                            error = %e,
                            query = %Sensitive(&query.query().statement.display(self.dialect.into())),
                            "Query not supported by inline migration"
                        );
                        self.query_status_cache
                            .unsupported_inlined_migration(query.query());
                        self.start_time.remove(query.query());
                    }
                }
            }

            // Perform a migration on the original query to update the cache
            // TODO(REA-2469): Implement retry
            //
            // We currently don't fully support inlined migrations, and piping through the
            // full 'create cache' string for _every_ query we see would be overly
            // expensive, so we fall back to constructing a 'create cache' statement from a
            // displayed version of the the SelectStatement we already parsed in this case
            if !successful_migrations.is_empty() {
                // TODO(REA-2469): Store a `create cache` statement for our new inlined cache.
                let _ = self
                    .noria
                    .handle_create_cached_query(
                        None,
                        query.query().clone(),
                        /* always */ false,
                        /* concurrently */ false,
                        0, // TODO: pass actual schema generation
                    )
                    .await;
                // Inform the query status cache of completed migrations
                self.query_status_cache
                    .created_inlined_query(query.query(), successful_migrations);
            }
        }
    }

    pub async fn run(&mut self) -> ReadySetResult<()> {
        let mut interval = tokio::time::interval(self.min_poll_interval);
        let success_counter = counter!(recorded::MIGRATION_HANDLER_SUCCESSES);
        let failure_counter = counter!(recorded::MIGRATION_HANDLER_FAILURES);

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
                    self.purge_expired_migrations();
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

        let result = match self
            .rewrite_context(view_request.schema_search_path.clone())
            .await
        {
            Ok(rewrite_context) => {
                self.noria
                    .prepare_select(view_request.statement.clone(), true, &rewrite_context)
                    .await
            }
            Err(e) => Err(e),
        };

        // Check if we can successfully prepare against noria as well.
        match result {
            Ok(_) => {
                counter!(recorded::MIGRATION_HANDLER_ALLOWED).increment(1);
                self.start_time.remove(view_request);
                self.query_status_cache.update_query_migration_state(
                    view_request,
                    MigrationState::Successful(CacheType::Deep),
                    None,
                );
            }
            Err(e) if e.is_transient() => {
                debug!(
                    error = %e,
                    query = %Sensitive(&view_request.statement.display(self.dialect.into())),
                    "Transient failure during migration"
                );
                if Instant::now() - *self.start_time.get(view_request).unwrap() > self.max_retry {
                    // Query failed for long enough, it is unsupported.
                    self.query_status_cache.update_query_migration_state(
                        view_request,
                        MigrationState::Unsupported("Migration timed out".to_string()),
                        None,
                    );
                }
            }
            Err(e) => {
                debug!(
                    error = %e,
                    query = %Sensitive(&view_request.statement.display(self.dialect.into())),
                    "Query not supported by migration"
                );

                self.start_time.remove(view_request);
                self.query_status_cache.update_query_migration_state(
                    view_request,
                    MigrationState::Unsupported(
                        e.unsupported_cause().unwrap_or_else(|| e.to_string()),
                    ),
                    None,
                );
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

        let req = ViewCreateRequest::new(
            view_request.statement.clone().inline_literals(&mapping),
            view_request.schema_search_path.clone(),
        );

        // TODO(mvzink): Pass actual schema generation
        self.noria
            .handle_create_cached_query(None, req, false, false, 0)
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
                query = %Sensitive(&view_request.statement.display(self.dialect.into())),
                "Max retry time of {:?} exceeded for dry run migration",
                self.max_retry
            );
            self.query_status_cache.update_query_migration_state(
                view_request,
                MigrationState::Unsupported("dry run timed out".into()),
                None,
            );
            return;
        }
        let qname = QueryId::from_select(
            &view_request.statement,
            view_request.schema_search_path.as_slice(),
        );

        let schema_generation = match self.schema_catalog_handle.get_catalog_retrying().await {
            Ok(catalog) => catalog.generation,
            Err(error) => {
                warn!(%error, "Failed to fetch schema generation for dry run migration");
                return;
            }
        };

        // We do not need to provide a real "create cache" String for a dry run migration
        let changelist = ChangeList::from_change(
            Change::create_cache(
                qname,
                view_request.statement.clone(),
                false,
                schema_generation,
            ),
            self.dialect,
        )
        .with_schema_search_path(view_request.schema_search_path.clone());
        match controller.dry_run(changelist).await {
            Ok(_) => {
                self.start_time.remove(view_request);

                // It's possible that the ViewsSynchronizer found an existing view for this query
                // on the server while we were performing a dry run, in which case it would have
                // updated the query's status to "successful". In this situation, we don't want to
                // overwrite the "successful" status, so we only write the new "dry run succeeded"
                // status if the query's status is still "pending"
                self.query_status_cache
                    .with_mut_migration_state(view_request, |status| {
                        if status.is_pending() {
                            *status = MigrationState::DryRunSucceeded;
                        }
                    });
            }
            Err(e) if e.is_transient() => {} // Leave it as pending.
            Err(e) => {
                self.start_time.remove(view_request);
                self.query_status_cache.update_query_migration_state(
                    view_request,
                    MigrationState::Unsupported(
                        e.unsupported_cause().unwrap_or_else(|| e.to_string()),
                    ),
                    None,
                );
            }
        }
    }

    /// Purge migrations that have exceeded the max retry time. Entries in the `start_time` map
    /// may have been removed by the QueryStatusCache due to it's `LruCache` eviction policy, and we
    /// might get stuck with orphan entries in the our map. Hence, we need to purge them here.
    fn purge_expired_migrations(&mut self) {
        let now = Instant::now();
        self.start_time
            .retain(|_, start_time| now - *start_time <= self.max_retry);
    }

    async fn rewrite_context(
        &self,
        search_path: Vec<SqlIdentifier>,
    ) -> ReadySetResult<RewriteContext> {
        Ok(RewriteContext::new(
            self.dialect,
            self.schema_catalog_handle.get_catalog().await?,
            search_path,
        ))
    }
}
