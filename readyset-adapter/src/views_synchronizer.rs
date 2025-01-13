use std::collections::HashSet;
use std::sync::Arc;

use dataflow_expression::Dialect;
use metrics::{counter, Counter};
use metrics::{gauge, Gauge};
use nom_sql::{DialectDisplay, Relation};
use readyset_client::query::MigrationState;
use readyset_client::{ReadySetHandle, ViewCreateRequest};
use readyset_client_metrics::recorded;
use readyset_util::shared_cache::LocalCache;
use readyset_util::shutdown::ShutdownReceiver;
use tokio::select;
use tracing::{debug, info, trace, warn};
use xxhash_rust::xxh3;

use crate::query_status_cache::QueryStatusCache;

struct Metrics {
    /// The number of queries we've checked for views
    queries_checked: Counter,
    query_status_cache_id_to_status_size: Gauge,
    query_status_cache_statuses_size: Gauge,
    query_status_cache_pending_inline_migrations: Gauge,
    view_name_cache_size_local: Gauge,
    view_name_cache_size_shared: Gauge,
    views_checked_size: Gauge,
}

impl Default for Metrics {
    fn default() -> Self {
        Self {
            queries_checked: counter!(recorded::VIEWS_SYNCHRONIZER_QUERIES_CHECKED),
            query_status_cache_id_to_status_size: gauge!(
                recorded::QUERY_STATUS_CACHE_ID_TO_STATUS_SIZE
            ),
            query_status_cache_statuses_size: gauge!(recorded::QUERY_STATUS_CACHE_STATUSES_SIZE),
            query_status_cache_pending_inline_migrations: gauge!(
                recorded::QUERY_STATUS_CACHE_PENDING_INLINE_MIGRATIONS
            ),
            view_name_cache_size_local: gauge!(
                recorded::VIEWS_SYNCHRONIZER_VIEW_NAME_CACHE_SIZE,
                &[("cache", "local")]
            ),
            view_name_cache_size_shared: gauge!(
                recorded::VIEWS_SYNCHRONIZER_VIEW_NAME_CACHE_SIZE,
                &[("cache", "shared")]
            ),
            views_checked_size: gauge!(recorded::VIEWS_SYNCHRONIZER_VIEWS_CHECKED_SIZE),
        }
    }
}

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
    /// A cache to keep track of the queries for which we've already checked the server for
    /// existing views. Note that this cache is *not* updated (i.e. a query is not removed) when a
    /// "dry run succeeded" query is migrated.
    ///
    /// This HashSet stores 128-bit hashes computed via xxHash in an attempt to minimize the amount
    /// of data we need to store to keep track of the queries we've already seen.2
    views_checked: HashSet<u128>,
    /// Simple struct for storing references to metrics.
    metrics: Metrics,
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
            views_checked: HashSet::new(),
            metrics: Metrics::default(),
        }
    }

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

    async fn report_metrics(&self) {
        let metrics = self.query_status_cache.reportable_metrics();
        self.metrics
            .query_status_cache_id_to_status_size
            .set(metrics.id_to_status_size as f64);
        self.metrics
            .query_status_cache_statuses_size
            .set(metrics.statuses_size as f64);
        self.metrics
            .query_status_cache_pending_inline_migrations
            .set(metrics.pending_inlined_migrations_size as f64);
        let view_name_cache_metrics = self.view_name_cache.metrics().await;
        self.metrics
            .view_name_cache_size_local
            .set(view_name_cache_metrics.local_cache_size as f64);
        self.metrics
            .view_name_cache_size_shared
            .set(view_name_cache_metrics.shared_cache_size as f64);

        self.metrics
            .views_checked_size
            .set(self.views_checked.len() as f64);
    }

    async fn poll(&mut self) {
        debug!("Views synchronizer polling");
        self.report_metrics().await;
        let (queries, hashes): (Vec<_>, Vec<_>) = self
            .query_status_cache
            .queries_with_statuses(&[MigrationState::DryRunSucceeded, MigrationState::Pending])
            .into_iter()
            .filter_map(|(q, _)| {
                q.into_parsed().and_then(|p| {
                    let hash = xxh3::xxh3_128(&bincode::serialize(&*p).unwrap());

                    if self.views_checked.contains(&hash) {
                        None
                    } else {
                        // once arc_unwrap_or_clone is stabilized, we can use that cleaner syntax
                        Some((
                            Arc::try_unwrap(p).unwrap_or_else(|arc| (*arc).clone()),
                            hash,
                        ))
                    }
                })
            })
            .unzip();

        for chunk in queries.chunks(128) {
            match self
                .controller
                .view_names(chunk.to_vec(), self.dialect)
                .await
            {
                Ok(statuses) => {
                    let chunk_hashes = hashes.iter().take(chunk.len());
                    for ((query, name), hash) in chunk.iter().zip(statuses).zip(chunk_hashes) {
                        trace!(
                            // FIXME(REA-2168): Use correct dialect.
                            query = %query.statement.display(nom_sql::Dialect::MySQL),
                            name = ?name,
                            "Loaded query status from controller"
                        );
                        if let Some(name) = name {
                            self.view_name_cache.insert(query.clone(), name).await;
                            self.query_status_cache
                                .update_query_migration_state(query, MigrationState::Successful);
                            self.views_checked.insert(*hash);
                        }
                    }
                }
                Err(error) => warn!(%error, "Could not get view statuses from leader"),
            }
            self.metrics.queries_checked.increment(chunk.len() as u64);
        }
    }
}
