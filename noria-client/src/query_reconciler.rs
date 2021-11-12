use crate::backend::{noria_connector, NoriaConnector};
use crate::query_status_cache::QueryStatusCache;
use crate::upstream_database::NoriaCompare;
use crate::{UpstreamDatabase, UpstreamPrepare};
use metrics::counter;
use noria::ReadySetResult;
use noria_client_metrics::recorded;
use tokio::select;
use tracing::{error, info, instrument, warn};

use nom_sql::SelectStatement;
use std::sync::Arc;

pub struct QueryReconciler<DB> {
    /// Connection used to issue prepare requests to Noria.
    noria: NoriaConnector,

    /// Connector used to issue prepares to the upstream db.
    upstream: DB,

    /// The query status cache is polled on a regular interval to
    /// determine which queries require processing.
    query_status_cache: Arc<QueryStatusCache>,

    /// The minimum interval between subsequent polls to the query
    /// status cache. In practice it may be longer if the queries
    /// that require processing take longer than `min_poll_interval`.
    min_poll_interval: std::time::Duration,

    /// Reciever to return the broadcast signal on.
    shutdown_recv: tokio::sync::broadcast::Receiver<()>,
}

impl<DB> QueryReconciler<DB>
where
    DB: UpstreamDatabase,
{
    pub fn new(
        noria: NoriaConnector,
        upstream: DB,
        query_status_cache: Arc<QueryStatusCache>,
        min_poll_interval: std::time::Duration,
        shutdown_recv: tokio::sync::broadcast::Receiver<()>,
    ) -> QueryReconciler<DB> {
        QueryReconciler {
            noria,
            upstream,
            query_status_cache,
            min_poll_interval,
            shutdown_recv,
        }
    }

    #[instrument(level = "warn", name = "reconciler", skip(self))]
    pub async fn run(&mut self) -> ReadySetResult<()> {
        let mut interval = tokio::time::interval(self.min_poll_interval);
        loop {
            select! {
                _ = interval.tick() => {
                    let to_process = self.query_status_cache.needs_processing().await;

                    for q in &to_process {
                        self.reconcile_query(q).await
                    }

                    counter!(recorded::RECONCILER_PROCESSED, to_process.len() as u64);
                }
                _ = self.shutdown_recv.recv() => {
                    info!("Query reconciler shutting down after shut down signal received");
                    break;
                }
            }
        }
        Ok(())
    }

    async fn reconcile_query(&mut self, stmt: &SelectStatement) {
        let upstream_result = self.upstream.prepare(stmt.to_string()).await;

        if let Err(u) = upstream_result {
            error!(
                error = %u,
                query = %stmt,
                "Query failed to be prepared against upstream",
            );
            return;
        }

        // Check if we can successfully prepare against noria as well.
        match self.noria.prepare_select(stmt.clone(), 0).await {
            Ok(n) => {
                if cfg!(feature = "reconciler-schema-check") {
                    if let noria_connector::PrepareResult::Select {
                        ref schema,
                        ref params,
                        ..
                    } = n
                    {
                        // Upstream is an Ok value as we check for the error above.
                        #[allow(clippy::unwrap_used)]
                        if let Err(e) = upstream_result.unwrap().meta.compare(schema, params) {
                            warn!(error = %e, query = %stmt, "Query compare failed");
                            return;
                        }
                    } else {
                        return;
                    }
                }
                counter!(recorded::RECONCILER_ALLOWED, 1);
                self.query_status_cache.set_successful_migration(stmt).await;
            }
            Err(e) if e.caused_by_unsupported() => {
                error!(error = %e,
                        query = %stmt,
                        "Select query is unsupported in ReadySet");
                self.query_status_cache.set_unsupported_query(stmt).await;
            }
            // Errors that were not caused by unsupported may be transient, do nothing
            // so we may retry query reconciliation on this query.
            _ => {}
        }
    }

    fn compare_prepare_result(
        _noria: noria_connector::PrepareResult,
        _upstream: UpstreamPrepare<DB>,
    ) -> bool {
        true
    }
}
