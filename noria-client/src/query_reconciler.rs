use crate::backend::{noria_connector, NoriaConnector, PrepareResult};
use crate::query_status_cache::QueryStatusCache;
use crate::upstream_database::NoriaCompare;
use crate::{UpstreamDatabase, UpstreamPrepare};
use metrics::counter;
use noria::{ReadySetError, ReadySetResult};
use noria_client_metrics::recorded;
use tokio::select;
use tracing::{error, info};

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
        // Issues a prepare statement in both noria and mysql and verifies that
        // the resulting schemas are equivalent.
        match self.mirror_prepare(stmt.clone()).await {
            Ok(PrepareResult::Both(noria_result, upstream_result)) => {
                if let noria_connector::PrepareResult::Select {
                    ref schema,
                    ref params,
                    ..
                } = noria_result
                {
                    match upstream_result.meta.compare(schema, params) {
                        Ok(true) => {}
                        Ok(false) => return,
                        Err(e) => {
                            error!("Error comparing schema: {}", e);
                            return;
                        }
                    }
                } else {
                    // Treat the wrong schema type returned as a failure.
                    return;
                }

                counter!(recorded::RECONCILER_ALLOWED, 1);
                self.query_status_cache.set_allow(stmt).await
            }
            // The query failed in Noria
            Ok(PrepareResult::Upstream(_)) => {}
            Ok(PrepareResult::Noria(_)) => error!(
                "Query failed in upstream but succeeded in noria: {}",
                stmt.to_string()
            ),
            Err(e) => {
                // TODO(justin): Consider removing this query from the cache so we never
                // retry it again. It likely is not a valid query.
                error!(%e, "Prepare failed in both noria and upstream: {}", stmt.to_string())
            }
        }
    }

    async fn mirror_prepare(&mut self, stmt: SelectStatement) -> ReadySetResult<PrepareResult<DB>> {
        let noria_res = self.noria.prepare_select(stmt.clone(), 0).await;
        let upstream_res = self.upstream.prepare(stmt.to_string()).await;

        // Convert from results into a `PrepareResult`.
        match (noria_res, upstream_res) {
            (Ok(n), Ok(u)) => Ok(PrepareResult::Both(n, u)),
            (Err(_), Ok(u)) => Ok(PrepareResult::Upstream(u)),
            (Ok(n), Err(_)) => Ok(PrepareResult::Noria(n)),
            (Err(_), Err(_)) => Err(ReadySetError::Internal(
                "Query failed in both noria and upstream".to_string(),
            )),
        }
    }

    fn compare_prepare_result(
        _noria: noria_connector::PrepareResult,
        _upstream: UpstreamPrepare<DB>,
    ) -> bool {
        true
    }
}
