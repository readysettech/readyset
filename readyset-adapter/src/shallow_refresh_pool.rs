use std::borrow::Borrow;
use std::sync::Arc;
use std::time::Duration;

use database_utils::UpstreamConfig;
use readyset_client::metrics::recorded::{
    SHALLOW_REFRESH, SHALLOW_REFRESH_QUERY_TIME, SHALLOW_REFRESH_QUEUE_EXCEEDED,
};
use readyset_client::query::QueryId;
use readyset_data::DfValue;
use readyset_shallow::CacheInsertGuard;
use readyset_sql::ast::SqlIdentifier;
use readyset_util::logging::*;
use tokio::sync::Mutex;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::time::timeout;
use tracing::warn;

use crate::upstream_database::{Refresh, UpstreamDatabase};

const CHANNEL_CAPACITY: usize = 5;
const WORKER_TIMEOUT: Duration = Duration::from_secs(10);

/// Request to refresh a shallow cached query.
#[derive(Debug)]
pub struct ShallowRefreshRequest<V, M>
where
    V: Send + Sync + 'static,
    M: Send + Sync + 'static,
{
    pub(crate) query_id: QueryId,
    pub(crate) path: Vec<SqlIdentifier>,
    pub(crate) query: String,
    pub(crate) cache: CacheInsertGuard<Vec<DfValue>, V>,
    pub(crate) shallow_exec_meta: Option<M>,
}

/// A LIFO pool of shallow refresh workers.
///
/// Workers are spawned lazily up to `worker_limit`. Each worker maintains its own upstream
/// database connection. When idle for `WORKER_TIMEOUT`, a worker shuts down and its connection is
/// dropped. When sending a request, idle workers are preferred (LIFO, most recently active
/// first). If no idle workers are available and the limit is reached, requests are
/// round-robined among busy workers' channels. If all channels are full, the request is dropped.
pub struct ShallowRefreshPool<DB: UpstreamDatabase> {
    inner: Mutex<PoolInner<DB>>,
    upstream_config: UpstreamConfig,
    rt: tokio::runtime::Handle,
    worker_limit: usize,
}

type WorkerSender<DB> = Sender<
    ShallowRefreshRequest<
        <DB as UpstreamDatabase>::CacheEntry,
        <DB as UpstreamDatabase>::ShallowExecMeta,
    >,
>;

struct PoolInner<DB: UpstreamDatabase> {
    /// All worker senders, indexed by worker id. `None` means the slot is free.
    workers: Vec<Option<WorkerSender<DB>>>,
    /// LIFO stack of idle worker indices, most recently active on top.
    idle_stack: Vec<usize>,
    /// Round-robin index for fallback when no workers are idle.
    next_rr: usize,
}

impl<DB: UpstreamDatabase + 'static> ShallowRefreshPool<DB> {
    /// Create a new pool. No workers are spawned until the first request.
    pub fn new(
        rt: &tokio::runtime::Handle,
        config: &UpstreamConfig,
        worker_limit: usize,
    ) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(PoolInner {
                workers: Vec::new(),
                idle_stack: Vec::new(),
                next_rr: 0,
            }),
            upstream_config: config.clone(),
            rt: rt.clone(),
            worker_limit,
        })
    }

    /// Round-robin among all active workers. Returns the request back if all channels are full.
    fn try_send_round_robin(
        self: &Arc<Self>,
        inner: &mut PoolInner<DB>,
        mut req: ShallowRefreshRequest<DB::CacheEntry, DB::ShallowExecMeta>,
    ) -> Option<ShallowRefreshRequest<DB::CacheEntry, DB::ShallowExecMeta>> {
        let len = inner.workers.len();
        for _ in 0..len {
            let idx = inner.next_rr % len;
            inner.next_rr = inner.next_rr.wrapping_add(1);
            if let Some(sender) = &inner.workers[idx] {
                req = match sender.try_send(req) {
                    Ok(()) => return None,
                    Err(mpsc::error::TrySendError::Closed(req)) => {
                        inner.workers[idx] = None;
                        self.spawn_worker_with_request(inner, Some(idx), req);
                        return None;
                    }
                    Err(mpsc::error::TrySendError::Full(req)) => req,
                };
            }
        }
        Some(req)
    }

    fn insert_worker(
        inner: &mut PoolInner<DB>,
        tx: Sender<ShallowRefreshRequest<DB::CacheEntry, DB::ShallowExecMeta>>,
    ) -> usize {
        if let Some(free) = inner.workers.iter().position(|w| w.is_none()) {
            inner.workers[free] = Some(tx);
            free
        } else {
            let idx = inner.workers.len();
            inner.workers.push(Some(tx));
            idx
        }
    }

    /// Spawn a new worker at the given slot (or allocate a new slot), sending `req` as its first
    /// message.
    fn spawn_worker_with_request(
        self: &Arc<Self>,
        inner: &mut PoolInner<DB>,
        slot: Option<usize>,
        req: ShallowRefreshRequest<DB::CacheEntry, DB::ShallowExecMeta>,
    ) {
        let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
        let Ok(()) = tx.try_send(req) else {
            unreachable!("fresh channel with capacity {CHANNEL_CAPACITY}");
        };
        let idx = match slot {
            Some(idx) => {
                inner.workers[idx] = Some(tx);
                idx
            }
            None => Self::insert_worker(inner, tx),
        };
        self.rt.spawn(Self::worker(Arc::clone(self), rx, idx));
    }

    /// Try spawning a new worker if under the limit. Returns the request back if at the limit.
    fn try_spawn_worker(
        self: &Arc<Self>,
        inner: &mut PoolInner<DB>,
        req: ShallowRefreshRequest<DB::CacheEntry, DB::ShallowExecMeta>,
    ) -> Option<ShallowRefreshRequest<DB::CacheEntry, DB::ShallowExecMeta>> {
        let active = inner.workers.iter().filter(|w| w.is_some()).count();
        if active < self.worker_limit {
            self.spawn_worker_with_request(inner, None, req);
            return None;
        }
        Some(req)
    }

    /// Try sending to an idle worker from the LIFO stack. Returns the request back if no idle
    /// worker accepted it.
    fn try_send_idle(
        self: &Arc<Self>,
        inner: &mut PoolInner<DB>,
        mut req: ShallowRefreshRequest<DB::CacheEntry, DB::ShallowExecMeta>,
    ) -> Option<ShallowRefreshRequest<DB::CacheEntry, DB::ShallowExecMeta>> {
        while let Some(idx) = inner.idle_stack.pop() {
            if let Some(sender) = &inner.workers[idx] {
                req = match sender.try_send(req) {
                    Ok(()) => return None,
                    Err(mpsc::error::TrySendError::Closed(req)) => {
                        inner.workers[idx] = None;
                        req
                    }
                    Err(mpsc::error::TrySendError::Full(req)) => req,
                };
            }
        }
        Some(req)
    }

    /// Send a refresh request to a worker.
    pub async fn send(
        self: &Arc<Self>,
        req: ShallowRefreshRequest<DB::CacheEntry, DB::ShallowExecMeta>,
    ) {
        let query_id = req.query_id;
        let mut inner = self.inner.lock().await;

        // Try idle stack (LIFO, recently used connections first)
        if let Some(req) = self.try_send_idle(&mut inner, req) {
            // Spawn new worker if under limit
            if let Some(req) = self.try_spawn_worker(&mut inner, req) {
                // Round-robin among all active workers
                if let Some(_req) = self.try_send_round_robin(&mut inner, req) {
                    // All channels full, drop request
                    metrics::counter!(SHALLOW_REFRESH_QUEUE_EXCEEDED).increment(1);
                    rate_limit(true, ADAPTER_SHALLOW_REFRESH_SEND_REQUEST, || {
                        warn!("All shallow refresh workers busy, dropping request");
                    });
                    return;
                }
            }
        }

        metrics::counter!(SHALLOW_REFRESH, "query_id" => query_id.to_string()).increment(1);
    }

    /// Spawn a send as a background task (for use from sync contexts like callbacks).
    pub fn spawn_send(
        self: &Arc<Self>,
        req: ShallowRefreshRequest<DB::CacheEntry, DB::ShallowExecMeta>,
    ) {
        let pool = Arc::clone(self);
        self.rt.spawn(async move {
            pool.send(req).await;
        });
    }

    async fn mark_idle(pool: &Arc<Self>, idx: usize) {
        pool.inner.lock().await.idle_stack.push(idx);
    }

    async fn remove_worker(pool: &Arc<Self>, idx: usize) {
        let mut inner = pool.inner.lock().await;
        inner.workers[idx] = None;
        inner.idle_stack.retain(|&i| i != idx);
    }

    async fn worker(
        pool: Arc<Self>,
        mut rx: Receiver<ShallowRefreshRequest<DB::CacheEntry, DB::ShallowExecMeta>>,
        idx: usize,
    ) {
        let mut upstream: Option<DB> = None;
        let mut reset = false;

        loop {
            let request = match timeout(WORKER_TIMEOUT, rx.recv()).await {
                Ok(Some(req)) => req,
                Ok(None) | Err(_) => break,
            };

            if upstream.is_none() || reset {
                reset = false;
                match DB::connect(pool.upstream_config.clone(), None, None).await {
                    Ok(conn) => upstream = Some(conn),
                    Err(e) => {
                        rate_limit(true, ADAPTER_SHALLOW_REFRESH_OPEN, || {
                            warn!(
                                error = %e,
                                "Failed to create upstream connection for shallow refresh",
                            )
                        });
                        Self::mark_idle(&pool, idx).await;
                        continue;
                    }
                }
            }

            let Some(ref mut conn) = upstream else {
                Self::mark_idle(&pool, idx).await;
                continue;
            };

            let ShallowRefreshRequest {
                query_id,
                path,
                query,
                cache,
                shallow_exec_meta,
            } = request;

            match conn.set_schema_search_path(&path).await {
                Ok(()) => {}
                Err(e) => {
                    rate_limit(true, ADAPTER_SHALLOW_REFRESH_SET_SCHEMA, || {
                        warn!(
                            error = %e,
                            path = ?path,
                            "Failed to set schema path for refresh",
                        )
                    });
                    reset = true;
                    Self::mark_idle(&pool, idx).await;
                    continue;
                }
            }

            let query_start = std::time::Instant::now();
            let result = match shallow_exec_meta {
                Some(ref exec_meta) => conn.query_ext(&query, exec_meta.borrow()).await,
                None => conn.query(&query).await,
            };

            let result = match result {
                Ok(result) => {
                    let query_time = query_start.elapsed();
                    metrics::histogram!(
                        SHALLOW_REFRESH_QUERY_TIME,
                        "query_id" => query_id.to_string()
                    )
                    .record(query_time.as_micros() as f64);
                    result
                }
                Err(e) => {
                    rate_limit(true, ADAPTER_SHALLOW_REFRESH_RUN, || {
                        warn!(
                            error = %e,
                            cache = %query_id,
                            "Failed to refresh cached query",
                        )
                    });
                    reset = true;
                    Self::mark_idle(&pool, idx).await;
                    continue;
                }
            };

            if let Err(e) = result.refresh(cache).await {
                rate_limit(true, ADAPTER_SHALLOW_REFRESH_READ, || {
                    warn!(
                        error = %e,
                        cache = %query_id,
                        "Failed to read results for cached query",
                    )
                });
            }

            Self::mark_idle(&pool, idx).await;
        }

        Self::remove_worker(&pool, idx).await;
    }
}
