//! Query Sampler
//!
//! The Query Sampler is a background task that validates that ReadySet returns the
//! same results as the upstream database for a sample of executed queries.
//!
//! High-level flow per sampled query:
//! - The sampler receives `(QueryExecutionEvent, String)` from a bounded channel
//!   and randomly accepts it based on `sample_rate`.
//! - It executes the query against ReadySet without creating caches, and separately
//!   against the upstream database.
//! - Both result sets are converted into `Vec<Vec<DfValue>>` and normalized into a
//!   canonical form before hashing, so semantically equal results produce equal hashes.
//! - If hashes differ, the sampler retries after `retry_delay` until either results match
//!   or `max_retry_attempts` is reached. On the final attempt, it emits a mismatch metric
//!   and logs the differing rows for inspection.
//!
//! Normalization and hashing:
//! - If the query has no ORDER BY, rows are sorted using a normalized comparison so that
//!   result ordering does not affect equality.
//! - Each `DfValue` is hashed using a tagged representation to disambiguate between text,
//!   raw bytes, and other types, avoiding cross-type collisions.
//!
//! Rate limiting:
//! - To bound sampler load, processing is gated by a simple QPS limiter.
//!
//! Scheduling and retries:
//! - Initial attempts are processed immediately; mismatches are re-enqueued into an internal
//!   retry queue with timestamps. The sampler prioritizes due retries so they are not starved
//!   by incoming samples.
//! - If the upstream hash changes across attempts, the query is treated as volatile and the retry
//!   sequence is abandoned to avoid false mismatches.
//!
//! Metrics and observability (non-exhaustive):
//! - `QUERY_SAMPLER_QUERIES_SAMPLED`: Count of queries sampled.
//! - `QUERY_SAMPLER_QUERIES_MISMATCHED`: Count of mismatches after retries.
//! - `QUERY_SAMPLER_RETRY_QUEUE_LEN`: Gauge of retry queue length.
//! - `QUERY_SAMPLER_RETRY_QUEUE_FULL`: Dropped retries due to capacity.
//! - `QUERY_SAMPLER_MAX_QPS_HIT`: Rate limiter activations.
//! - `QUERY_SAMPLER_RECONNECTS`: Upstream reconnections.
//!
//! Execution model:
//! - The sampler runs on a dedicated thread with a single-threaded Tokio runtime and performs
//!   all operations serially. If parallelism is introduced in the future, the rate limiter and
//!   shared state must be adapted accordingly.
//!

use metrics::{counter, gauge, Label};
use std::hash::{Hash, Hasher};
use std::{
    cmp::Ordering,
    collections::{HashMap, VecDeque},
    sync::{atomic::AtomicUsize, Arc},
    time::Duration,
};
use tokio::time::Instant as TokioInstant;
use tokio::{
    select,
    sync::{
        mpsc::{Receiver, Sender},
        RwLock,
    },
};
use tracing::{debug, info, trace, warn};
use xxhash_rust::xxh3::Xxh3;

use database_utils::tls::ServerCertVerification;
use database_utils::{
    DatabaseConnection, DatabaseError, DatabaseURL, QueryResults, QueryableConnection,
    UpstreamConfig,
};
use readyset_client::metrics::recorded::QUERY_SAMPLER_QUEUE_LEN;
use readyset_client::{
    metrics::recorded::{
        QUERY_SAMPLER_MAX_QPS_HIT, QUERY_SAMPLER_QUERIES_MISMATCHED, QUERY_SAMPLER_QUERIES_SAMPLED,
        QUERY_SAMPLER_RECONNECTS, QUERY_SAMPLER_RETRY_QUEUE_FULL, QUERY_SAMPLER_RETRY_QUEUE_LEN,
    },
    ReadySetHandle, View, ViewCreateRequest,
};
use readyset_client_metrics::{QueryExecutionEvent, SqlQueryType};
use readyset_data::DfValue;
use readyset_dataflow::Readers;
use readyset_errors::ReadySetError;
use readyset_server::worker::readers::{retry_misses, Ack, BlockingRead, ReadRequestHandler};
use readyset_sql::{
    ast::{Relation, SqlIdentifier, SqlQuery},
    Dialect,
};
use readyset_sql_parsing::{parse_query_with_config, ParsingPreset};
use readyset_sql_passes::adapter_rewrites::AdapterRewriteParams;
use readyset_util::{
    logging::{rate_limit, SAMPLER_LOG_SAMPLER},
    shared_cache::LocalCache,
    shutdown::ShutdownReceiver,
};

use crate::{
    backend::noria_connector::{ExecuteSelectContext, NoriaConnector, QueryResult, ReadBehavior},
    DeploymentMode,
};

/// Tag byte used when hashing a value that normalizes to UTF-8 text
const TAG_NORMALIZED_TEXT: u8 = 0x01;
/// Tag byte used when hashing any other value via its `Hash` implementation
const TAG_FALLBACK: u8 = 0xff;

/// Configuration for the background query sampler
#[derive(Debug)]
pub struct SamplerConfig {
    /// Probability [0.0, 1.0] of sampling an enqueued query
    pub sample_rate: f64,
    /// Maximum queue size (bounded channel capacity)
    pub queue_capacity: usize,
    /// Timeout for upstream queries
    pub upstream_timeout: Duration,
    /// Maximum QPS for the sampler (applies to processed sampled queries)
    pub max_qps: u64,
    /// Delay between retry attempts for mismatched queries
    pub retry_delay: Duration,
    /// Maximum number of retry attempts before reporting a mismatch
    pub max_retry_attempts: u8,
}

impl Default for SamplerConfig {
    fn default() -> Self {
        Self {
            sample_rate: 0.01,
            queue_capacity: 1024,
            upstream_timeout: Duration::from_secs(1),
            max_qps: 100,
            retry_delay: Duration::from_secs(10),
            max_retry_attempts: 6,
        }
    }
}

pub struct Sampler {
    config: SamplerConfig,
    rx: Receiver<(QueryExecutionEvent, String)>,
    noria: NoriaConnector,
    upstream_conn: Option<DatabaseConnection>,
    upstream_config: UpstreamConfig,
    parse_dialect: Dialect,
    parsing_preset: ParsingPreset,
    shutdown_recv: ShutdownReceiver,
    last_executed_at: Option<std::time::Instant>,
    retry_queue: VecDeque<(TokioInstant, Entry)>,
}

#[derive(Clone)]
struct Entry {
    /// The query to retry
    q: String,
    /// The event that triggered the retry
    event: QueryExecutionEvent,
    /// The number of attempts made to retry the query
    attempts: u8,
    /// The hash of the last upstream result
    last_up_hash: Option<u64>,
}

/// Build a bounded channel for sampler input
type SamplerTx = Sender<(QueryExecutionEvent, String)>;
type SamplerRx = Receiver<(QueryExecutionEvent, String)>;

fn build_channel(config: &SamplerConfig) -> (SamplerTx, SamplerRx) {
    tokio::sync::mpsc::channel(config.queue_capacity)
}

async fn connect(config: &UpstreamConfig) -> Option<DatabaseConnection> {
    let url: DatabaseURL = if let Some(url) = &config.upstream_db_url {
        match url.parse() {
            Ok(url) => url,
            Err(error) => {
                warn!(%error, "Failed to parse sampler upstream URL");
                return None;
            }
        }
    } else {
        return None;
    };

    let verification = match ServerCertVerification::from(config).await {
        Ok(verification) => verification,
        Err(error) => {
            warn!(%error, "Failed to initialize TLS");
            return None;
        }
    };

    debug!("Establishing sampler upstream connection");
    let conn = match url.connect(verification).await {
        Ok(conn) => conn,
        Err(error) => {
            warn!(%error, "Failed to establish sampler upstream connection");
            return None;
        }
    };
    Some(conn)
}

/// Builder responsible for creating a sampler, a channel, a connection to upstream and a handler to Readyset
#[allow(clippy::too_many_arguments)]
pub async fn sampler_builder(
    sampler_cfg: SamplerConfig,
    rs_handle: ReadySetHandle,
    auto_increments: Arc<RwLock<HashMap<Relation, AtomicUsize>>>,
    view_name_cache: LocalCache<ViewCreateRequest, Relation>,
    view_cache: LocalCache<Relation, View>,
    adapter_rewrite_params: AdapterRewriteParams,
    schema_search_path: Vec<SqlIdentifier>,
    upstream_config: UpstreamConfig,
    options: DeploymentMode,
    readers: Readers,
    parse_dialect: Dialect,
    parsing_preset: ParsingPreset,
    expr_dialect: readyset_data::Dialect,
    shutdown_rx: ShutdownReceiver,
) -> (
    Option<Sampler>,
    Option<Sender<(QueryExecutionEvent, String)>>,
) {
    if sampler_cfg.max_qps == 0 {
        return (None, None);
    }

    let (global_sampler_tx, global_sampler_rx) = build_channel(&sampler_cfg);

    // Build a local read handler for the sampler connector if embedded readers enabled
    let read_handler = if options.has_reader_nodes() {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<(BlockingRead, Ack)>();
        tokio::spawn(retry_misses(rx));
        Some(ReadRequestHandler::new(
            readers.clone(),
            tx,
            Duration::from_secs(sampler_cfg.upstream_timeout.as_secs()),
        ))
    } else {
        None
    };

    // Build sampler connector
    let rs_connector = NoriaConnector::new_with_local_reads(
        rs_handle,
        auto_increments,
        view_name_cache,
        view_cache,
        ReadBehavior::NonBlocking,
        read_handler,
        expr_dialect,
        parse_dialect,
        schema_search_path,
        adapter_rewrite_params,
    )
    .await;

    // Connect to upstream once if configured
    let upstream_conn = connect(&upstream_config).await;
    let sampler = Sampler::new(
        sampler_cfg,
        rs_connector,
        upstream_conn,
        upstream_config,
        parse_dialect,
        parsing_preset,
        shutdown_rx.clone(),
        global_sampler_rx,
    );
    (Some(sampler), Some(global_sampler_tx))
}

fn get_cache_name(event: &QueryExecutionEvent) -> String {
    match &event.readyset_event {
        Some(readyset_client_metrics::ReadysetExecutionEvent::CacheRead { cache_name, .. }) => {
            cache_name.name.to_string()
        }
        _ => "unknown".to_string(),
    }
}

impl Sampler {
    fn retry_due(&self) -> bool {
        if let Some((ts, _)) = self.retry_queue.front() {
            *ts <= TokioInstant::now()
        } else {
            false
        }
    }

    async fn drain_due_retries(&mut self) {
        let now = TokioInstant::now();
        while let Some((ts, _)) = self.retry_queue.front() {
            if *ts > now {
                break;
            }
            if let Some((_ts, entry)) = self.retry_queue.pop_front() {
                gauge!(QUERY_SAMPLER_RETRY_QUEUE_LEN).decrement(1);
                self.enforce_rate_limit().await;
                self.process_entry(entry).await;
            }
        }
    }

    async fn enforce_rate_limit(&mut self) {
        if self.config.max_qps == 0 {
            return;
        }
        let min_interval = std::time::Duration::from_secs_f64(1.0 / self.config.max_qps as f64);
        if let Some(last) = self.last_executed_at {
            let now = std::time::Instant::now();
            if let Some(remaining) = min_interval.checked_sub(now.saturating_duration_since(last)) {
                if !remaining.is_zero() {
                    counter!(QUERY_SAMPLER_MAX_QPS_HIT).increment(1);
                    tokio::time::sleep(remaining).await;
                }
            }
        }
        self.last_executed_at = Some(std::time::Instant::now());
    }

    async fn query_readyset(
        &mut self,
        q: &String,
        event: &mut QueryExecutionEvent,
    ) -> Result<QueryResult<'_>, ReadySetError> {
        let res = match event.sql_type {
            SqlQueryType::Read => {
                let mut stmt = if let Some(s) = event.query.as_deref().and_then(|q| match q {
                    SqlQuery::Select(stmt) => Some(stmt.clone()),
                    _ => None,
                }) {
                    s
                } else {
                    match parse_query_with_config(
                        self.parsing_preset.into_config(),
                        self.parse_dialect,
                        q,
                    ) {
                        Ok(SqlQuery::Select(stmt)) => stmt,
                        _ => return Err(ReadySetError::UnparseableQuery(q.clone())),
                    }
                };

                let res = match readyset_sql_passes::adapter_rewrites::process_query(
                    &mut stmt,
                    self.noria.rewrite_params(),
                ) {
                    Ok(processed) => {
                        let ctx = ExecuteSelectContext::AdHoc {
                            statement: &stmt,
                            create_if_missing: false,
                            processed_query_params: processed,
                        };
                        match tokio::time::timeout(
                            std::time::Duration::from_millis(1000),
                            self.noria.execute_select(ctx, event),
                        )
                        .await
                        {
                            Ok(res) => res,
                            Err(e) => {
                                rate_limit(true, SAMPLER_LOG_SAMPLER, || {
                                    warn!("Sampler Readyset read timeout: {:?}", e);
                                });
                                Err(readyset_errors::rpc_err_no_downcast(
                                    "Sampler Readyset read",
                                    ReadySetError::Internal("timeout".into()),
                                ))
                            }
                        }
                    }
                    Err(e) => Err(e),
                };
                res
            }
            _ => Err(ReadySetError::InvalidQuery(q.clone())),
        };
        res
    }

    async fn query_upstream(&mut self, q: &str) -> Result<QueryResults, DatabaseError> {
        let fut = async {
            match self.upstream_conn.as_mut() {
                Some(conn) => conn.query(q).await,
                None => Err(DatabaseError::UpstreamConnectionNone),
            }
        };
        match tokio::time::timeout(self.config.upstream_timeout, fut).await {
            Ok(res) => res,
            Err(_) => {
                rate_limit(true, SAMPLER_LOG_SAMPLER, || {
                    warn!("Sampler upstream read timeout");
                });
                Err(DatabaseError::UpstreamQueryTimeout)
            }
        }
    }

    fn schedule_retry(&mut self, entry: Entry) {
        if self.retry_queue.len() >= self.config.queue_capacity {
            counter!(QUERY_SAMPLER_RETRY_QUEUE_FULL).increment(1);
            return;
        }
        let ts = TokioInstant::now() + self.config.retry_delay;
        self.retry_queue.push_back((ts, entry));
        gauge!(QUERY_SAMPLER_RETRY_QUEUE_LEN).increment(1);
    }

    /// Process a query entry: execute on Readyset, execute on upstream, compare normalized
    /// results, and handle initial attempt vs retries uniformly.
    async fn process_entry(&mut self, mut entry: Entry) {
        // Check if the query has an ORDER BY clause. If it doesn't,
        // we can sort the rows to make the hash more stable.
        let has_order_by = matches!(
            entry.event.query.as_deref(),
            Some(SqlQuery::Select(stmt)) if stmt.order.is_some()
        );

        // Readyset execution (materialize rows for optional tracing on mismatch)
        let rs_res = self.query_readyset(&entry.q, &mut entry.event).await;
        let (rs_hash_opt, rs_rows_opt) = match rs_res {
            Ok(rs_result) => match rs_result {
                crate::backend::noria_connector::QueryResult::Select { rows, .. } => {
                    let mut it = rows;
                    let mut rows_vec = Vec::new();
                    while let Some(r) = streaming_iterator::StreamingIterator::next(&mut it) {
                        rows_vec.push(r.to_vec());
                    }
                    let hash = Some(normalize_and_hash(&mut rows_vec, has_order_by));
                    (hash, Some(rows_vec))
                }
                _ => (None, None),
            },
            Err(e) => {
                debug!(error = %e, "Sampler Readyset execution error");
                (None, None)
            }
        };

        // Count only the initial attempt as a sampled query
        if entry.attempts == 0 {
            counter!(
                QUERY_SAMPLER_QUERIES_SAMPLED,
                vec![Label::new("cache_name", get_cache_name(&entry.event))]
            )
            .increment(1);
        }

        // Upstream execution (materialize rows for optional tracing on mismatch)
        let up_res = self.query_upstream(&entry.q).await;
        let (up_hash_opt, up_rows_opt) = match up_res {
            Ok(up_rows) => {
                let mut rows_vec: Vec<Vec<DfValue>> = match <Vec<Vec<DfValue>>>::try_from(up_rows) {
                    Ok(v) => v,
                    Err(e) => {
                        debug!(error = %e, "Sampler upstream row conversion error");
                        Vec::new()
                    }
                };
                let hash = Some(normalize_and_hash(&mut rows_vec, has_order_by));
                (hash, Some(rows_vec))
            }
            Err(e) => {
                debug!(error = %e, "Sampler upstream error");
                self.reconnect_if_closed().await;
                (None, None)
            }
        };

        self.validate_results(rs_hash_opt, up_hash_opt, rs_rows_opt, up_rows_opt, entry)
            .await;
    }

    async fn validate_results(
        &mut self,
        rs_hash_opt: Option<u64>,
        up_hash_opt: Option<u64>,
        rs_rows_opt: Option<Vec<Vec<DfValue>>>,
        up_rows_opt: Option<Vec<Vec<DfValue>>>,
        entry: Entry,
    ) {
        match (rs_hash_opt, up_hash_opt) {
            (Some(rs_hash), Some(up_hash)) => {
                // If retrying, detect volatility in upstream results
                if let Some(prev_up) = entry.last_up_hash {
                    if prev_up != up_hash {
                        debug!(
                            query = %entry.q,
                            prev_up_hash = prev_up,
                            up_hash,
                            "Sampler upstream hash changed between attempts; treating as volatile and skipping"
                        );
                        return;
                    }
                }

                if rs_hash != up_hash {
                    self.handle_mismatch(
                        entry,
                        Some(up_hash),
                        Some(rs_hash),
                        rs_rows_opt,
                        up_rows_opt,
                    )
                    .await;
                }
            }
            (Some(rs_hash), None) => {
                self.handle_mismatch(entry, None, Some(rs_hash), rs_rows_opt, up_rows_opt)
                    .await;
            }
            (None, Some(up_hash)) => {
                self.handle_mismatch(entry, Some(up_hash), None, rs_rows_opt, up_rows_opt)
                    .await;
            }
            (None, None) => {
                self.handle_mismatch(entry, None, None, rs_rows_opt, up_rows_opt)
                    .await;
            }
        }
    }

    async fn handle_mismatch(
        &mut self,
        mut entry: Entry,
        up_hash: Option<u64>,
        rs_hash: Option<u64>,
        rs_rows_opt: Option<Vec<Vec<DfValue>>>,
        up_rows_opt: Option<Vec<Vec<DfValue>>>,
    ) {
        entry.attempts = entry.attempts.saturating_add(1);
        entry.last_up_hash = up_hash;
        if entry.attempts >= self.config.max_retry_attempts {
            counter!(
                QUERY_SAMPLER_QUERIES_MISMATCHED,
                vec![Label::new("cache_name", get_cache_name(&entry.event))]
            )
            .increment(1);
            rate_limit(true, SAMPLER_LOG_SAMPLER, || {
                warn!(
                    query = %entry.q,
                    rs_hash,
                    up_hash,
                    "Sampler mismatch after retries: normalized result hash differs between Readyset and upstream"
                );
            });
            trace!(query = %entry.q, rs_rows = ?rs_rows_opt, up_rows = ?up_rows_opt, "Sampler mismatch after retries: normalized result hash differs between Readyset and upstream");
        } else {
            self.schedule_retry(entry);
        }
    }
    /// Check if upstream connection is closed and create a new one if necessary.
    async fn reconnect_if_closed(&mut self) {
        if self
            .upstream_conn
            .as_ref()
            .map(|c| c.is_closed())
            .unwrap_or(false)
        {
            debug!("Sampler upstream connection is closed, creating a new connection");
            counter!(QUERY_SAMPLER_RECONNECTS).increment(1);
            self.upstream_conn = connect(&self.upstream_config).await;
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: SamplerConfig,
        noria: NoriaConnector,
        upstream_conn: Option<DatabaseConnection>,
        upstream_config: UpstreamConfig,
        parse_dialect: Dialect,
        parsing_preset: ParsingPreset,
        shutdown_recv: ShutdownReceiver,
        rx: Receiver<(QueryExecutionEvent, String)>,
    ) -> Self {
        Self {
            config,
            rx,
            noria,
            upstream_conn,
            upstream_config,
            parse_dialect,
            parsing_preset,
            shutdown_recv,
            last_executed_at: None,
            retry_queue: VecDeque::new(),
        }
    }

    /// Run the sampler loop: randomly sample queries and compare Readyset vs upstream results.
    /// Differences are logged as warnings.
    pub async fn run_sampler(&mut self) {
        info!("Starting query sampler with config: {:?}", self.config);
        loop {
            select! {
                biased;
                _ = self.shutdown_recv.recv() => {
                    debug!("Query Sampler task shutting down after signal received.");
                    break;
                }
                // Check retries first so they are not starved by a busy rx
                // TODO: If recv is empty, we will not drain retries.
                _ = async {}, if self.retry_due() => {
                    self.drain_due_retries().await;
                }
                q = self.rx.recv() => {
                    gauge!(QUERY_SAMPLER_QUEUE_LEN).set(self.rx.len() as f64);
                    if let Some((event, q)) = q {
                        if rand::random::<f64>() > self.config.sample_rate {
                            continue;
                        }
                        // Rate limit sampled query processing
                        self.enforce_rate_limit().await;
                        let entry = Entry {
                            q,
                            event,
                            attempts: 0,
                            last_up_hash: None,
                        };
                        self.process_entry(entry).await;
                    }
                }
            }
        }
    }
}

/// Compute a stable hash of rows of `DfValue`s, optionally sorting rows when there is no
/// explicit ORDER BY. Values are hashed using a tagged, normalized representation to
/// disambiguate between types that could otherwise collide (e.g., text vs bytes).
fn normalize_and_hash(rows: &mut [Vec<DfValue>], has_order_by: bool) -> u64 {
    if !has_order_by {
        rows.sort_unstable_by(|a, b| cmp_rows(a, b));
    }

    let mut hasher = Xxh3::new();
    hasher.write_u64(rows.len() as u64);
    for row in rows.iter() {
        hasher.write_u64(row.len() as u64);
        for v in row.iter() {
            hash_dfvalue_normalized(&mut hasher, v);
        }
    }
    hasher.finish()
}

/// Compare two rows using normalized `DfValue` comparison semantics.
fn cmp_rows(a: &[DfValue], b: &[DfValue]) -> Ordering {
    for (x, y) in a.iter().zip(b.iter()) {
        let o = cmp_dfvalue_normalized(x, y);
        if o != Ordering::Equal {
            return o;
        }
    }
    a.len().cmp(&b.len())
}

/// Return a `&str` view for `DfValue` if the value is textual or UTF-8 decodable bytes.
/// Trailing spaces are trimmed to align with SQL text comparison semantics.
fn as_text_if_utf8(v: &DfValue) -> Option<&str> {
    match v {
        DfValue::Text(t) => Some(t.as_str().trim_end_matches(' ')),
        DfValue::TinyText(tt) => Some(tt.as_str().trim_end_matches(' ')),
        DfValue::ByteArray(bytes) => std::str::from_utf8(bytes.as_ref())
            .ok()
            .map(|s| s.trim_end_matches(' ')),
        _ => None,
    }
}

/// Compare two `DfValue`s with normalization:
/// - If both are text or UTF-8 bytes, compare as strings with trailing spaces trimmed.
/// - Otherwise, fall back to the standard `DfValue` ordering.
fn cmp_dfvalue_normalized(a: &DfValue, b: &DfValue) -> Ordering {
    if let (Some(sa), Some(sb)) = (as_text_if_utf8(a), as_text_if_utf8(b)) {
        return sa.cmp(sb);
    }
    a.cmp(b)
}

/// Hash a single `DfValue` using a tagged representation to avoid cross-type collisions.
/// The encoding is `[tag][len][bytes]` for variable-length representations:
/// - `TAG_NORMALIZED_TEXT` for normalized UTF-8 text (trims trailing spaces)
/// - `TAG_BYTE_ARRAY` for raw byte arrays
/// - `TAG_FALLBACK` followed by the value's `Hash` otherwise
fn hash_dfvalue_normalized<H: Hasher>(h: &mut H, v: &DfValue) {
    if let Some(s) = as_text_if_utf8(v) {
        h.write_u8(TAG_NORMALIZED_TEXT);
        h.write_u64(s.len() as u64);
        h.write(s.as_bytes());
        return;
    }
    h.write_u8(TAG_FALLBACK);
    v.hash(h);
}
