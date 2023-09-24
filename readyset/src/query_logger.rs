use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use metrics::{register_counter, register_histogram, Counter, Histogram, SharedString};
use nom_sql::{DialectDisplay, SqlQuery};
use readyset_client::query::QueryId;
use readyset_client_metrics::{
    recorded, DatabaseType, EventType, QueryExecutionEvent, QueryLogMode, SqlQueryType,
};
use readyset_sql_passes::anonymize::anonymize_literals;
use readyset_util::shutdown::ShutdownReceiver;
use tokio::select;
use tokio::sync::mpsc::UnboundedReceiver;
use tracing::{info, info_span};

pub(crate) struct QueryLogger {
    per_id_metrics: BTreeMap<QueryId, QueryMetrics>,
    per_query_metrics: HashMap<Arc<SqlQuery>, QueryMetrics>,
    parse_error_count: Counter,
    set_disallowed_count: Counter,
    view_not_found_count: Counter,
    rpc_error_count: Counter,
}

struct QueryMetrics {
    query: SharedString,
    query_id: Option<SharedString>,
    num_keys: Counter,
    cache_misses: Counter,
    cache_keys_missed: Counter,
    histograms: BTreeMap<(EventType, SqlQueryType), QueryHistograms>,
    counters: BTreeMap<(EventType, SqlQueryType), QueryCounters>,
}

#[derive(Default)]
struct QueryHistograms {
    parse_time: Option<Histogram>,
    upstream_exe_time: Option<Histogram>,
    readyset_exe_time: Option<Histogram>,
}

// this counter is for use in Day 1, demo mode
#[derive(Default)]
struct QueryCounters {
    upstream_exe_count: Option<Counter>,
    readyset_exe_count: Option<Counter>,
}

impl QueryMetrics {
    fn parse_histogram(
        &mut self,
        kind: (EventType, SqlQueryType),
        mode: &QueryLogMode,
    ) -> &mut Histogram {
        self.histograms
            .entry(kind)
            .or_default()
            .parse_time
            .get_or_insert_with(|| {
                let mut labels = vec![
                    ("event_type", SharedString::from(kind.0)),
                    ("query_type", SharedString::from(kind.1)),
                ];

                if mode.is_verbose() {
                    labels.push(("query", self.query.clone()));
                }

                if let Some(id) = &self.query_id {
                    labels.push(("query_id", id.clone()));
                }

                register_histogram!(recorded::QUERY_LOG_PARSE_TIME, &labels)
            })
    }

    fn readyset_histogram(
        &mut self,
        kind: (EventType, SqlQueryType),
        mode: &QueryLogMode,
    ) -> &mut Histogram {
        self.histograms
            .entry(kind)
            .or_default()
            .readyset_exe_time
            .get_or_insert_with(|| {
                let mut labels = vec![
                    ("event_type", SharedString::from(kind.0)),
                    ("query_type", SharedString::from(kind.1)),
                    ("database_type", SharedString::from(DatabaseType::ReadySet)),
                ];

                if mode.is_verbose() {
                    labels.push(("query", self.query.clone()));
                }

                if let Some(id) = &self.query_id {
                    labels.push(("query_id", id.clone()));
                }

                register_histogram!(recorded::QUERY_LOG_EXECUTION_TIME, &labels)
            })
    }

    fn upstream_histogram(
        &mut self,
        kind: (EventType, SqlQueryType),
        mode: &QueryLogMode,
    ) -> &mut Histogram {
        self.histograms
            .entry(kind)
            .or_default()
            .upstream_exe_time
            .get_or_insert_with(|| {
                let mut labels = vec![
                    ("event_type", SharedString::from(kind.0)),
                    ("query_type", SharedString::from(kind.1)),
                    ("database_type", SharedString::from(DatabaseType::MySql)),
                ];

                if mode.is_verbose() {
                    labels.push(("query", self.query.clone()));
                }

                if let Some(id) = &self.query_id {
                    labels.push(("query_id", id.clone()));
                }

                register_histogram!(recorded::QUERY_LOG_EXECUTION_TIME, &labels)
            })
    }

    fn readyset_counter(&mut self, kind: (EventType, SqlQueryType)) -> &mut Counter {
        self.counters
            .entry(kind)
            .or_default()
            .readyset_exe_count
            .get_or_insert_with(|| {
                let mut labels = vec![
                    ("query", self.query.clone()),
                    ("event_type", SharedString::from(kind.0)),
                    ("query_type", SharedString::from(kind.1)),
                    ("database_type", SharedString::from(DatabaseType::ReadySet)),
                ];

                if let Some(id) = &self.query_id {
                    labels.push(("query_id", id.clone()));
                }

                register_counter!(recorded::QUERY_LOG_EXECUTION_COUNT, &labels)
            })
    }

    fn upstream_counter(&mut self, kind: (EventType, SqlQueryType)) -> &mut Counter {
        self.counters
            .entry(kind)
            .or_default()
            .upstream_exe_count
            .get_or_insert_with(|| {
                let mut labels = vec![
                    ("query", self.query.clone()),
                    ("event_type", SharedString::from(kind.0)),
                    ("query_type", SharedString::from(kind.1)),
                    ("database_type", SharedString::from(DatabaseType::MySql)),
                ];

                if let Some(id) = &self.query_id {
                    labels.push(("query_id", id.clone()));
                }

                register_counter!(recorded::QUERY_LOG_EXECUTION_COUNT, &labels)
            })
    }
}

impl QueryLogger {
    fn query_string(query: &SqlQuery) -> SharedString {
        SharedString::from(match query {
            SqlQuery::Select(stmt) => {
                let mut stmt = stmt.clone();
                if readyset_adapter::rewrite::process_query(&mut stmt, true).is_ok() {
                    anonymize_literals(&mut stmt);
                    // FIXME(REA-2168): Use correct dialect.
                    stmt.display(nom_sql::Dialect::MySQL).to_string()
                } else {
                    "".to_string()
                }
            }
            _ => "".to_string(),
        })
    }

    fn metrics_for_id(&mut self, query_id: QueryId, query: Arc<SqlQuery>) -> &mut QueryMetrics {
        self.per_id_metrics.entry(query_id).or_insert_with(|| {
            let query_string = Self::query_string(&query);
            let query_id = SharedString::from(query_id.to_string());

            QueryMetrics {
                num_keys: register_counter!(
                    recorded::QUERY_LOG_TOTAL_KEYS_READ,
                    "query" => query_string.clone(),
                    "query_id" => query_id.clone(),
                ),
                cache_misses: register_counter!(
                    recorded::QUERY_LOG_QUERY_CACHE_MISSED,
                    "query" => query_string.clone(),
                    "query_id" => query_id.clone(),
                ),
                cache_keys_missed: register_counter!(
                    recorded::QUERY_LOG_TOTAL_CACHE_MISSES,
                    "query" => query_string.clone(),
                    "query_id" => query_id.clone(),
                ),
                query: query_string,
                query_id: Some(query_id),
                histograms: BTreeMap::new(),
                counters: BTreeMap::new(),
            }
        })
    }

    fn metrics_for_query(&mut self, query: Arc<SqlQuery>) -> &mut QueryMetrics {
        self.per_query_metrics
            .entry(query)
            .or_insert_with_key(|query| {
                let query_string = Self::query_string(query);

                QueryMetrics {
                    num_keys: register_counter!(
                        readyset_client_metrics::recorded::QUERY_LOG_TOTAL_KEYS_READ,
                        "query" => query_string.clone(),
                    ),
                    cache_misses: register_counter!(
                        readyset_client_metrics::recorded::QUERY_LOG_QUERY_CACHE_MISSED,
                        "query" => query_string.clone(),
                    ),
                    cache_keys_missed: register_counter!(
                        readyset_client_metrics::recorded::QUERY_LOG_TOTAL_CACHE_MISSES,
                        "query" => query_string.clone(),
                    ),
                    query: query_string,
                    query_id: None,
                    histograms: BTreeMap::new(),
                    counters: BTreeMap::new(),
                }
            })
    }

    /// Async task that logs query stats.
    pub(crate) async fn run(
        mut receiver: UnboundedReceiver<QueryExecutionEvent>,
        mut shutdown_recv: ShutdownReceiver,
        mode: QueryLogMode,
    ) {
        let _span = info_span!("query-logger");

        let mut logger = QueryLogger {
            per_query_metrics: HashMap::new(),
            per_id_metrics: BTreeMap::new(),
            parse_error_count: register_counter!(
                readyset_client_metrics::recorded::QUERY_LOG_PARSE_ERRORS,
            ),
            set_disallowed_count: register_counter!(
                readyset_client_metrics::recorded::QUERY_LOG_SET_DISALLOWED,
            ),
            view_not_found_count: register_counter!(
                readyset_client_metrics::recorded::QUERY_LOG_VIEW_NOT_FOUND,
            ),
            rpc_error_count: register_counter!(
                readyset_client_metrics::recorded::QUERY_LOG_RPC_ERRORS,
            ),
        };

        loop {
            select! {
                // We use `biased` here to ensure that our shutdown signal will be received and
                // acted upon even if the other branches in this `select!` are constantly in a
                // ready state (e.g. a stream that has many messages where very little time passes
                // between receipt of these messages). More information about this situation can
                // be found in the docs for `tokio::select`.
                biased;
                _ = shutdown_recv.recv() => {
                    info!("Metrics task shutting down after signal received.");
                    break;
                }
                event = receiver.recv() => {
                    let event = match event {
                        Some(event) => event,
                        None => {
                            info!("Metrics task shutting down after request handle dropped.");
                            break;
                        }
                    };

                    if let Some(error) = event.noria_error {
                        if error.caused_by_unparseable_query() {
                            logger.parse_error_count.increment(1);
                        } else if error.is_set_disallowed() {
                            logger.set_disallowed_count.increment(1);
                        } else if error.caused_by_view_not_found() {
                            logger.view_not_found_count.increment(1);
                        } else if error.is_networking_related() {
                            logger.rpc_error_count.increment(1);
                        }
                    };

                    let query = match event.query {
                        Some(query) => query,
                        None => continue,
                    };

                    let metrics = if let Some(id) = event.query_id {
                        logger.metrics_for_id(id, query)
                    } else {
                        logger.metrics_for_query(query)
                    };

                    if let Some(num_keys) = event.num_keys {
                        metrics.num_keys.increment(num_keys);
                    }

                    if let Some(cache_misses) = event.cache_misses {
                        metrics.cache_keys_missed.increment(cache_misses);
                        if cache_misses != 0 {
                            metrics.cache_misses.increment(1);
                        }
                    }

                    if mode.is_verbose() && let Some(duration) = event.parse_duration {
                        metrics
                            .parse_histogram((event.event, event.sql_type), &mode)
                            .record(duration);
                    }

                    if let Some(duration) = event.readyset_duration {
                        metrics
                            .readyset_histogram((event.event, event.sql_type), &mode)
                            .record(duration);
                        metrics.readyset_counter((event.event, event.sql_type)).increment(1);
                    }

                    if mode.allow_proxied_queries() && let Some(duration) = event.upstream_duration {
                        metrics
                            .upstream_histogram((event.event, event.sql_type), &mode)
                            .record(duration);
                        metrics.upstream_counter((event.event, event.sql_type)).increment(1);
                    }
                }
            }
        }
    }
}
