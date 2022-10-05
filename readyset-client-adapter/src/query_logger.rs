use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use metrics::{register_counter, register_histogram, Counter, Histogram, SharedString};
use nom_sql::SqlQuery;
use readyset_client_metrics::{EventType, QueryExecutionEvent, SqlQueryType};
use readyset_sql_passes::anonymize::anonymize_literals;
use tokio::select;
use tokio::sync::broadcast;
use tokio::sync::mpsc::UnboundedReceiver;
use tracing::{info, info_span};

pub(crate) struct QueryLogger {
    per_query_metrics: HashMap<Arc<SqlQuery>, QueryMetrics>,
}

struct QueryMetrics {
    query: SharedString,
    num_keys: Counter,
    cache_misses: Counter,
    cache_keys_missed: Counter,
    histograms: BTreeMap<(EventType, SqlQueryType), QueryHistograms>,
}

#[derive(Default)]
struct QueryHistograms {
    parse_time: Option<Histogram>,
    upstream_exe_time: Option<Histogram>,
    readyset_exe_time: Option<Histogram>,
}

impl QueryMetrics {
    fn parse_histogram(&mut self, kind: (EventType, SqlQueryType)) -> &mut Histogram {
        self.histograms
            .entry(kind)
            .or_default()
            .parse_time
            .get_or_insert_with(|| {
                register_histogram!(readyset_client_metrics::recorded::QUERY_LOG_PARSE_TIME,
                    "query" => self.query.clone(),
                    "event_type" => SharedString::from(kind.0),
                    "query_type" => SharedString::from(kind.1))
            })
    }

    fn readyset_histogram(&mut self, kind: (EventType, SqlQueryType)) -> &mut Histogram {
        self.histograms
            .entry(kind)
            .or_default()
            .readyset_exe_time
            .get_or_insert_with(|| {
                register_histogram!(readyset_client_metrics::recorded::QUERY_LOG_EXECUTION_TIME,
                    "query" => self.query.clone(),
                    "database_type" => SharedString::from(readyset_client_metrics::DatabaseType::ReadySet),
                    "event_type" => SharedString::from(kind.0),
                    "query_type" => SharedString::from(kind.1))
            })
    }

    fn upstream_histogram(&mut self, kind: (EventType, SqlQueryType)) -> &mut Histogram {
        self.histograms
            .entry(kind)
            .or_default()
            .upstream_exe_time
            .get_or_insert_with(|| {
                register_histogram!(readyset_client_metrics::recorded::QUERY_LOG_EXECUTION_TIME,
                    "query" => self.query.clone(),
                    "database_type" => SharedString::from(readyset_client_metrics::DatabaseType::MySql),
                    "event_type" => SharedString::from(kind.0),
                    "query_type" => SharedString::from(kind.1))
            })
    }
}

impl QueryLogger {
    fn metrics_for_query(&mut self, query: Arc<SqlQuery>) -> &mut QueryMetrics {
        self.per_query_metrics
            .entry(query)
            .or_insert_with_key(|query| {
                let query_string = SharedString::from(match query.as_ref() {
                    SqlQuery::Select(stmt) => {
                        let mut stmt = stmt.clone();
                        if readyset_client::rewrite::process_query(&mut stmt, true).is_ok() {
                            anonymize_literals(&mut stmt);
                            stmt.to_string()
                        } else {
                            "".to_string()
                        }
                    }
                    _ => "".to_string(),
                });

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
                    histograms: BTreeMap::new(),
                }
            })
    }

    /// Async task that logs query stats.
    pub(crate) async fn run(
        mut receiver: UnboundedReceiver<QueryExecutionEvent>,
        mut shutdown_recv: broadcast::Receiver<()>,
    ) {
        let _span = info_span!("query-logger");

        let mut logger = QueryLogger {
            per_query_metrics: HashMap::new(),
        };

        loop {
            select! {
                event = receiver.recv() => {
                    let event = match event {
                        Some(event) => event,
                        None => {
                            info!("Metrics task shutting down after request handle dropped.");
                            break;
                        },
                    };

                    let query = match event.query {
                        Some(query) => query,
                        None => continue,
                    };

                    let metrics = logger.metrics_for_query(query);

                    if let Some(num_keys) = event.num_keys {
                        metrics.num_keys.increment(num_keys);
                    }

                    if let Some(cache_misses) = event.cache_misses {
                        metrics.cache_keys_missed.increment(cache_misses);
                        if cache_misses != 0 {
                            metrics.cache_misses.increment(1);
                        }
                    }

                    if let Some(duration) = event.parse_duration {
                        metrics.parse_histogram((event.event, event.sql_type)).record(duration);
                    }

                    if let Some(duration) = event.readyset_duration {
                        metrics.readyset_histogram((event.event, event.sql_type)).record(duration);
                    }

                    if let Some(duration) = event.upstream_duration {
                        metrics.upstream_histogram((event.event, event.sql_type)).record(duration);
                    }
                }
                _ = shutdown_recv.recv() => {
                    info!("Metrics task shutting down after signal received.");
                    break;
                }
            }
        }
    }
}
