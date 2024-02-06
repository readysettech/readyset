use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use metrics::{
    counter, histogram, register_counter, register_histogram, Counter, Histogram, SharedString,
};
use nom_sql::{DialectDisplay, SqlQuery};
use readyset_client::query::QueryId;
use readyset_client_metrics::{
    recorded, DatabaseType, EventType, QueryExecutionEvent, QueryLogMode, ReadysetExecutionEvent,
    SqlQueryType,
};
use readyset_sql_passes::adapter_rewrites;
use readyset_sql_passes::anonymize::anonymize_literals;
use readyset_util::shutdown::ShutdownReceiver;
use tokio::select;
use tokio::sync::mpsc::UnboundedReceiver;
use tracing::{info, info_span};

pub(crate) struct QueryLogger {
    per_query_metrics: HashMap<Arc<SqlQuery>, QueryMetrics>,
    parse_error_count: Counter,
    set_disallowed_count: Counter,
    view_not_found_count: Counter,
    rpc_error_count: Counter,

    // simple counts of the EventType received
    query_count: Counter,
    prepare_count: Counter,
    execute_count: Counter,
}

struct QueryMetrics {
    query: SharedString,
    query_id: Option<SharedString>,
    parse_time: BTreeMap<(EventType, SqlQueryType), Histogram>,
}

impl QueryMetrics {
    fn parse_histogram(
        &mut self,
        kind: (EventType, SqlQueryType),
        mode: QueryLogMode,
    ) -> &mut Histogram {
        self.parse_time.entry(kind).or_insert_with(|| {
            let mut labels = vec![
                ("event_type", SharedString::from(kind.0)),
                ("query_type", SharedString::from(kind.1)),
            ];

            if mode.is_verbose() {
                labels.push(("query", self.query.clone()));

                if let Some(id) = &self.query_id {
                    labels.push(("query_id", id.clone()));
                }
            }

            register_histogram!(recorded::QUERY_LOG_PARSE_TIME, &labels)
        })
    }
}

impl QueryLogger {
    fn query_string(query: &SqlQuery) -> SharedString {
        SharedString::from(match query {
            SqlQuery::Select(stmt) => {
                let mut stmt = stmt.clone();
                if adapter_rewrites::process_query(&mut stmt, true).is_ok() {
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

    fn metrics_for_query(
        &mut self,
        query: Arc<SqlQuery>,
        query_id: Option<QueryId>,
    ) -> &mut QueryMetrics {
        self.per_query_metrics
            .entry(query)
            .or_insert_with_key(|query| {
                let query_string = Self::query_string(query);

                QueryMetrics {
                    query: query_string,
                    query_id: query_id.map(|id| id.to_string().into()),
                    parse_time: BTreeMap::new(),
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

            query_count: register_counter!(recorded::QUERY_LOG_EVENT_TYPE, "type" => "query"),
            prepare_count: register_counter!(recorded::QUERY_LOG_EVENT_TYPE, "type" => "prepare"),
            execute_count: register_counter!(recorded::QUERY_LOG_EVENT_TYPE, "type" => "execute"),
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

                    match event.event {
                        EventType::Query => logger.query_count.increment(1),
                        EventType::Prepare => logger.prepare_count.increment(1),
                        EventType::Execute => logger.execute_count.increment(1),
                    }

                    let query = match event.query {
                        Some(query) => query,
                        None => continue,
                    };

                    let metrics = logger.metrics_for_query(query.clone(), event.query_id);

                    if mode.is_verbose() && let Some(duration) = event.parse_duration {
                        metrics
                            .parse_histogram((event.event, event.sql_type), mode)
                            .record(duration);
                    }

                    match event.readyset_event {
                        Some(ReadysetExecutionEvent::CacheRead { cache_misses, num_keys, duration, cache_name }) => {
                            let mut labels = vec![("cache_name", SharedString::from(cache_name.display_unquoted().to_string()))];

                            counter!(recorded::QUERY_LOG_TOTAL_KEYS_READ, num_keys, &labels);
                            counter!(recorded::QUERY_LOG_TOTAL_CACHE_MISSES, cache_misses, &labels);

                            if cache_misses != 0 {
                                counter!(recorded::QUERY_LOG_QUERY_CACHE_MISSED, cache_misses, &labels);
                            }

                            labels.push(("database_type", SharedString::from(DatabaseType::ReadySet)));

                            if mode.is_verbose() {
                                labels.push(("query", Self::query_string(&query)));

                                if let Some(id) = &event.query_id {
                                    labels.push(("query_id", SharedString::from(id.to_string())));
                                }
                            }

                            histogram!(recorded::QUERY_LOG_EXECUTION_TIME, duration.as_micros() as f64, &labels);
                            counter!(recorded::QUERY_LOG_EXECUTION_COUNT, 1, &labels);
                        }
                        Some(ReadysetExecutionEvent::Other { duration }) => {
                            let mut labels =
                                vec![("database_type", SharedString::from(DatabaseType::ReadySet))];

                            if mode.is_verbose() {
                                labels.push(("query", Self::query_string(&query)));

                                if let Some(id) = &event.query_id {
                                    labels.push(("query_id", SharedString::from(id.to_string())));
                                }
                            }

                            histogram!(recorded::QUERY_LOG_EXECUTION_TIME, duration.as_micros() as f64, &labels);
                            counter!(recorded::QUERY_LOG_EXECUTION_COUNT, 1, &labels);
                        }
                        None => (),
                    }

                    if let Some(duration) = event.upstream_duration {
                        let mut labels =
                            vec![("database_type", SharedString::from(DatabaseType::MySql))];

                        if mode.is_verbose() {
                            labels.push(("query", Self::query_string(&query)));

                            if let Some(id) = &event.query_id {
                                labels.push(("query_id", SharedString::from(id.to_string())));
                            }
                        }

                        histogram!(recorded::QUERY_LOG_EXECUTION_TIME, duration.as_micros() as f64, &labels);
                        counter!(recorded::QUERY_LOG_EXECUTION_COUNT, 1, &labels);
                    }
                }
            }
        }
    }
}
