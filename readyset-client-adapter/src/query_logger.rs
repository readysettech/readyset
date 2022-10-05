use metrics::SharedString;
use nom_sql::SqlQuery;
use readyset_client_metrics::QueryExecutionEvent;
use readyset_sql_passes::anonymize::anonymize_literals;
use tokio::select;
use tokio::sync::broadcast;
use tokio::sync::mpsc::UnboundedReceiver;
use tracing::{info, info_span};

/// Async task that logs query stats.
pub(crate) async fn query_logger(
    mut receiver: UnboundedReceiver<QueryExecutionEvent>,
    mut shutdown_recv: broadcast::Receiver<()>,
) {
    let _span = info_span!("query-logger");

    loop {
        select! {
            event = receiver.recv() => {
                if let Some(event) = event {
                    let query = match event.query {
                        Some(s) => match s.as_ref() {
                            SqlQuery::Select(stmt) => {
                                let mut stmt = stmt.clone();
                                if readyset_client::rewrite::process_query(&mut stmt, true).is_ok() {
                                    anonymize_literals(&mut stmt);
                                    stmt.to_string()
                                } else {
                                    "".to_string()
                                }
                            },
                            _ => "".to_string()
                        },
                        _ => "".to_string()
                    };

                    if let Some(num_keys) = event.num_keys {
                        metrics::counter!(
                            readyset_client_metrics::recorded::QUERY_LOG_TOTAL_KEYS_READ,
                            num_keys,
                            "query" => query.clone(),
                        );
                    }

                    if let Some(parse) = event.parse_duration {
                        metrics::histogram!(
                            readyset_client_metrics::recorded::QUERY_LOG_PARSE_TIME,
                            parse,
                            "query" => query.clone(),
                            "event_type" => SharedString::from(event.event),
                            "query_type" => SharedString::from(event.sql_type)
                        );
                    }

                    if let Some(readyset) = event.readyset_duration {
                        metrics::histogram!(
                            readyset_client_metrics::recorded::QUERY_LOG_EXECUTION_TIME,
                            readyset.as_secs_f64(),
                            "query" => query.clone(),
                            "database_type" => String::from(readyset_client_metrics::DatabaseType::ReadySet),
                            "event_type" => SharedString::from(event.event),
                            "query_type" => SharedString::from(event.sql_type)
                        );
                    }

                    if let Some(upstream) = event.upstream_duration {
                        metrics::histogram!(
                            readyset_client_metrics::recorded::QUERY_LOG_EXECUTION_TIME,
                            upstream.as_secs_f64(),
                            "query" => query.clone(),
                            "database_type" => String::from(readyset_client_metrics::DatabaseType::MySql),
                            "event_type" => SharedString::from(event.event),
                            "query_type" => SharedString::from(event.sql_type)
                        );
                    }

                    if let Some(cache_misses) = event.cache_misses {
                        metrics::counter!(
                            readyset_client_metrics::recorded::QUERY_LOG_TOTAL_CACHE_MISSES,
                            cache_misses,
                            "query" => query.clone(),
                        );
                        if cache_misses != 0 {
                            metrics::counter!(
                                readyset_client_metrics::recorded::QUERY_LOG_QUERY_CACHE_MISSED,
                                1,
                                "query" => query.clone(),
                            );
                        }
                    }
                } else {
                    info!("Metrics task shutting down after request handle dropped.");
                }
            }
            _ = shutdown_recv.recv() => {
                info!("Metrics task shutting down after signal received.");
                break;
            }
        }
    }
}
