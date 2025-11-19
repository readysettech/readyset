use metrics::{counter, gauge, histogram, Counter, SharedString};
use readyset_client::query::QueryId;
use readyset_client_metrics::{
    recorded, DatabaseType, EventType, QueryExecutionEvent, QueryIdWrapper, QueryLogMode,
    ReadysetExecutionEvent,
};
use readyset_sql::ast::SqlQuery;

use readyset_sql::{Dialect, DialectDisplay};
use readyset_sql_passes::adapter_rewrites::{self, AdapterRewriteParams};
use readyset_sql_passes::anonymize::anonymize_literals;
use readyset_util::shutdown::ShutdownReceiver;
use tokio::select;
use tokio::sync::mpsc::UnboundedReceiver;
use tracing::info;

pub struct QueryLogger {
    parse_error_count: Counter,
    set_disallowed_count: Counter,
    view_not_found_count: Counter,
    rpc_error_count: Counter,

    // simple counts of the EventType received
    query_count: Counter,
    prepare_count: Counter,
    execute_count: Counter,

    log_mode: QueryLogMode,
    rewrite_params: AdapterRewriteParams,
    dialect: Dialect,
}

impl QueryLogger {
    pub fn new(mode: QueryLogMode, dialect: Dialect, rewrite_params: AdapterRewriteParams) -> Self {
        QueryLogger {
            parse_error_count: counter!(readyset_client_metrics::recorded::QUERY_LOG_PARSE_ERRORS,),
            set_disallowed_count: counter!(
                readyset_client_metrics::recorded::QUERY_LOG_SET_DISALLOWED,
            ),
            view_not_found_count: counter!(
                readyset_client_metrics::recorded::QUERY_LOG_VIEW_NOT_FOUND,
            ),
            rpc_error_count: counter!(readyset_client_metrics::recorded::QUERY_LOG_RPC_ERRORS,),

            query_count: counter!(recorded::QUERY_LOG_EVENT_TYPE, "type" => "query"),
            prepare_count: counter!(recorded::QUERY_LOG_EVENT_TYPE, "type" => "prepare"),
            execute_count: counter!(recorded::QUERY_LOG_EVENT_TYPE, "type" => "execute"),

            log_mode: mode,
            rewrite_params,
            dialect,
        }
    }

    fn process_query(
        query: &SqlQuery,
        query_id_wrapper: &QueryIdWrapper,
        rewrite_params: AdapterRewriteParams,
        dialect: Dialect,
    ) -> (SharedString, Option<SharedString>) {
        match query {
            SqlQuery::Select(stmt) => {
                let mut stmt = stmt.clone();
                if adapter_rewrites::rewrite_query(&mut stmt, rewrite_params).is_ok() {
                    anonymize_literals(&mut stmt);
                    let query_string = stmt.display(dialect).to_string();

                    let query_id = match query_id_wrapper {
                        QueryIdWrapper::Uncalculated(schema_search_path) => Some(
                            QueryId::from_select(&stmt, schema_search_path)
                                .to_string()
                                .into(),
                        ),
                        QueryIdWrapper::Calculated(qid) => Some(qid.to_string().into()),
                        QueryIdWrapper::None => None,
                    };

                    (SharedString::from(query_string), query_id)
                } else {
                    (SharedString::from(""), None)
                }
            }
            _ => (SharedString::from(""), None),
        }
    }

    fn create_labels(
        database_type: DatabaseType,
        query_string: Option<SharedString>,
        query_id: Option<SharedString>,
    ) -> Vec<(&'static str, SharedString)> {
        let mut labels = vec![("database_type", database_type.into())];
        if let Some(query) = query_string {
            labels.push(("query", query));
        }
        if let Some(id) = query_id {
            labels.push(("query_id", id));
        }
        labels
    }

    fn record_query_metrics(
        &self,
        event: &QueryExecutionEvent,
        query_string: Option<SharedString>,
        query_id: Option<SharedString>,
    ) {
        if let Some(duration) = event.upstream_duration {
            let upstream_labels = Self::create_labels(
                DatabaseType::Upstream,
                query_string.clone(),
                query_id.clone(),
            );
            histogram!(recorded::QUERY_LOG_EXECUTION_TIME, &upstream_labels)
                .record(duration.as_micros() as f64);
            counter!(recorded::QUERY_LOG_EXECUTION_COUNT, &upstream_labels).increment(1);
        }

        let labels = Self::create_labels(DatabaseType::ReadySet, query_string, query_id);

        match &event.readyset_event {
            Some(ReadysetExecutionEvent::CacheRead {
                cache_misses,
                num_keys,
                duration,
                cache_name,
            }) => {
                let cache_name = SharedString::from(cache_name.display_unquoted().to_string());
                let mut cached_labels = vec![("cache_name", cache_name.clone())];

                counter!(recorded::QUERY_LOG_TOTAL_KEYS_READ, &cached_labels).increment(*num_keys);
                counter!(recorded::QUERY_LOG_TOTAL_CACHE_MISSES, &cached_labels)
                    .increment(*cache_misses);

                if *cache_misses != 0 {
                    counter!(recorded::QUERY_LOG_QUERY_CACHE_MISSED, &cached_labels)
                        .increment(*cache_misses);
                }

                cached_labels.extend_from_slice(&labels);

                histogram!(recorded::QUERY_LOG_EXECUTION_TIME, &cached_labels)
                    .record(duration.as_micros() as f64);
                counter!(recorded::QUERY_LOG_EXECUTION_COUNT, &cached_labels).increment(1);
            }
            Some(ReadysetExecutionEvent::Other { duration }) => {
                histogram!(recorded::QUERY_LOG_EXECUTION_TIME, &labels)
                    .record(duration.as_micros() as f64);
                counter!(recorded::QUERY_LOG_EXECUTION_COUNT, &labels).increment(1);
            }
            None => (),
        }
    }

    pub fn handle_event(&mut self, event: &QueryExecutionEvent) {
        if let Some(error) = &event.noria_error {
            if error.caused_by_unparseable_query() {
                self.parse_error_count.increment(1);
            } else if error.is_set_disallowed() {
                self.set_disallowed_count.increment(1);
            } else if error.caused_by_view_not_found() {
                self.view_not_found_count.increment(1);
            } else if error.is_rpc_failure() {
                self.rpc_error_count.increment(1);
            }
        };

        match event.event {
            EventType::Query => self.query_count.increment(1),
            EventType::Prepare => self.prepare_count.increment(1),
            EventType::Execute => self.execute_count.increment(1),
        }

        if !self.log_mode.is_verbose() {
            self.record_query_metrics(event, None, None);
            return;
        }

        let query = match &event.query {
            Some(query) => query,
            None => return,
        };

        let (query_string, query_id) =
            Self::process_query(query, &event.query_id, self.rewrite_params, self.dialect);

        if let Some(duration) = event.parse_duration {
            let mut labels = Self::create_labels(
                DatabaseType::ReadySet,
                Some(query_string.clone()),
                query_id.clone(),
            );
            labels.push(("event_type", SharedString::from(event.event)));
            labels.push(("query_type", SharedString::from(event.sql_type)));
            histogram!(recorded::QUERY_LOG_PARSE_TIME, &labels).record(duration.as_micros() as f64);
        }

        self.record_query_metrics(event, Some(query_string), query_id);
    }

    /// Async task that logs query stats.
    pub(crate) async fn run(
        &mut self,
        mut receiver: UnboundedReceiver<QueryExecutionEvent>,
        mut shutdown_recv: ShutdownReceiver,
    ) {
        let backlog_size = gauge!(recorded::QUERY_LOG_BACKLOG_SIZE);
        let processed_events = counter!(recorded::QUERY_LOG_PROCESSED_EVENTS);
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
                    backlog_size.set(receiver.len() as f64);
                    match event {
                        Some(event) => {
                            self.handle_event(&event);
                            processed_events.increment(1);
                        }
                        None => {
                            info!("Metrics task shutting down after request handle dropped.");
                            break;
                        }
                    };

                }
            }
        }
    }
}
