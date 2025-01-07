use metrics::{counter, gauge, histogram, Counter, SharedString};
use nom_sql::{DialectDisplay, SqlIdentifier, SqlQuery};
use readyset_client::query::QueryId;
use readyset_client_metrics::{
    recorded, DatabaseType, EventType, QueryExecutionEvent, QueryIdWrapper, QueryLogMode,
    ReadysetExecutionEvent,
};
use readyset_sql_passes::adapter_rewrites::{self, AdapterRewriteParams};
use readyset_sql_passes::anonymize::anonymize_literals;
use readyset_util::shutdown::ShutdownReceiver;
use tokio::select;
use tokio::sync::mpsc::UnboundedReceiver;
use tracing::{info, instrument};

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
}

impl QueryLogger {
    pub fn new(mode: QueryLogMode, rewrite_params: AdapterRewriteParams) -> Self {
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
        }
    }

    fn query_id(
        query: &SqlQuery,
        schema_search_path: &[SqlIdentifier],
        rewrite_params: AdapterRewriteParams,
    ) -> QueryId {
        if let SqlQuery::Select(stmt) = query {
            let mut stmt = stmt.clone();
            if adapter_rewrites::process_query(&mut stmt, rewrite_params).is_ok() {
                anonymize_literals(&mut stmt);
                return QueryId::from_select(&stmt, schema_search_path);
            }
        }

        Default::default()
    }

    fn query_string(query: &SqlQuery, rewrite_params: AdapterRewriteParams) -> SharedString {
        SharedString::from(match query {
            SqlQuery::Select(stmt) => {
                let mut stmt = stmt.clone();
                if adapter_rewrites::process_query(&mut stmt, rewrite_params).is_ok() {
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

    fn create_labels(
        &self,
        database_type: DatabaseType,
        query_string: Option<SharedString>,
        query_id: Option<QueryId>,
    ) -> Vec<(&'static str, SharedString)> {
        let mut labels = vec![("database_type", SharedString::from(database_type))];

        if self.log_mode.is_verbose() {
            if let Some(query) = query_string {
                labels.push(("query", query));
            }
            if let Some(id) = query_id {
                labels.push(("query_id", SharedString::from(id.to_string())));
            }
        }
        labels
    }

    pub fn handle_event(&mut self, event: &QueryExecutionEvent) {
        if let Some(error) = &event.noria_error {
            if error.caused_by_unparseable_query() {
                self.parse_error_count.increment(1);
            } else if error.is_set_disallowed() {
                self.set_disallowed_count.increment(1);
            } else if error.caused_by_view_not_found() {
                self.view_not_found_count.increment(1);
            } else if error.is_networking_related() {
                self.rpc_error_count.increment(1);
            }
        };

        match event.event {
            EventType::Query => self.query_count.increment(1),
            EventType::Prepare => self.prepare_count.increment(1),
            EventType::Execute => self.execute_count.increment(1),
        }

        let query = match &event.query {
            Some(query) => query,
            None => return,
        };

        let query_string = Self::query_string(query, self.rewrite_params);
        let query_id = match &event.query_id {
            QueryIdWrapper::Uncalculated(schema_search_path) => Some(Self::query_id(
                query,
                schema_search_path,
                self.rewrite_params,
            )),
            QueryIdWrapper::Calculated(qid) => Some(*qid),
            QueryIdWrapper::None => None,
        };

        if self.log_mode.is_verbose() && event.parse_duration.is_some() {
            let mut labels = vec![
                ("event_type", SharedString::from(event.event)),
                ("query_type", SharedString::from(event.sql_type)),
            ];

            if self.log_mode.is_verbose() {
                labels.push(("query", query_string.clone()));

                if let Some(id) = &query_id {
                    labels.push(("query_id", SharedString::from(id.to_string())));
                }

                histogram!(recorded::QUERY_LOG_PARSE_TIME, &labels)
                    .record(event.parse_duration.unwrap().as_micros() as f64);
            }
        }

        match &event.readyset_event {
            Some(ReadysetExecutionEvent::CacheRead {
                cache_misses,
                num_keys,
                duration,
                cache_name,
            }) => {
                let cache_name = SharedString::from(cache_name.display_unquoted().to_string());
                let labels = vec![("cache_name", cache_name.clone())];

                counter!(recorded::QUERY_LOG_TOTAL_KEYS_READ, &labels).increment(*num_keys);
                counter!(recorded::QUERY_LOG_TOTAL_CACHE_MISSES, &labels).increment(*cache_misses);

                if *cache_misses != 0 {
                    counter!(recorded::QUERY_LOG_QUERY_CACHE_MISSED, &labels)
                        .increment(*cache_misses);
                }

                let mut labels = self.create_labels(
                    DatabaseType::ReadySet,
                    Some(query_string.clone()),
                    query_id,
                );
                labels.push(("cache_name", cache_name));

                histogram!(recorded::QUERY_LOG_EXECUTION_TIME, &labels)
                    .record(duration.as_micros() as f64);
                counter!(recorded::QUERY_LOG_EXECUTION_COUNT, &labels).increment(1);
            }
            Some(ReadysetExecutionEvent::Other { duration }) => {
                let labels = self.create_labels(
                    DatabaseType::ReadySet,
                    Some(query_string.clone()),
                    query_id,
                );

                histogram!(recorded::QUERY_LOG_EXECUTION_TIME, &labels)
                    .record(duration.as_micros() as f64);
                counter!(recorded::QUERY_LOG_EXECUTION_COUNT, &labels).increment(1);
            }
            None => (),
        }

        if let Some(duration) = event.upstream_duration {
            let labels =
                self.create_labels(DatabaseType::Upstream, Some(query_string.clone()), query_id);

            histogram!(recorded::QUERY_LOG_EXECUTION_TIME, &labels)
                .record(duration.as_micros() as f64);
            counter!(recorded::QUERY_LOG_EXECUTION_COUNT, &labels).increment(1);
        }
    }

    /// Async task that logs query stats.
    #[instrument(name = "query_logger", skip_all)]
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
