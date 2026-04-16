use std::sync::Arc;

use metrics::{counter, gauge, histogram, Counter, SharedString};
use readyset_client_metrics::{
    recorded, DatabaseType, EventType, QueryExecutionEvent, QueryLogMode, ReadysetExecutionEvent,
};
use readyset_sql::ast::{SqlIdentifier, SqlQuery};

use readyset_sql::{Dialect, DialectDisplay};
use readyset_sql_passes::adapter_rewrites::{self, AdapterRewriteParams};
use readyset_sql_passes::anonymize::anonymize_literals;
use readyset_util::shutdown::ShutdownReceiver;
use readyset_util::timestamp::current_timestamp_ms;
use schema_catalog::{RewriteContext, SchemaCatalog, SchemaCatalogHandle};
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
    search_path: Vec<SqlIdentifier>,
    schema_catalog_handle: SchemaCatalogHandle,
}

impl QueryLogger {
    pub fn new(
        mode: QueryLogMode,
        rewrite_params: AdapterRewriteParams,
        dialect: Dialect,
        search_path: Vec<SqlIdentifier>,
        schema_catalog_handle: SchemaCatalogHandle,
    ) -> Self {
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
            search_path,
            schema_catalog_handle,
        }
    }

    fn process_query(
        query: &SqlQuery,
        rewrite_params: AdapterRewriteParams,
        dialect: Dialect,
        rewrite_context: RewriteContext,
    ) -> Option<SharedString> {
        match query {
            SqlQuery::Select(stmt) => {
                let mut stmt = stmt.clone();
                if adapter_rewrites::rewrite_query(&mut stmt, rewrite_params, &rewrite_context)
                    .is_ok()
                {
                    anonymize_literals(&mut stmt);
                    Some(SharedString::from(stmt.display(dialect).to_string()))
                } else {
                    None
                }
            }
            _ => None,
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
        let now_s = (current_timestamp_ms() / 1000) as f64;

        if let Some(duration) = event.upstream_duration {
            let upstream_labels = Self::create_labels(
                DatabaseType::Upstream,
                query_string.clone(),
                query_id.clone(),
            );
            histogram!(recorded::QUERY_LOG_EXECUTION_TIME, &upstream_labels)
                .record(duration.as_micros() as f64);
            counter!(recorded::QUERY_LOG_EXECUTION_COUNT, &upstream_labels).increment(1);
            gauge!(recorded::QUERY_LOG_LAST_EXECUTION_EPOCH_S, &upstream_labels).set(now_s);
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
                gauge!(recorded::QUERY_LOG_LAST_EXECUTION_EPOCH_S, &cached_labels).set(now_s);
            }
            Some(ReadysetExecutionEvent::Other { duration }) => {
                histogram!(recorded::QUERY_LOG_EXECUTION_TIME, &labels)
                    .record(duration.as_micros() as f64);
                counter!(recorded::QUERY_LOG_EXECUTION_COUNT, &labels).increment(1);
                gauge!(recorded::QUERY_LOG_LAST_EXECUTION_EPOCH_S, &labels).set(now_s);
            }
            None => (),
        }
    }

    pub async fn handle_event(&mut self, event: &QueryExecutionEvent) {
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

        let query_id = event.query_id.map(|id| SharedString::from(id.to_string()));

        if !self.log_mode.is_verbose() {
            self.record_query_metrics(event, None, query_id);
            return;
        }

        let query = match &event.query {
            Some(query) => query,
            None => return,
        };

        let query_string = Self::process_query(
            query,
            self.rewrite_params,
            self.dialect,
            self.rewrite_context().await,
        );

        if let Some(duration) = event.parse_duration {
            let mut labels = Self::create_labels(
                DatabaseType::ReadySet,
                query_string.clone(),
                query_id.clone(),
            );
            labels.push(("event_type", SharedString::from(event.event)));
            labels.push(("query_type", SharedString::from(event.sql_type)));
            histogram!(recorded::QUERY_LOG_PARSE_TIME, &labels).record(duration.as_micros() as f64);
        }

        self.record_query_metrics(event, query_string, query_id);
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
                            self.handle_event(&event).await;
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

    async fn rewrite_context(&self) -> RewriteContext {
        // Unlike in the adapter, we just chug along if there's no schema catalog available.
        let schema_catalog = self
            .schema_catalog_handle
            .get_catalog()
            .await
            .unwrap_or_else(|_err| Arc::new(SchemaCatalog::new()));
        RewriteContext::new(
            self.dialect.into(),
            schema_catalog,
            self.search_path.clone(),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use metrics::Key;
    use metrics_util::debugging::{DebuggingRecorder, Snapshotter};
    use metrics_util::MetricKind;
    use readyset_client::query::QueryId;
    use readyset_client_metrics::{
        recorded, EventType, QueryExecutionEvent, QueryLogMode, ReadysetExecutionEvent,
        SqlQueryType,
    };
    use readyset_sql::Dialect;
    use readyset_sql_passes::adapter_rewrites::AdapterRewriteParams;
    use schema_catalog::SchemaCatalogHandle;

    use super::QueryLogger;

    const DIALECT: Dialect = Dialect::PostgreSQL;

    fn new_logger(mode: QueryLogMode) -> QueryLogger {
        QueryLogger::new(
            mode,
            AdapterRewriteParams::new(DIALECT),
            DIALECT,
            vec!["public".into()],
            SchemaCatalogHandle::new(),
        )
    }

    fn label_map(key: &Key) -> std::collections::HashMap<&str, &str> {
        key.labels().map(|l| (l.key(), l.value())).collect()
    }

    fn find_execution_count_labels(
        snapshotter: &Snapshotter,
    ) -> Vec<std::collections::HashMap<String, String>> {
        snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .filter_map(|(ck, _unit, _desc, _val)| {
                let (kind, key) = ck.into_parts();
                if kind == MetricKind::Counter && key.name() == recorded::QUERY_LOG_EXECUTION_COUNT
                {
                    Some(
                        label_map(&key)
                            .into_iter()
                            .map(|(k, v)| (k.to_string(), v.to_string()))
                            .collect(),
                    )
                } else {
                    None
                }
            })
            .collect()
    }

    fn cache_read_event(query_id: Option<QueryId>) -> QueryExecutionEvent {
        QueryExecutionEvent {
            event: EventType::Query,
            sql_type: SqlQueryType::Read,
            query: None,
            query_id,
            parse_duration: None,
            upstream_duration: None,
            readyset_event: Some(ReadysetExecutionEvent::Other {
                duration: Duration::from_micros(100),
            }),
            noria_error: None,
            destination: None,
        }
    }

    #[tokio::test]
    async fn non_verbose_mode_includes_query_id_label() {
        let qid = QueryId::from_unparsed_select("select 1");
        let qid_str = qid.to_string();
        let event = cache_read_event(Some(qid));
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let guard = metrics::set_default_local_recorder(&recorder);
        new_logger(QueryLogMode::Enabled).handle_event(&event).await;
        drop(guard);

        let label_sets = find_execution_count_labels(&snapshotter);
        assert!(
            !label_sets.is_empty(),
            "expected at least one execution_count emission"
        );
        for labels in &label_sets {
            assert_eq!(
                labels.get("query_id").map(String::as_str),
                Some(qid_str.as_str()),
                "query_id label missing in non-verbose mode: {labels:?}"
            );
            assert!(
                !labels.contains_key("query"),
                "query label should only appear in verbose mode: {labels:?}"
            );
        }
    }

    #[tokio::test]
    async fn non_verbose_mode_omits_query_id_when_absent() {
        let event = cache_read_event(None);
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let guard = metrics::set_default_local_recorder(&recorder);
        new_logger(QueryLogMode::Enabled).handle_event(&event).await;
        drop(guard);

        let label_sets = find_execution_count_labels(&snapshotter);
        assert!(
            !label_sets.is_empty(),
            "expected at least one execution_count emission"
        );
        for labels in &label_sets {
            assert!(
                !labels.contains_key("query_id"),
                "query_id label should be absent when event.query_id is None: {labels:?}"
            );
            assert!(
                !labels.contains_key("query"),
                "query label should only appear in verbose mode: {labels:?}"
            );
        }
    }

    #[tokio::test]
    async fn verbose_mode_includes_query_id_label() {
        let stmt = "select 1";
        let sql_query =
            readyset_sql_parsing::parse_query(DIALECT, stmt).expect("failed to parse test query");
        let qid = QueryId::from_unparsed_select("select 1 as a");
        let qid_str = qid.to_string();
        let mut event = cache_read_event(Some(qid));
        event.query = Some(Arc::new(sql_query));

        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let guard = metrics::set_default_local_recorder(&recorder);
        new_logger(QueryLogMode::Verbose).handle_event(&event).await;
        drop(guard);

        let label_sets = find_execution_count_labels(&snapshotter);
        assert!(
            !label_sets.is_empty(),
            "expected at least one execution_count emission"
        );
        for labels in &label_sets {
            assert_eq!(
                labels.get("query_id").map(String::as_str),
                Some(qid_str.as_str()),
                "query_id label missing in verbose mode: {labels:?}"
            );
        }
    }
}
