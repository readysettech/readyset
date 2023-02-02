use std::collections::HashMap;

use async_trait::async_trait;
use readyset_client::query::{DeniedQuery, MigrationState, QueryId};
use readyset_sql_passes::anonymize::Anonymizer;
use readyset_telemetry_reporter::{
    PeriodicReport, ReporterResult as Result, Telemetry, TelemetryBuilder, TelemetryEvent,
};
use readyset_tracing::debug;
use tokio::sync::Mutex;

use crate::query_status_cache::QueryStatusCache;

pub struct ProxiedQueriesReporter {
    query_status_cache: &'static QueryStatusCache,
    reported_queries: Mutex<HashMap<QueryId, MigrationState>>,
    anonymizer: Mutex<Anonymizer>,
}

impl ProxiedQueriesReporter {
    pub fn new(query_status_cache: &'static QueryStatusCache) -> Self {
        Self {
            query_status_cache,
            reported_queries: Mutex::new(HashMap::new()),
            anonymizer: Mutex::new(Anonymizer::new()),
        }
    }

    // Returns Some if we should report the query, along with the telemetry to report
    // If we have already reported the query, returns None
    pub async fn report_query(
        &self,
        query: &mut DeniedQuery,
    ) -> Option<(TelemetryEvent, Telemetry)> {
        debug!(?query, "reporting query");
        let mut reported_queries = self.reported_queries.lock().await;
        let mut anonymizer = self.anonymizer.lock().await;
        let mut build_event = || {
            let anon_q = query.query.to_anonymized_string(&mut anonymizer);
            Some((
                TelemetryEvent::ProxiedQuery,
                TelemetryBuilder::new()
                    .proxied_query(anon_q)
                    .migration_status(query.status.migration_state.to_string())
                    .build(),
            ))
        };

        match reported_queries.insert(query.id, query.status.migration_state) {
            Some(old_migration_state) => {
                // Check whether we know of a new migration state for this query. Send an event if
                // so
                if old_migration_state != query.status.migration_state {
                    build_event()
                } else {
                    None
                }
            }
            None => {
                // First time inserting this query
                build_event()
            }
        }
    }
}

#[async_trait]
impl PeriodicReport for ProxiedQueriesReporter {
    async fn report(&self) -> Result<Vec<(TelemetryEvent, Telemetry)>> {
        debug!("running report for proxied queries");
        let mut denied_queries = self.query_status_cache.deny_list();
        Ok(futures::future::join_all(
            denied_queries
                .iter_mut()
                .inspect(|q| debug!("{q:?}"))
                .map(|q| self.report_query(q)),
        )
        .await
        .into_iter()
        .flatten()
        .inspect(|(e, t)| debug!("{e:?} {t:?}"))
        .collect())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use readyset_client::query::{Query, QueryStatus};

    use super::*;
    use crate::query_status_cache::MigrationStyle;

    #[tokio::test]
    async fn test_update_migration_state() {
        let query_status_cache = Box::leak(Box::new(
            QueryStatusCache::new().style(MigrationStyle::Explicit),
        ));
        let proxied_queries_reporter = Arc::new(ProxiedQueriesReporter::new(query_status_cache));

        let query_id = QueryId::new(42);
        let mut init_q = DeniedQuery {
            id: query_id,
            query: Query::ParseFailed(Arc::new(
                "this is easier than making a view create request".to_string(),
            )),
            status: QueryStatus {
                migration_state: MigrationState::Pending,
                execution_info: None,
                always: false,
            },
        };
        proxied_queries_reporter.report_query(&mut init_q).await;
        let status = {
            let queries = proxied_queries_reporter.reported_queries.lock().await;
            *queries.get(&query_id).expect("query should be mapped")
        };
        assert_eq!(MigrationState::Pending, status);

        let mut updated_q = DeniedQuery {
            id: query_id,
            query: Query::ParseFailed(Arc::new(
                "this is easier than making a view create request".to_string(),
            )),
            status: QueryStatus {
                migration_state: MigrationState::Successful,
                execution_info: None,
                always: false,
            },
        };
        proxied_queries_reporter.report_query(&mut updated_q).await;
        let status = {
            let queries = proxied_queries_reporter.reported_queries.lock().await;
            *queries.get(&query_id).expect("query should be mapped")
        };
        assert_eq!(MigrationState::Successful, status);
    }
}
