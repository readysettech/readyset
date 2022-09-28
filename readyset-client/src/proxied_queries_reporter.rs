use std::collections::HashSet;

use async_trait::async_trait;
use readyset::query::{DeniedQuery, QueryId};
use readyset_sql_passes::anonymize::Anonymizer;
use readyset_telemetry_reporter::{
    PeriodicReport, ReporterResult as Result, Telemetry, TelemetryBuilder, TelemetryEvent,
};
use tokio::sync::Mutex;

use crate::query_status_cache::QueryStatusCache;

pub struct ProxiedQueriesReporter {
    query_status_cache: &'static QueryStatusCache,
    reported_query_ids: Mutex<HashSet<QueryId>>,
    anonymizer: Mutex<Anonymizer>,
}

impl ProxiedQueriesReporter {
    pub fn new(query_status_cache: &'static QueryStatusCache) -> Self {
        Self {
            query_status_cache,
            reported_query_ids: Mutex::new(HashSet::new()),
            anonymizer: Mutex::new(Anonymizer::new()),
        }
    }

    // Returns Some if we should report the query, along with the telemetry to report
    // If we have already reported the query, returns None
    pub async fn report_query(
        &self,
        query: &mut DeniedQuery,
    ) -> Option<(TelemetryEvent, Telemetry)> {
        tracing::debug!(?query, "reporting query");
        let mut reported_query_ids = self.reported_query_ids.lock().await;
        let mut anonymizer = self.anonymizer.lock().await;
        match reported_query_ids.insert(query.id.clone()) {
            true => None,
            false => {
                let anon_q = query.query.to_anonymized_string(&mut anonymizer);
                Some((
                    TelemetryEvent::ProxiedQuery,
                    TelemetryBuilder::new().proxied_query(anon_q).build(),
                ))
            }
        }
    }
}

#[async_trait]
impl PeriodicReport for ProxiedQueriesReporter {
    async fn report(&self) -> Result<Vec<(TelemetryEvent, Telemetry)>> {
        tracing::debug!("running report for proxied queries");
        let mut denied_queries = self.query_status_cache.deny_list();
        Ok(futures::future::join_all(
            denied_queries
                .iter_mut()
                .inspect(|q| tracing::debug!("{q:?}"))
                .map(|q| self.report_query(q)),
        )
        .await
        .into_iter()
        .flatten()
        .inspect(|(e, t)| tracing::debug!("{e:?} {t:?}"))
        .collect())
    }
}
