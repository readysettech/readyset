use std::collections::HashMap;

use indexmap::IndexMap;
use metrics::SharedString;
use metrics_exporter_prometheus::formatting::{sanitize_label_key, sanitize_label_value};
use metrics_exporter_prometheus::{Distribution, PrometheusHandle};
// adding an alias to disambiguate vs readyset_client_metrics::recorded
use readyset_client::metrics::recorded as client_recorded;
use readyset_client_metrics::recorded::QUERY_LOG_EXECUTION_COUNT;
use readyset_client_metrics::DatabaseType;
use readyset_data::TimestampTz;

#[derive(Debug, Default, Clone)]
pub struct MetricsSummary {
    pub sample_count: u64,
}

#[derive(Clone)]
pub struct MetricsHandle {
    inner: PrometheusHandle,
    snapshot: Option<HashMap<String, u64>>,
}

impl MetricsHandle {
    /// Create a new [`MetricsHandle`] given a [`PrometheusHandle`]
    pub fn new(prometheus_handle: PrometheusHandle) -> Self {
        Self {
            inner: prometheus_handle,
            snapshot: None,
        }
    }

    /// Returns a snapshot of the counters held by [`Self`]. Optionally filters by `filter`.
    pub fn counters<F: Fn(&str) -> bool>(
        &self,
        filter: Option<F>,
    ) -> HashMap<String, HashMap<Vec<String>, u64>> {
        self.inner.counters(filter)
    }

    /// Returns a snapshot of the gauges held by [`Self`]. Optionally filters by `filter`.
    pub fn gauges<F: Fn(&str) -> bool>(
        &self,
        filter: Option<F>,
    ) -> HashMap<String, HashMap<Vec<String>, f64>> {
        self.inner.gauges(filter)
    }

    /// Returns a snapshot of the histograms and summaries held by [`Self`]. Optionall filters by
    /// `filter`.
    pub fn histograms<F: Fn(&str) -> bool>(
        &self,
        filter: Option<F>,
    ) -> HashMap<String, IndexMap<Vec<String>, Distribution>> {
        self.inner.distributions(filter)
    }

    /// Clone a snapshot of all QUERY_LOG_EXECUTION_COUNT counters.
    pub fn snapshot_counters(&mut self, database_type: DatabaseType) {
        fn filter(key: &str) -> bool {
            key == QUERY_LOG_EXECUTION_COUNT
        }

        let db_type = SharedString::from(database_type).to_string();

        let counters = self
            .counters(Some(filter))
            .get(QUERY_LOG_EXECUTION_COUNT)
            .cloned()
            .map(|h| {
                h.into_iter()
                    .filter_map(|(k, count)| {
                        let mut query_id_tag = None;
                        for tag in k {
                            if tag.starts_with("query_id") {
                                query_id_tag = Some(tag);
                            } else if tag.starts_with("database_type") && !tag.contains(&db_type) {
                                return None;
                            }
                        }

                        query_id_tag.map(|k| (k, count))
                    })
                    .collect::<HashMap<_, _>>()
            });
        self.snapshot = counters;
    }

    /// Returns the execution count query specified by `query_id`.
    ///
    /// NOTE: Values are queried from the last snapshot obtained by calling
    /// [`Self::snapshot_counters`]
    pub fn metrics_summary(&self, query_id: String) -> Option<MetricsSummary> {
        let label = format!(
            "{}=\"{}\"",
            sanitize_label_key("query_id"),
            sanitize_label_value(&query_id)
        );
        let summary = self.snapshot.as_ref()?.get(&label).or(Some(&0))?;

        Some(MetricsSummary {
            sample_count: *summary,
        })
    }

    fn sum_counter(&self, name: &str) -> u64 {
        self.counters(Some(|x: &str| x.eq(name)))
            .get(name)
            .map(|res| res.values().sum())
            .unwrap_or(0)
    }

    fn sum_gauge(&self, name: &str) -> f64 {
        self.gauges(Some(|x: &str| x.eq(name)))
            .get(name)
            .map(|res| res.values().sum())
            .unwrap_or(0.0)
    }

    /// Gather all the metrics that are relevant to be printed with
    /// `SHOW READYSET STATUS`
    pub fn readyset_status(&self) -> Vec<(String, String)> {
        let mut statuses = Vec::new();

        let time_ms = self.sum_counter(client_recorded::NORIA_STARTUP_TIMESTAMP);
        let time = TimestampTz::from_unix_ms(time_ms);
        statuses.push(("Process start time".to_string(), time.to_string()));

        let val = self.sum_gauge(readyset_client_metrics::recorded::CONNECTED_CLIENTS);
        statuses.push(("Connected clients count".to_string(), val.to_string()));

        let val = self.sum_gauge(readyset_client_metrics::recorded::CLIENT_UPSTREAM_CONNECTIONS);
        statuses.push((
            "Upstream database connection count".to_string(),
            val.to_string(),
        ));

        let val = self.sum_counter(readyset_client_metrics::recorded::QUERY_LOG_PARSE_ERRORS);
        statuses.push(("Query parse failures".to_string(), val.to_string()));

        let val = self.sum_counter(readyset_client_metrics::recorded::QUERY_LOG_SET_DISALLOWED);
        statuses.push((
            "SET statement disallowed count".to_string(),
            val.to_string(),
        ));

        let val = self.sum_counter(readyset_client_metrics::recorded::QUERY_LOG_VIEW_NOT_FOUND);
        statuses.push(("View not found count".to_string(), val.to_string()));

        let val = self.sum_counter(readyset_client_metrics::recorded::QUERY_LOG_RPC_ERRORS);
        statuses.push(("RPC error count".to_string(), val.to_string()));

        statuses
    }
}
