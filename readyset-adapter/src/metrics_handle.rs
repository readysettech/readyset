use std::collections::HashMap;

use indexmap::IndexMap;
use metrics_exporter_prometheus::formatting::{sanitize_label_key, sanitize_label_value};
use metrics_exporter_prometheus::{Distribution, PrometheusHandle};
use metrics_util::Summary;
use quanta::Instant;
// adding an alias to disambiguate vs readyset_client_metrics::recorded
use readyset_client::metrics::recorded as client_recorded;
use readyset_client_metrics::recorded::QUERY_LOG_EXECUTION_TIME;

#[derive(Debug, Default, Clone)]
pub struct MetricsSummary {
    // i64 because Postgres doesn't have unsigned ints
    pub sample_count: i64,
    pub p50_us: f64,
    pub p90_us: f64,
    pub p99_us: f64,
}

#[derive(Clone)]
pub struct MetricsHandle {
    inner: PrometheusHandle,
    snapshot: Option<HashMap<String, Summary>>,
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

    /// Clone a snapshot of all QUERY_LOG_EXECUTION_TIME histograms.
    pub fn snapshot_histograms(&mut self) {
        fn filter(key: &str) -> bool {
            key == QUERY_LOG_EXECUTION_TIME
        }

        let histograms = self
            .histograms(Some(filter))
            .get(QUERY_LOG_EXECUTION_TIME)
            .cloned()
            .map(|h| {
                h.into_iter()
                    .filter_map(|(k, dist)| {
                        k.into_iter()
                            .find(|k| k.starts_with("query_id"))
                            .and_then(|k| {
                                let summary = match dist {
                                    Distribution::Summary(summary, _, _) => {
                                        Some(summary.snapshot(Instant::now()))
                                    }
                                    _ => None,
                                };
                                summary.map(|s| (k, s))
                            })
                    })
                    .collect::<HashMap<_, _>>()
            });
        self.snapshot = histograms;
    }

    /// Return the count (number of samples), 0.5, 0.9 and 0.99 quantiles for the query specified by
    /// `query_id`.
    ///
    /// NOTE: Quantiles are queried from the last snapshot obtained by calling
    /// [`Self::snapshot_histograms`]
    pub fn metrics_summary(&self, query_id: String) -> Option<MetricsSummary> {
        let label = format!(
            "{}=\"{}\"",
            sanitize_label_key("query_id"),
            sanitize_label_value(&query_id)
        );
        let summary = self.snapshot.as_ref()?.get(&label)?;

        Some(MetricsSummary {
            sample_count: summary.count().try_into().unwrap_or_default(),
            p50_us: summary.quantile(0.5).unwrap_or_default(),
            p90_us: summary.quantile(0.90).unwrap_or_default(),
            p99_us: summary.quantile(0.99).unwrap_or_default(),
        })
    }

    fn sum_counter(&self, name: &str) -> u64 {
        let results = self.counters(Some(|x: &str| x.eq(name)));
        match results.get(name) {
            Some(res) => {
                let mut f = 0;
                for (_, v) in res.iter() {
                    f += v;
                }
                f
            }
            None => 0,
        }
    }

    /// Gather all the metrics that are relevant to be printed with
    /// `SHOW READYSET STATUS`
    pub fn readyset_status(&self) -> Vec<(String, String)> {
        let mut statuses = Vec::new();

        let val = self.sum_counter(client_recorded::NORIA_STARTUP_TIMESTAMP);
        statuses.push(("Process start time".to_string(), val.to_string()));

        let val = self.sum_counter(readyset_client_metrics::recorded::QUERY_LOG_PARSE_ERRORS);
        statuses.push(("Query parse failures".to_string(), val.to_string()));

        statuses
    }
}
