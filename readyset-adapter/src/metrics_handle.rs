use std::collections::HashMap;

use indexmap::IndexMap;
use metrics_exporter_prometheus::formatting::{sanitize_label_key, sanitize_label_value};
use metrics_exporter_prometheus::{Distribution, PrometheusHandle};
use metrics_util::Summary;
use quanta::Instant;
use readyset_client_metrics::recorded::QUERY_LOG_EXECUTION_TIME;

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

    /// Return the 0.5, 0.9 and 0.99 quantiles for the query specified by `query_id`.
    ///
    /// NOTE: Quantiles are queried from the last snapshot obtained by calling
    /// [`Self::snapshot_histograms`]
    pub fn quantiles(&self, query_id: String) -> Option<(f64, f64, f64)> {
        let label = format!(
            "{}=\"{}\"",
            sanitize_label_key("query_id"),
            sanitize_label_value(&query_id)
        );
        let summary = self.snapshot.as_ref()?.get(&label)?;

        Some((
            summary.quantile(0.5).unwrap_or_default(),
            summary.quantile(0.90).unwrap_or_default(),
            summary.quantile(0.99).unwrap_or_default(),
        ))
    }
}
