use std::str::FromStr;
use std::sync::Arc;

use anyhow::Error;
use futures::future::ready;
use futures::io::AsyncBufReadExt;
use futures::stream::{Stream, TryStreamExt};
use serde::{Deserialize, Serialize};

pub mod metric;
use metric::Metric;

/// Convenience helper to construct a [ForwardPrometheusMetrics] from components; takes a
/// [ PrometheusEndpoint]instead of a `&str` or `String` because it is generally expected that
/// `PrometheusEndpoint` will be used in a benchmark's [Parser](clap::Parser)
pub fn forward(
    endpoint: PrometheusEndpoint,
    filter_predicate: fn(&Metric) -> bool,
) -> ForwardPrometheusMetrics {
    ForwardPrometheusMetrics {
        endpoint,
        filter_predicate,
    }
}

/// Represents a "set" of Prometheus endpoints to re-export as part of this benchmark - for
/// example, the write latency benchmark pulls down write propagation time metrics from Noria and
/// re-exports them.
///
/// Represented as an HTTP URL, newtyped in [PrometheusEndpoint], and a filter function.
#[derive(Clone)]
pub struct ForwardPrometheusMetrics {
    endpoint: PrometheusEndpoint,
    filter_predicate: fn(&Metric) -> bool,
}

impl ForwardPrometheusMetrics {
    /// Returns a stream of parsed Metrics that match our filter
    pub async fn metrics(
        &self,
    ) -> Result<impl Stream<Item = Result<Metric, metric::parser::Error>>, Error> {
        self.endpoint
            .metrics_with_filter(self.filter_predicate)
            .await
    }

    /// It would be nice to run these through our own exporter, but the `metrics` API is missing
    /// some functionality that we would need - for example, writing absolute values to counters -
    /// so the play currently is to parse the metrics, add our labels, and then hand-write the
    /// request to send them to our push gateway.
    async fn render_metrics(
        self,
        global_labels: Arc<Vec<(String, String)>>,
    ) -> Result<impl Stream<Item = Result<String, metric::parser::Error>> + 'static, Error> {
        let stream = self
            .endpoint
            .metrics_with_filter(self.filter_predicate)
            .await?
            .map_ok(move |mut m| {
                m.add_global_labels(&global_labels);
                m.to_string()
            });
        Ok(stream)
    }

    /// Take a `reqwest::RequestBuilder` and `Arc<Vec>` containing the benchmark's global labels,
    /// pull down metrics from `self.endpoint`, apply `self.filter_predicate`, add all the global
    /// labels, and send to the push gateway that was configured in `req`
    pub async fn forward(
        self,
        req: reqwest::RequestBuilder,
        global_labels: Arc<Vec<(String, String)>>,
    ) -> Result<(), Error> {
        let body = reqwest::Body::wrap_stream(self.render_metrics(global_labels.clone()).await?);
        req.body(body).send().await?;
        Ok(())
    }
}

/// Newtype wrapper around a string containing a URL that provides functions, used internally by
/// the benchmark framework, to stream metrics; implements `FromStr`, so it can be used in a
/// [clap::Parser].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PrometheusEndpoint {
    metrics_url: String,
}

impl FromStr for PrometheusEndpoint {
    type Err = !;
    fn from_str(metrics_url: &str) -> Result<Self, Self::Err> {
        Ok(Self {
            metrics_url: metrics_url.into(),
        })
    }
}

impl PrometheusEndpoint {
    async fn metrics(
        &self,
    ) -> Result<impl Stream<Item = Result<Metric, metric::parser::Error>>, Error> {
        Ok(metric::parser::parse(self.lines().await?))
    }

    async fn metrics_with_filter<F>(
        &self,
        predicate: F,
    ) -> Result<impl Stream<Item = Result<Metric, metric::parser::Error>>, Error>
    where
        F: Fn(&Metric) -> bool,
    {
        Ok(self
            .metrics()
            .await?
            .try_filter(move |m| ready(predicate(m))))
    }

    async fn lines(&self) -> Result<impl Stream<Item = Result<String, std::io::Error>>, Error> {
        let stream = reqwest::get(&self.metrics_url)
            .await?
            .error_for_status()?
            .bytes_stream()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
            .into_async_read()
            .lines();
        Ok(stream)
    }
}
