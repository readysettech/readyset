use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::{PoisonError, RwLock};
use std::time::Duration;

use hyper::body::{aggregate, Buf};
use hyper::header::HeaderValue;
use hyper::{Body, Client, Method, Request, Uri};
use indexmap::IndexMap;
use metrics::HistogramFn;
use metrics::{Counter, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit};
use metrics_exporter_prometheus::formatting::{
    key_to_parts, sanitize_metric_name, write_help_line, write_metric_line, write_type_line,
};
use metrics_exporter_prometheus::{BuildError, Distribution, DistributionBuilder};
use metrics_util::registry::{Recency, Registry};
use metrics_util::MetricKindMask;
use metrics_util::{registry::GenerationalStorage, AtomicBucket};
use quanta::Instant;
use tracing::error;

use crate::metrics::noria_recorder::NoriaMetricsRecorder;
use crate::metrics::{Clear, Render};

/// The name for the Recorder as stored in CompositeMetricsRecorder.
pub enum MetricsRecorder {
    /// A recorder for ReadySet-style metrics.
    Noria(NoriaMetricsRecorder),
    /// A recorder for Prometheus.
    Prometheus(PrometheusRecorder),
}

impl Render for MetricsRecorder {
    fn render(&self) -> String {
        match self {
            MetricsRecorder::Noria(nmr) => nmr.render(),
            MetricsRecorder::Prometheus(pr) => pr.render(),
        }
    }
}

impl Clear for MetricsRecorder {
    fn clear(&self) -> bool {
        match self {
            MetricsRecorder::Noria(nmr) => nmr.clear(),
            MetricsRecorder::Prometheus(pr) => pr.clear(),
        }
    }
}

struct Snapshot {
    pub counters: HashMap<String, HashMap<Vec<String>, u64>>,
    pub gauges: HashMap<String, HashMap<Vec<String>, f64>>,
    pub distributions: HashMap<String, IndexMap<Vec<String>, Distribution>>,
}

pub type GenerationalAtomicStorage = GenerationalStorage<AtomicStorage>;

/// Atomic metric storage for the prometheus exporter.
pub struct AtomicStorage;

impl<K> metrics_util::registry::Storage<K> for AtomicStorage {
    type Counter = Arc<AtomicU64>;
    type Gauge = Arc<AtomicU64>;
    type Histogram = Arc<AtomicBucketInstant<f64>>;

    fn counter(&self, _: &K) -> Self::Counter {
        Arc::new(AtomicU64::new(0))
    }

    fn gauge(&self, _: &K) -> Self::Gauge {
        Arc::new(AtomicU64::new(0))
    }

    fn histogram(&self, _: &K) -> Self::Histogram {
        Arc::new(AtomicBucketInstant::new())
    }
}

/// An `AtomicBucket` newtype wrapper that tracks the time of value insertion.
pub struct AtomicBucketInstant<T> {
    inner: AtomicBucket<(T, Instant)>,
}

impl<T> AtomicBucketInstant<T> {
    fn new() -> AtomicBucketInstant<T> {
        Self {
            inner: AtomicBucket::new(),
        }
    }

    pub fn clear_with<F>(&self, f: F)
    where
        F: FnMut(&[(T, Instant)]),
    {
        self.inner.clear_with(f);
    }
}

impl HistogramFn for AtomicBucketInstant<f64> {
    fn record(&self, value: f64) {
        let now = Instant::now();
        self.inner.push((value, now));
    }
}

pub(crate) struct Inner {
    pub registry: Registry<Key, GenerationalAtomicStorage>,
    pub recency: Recency<Key>,
    pub distributions: RwLock<HashMap<String, IndexMap<Vec<String>, Distribution>>>,
    pub distribution_builder: DistributionBuilder,
    pub descriptions: RwLock<HashMap<String, SharedString>>,
    pub global_labels: IndexMap<String, String>,
}

impl Inner {
    fn counters<F>(&self, filter: Option<F>) -> HashMap<String, HashMap<Vec<String>, u64>>
    where
        F: Fn(&str) -> bool,
    {
        let mut counters = HashMap::new();
        let counter_handles = self.registry.get_counter_handles();
        for (key, counter) in counter_handles {
            let gen = counter.get_generation();
            if !self.recency.should_store_counter(&key, gen, &self.registry) {
                continue;
            }

            let (name, labels) = key_to_parts(&key, Some(&self.global_labels));
            if filter.as_ref().map(|f| f(&name)).unwrap_or(true) {
                let value = counter.get_inner().load(Ordering::Acquire);
                let entry = counters
                    .entry(name)
                    .or_insert_with(HashMap::new)
                    .entry(labels)
                    .or_insert(0);
                *entry = value;
            }
        }
        counters
    }

    fn gauges<F>(&self, filter: Option<F>) -> HashMap<String, HashMap<Vec<String>, f64>>
    where
        F: Fn(&str) -> bool,
    {
        let mut gauges = HashMap::new();
        let gauge_handles = self.registry.get_gauge_handles();
        for (key, gauge) in gauge_handles {
            let gen = gauge.get_generation();
            if !self.recency.should_store_gauge(&key, gen, &self.registry) {
                continue;
            }

            let (name, labels) = key_to_parts(&key, Some(&self.global_labels));
            if filter.as_ref().map(|f| f(&name)).unwrap_or(true) {
                let value = f64::from_bits(gauge.get_inner().load(Ordering::Acquire));
                let entry = gauges
                    .entry(name)
                    .or_insert_with(HashMap::new)
                    .entry(labels)
                    .or_insert(0.0);
                *entry = value;
            }
        }
        gauges
    }

    fn distributions<F>(
        &self,
        filter: Option<F>,
    ) -> HashMap<String, IndexMap<Vec<String>, Distribution>>
    where
        F: Fn(&str) -> bool,
    {
        // Update distributions
        self.drain_histograms_to_distributions();
        // Remove expired histograms
        let histogram_handles = self.registry.get_histogram_handles();
        for (key, histogram) in histogram_handles {
            let gen = histogram.get_generation();
            if !self
                .recency
                .should_store_histogram(&key, gen, &self.registry)
            {
                // Since we store aggregated distributions directly, when we're told that a metric
                // is not recent enough and should be/was deleted from the registry, we also need to
                // delete it on our side as well.
                let (name, labels) = key_to_parts(&key, Some(&self.global_labels));
                let mut wg = self
                    .distributions
                    .write()
                    .unwrap_or_else(PoisonError::into_inner);
                let delete_by_name = if let Some(by_name) = wg.get_mut(&name) {
                    by_name.swap_remove(&labels);
                    by_name.is_empty()
                } else {
                    false
                };

                // If there's no more variants in the per-metric-name distribution map, then delete
                // it entirely, otherwise we end up with weird empty output during render.
                if delete_by_name {
                    wg.remove(&name);
                }

                continue;
            }
        }

        let distributions = self
            .distributions
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .filter_map(|(key, map)| {
                if filter.as_ref().map(|f| f(key)).unwrap_or(true) {
                    Some((key.clone(), map.clone()))
                } else {
                    None
                }
            })
            .collect();
        distributions
    }

    fn get_recent_metrics(&self) -> Snapshot {
        Snapshot {
            counters: self.counters(None::<fn(&str) -> bool>),
            gauges: self.gauges(None::<fn(&str) -> bool>),
            distributions: self.distributions(None::<fn(&str) -> bool>),
        }
    }

    /// Drains histogram samples into distribution.
    fn drain_histograms_to_distributions(&self) {
        let histogram_handles = self.registry.get_histogram_handles();
        for (key, histogram) in histogram_handles {
            let (name, labels) = key_to_parts(&key, Some(&self.global_labels));

            let mut wg = self
                .distributions
                .write()
                .unwrap_or_else(PoisonError::into_inner);
            let entry = wg
                .entry(name.clone())
                .or_default()
                .entry(labels)
                .or_insert_with(|| self.distribution_builder.get_distribution(name.as_str()));

            histogram
                .get_inner()
                .clear_with(|samples| entry.record_samples(samples));
        }
    }

    fn render(&self) -> String {
        let Snapshot {
            mut counters,
            mut distributions,
            mut gauges,
        } = self.get_recent_metrics();

        let mut output = String::new();
        let descriptions = self
            .descriptions
            .read()
            .unwrap_or_else(PoisonError::into_inner);

        for (name, mut by_labels) in counters.drain() {
            if let Some(desc) = descriptions.get(name.as_str()) {
                write_help_line(&mut output, name.as_str(), desc);
            }

            write_type_line(&mut output, name.as_str(), "counter");
            for (labels, value) in by_labels.drain() {
                write_metric_line::<&str, u64>(&mut output, &name, None, &labels, None, value);
            }
            output.push('\n');
        }

        for (name, mut by_labels) in gauges.drain() {
            if let Some(desc) = descriptions.get(name.as_str()) {
                write_help_line(&mut output, name.as_str(), desc);
            }

            write_type_line(&mut output, name.as_str(), "gauge");
            for (labels, value) in by_labels.drain() {
                write_metric_line::<&str, f64>(&mut output, &name, None, &labels, None, value);
            }
            output.push('\n');
        }

        for (name, mut by_labels) in distributions.drain() {
            if let Some(desc) = descriptions.get(name.as_str()) {
                write_help_line(&mut output, name.as_str(), desc);
            }

            let distribution_type = self
                .distribution_builder
                .get_distribution_type(name.as_str());
            write_type_line(&mut output, name.as_str(), distribution_type);
            for (labels, distribution) in by_labels.drain(..) {
                let (sum, count) = match distribution {
                    Distribution::Summary(summary, quantiles, sum) => {
                        let snapshot = summary.snapshot(Instant::now());
                        for quantile in quantiles.iter() {
                            let value = snapshot.quantile(quantile.value()).unwrap_or(0.0);
                            write_metric_line(
                                &mut output,
                                &name,
                                None,
                                &labels,
                                Some(("quantile", quantile.value())),
                                value,
                            );
                        }

                        (sum, summary.count() as u64)
                    }
                    Distribution::Histogram(histogram) => {
                        for (le, count) in histogram.buckets() {
                            write_metric_line(
                                &mut output,
                                &name,
                                Some("bucket"),
                                &labels,
                                Some(("le", le)),
                                count,
                            );
                        }
                        write_metric_line(
                            &mut output,
                            &name,
                            Some("bucket"),
                            &labels,
                            Some(("le", "+Inf")),
                            histogram.count(),
                        );

                        (histogram.sum(), histogram.count())
                    }
                };

                write_metric_line::<&str, f64>(&mut output, &name, Some("sum"), &labels, None, sum);
                write_metric_line::<&str, u64>(
                    &mut output,
                    &name,
                    Some("count"),
                    &labels,
                    None,
                    count,
                );
            }

            output.push('\n');
        }

        output
    }

    fn run_upkeep(&self) {
        self.drain_histograms_to_distributions();
    }
}

/// A Prometheus recorder.
pub struct PrometheusRecorder {
    inner: Arc<Inner>,
}

impl PrometheusRecorder {
    /// Gets a [`PrometheusHandle`] to this recorder.
    pub fn handle(&self) -> PrometheusHandle {
        PrometheusHandle {
            inner: self.inner.clone(),
        }
    }

    fn add_description_if_missing(&self, key_name: &KeyName, description: SharedString) {
        let sanitized = sanitize_metric_name(key_name.as_str());
        let mut descriptions = self
            .inner
            .descriptions
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        descriptions.entry(sanitized).or_insert(description);
    }
}

impl From<Inner> for PrometheusRecorder {
    fn from(inner: Inner) -> Self {
        PrometheusRecorder {
            inner: Arc::new(inner),
        }
    }
}

impl Recorder for PrometheusRecorder {
    fn describe_counter(&self, key_name: KeyName, _unit: Option<Unit>, description: SharedString) {
        self.add_description_if_missing(&key_name, description);
    }

    fn describe_gauge(&self, key_name: KeyName, _unit: Option<Unit>, description: SharedString) {
        self.add_description_if_missing(&key_name, description);
    }

    fn describe_histogram(
        &self,
        key_name: KeyName,
        _unit: Option<Unit>,
        description: SharedString,
    ) {
        self.add_description_if_missing(&key_name, description);
    }

    fn register_counter(&self, key: &Key, _metadata: &Metadata<'_>) -> Counter {
        self.inner
            .registry
            .get_or_create_counter(key, |c| c.clone().into())
    }

    fn register_gauge(&self, key: &Key, _metadata: &Metadata<'_>) -> Gauge {
        self.inner
            .registry
            .get_or_create_gauge(key, |c| c.clone().into())
    }

    fn register_histogram(&self, key: &Key, _metadata: &Metadata<'_>) -> Histogram {
        self.inner
            .registry
            .get_or_create_histogram(key, |c| c.clone().into())
    }
}

impl Render for PrometheusRecorder {
    fn render(&self) -> String {
        self.inner.render()
    }
}

impl Clear for PrometheusRecorder {
    fn clear(&self) -> bool {
        false
    }
}

/// Handle for accessing metrics stored via [`PrometheusRecorder`].
///
/// In certain scenarios, it may be necessary to directly handle requests that would otherwise be
/// handled directly by the HTTP listener, or push gateway background task.  [`PrometheusHandle`]
/// allows rendering a snapshot of the current metrics stored by an installed [`PrometheusRecorder`]
/// as a payload conforming to the Prometheus exposition format.
#[derive(Clone)]
pub struct PrometheusHandle {
    inner: Arc<Inner>,
}

impl PrometheusHandle {
    /// Takes a snapshot of the metrics held by the recorder and generates a payload conforming to
    /// the Prometheus exposition format.
    pub fn render(&self) -> String {
        self.inner.render()
    }

    /// Performs upkeeping operations to ensure metrics held by recorder are up-to-date and do not
    /// grow unboundedly.
    pub fn run_upkeep(&self) {
        self.inner.run_upkeep();
    }

    /// Returns a snapshot of the counters held by [`Self`]. Optionally filters the returned
    /// counters by `filter`.
    pub fn counters<F>(&self, filter: Option<F>) -> HashMap<String, HashMap<Vec<String>, u64>>
    where
        F: Fn(&str) -> bool,
    {
        self.inner.counters(filter)
    }

    /// Returns a snapshot of the gauges held by [`Self`]. Optionally filters the returned gauges
    /// by `filter`.
    pub fn gauges<F>(&self, filter: Option<F>) -> HashMap<String, HashMap<Vec<String>, f64>>
    where
        F: Fn(&str) -> bool,
    {
        self.inner.gauges(filter)
    }

    /// Returns a snapshot of the histograms and summaries held by [`Self`]. Optionally filters the
    /// returned distributions by `filter`.
    pub fn distributions<F>(
        &self,
        filter: Option<F>,
    ) -> HashMap<String, IndexMap<Vec<String>, Distribution>>
    where
        F: Fn(&str) -> bool,
    {
        self.inner.distributions(filter)
    }
}

#[derive(Clone)]
struct PushGateway {
    endpoint: Uri,
    interval: Duration,
    username: Option<String>,
    password: Option<String>,
}

fn basic_auth(username: &str, password: Option<&str>) -> HeaderValue {
    use base64::prelude::BASE64_STANDARD;
    use base64::write::EncoderWriter;
    use std::io::Write;

    let mut buf = b"Basic ".to_vec();
    {
        let mut encoder = EncoderWriter::new(&mut buf, &BASE64_STANDARD);
        let _ = write!(encoder, "{username}:");
        if let Some(password) = password {
            let _ = write!(encoder, "{password}");
        }
    }
    let mut header = HeaderValue::from_bytes(&buf).expect("base64 is always valid HeaderValue");
    header.set_sensitive(true);
    header
}

fn exporter(
    handle: PrometheusHandle,
    username: Option<String>,
    password: Option<String>,
    interval: Duration,
    endpoint: Uri,
) -> impl std::future::Future<Output = Result<(), Box<dyn std::error::Error + Send>>> {
    Box::pin(async move {
        let client = Client::new();
        let auth = username
            .as_ref()
            .map(|name| basic_auth(name, password.as_deref()));

        loop {
            // Sleep for `interval` amount of time, and then do a push.
            tokio::time::sleep(interval).await;

            let mut builder = Request::builder();
            if let Some(auth) = &auth {
                builder = builder.header("authorization", auth.clone());
            }

            let output = handle.render();
            let result = builder
                .method(Method::PUT)
                .uri(endpoint.clone())
                .body(Body::from(output));
            let req = match result {
                Ok(req) => req,
                Err(e) => {
                    error!("failed to build push gateway request: {}", e);
                    continue;
                }
            };

            match client.request(req).await {
                Ok(response) => {
                    if !response.status().is_success() {
                        let status = response.status();
                        let status = status.canonical_reason().unwrap_or_else(|| status.as_str());
                        let body = aggregate(response.into_body()).await;
                        let body = body
                            .map_err(|_| ())
                            .map(|mut b| b.copy_to_bytes(b.remaining()))
                            .map(|b| b[..].to_vec())
                            .and_then(|s| String::from_utf8(s).map_err(|_| ()))
                            .unwrap_or_else(|_| String::from("<failed to read response body>"));
                        error!(
                            message = "unexpected status after pushing metrics to push gateway",
                            status,
                            %body,
                        );
                    }
                }
                Err(e) => error!("error sending request to push gateway: {:?}", e),
            }
        }
    })
}

#[derive(Default)]
pub struct PrometheusBuilder {
    global_labels: Option<IndexMap<String, String>>,
    push_gateway: Option<PushGateway>,
}

impl PrometheusBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_global_label<K, V>(mut self, key: K, value: V) -> Self
    where
        K: Into<String>,
        V: Into<String>,
    {
        let labels = self.global_labels.get_or_insert_with(IndexMap::new);
        labels.insert(key.into(), value.into());
        self
    }

    pub fn with_push_gateway<T>(
        mut self,
        endpoint: T,
        interval: Duration,
        username: Option<String>,
        password: Option<String>,
    ) -> Result<Self, BuildError>
    where
        T: AsRef<str>,
    {
        self.push_gateway = Some(PushGateway {
            endpoint: Uri::try_from(endpoint.as_ref())
                .map_err(|e| BuildError::InvalidPushGatewayEndpoint(e.to_string()))?,
            interval,
            username,
            password,
        });
        Ok(self)
    }

    async fn run_upkeep(handle: PrometheusHandle) {
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        loop {
            interval.tick().await;
            handle.run_upkeep();
        }
    }

    pub fn build_recorder(self) -> PrometheusRecorder {
        let quantiles = metrics_util::parse_quantiles(&[0.0, 0.5, 0.9, 0.95, 0.99, 0.999, 1.0]);

        let rec = PrometheusRecorder::from(Inner {
            registry: Registry::new(GenerationalStorage::new(AtomicStorage)),
            recency: Recency::new(quanta::Clock::new(), MetricKindMask::NONE, None),
            distributions: RwLock::new(HashMap::new()),
            distribution_builder: DistributionBuilder::new(quantiles, None, None, None, None),
            descriptions: RwLock::new(HashMap::new()),
            global_labels: self.global_labels.unwrap_or_default(),
        });

        let handle = rec.handle().clone();
        tokio::spawn(Self::run_upkeep(handle));

        rec
    }

    pub fn build(
        self,
    ) -> Result<
        (
            PrometheusRecorder,
            impl std::future::Future<Output = Result<(), Box<dyn std::error::Error + Send>>>,
        ),
        BuildError,
    > {
        let gw = self.push_gateway.clone().expect("gateway not configured");
        let recorder = self.build_recorder();
        let handle = recorder.handle();
        let exporter = exporter(handle, gw.username, gw.password, gw.interval, gw.endpoint);
        Ok((recorder, exporter))
    }
}
