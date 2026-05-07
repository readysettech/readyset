use std::collections::{HashMap, HashSet};
use std::num::NonZeroU32;
use std::ops::AddAssign;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::{LazyLock, PoisonError, RwLock};
use std::time::Duration;

use indexmap::IndexMap;
use metrics::HistogramFn;
use metrics::{Counter, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit};
#[cfg(feature = "push-gateway")]
use metrics_exporter_prometheus::BuildError;
use metrics_exporter_prometheus::formatting::{
    sanitize_metric_name, write_help_line, write_metric_line, write_type_line,
};
use metrics_exporter_prometheus::{Distribution, DistributionBuilder, LabelSet};
use metrics_util::MetricKindMask;
use metrics_util::registry::{Recency, Registry};
use metrics_util::{registry::GenerationalStorage, storage::AtomicBucket};
use quanta::Instant;
#[cfg(feature = "push-gateway")]
use tracing::error;
use tracing::warn;

/// Quantile cutoffs Readyset exposes for summary metrics.
const SUMMARY_QUANTILES: &[f64] = &[0.0, 0.5, 0.9, 0.95, 0.99, 0.999, 1.0];

// Default filler values extracted from metrics_exporter_prometheus::DistributionBuilder.
const SUMMARY_BUCKET_DURATION: Duration = Duration::from_secs(20);
const SUMMARY_BUCKET_COUNT: NonZeroU32 = NonZeroU32::new(3).expect("3 is nonzero");

static EMPTY_SUMMARY: LazyLock<Distribution> = LazyLock::new(|| {
    Distribution::new_summary(
        Arc::new(metrics_util::parse_quantiles(SUMMARY_QUANTILES)),
        SUMMARY_BUCKET_DURATION,
        SUMMARY_BUCKET_COUNT,
    )
});

pub(crate) type GenerationalAtomicStorage = GenerationalStorage<AtomicStorage>;

/// Atomic metric storage for the prometheus exporter.
pub(crate) struct AtomicStorage;

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
pub(crate) struct AtomicBucketInstant<T> {
    inner: AtomicBucket<(T, Instant)>,
}

impl<T> AtomicBucketInstant<T> {
    fn new() -> AtomicBucketInstant<T> {
        Self {
            inner: AtomicBucket::new(),
        }
    }

    pub(crate) fn clear_with<F>(&self, f: F)
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
    pub distributions: RwLock<HashMap<String, IndexMap<Key, Distribution>>>,
    pub distribution_builder: DistributionBuilder,
    pub descriptions: RwLock<HashMap<String, SharedString>>,
    pub global_labels: IndexMap<String, String>,
}

/// Walk `handles`, drop entries the recency policy has expired, and group the rest by metric
/// name.  `extract` reads the per-handle value; `name_filter`, if `Some`, restricts the result
/// to those names.
fn collect_handles<I, H, V>(
    handles: I,
    is_recent: impl Fn(&Key, &H) -> bool,
    extract: impl Fn(&H) -> V,
    name_filter: Option<&[&str]>,
) -> HashMap<String, HashMap<Key, V>>
where
    I: IntoIterator<Item = (Key, H)>,
{
    let mut out = HashMap::new();
    for (key, handle) in handles {
        if !is_recent(&key, &handle) {
            continue;
        }
        if !name_filter.is_none_or(|names| names.contains(&key.name())) {
            continue;
        }
        out.entry(key.name().to_owned())
            .or_insert_with(HashMap::new)
            .insert(key, extract(&handle));
    }
    out
}

impl Inner {
    /// Counter values keyed by [`Key`] so callers can walk labels without re-parsing exposition
    /// strings.  `name_filter`, if `Some`, restricts the result to those metric names.
    fn counters(&self, name_filter: Option<&[&str]>) -> HashMap<String, HashMap<Key, u64>> {
        let handles = self.registry.get_counter_handles();
        collect_handles(
            handles,
            |k, c| {
                self.recency
                    .should_store_counter(k, c.get_generation(), &self.registry)
            },
            |c| c.get_inner().load(Ordering::Acquire),
            name_filter,
        )
    }

    /// Gauge values; see [`Inner::counters`].
    fn gauges(&self, name_filter: Option<&[&str]>) -> HashMap<String, HashMap<Key, f64>> {
        let handles = self.registry.get_gauge_handles();
        collect_handles(
            handles,
            |k, g| {
                self.recency
                    .should_store_gauge(k, g.get_generation(), &self.registry)
            },
            |g| f64::from_bits(g.get_inner().load(Ordering::Acquire)),
            name_filter,
        )
    }

    /// Walk distribution storage under the read lock and invoke `per_metric` once per requested
    /// metric name, passing the matching entries (or `None` if absent).
    fn for_distribution_batch<const M: usize, T>(
        &self,
        metrics: [&str; M],
        per_metric: impl Fn(usize, Option<&IndexMap<Key, Distribution>>) -> T,
    ) -> [T; M] {
        self.refresh_distributions();
        let guard = self
            .distributions
            .read()
            .unwrap_or_else(PoisonError::into_inner);
        std::array::from_fn(|i| per_metric(i, guard.get(metrics[i])))
    }

    /// In one locked pass over the histogram registry: drain samples from
    /// active histograms into their `Distribution` entries, and remove
    /// storage entries for histograms expired by the recency policy.
    fn refresh_distributions(&self) {
        let handles = self.registry.get_histogram_handles();
        if handles.is_empty() {
            return;
        }
        let mut wg = self
            .distributions
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        for (key, histogram) in handles {
            let name = key.name().to_owned();
            if self
                .recency
                .should_store_histogram(&key, histogram.get_generation(), &self.registry)
            {
                let entry = wg
                    .entry(name.clone())
                    .or_default()
                    .entry(key)
                    .or_insert_with(|| self.distribution_builder.get_distribution(name.as_str()));
                histogram
                    .get_inner()
                    .clear_with(|samples| entry.record_samples(samples));
            } else if let Some(by_name) = wg.get_mut(&name) {
                by_name.swap_remove(&key);
                if by_name.is_empty() {
                    wg.remove(&name);
                }
            }
        }
    }

    fn render(&self) -> String {
        let counters = self.counters(None);
        let gauges = self.gauges(None);
        // Distributions read storage directly so the IndexMap insertion order is
        // preserved on the wire (counters/gauges have no such storage and have
        // always rendered in random HashMap order).
        self.refresh_distributions();
        let distributions = self
            .distributions
            .read()
            .unwrap_or_else(PoisonError::into_inner);

        let mut output = String::new();
        let descriptions = self
            .descriptions
            .read()
            .unwrap_or_else(PoisonError::into_inner);
        let to_label_set = |key: &Key| LabelSet::from_key_and_global(key, &self.global_labels);
        // Sanitize each metric name once for the wire and reuse across HELP/TYPE/sample lines.
        let write_header = |name: &str, sanitized: &str, kind: &str, output: &mut String| {
            if let Some(desc) = descriptions.get(name) {
                write_help_line(output, sanitized, None, None, desc);
            }
            write_type_line(output, sanitized, None, None, kind);
        };

        macro_rules! write_scalar {
            ($map:expr, $kind:literal, $value_ty:ty) => {
                for (name, by_keys) in $map {
                    let sanitized = sanitize_metric_name(&name);
                    write_header(&name, &sanitized, $kind, &mut output);
                    for (key, value) in by_keys {
                        write_metric_line::<&str, $value_ty>(
                            &mut output,
                            &sanitized,
                            None,
                            &to_label_set(&key),
                            None,
                            value,
                            None,
                        );
                    }
                    output.push('\n');
                }
            };
        }
        write_scalar!(counters, "counter", u64);
        write_scalar!(gauges, "gauge", f64);

        for (name, by_keys) in distributions.iter() {
            let sanitized = sanitize_metric_name(name);
            let distribution_type = self.distribution_builder.get_distribution_type(name);
            write_header(name, &sanitized, distribution_type, &mut output);
            for (key, distribution) in by_keys {
                let labels = to_label_set(key);
                let (sum, count) = match distribution {
                    Distribution::NativeHistogram(_) => continue,
                    Distribution::Summary(summary, quantiles, sum) => {
                        let snapshot = summary.snapshot(Instant::now());
                        for quantile in quantiles.iter() {
                            let value = snapshot.quantile(quantile.value()).unwrap_or(0.0);
                            write_metric_line(
                                &mut output,
                                &sanitized,
                                None,
                                &labels,
                                Some(("quantile", quantile.value())),
                                value,
                                None,
                            );
                        }
                        (*sum, summary.count() as u64)
                    }
                    Distribution::Histogram(histogram) => {
                        for (le, count) in histogram.buckets() {
                            write_metric_line(
                                &mut output,
                                &sanitized,
                                Some("bucket"),
                                &labels,
                                Some(("le", le)),
                                count,
                                None,
                            );
                        }
                        write_metric_line(
                            &mut output,
                            &sanitized,
                            Some("bucket"),
                            &labels,
                            Some(("le", "+Inf")),
                            histogram.count(),
                            None,
                        );
                        (histogram.sum(), histogram.count())
                    }
                };

                write_metric_line::<&str, f64>(
                    &mut output,
                    &sanitized,
                    Some("sum"),
                    &labels,
                    None,
                    sum,
                    None,
                );
                write_metric_line::<&str, u64>(
                    &mut output,
                    &sanitized,
                    Some("count"),
                    &labels,
                    None,
                    count,
                    None,
                );
            }
            output.push('\n');
        }

        output
    }

    fn run_upkeep(&self) {
        self.refresh_distributions();
    }
}

/// A Prometheus recorder.
pub struct PrometheusRecorder {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for PrometheusRecorder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrometheusRecorder")
            .field("global_labels", &self.inner.global_labels)
            .finish_non_exhaustive()
    }
}

impl PrometheusRecorder {
    /// Gets a [`PrometheusHandle`] to this recorder.
    pub fn handle(&self) -> PrometheusHandle {
        PrometheusHandle {
            inner: self.inner.clone(),
        }
    }

    fn add_description_if_missing(&self, key_name: &KeyName, description: SharedString) {
        self.inner
            .descriptions
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(key_name.as_str().to_owned())
            .or_insert(description);
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

/// Counter samples for one metric grouped by a single label value.
#[derive(Default)]
pub struct CountersSnapshot {
    samples: HashMap<String, u64>,
}

impl CountersSnapshot {
    pub fn iter(&self) -> impl Iterator<Item = (&str, u64)> {
        self.samples.iter().map(|(k, v)| (k.as_str(), *v))
    }

    /// Sum for `label_value`'s bucket, or 0 if nothing landed there.
    pub fn get(&self, label_value: &str) -> u64 {
        self.samples.get(label_value).copied().unwrap_or(0)
    }
}

/// Aggregated counter total for one metric.
pub struct CounterSnapshot {
    value: u64,
}

impl CounterSnapshot {
    pub fn get(&self) -> u64 {
        self.value
    }
}

/// Gauge samples for one metric grouped by a single label value.
#[derive(Default)]
pub struct GaugesSnapshot {
    samples: HashMap<String, f64>,
}

impl GaugesSnapshot {
    pub fn iter(&self) -> impl Iterator<Item = (&str, f64)> {
        self.samples.iter().map(|(k, v)| (k.as_str(), *v))
    }

    /// Sum for `label_value`'s bucket, or 0.0 if nothing landed there.
    pub fn get(&self, label_value: &str) -> f64 {
        self.samples.get(label_value).copied().unwrap_or(0.0)
    }
}

/// Aggregated gauge total for one metric.
pub struct GaugeSnapshot {
    value: f64,
}

impl GaugeSnapshot {
    pub fn get(&self) -> f64 {
        self.value
    }
}

/// Distribution samples for one metric grouped by a single label value.
pub struct DistributionsSnapshot {
    samples: HashMap<String, Distribution>,
}

impl DistributionsSnapshot {
    pub fn iter(&self) -> impl Iterator<Item = (&str, &Distribution)> {
        self.samples.iter().map(|(k, v)| (k.as_str(), v))
    }

    /// Distribution for `label_value`'s bucket, or an empty Summary if nothing landed there.
    pub fn get(&self, label_value: &str) -> &Distribution {
        self.samples.get(label_value).unwrap_or(&EMPTY_SUMMARY)
    }
}

/// A single distribution sample for one metric.
pub struct DistributionSnapshot {
    value: Option<Distribution>,
}

impl DistributionSnapshot {
    /// First matching sample, or an empty Summary if nothing matched.
    pub fn get(&self) -> &Distribution {
        self.value.as_ref().unwrap_or(&EMPTY_SUMMARY)
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

impl std::fmt::Debug for PrometheusHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrometheusHandle")
            .field("global_labels", &self.inner.global_labels)
            .finish_non_exhaustive()
    }
}

impl PrometheusHandle {
    /// Takes a snapshot of the metrics held by the recorder and generates a payload conforming to
    /// the Prometheus exposition format.
    pub fn render(&self) -> String {
        self.inner.render()
    }

    pub(crate) fn run_upkeep(&self) {
        self.inner.run_upkeep();
    }

    /// Walk the counter registry once and partition the matching samples into one slot per
    /// requested metric name.  Slots for absent metrics come back empty.
    fn read_counter_batch<const M: usize>(&self, metrics: [&str; M]) -> [HashMap<Key, u64>; M] {
        let mut by_name = self.inner.counters(Some(&metrics));
        std::array::from_fn(|i| by_name.remove(metrics[i]).unwrap_or_default())
    }

    /// Sums of each metric's counter samples matching every `(name, value)` in `filters`.
    /// One registry walk for the whole batch.
    pub fn counters<const M: usize, const N: usize>(
        &self,
        metrics: [&str; M],
        filters: [(&str, &str); N],
    ) -> [CounterSnapshot; M] {
        self.read_counter_batch(metrics)
            .map(|samples| CounterSnapshot {
                value: total_sum(samples, filters),
            })
    }

    /// Counter samples for each metric, grouped by `group_by`'s label value and restricted to
    /// those matching every `(name, value)` in `filters`.  Samples without the `group_by` label
    /// are dropped.  One registry walk for the whole batch.
    pub fn counters_by_label<const M: usize, const N: usize>(
        &self,
        metrics: [&str; M],
        group_by: &str,
        filters: [(&str, &str); N],
    ) -> [CountersSnapshot; M] {
        self.read_counter_batch(metrics)
            .map(|samples| CountersSnapshot {
                samples: bucket_sum(samples, group_by, filters),
            })
    }

    /// Walk the gauge registry once; see [`Self::read_counter_batch`].
    fn read_gauge_batch<const M: usize>(&self, metrics: [&str; M]) -> [HashMap<Key, f64>; M] {
        let mut by_name = self.inner.gauges(Some(&metrics));
        std::array::from_fn(|i| by_name.remove(metrics[i]).unwrap_or_default())
    }

    /// Sums of each metric's gauge samples matching every `(name, value)` in `filters`.
    /// One registry walk for the whole batch.
    pub fn gauges<const M: usize, const N: usize>(
        &self,
        metrics: [&str; M],
        filters: [(&str, &str); N],
    ) -> [GaugeSnapshot; M] {
        self.read_gauge_batch(metrics).map(|samples| GaugeSnapshot {
            value: total_sum(samples, filters),
        })
    }

    /// Gauge samples for each metric, grouped by `group_by`'s label value and restricted to those
    /// matching every `(name, value)` in `filters`.  Samples without the `group_by` label are
    /// dropped.  One registry walk for the whole batch.
    pub fn gauges_by_label<const M: usize, const N: usize>(
        &self,
        metrics: [&str; M],
        group_by: &str,
        filters: [(&str, &str); N],
    ) -> [GaugesSnapshot; M] {
        self.read_gauge_batch(metrics)
            .map(|samples| GaugesSnapshot {
                samples: bucket_sum(samples, group_by, filters),
            })
    }

    /// First distribution sample for each metric matching `filters`.  Logs a warning if more
    /// than one sample matches.  One registry walk for the whole batch.
    pub fn distributions<const M: usize, const N: usize>(
        &self,
        metrics: [&str; M],
        filters: [(&str, &str); N],
    ) -> [DistributionSnapshot; M] {
        self.inner.for_distribution_batch(metrics, |i, by_keys| {
            let value = by_keys.and_then(|m| first_distribution(m.iter(), metrics[i], filters));
            DistributionSnapshot { value }
        })
    }

    /// Distribution samples for each metric, grouped by `group_by`'s label value.  Samples
    /// without the `group_by` label are dropped.  When more than one sample lands in a bucket
    /// the first is kept and a warning is logged once per bucket.  One registry walk for the
    /// whole batch.
    pub fn distributions_by_label<const M: usize, const N: usize>(
        &self,
        metrics: [&str; M],
        group_by: &str,
        filters: [(&str, &str); N],
    ) -> [DistributionsSnapshot; M] {
        self.inner.for_distribution_batch(metrics, |i, by_keys| {
            let samples = by_keys
                .map(|m| bucket_first(m.iter(), metrics[i], group_by, filters))
                .unwrap_or_default();
            DistributionsSnapshot { samples }
        })
    }
}

fn label_value<'a>(key: &'a Key, name: &str) -> Option<&'a str> {
    key.labels().find(|l| l.key() == name).map(|l| l.value())
}

fn matches<const N: usize>(key: &Key, filters: [(&str, &str); N]) -> bool {
    filters.iter().all(|(n, v)| label_value(key, n) == Some(v))
}

fn bucket_first<'a, I, const N: usize>(
    samples: I,
    metric: &str,
    group_by: &str,
    filters: [(&str, &str); N],
) -> HashMap<String, Distribution>
where
    I: IntoIterator<Item = (&'a Key, &'a Distribution)>,
{
    let mut out: HashMap<String, Distribution> = HashMap::new();
    let mut warned: HashSet<String> = HashSet::new();
    for (key, dist) in samples {
        if !matches(key, filters) {
            continue;
        }
        let Some(bucket) = label_value(key, group_by) else {
            continue;
        };
        if out.contains_key(bucket) {
            if !warned.contains(bucket) {
                warned.insert(bucket.to_string());
                warn!(
                    metric,
                    %group_by,
                    ?filters,
                    label_value = bucket,
                    "distribution bucket has multiple matching samples; using first",
                );
            }
        } else {
            out.insert(bucket.to_string(), dist.clone());
        }
    }
    out
}

fn bucket_sum<I, V, const N: usize>(
    samples: I,
    group_by: &str,
    filters: [(&str, &str); N],
) -> HashMap<String, V>
where
    I: IntoIterator<Item = (Key, V)>,
    V: Default + AddAssign,
{
    let mut out: HashMap<String, V> = HashMap::new();
    for (key, value) in samples {
        if !matches(&key, filters) {
            continue;
        }
        let Some(bucket) = label_value(&key, group_by) else {
            continue;
        };
        if let Some(slot) = out.get_mut(bucket) {
            *slot += value;
        } else {
            out.insert(bucket.to_string(), value);
        }
    }
    out
}

fn total_sum<I, V, const N: usize>(samples: I, filters: [(&str, &str); N]) -> V
where
    I: IntoIterator<Item = (Key, V)>,
    V: Default + AddAssign,
{
    let mut total = V::default();
    for (key, value) in samples {
        if !matches(&key, filters) {
            continue;
        }
        total += value;
    }
    total
}

fn first_distribution<'a, I, const N: usize>(
    samples: I,
    metric: &str,
    filters: [(&str, &str); N],
) -> Option<Distribution>
where
    I: IntoIterator<Item = (&'a Key, &'a Distribution)>,
{
    let mut result: Option<Distribution> = None;
    let mut warned = false;
    for (key, dist) in samples {
        if !matches(key, filters) {
            continue;
        }
        if result.is_none() {
            result = Some(dist.clone());
        } else if !warned {
            warn!(
                metric,
                ?filters,
                "distribution has multiple matching samples; using first - did you mean to group \
                 with distributions_by_label?"
            );
            warned = true;
        }
    }
    result
}

#[cfg(feature = "push-gateway")]
#[derive(Clone, Debug)]
struct PushGateway {
    endpoint: String,
    interval: Duration,
    username: Option<String>,
    password: Option<String>,
}

#[cfg(feature = "push-gateway")]
async fn exporter(
    handle: PrometheusHandle,
    username: Option<String>,
    password: Option<String>,
    interval: Duration,
    endpoint: String,
) -> Result<(), Box<dyn std::error::Error + Send>> {
    let client = reqwest::Client::new();

    loop {
        tokio::time::sleep(interval).await;

        let output = handle.render();
        let mut req = client.put(&endpoint).body(output);
        if let Some(name) = &username {
            req = req.basic_auth(name, password.as_ref());
        }

        match req.send().await {
            Ok(response) => {
                if !response.status().is_success() {
                    let status = response.status();
                    let status = status.canonical_reason().unwrap_or_else(|| status.as_str());
                    let body = response
                        .text()
                        .await
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
}

#[derive(Debug, Default)]
pub struct PrometheusBuilder {
    global_labels: IndexMap<String, String>,
    #[cfg(feature = "push-gateway")]
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
        self.global_labels.insert(key.into(), value.into());
        self
    }

    #[cfg(feature = "push-gateway")]
    pub fn with_push_gateway<T: AsRef<str>>(
        mut self,
        endpoint: T,
        interval: Duration,
        username: Option<String>,
        password: Option<String>,
    ) -> Result<Self, BuildError> {
        // Validate the URL up front so misconfiguration is detected at startup
        reqwest::Url::parse(endpoint.as_ref())
            .map_err(|e| BuildError::InvalidPushGatewayEndpoint(e.to_string()))?;
        self.push_gateway = Some(PushGateway {
            endpoint: endpoint.as_ref().to_owned(),
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

    /// # Panics
    ///
    /// Panics if called outside a tokio runtime: spawns a background upkeep task that drains
    /// histograms into their `Distribution` storage every 5s.
    pub fn build_recorder(self) -> PrometheusRecorder {
        let quantiles = metrics_util::parse_quantiles(SUMMARY_QUANTILES);
        let rec = PrometheusRecorder::from(Inner {
            registry: Registry::new(GenerationalStorage::new(AtomicStorage)),
            recency: Recency::new(quanta::Clock::new(), MetricKindMask::NONE, None),
            distributions: RwLock::new(HashMap::new()),
            distribution_builder: DistributionBuilder::new(quantiles, None, None, None, None, None),
            descriptions: RwLock::new(HashMap::new()),
            global_labels: self.global_labels,
        });
        tokio::spawn(Self::run_upkeep(rec.handle()));
        rec
    }

    #[cfg(feature = "push-gateway")]
    #[allow(clippy::type_complexity)]
    pub fn build(
        self,
    ) -> Result<
        (
            PrometheusRecorder,
            impl std::future::Future<Output = Result<(), Box<dyn std::error::Error + Send>>>,
        ),
        BuildError,
    > {
        let gw = self
            .push_gateway
            .clone()
            .ok_or(BuildError::MissingExporterConfiguration)?;
        let recorder = self.build_recorder();
        let handle = recorder.handle();
        let exporter = exporter(handle, gw.username, gw.password, gw.interval, gw.endpoint);
        Ok((recorder, exporter))
    }
}
