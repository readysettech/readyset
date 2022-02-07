use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;

use metrics::{Counter, Gauge, Histogram, KeyName, Recorder, Unit};
use parking_lot::Mutex;

use noria::metrics::{Key, MetricsDump};

use crate::metrics::{Clear, Render};

/// A simplistic metrics recorder for Noria, which just stores the different metrics.
#[derive(Default)]
pub struct NoriaMetricsRecorder {
    counters: Mutex<HashMap<Key, Arc<AtomicU64>>>,
    gauges: Mutex<HashMap<Key, Arc<AtomicU64>>>,
    histograms: Mutex<HashMap<Key, Arc<NoriaHistogram>>>,
}

impl NoriaMetricsRecorder {
    /// Makes a new `NoriaMetricsRecorder`
    pub fn new() -> Self {
        Default::default()
    }
}

struct NoriaHistogram(Mutex<metrics_util::Histogram>);

impl NoriaHistogram {
    fn new() -> Self {
        // Define the buckets for the histogram to be power of 2s up to
        // 2^20.
        let bounds: Vec<f64> = (1..20).map(|v| 2u32.pow(v) as f64).collect();
        NoriaHistogram(Mutex::new(metrics_util::Histogram::new(&bounds).unwrap()))
    }

    fn clear(&self) {
        let bounds: Vec<f64> = (1..20).map(|v| 2u32.pow(v) as f64).collect();
        *self.0.lock() = metrics_util::Histogram::new(&bounds).unwrap();
    }
}

impl metrics::HistogramFn for NoriaHistogram {
    fn record(&self, value: f64) {
        self.0.lock().record(value)
    }
}

impl Recorder for NoriaMetricsRecorder {
    fn register_counter(&self, key: &Key) -> Counter {
        let mut counters = self.counters.lock();
        counters
            .raw_entry_mut()
            .from_key(key)
            .or_insert_with(|| (key.clone(), Default::default()))
            .1
            .clone()
            .into()
    }

    fn register_gauge(&self, key: &Key) -> Gauge {
        let mut gauges = self.gauges.lock();
        gauges
            .raw_entry_mut()
            .from_key(key)
            .or_insert_with(|| (key.clone(), Default::default()))
            .1
            .clone()
            .into()
    }

    fn register_histogram(&self, key: &Key) -> Histogram {
        let mut histograms = self.histograms.lock();
        histograms
            .raw_entry_mut()
            .from_key(key)
            .or_insert_with(|| (key.clone(), Arc::new(NoriaHistogram::new())))
            .1
            .clone()
            .into()
    }

    fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: &'static str) {
        // no-op
    }

    fn describe_gauge(&self, _: KeyName, _: Option<metrics::Unit>, _: &'static str) {
        // no-op
    }

    fn describe_histogram(&self, _: KeyName, _: Option<metrics::Unit>, _: &'static str) {
        // no-op
    }
}

impl Render for NoriaMetricsRecorder {
    fn render(&self) -> String {
        let md = MetricsDump::from_metrics(
            self.counters
                .lock()
                .iter()
                .map(|(k, v)| (k.clone(), v.load(Relaxed)))
                .collect(),
            self.gauges
                .lock()
                .iter()
                .map(|(k, v)| (k.clone(), f64::from_bits(v.load(Relaxed))))
                .collect(),
            self.histograms
                .lock()
                .iter()
                .map(|(k, v)| (k.clone(), v.0.lock().clone()))
                .collect(),
        );
        serde_json::to_string(&md).unwrap()
    }
}

impl Clear for NoriaMetricsRecorder {
    fn clear(&self) -> bool {
        self.counters
            .lock()
            .iter()
            .for_each(|(_, v)| v.store(0, Relaxed));

        self.gauges
            .lock()
            .iter()
            .for_each(|(_, v)| v.store(0, Relaxed));

        self.histograms.lock().iter().for_each(|(_, v)| v.clear());
        true
    }
}
