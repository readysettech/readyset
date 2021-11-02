use std::collections::HashMap;
use std::sync::Arc;

use metrics::{GaugeValue, Recorder, Unit};
use metrics_util::Histogram;
use parking_lot::lock_api::Mutex;
use parking_lot::RawMutex;

use noria::metrics::{Key, MetricsDump};

use crate::metrics::{Clear, Render};

/// A simplistic metrics recorder for Noria, which just stores the different metrics.
#[derive(Default)]
pub struct NoriaMetricsRecorder {
    counters: Arc<Mutex<RawMutex, HashMap<Key, u64>>>,
    gauges: Arc<Mutex<RawMutex, HashMap<Key, f64>>>,
    histograms: Arc<Mutex<RawMutex, HashMap<Key, Histogram>>>,
}

impl NoriaMetricsRecorder {
    /// Makes a new `NoriaMetricsRecorder`
    pub fn new() -> Self {
        Default::default()
    }
}

impl Recorder for NoriaMetricsRecorder {
    fn register_counter(
        &self,
        _key: &Key,
        _unit: Option<Unit>,
        _description: Option<&'static str>,
    ) {
        // no-op
    }

    fn register_gauge(&self, _key: &Key, _unit: Option<Unit>, _description: Option<&'static str>) {
        // no-op
    }

    fn register_histogram(
        &self,
        _key: &Key,
        _unit: Option<Unit>,
        _description: Option<&'static str>,
    ) {
        // no-op
    }

    fn increment_counter(&self, key: &Key, value: u64) {
        let mut counters = self.counters.lock();
        let ent = counters.entry(key.clone()).or_default();
        *ent += value;
    }

    fn update_gauge(&self, key: &Key, value: GaugeValue) {
        let mut gauges = self.gauges.lock();
        let ent = gauges.entry(key.clone()).or_default();
        match value {
            GaugeValue::Increment(v) => {
                *ent += v;
            }
            GaugeValue::Absolute(v) => {
                *ent = v;
            }
            GaugeValue::Decrement(v) => {
                *ent -= v;
            }
        }
    }

    fn record_histogram(&self, key: &Key, value: f64) {
        // TODO(justin): Support different bounds in histogram metrics.
        let mut histograms = self.histograms.lock();
        let ent = histograms.entry(key.clone()).or_insert_with(|| {
            // Define the buckets for the histogram to be power of 2s up to
            // 2^20.
            let bounds: Vec<f64> = (1..20).map(|v| 2u32.pow(v) as f64).collect();
            Histogram::new(&bounds).unwrap()
        });
        ent.record(value);
    }
}

impl Render for NoriaMetricsRecorder {
    fn render(&self) -> String {
        let md = MetricsDump::from_metrics(
            self.counters.lock().clone(),
            self.gauges.lock().clone(),
            self.histograms.lock().clone(),
        );
        serde_json::to_string(&md).unwrap()
    }
}

impl Clear for NoriaMetricsRecorder {
    fn clear(&self) -> bool {
        self.counters.lock().clear();
        self.gauges.lock().clear();
        self.histograms.lock().clear();
        true
    }
}
