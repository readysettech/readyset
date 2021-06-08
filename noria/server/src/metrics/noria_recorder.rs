use std::cell::RefCell;
use std::collections::HashMap;

use metrics::{GaugeValue, Recorder, Unit};
use metrics_util::Histogram;

use noria::metrics::{Key, MetricsDump};

use crate::metrics::{Clear, Render};

/// A simplistic metrics recorder for Noria, which just stores the different metrics.
#[derive(Default)]
pub struct NoriaMetricsRecorder {
    counters: RefCell<HashMap<Key, u64>>,
    gauges: RefCell<HashMap<Key, f64>>,
    histograms: RefCell<HashMap<Key, Histogram>>,
}

impl NoriaMetricsRecorder {
    /// Makes a new `NoriaMetricsRecorder`
    pub fn new() -> Self {
        Default::default()
    }
}

impl Recorder for NoriaMetricsRecorder {
    fn register_counter(&self, _key: Key, _unit: Option<Unit>, _description: Option<&'static str>) {
        // no-op
    }

    fn register_gauge(&self, _key: Key, _unit: Option<Unit>, _description: Option<&'static str>) {
        // no-op
    }

    fn register_histogram(
        &self,
        _key: Key,
        _unit: Option<Unit>,
        _description: Option<&'static str>,
    ) {
        // no-op
    }

    fn increment_counter(&self, key: Key, value: u64) {
        let mut counters = self.counters.borrow_mut();
        let ent = counters.entry(key).or_default();
        *ent += value;
    }

    fn update_gauge(&self, key: Key, value: GaugeValue) {
        let mut gauges = self.gauges.borrow_mut();
        let ent = gauges.entry(key).or_default();
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

    fn record_histogram(&self, key: Key, value: f64) {
        // TODO(justin): Support different bounds in histogram metrics.
        let mut histograms = self.histograms.borrow_mut();
        let ent = histograms.entry(key).or_insert_with(|| {
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
            self.counters.borrow().clone(),
            self.gauges.borrow().clone(),
            self.histograms.borrow().clone(),
        );
        serde_json::to_string(&md).unwrap()
    }
}

impl Clear for NoriaMetricsRecorder {
    fn clear(&self) -> bool {
        self.counters.borrow_mut().clear();
        self.gauges.borrow_mut().clear();
        self.histograms.borrow_mut().clear();
        true
    }
}
