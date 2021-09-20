use std::collections::HashMap;
use std::sync::RwLock;

use metrics::{GaugeValue, Recorder, Unit};

use noria::metrics::Key;

use crate::metrics::recorders::MetricsRecorder;
use crate::metrics::{Clear, Render};

/// A recorder that maintains a set of recorders and notifies all of them of all updates.
#[derive(Default)]
pub struct CompositeMetricsRecorder {
    elements: RwLock<HashMap<RecorderType, MetricsRecorder>>,
}

/// The name for the Recorder as stored in CompositeMetricsRecorder.
#[derive(Eq, PartialEq, Hash, Clone, Copy)]
pub enum RecorderType {
    /// A Noria recorder.
    Noria,
    /// A Prometheus recorder.
    Prometheus,
}

macro_rules! try_poisoned {
    ($expr:expr) => {
        $expr.expect("Metrics mutex poisoned.")
    };
}

impl CompositeMetricsRecorder {
    /// Makes a new `CompositeMetricsRecorder`
    pub fn new() -> Self {
        Default::default()
    }

    /// Add a new sub-recorder to this CompositeMetricsRecorder
    pub fn add(&self, recorder: MetricsRecorder) {
        let mut elements = try_poisoned!(self.elements.write());
        match recorder {
            MetricsRecorder::Noria(_) => (*elements).insert(RecorderType::Noria, recorder),
            MetricsRecorder::Prometheus(_) => {
                (*elements).insert(RecorderType::Prometheus, recorder)
            }
        };
    }

    /// Render the named sub-recorder of this CompositeMetricsRecorder, if it exists
    pub fn render(&self, recorder_type: RecorderType) -> Option<String> {
        let elements = try_poisoned!(self.elements.read());
        (*elements).get(&recorder_type).map(|x| x.render())
    }
}

impl Clear for CompositeMetricsRecorder {
    fn clear(&self) -> bool {
        let elements = try_poisoned!(self.elements.read());
        let mut cleared = true;
        for recorder in elements.values() {
            cleared = cleared && recorder.clear();
        }
        cleared
    }
}

impl Recorder for CompositeMetricsRecorder {
    fn register_counter(&self, key: &Key, unit: Option<Unit>, description: Option<&'static str>) {
        let elements = try_poisoned!(self.elements.read());
        for recorder in elements.values() {
            recorder.register_counter(key, unit.clone(), description);
        }
    }

    fn register_gauge(&self, key: &Key, unit: Option<Unit>, description: Option<&'static str>) {
        let elements = try_poisoned!(self.elements.read());
        for recorder in elements.values() {
            recorder.register_gauge(key, unit.clone(), description);
        }
    }

    fn register_histogram(&self, key: &Key, unit: Option<Unit>, description: Option<&'static str>) {
        let elements = try_poisoned!(self.elements.read());
        for recorder in elements.values() {
            recorder.register_histogram(key, unit.clone(), description);
        }
    }

    fn increment_counter(&self, key: &Key, value: u64) {
        let elements = try_poisoned!(self.elements.read());
        for recorder in elements.values() {
            recorder.increment_counter(key, value);
        }
    }

    fn update_gauge(&self, key: &Key, value: GaugeValue) {
        let elements = try_poisoned!(self.elements.read());
        for recorder in elements.values() {
            recorder.update_gauge(key, value.clone());
        }
    }

    fn record_histogram(&self, key: &Key, value: f64) {
        let elements = try_poisoned!(self.elements.read());
        for recorder in elements.values() {
            recorder.record_histogram(key, value);
        }
    }
}
