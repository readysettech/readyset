use metrics::{GaugeValue, Recorder, Unit};
use noria::metrics::Key;

use crate::metrics::recorders::MetricsRecorder;
use crate::metrics::{Clear, Render};

/// A recorder that maintains a set of recorders and notifies all of them of all updates.
#[derive(Default)]
pub struct CompositeMetricsRecorder {
    recorders: [Option<MetricsRecorder>; 2],
}

/// The name for the Recorder as stored in CompositeMetricsRecorder.
#[derive(Eq, PartialEq, Hash, Clone, Copy)]
pub enum RecorderType {
    /// A Noria recorder.
    Noria = 0,
    /// A Prometheus recorder.
    Prometheus = 1,
}

impl CompositeMetricsRecorder {
    /// Makes a new `CompositeMetricsRecorder` from a vector of recorders
    pub fn with_recorders(recorders: Vec<MetricsRecorder>) -> Self {
        let mut rec = CompositeMetricsRecorder {
            recorders: Default::default(),
        };

        for recorder in recorders {
            match recorder {
                MetricsRecorder::Noria(_) => {
                    rec.recorders[RecorderType::Noria as usize] = Some(recorder)
                }
                MetricsRecorder::Prometheus(_) => {
                    rec.recorders[RecorderType::Prometheus as usize] = Some(recorder)
                }
            }
        }

        rec
    }

    /// Render the named sub-recorder of this CompositeMetricsRecorder, if it exists
    pub fn render(&self, recorder_type: RecorderType) -> Option<String> {
        self.recorders[recorder_type as usize]
            .as_ref()
            .map(|x| x.render())
    }
}

impl Clear for CompositeMetricsRecorder {
    fn clear(&self) -> bool {
        let mut cleared = true;
        for recorder in self.recorders.iter().filter_map(Option::as_ref) {
            cleared = cleared && recorder.clear()
        }
        cleared
    }
}

impl Recorder for CompositeMetricsRecorder {
    fn register_counter(&self, key: &Key, unit: Option<Unit>, description: Option<&'static str>) {
        for recorder in self.recorders.iter().filter_map(Option::as_ref) {
            recorder.register_counter(key, unit.clone(), description);
        }
    }

    fn register_gauge(&self, key: &Key, unit: Option<Unit>, description: Option<&'static str>) {
        for recorder in self.recorders.iter().filter_map(Option::as_ref) {
            recorder.register_gauge(key, unit.clone(), description);
        }
    }

    fn register_histogram(&self, key: &Key, unit: Option<Unit>, description: Option<&'static str>) {
        for recorder in self.recorders.iter().filter_map(Option::as_ref) {
            recorder.register_histogram(key, unit.clone(), description);
        }
    }

    fn increment_counter(&self, key: &Key, value: u64) {
        for recorder in self.recorders.iter().filter_map(Option::as_ref) {
            recorder.increment_counter(key, value);
        }
    }

    fn update_gauge(&self, key: &Key, value: GaugeValue) {
        for recorder in self.recorders.iter().filter_map(Option::as_ref) {
            recorder.update_gauge(key, value.clone());
        }
    }

    fn record_histogram(&self, key: &Key, value: f64) {
        for recorder in self.recorders.iter().filter_map(Option::as_ref) {
            recorder.record_histogram(key, value);
        }
    }
}
