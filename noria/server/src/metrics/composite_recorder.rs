#![allow(clippy::option_map_unit_fn)]
use metrics::{Counter, Gauge, Histogram, KeyName, Recorder, Unit};
use metrics_exporter_prometheus::PrometheusRecorder;
use noria::metrics::Key;
use std::sync::Arc;

use crate::metrics::recorders::MetricsRecorder;
use crate::metrics::{Clear, Render};
use crate::NoriaMetricsRecorder;

/// A recorder that maintains a set of recorders and notifies all of them of all updates.
#[derive(Default)]
pub struct CompositeMetricsRecorder {
    noria_recorder: Option<NoriaMetricsRecorder>,
    prom_recorder: Option<PrometheusRecorder>,
}

/// The name for the Recorder as stored in CompositeMetricsRecorder.
#[derive(Eq, PartialEq, Hash, Clone, Copy)]
pub enum RecorderType {
    /// A Noria recorder.
    Noria = 0,
    /// A Prometheus recorder.
    Prometheus = 1,
}

pub struct CompositeCounter {
    noria: Counter,
    prom: Counter,
}

pub struct CompositeGauge {
    noria: Gauge,
    prom: Gauge,
}

pub struct CompositeHistogram {
    noria: Histogram,
    prom: Histogram,
}

impl metrics::CounterFn for CompositeCounter {
    fn increment(&self, value: u64) {
        self.prom.increment(value);
        self.noria.increment(value);
    }

    fn absolute(&self, value: u64) {
        self.prom.absolute(value);
        self.noria.absolute(value);
    }
}

impl metrics::GaugeFn for CompositeGauge {
    fn increment(&self, value: f64) {
        self.prom.increment(value);
        self.noria.increment(value);
    }

    fn decrement(&self, value: f64) {
        self.prom.decrement(value);
        self.noria.decrement(value);
    }

    fn set(&self, value: f64) {
        self.prom.set(value);
        self.noria.set(value);
    }
}

impl metrics::HistogramFn for CompositeHistogram {
    fn record(&self, value: f64) {
        self.prom.record(value);
        self.noria.record(value);
    }
}

impl CompositeMetricsRecorder {
    /// Makes a new `CompositeMetricsRecorder` from a vector of recorders
    pub fn with_recorders(recorders: Vec<MetricsRecorder>) -> Self {
        let mut rec: CompositeMetricsRecorder = Default::default();

        for recorder in recorders {
            match recorder {
                MetricsRecorder::Noria(noria) => rec.noria_recorder = Some(noria),
                MetricsRecorder::Prometheus(prom) => rec.prom_recorder = Some(prom),
            }
        }

        rec
    }

    /// Render the named sub-recorder of this CompositeMetricsRecorder, if it exists
    pub fn render(&self, recorder_type: RecorderType) -> Option<String> {
        match recorder_type {
            RecorderType::Noria => self.noria_recorder.as_ref().map(|x| x.render()),
            RecorderType::Prometheus => self.prom_recorder.as_ref().map(|x| x.render()),
        }
    }
}

impl Clear for CompositeMetricsRecorder {
    fn clear(&self) -> bool {
        let mut clr = true;
        self.noria_recorder.as_ref().map(|x| clr = clr && x.clear());
        self.prom_recorder.as_ref().map(|x| clr = clr && x.clear());
        clr
    }
}

impl Recorder for CompositeMetricsRecorder {
    fn register_counter(&self, key: &Key) -> Counter {
        match (&self.prom_recorder, &self.noria_recorder) {
            (Some(p), None) => p.register_counter(key),
            (None, Some(n)) => n.register_counter(key),
            (None, None) => Counter::noop(),
            (Some(p), Some(n)) => Arc::new(CompositeCounter {
                noria: n.register_counter(key),
                prom: p.register_counter(key),
            })
            .into(),
        }
    }

    fn register_gauge(&self, key: &Key) -> Gauge {
        match (&self.prom_recorder, &self.noria_recorder) {
            (Some(p), None) => p.register_gauge(key),
            (None, Some(n)) => n.register_gauge(key),
            (None, None) => Gauge::noop(),
            (Some(p), Some(n)) => Arc::new(CompositeGauge {
                noria: n.register_gauge(key),
                prom: p.register_gauge(key),
            })
            .into(),
        }
    }

    fn register_histogram(&self, key: &Key) -> Histogram {
        match (&self.prom_recorder, &self.noria_recorder) {
            (Some(p), None) => p.register_histogram(key),
            (None, Some(n)) => n.register_histogram(key),
            (None, None) => Histogram::noop(),
            (Some(p), Some(n)) => Arc::new(CompositeHistogram {
                noria: n.register_histogram(key),
                prom: p.register_histogram(key),
            })
            .into(),
        }
    }

    fn describe_counter(&self, key: KeyName, unit: Option<Unit>, desc: &'static str) {
        self.prom_recorder
            .as_ref()
            .map(|x| x.describe_counter(key, unit, desc));
    }

    fn describe_gauge(&self, key: KeyName, unit: Option<metrics::Unit>, desc: &'static str) {
        self.prom_recorder
            .as_ref()
            .map(|x| x.describe_gauge(key, unit, desc));
    }

    fn describe_histogram(&self, key: KeyName, unit: Option<metrics::Unit>, desc: &'static str) {
        self.prom_recorder
            .as_ref()
            .map(|x| x.describe_histogram(key, unit, desc));
    }
}
