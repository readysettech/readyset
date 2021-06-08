use metrics::{GaugeValue, Recorder, Unit};

use crate::metrics::noria_recorder::NoriaMetricsRecorder;
use crate::metrics::{Clear, Key, Render};

/// The name for the Recorder as stored in CompositeMetricsRecorder.
pub enum MetricsRecorder {
    /// A recorder for Noria-style metrics.
    Noria(NoriaMetricsRecorder),
}

impl Render for MetricsRecorder {
    fn render(&self) -> String {
        match self {
            MetricsRecorder::Noria(nmr) => nmr.render(),
        }
    }
}

impl Clear for MetricsRecorder {
    fn clear(&self) -> bool {
        match self {
            MetricsRecorder::Noria(nmr) => nmr.clear(),
        }
    }
}

macro_rules! impl_method {
    ($self:ident, $method:ident,$($arg:tt)*) => {
        match $self {
            MetricsRecorder::Noria(nmr) => nmr.$method($($arg)*),
        }
    }
}

impl Recorder for MetricsRecorder {
    fn register_counter(&self, key: Key, unit: Option<Unit>, description: Option<&'static str>) {
        impl_method!(self, register_counter, key, unit, description);
    }

    fn register_gauge(&self, key: Key, unit: Option<Unit>, description: Option<&'static str>) {
        impl_method!(self, register_gauge, key, unit, description);
    }

    fn register_histogram(&self, key: Key, unit: Option<Unit>, description: Option<&'static str>) {
        impl_method!(self, register_histogram, key, unit, description);
    }

    fn increment_counter(&self, key: Key, value: u64) {
        impl_method!(self, increment_counter, key, value);
    }

    fn update_gauge(&self, key: Key, value: GaugeValue) {
        impl_method!(self, update_gauge, key, value);
    }

    fn record_histogram(&self, key: Key, value: f64) {
        impl_method!(self, record_histogram, key, value);
    }
}
