use metrics_exporter_prometheus::PrometheusRecorder;

use crate::metrics::noria_recorder::NoriaMetricsRecorder;
use crate::metrics::{Clear, Render};

/// The name for the Recorder as stored in CompositeMetricsRecorder.
pub enum MetricsRecorder {
    /// A recorder for Noria-style metrics.
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
