use metrics_exporter_prometheus::PrometheusRecorder;
use tracing::warn;

use crate::metrics::{Clear, Render};

impl Render for PrometheusRecorder {
    fn render(&self) -> String {
        self.handle().render()
    }
}

impl Clear for PrometheusRecorder {
    fn clear(&self) -> bool {
        warn!("Attempted to clear PrometheusRecorder, which cannot be cleared. Ignoring...");
        false
    }
}
