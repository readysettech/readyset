use metrics_exporter_prometheus::PrometheusRecorder;

use crate::metrics::{Clear, Render};

impl Render for PrometheusRecorder {
    fn render(&self) -> String {
        self.handle().render()
    }
}

impl Clear for PrometheusRecorder {
    fn clear(&self) -> bool {
        // TODO(fran): Use a real log here.
        eprintln!("Attempted to clear PrometheusRecorder, which cannot be cleared. Ignoring...");
        false
    }
}
