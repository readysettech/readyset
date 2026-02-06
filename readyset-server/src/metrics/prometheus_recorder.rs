use metrics_exporter_prometheus::PrometheusRecorder;

use crate::metrics::Render;

impl Render for PrometheusRecorder {
    fn render(&self) -> String {
        self.handle().render()
    }
}
