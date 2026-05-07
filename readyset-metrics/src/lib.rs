//! In-memory metrics recording and Prometheus exposition for Readyset.

use std::sync::OnceLock;

pub use crate::recorders::{
    CounterSnapshot, CountersSnapshot, DistributionSnapshot, DistributionsSnapshot, GaugeSnapshot,
    GaugesSnapshot, PrometheusBuilder, PrometheusHandle, PrometheusRecorder,
};

mod recorders;

static METRICS_RECORDER: OnceLock<&'static PrometheusRecorder> = OnceLock::new();

/// Body for the standard `/metrics` HTTP 404 when no recorder is installed.
pub const METRICS_DISABLED_MESSAGE: &str =
    "Prometheus metrics are not enabled.  Enable with --prometheus-metrics.";

/// Initializes the global recorder on first call; subsequent calls are no-ops.  `global_labels`
/// are added to every metric; pass `&[]` if none are needed.
///
/// # Panics
///
/// Must be called from within a tokio runtime: the recorder spawns a background upkeep task on
/// first init via [`PrometheusBuilder::build_recorder`].
pub fn init_global_recorder(global_labels: &[(&str, &str)]) {
    METRICS_RECORDER.get_or_init(|| {
        let mut builder = PrometheusBuilder::new();
        for &(k, v) in global_labels {
            builder = builder.add_global_label(k, v);
        }
        let rec = Box::leak(Box::new(builder.build_recorder()));
        metrics::set_global_recorder(&*rec).ok();
        rec
    });
}

/// Gets the global metrics handle, if one has been initialized.
pub fn metrics_handle() -> Option<PrometheusHandle> {
    METRICS_RECORDER.get().map(|r| r.handle())
}

/// Body for an HTTP `/metrics` response: rendered exposition if a recorder is installed,
/// otherwise [`METRICS_DISABLED_MESSAGE`] in the `Err` arm so the caller can map it to 404.
pub fn metrics_body() -> Result<String, &'static str> {
    match metrics_handle() {
        Some(h) => Ok(h.render()),
        None => Err(METRICS_DISABLED_MESSAGE),
    }
}
