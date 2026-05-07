//! In-memory metrics recording and Prometheus exposition for Readyset.

use std::sync::OnceLock;

pub use crate::recorders::{PrometheusBuilder, PrometheusHandle, PrometheusRecorder};

mod recorders;

static METRICS_RECORDER: OnceLock<&'static PrometheusRecorder> = OnceLock::new();

/// Body for the standard `/metrics` HTTP 404 when no recorder is installed.
pub const METRICS_DISABLED_MESSAGE: &str =
    "Prometheus metrics are not enabled.  Enable with --prometheus-metrics.";

/// Returns the global recorder, initializing it on first call.  `global_labels` are added to every
/// metric; pass `&[]` if none are needed.
///
/// # Panics
///
/// Must be called from within a tokio runtime: the recorder spawns a background upkeep task on
/// first init via [`PrometheusBuilder::build_recorder`].
pub fn get_or_init_global_recorder(global_labels: &[(&str, &str)]) -> &'static PrometheusRecorder {
    METRICS_RECORDER.get_or_init(|| {
        let mut builder = PrometheusBuilder::new();
        for &(k, v) in global_labels {
            builder = builder.add_global_label(k, v);
        }
        let rec = Box::leak(Box::new(builder.build_recorder()));
        metrics::set_global_recorder(&*rec).ok();
        rec
    })
}

/// Gets the global recorder, if one has been initialized.
pub fn get_global_recorder() -> Option<&'static PrometheusRecorder> {
    METRICS_RECORDER.get().copied()
}

/// Body for an HTTP `/metrics` response: rendered exposition if a recorder is installed,
/// otherwise [`METRICS_DISABLED_MESSAGE`] in the `Err` arm so the caller can map it to 404.
pub fn metrics_body() -> Result<String, &'static str> {
    match get_global_recorder() {
        Some(r) => Ok(r.handle().render()),
        None => Err(METRICS_DISABLED_MESSAGE),
    }
}
