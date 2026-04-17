//! Support for recording and exporting in-memory metrics using the [`metrics`] crate

use std::sync::OnceLock;

pub use crate::metrics::recorders::{PrometheusBuilder, PrometheusHandle, PrometheusRecorder};

mod prometheus_recorder;
mod recorders;

/// The type of the static, globally accessible metrics recorder.
type GlobalRecorder = PrometheusRecorder;
static METRICS_RECORDER: OnceLock<&'static GlobalRecorder> = OnceLock::new();

/// Returns the global recorder, initializing it on first call.
/// `global_labels` are added to every metric; pass `&[]` if none are needed.
pub fn get_or_init_global_recorder(global_labels: &[(&str, &str)]) -> &'static GlobalRecorder {
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
pub fn get_global_recorder() -> Option<&'static GlobalRecorder> {
    METRICS_RECORDER.get().cloned()
}

/// A metrics recorder that can be rendered.
/// We do not use [`std::fmt::Display`] since we might be extending
/// [`Recorder`]s from other libraries as well.
pub trait Render {
    /// Renders the metrics stored in the recorder.
    fn render(&self) -> String;
}
