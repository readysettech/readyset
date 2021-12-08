//! Support for recording and exporting in-memory metrics using the [`metrics`] crate

use noria::metrics::Key;
use thiserror::Error;

pub use crate::metrics::composite_recorder::CompositeMetricsRecorder;
pub use crate::metrics::composite_recorder::RecorderType;
pub use crate::metrics::noria_recorder::NoriaMetricsRecorder;
pub use crate::metrics::recorders::MetricsRecorder;

mod composite_recorder;
mod noria_recorder;
mod prometheus_recorder;
mod recorders;

/// The type of the static, globally accessible metrics recorder.
type GlobalRecorder = CompositeMetricsRecorder;
static mut METRICS_RECORDER: Option<GlobalRecorder> = None;

/// Error value returned from [`install_global_recorder`] if a metrics recorder is already set.
///
/// Essentially identical to [`metrics::SetRecorderError`], except that can't be constructed so we
/// define our own version here.
#[derive(Debug, Error)]
#[error("Metrics recorder installed twice!")]
pub struct RecorderInstalledTwice;

impl From<metrics::SetRecorderError> for RecorderInstalledTwice {
    fn from(_: metrics::SetRecorderError) -> Self {
        RecorderInstalledTwice
    }
}

/// Installs a new global recorder
///
/// # Safety
///
/// This function is unsafe to call when there are multiple threads; it MUST be called before
/// other threads are created.
pub unsafe fn install_global_recorder(rec: GlobalRecorder) -> Result<(), RecorderInstalledTwice> {
    #[allow(clippy::panic)] // documented panic
    if std::mem::replace(&mut METRICS_RECORDER, Some(rec)).is_some() {
        return Err(RecorderInstalledTwice);
    }
    metrics::set_recorder_racy(METRICS_RECORDER.as_ref().unwrap()).map_err(|e| e.into())
}

/// Gets an [`Option`] with the static reference to the installed metrics recorder.
/// This method returns [`None`] if `install()` has not been called yet.
pub fn get_global_recorder() -> Option<&'static GlobalRecorder> {
    // SAFETY: no data races possible, since METRICS_RECORDER is only mutated once (and the
    // `install()` function is marked `unsafe`).
    unsafe { METRICS_RECORDER.as_ref() }
}

/// A metrics recorder that can be cleared.
pub trait Clear {
    /// Clear the data saved in the recorder.
    /// Returns [`true`] if the recorder was successfully cleared; or [`false`] otherwise.
    fn clear(&self) -> bool;
}

/// A metrics recorder that can be rendered.
/// We do not use [`std::fmt::Display`] since we might be extending
/// [`Recorder`]s from other libraries as well.
pub trait Render {
    /// Renders the metrics stored in the recorder.
    fn render(&self) -> String;
}
