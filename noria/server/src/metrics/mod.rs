//! Support for recording and exporting in-memory metrics using the [`metrics`] crate

use crossbeam::queue::ArrayQueue;
use metrics::{GaugeValue, SetRecorderError, Unit};

use noria::metrics::Key;

pub use crate::metrics::buffered_recorder::BufferedRecorder;
pub use crate::metrics::composite_recorder::CompositeMetricsRecorder;
pub use crate::metrics::composite_recorder::RecorderType;
pub use crate::metrics::noria_recorder::NoriaMetricsRecorder;
pub use crate::metrics::recorders::MetricsRecorder;

mod buffered_recorder;
mod composite_recorder;
mod noria_recorder;
mod prometheus_recorder;
mod recorders;

/// The type of the static, globally accessible metrics recorder.
type GlobalRecorder = BufferedRecorder<CompositeMetricsRecorder>;
static mut METRICS_RECORDER: Option<GlobalRecorder> = None;

enum MetricsOp {
    RegisterCounter(Key, Option<Unit>, Option<&'static str>),
    RegisterGauge(Key, Option<Unit>, Option<&'static str>),
    RegisterHistogram(Key, Option<Unit>, Option<&'static str>),
    IncrementCounter(Key, u64),
    UpdateGauge(Key, GaugeValue),
    RecordHistogram(Key, f64),
}

type OpQueue = ArrayQueue<MetricsOp>;

/// Installs a new global recorder
///
/// # Safety
///
/// This function is unsafe to call when there are multiple threads; it MUST be called before
/// other threads are created.
pub unsafe fn install_global_recorder(rec: GlobalRecorder) -> Result<(), SetRecorderError> {
    if std::mem::replace(&mut METRICS_RECORDER, Some(rec)).is_some() {
        panic!("metrics recorder installed twice!")
    }
    metrics::set_recorder_racy(METRICS_RECORDER.as_ref().unwrap())
}

/// Gets a static reference to the installed metrics recorder.
///
/// # Panics
///
/// This method panics if `install()` has not been called yet.
pub fn get_global_recorder() -> &'static GlobalRecorder {
    // SAFETY: no data races possible, since METRICS_RECORDER is only mutated once (and the
    // `install()` function is marked `unsafe`).
    get_global_recorder_opt().expect("metrics recorder not installed yet")
}

/// Gets an [`Option`] with the static reference to the installed metrics recorder.
/// This method returns [`None`] if `install()` has not been called yet.
pub fn get_global_recorder_opt() -> Option<&'static GlobalRecorder> {
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
