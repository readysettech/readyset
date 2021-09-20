use std::sync::{Mutex, MutexGuard};

use crossbeam::queue::ArrayQueue;
use metrics::{GaugeValue, Recorder, Unit};

use noria::metrics::Key;

use crate::metrics::{Clear, MetricsOp, OpQueue, Render, METRICS_RECORDER};

/// A recorder that buffers updates to underlying data structures for efficiency.
pub struct BufferedRecorder<T> {
    inner: Mutex<T>,
    queue: OpQueue,
}

impl<T: Recorder> BufferedRecorder<T> {
    /// Create a new buffered recorder with interior `inner` and buffer capacity `cap`.
    pub fn new(inner: T, cap: usize) -> Self {
        BufferedRecorder {
            inner: Mutex::new(inner),
            queue: ArrayQueue::new(cap),
        }
    }

    /// Adds a [`MetricsOp`] to the operation queue, blocking it while doing so.
    /// If the queue is full, it will process one of the [`MetricsOp`] to free one slot.
    ///
    /// This design should mean that metrics are pretty lightweight on average, with the occasional
    /// spike.
    fn push_op(&self, op: MetricsOp) {
        let guard = self.inner.lock().unwrap();
        if let Err(op) = self.queue.push(op) {
            // If the queue is full, just process one item and try again
            self.process_next(&guard);
            if self.queue.push(op).is_err() {
                eprintln!("WARNING: failed to push metrics op after freeing one slot!");
            }
        }
    }

    /// Processes the next item in the queue. Returns [`true`] if there are potentially more
    /// items in the queue; or [`false`] if the queue is empty.
    ///
    /// This private method is to be called in a blocking context (just to ensure no other thread
    /// is peaking at the queue), thus it takes a [`MutexGuard`] reference (instead of just trying to lock the
    /// queue by itself).
    /// This allows us to decide if we want to process just one, or if we want to process all of them
    /// in the context of a single transaction.
    fn process_next(&self, guard: &MutexGuard<'_, T>) -> bool {
        if let Some(op) = self.queue.pop() {
            match op {
                MetricsOp::RegisterCounter(k, u, o) => guard.register_counter(&k, u, o),
                MetricsOp::RegisterGauge(k, u, o) => guard.register_gauge(&k, u, o),
                MetricsOp::RegisterHistogram(k, u, o) => guard.register_histogram(&k, u, o),
                MetricsOp::IncrementCounter(k, v) => guard.increment_counter(&k, v),
                MetricsOp::UpdateGauge(k, v) => guard.update_gauge(&k, v),
                MetricsOp::RecordHistogram(k, v) => guard.record_histogram(&k, v),
            }
            true
        } else {
            false
        }
    }

    /// Processes all the items in the queue.
    /// See [`process_next`] for more information as to why this method takes
    /// a [`MutexGuard`] reference as an argument.
    fn collapse(&self, guard: &MutexGuard<'_, T>) {
        while self.process_next(guard) {}
    }

    /// Returns [`true`] if the metrics recorder has been installed.
    ///
    /// This function should only be used in tests that may be run out of
    /// order to verify that a metrics recorder already exists.
    pub fn installed() -> bool {
        unsafe { METRICS_RECORDER.is_some() }
    }

    /// Execute function `F` on the fully-flushed interior recorder
    pub fn flush_and_then<F, R>(&self, func: F) -> R
    where
        F: FnOnce(&T) -> R,
    {
        let guard = self.inner.lock().unwrap();
        self.collapse(&guard);
        func(&*guard)
    }
}

impl<T: Recorder> Recorder for BufferedRecorder<T> {
    fn register_counter(&self, key: &Key, unit: Option<Unit>, description: Option<&'static str>) {
        self.push_op(MetricsOp::RegisterCounter(key.clone(), unit, description))
    }

    fn register_gauge(&self, key: &Key, unit: Option<Unit>, description: Option<&'static str>) {
        self.push_op(MetricsOp::RegisterGauge(key.clone(), unit, description))
    }

    fn register_histogram(&self, key: &Key, unit: Option<Unit>, description: Option<&'static str>) {
        self.push_op(MetricsOp::RegisterHistogram(key.clone(), unit, description))
    }

    fn increment_counter(&self, key: &Key, value: u64) {
        self.push_op(MetricsOp::IncrementCounter(key.clone(), value))
    }

    fn update_gauge(&self, key: &Key, value: GaugeValue) {
        self.push_op(MetricsOp::UpdateGauge(key.clone(), value))
    }

    fn record_histogram(&self, key: &Key, value: f64) {
        self.push_op(MetricsOp::RecordHistogram(key.clone(), value))
    }
}

impl<T: Recorder + Render> Render for BufferedRecorder<T> {
    fn render(&self) -> String {
        self.flush_and_then(|recorder| recorder.render())
    }
}

impl<T: Recorder + Clear> Clear for BufferedRecorder<T> {
    fn clear(&self) -> bool {
        self.flush_and_then(|x| x.clear())
    }
}
