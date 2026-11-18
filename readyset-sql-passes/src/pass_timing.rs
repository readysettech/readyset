//! The time each rewrite pass takes, recorded only with the `bench` feature.
//!
//! `trace_pass!` marks the end of each pass it runs.  Between `start` and `drain` on a thread, each mark
//! records the time since the mark before it, or since `start`, as the time its pass took.

#[cfg(feature = "bench")]
mod imp {
    use std::cell::RefCell;
    use std::mem;
    use std::time::{Duration, Instant};

    struct Recording {
        /// When the last pass ended; set only while the thread records.
        last: Option<Instant>,
        passes: Vec<(&'static str, Duration)>,
    }

    thread_local! {
        static RECORDING: RefCell<Recording> =
            const { RefCell::new(Recording { last: None, passes: Vec::new() }) };
    }

    /// Starts recording the passes that run on this thread, discarding any earlier recording.
    pub fn start() {
        RECORDING.with_borrow_mut(|recording| {
            recording.passes.clear();
            recording.last = Some(Instant::now());
        })
    }

    /// Stops recording, and returns each pass that ran since [`start`] with the time it took.
    pub fn drain() -> Vec<(&'static str, Duration)> {
        RECORDING.with_borrow_mut(|recording| {
            recording.last = None;
            // Replace the buffer with one of the same capacity, so marks in later recordings push
            // into preallocated space.
            let capacity = recording.passes.capacity();
            mem::replace(&mut recording.passes, Vec::with_capacity(capacity))
        })
    }

    /// Records the time since the last mark, or since [`start`], as the time `pass` took.
    pub(crate) fn mark(pass: &'static str) {
        RECORDING.with_borrow_mut(|recording| {
            if let Some(last) = recording.last {
                let now = Instant::now();
                recording.passes.push((pass, now - last));
                recording.last = Some(now);
            }
        })
    }
}

#[cfg(feature = "bench")]
pub(crate) use imp::mark;
#[cfg(feature = "bench")]
pub use imp::{drain, start};

#[cfg(not(feature = "bench"))]
pub(crate) fn mark(_pass: &'static str) {}
