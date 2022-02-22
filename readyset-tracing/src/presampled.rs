use std::future::Future;
use std::sync::atomic::{AtomicU32, Ordering};

use once_cell::sync::Lazy;
use parking_lot::Mutex;
use tracing::{Instrument, Span};

use crate::Percent;

/// Provides "random" sampling
pub(crate) struct Sampler {
    requests_per_sample: u32,
    counter: AtomicU32,
}

impl Sampler {
    /// Creates a new sampler with the given number of requests per sample; the counter is started
    /// from a random value so that, given a set of thread-local Samplers, they don't all send a
    /// sample at roughly the same time.
    pub(crate) fn new(requests_per_sample: u32) -> Self {
        let counter = rand::random::<u32>() % requests_per_sample;
        Self {
            requests_per_sample,
            counter: counter.into(),
        }
    }

    fn sample(&self) -> bool {
        let i = self.counter.fetch_add(1, Ordering::Relaxed);
        if std::intrinsics::unlikely(i >= self.requests_per_sample) {
            self.counter.store(0, Ordering::SeqCst);
            true
        } else {
            false
        }
    }
}

impl From<&Percent> for Sampler {
    fn from(percent: &Percent) -> Self {
        Self::new(percent.integral_divisor())
    }
}

#[doc(hidden)]
#[inline]
pub fn sample() -> bool {
    SAMPLER.with(|s| s.sample())
}

// Thread-local storage is used instead of task-local storage because we want the counter value to
// persist across requests (e.g. queries) while not paying a performance penalty to share a counter
// amongst threads.
thread_local! {
    static SAMPLER: Sampler = Sampler::from(SAMPLE_GENERATOR.lock().as_ref().unwrap());
}

static SAMPLE_GENERATOR: Lazy<Mutex<Option<Percent>>> = Lazy::new(|| Mutex::new(None));

pub(crate) fn install_sample_generator(percent: Percent) {
    *SAMPLE_GENERATOR.lock() = Some(percent);
}

/// Selectively instruments a `Future` depending on whether or not the `Span` is enabled.  Not
/// instrumenting when the `Span` is disabled saves a bit of time every time the future is polled.
#[inline]
pub async fn instrument_if_enabled<T>(future: impl Future<Output = T>, span: Span) -> T {
    if span.is_disabled() {
        future.await
    } else {
        future.instrument(span).await
    }
}

/// Provides all the overloads from [tracing::span!] that make sense given that root spans cannot
/// have a parent.
#[macro_export]
macro_rules! root_span {
    (target: $target:expr, $lvl:ident, $name:expr, $($fields:tt)*) => {
        if $crate::presampled::sample() {
            ::tracing::span!(target: $target, parent: None, ::tracing::Level::$lvl, $name, $($fields)*)
        } else {
            ::tracing::Span::none()
        }
    };
    (target: $target:expr, $lvl:ident, $name:expr) => {
        $crate::root_span!(target: $target, $lvl, $name,)
    };
    ($lvl:ident, $name:expr, $($fields:tt)*) => {
        $crate::root_span!(target: module_path!(), $lvl, $name, $($fields)*)
    };
    ($lvl:ident, $name:expr) => {
        $crate::root_span!(target: module_path!(), $lvl, $name,)
    };
}

/// Provides all the overloads from [tracing::span!] that make sense given that explicit parents
/// will not work; we need to detect the existence of a parent
#[macro_export]
macro_rules! child_span {
    (target: $target:expr, $lvl:ident, $name:expr, $($fields:tt)*) => {{
        let span = ::tracing::Span::current();
        if span.is_disabled() {
            span
        } else {
            ::tracing::span!(target: $target, parent: span, ::tracing::Level::$lvl, $name, $($fields)*)
        }
    }};
    (target: $target:expr, $lvl:ident, $name:expr) => {
        $crate::child_span!(target: $target, $lvl, $name,)
    };
    ($lvl:ident, $name:expr, $($fields:tt)*) => {
        $crate::child_span!(target: module_path!(), $lvl, $name, $($fields)*)
    };
    ($lvl:ident, $name:expr) => {
        $crate::child_span!(target: module_path!(), $lvl, $name,)
    };
}

/// Unpacks an [Instrumented](crate::propagation::Instrumented) message.  If it has an active span,
/// the span and unpacked message are returned; if not, an empty span and the unpacked message are
/// returned.
#[macro_export]
macro_rules! remote_span {
    ($instrumented:ident, target: $target:expr, $lvl:ident, $name:expr, $($fields:tt)*) => {
        if $instrumented.is_enabled() {
            let span = ::tracing::span!(target: $target, parent: None, ::tracing::Level::$lvl, $name, $($fields)*);
            let inner = span.in_scope(|| $instrumented.unpack());
            (span, inner)
        } else {
            (::tracing::Span::none(), $instrumented.unpack())
        }
    };
    ($instrumented:ident, target: $target:expr, $lvl:ident, $name:expr) => {
        $crate::remote_span!($instrumented, target: $target, $lvl, $name,)
    };
    ($instrumented:ident, $lvl:ident, $name:expr, $($fields:tt)*) => {
        $crate::remote_span!($instrumented, target: module_path!(), $lvl, $name, $($fields)*)
    };
    ($instrumented:ident, $lvl:ident, $name:expr) => {
        $crate::remote_span!($instrumented, target: module_path!(), $lvl, $name,)
    };
}
