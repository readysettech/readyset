use std::future::Future;

use tracing::{Instrument, Span};

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
