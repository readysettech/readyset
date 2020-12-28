//! Annoying hacks to work around Rust's dodgy error handling ecosystem.
//!
//! Mostly cribbed from https://docs.rs/crate/failure/0.1.8/source/src/box_std.rs.

/// Wraps a boxed `std::error::Error` to make it implement, um, `std::error::Error`.
/// Yes, I'm as disappointed as you are.
#[repr(transparent)]
pub struct BoxedErrorWrapper(pub Box<dyn std::error::Error + Send + Sync + 'static>);

impl std::fmt::Display for BoxedErrorWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl std::fmt::Debug for BoxedErrorWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.0, f)
    }
}

impl std::error::Error for BoxedErrorWrapper {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.source()
    }
}

/// Wrap a boxed `std::error::Error` in a nice warm `anyhow::Error` blanket.
pub fn wrap_boxed_error(
    boxed: Box<dyn std::error::Error + Send + Sync + 'static>,
) -> anyhow::Error {
    anyhow::Error::new(BoxedErrorWrapper(boxed))
}
