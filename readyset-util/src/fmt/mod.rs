//! Formatting utilities.

mod datetime;
mod num;

use std::fmt::*;

use bytes::BytesMut;
pub use num::write_padded_u32;

/// Like [`std::format_args!`] but with ownership of arguments.
#[macro_export]
macro_rules! fmt_args {
    ($($tt:tt)+) => {
        $crate::fmt::fmt_with(move |__format_args_formatter__| {
            ::std::write!(__format_args_formatter__, $($tt)+)
        })
    };
}

/// See [`fmt_with()`].
#[derive(Clone, Copy)]
pub struct FmtWith<F = fn(&mut Formatter) -> Result> {
    fmt: F,
}

/// Formats via a closure.
pub fn fmt_with<F: Fn(&mut Formatter) -> Result>(fmt: F) -> FmtWith<F> {
    fmt.into()
}

impl<F: Fn(&mut Formatter) -> Result> From<F> for FmtWith<F> {
    fn from(fmt: F) -> Self {
        Self { fmt }
    }
}

impl<F: Fn(&mut Formatter) -> Result> Debug for FmtWith<F> {
    fn fmt(&self, f: &mut Formatter) -> Result {
        (self.fmt)(f)
    }
}

impl<F: Fn(&mut Formatter) -> Result> Display for FmtWith<F> {
    fn fmt(&self, f: &mut Formatter) -> Result {
        (self.fmt)(f)
    }
}

/// This trait should be implemented to provide types with more optimized algorithms to encode
/// their data as formatted strings into a [`BytesMut`].
pub trait FastEncode {
    /// Encodes self into the given [`BytesMut`] as a formatted string.
    fn put(self, buf: &mut BytesMut);
}
