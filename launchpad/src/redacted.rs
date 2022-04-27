//! Wrapper types which hide the contents of the wrapped types when printed with Debug and/or
//! Display. These wrappers are intended to be used to hide user PII in logs or errors.

use std::convert::Infallible;
use std::fmt::{Debug, Display};
use std::str::FromStr;

/// Wraps a type that implements Display and Debug, overriding both implementations unless the
/// display_literals feature is enabled
pub struct Sensitive<'a, T>(pub &'a T);

impl<'a, T> Display for Sensitive<'a, T>
where
    T: Display,
{
    #[cfg(feature = "display_literals")]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
    #[cfg(not(feature = "display_literals"))]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<redacted>")
    }
}

impl<'a, T> Debug for Sensitive<'a, T>
where
    T: Debug,
{
    #[cfg(feature = "display_literals")]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
    #[cfg(not(feature = "display_literals"))]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<redacted>")
    }
}

/// Wraps a given string, replacing its contents with "<anonymized>" when debug
/// printed
#[derive(Clone)]
pub struct RedactedString(pub String);

impl std::fmt::Debug for RedactedString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("<redacted>")
    }
}

impl FromStr for RedactedString {
    type Err = Infallible;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        Ok(RedactedString(input.to_string()))
    }
}
