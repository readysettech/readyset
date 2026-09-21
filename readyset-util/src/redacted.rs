//! Wrapper types which hide the contents of the wrapped types when printed with Debug and/or
//! Display. These wrappers are intended to be used to hide user PII and credentials in logs or
//! errors.

use std::convert::Infallible;
use std::fmt::{self, Debug, Display, Formatter};
use std::ops::Deref;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// Wraps a type that implements Display and Debug, overriding both implementations if the
/// `redact_sensitive` feature is enabled
pub struct Sensitive<'a, T: ?Sized>(pub &'a T);

impl<T> Display for Sensitive<'_, T>
where
    T: ?Sized + Display,
{
    #[cfg(not(feature = "redact_sensitive"))]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }

    #[cfg(feature = "redact_sensitive")]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "<redacted>")
    }
}

impl<T> Debug for Sensitive<'_, T>
where
    T: ?Sized + Debug,
{
    #[cfg(not(feature = "redact_sensitive"))]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.0)
    }

    #[cfg(feature = "redact_sensitive")]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "<redacted>")
    }
}

/// A string that always prints as "<redacted>" with Display and Debug. The wrapped value is
/// reachable through Deref or the public field.
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RedactedString(pub String);

impl proptest::arbitrary::Arbitrary for RedactedString {
    type Parameters = ();
    type Strategy = proptest::strategy::BoxedStrategy<RedactedString>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        use proptest::prelude::*;
        any::<String>().prop_map(RedactedString).boxed()
    }
}

impl Deref for RedactedString {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Display for RedactedString {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "<redacted>")
    }
}

impl Debug for RedactedString {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "<redacted>")
    }
}

impl FromStr for RedactedString {
    type Err = Infallible;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        Ok(RedactedString(input.to_string()))
    }
}

impl From<String> for RedactedString {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<RedactedString> for String {
    fn from(s: RedactedString) -> Self {
        s.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacted_string_hides_value_when_formatted() {
        let secret = RedactedString("hunter2".to_string());
        assert_eq!(format!("{secret}"), "<redacted>");
        assert_eq!(format!("{secret:?}"), "<redacted>");
        assert_eq!(*secret, "hunter2");
        assert_eq!(String::from(secret), "hunter2");
    }
}
