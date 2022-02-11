//! String wrapper type which hides the contents when debug printed, ideal for
//! password-containing strings

use std::str::FromStr;

/// Wraps a given string, replacing its contents with "<anonymized>" when debug
/// printed
#[derive(Clone)]
pub struct RedactedString(pub String);

impl std::fmt::Debug for RedactedString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("<anonymized>")
    }
}

impl FromStr for RedactedString {
    type Err = anyhow::Error;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        Ok(RedactedString(input.to_string()))
    }
}
