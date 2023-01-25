use std::fmt;
use std::str::FromStr;

#[derive(Debug, Clone)]
pub(crate) struct Percent(pub f64);

impl fmt::Display for Percent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum InvalidPercent {
    #[error("{0}")]
    Parse(#[from] std::num::ParseFloatError),
    #[error("Invalid percent {0}; must be between 0.0 and 100.0")]
    OutOfRange(f64),
}

impl FromStr for Percent {
    type Err = InvalidPercent;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let value = s.parse()?;
        if (0.0..=100.0).contains(&value) {
            Ok(Self(value))
        } else {
            Err(InvalidPercent::OutOfRange(value))
        }
    }
}
