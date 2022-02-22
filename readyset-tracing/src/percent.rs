use std::fmt;
use std::str::FromStr;

#[derive(Debug, Clone)]
pub(crate) struct Percent(pub f64);

impl Percent {
    pub(crate) fn integral_divisor(&self) -> u32 {
        (1.0 / (self.0 * 0.01)) as u32
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_integral_divisor() {
        assert_eq!(Percent(100.0).integral_divisor(), 1);
        assert_eq!(Percent(1.0).integral_divisor(), 100);
        assert_eq!(Percent(0.1).integral_divisor(), 1000);
        assert_eq!(Percent(0.01).integral_divisor(), 10000);

        // Small precision loss here but it's close enough for sampling and still validates our
        // conversion logic
        assert_eq!(Percent(0.0001).integral_divisor(), 999999);

        assert_eq!(Percent(0.0).integral_divisor(), u32::MAX);
    }
}
