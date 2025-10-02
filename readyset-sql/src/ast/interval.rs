use crate::AstConversionError;
use crate::ast::Expr;
use crate::ast::Literal;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::num::NonZeroUsize;
use test_strategy::Arbitrary;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Ord, Hash, Serialize, Deserialize, Arbitrary)]
pub enum TimeInterval {
    Year(NonZeroUsize),
    Month(NonZeroUsize),
    Day(NonZeroUsize),
    Hour(NonZeroUsize),
    Minute(NonZeroUsize),
    Second(NonZeroUsize),
}

impl Display for TimeInterval {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TimeInterval::Year(n) if n.get() == 1 => write!(f, "{} year", n),
            TimeInterval::Month(n) if n.get() == 1 => write!(f, "{} month", n),
            TimeInterval::Day(n) if n.get() == 1 => write!(f, "{} day", n),
            TimeInterval::Hour(n) if n.get() == 1 => write!(f, "{} hour", n),
            TimeInterval::Minute(n) if n.get() == 1 => write!(f, "{} minute", n),
            TimeInterval::Second(n) if n.get() == 1 => write!(f, "{} second", n),
            TimeInterval::Year(n) => write!(f, "{} years", n),
            TimeInterval::Month(n) => write!(f, "{} months", n),
            TimeInterval::Day(n) => write!(f, "{} days", n),
            TimeInterval::Hour(n) => write!(f, "{} hours", n),
            TimeInterval::Minute(n) => write!(f, "{} minutes", n),
            TimeInterval::Second(n) => write!(f, "{} seconds", n),
        }
    }
}

impl TryFrom<String> for TimeInterval {
    type Error = AstConversionError;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        let err = || {
            unsupported_err!(
                "Can not parse {} as interval. Expected the format '<positive integer> <unit>' \
                where unit is one of year[s], month[s], day[s], hour[s], minute[s], or second[s]",
                s
            )
        };

        let (n, unit) = s.split_once(' ').ok_or_else(err)?;

        let n: NonZeroUsize = n.parse().map_err(|_| err())?;

        match unit.to_ascii_lowercase().as_str() {
            "year" | "years" => Ok(TimeInterval::Year(n)),
            "month" | "months" => Ok(TimeInterval::Month(n)),
            "day" | "days" => Ok(TimeInterval::Day(n)),
            "hour" | "hours" => Ok(TimeInterval::Hour(n)),
            "minute" | "minutes" => Ok(TimeInterval::Minute(n)),
            "second" | "seconds" => Ok(TimeInterval::Second(n)),
            _ => Err(err()),
        }
    }
}

impl TryFrom<Expr> for TimeInterval {
    type Error = AstConversionError;

    fn try_from(value: Expr) -> Result<Self, Self::Error> {
        match value {
            Expr::Literal(Literal::String(s)) => s.try_into(),
            v => unsupported!(
                "Expected a string literal for Interval argument, but got {:?}",
                v
            ),
        }
    }
}

impl TryFrom<Box<Expr>> for TimeInterval {
    type Error = AstConversionError;

    fn try_from(value: Box<Expr>) -> Result<Self, Self::Error> {
        (*value).try_into()
    }
}
