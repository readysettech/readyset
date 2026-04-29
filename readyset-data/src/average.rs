//! Shared AVG display-scale policy for the dataflow grouped/window
//! operators and the adapter's post-lookup aggregation path.

use readyset_errors::{internal, ReadySetResult};
use serde::{Deserialize, Serialize};

use crate::{DfType, Dialect, SqlEngine};

/// Controls how AVG display scale is computed after division.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AvgScaleMode {
    /// MySQL: round to a fixed number of decimal places (determined at plan time).
    Fixed(i64),
    /// PostgreSQL: dynamically compute display scale targeting ~16 significant digits,
    /// dependent on the magnitude of sum and count.
    PostgresComputed,
}

impl AvgScaleMode {
    /// Choose the scale-selection policy for AVG given the dialect and
    /// AVG output type.
    pub fn for_avg(dialect: Dialect, out_ty: &DfType) -> ReadySetResult<Self> {
        match (dialect.engine(), out_ty) {
            (SqlEngine::MySQL, DfType::Numeric { scale, .. }) => Ok(Self::Fixed(i64::from(*scale))),
            (SqlEngine::PostgreSQL, DfType::Numeric { .. }) => Ok(Self::PostgresComputed),
            // Sentinel: Double short-circuits before rounding, but a
            // serialisable mode must still be stored.
            (_, DfType::Double) => Ok(Self::Fixed(0)),
            (engine, ty) => {
                internal!("for_avg: unexpected (dialect, out_ty) = ({engine:?}, {ty:?})")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn for_avg_dispatch() {
        let mysql = Dialect::DEFAULT_MYSQL;
        let pg = Dialect::DEFAULT_POSTGRESQL;
        let cases: &[(Dialect, DfType, Option<AvgScaleMode>)] = &[
            (
                mysql,
                DfType::Numeric { prec: 23, scale: 4 },
                Some(AvgScaleMode::Fixed(4)),
            ),
            (
                pg,
                DfType::DEFAULT_NUMERIC,
                Some(AvgScaleMode::PostgresComputed),
            ),
            (mysql, DfType::Double, Some(AvgScaleMode::Fixed(0))),
            (pg, DfType::Double, Some(AvgScaleMode::Fixed(0))),
            (mysql, DfType::BigInt, None),
            (mysql, DfType::Unknown, None),
            (mysql, DfType::Float, None),
            (pg, DfType::BigInt, None),
            (pg, DfType::Unknown, None),
            (pg, DfType::Float, None),
        ];
        for (dialect, ty, expected) in cases {
            let actual = AvgScaleMode::for_avg(*dialect, ty);
            match expected {
                Some(e) => assert_eq!(&actual.unwrap(), e, "({dialect:?}, {ty:?})"),
                None => assert!(actual.is_err(), "({dialect:?}, {ty:?}) should error"),
            }
        }
    }
}
