//! Shared AVG types — the display-scale policy and the typed running
//! state — used by the dataflow grouped/window operators and the
//! adapter's post-lookup aggregation path.

use readyset_decimal::Decimal;
use readyset_errors::{internal, internal_err, ReadySetResult};
use serde::{Deserialize, Serialize};

use crate::{DfType, DfValue, Dialect, SqlEngine};

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

    /// Compute `sum / count` rounded per this policy. Non-finite
    /// quotients (`NaN`, `Infinity`) pass through unchanged. Not for
    /// AVG-over-`Double`: the `Fixed(0)` sentinel from `for_avg` would
    /// round to integer scale.
    pub fn divide(&self, sum: &Decimal, count: i64) -> ReadySetResult<Decimal> {
        let count_dec = Decimal::from(count);
        let quotient = sum.checked_div(&count_dec).ok_or_else(|| {
            internal_err!("AvgScaleMode::divide: checked_div failed (sum={sum:?}, count={count})")
        })?;
        if !matches!(quotient, Decimal::Number(_)) {
            return Ok(quotient);
        }
        match self {
            Self::Fixed(scale) => Ok(quotient.round_dp(*scale)),
            Self::PostgresComputed => {
                let rscale = Decimal::select_div_scale(sum, &count_dec)?;
                Ok(quotient.round_dp(rscale))
            }
        }
    }
}

/// Per-group running state for AVG, typed by output so the hot path
/// avoids `DfValue` arithmetic.
#[derive(Debug, Clone)]
pub enum AverageAccumulator {
    Numeric {
        sum: Decimal,
        count: i64,
        scale_mode: AvgScaleMode,
    },
    Double {
        sum: f64,
        count: i64,
    },
}

impl AverageAccumulator {
    /// Empty accumulator for the given AVG output type.
    pub fn for_out_ty(out_ty: &DfType, scale_mode: AvgScaleMode) -> Self {
        if matches!(out_ty, DfType::Double) {
            Self::Double { sum: 0.0, count: 0 }
        } else {
            Self::Numeric {
                sum: Decimal::zero(),
                count: 0,
                scale_mode,
            }
        }
    }

    /// Seed an accumulator from already-aggregated totals (e.g. SUM /
    /// COUNT columns from upstream view rows in the post-lookup path).
    pub fn from_totals(
        out_ty: &DfType,
        scale_mode: AvgScaleMode,
        sum: &DfValue,
        count: i64,
    ) -> ReadySetResult<Self> {
        let mut acc = Self::for_out_ty(out_ty, scale_mode);
        match &mut acc {
            Self::Numeric {
                sum: s, count: c, ..
            } => {
                *s = Decimal::try_from(sum)?;
                *c = count;
            }
            Self::Double { sum: s, count: c } => {
                *s = f64::try_from(sum)?;
                *c = count;
            }
        }
        Ok(acc)
    }

    /// Apply a non-NULL value to the running totals: add when
    /// `positive`, subtract otherwise.
    pub fn delta(&mut self, value: &DfValue, positive: bool) -> ReadySetResult<()> {
        match self {
            Self::Numeric { sum, count, .. } => {
                let DfValue::Numeric(v) = value else {
                    internal!("Numeric AVG got non-Numeric value: {value:?}");
                };
                if positive {
                    *sum = &*sum + v.as_ref();
                    *count += 1;
                } else {
                    *sum = &*sum - v.as_ref();
                    *count -= 1;
                }
            }
            Self::Double { sum, count } => {
                let DfValue::Double(v) = value else {
                    internal!("Double AVG got non-Double value: {value:?}");
                };
                if positive {
                    *sum += v;
                    *count += 1;
                } else {
                    *sum -= v;
                    *count -= 1;
                }
            }
        }
        Ok(())
    }

    /// AVG result, or NULL when the running count is non-positive.
    pub fn result(&self) -> ReadySetResult<DfValue> {
        match self {
            Self::Numeric {
                sum,
                count,
                scale_mode,
            } => {
                if *count <= 0 {
                    return Ok(DfValue::None);
                }
                Ok(DfValue::from(scale_mode.divide(sum, *count)?))
            }
            Self::Double { sum, count } => {
                if *count <= 0 {
                    return Ok(DfValue::None);
                }
                Ok(DfValue::Double(*sum / *count as f64))
            }
        }
    }
}

impl readyset_util::SizeOf for AverageAccumulator {
    fn deep_size_of(&self) -> usize {
        // A Numeric sum's BigDecimal heap allocation is not counted, matching
        // `DfValue::deep_size_of`, which also treats `Numeric` as inline-only.
        size_of::<Self>()
    }

    fn size_is_empty(&self) -> bool {
        false
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

    #[test]
    fn divide_fixed_rounds_to_scale() {
        let rounded = AvgScaleMode::Fixed(4)
            .divide(&Decimal::from(100), 3)
            .unwrap();
        assert_eq!(rounded.to_string(), "33.3333");
    }

    #[test]
    fn divide_postgres_computed_uses_select_div_scale() {
        // 9007199254740993 / 2 → select_div_scale rscale=4 → ...0496.5000
        let sum = Decimal::from(9007199254740993i64);
        let rounded = AvgScaleMode::PostgresComputed.divide(&sum, 2).unwrap();
        assert_eq!(rounded.to_string(), "4503599627370496.5000");
        assert_eq!(rounded.scale(), Some(4));
    }

    #[test]
    fn divide_nan_sum_propagates() {
        // Postgres semantics: AVG(NaN) = NaN; rounding must not coerce.
        for mode in [AvgScaleMode::Fixed(4), AvgScaleMode::PostgresComputed] {
            let out = mode.divide(&Decimal::NaN, 3).unwrap();
            assert_eq!(out, Decimal::NaN, "{mode:?} should propagate NaN");
        }
    }

    #[test]
    fn numeric_rejects_non_numeric_value() {
        let mut acc =
            AverageAccumulator::for_out_ty(&DfType::DEFAULT_NUMERIC, AvgScaleMode::Fixed(2));
        assert!(acc.delta(&DfValue::Double(1.0), true).is_err());
    }

    #[test]
    fn numeric_rounds_per_scale_mode() {
        let mut acc =
            AverageAccumulator::for_out_ty(&DfType::DEFAULT_NUMERIC, AvgScaleMode::Fixed(4));
        for v in [1, 4] {
            acc.delta(&DfValue::from(Decimal::from(v)), true).unwrap();
        }
        let DfValue::Numeric(dec) = acc.result().unwrap() else {
            panic!("expected Numeric");
        };
        assert_eq!(dec.to_string(), "2.5000");
    }

    #[test]
    fn double_avg_skips_rounding() {
        let mut acc = AverageAccumulator::for_out_ty(&DfType::Double, AvgScaleMode::Fixed(0));
        acc.delta(&DfValue::Double(5.0), true).unwrap();
        assert_eq!(acc.result().unwrap(), DfValue::Double(5.0));
    }

    #[test]
    fn empty_accumulator_results_in_null() {
        let acc = AverageAccumulator::for_out_ty(&DfType::DEFAULT_NUMERIC, AvgScaleMode::Fixed(2));
        assert_eq!(acc.result().unwrap(), DfValue::None);
    }

    #[test]
    fn sub_to_zero_count_returns_null() {
        let mut acc =
            AverageAccumulator::for_out_ty(&DfType::DEFAULT_NUMERIC, AvgScaleMode::Fixed(2));
        acc.delta(&DfValue::from(Decimal::from(5)), true).unwrap();
        acc.delta(&DfValue::from(Decimal::from(5)), false).unwrap();
        assert_eq!(acc.result().unwrap(), DfValue::None);
    }

    #[test]
    fn from_totals_seeds_numeric() {
        let acc = AverageAccumulator::from_totals(
            &DfType::DEFAULT_NUMERIC,
            AvgScaleMode::PostgresComputed,
            &DfValue::from(Decimal::from(9007199254740993i64)),
            2,
        )
        .unwrap();
        let DfValue::Numeric(d) = acc.result().unwrap() else {
            panic!("expected Numeric");
        };
        assert_eq!(d.to_string(), "4503599627370496.5000");
    }

    #[test]
    fn from_totals_seeds_double() {
        let acc = AverageAccumulator::from_totals(
            &DfType::Double,
            AvgScaleMode::Fixed(0),
            &DfValue::Double(34.0),
            5,
        )
        .unwrap();
        assert_eq!(acc.result().unwrap(), DfValue::Double(6.8));
    }
}
