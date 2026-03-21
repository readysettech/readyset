use std::collections::HashMap;

use readyset_data::dialect::SqlEngine;
use readyset_data::{DfType, Dialect};
use readyset_errors::{invariant, ReadySetResult};
pub use readyset_sql::ast::{BinaryOperator, Literal, SqlType};
use serde::{Deserialize, Serialize};

use crate::node::AuxiliaryNodeState;
use crate::ops::grouped::{GroupedOperation, GroupedOperator};
use crate::prelude::*;

use super::{hash_grouped_records, GroupHash};

/// Supported aggregation operators.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Aggregation {
    /// Count the number of non-null values.
    Count,
    /// Sum the value of the `over` column for all records of each group.
    Sum,
    /// Average the value of the `over` column. Maintains count and sum in HashMap
    Avg,
}

impl Aggregation {
    /// Construct a new [`Aggregator`] that performs this operation.
    ///
    /// The aggregation will aggregate the value in column number `over` from its inputs (i.e.,
    /// from the `src` node in the graph), and use the columns in the `group_by` array as a group
    /// identifier.
    pub fn over(
        self,
        src: NodeIndex,
        over: usize,
        group_by: &[usize],
        over_col_ty: &DfType,
        dialect: &Dialect,
    ) -> ReadySetResult<GroupedOperator<Aggregator>> {
        let out_ty = match &self {
            Aggregation::Count => {
                antithesis_sdk::assert_reachable!("Aggregation::Count");
                DfType::BigInt
            }
            Aggregation::Sum => {
                antithesis_sdk::assert_reachable!("Aggregation::Sum");

                match dialect.engine() {
                    SqlEngine::MySQL => {
                        if over_col_ty.is_any_float() || over_col_ty.is_any_text() {
                            // MySQL returns DOUBLE for SUM over float and text columns
                            DfType::Double
                        } else if let Some(dec_prec) = over_col_ty.mysql_decimal_precision() {
                            // MySQL SUM on integers: DECIMAL(prec + 22, 0), capped at 65
                            DfType::Numeric {
                                prec: (dec_prec + 22).min(65),
                                scale: 0,
                            }
                        } else if let DfType::Numeric { prec, scale } = over_col_ty {
                            // MySQL SUM on DECIMAL: DECIMAL(min(prec+22, 65), scale)
                            DfType::Numeric {
                                prec: (*prec + 22).min(65),
                                scale: *scale,
                            }
                        } else {
                            DfType::DEFAULT_NUMERIC
                        }
                    }
                    SqlEngine::PostgreSQL => {
                        if over_col_ty.is_any_bigint() {
                            // SUM(bigint) → numeric (scale 0)
                            DfType::DEFAULT_NUMERIC
                        } else if let DfType::Numeric { prec, scale } = over_col_ty {
                            // SUM(numeric(p,s)) → numeric preserving scale
                            DfType::Numeric {
                                prec: *prec,
                                scale: *scale,
                            }
                        } else if over_col_ty.is_numeric() {
                            // SUM(numeric) without explicit precision
                            DfType::DEFAULT_NUMERIC
                        } else if over_col_ty.is_any_int() {
                            DfType::BigInt
                        } else if over_col_ty.is_float() {
                            DfType::Float
                        } else if over_col_ty.is_double() {
                            DfType::Double
                        } else {
                            invalid_query!("Cannot sum over type {}", over_col_ty)
                        }
                    }
                }
            }
            Aggregation::Avg => {
                antithesis_sdk::assert_reachable!("Aggregation::Avg");
                match dialect.engine() {
                    SqlEngine::MySQL => DfType::mysql_avg_output_type(over_col_ty),
                    SqlEngine::PostgreSQL => {
                        if over_col_ty.is_any_float() {
                            DfType::Double
                        } else {
                            DfType::DEFAULT_NUMERIC
                        }
                    }
                }
            }
        };

        let avg_scale_mode = match (&self, dialect.engine()) {
            (Aggregation::Avg, SqlEngine::PostgreSQL) => match &out_ty {
                DfType::Numeric { .. } => AvgScaleMode::PostgresComputed,
                _ => AvgScaleMode::Fixed(0), // Double — no rounding
            },
            (Aggregation::Avg, SqlEngine::MySQL) => match &out_ty {
                DfType::Numeric { scale, .. } => AvgScaleMode::Fixed(*scale as i64),
                _ => AvgScaleMode::Fixed(0),
            },
            _ => AvgScaleMode::Fixed(0),
        };

        Ok(GroupedOperator::new(
            src,
            Aggregator {
                op: self,
                over,
                group: group_by.into(),
                over_else: None,
                out_ty,
                avg_scale_mode,
            },
        ))
    }
}

/// Aggregator implements a Dataflow node that performs common aggregation operations such as counts
/// and sums
///
/// `Aggregator` nodes are constructed through `Aggregation` variants using `Aggregation::new`.
///
/// When a new record arrives, the aggregator will first query the currently aggregated value for
/// the new record's group by doing a query into its own output. The aggregated column
/// (`self.over`) of the incoming record is then added to the current aggregation value according
/// to the operator in use. The output record is constructed by concatenating the columns
/// identifying the group, and appending the aggregated value. For example, for a sum with
/// `self.over == 1`, a previous sum of `3`, and an incoming record with `[a, 1, x]`, the output
/// would be `[a, x, 4]`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Aggregator {
    op: Aggregation,
    over: usize,
    group: Vec<usize>,
    over_else: Option<Literal>,
    // Output type of this column
    out_ty: DfType,
    /// How to compute the display scale for AVG results.
    #[serde(default = "default_scale_mode")]
    avg_scale_mode: AvgScaleMode,
}

/// Diff type for numerical aggregations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NumericalDiff {
    /// Numerical value of the diff of the `over` column
    value: DfValue,
    /// True if positive record, false if negative
    positive: bool,
    /// Hash of the values of the group by columns, needed for AVG
    group_hash: GroupHash,
}

/// Controls how AVG display scale is computed after division.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
enum AvgScaleMode {
    /// MySQL: round to a fixed number of decimal places (determined at plan time).
    Fixed(i64),
    /// PostgreSQL: dynamically compute display scale targeting ~16 significant digits,
    /// dependent on the magnitude of sum and count.
    PostgresComputed,
}

/// For storing (Count, Sum) in additional state for Average.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct AverageDataPair {
    count: DfValue,
    sum: DfValue,
    /// The value returned when the group has no non-NULL rows (i.e., count
    /// drops to 0).  This is SQL NULL, not a numeric zero — `sum` uses a
    /// typed numeric zero for correct arithmetic accumulation.
    #[serde(default = "default_avg_empty_value")]
    empty_value: DfValue,
    /// If set, round the result to this many decimal places.
    #[serde(default)]
    target_scale: Option<i64>,
    /// Scale computation strategy for the AVG result.
    #[serde(default = "default_scale_mode")]
    scale_mode: AvgScaleMode,
}

fn default_scale_mode() -> AvgScaleMode {
    // Legacy default: use target_scale (Fixed) or no rounding
    AvgScaleMode::Fixed(0)
}

fn default_avg_empty_value() -> DfValue {
    DfValue::None
}

/// Computes display scale for Postgres numeric division, matching PostgreSQL behavior.
///
/// Returns the number of digits after the decimal point for a numeric division
/// result, targeting ~16 significant digits.
///
/// The algorithm operates on base-10000 digit weights:
/// - Estimate the quotient's weight (number of base-10000 groups before the decimal)
/// - Set scale = 16 - weight * 4 (more integer digits → fewer fractional digits)
/// - Ensure at least as many decimal places as either input has
/// - Clamp to [0, 1000]
fn postgres_select_div_scale(sum: &DfValue, count: &DfValue) -> i64 {
    const NUMERIC_MIN_SIG_DIGITS: i64 = 16;
    const DEC_DIGITS: i64 = 4;
    const MAX_DISPLAY_SCALE: i64 = 1000;

    /// Compute the base-10000 weight and first base-10000 digit of an integer value.
    fn weight_and_first_int(val: u128) -> (i64, u16) {
        if val == 0 {
            return (0, 0);
        }
        let digits = (val as f64).log10().floor() as i64 + 1;
        let weight = (digits - 1) / DEC_DIGITS;
        let first_group_digits = ((digits - 1) % DEC_DIGITS) + 1;
        let divisor = 10u128.pow((digits - first_group_digits) as u32);
        let first = val / divisor;
        (weight, first.max(1) as u16)
    }

    /// Compute weight/first-digit and display scale from a DfValue.
    fn decompose(val: &DfValue) -> (i64, u16, i64) {
        match val {
            DfValue::Numeric(dec) => {
                let (mantissa, scale) = match dec.mantissa_and_scale() {
                    Some(ms) => ms,
                    None => return (0, 0, 0),
                };
                if mantissa == 0 {
                    return (0, 0, scale);
                }
                let abs = mantissa.unsigned_abs();
                let total_digits = (abs as f64).log10().floor() as i64 + 1;
                let int_digits = total_digits - scale;

                let (weight, first) = if int_digits <= 0 {
                    // Value < 1
                    let leading_zeros = (-int_digits) / DEC_DIGITS;
                    let w = -(leading_zeros + 1);
                    // First non-zero base-10000 digit
                    let frac_offset = ((-int_digits) % DEC_DIGITS) as u32;
                    let f = (abs / 10u128.pow(total_digits as u32 - 1 - frac_offset)) % 10000;
                    (w, f.max(1) as u16)
                } else {
                    let w = (int_digits - 1) / DEC_DIGITS;
                    let first_group = ((int_digits - 1) % DEC_DIGITS) + 1;
                    let divisor = 10u128.pow((int_digits - first_group) as u32);
                    let int_part = abs / 10u128.pow(scale as u32);
                    let f = int_part / divisor;
                    (w, f.max(1) as u16)
                };
                (weight, first, scale)
            }
            DfValue::Int(n) => {
                let (w, f) = weight_and_first_int(n.unsigned_abs() as u128);
                (w, f, 0)
            }
            DfValue::UnsignedInt(n) => {
                let (w, f) = weight_and_first_int(*n as u128);
                (w, f, 0)
            }
            _ => (0, 1, 0),
        }
    }

    let (weight1, first1, sum_dscale) = decompose(sum);
    let (weight2, first2, count_dscale) = decompose(count);

    let mut qweight = weight1 - weight2;
    if first1 <= first2 {
        qweight -= 1;
    }

    let mut rscale = NUMERIC_MIN_SIG_DIGITS - qweight * DEC_DIGITS;
    rscale = rscale.max(sum_dscale);
    rscale = rscale.max(count_dscale);
    rscale = rscale.max(0);
    rscale = rscale.min(MAX_DISPLAY_SCALE);

    rscale
}
impl AverageDataPair {
    fn apply_diff(&mut self, d: NumericalDiff) -> ReadySetResult<DfValue> {
        if d.positive {
            self.sum = (&self.sum + &d.value)?;
            self.count = (&self.count + &DfValue::Int(1))?;
        } else {
            self.sum = (&self.sum - &d.value)?;
            self.count = (&self.count - &DfValue::Int(1))?;
        }

        if self.count > DfValue::Int(0) {
            let result = (&self.sum / &self.count)?;
            match (&result, &self.scale_mode) {
                (DfValue::Numeric(dec), AvgScaleMode::Fixed(scale)) => {
                    Ok(DfValue::from(dec.round_dp(*scale)))
                }
                (DfValue::Numeric(dec), AvgScaleMode::PostgresComputed) => {
                    let scale = postgres_select_div_scale(&self.sum, &self.count);
                    Ok(DfValue::from(dec.round_dp(scale)))
                }
                _ => Ok(result),
            }
        } else {
            Ok(self.empty_value.clone())
        }
    }
}

#[derive(Debug, Default)]
/// Auxiliary State for an Aggregator node, which is owned by a Domain
pub struct AggregatorState {
    count_sum_map: HashMap<GroupHash, AverageDataPair>,
}

impl Aggregator {
    fn new_data(&self) -> ReadySetResult<DfValue> {
        match &self.out_ty {
            DfType::BigInt => Ok(DfValue::Int(Default::default())),
            DfType::Double => Ok(DfValue::Double(Default::default())),
            DfType::Float => Ok(DfValue::Float(Default::default())),
            DfType::Numeric { .. } => Ok(DfValue::Numeric(Default::default())),
            DfType::Text { .. } => Ok(DfValue::from("" /* TODO(aspen): Use collation here */)),
            e => unsupported!("Unsupported output type for aggregation: {}", e),
        }
    }
}

impl GroupedOperation for Aggregator {
    type Diff = NumericalDiff;

    fn setup(&mut self, parent: &Node) -> ReadySetResult<()> {
        invariant!(
            self.over < parent.columns().len(),
            "cannot aggregate over non-existing column"
        );

        Ok(())
    }

    fn group_by(&self) -> &[usize] {
        &self.group[..]
    }

    fn to_diff(&self, r: &[DfValue], pos: bool) -> ReadySetResult<Self::Diff> {
        let group_hash = hash_grouped_records(r, self.group_by());
        let value = r[self.over].clone();
        let value = match self.op {
            Aggregation::Sum | Aggregation::Avg => {
                // MySQL implicitly coerces text to numeric for SUM/AVG,
                // treating non-numeric strings as 0.
                value
                    .coerce_to(&self.out_ty, &DfType::Unknown)
                    .unwrap_or_else(|_| self.new_data().unwrap_or(DfValue::Int(0)))
            }
            Aggregation::Count => value,
        };
        Ok(NumericalDiff {
            value,
            positive: pos,
            group_hash,
        })
    }

    fn apply(
        &self,
        current: Option<&DfValue>,
        diffs: &mut dyn Iterator<Item = Self::Diff>,
        auxiliary_node_state: Option<&mut AuxiliaryNodeState>,
    ) -> ReadySetResult<Option<DfValue>> {
        let apply_count = |curr: DfValue, diff: Self::Diff| -> ReadySetResult<DfValue> {
            if diff.positive {
                &curr + &DfValue::Int(1)
            } else {
                &curr - &DfValue::Int(1)
            }
        };

        let apply_sum = |curr: DfValue, diff: Self::Diff| -> ReadySetResult<DfValue> {
            if curr.is_none() {
                if diff.positive {
                    // First non-NULL positive value initializes the sum.
                    Ok(diff.value)
                } else {
                    // A negative record when sum is NULL should not occur
                    // in normal operation, but can arise during state
                    // recovery / replay.  Start from the typed zero and
                    // subtract.
                    let zero = self.new_data()?;
                    &zero - &diff.value
                }
            } else if diff.positive {
                &curr + &diff.value
            } else {
                &curr - &diff.value
            }
        };

        let count_sum_map = match auxiliary_node_state {
            Some(AuxiliaryNodeState::Aggregation(ref mut aggregator_state)) => {
                &mut aggregator_state.count_sum_map
            }
            Some(_) => internal!("Incorrect auxiliary state for Aggregation node"),
            None => internal!("Missing auxiliary state for Aggregation node"),
        };

        let avg_zero = self.new_data()?;
        let avg_target_scale = match &self.out_ty {
            DfType::Numeric { scale, .. } => Some(*scale as i64),
            _ => None,
        };
        let avg_scale_mode = self.avg_scale_mode;
        let mut apply_avg = |_curr, diff: Self::Diff| -> ReadySetResult<DfValue> {
            count_sum_map
                .entry(diff.group_hash)
                .or_insert(AverageDataPair {
                    // sum starts at typed numeric zero (not NULL) so that
                    // arithmetic with the first non-NULL value works
                    // correctly (0 + x = x).  empty_value controls what
                    // is returned when count drops to 0.
                    sum: avg_zero.clone(),
                    count: DfValue::Int(0),
                    empty_value: DfValue::None,
                    target_scale: avg_target_scale,
                    scale_mode: avg_scale_mode,
                })
                .apply_diff(diff)
        };

        let apply_diff =
            |curr: ReadySetResult<DfValue>, diff: Self::Diff| -> ReadySetResult<DfValue> {
                if diff.value.is_none() {
                    return curr;
                }

                match self.op {
                    Aggregation::Count => apply_count(curr?, diff),
                    Aggregation::Sum => apply_sum(curr?, diff),
                    Aggregation::Avg => apply_avg(curr?, diff),
                }
            };

        let initial = match (current, &self.op) {
            (Some(val), _) => val.clone(),
            (None, Aggregation::Sum | Aggregation::Avg) => DfValue::None,
            (None, _) => self.new_data()?,
        };
        diffs.fold(Ok(initial), apply_diff).map(Some)
    }

    fn description(&self) -> String {
        let op_string = match self.op {
            Aggregation::Count => "|*|".to_owned(),
            Aggregation::Sum => format!("𝛴({})", self.over),
            Aggregation::Avg => format!("Avg({})", self.over),
        };
        let group_cols = self
            .group
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        format!("{op_string} γ[{group_cols}]")
    }

    fn over_column(&self) -> usize {
        self.over
    }

    fn output_col_type(&self) -> DfType {
        self.out_ty.clone()
    }

    fn empty_value(&self) -> Option<DfValue> {
        match self.op {
            Aggregation::Count => Some(DfValue::Int(0)),
            Aggregation::Sum | Aggregation::Avg => Some(DfValue::None),
        }
    }

    fn emit_empty(&self) -> bool {
        self.group_by().is_empty()
    }

    fn can_lose_state(&self) -> bool {
        false
    }
}

// TODO: These unit tests are lengthy, repetitive, and hard to read.
// Could look into refactoring / creating a more robust testing infrastructure to consolidate
// logic and create test cases more easily.

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use readyset_decimal::Decimal;

    use super::*;
    use crate::{ops, LookupIndex};

    fn setup(aggregation: Aggregation, mat: bool) -> ops::test::MockGraph {
        let mut g = ops::test::MockGraph::new();
        let s = g.add_base("source", &["x", "y"]);
        g.set_op(
            "identity",
            &["x", "ys"],
            aggregation
                .over(
                    s.as_global(),
                    1,
                    &[0],
                    &DfType::Double,
                    &Dialect::DEFAULT_MYSQL,
                )
                .unwrap(),
            mat,
        );
        g
    }

    fn setup_multicolumn(aggregation: Aggregation, mat: bool) -> ops::test::MockGraph {
        let mut g = ops::test::MockGraph::new();
        let s = g.add_base("source", &["x", "y", "z"]);
        g.set_op(
            "identity",
            &["x", "z", "ys"],
            aggregation
                .over(
                    s.as_global(),
                    1,
                    &[0, 2],
                    &DfType::Double,
                    &Dialect::DEFAULT_MYSQL,
                )
                .unwrap(),
            mat,
        );
        g
    }

    #[test]
    fn it_describes() {
        let src = 0.into();

        let c = Aggregation::Count
            .over(src, 1, &[0, 2], &DfType::Unknown, &Dialect::DEFAULT_MYSQL)
            .unwrap();
        assert_eq!(c.description(), "|*| γ[0, 2]");

        let s = Aggregation::Sum
            .over(src, 1, &[2, 0], &DfType::Unknown, &Dialect::DEFAULT_MYSQL)
            .unwrap();
        assert_eq!(s.description(), "𝛴(1) γ[2, 0]");

        let a = Aggregation::Avg
            .over(src, 1, &[2, 0], &DfType::Unknown, &Dialect::DEFAULT_MYSQL)
            .unwrap();
        assert_eq!(a.description(), "Avg(1) γ[2, 0]");
    }

    /// Testing count emits correct records with single column group and single over column
    /// Records are in the form of (GroupCol, OverCol).
    /// Includes adding and removing records from different groups independently and in batch.
    #[test]
    #[allow(clippy::cognitive_complexity)]
    fn count_forwards() {
        let mut c = setup(Aggregation::Count, true);

        // Add Group=1, Value=1
        let u: Record = vec![1.into(), 1.into()].into();
        let rs = c.narrow_one(u, true);
        assert_eq!(rs.len(), 1);
        let mut rs = rs.into_iter();

        // Expect Positive(1,1)
        match rs.next().unwrap() {
            Record::Positive(r) => {
                assert_eq!(r[0], 1.into());
                assert_eq!(r[1], 1.into());
            }
            _ => unreachable!(),
        }

        // Add Group=2, Value=2
        let u: Record = vec![2.into(), 2.into()].into();
        let rs = c.narrow_one(u, true);
        assert_eq!(rs.len(), 1);
        let mut rs = rs.into_iter();

        // Expect Positive(1,1)
        match rs.next().unwrap() {
            Record::Positive(r) => {
                assert_eq!(r[0], 2.into());
                assert_eq!(r[1], 1.into());
            }
            _ => unreachable!(),
        }

        // Add Group=1, Value=2
        let u: Record = vec![1.into(), 2.into()].into();
        let rs = c.narrow_one(u, true);
        assert_eq!(rs.len(), 2);
        let mut rs = rs.into_iter();

        //Expect Negative(1,1), Positive(1,2)
        match rs.next().unwrap() {
            Record::Negative(r) => {
                assert_eq!(r[0], 1.into());
                assert_eq!(r[1], 1.into());
            }
            _ => unreachable!(),
        }
        match rs.next().unwrap() {
            Record::Positive(r) => {
                assert_eq!(r[0], 1.into());
                assert_eq!(r[1], 2.into());
            }
            _ => unreachable!(),
        }

        // Remove Group=1, Value=1
        let u = (vec![1.into(), 1.into()], false);
        let rs = c.narrow_one_row(u, true);
        assert_eq!(rs.len(), 2);
        let mut rs = rs.into_iter();

        // Expect Negative(1,2), Positive(1,1)
        match rs.next().unwrap() {
            Record::Negative(r) => {
                assert_eq!(r[0], 1.into());
                assert_eq!(r[1], 2.into());
            }
            _ => unreachable!(),
        }
        match rs.next().unwrap() {
            Record::Positive(r) => {
                assert_eq!(r[0], 1.into());
                assert_eq!(r[1], 1.into());
            }
            _ => unreachable!(),
        }

        // Test multiple records at once
        let u = vec![
            (vec![1.into(), 1.into()], false),
            (vec![1.into(), 1.into()], true),
            (vec![1.into(), 2.into()], true),
            (vec![2.into(), 2.into()], false),
            (vec![2.into(), 2.into()], true),
            (vec![2.into(), 3.into()], true),
            (vec![2.into(), 1.into()], true),
            (vec![3.into(), 3.into()], true),
        ];

        // Group 1 expect Negative(1,1), Positive(1,2)
        let rs = c.narrow_one(u, true);
        assert_eq!(rs.len(), 5);
        assert!(rs.iter().any(|r| if let Record::Negative(ref r) = *r {
            r[0] == 1.into() && r[1] == 1.into()
        } else {
            false
        }));
        assert!(rs.iter().any(|r| if let Record::Positive(ref r) = *r {
            r[0] == 1.into() && r[1] == 2.into()
        } else {
            false
        }));

        // Group 2 expect Negative(2,1), Positive(2,3)
        assert!(rs.iter().any(|r| if let Record::Negative(ref r) = *r {
            r[0] == 2.into() && r[1] == 1.into()
        } else {
            false
        }));
        assert!(rs.iter().any(|r| if let Record::Positive(ref r) = *r {
            r[0] == 2.into() && r[1] == 3.into()
        } else {
            false
        }));

        //Group 3 expect Positive(3,1)
        assert!(rs.iter().any(|r| if let Record::Positive(ref r) = *r {
            r[0] == 3.into() && r[1] == 1.into()
        } else {
            false
        }));
    }

    #[test]
    fn count_empty_group() {
        let mut c = setup(Aggregation::Count, true);

        let u = Record::from(vec![1.into(), 1.into()]);
        let rs = c.narrow_one(u, true);
        assert_eq!(
            rs,
            vec![Record::Positive(vec![1.into(), 1.into(), 1.into()])].into()
        );

        let del = Record::Negative(vec![1.into(), 1.into()]);
        let del_res = c.narrow_one(del, true);
        assert_eq!(
            del_res,
            vec![Record::Negative(vec![1.into(), 1.into(), 1.into()])].into()
        );
    }

    /// Testing SUM emits correct records with single column group and single over column
    /// Records are in the form of (GroupCol, OverCol)
    /// Includes adding and removing records from different groups independently and in batch.
    #[test]
    #[allow(clippy::cognitive_complexity)]
    fn sum_forwards() {
        let mut c = setup(Aggregation::Sum, true);

        // Add Group=1, Value=2
        let u: Record = vec![1.into(), 2.into()].into();
        let rs = c.narrow_one(u, true);
        assert_eq!(rs.len(), 1);
        let mut rs = rs.into_iter();

        // Expect Positive(1,2)
        match rs.next().unwrap() {
            Record::Positive(r) => {
                assert_eq!(r[0], 1.into());
                assert_eq!(r[1], (2.).try_into().unwrap());
            }
            _ => unreachable!(),
        }

        // Add Group=2, Value=5
        let u: Record = vec![2.into(), 5.into()].into();
        let rs = c.narrow_one(u, true);
        assert_eq!(rs.len(), 1);
        let mut rs = rs.into_iter();

        // Expect Positive(2,5)
        match rs.next().unwrap() {
            Record::Positive(r) => {
                assert_eq!(r[0], 2.into());
                assert_eq!(r[1], (5.).try_into().unwrap());
            }
            _ => unreachable!(),
        }

        // Add Group=1, Value=3
        let u: Record = vec![1.into(), 3.into()].into();
        let rs = c.narrow_one(u, true);
        assert_eq!(rs.len(), 2);
        let mut rs = rs.into_iter();

        // Expect Negative(1,2), Positive(1,5)
        match rs.next().unwrap() {
            Record::Negative(r) => {
                assert_eq!(r[0], 1.into());
                assert_eq!(r[1], (2.).try_into().unwrap());
            }
            _ => unreachable!(),
        }
        match rs.next().unwrap() {
            Record::Positive(r) => {
                assert_eq!(r[0], 1.into());
                assert_eq!(r[1], (5.).try_into().unwrap());
            }
            _ => unreachable!(),
        }

        // Remove Group=1, Value=2
        let u = (vec![1.into(), 2.into()], false);
        let rs = c.narrow_one_row(u, true);
        assert_eq!(rs.len(), 2);
        let mut rs = rs.into_iter();

        // Expect Negative(1,5), Positive(1,3)
        match rs.next().unwrap() {
            Record::Negative(r) => {
                assert_eq!(r[0], 1.into());
                assert_eq!(r[1], (5.).try_into().unwrap());
            }
            _ => unreachable!(),
        }
        match rs.next().unwrap() {
            Record::Positive(r) => {
                assert_eq!(r[0], 1.into());
                assert_eq!(r[1], (3.).try_into().unwrap());
            }
            _ => unreachable!(),
        }

        // Test multiple records at once
        let u = vec![
            (vec![1.into(), 2.into()], true),
            (vec![1.into(), 3.into()], true),
            (vec![1.into(), 2.into()], false),
            (vec![1.into(), 5.into()], true),
            (vec![1.into(), 3.into()], false), // Group 1 gains +5
            (vec![2.into(), 5.into()], true),
            (vec![2.into(), 5.into()], false),
            (vec![2.into(), 2.into()], true),
            (vec![2.into(), 2.into()], false),
            (vec![2.into(), 5.into()], false), // Group 2 loses last row and disappears
            (vec![3.into(), 3.into()], true),  // Group 3 is new, +3
        ];

        // Group 1: 3 -> 8
        // Group 2: 5 -> 4
        // Group 3: new 3

        // multiple positives and negatives should update aggregation value by appropriate amount
        let rs = c.narrow_one(u, true);
        assert_eq!(rs.len(), 4);
        assert!(rs.iter().any(|r| if let Record::Negative(ref r) = *r {
            r[0] == 1.into() && r[1] == (3.).try_into().unwrap()
        } else {
            false
        }));
        assert!(rs.iter().any(|r| if let Record::Positive(ref r) = *r {
            r[0] == 1.into() && r[1] == (8.).try_into().unwrap()
        } else {
            false
        }));
        assert!(rs.iter().any(|r| if let Record::Negative(ref r) = *r {
            r[0] == 2.into() && r[1] == (5.).try_into().unwrap()
        } else {
            false
        }));
        assert!(rs.iter().any(|r| if let Record::Positive(ref r) = *r {
            r[0] == 3.into() && r[1] == (3.).try_into().unwrap()
        } else {
            false
        }));
    }

    /// Testing AVG emits correct records with single column group and single integer over column
    /// Records are in the form of (GroupCol, OverCol)
    /// Includes adding and removing records from different groups independently and in batch.
    #[test]
    #[allow(clippy::cognitive_complexity)]
    fn avg_of_integers_forwards() {
        use std::convert::TryFrom;
        let mut c = setup(Aggregation::Avg, true);

        // Add Group=1, Value=2
        let u: Record = vec![1.into(), 2.into()].into();
        let rs = c.narrow_one(u, true);
        assert_eq!(rs.len(), 1);
        let mut rs = rs.into_iter();

        // Expect Positive(1, 2.0)
        match rs.next().unwrap() {
            Record::Positive(r) => {
                assert_eq!(r[0], 1.into());
                assert_eq!(r[1], DfValue::try_from(2.0).unwrap());
            }
            _ => unreachable!(),
        }

        // Add Group=2, Value=5
        let u: Record = vec![2.into(), 5.into()].into();
        let rs = c.narrow_one(u, true);
        assert_eq!(rs.len(), 1);
        let mut rs = rs.into_iter();

        //Expect Positive(2, 5.0)
        match rs.next().unwrap() {
            Record::Positive(r) => {
                assert_eq!(r[0], 2.into());
                assert_eq!(r[1], DfValue::try_from(5.0).unwrap());
            }
            _ => unreachable!(),
        }

        // Add Group=1, Value=3. Expect -2.0, +2.5
        let u: Record = vec![1.into(), 3.into()].into();
        let rs = c.narrow_one(u, true);
        assert_eq!(rs.len(), 2);
        let mut rs = rs.into_iter();

        // Expect Negative(1, 2.0), Positive(1, 2.5)
        match rs.next().unwrap() {
            Record::Negative(r) => {
                assert_eq!(r[0], 1.into());
                assert_eq!(r[1], DfValue::try_from(2.0).unwrap());
            }
            _ => unreachable!(),
        }
        match rs.next().unwrap() {
            Record::Positive(r) => {
                assert_eq!(r[0], 1.into());
                assert_eq!(r[1], DfValue::try_from(2.5).unwrap());
            }
            _ => unreachable!(),
        }

        // Remove Group=1, Value=2
        let u = (vec![1.into(), 2.into()], false);
        let rs = c.narrow_one_row(u, true);
        assert_eq!(rs.len(), 2);
        let mut rs = rs.into_iter();

        // Expect Negative(1, 2.5), Positive(1, 3.0)
        match rs.next().unwrap() {
            Record::Negative(r) => {
                assert_eq!(r[0], 1.into());
                assert_eq!(r[1], DfValue::try_from(2.5).unwrap());
            }
            _ => unreachable!(),
        }
        match rs.next().unwrap() {
            Record::Positive(r) => {
                assert_eq!(r[0], 1.into());
                assert_eq!(r[1], DfValue::try_from(3.0).unwrap());
            }
            _ => unreachable!(),
        }

        // Test multiple records at once
        // Group 1: (3/1) -> (24/2)
        // Group 2: (5/1) -> (25/4)
        // Group 3: new 3
        let u = vec![
            (vec![1.into(), 14.into()], true),
            (vec![1.into(), 8.into()], true),
            (vec![1.into(), 3.into()], true),
            (vec![1.into(), 2.into()], false),
            (vec![1.into(), 2.into()], false),
            (vec![2.into(), 10.into()], true),
            (vec![2.into(), 7.into()], true),
            (vec![2.into(), 3.into()], true),
            (vec![2.into(), 2.into()], true),
            (vec![2.into(), 2.into()], false),
            (vec![3.into(), 3.into()], true),
        ];

        let rs = c.narrow_one(u, true);
        assert_eq!(rs.len(), 5);
        assert!(rs.iter().any(|r| if let Record::Negative(ref r) = *r {
            r[0] == 1.into() && r[1] == DfValue::try_from(3.0).unwrap()
        } else {
            false
        }));
        assert!(rs.iter().any(|r| if let Record::Positive(ref r) = *r {
            r[0] == 1.into() && r[1] == DfValue::try_from(12.0).unwrap()
        } else {
            false
        }));
        assert!(rs.iter().any(|r| if let Record::Negative(ref r) = *r {
            r[0] == 2.into() && r[1] == DfValue::try_from(5.0).unwrap()
        } else {
            false
        }));
        assert!(rs.iter().any(|r| if let Record::Positive(ref r) = *r {
            r[0] == 2.into() && r[1] == DfValue::try_from(6.25).unwrap()
        } else {
            false
        }));
        assert!(rs.iter().any(|r| if let Record::Positive(ref r) = *r {
            r[0] == 3.into() && r[1] == DfValue::try_from(3.0).unwrap()
        } else {
            false
        }));
    }

    /// Testing AVG emits correct records with single column group and single decimal over column
    /// Records are in the form of (GroupCol, OverCol)
    /// Includes adding and removing records from different groups independently and in batch.
    #[test]
    #[allow(clippy::cognitive_complexity)]
    fn avg_of_decimals_forwards() {
        use std::convert::TryFrom;
        let mut c = setup(Aggregation::Avg, true);

        // Add [1, 1.25]
        let u: Record = vec![1.into(), DfValue::try_from(1.25).unwrap()].into();
        let rs = c.narrow_one(u, true);
        assert_eq!(rs.len(), 1);
        let mut rs = rs.into_iter();

        // Expect Positive(1, 1.25)
        match rs.next().unwrap() {
            Record::Positive(r) => {
                assert_eq!(r[0], 1.into());
                assert_eq!(r[1], DfValue::try_from(1.25).unwrap());
            }
            _ => unreachable!(),
        }

        // Add [2, 5.5]
        let u: Record = vec![2.into(), DfValue::try_from(5.5).unwrap()].into();
        let rs = c.narrow_one(u, true);
        assert_eq!(rs.len(), 1);
        let mut rs = rs.into_iter();

        // Expect Positive(2, 5.5)
        match rs.next().unwrap() {
            Record::Positive(r) => {
                assert_eq!(r[0], 2.into());
                assert_eq!(r[1], DfValue::try_from(5.5).unwrap());
            }
            _ => unreachable!(),
        }

        // Add [1,2.25]
        // Now: [1, 2.25], [1, 1.25]
        let u: Record = vec![1.into(), DfValue::try_from(2.25).unwrap()].into();
        let rs = c.narrow_one(u, true);
        assert_eq!(rs.len(), 2);
        let mut rs = rs.into_iter();

        // Expect Negative(1,1.25), Positive(1, 1.75)
        match rs.next().unwrap() {
            Record::Negative(r) => {
                assert_eq!(r[0], 1.into());
                assert_eq!(r[1], DfValue::try_from(1.25).unwrap());
            }
            _ => unreachable!(),
        }
        match rs.next().unwrap() {
            Record::Positive(r) => {
                assert_eq!(r[0], 1.into());
                assert_eq!(r[1], DfValue::try_from(1.75).unwrap());
            }
            _ => unreachable!(),
        }

        // Remove [1, 1.25]
        let u = (vec![1.into(), DfValue::try_from(1.25).unwrap()], false);
        let rs = c.narrow_one_row(u, true);
        assert_eq!(rs.len(), 2);
        let mut rs = rs.into_iter();

        // Expect Negative(1, 1.75), Positive(1, 2.25)
        match rs.next().unwrap() {
            Record::Negative(r) => {
                assert_eq!(r[0], 1.into());
                assert_eq!(r[1], DfValue::try_from(1.75).unwrap());
            }
            _ => unreachable!(),
        }
        match rs.next().unwrap() {
            Record::Positive(r) => {
                assert_eq!(r[0], 1.into());
                assert_eq!(r[1], DfValue::try_from(2.25).unwrap());
            }
            _ => unreachable!(),
        }

        // Test multiple records at once
        // Group 1: (2.25/1) -> (15.75/3) = 5.25
        // Group 2: (5.5/1) -> (10.5/2) = 5.25
        // Group 3: new 3
        let u = vec![
            (vec![1.into(), DfValue::try_from(12.4).unwrap()], true),
            (vec![1.into(), DfValue::try_from(1.15).unwrap()], true),
            (vec![1.into(), DfValue::try_from(1.05).unwrap()], true),
            (vec![1.into(), DfValue::try_from(1.1).unwrap()], true),
            (vec![1.into(), DfValue::try_from(1.15).unwrap()], false),
            (vec![1.into(), DfValue::try_from(1.05).unwrap()], false),
            (vec![2.into(), DfValue::try_from(5.25).unwrap()], true),
            (vec![2.into(), DfValue::try_from(0.75).unwrap()], true),
            (vec![2.into(), DfValue::try_from(1.0).unwrap()], false),
            (vec![3.into(), 3.into()], true),
        ];

        let rs = c.narrow_one(u, true);
        assert_eq!(rs.len(), 5);
        let precision = Some(10.0_f64.powf(2.0_f64));
        assert!(rs.iter().any(|r| if let Record::Negative(ref r) = *r {
            r[0] == 1.into()
                && r[1].equal_under_error_margin(&DfValue::try_from(2.25).unwrap(), precision)
        } else {
            false
        }));
        assert!(rs.iter().any(|r| if let Record::Positive(ref r) = *r {
            r[0] == 1.into()
                && r[1].equal_under_error_margin(&DfValue::try_from(5.25).unwrap(), precision)
        } else {
            false
        }));
        assert!(rs.iter().any(|r| if let Record::Negative(ref r) = *r {
            r[0] == 2.into()
                && r[1].equal_under_error_margin(&DfValue::try_from(5.5).unwrap(), precision)
        } else {
            false
        }));
        assert!(rs.iter().any(|r| if let Record::Positive(ref r) = *r {
            r[0] == 2.into()
                && r[1].equal_under_error_margin(&DfValue::try_from(5.25).unwrap(), precision)
        } else {
            false
        }));
        assert!(rs.iter().any(|r| if let Record::Positive(ref r) = *r {
            r[0] == 3.into()
                && r[1].equal_under_error_margin(&DfValue::try_from(3.0).unwrap(), precision)
        } else {
            false
        }));
    }

    /// Testing AVG emits correct records with multiple group by columns and single decimal
    /// over column. Similar to `avg_of_decimals_forwards` with additional group column.
    /// Records are in the form of (GroupCol1, OverCol, GroupCol2).
    #[test]
    #[allow(clippy::cognitive_complexity)]
    fn avg_groups_by_multiple_columns() {
        use std::convert::TryFrom;
        let mut c = setup_multicolumn(Aggregation::Avg, true);

        // Add Group=(1,1), Value=1.25
        let u: Record = vec![1.into(), DfValue::try_from(1.25).unwrap(), 1.into()].into();
        let rs = c.narrow_one(u, true);
        assert_eq!(rs.len(), 1);
        let mut rs = rs.into_iter();

        match rs.next().unwrap() {
            Record::Positive(r) => {
                assert_eq!(r[0], 1.into());
                assert_eq!(r[2], DfValue::try_from(1.25).unwrap());
            }
            _ => unreachable!(),
        }

        // Add Group=(2,1), Value=5.5
        let u: Record = vec![2.into(), DfValue::try_from(5.5).unwrap(), 1.into()].into();
        let rs = c.narrow_one(u, true);
        assert_eq!(rs.len(), 1);
        let mut rs = rs.into_iter();

        match rs.next().unwrap() {
            Record::Positive(r) => {
                assert_eq!(r[0], 2.into());
                assert_eq!(r[2], DfValue::try_from(5.5).unwrap());
            }
            _ => unreachable!(),
        }

        // Add Group=(1,1), Value=2.25
        let u: Record = vec![1.into(), DfValue::try_from(2.25).unwrap(), 1.into()].into();
        let rs = c.narrow_one(u, true);
        assert_eq!(rs.len(), 2);
        let mut rs = rs.into_iter();

        match rs.next().unwrap() {
            Record::Negative(r) => {
                assert_eq!(r[0], 1.into());
                assert_eq!(r[2], DfValue::try_from(1.25).unwrap());
            }
            _ => unreachable!(),
        }
        match rs.next().unwrap() {
            Record::Positive(r) => {
                assert_eq!(r[0], 1.into());
                assert_eq!(r[2], DfValue::try_from(1.75).unwrap());
            }
            _ => unreachable!(),
        }

        // Remove Group=(1,1), Value=1.25
        let u = (
            vec![1.into(), DfValue::try_from(1.25).unwrap(), 1.into()],
            false,
        );
        let rs = c.narrow_one_row(u, true);
        assert_eq!(rs.len(), 2);
        let mut rs = rs.into_iter();

        match rs.next().unwrap() {
            Record::Negative(r) => {
                assert_eq!(r[0], 1.into());
                assert_eq!(r[2], DfValue::try_from(1.75).unwrap());
            }
            _ => unreachable!(),
        }
        match rs.next().unwrap() {
            Record::Positive(r) => {
                assert_eq!(r[0], 1.into());
                assert_eq!(r[2], DfValue::try_from(2.25).unwrap());
            }
            _ => unreachable!(),
        }
    }

    /// Testing COUNT emits correct records with multiple group by columns and single
    /// over column. Similar to `count_forwards` with additional group column.
    /// Records are in the form of (GroupCol1, OverCol, GroupCol2).
    #[test]
    #[allow(clippy::cognitive_complexity)]
    fn count_groups_by_multiple_columns() {
        let mut c = setup_multicolumn(Aggregation::Count, true);

        // Add Group=(1,2), Value=1
        let u: Record = vec![1.into(), 1.into(), 2.into()].into();
        let rs = c.narrow_one(u, true);
        assert_eq!(rs.len(), 1);
        let mut rs = rs.into_iter();

        match rs.next().unwrap() {
            Record::Positive(r) => {
                assert_eq!(r[0], 1.into());
                assert_eq!(r[1], 2.into());
                assert_eq!(r[2], 1.into());
            }
            _ => unreachable!(),
        }

        // Add Group=(2,2), Value=1
        let u: Record = vec![2.into(), 1.into(), 2.into()].into();
        let rs = c.narrow_one(u, true);
        assert_eq!(rs.len(), 1);
        let mut rs = rs.into_iter();

        match rs.next().unwrap() {
            Record::Positive(r) => {
                assert_eq!(r[0], 2.into());
                assert_eq!(r[1], 2.into());
                assert_eq!(r[2], 1.into());
            }
            _ => unreachable!(),
        }

        // Add Group=(1,2), Value=1
        let u: Record = vec![1.into(), 1.into(), 2.into()].into();
        let rs = c.narrow_one(u, true);
        assert_eq!(rs.len(), 2);
        let mut rs = rs.into_iter();

        match rs.next().unwrap() {
            Record::Negative(r) => {
                assert_eq!(r[0], 1.into());
                assert_eq!(r[1], 2.into());
                assert_eq!(r[2], 1.into());
            }
            _ => unreachable!(),
        }
        match rs.next().unwrap() {
            Record::Positive(r) => {
                assert_eq!(r[0], 1.into());
                assert_eq!(r[1], 2.into());
                assert_eq!(r[2], 2.into());
            }
            _ => unreachable!(),
        }

        // Remove Group=(1,2), Value=1
        let u = (vec![1.into(), 1.into(), 2.into()], false);
        let rs = c.narrow_one_row(u, true);
        assert_eq!(rs.len(), 2);
        let mut rs = rs.into_iter();

        match rs.next().unwrap() {
            Record::Negative(r) => {
                assert_eq!(r[0], 1.into());
                assert_eq!(r[1], 2.into());
                assert_eq!(r[2], 2.into());
            }
            _ => unreachable!(),
        }
        match rs.next().unwrap() {
            Record::Positive(r) => {
                assert_eq!(r[0], 1.into());
                assert_eq!(r[1], 2.into());
                assert_eq!(r[2], 1.into());
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn it_suggests_indices() {
        let me = 1.into();
        let c = setup(Aggregation::Avg, false);
        let idx = c.node().suggest_indexes(me);

        // should only add index on own columns
        assert_eq!(idx.len(), 1);
        assert!(idx.contains_key(&me));

        // should only index on the group-by column
        assert_eq!(idx[&me], LookupIndex::Strict(Index::hash_map(vec![0])));
    }

    #[test]
    fn it_resolves() {
        let c = setup(Aggregation::Avg, false);
        assert_eq!(
            c.node().resolve(0),
            Some(vec![(c.narrow_base_id().as_global(), 0)])
        );
        assert_eq!(c.node().resolve(1), None);
    }

    #[test]
    fn sum_add_zero() {
        let mut c = setup(Aggregation::Sum, true);
        let out = c.narrow_one_row(vec!["grp".into(), 1.into()], true);
        assert_eq!(
            out,
            vec![vec![
                DfValue::from("grp"),
                DfValue::try_from(1.0f64).unwrap(),
                DfValue::from(1)
            ]]
            .into()
        );

        // Adding 0 to the group should still add 1 to the row count (the last column)
        let out = c.narrow_one_row(vec!["grp".into(), 0.into()], true);
        assert_eq!(
            out,
            vec![
                (
                    vec![
                        DfValue::from("grp"),
                        DfValue::try_from(1.0f64).unwrap(),
                        DfValue::from(1)
                    ],
                    false
                ),
                (
                    vec![
                        DfValue::from("grp"),
                        DfValue::try_from(1.0f64).unwrap(),
                        DfValue::from(2)
                    ],
                    true
                )
            ]
            .into()
        );
    }

    /// Helper to set up an aggregation over a text column (MySQL).
    fn setup_text_column(aggregation: Aggregation, mat: bool) -> ops::test::MockGraph {
        let mut g = ops::test::MockGraph::new();
        let s = g.add_base("source", &["x", "y"]);
        g.set_op(
            "identity",
            &["x", "ys"],
            aggregation
                .over(
                    s.as_global(),
                    1,
                    &[0],
                    &DfType::DEFAULT_TEXT,
                    &Dialect::DEFAULT_MYSQL,
                )
                .expect("failed to create aggregation over text column"),
            mat,
        );
        g
    }

    /// SUM over a text column should coerce text values to numeric (MySQL behavior).
    /// Non-numeric strings are treated as 0. MySQL returns DOUBLE for SUM(text).
    #[test]
    fn sum_over_text_column() {
        let mut c = setup_text_column(Aggregation::Sum, true);

        // Add a numeric string "5"
        let u: Record = vec![1.into(), DfValue::from("5")].into();
        let rs = c.narrow_one(u, true);
        assert_eq!(rs.len(), 1);
        match rs.into_iter().next().expect("expected one record") {
            Record::Positive(r) => {
                assert_eq!(r[0], 1.into());
                // "5" coerced to Double 5.0
                assert_eq!(r[1], DfValue::Double(5.0));
            }
            other => panic!("expected Positive, got {other:?}"),
        }

        // Add a non-numeric string "hello" — should be treated as 0
        let u: Record = vec![1.into(), DfValue::from("hello")].into();
        let rs = c.narrow_one(u, true);
        assert_eq!(rs.len(), 2);
        let mut rs = rs.into_iter();
        // Negative of previous value
        match rs.next().expect("expected record") {
            Record::Negative(r) => {
                assert_eq!(r[0], 1.into());
                assert_eq!(r[1], DfValue::Double(5.0));
            }
            other => panic!("expected Negative, got {other:?}"),
        }
        // Positive with same value (0 was added)
        match rs.next().expect("expected record") {
            Record::Positive(r) => {
                assert_eq!(r[0], 1.into());
                assert_eq!(r[1], DfValue::Double(5.0));
            }
            other => panic!("expected Positive, got {other:?}"),
        }

        // Add another numeric string "3"
        let u: Record = vec![1.into(), DfValue::from("3")].into();
        let rs = c.narrow_one(u, true);
        assert_eq!(rs.len(), 2);
        let mut rs = rs.into_iter();
        match rs.next().expect("expected record") {
            Record::Negative(r) => {
                assert_eq!(r[0], 1.into());
                assert_eq!(r[1], DfValue::Double(5.0));
            }
            other => panic!("expected Negative, got {other:?}"),
        }
        match rs.next().expect("expected record") {
            Record::Positive(r) => {
                assert_eq!(r[0], 1.into());
                // 5.0 + 0.0 + 3.0 = 8.0
                assert_eq!(r[1], DfValue::Double(8.0));
            }
            other => panic!("expected Positive, got {other:?}"),
        }
    }

    /// Helper to set up an aggregation over an integer column (MySQL).
    fn setup_int_column(aggregation: Aggregation, mat: bool) -> ops::test::MockGraph {
        let mut g = ops::test::MockGraph::new();
        let s = g.add_base("source", &["x", "y"]);
        g.set_op(
            "identity",
            &["x", "ys"],
            aggregation
                .over(
                    s.as_global(),
                    1,
                    &[0],
                    &DfType::Int,
                    &Dialect::DEFAULT_MYSQL,
                )
                .expect("failed to create aggregation over int column"),
            mat,
        );
        g
    }

    /// AVG over an integer column in MySQL should produce Numeric(14,4) values,
    /// matching MySQL's DECIMAL(14,4) output for AVG on integer inputs.
    #[test]
    fn avg_of_integers_mysql_decimal_precision() {
        let mut c = setup_int_column(Aggregation::Avg, true);

        // Add Group=1, Value=1
        let u: Record = vec![1.into(), 1.into()].into();
        let rs = c.narrow_one(u, true);
        assert_eq!(rs.len(), 1);
        match rs.into_iter().next().unwrap() {
            Record::Positive(r) => {
                assert_eq!(r[0], 1.into());
                // Verify it's actually a Numeric, not a Double
                assert!(
                    matches!(r[1], DfValue::Numeric(_)),
                    "AVG result should be DfValue::Numeric, got {:?}",
                    r[1]
                );
                // MySQL AVG(int) should return DECIMAL with 4 decimal places
                assert_eq!(
                    r[1].to_string(),
                    "1.0000",
                    "AVG(1) should be 1.0000, got {:?}",
                    r[1]
                );
            }
            other => panic!("expected Positive, got {other:?}"),
        }

        // Add Group=1, Value=3 — average should be 2.0000
        let u: Record = vec![1.into(), 3.into()].into();
        let rs = c.narrow_one(u, true);
        assert_eq!(rs.len(), 2);
        let mut rs = rs.into_iter();
        // Skip the Negative record
        rs.next().unwrap();
        match rs.next().unwrap() {
            Record::Positive(r) => {
                assert_eq!(r[0], 1.into());
                assert_eq!(
                    r[1].to_string(),
                    "2.0000",
                    "AVG(1,3) should be 2.0000, got {:?}",
                    r[1]
                );
            }
            other => panic!("expected Positive, got {other:?}"),
        }
    }

    /// AVG that produces a repeating decimal should be rounded to 4 decimal places
    /// (MySQL DECIMAL(14,4) for integer inputs).
    #[test]
    fn avg_of_integers_mysql_rounds_repeating_decimal() {
        let mut c = setup_int_column(Aggregation::Avg, true);

        // Insert 7 values into group 1 that sum to 249440
        // 249440 / 7 = 35634.285714... which should round to scale 4
        let values: Vec<i32> = vec![35000, 35100, 35200, 35300, 35400, 35500, 37940];
        let mut last_rs = Records::default();
        for v in values {
            let u: Record = vec![1.into(), v.into()].into();
            last_rs = c.narrow_one(u, true);
        }

        let positive = Vec::from(last_rs)
            .into_iter()
            .find_map(|r| match r {
                Record::Positive(r) => Some(r),
                _ => None,
            })
            .expect("expected Positive record");

        let expected = Decimal::from(249440_i64)
            .checked_div(&Decimal::from(7_i64))
            .unwrap()
            .round_dp(4);
        assert_eq!(positive[1], DfValue::from(expected));
    }

    #[test]
    fn it_determines_postgres_sum_output_type() {
        let grouped = Aggregation::Sum
            .over(
                Default::default(),
                0,
                &[1],
                &DfType::Int,
                &Dialect::DEFAULT_POSTGRESQL,
            )
            .unwrap();
        assert_eq!(grouped.output_col_type(), DfType::BigInt);

        let grouped = Aggregation::Sum
            .over(
                Default::default(),
                0,
                &[1],
                &DfType::Int,
                &Dialect::DEFAULT_MYSQL,
            )
            .unwrap();
        assert!(matches!(grouped.output_col_type(), DfType::Numeric { .. }));
    }

    fn setup_no_group_by(aggregation: Aggregation, mat: bool) -> ops::test::MockGraph {
        let mut g = ops::test::MockGraph::new();
        let s = g.add_base("source", &["x"]);
        g.set_op(
            "identity",
            &["xs"],
            aggregation
                .over(
                    s.as_global(),
                    0,
                    &[], // no group by
                    &DfType::BigInt,
                    &Dialect::DEFAULT_MYSQL,
                )
                .unwrap(),
            mat,
        );
        g
    }

    #[test]
    fn count_no_group_by_empty_input() {
        let mut c = setup_no_group_by(Aggregation::Count, true);
        // Empty input should still produce count=0
        let rs = c.narrow_one(Records::default(), true);
        assert_eq!(
            rs,
            vec![Record::Positive(vec![DfValue::Int(0), DfValue::Int(0)])].into()
        );
        // Idempotency: second empty input should produce nothing
        let rs2 = c.narrow_one(Records::default(), true);
        assert!(rs2.is_empty());
    }

    #[test]
    fn sum_no_group_by_empty_input() {
        let mut c = setup_no_group_by(Aggregation::Sum, true);
        // Empty input should produce sum=NULL
        let rs = c.narrow_one(Records::default(), true);
        assert_eq!(
            rs,
            vec![Record::Positive(vec![DfValue::None, DfValue::Int(0)])].into()
        );
    }

    #[test]
    fn sum_no_group_by_empty_then_real_data() {
        let mut c = setup_no_group_by(Aggregation::Sum, true);
        // Empty input → default [NULL, 0]
        let rs = c.narrow_one(Records::default(), true);
        assert_eq!(rs.len(), 1);

        // Real data arrives: rows with value 5 and 3
        let u: Records = vec![
            Record::Positive(vec![5.into()]),
            Record::Positive(vec![3.into()]),
        ]
        .into();
        let rs2 = c.narrow_one(u, true);
        // Should revoke [NULL, 0] and emit [8, 2] — NOT [NULL, 2]
        assert!(rs2
            .iter()
            .any(|r| r.is_positive() && r[0] == DfValue::from(8i64)));
    }

    #[test]
    fn sum_no_group_by_data_then_delete_all() {
        let mut c = setup_no_group_by(Aggregation::Sum, true);
        // Insert rows: 5 + 3 = 8
        let u: Records = vec![
            Record::Positive(vec![5.into()]),
            Record::Positive(vec![3.into()]),
        ]
        .into();
        let rs = c.narrow_one(u, true);
        assert!(rs
            .iter()
            .any(|r| r.is_positive() && r[0] == DfValue::from(8i64)));

        // Delete all rows
        let d: Records = vec![
            Record::Negative(vec![5.into()]),
            Record::Negative(vec![3.into()]),
        ]
        .into();
        let rs2 = c.narrow_one(d, true);
        // Should emit [NULL, 0] — NOT [0, 0]
        // SQL: SUM on empty input returns NULL, not 0
        assert!(rs2.iter().any(|r| r.is_positive() && r[0] == DfValue::None));
    }

    fn setup_pg(aggregation: Aggregation, over_col_ty: &DfType, mat: bool) -> ops::test::MockGraph {
        let mut g = ops::test::MockGraph::new();
        let s = g.add_base("source", &["x", "y"]);
        g.set_op(
            "identity",
            &["x", "ys"],
            aggregation
                .over(
                    s.as_global(),
                    1,
                    &[0],
                    over_col_ty,
                    &Dialect::DEFAULT_POSTGRESQL,
                )
                .unwrap(),
            mat,
        );
        g
    }

    /// Postgres AVG(INT) should return NUMERIC with display scale ~16 for small values.
    #[test]
    fn avg_postgres_int_scale() {
        let mut c = setup_pg(Aggregation::Avg, &DfType::Int, true);

        // Insert group=1, value=5
        let u: Record = vec![1.into(), DfValue::Int(5)].into();
        c.narrow_one(u, true);

        // Insert group=1, value=3 → AVG = 4.0
        let u: Record = vec![1.into(), DfValue::Int(3)].into();
        let rs = c.narrow_one(u, true);
        let Record::Positive(r) = rs.into_iter().find(|r| r.is_positive()).unwrap() else {
            unreachable!()
        };
        assert_eq!(r[0], 1.into());
        // sum=8 (weight 0), count=2 (weight 0), rscale = 16 - 0*4 = 16
        assert_matches!(&r[1], DfValue::Numeric(dec) => assert_eq!(dec.scale(), Some(16)));
    }

    /// Postgres AVG scale decreases with larger magnitude results.
    /// AVG(50000, 30000) = 40000 → scale 12 (not 16) because result has more integer digits.
    #[test]
    fn avg_postgres_large_values_scale() {
        let mut c = setup_pg(Aggregation::Avg, &DfType::Int, true);
        let u: Record = vec![1.into(), DfValue::Int(50000)].into();
        c.narrow_one(u, true);
        let u: Record = vec![1.into(), DfValue::Int(30000)].into();
        let rs = c.narrow_one(u, true);
        let Record::Positive(r) = rs.into_iter().find(|r| r.is_positive()).unwrap() else {
            unreachable!()
        };
        // 40000 has weight=1 in base-10000, so scale = 16 - 1*4 = 12
        assert_matches!(&r[1], DfValue::Numeric(dec) => assert_eq!(dec.scale(), Some(12)));
    }

    /// MySQL AVG output type varies by integer subtype.
    #[test]
    fn avg_mysql_per_type_precision() {
        fn check(ty: &DfType, expected_prec: u16) {
            let mut g = ops::test::MockGraph::new();
            let s = g.add_base("source", &["x", "y"]);
            let agg = Aggregation::Avg
                .over(s.as_global(), 1, &[0], ty, &Dialect::DEFAULT_MYSQL)
                .unwrap();
            assert_eq!(
                agg.output_col_type(),
                DfType::Numeric {
                    prec: expected_prec,
                    scale: 4
                },
                "MySQL AVG({:?}) should be DECIMAL({},4)",
                ty,
                expected_prec
            );
        }

        check(&DfType::TinyInt, 7);
        check(&DfType::UnsignedTinyInt, 7);
        check(&DfType::SmallInt, 9);
        check(&DfType::UnsignedSmallInt, 9);
        check(&DfType::MediumInt, 11);
        check(&DfType::UnsignedMediumInt, 12);
        check(&DfType::Int, 14);
        check(&DfType::UnsignedInt, 14);
        check(&DfType::BigInt, 23);
        check(&DfType::UnsignedBigInt, 24);
    }

    // ---- Regression tests for aggregate precision/scale bugs ----

    /// Helper for setting up MySQL aggregation tests with specific column types.
    fn setup_mysql(
        aggregation: Aggregation,
        over_col_ty: &DfType,
        mat: bool,
    ) -> ops::test::MockGraph {
        let mut g = ops::test::MockGraph::new();
        let s = g.add_base("source", &["x", "y"]);
        g.set_op(
            "identity",
            &["x", "ys"],
            aggregation
                .over(s.as_global(), 1, &[0], over_col_ty, &Dialect::DEFAULT_MYSQL)
                .unwrap(),
            mat,
        );
        g
    }

    /// REA-6199: Postgres AVG on INT should return Numeric with dynamic scale
    /// (~16 significant digits for small values), not scale 0.
    ///
    /// Bug: Postgres AVG used DEFAULT_NUMERIC (scale=0) for all non-float types,
    /// causing results like `33437` instead of `33437.314617500000`.
    #[test]
    fn regression_rea_6199_postgres_avg_int_scale() {
        let mut c = setup_pg(Aggregation::Avg, &DfType::Int, true);

        // Insert group=1, value=5
        let u: Record = vec![1.into(), DfValue::Int(5)].into();
        c.narrow_one(u, true);

        // Insert group=1, value=3 → AVG = 4.0
        let u: Record = vec![1.into(), DfValue::Int(3)].into();
        let rs = c.narrow_one(u, true);
        let Record::Positive(r) = rs.into_iter().find(|r| r.is_positive()).unwrap() else {
            unreachable!()
        };
        // sum=8 (weight 0), count=2 (weight 0), rscale = 16 - 0*4 = 16
        assert_matches!(&r[1], DfValue::Numeric(dec) => assert_eq!(dec.scale(), Some(16)));
    }

    /// REA-6199: Postgres AVG on NUMERIC(10,6) should return result with appropriate
    /// display scale, not 0 or excessive digits.
    #[test]
    fn regression_rea_6199_postgres_avg_numeric_scale() {
        let mut c = setup_pg(
            Aggregation::Avg,
            &DfType::Numeric { prec: 10, scale: 6 },
            true,
        );

        // Insert group=1, value=33437.314617
        let v1 = DfValue::from(readyset_decimal::Decimal::new(33437314617, 6));
        let u: Record = vec![1.into(), v1].into();
        c.narrow_one(u, true);

        // Insert group=1, value=33437.314618 → AVG ≈ 33437.3146175
        let v2 = DfValue::from(readyset_decimal::Decimal::new(33437314618, 6));
        let u: Record = vec![1.into(), v2].into();
        let rs = c.narrow_one(u, true);
        let Record::Positive(r) = rs.into_iter().find(|r| r.is_positive()).unwrap() else {
            unreachable!()
        };
        // postgres_select_div_scale for sum≈66874.6 (weight 1, first 6),
        // count=2 (weight 0, first 2) yields rscale = 16 - 1*4 = 12.
        assert_matches!(&r[1], DfValue::Numeric(dec) => assert_eq!(dec.scale(), Some(12)));
    }

    /// REA-6118: MySQL AVG on INT should return DECIMAL with scale 4, not Double.
    ///
    /// Bug: MySQL AVG(INT) returned a Double-like value (e.g., 35634.28571428572)
    /// instead of DECIMAL(14,4) (e.g., 35634.2857).
    #[test]
    fn regression_rea_6118_mysql_avg_int_returns_decimal_not_double() {
        let mut c = setup_mysql(Aggregation::Avg, &DfType::Int, true);

        // Insert values that produce a non-integer average, mimicking the
        // Antithesis scenario from REA-6118
        let u: Record = vec![1.into(), DfValue::Int(71268)].into();
        c.narrow_one(u, true);
        let u: Record = vec![1.into(), DfValue::Int(1)].into();
        let rs = c.narrow_one(u, true);
        let Record::Positive(r) = rs.into_iter().find(|r| r.is_positive()).unwrap() else {
            unreachable!()
        };
        assert_matches!(&r[1], DfValue::Numeric(dec) => assert_eq!(dec.scale(), Some(4)));
    }

    /// REA-5423: MySQL AVG on DECIMAL(7,2) should return DECIMAL with scale 6
    /// (input_scale 2 + div_precision_increment 4 = 6).
    ///
    /// Bug: AVG on DECIMAL returned too many digits (like 3.1617615204639344
    /// instead of 3.161762), suggesting wrong scale or Double output.
    #[test]
    fn regression_rea_5423_mysql_avg_decimal_scale() {
        let mut c = setup_mysql(
            Aggregation::Avg,
            &DfType::Numeric { prec: 7, scale: 2 },
            true,
        );

        // Insert group=1, value=3.16
        let v1 = DfValue::from(readyset_decimal::Decimal::new(316, 2));
        let u: Record = vec![1.into(), v1].into();
        c.narrow_one(u, true);

        // Insert group=1, value=3.17 → AVG = 3.165
        let v2 = DfValue::from(readyset_decimal::Decimal::new(317, 2));
        let u: Record = vec![1.into(), v2].into();
        let rs = c.narrow_one(u, true);
        let Record::Positive(r) = rs.into_iter().find(|r| r.is_positive()).unwrap() else {
            unreachable!()
        };
        assert_matches!(&r[1], DfValue::Numeric(dec) => assert_eq!(dec.scale(), Some(6)));
    }

    /// REA-5423: MySQL SUM on DECIMAL(10,2) should preserve scale 2, not use
    /// DEFAULT_NUMERIC (scale 0).
    #[test]
    fn regression_rea_5423_mysql_sum_decimal_preserves_scale() {
        let sum_op = Aggregation::Sum
            .over(
                0.into(),
                1,
                &[0],
                &DfType::Numeric { prec: 10, scale: 2 },
                &Dialect::DEFAULT_MYSQL,
            )
            .unwrap();

        assert_eq!(
            sum_op.output_col_type(),
            DfType::Numeric { prec: 32, scale: 2 },
        );
    }

    /// REA-6199: Postgres SUM on NUMERIC(10,2) should preserve scale 2, not use
    /// DEFAULT_NUMERIC (scale 0).
    #[test]
    fn regression_rea_6199_postgres_sum_numeric_preserves_scale() {
        let sum_op = Aggregation::Sum
            .over(
                0.into(),
                1,
                &[0],
                &DfType::Numeric { prec: 10, scale: 2 },
                &Dialect::DEFAULT_POSTGRESQL,
            )
            .unwrap();

        assert_eq!(
            sum_op.output_col_type(),
            DfType::Numeric { prec: 10, scale: 2 },
        );
    }

    /// REA-6118: MySQL AVG on INT should produce output type DECIMAL(14,4),
    /// matching MySQL's convention for AVG on regular INT.
    #[test]
    fn regression_rea_6118_mysql_avg_int_output_type() {
        let avg_op = Aggregation::Avg
            .over(0.into(), 1, &[0], &DfType::Int, &Dialect::DEFAULT_MYSQL)
            .unwrap();

        assert_eq!(
            avg_op.output_col_type(),
            DfType::Numeric { prec: 14, scale: 4 },
        );
    }

    /// REA-6118: MySQL AVG on BIGINT should produce DECIMAL(23,4), not DECIMAL(14,4).
    #[test]
    fn regression_rea_6118_mysql_avg_bigint_output_type() {
        let avg_op = Aggregation::Avg
            .over(0.into(), 1, &[0], &DfType::BigInt, &Dialect::DEFAULT_MYSQL)
            .unwrap();

        assert_eq!(
            avg_op.output_col_type(),
            DfType::Numeric { prec: 23, scale: 4 },
        );
    }

    /// Helper: assert that a single NULL input to the given aggregation
    /// produces a NULL output (not zero).
    fn assert_all_null_returns_null(agg: Aggregation) {
        let mut c = setup(agg, true);

        let u: Record = vec![1.into(), DfValue::None].into();
        let rs = c.narrow_one(u, true);
        assert_eq!(rs.len(), 1);

        match rs.into_iter().next().unwrap() {
            Record::Positive(r) => {
                assert_eq!(r[0], 1.into());
                assert_eq!(r[1], DfValue::None);
            }
            other => panic!("expected Record::Positive, got {other:?}"),
        }
    }

    #[test]
    fn sum_all_null_returns_null() {
        assert_all_null_returns_null(Aggregation::Sum);
    }

    #[test]
    fn avg_all_null_returns_null() {
        assert_all_null_returns_null(Aggregation::Avg);
    }

    /// Helper: assert that a NULL followed by non-NULL values produces the
    /// expected aggregate (only non-NULL values contribute).
    fn assert_mix_null_nonnull(agg: Aggregation, values: &[f64], expected: DfValue) {
        let mut c = setup(agg, true);

        // Add Group=1, Value=NULL
        let u: Record = vec![1.into(), DfValue::None].into();
        c.narrow_one(u, true);

        // Add non-NULL values; keep the last result set
        let mut last_rs = Records::default();
        for &v in values {
            let u: Record = vec![1.into(), DfValue::Double(v)].into();
            last_rs = c.narrow_one(u, true);
        }
        let positive = last_rs.into_iter().find(|r| r.is_positive()).unwrap();
        match positive {
            Record::Positive(r) => {
                assert_eq!(r[0], 1.into());
                assert_eq!(r[1], expected);
            }
            other => panic!("expected Record::Positive, got {other:?}"),
        }
    }

    #[test]
    fn sum_mix_null_nonnull() {
        assert_mix_null_nonnull(Aggregation::Sum, &[5.0], DfValue::Double(5.0));
    }

    #[test]
    fn avg_mix_null_nonnull() {
        // AVG of 4.0, 6.0 = 5.0 (NULL excluded)
        assert_mix_null_nonnull(Aggregation::Avg, &[4.0, 6.0], DfValue::Double(5.0));
    }

    /// SUM with multiple NULLs should still return NULL.
    #[test]
    fn sum_multiple_nulls_returns_null() {
        let mut c = setup(Aggregation::Sum, true);

        let u: Record = vec![1.into(), DfValue::None].into();
        c.narrow_one(u, true);
        let u: Record = vec![1.into(), DfValue::None].into();
        let rs = c.narrow_one(u, true);

        // Should still be NULL — adding more NULL rows doesn't make it 0
        let positive = rs.into_iter().find(|r| r.is_positive()).unwrap();
        match positive {
            Record::Positive(r) => {
                assert_eq!(r[0], 1.into());
                assert_eq!(r[1], DfValue::None);
            }
            other => panic!("expected Record::Positive, got {other:?}"),
        }
    }

    /// SUM negative from NULL state (defensive path for replay/recovery).
    /// When a group has no prior state and a negative diff arrives, the sum
    /// should start from zero and subtract.
    #[test]
    fn sum_negative_from_null_state() {
        let mut c = setup(Aggregation::Sum, true);

        // Insert value=5 for group 1, then remove it.
        let u: Record = vec![1.into(), DfValue::Double(5.0)].into();
        c.narrow_one(u, true);
        let u = (vec![1.into(), DfValue::Double(5.0)], false);
        let rs = c.narrow_one_row(u, true);

        // After removing the only row, group should report NULL (via
        // empty_value), not 0.
        let positive = rs.into_iter().find(|r| r.is_positive());
        // When the group drops to 0 rows and emit_empty is false, no
        // positive record is emitted — the group simply disappears.
        assert!(
            positive.is_none(),
            "group with 0 rows should not emit a positive record"
        );
    }
}
