//! This module implements SQL aggregation functions that accumulate multiple input values
//! into a single output value. Currently only supports mysql's `GROUP_CONCAT`, postgres' `STRING_AGG`
//! and `ARRAY_AGG`, and the various JSON object aggregation functions.
//!
//! Both mysql and postgres generally allow the argument to the function to be a free-wheeling `expr`,
//! but we currently limit this to being a column.
//!
//! Some of the supported functions allow elements to be distinct and/or ordered within their result.
//! For example, `select group_concat(distinct col1 order by col1 desc nulls last separator "::")`.
//! Currently, the `ORDER BY` expr must either be a positional indicator and must be a value of `1`,
//! or must match the column name of the function's expr (which currently must be a column).

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt::{Display, Formatter, Result};

use crate::eval::json;
use readyset_data::DfValue;
use readyset_errors::{internal, internal_err, unsupported, ReadySetError, ReadySetResult};
use readyset_sql::ast::{
    Column, DistinctOption, Expr, FieldReference, FunctionExpr, NullOrder, OrderClause, OrderType,
};
use serde::{Deserialize, Serialize};

/// Supported accumulation operators.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AccumulationOp {
    /// Concatenate values into an array. Allows NULL values in the output array.
    ArrayAgg {
        distinct: DistinctOption,
        order_by: Option<(OrderType, NullOrder)>,
    },
    /// Concatenates using the given separator between values. The string result is with the concatenated non-NULL
    /// values from a group. It returns NULL if there are no non-NULL values.
    GroupConcat {
        separator: String,
        distinct: DistinctOption,
        order_by: Option<(OrderType, NullOrder)>,
    },
    /// Aggregates key-value pairs into JSON object.
    JsonObjectAgg { allow_duplicate_keys: bool },
    /// Concatenates using the given separator between values. Postgres allows the separator
    /// to be `NULL` (yet it must be specified in the query string). The string result is the
    /// concatenated non-NULL values from a group. It returns NULL if there are no non-NULL values.
    StringAgg {
        separator: Option<String>,
        distinct: DistinctOption,
        order_by: Option<(OrderType, NullOrder)>,
    },
}

impl AccumulationOp {
    pub fn ignore_nulls(&self) -> bool {
        match self {
            Self::ArrayAgg { .. } => false,
            Self::GroupConcat { .. } | Self::JsonObjectAgg { .. } | Self::StringAgg { .. } => true,
        }
    }

    fn is_distinct(&self) -> bool {
        match self {
            Self::ArrayAgg { distinct, .. }
            | Self::GroupConcat { distinct, .. }
            | Self::StringAgg { distinct, .. } => *distinct == DistinctOption::IsDistinct,
            // uniqueness is handled is slightly differently for the JSON aggregators
            Self::JsonObjectAgg { .. } => false,
        }
    }

    fn order_by(&self) -> Option<(OrderType, NullOrder)> {
        match self {
            AccumulationOp::ArrayAgg { order_by, .. }
            | AccumulationOp::GroupConcat { order_by, .. }
            | AccumulationOp::StringAgg { order_by, .. } => *order_by,
            AccumulationOp::JsonObjectAgg { .. } => None,
        }
    }

    fn apply_array_agg(&self, data: &AccumulatorData) -> ReadySetResult<DfValue> {
        if data.is_empty() {
            return Ok(DfValue::Array(std::sync::Arc::new(
                readyset_data::Array::from(vec![]),
            )));
        }

        match data {
            AccumulatorData::Simple(v) => Ok(DfValue::Array(std::sync::Arc::new(
                readyset_data::Array::from(v.to_vec()),
            ))),
            AccumulatorData::DistinctOrdered(t) => {
                let vals: Vec<_> = t
                    .iter()
                    .flat_map(|(k, &count)| {
                        let repeat_count = if self.is_distinct() { 1 } else { count };
                        std::iter::repeat_n(k.value.clone(), repeat_count)
                    })
                    .collect();

                Ok(DfValue::Array(std::sync::Arc::new(
                    readyset_data::Array::from(vals),
                )))
            }
        }
    }

    fn apply_group_concat(
        &self,
        data: &AccumulatorData,
        separator: &str,
    ) -> ReadySetResult<DfValue> {
        // return SQL NULL if no non-NULL values in the `data`. we won't have NULL values as we've
        // filtered those out already.
        if data.is_empty() {
            return Ok(DfValue::None);
        }

        let strings: Vec<_> = match data {
            AccumulatorData::Simple(v) => {
                v.iter().map(|piece| piece.to_string()).collect::<Vec<_>>()
            }
            AccumulatorData::DistinctOrdered(t) => t
                .iter()
                .flat_map(|(k, &count)| {
                    let repeat_count = if self.is_distinct() { 1 } else { count };
                    std::iter::repeat_n(k.to_string().clone(), repeat_count)
                })
                .collect::<Vec<_>>(),
        };

        let out_str = strings.join(separator);
        Ok(out_str.into())
    }

    fn apply_json_object_agg(
        &self,
        data: &AccumulatorData,
        allow_duplicate_keys: bool,
    ) -> ReadySetResult<DfValue> {
        let data = match data {
            AccumulatorData::Simple(v) => v,
            AccumulatorData::DistinctOrdered(_) => {
                internal!("Unsupported AccumulatorData type for json_object_agg")
            }
        };

        let mut json_keys = Vec::new();
        let mut json_vals = Vec::new();

        for value in data {
            let (key, val) = value
                .to_json()
                .map_err(|_| internal_err!("json_object_agg: failed to parse json"))?
                .as_object()
                .ok_or_else(|| {
                    internal_err!("json_object_agg: json_object value is not an object")
                })?
                .iter()
                .next()
                .ok_or_else(|| internal_err!("json_object_agg: json_object is empty"))
                .map(|(k, v)| (DfValue::from(k.as_str()), DfValue::from(v)))?;

            json_keys.push(key);
            json_vals.push(val);
        }

        json::json_object_from_keys_and_values(
            &json_keys.into(),
            &json_vals.into(),
            allow_duplicate_keys,
        )
    }

    pub fn apply(&self, data: &AccumulatorData) -> ReadySetResult<DfValue> {
        match self {
            AccumulationOp::ArrayAgg { .. } => self.apply_array_agg(data),
            AccumulationOp::GroupConcat { separator, .. } => {
                self.apply_group_concat(data, separator)
            }
            AccumulationOp::JsonObjectAgg {
                allow_duplicate_keys,
            } => self.apply_json_object_agg(data, *allow_duplicate_keys),
            AccumulationOp::StringAgg { separator, .. } => {
                let sep = separator.clone().unwrap_or("".to_string());
                self.apply_group_concat(data, &sep)
            }
        }
    }

    // Split an assumedly already accumulated `DfValue` into consitutent parts.
    // The intended use for this is in post-lookup aggregations where we need
    // to apply `DISTINCT` and `ORDER BY` operations across mutliple rows that
    // are to be grouped together.
    fn split(&self, value: &DfValue) -> ReadySetResult<Vec<DfValue>> {
        if value.is_none() {
            return Ok(vec![]);
        }

        let res = match self {
            AccumulationOp::ArrayAgg { .. } => match value {
                DfValue::Array(arr) => arr.values().cloned().collect(),
                _ => vec![value.clone()],
            },
            AccumulationOp::GroupConcat { separator, .. } => match value.as_str() {
                Some(t) => t.split(separator).map(DfValue::from).collect(),
                None => internal!("Must be a text type: {:?}", value),
            },
            AccumulationOp::JsonObjectAgg { .. } => {
                unsupported!("Post-lookup json_object_agg not supoorted yet")
            }
            AccumulationOp::StringAgg { separator, .. } => {
                let sep = match separator {
                    Some(s) => s,
                    None => unsupported!("Separator cannot be None when splitting a prior value"),
                };

                match value.as_str() {
                    Some(t) => t.split(sep).map(DfValue::from).collect(),
                    None => internal!("Must be a text type: {:?}", value),
                }
            }
        };
        Ok(res)
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum AccumulatorData {
    /// Simple accumulation - maintains insertion order, allows duplicates
    Simple(Vec<DfValue>),

    /// Distinct accumulation - automatically ordered, tracks counts for proper deletions.
    /// Even without explicit order by clauses to the various functions, both mysql and postgres
    /// impose a default (alphbetical) ordering.
    ///
    /// Note: the current implementation will order keys by their natural order (`DfValue`).
    /// This has the effect of sorting SQL NULLs (`DfValue::None`) to the beginning of the orderings;
    /// for most the functions implemented here, the upstream databases default NULLs to the end of the orderings.
    /// If the user didn't specify an ordering via an `order by` clause, they get what they get and
    /// this "NULLs ordered first" is not a bug :shrug:). Further note: the `group_concat()` and `string_agg()`
    /// functions don't output NULLs, so really this only affects `array_agg()` which does output NULLs.
    DistinctOrdered(BTreeMap<OrderableDfValue, usize>),
}

/// A wrapper for DfValue + order_by information.
///
/// Wanted this to be a newtype wrapper, but life is hard.
/// The comparison functions consider the order_by then the DfValue;
/// `Eq` simply defers to the DfValue.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OrderableDfValue {
    value: DfValue,
    order_by: Option<(OrderType, NullOrder)>,
}

impl Ord for OrderableDfValue {
    fn cmp(&self, other: &Self) -> Ordering {
        if let Some((order_type, null_order)) = self.order_by {
            null_order
                .apply(self.value.is_none(), other.value.is_none())
                .then(order_type.apply(self.value.cmp(&other.value)))
        } else {
            self.value.cmp(&other.value)
        }
    }
}

impl PartialOrd for OrderableDfValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for OrderableDfValue {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl Eq for OrderableDfValue {}

impl Display for OrderableDfValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "{}", self.value)?;
        Ok(())
    }
}

impl AccumulatorData {
    pub fn add_accummulated(&mut self, op: &AccumulationOp, value: DfValue) -> ReadySetResult<()> {
        op.split(&value)?.into_iter().for_each(|v| self.add(op, v));
        Ok(())
    }

    pub fn add(&mut self, op: &AccumulationOp, value: DfValue) {
        match self {
            AccumulatorData::Simple(v) => {
                // check to make sure we've got the correct data structure
                assert!(!op.is_distinct());
                v.push(value);
            }
            AccumulatorData::DistinctOrdered(v) => {
                let order_by = op.order_by();
                let key = OrderableDfValue { value, order_by };
                v.entry(key).and_modify(|cnt| *cnt += 1).or_insert(1);
            }
        }
    }

    pub fn remove(&mut self, op: &AccumulationOp, value: DfValue) -> ReadySetResult<()> {
        match self {
            AccumulatorData::Simple(v) => {
                let item_pos = v
                    .iter()
                    .rposition(|x| x == &value)
                    .ok_or_else(|| internal_err!("accumulator couldn't remove value from data"))?;
                v.remove(item_pos);
            }
            AccumulatorData::DistinctOrdered(v) => {
                let key = OrderableDfValue {
                    value,
                    order_by: op.order_by(),
                };
                match v.get_mut(&key) {
                    Some(cnt) => {
                        if *cnt == 1 {
                            v.remove(&key);
                        } else {
                            *cnt -= 1;
                        }
                    }
                    None => {
                        return Err(internal_err!("accumulator couldn't remove value from data"));
                    }
                };
            }
        }

        Ok(())
    }

    fn is_empty(&self) -> bool {
        match self {
            AccumulatorData::Simple(v) => v.is_empty(),
            AccumulatorData::DistinctOrdered(v) => v.is_empty(),
        }
    }
}

impl From<&AccumulationOp> for AccumulatorData {
    fn from(value: &AccumulationOp) -> Self {
        use AccumulationOp::*;

        match value {
            // uniqueness is handled is slightly differently for the JSON aggregators
            JsonObjectAgg { .. } => AccumulatorData::Simple(Default::default()),
            ArrayAgg { distinct, order_by }
            | GroupConcat {
                distinct, order_by, ..
            }
            | StringAgg {
                distinct, order_by, ..
            } => {
                if *distinct == DistinctOption::IsDistinct || order_by.is_some() {
                    AccumulatorData::DistinctOrdered(Default::default())
                } else {
                    AccumulatorData::Simple(Default::default())
                }
            }
        }
    }
}

fn validate_accumulator_order_by(
    over_col: &Column,
    order_by: &Option<OrderClause>,
) -> ReadySetResult<Option<(OrderType, NullOrder)>> {
    match order_by {
        Some(o) if o.order_by.is_empty() => Ok(None),
        Some(o) => {
            if o.order_by.len() > 1 {
                unsupported!("Multiple ORDER BY expressions not supported")
            }

            let order_by_expr = &o.order_by[0];

            // Ensure the ORDER BY expr is either a position indicator (which must be 1)
            // or match the column name in the function.
            match &order_by_expr.field {
                FieldReference::Numeric(ref i) => {
                    if *i != 1 {
                        unsupported!("ORDER BY position indicator must be '1'")
                    }
                }
                FieldReference::Expr(e) => {
                    let order_by_col = match e {
                        Expr::Column(c) => c,
                        _ => unsupported!("ORDER BY expr must be a column"),
                    };

                    if over_col != order_by_col {
                        unsupported!("ORDER BY column must equal the function's column: {:?}, order_by expr: {:?}", over_col, order_by_col)
                    }
                }
            };
            let order_type = order_by_expr.order_type.unwrap_or_default();
            Ok(Some((order_type, order_by_expr.null_order)))
        }
        None => Ok(None),
    }
}

impl TryFrom<&FunctionExpr> for AccumulationOp {
    type Error = ReadySetError;

    fn try_from(fn_expr: &FunctionExpr) -> ReadySetResult<AccumulationOp> {
        // Enforce only column references, not any random expr
        let over_col = |expr: &Expr| -> ReadySetResult<Column> {
            match *expr {
                Expr::Column(ref c) => Ok(c.clone()),
                _ => unsupported!("expr must be a column"),
            }
        };

        let op = match fn_expr {
            FunctionExpr::ArrayAgg {
                expr,
                distinct,
                order_by,
            } => AccumulationOp::ArrayAgg {
                distinct: *distinct,
                order_by: validate_accumulator_order_by(&over_col(expr)?, order_by)?,
            },
            FunctionExpr::GroupConcat {
                expr,
                separator,
                distinct,
                order_by,
            } => AccumulationOp::GroupConcat {
                separator: separator.clone().unwrap_or_else(|| ",".to_owned()),
                distinct: *distinct,
                order_by: validate_accumulator_order_by(&over_col(expr)?, order_by)?,
            },
            FunctionExpr::JsonObjectAgg {
                allow_duplicate_keys,
                ..
            } => AccumulationOp::JsonObjectAgg {
                allow_duplicate_keys: *allow_duplicate_keys,
            },
            FunctionExpr::StringAgg {
                expr,
                separator,
                distinct,
                order_by,
            } => AccumulationOp::StringAgg {
                separator: separator.clone(),
                distinct: *distinct,
                order_by: validate_accumulator_order_by(&over_col(expr)?, order_by)?,
            },
            _ => internal!("Unsupported accumulation for function expr: {:?}", fn_expr),
        };
        Ok(op)
    }
}
