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
use readyset_data::{Collation, DfValue};
use readyset_errors::{internal, internal_err, unsupported, ReadySetError, ReadySetResult};
use readyset_sql::analysis::is_aggregate;
use readyset_sql::analysis::visit::{self, Visitor};
use readyset_sql::ast::{
    DistinctOption, Expr, FieldReference, FunctionExpr, NullOrder, OrderClause, OrderType,
};
use serde::{Deserialize, Serialize};
use strum::IntoStaticStr;

/// Supported accumulation operators.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, IntoStaticStr)]
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

    pub fn is_distinct(&self) -> bool {
        match self {
            Self::ArrayAgg { distinct, .. }
            | Self::GroupConcat { distinct, .. }
            | Self::StringAgg { distinct, .. } => *distinct == DistinctOption::IsDistinct,
            // uniqueness is handled is slightly differently for the JSON aggregators
            Self::JsonObjectAgg { .. } => false,
        }
    }

    pub fn order_by(&self) -> Option<(OrderType, NullOrder)> {
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

        let vals: Vec<DfValue> = match data {
            AccumulatorData::Simple(v) => v.to_vec(),
            AccumulatorData::DistinctOrdered(t) => t
                .iter()
                .flat_map(|(k, &count)| {
                    let repeat_count = if self.is_distinct() { 1 } else { count };
                    std::iter::repeat_n(k.value.clone(), repeat_count)
                })
                .collect(),
        };

        // When aggregating array-typed columns, stack sub-arrays into a multidimensional
        // array rather than creating a 1D array of array elements. PostgreSQL's array_agg()
        // on an int[] column produces int[][], not an array-of-arrays.
        let arr = if vals.first().is_some_and(|v| matches!(v, DfValue::Array(_))) {
            let sub_arrays: ReadySetResult<Vec<_>> = vals
                .into_iter()
                .map(|v| match v {
                    DfValue::Array(a) => Ok(a),
                    other => Err(readyset_errors::invalid_query_err!(
                        "Mixed array and non-array elements in array_agg: expected Array, got {:?}",
                        other.infer_dataflow_type()
                    )),
                })
                .collect();
            readyset_data::Array::from_sub_arrays(&sub_arrays?)?
        } else {
            readyset_data::Array::from(vals)
        };

        Ok(DfValue::Array(std::sync::Arc::new(arr)))
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

    /// Emit raw accumulated values as a `DfValue::Array` without finalization.
    /// This is used when skip_finalization is set, so post-lookup can work
    /// with the raw constituent values instead of the finalized string.
    ///
    /// Text values are normalized to `Collation::Utf8` so that post-lookup's
    /// `DistinctOrdered` BTreeMap compares them case-sensitively, matching the
    /// behavior of the old `split()` path (which created fresh `DfValue::from(&str)`
    /// values with `Collation::Utf8`). Without this, MySQL's default `Utf8AiCi`
    /// collation causes the BTreeMap to merge values that differ only in case
    /// (e.g. `"aaa"` and `"AAA"`).
    pub fn emit_raw(&self, data: &AccumulatorData) -> DfValue {
        if data.is_empty() {
            return DfValue::Array(std::sync::Arc::new(readyset_data::Array::from(vec![])));
        }

        let values: Vec<DfValue> = match data {
            AccumulatorData::Simple(v) => v.iter().map(Self::normalize_collation).collect(),
            AccumulatorData::DistinctOrdered(t) => t
                .iter()
                .flat_map(|(k, &count)| {
                    let repeat_count = if self.is_distinct() { 1 } else { count };
                    let normalized = Self::normalize_collation(&k.value);
                    std::iter::repeat_n(normalized, repeat_count)
                })
                .collect(),
        };

        DfValue::Array(std::sync::Arc::new(readyset_data::Array::from(values)))
    }

    /// Normalize a DfValue's collation to `Utf8` (case-sensitive) for raw emission.
    /// Non-text values are cloned as-is.
    fn normalize_collation(v: &DfValue) -> DfValue {
        match v.collation() {
            Some(Collation::Utf8) | None => v.clone(),
            Some(_) => {
                // Re-create the text value with Utf8 collation
                let s = <&str>::try_from(v).expect("text DfValue must convert to str");
                DfValue::from_str_and_collation(s, Collation::Utf8)
            }
        }
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
                DfValue::Array(arr) if arr.num_dimensions() > 1 => {
                    // Reconstruct DfValue::Array sub-arrays from the outer dimension
                    // so that re-aggregation via apply_array_agg → from_sub_arrays
                    // preserves dimensionality.
                    std::sync::Arc::unwrap_or_clone(arr.clone())
                        .into_sub_arrays()
                        .into_iter()
                        .map(|sub| DfValue::Array(std::sync::Arc::new(sub)))
                        .collect()
                }
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

    /// Add raw values from a `DfValue::Array` directly, bypassing split().
    /// Uses `Vec::extend` for the `Simple` variant for fewer capacity checks.
    pub fn add_raw(&mut self, op: &AccumulationOp, value: &DfValue) -> ReadySetResult<()> {
        match value {
            DfValue::Array(arr) => {
                match self {
                    AccumulatorData::Simple(v) => {
                        v.extend(arr.values().cloned());
                    }
                    AccumulatorData::DistinctOrdered(map) => {
                        let order_by = op.order_by();
                        for val in arr.values().cloned() {
                            let key = OrderableDfValue {
                                value: val,
                                order_by,
                            };
                            map.entry(key).and_modify(|cnt| *cnt += 1).or_insert(1);
                        }
                    }
                }
                Ok(())
            }
            DfValue::None => Ok(()),
            other => Err(internal_err!(
                "add_raw: expected DfValue::Array, got {:?}",
                other
            )),
        }
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

/// Visitor that validates an aggregation expression doesn't contain aggregate expressions
/// or window function calls.
///
/// As per PostgreSQL documentation [0]:
/// "expression is any value expression that does not itself contain an aggregate expression
/// or a window function call."
///
/// [0] https://www.postgresql.org/docs/current/sql-expressions.html#SYNTAX-AGGREGATES
struct AggregateValidator;

impl<'ast> Visitor<'ast> for AggregateValidator {
    type Error = ReadySetError;

    fn visit_expr(&mut self, expr: &'ast Expr) -> ReadySetResult<()> {
        match expr {
            // Window functions are not allowed
            Expr::WindowFunction { .. } => {
                // this is the error message from postgres
                unsupported!("aggregate function calls cannot contain window function calls")
            }
            // Scalar subqueries in aggregates are not yet supported in ReadySet.
            Expr::NestedSelect(_) | Expr::Exists(_) => {
                unsupported!("Subqueries in aggregate function arguments are not yet supported")
            }
            // For all other expressions, continue walking the tree
            _ => visit::walk_expr(self, expr),
        }
    }

    fn visit_function_expr(&mut self, func: &'ast FunctionExpr) -> ReadySetResult<()> {
        if is_aggregate(func) {
            // this is the error message from postgres
            unsupported!("aggregate function calls cannot be nested")
        }
        visit::walk_function_expr(self, func)
    }
}

/// Validates that an expression doesn't contain nested aggregate expressions or window function calls.
fn validate_no_nested_aggregates(expr: &Expr) -> ReadySetResult<()> {
    AggregateValidator.visit_expr(expr)
}

fn validate_accumulator_order_by(
    expr: &Expr,
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
                    // Enforce only column references, not any random expr (yet)
                    let over_col = match expr {
                        Expr::Column(ref c) => c,
                        _ => unsupported!("expr must be a column"),
                    };
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
        let op = match fn_expr {
            FunctionExpr::ArrayAgg {
                expr,
                distinct,
                order_by,
            } => {
                validate_no_nested_aggregates(expr)?;
                AccumulationOp::ArrayAgg {
                    distinct: *distinct,
                    order_by: validate_accumulator_order_by(expr, order_by)?,
                }
            }
            FunctionExpr::GroupConcat {
                expr,
                separator,
                distinct,
                order_by,
            } => {
                validate_no_nested_aggregates(expr)?;
                AccumulationOp::GroupConcat {
                    separator: separator.clone().unwrap_or_else(|| ",".to_owned()),
                    distinct: *distinct,
                    order_by: validate_accumulator_order_by(expr, order_by)?,
                }
            }
            FunctionExpr::JsonObjectAgg {
                key,
                value,
                allow_duplicate_keys,
            } => {
                validate_no_nested_aggregates(key)?;
                validate_no_nested_aggregates(value)?;
                AccumulationOp::JsonObjectAgg {
                    allow_duplicate_keys: *allow_duplicate_keys,
                }
            }
            FunctionExpr::StringAgg {
                expr,
                separator,
                distinct,
                order_by,
            } => {
                validate_no_nested_aggregates(expr)?;
                AccumulationOp::StringAgg {
                    separator: separator.clone(),
                    distinct: *distinct,
                    order_by: validate_accumulator_order_by(expr, order_by)?,
                }
            }
            _ => internal!("Unsupported accumulation for function expr: {:?}", fn_expr),
        };
        Ok(op)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use readyset_sql::ast::{Column, LimitClause, OrderBy, SelectStatement, SqlIdentifier};

    fn make_column(name: &str) -> Column {
        Column {
            name: SqlIdentifier::from(name),
            table: None,
        }
    }

    fn make_column_expr(name: &str) -> Expr {
        Expr::Column(make_column(name))
    }

    fn make_order_clause(fields: Vec<FieldReference>, order_type: OrderType) -> OrderClause {
        OrderClause {
            order_by: fields
                .into_iter()
                .map(|field| OrderBy {
                    field,
                    order_type: Some(order_type),
                    null_order: NullOrder::NullsFirst,
                })
                .collect(),
        }
    }

    mod validate_accumulator_order_by_tests {
        use super::*;

        #[test]
        fn test_multiple_order_by_expressions() {
            let expr = make_column_expr("col1");
            let order_clause = Some(make_order_clause(
                vec![
                    FieldReference::Expr(make_column_expr("col1")),
                    FieldReference::Expr(make_column_expr("col2")),
                ],
                OrderType::OrderAscending,
            ));

            let result = validate_accumulator_order_by(&expr, &order_clause);
            assert!(result.is_err());
        }

        #[test]
        fn test_numeric_position_not_one() {
            let expr = make_column_expr("col1");
            let order_clause = Some(make_order_clause(
                vec![FieldReference::Numeric(2)],
                OrderType::OrderAscending,
            ));

            let result = validate_accumulator_order_by(&expr, &order_clause);
            assert!(result.is_err());
        }

        #[test]
        fn test_expr_not_a_column() {
            // Create a literal expression instead of a column
            let expr = Expr::Literal(readyset_sql::ast::Literal::Integer(42));
            let order_clause = Some(make_order_clause(
                vec![FieldReference::Expr(make_column_expr("col1"))],
                OrderType::OrderAscending,
            ));

            let result = validate_accumulator_order_by(&expr, &order_clause);
            assert!(result.is_err());
        }

        #[test]
        fn test_order_by_expr_not_a_column() {
            let expr = make_column_expr("col1");
            // Use a literal in ORDER BY instead of a column
            let order_clause = Some(make_order_clause(
                vec![FieldReference::Expr(Expr::Literal(
                    readyset_sql::ast::Literal::Integer(1),
                ))],
                OrderType::OrderAscending,
            ));

            let result = validate_accumulator_order_by(&expr, &order_clause);
            assert!(result.is_err());
        }

        #[test]
        fn test_columns_dont_match() {
            let expr = make_column_expr("col1");
            let order_clause = Some(make_order_clause(
                vec![FieldReference::Expr(make_column_expr("col2"))],
                OrderType::OrderAscending,
            ));

            let result = validate_accumulator_order_by(&expr, &order_clause);
            assert!(result.is_err());
        }
    }

    mod validate_no_nested_aggregates_tests {
        use super::*;

        #[test]
        fn test_window_function_rejected() {
            let expr = Expr::WindowFunction {
                function: FunctionExpr::Call {
                    name: SqlIdentifier::from("row_number"),
                    arguments: Some(vec![]),
                },
                partition_by: vec![],
                order_by: vec![],
            };

            let result = validate_no_nested_aggregates(&expr);
            assert!(result.is_err());
        }

        #[test]
        fn test_nested_select_rejected() {
            let expr = Expr::NestedSelect(Box::new(SelectStatement {
                ctes: vec![],
                distinct: false,
                lateral: false,
                fields: vec![],
                tables: vec![],
                join: vec![],
                where_clause: None,
                group_by: None,
                having: None,
                order: None,
                limit_clause: LimitClause::default(),
                metadata: Default::default(),
            }));

            let result = validate_no_nested_aggregates(&expr);
            assert!(result.is_err());
        }

        #[test]
        fn test_exists_subquery_rejected() {
            let expr = Expr::Exists(Box::new(SelectStatement {
                ctes: vec![],
                distinct: false,
                lateral: false,
                fields: vec![],
                tables: vec![],
                join: vec![],
                where_clause: None,
                group_by: None,
                having: None,
                order: None,
                limit_clause: LimitClause::default(),
                metadata: Default::default(),
            }));

            let result = validate_no_nested_aggregates(&expr);
            assert!(result.is_err());
        }

        #[test]
        fn test_nested_aggregate_rejected() {
            // Create SUM(COUNT(*)) - nested aggregate
            let inner_agg = FunctionExpr::Count {
                expr: Box::new(Expr::Column(make_column("col1"))),
                distinct: false,
            };

            let expr = Expr::Call(FunctionExpr::Sum {
                expr: Box::new(Expr::Call(inner_agg)),
                distinct: false,
            });

            let result = validate_no_nested_aggregates(&expr);
            assert!(result.is_err());
        }

        #[test]
        fn test_nested_aggregate_in_arithmetic() {
            // Create 1 + COUNT(*) - aggregate in arithmetic expression
            let count_expr = Expr::Call(FunctionExpr::Count {
                expr: Box::new(Expr::Column(make_column("col1"))),
                distinct: false,
            });

            let expr = Expr::BinaryOp {
                lhs: Box::new(Expr::Literal(readyset_sql::ast::Literal::Integer(1))),
                op: readyset_sql::ast::BinaryOperator::Add,
                rhs: Box::new(count_expr),
            };

            let result = validate_no_nested_aggregates(&expr);
            assert!(result.is_err());
        }
    }

    mod emit_raw_tests {
        use super::*;

        fn make_simple_data(values: Vec<DfValue>) -> AccumulatorData {
            AccumulatorData::Simple(values)
        }

        fn make_distinct_ordered_data(
            values: Vec<(DfValue, usize)>,
            order_by: Option<(OrderType, NullOrder)>,
        ) -> AccumulatorData {
            let mut map = std::collections::BTreeMap::new();
            for (val, count) in values {
                map.insert(
                    OrderableDfValue {
                        value: val,
                        order_by,
                    },
                    count,
                );
            }
            AccumulatorData::DistinctOrdered(map)
        }

        #[test]
        fn test_emit_raw_simple() {
            let op = AccumulationOp::StringAgg {
                separator: Some(",".to_string()),
                distinct: false.into(),
                order_by: None,
            };
            let data = make_simple_data(vec![
                DfValue::from("a"),
                DfValue::from("b"),
                DfValue::from("c"),
            ]);

            let result = op.emit_raw(&data);
            match result {
                DfValue::Array(arr) => {
                    let vals: Vec<_> = arr.values().cloned().collect();
                    assert_eq!(
                        vals,
                        vec![DfValue::from("a"), DfValue::from("b"), DfValue::from("c")]
                    );
                }
                other => panic!("Expected Array, got {:?}", other),
            }
        }

        #[test]
        fn test_emit_raw_distinct_ordered() {
            let op = AccumulationOp::StringAgg {
                separator: Some(",".to_string()),
                distinct: true.into(),
                order_by: None,
            };
            let data = make_distinct_ordered_data(
                vec![(DfValue::from("a"), 3), (DfValue::from("b"), 2)],
                None,
            );

            let result = op.emit_raw(&data);
            match result {
                DfValue::Array(arr) => {
                    let vals: Vec<_> = arr.values().cloned().collect();
                    assert_eq!(vals, vec![DfValue::from("a"), DfValue::from("b")]);
                }
                other => panic!("Expected Array, got {:?}", other),
            }
        }

        #[test]
        fn test_emit_raw_non_distinct_ordered() {
            let op = AccumulationOp::GroupConcat {
                separator: ",".to_string(),
                distinct: false.into(),
                order_by: Some((OrderType::OrderAscending, NullOrder::NullsFirst)),
            };
            let data = make_distinct_ordered_data(
                vec![(DfValue::from("a"), 2), (DfValue::from("b"), 1)],
                Some((OrderType::OrderAscending, NullOrder::NullsFirst)),
            );

            let result = op.emit_raw(&data);
            match result {
                DfValue::Array(arr) => {
                    let vals: Vec<_> = arr.values().cloned().collect();
                    assert_eq!(
                        vals,
                        vec![DfValue::from("a"), DfValue::from("a"), DfValue::from("b")]
                    );
                }
                other => panic!("Expected Array, got {:?}", other),
            }
        }

        #[test]
        fn test_emit_raw_empty() {
            let op = AccumulationOp::StringAgg {
                separator: Some(",".to_string()),
                distinct: false.into(),
                order_by: None,
            };
            let data = make_simple_data(vec![]);

            let result = op.emit_raw(&data);
            match result {
                DfValue::Array(arr) => {
                    assert_eq!(arr.values().count(), 0);
                }
                other => panic!("Expected empty Array, got {:?}", other),
            }
        }

        #[test]
        fn test_emit_raw_normalizes_collation_simple() {
            let op = AccumulationOp::GroupConcat {
                separator: ",".to_string(),
                distinct: false.into(),
                order_by: None,
            };
            // Simulate MySQL-origin values with Utf8AiCi collation
            let data = make_simple_data(vec![
                DfValue::from_str_and_collation("aaa", Collation::Utf8AiCi),
                DfValue::from_str_and_collation("AAA", Collation::Utf8AiCi),
            ]);

            let result = op.emit_raw(&data);
            let arr = match result {
                DfValue::Array(arr) => arr,
                other => panic!("Expected Array, got {:?}", other),
            };
            let vals: Vec<_> = arr.values().cloned().collect();
            assert_eq!(vals.len(), 2);
            // Both values should now have Utf8 collation
            assert_eq!(vals[0].collation(), Some(Collation::Utf8));
            assert_eq!(vals[1].collation(), Some(Collation::Utf8));
            // And they should compare as NOT equal (case-sensitive)
            assert_ne!(vals[0], vals[1]);
        }

        #[test]
        fn test_emit_raw_normalizes_collation_distinct_ordered() {
            let op = AccumulationOp::StringAgg {
                separator: Some(",".to_string()),
                distinct: true.into(),
                order_by: None,
            };
            // Build a DistinctOrdered map with Utf8AiCi values.
            // Note: with Utf8AiCi, "aaa" and "AAA" would be the same key in the
            // BTreeMap, so we only insert one of them here. In practice, the
            // dataflow accumulator already deduplicates per-key.
            let data = make_distinct_ordered_data(
                vec![
                    (
                        DfValue::from_str_and_collation("aaa", Collation::Utf8AiCi),
                        2,
                    ),
                    (
                        DfValue::from_str_and_collation("bbb", Collation::Utf8AiCi),
                        1,
                    ),
                ],
                None,
            );

            let result = op.emit_raw(&data);
            let arr = match result {
                DfValue::Array(arr) => arr,
                other => panic!("Expected Array, got {:?}", other),
            };
            let vals: Vec<_> = arr.values().cloned().collect();
            // Distinct: each emitted once regardless of count
            assert_eq!(vals.len(), 2);
            for v in &vals {
                assert_eq!(v.collation(), Some(Collation::Utf8));
            }
        }

        #[test]
        fn test_emit_raw_preserves_non_text_values() {
            let op = AccumulationOp::ArrayAgg {
                distinct: false.into(),
                order_by: None,
            };
            let data = make_simple_data(vec![DfValue::Int(1), DfValue::Int(2), DfValue::None]);

            let result = op.emit_raw(&data);
            let arr = match result {
                DfValue::Array(arr) => arr,
                other => panic!("Expected Array, got {:?}", other),
            };
            let vals: Vec<_> = arr.values().cloned().collect();
            assert_eq!(vals, vec![DfValue::Int(1), DfValue::Int(2), DfValue::None]);
        }

        /// Simulates the cross-key post-lookup scenario that caused the original
        /// collation bug: key 1 has "aaa" and key 2 has "AAA" (both Utf8AiCi).
        /// After emit_raw normalizes to Utf8, add_raw into a DistinctOrdered map
        /// should keep them as separate entries.
        #[test]
        fn test_add_raw_preserves_case_after_normalization() {
            let op = AccumulationOp::GroupConcat {
                separator: "::".to_string(),
                distinct: true.into(),
                order_by: None,
            };

            // Simulate key 1's raw array (already normalized by emit_raw)
            let key1_values =
                DfValue::Array(std::sync::Arc::new(readyset_data::Array::from(vec![
                    DfValue::from("aaa"),
                    DfValue::from("bbb"),
                ])));
            // Simulate key 2's raw array
            let key2_values =
                DfValue::Array(std::sync::Arc::new(readyset_data::Array::from(vec![
                    DfValue::from("AAA"),
                    DfValue::from("zzz"),
                ])));

            let mut acc_data: AccumulatorData = (&op).into();
            acc_data.add_raw(&op, &key1_values).unwrap();
            acc_data.add_raw(&op, &key2_values).unwrap();

            // With Utf8 collation (post-normalization), "aaa" and "AAA" are distinct
            let result = op.apply(&acc_data).unwrap();
            let result_str = result.as_str().expect("should be text");
            // Should contain all 4 values
            let parts: Vec<&str> = result_str.split("::").collect();
            assert_eq!(parts.len(), 4);
            assert!(parts.contains(&"aaa"));
            assert!(parts.contains(&"AAA"));
            assert!(parts.contains(&"bbb"));
            assert!(parts.contains(&"zzz"));
        }

        /// Without collation normalization, Utf8AiCi values would merge in the
        /// BTreeMap. This test demonstrates that add_raw with original Utf8AiCi
        /// values WOULD lose "AAA" (merged into "aaa"), confirming the bug exists
        /// when normalization is skipped.
        #[test]
        fn test_add_raw_without_normalization_merges_case() {
            let op = AccumulationOp::GroupConcat {
                separator: "::".to_string(),
                distinct: true.into(),
                order_by: None,
            };

            // Use Utf8AiCi values directly (simulating what would happen without
            // the normalization fix in emit_raw)
            let key1_values =
                DfValue::Array(std::sync::Arc::new(readyset_data::Array::from(vec![
                    DfValue::from_str_and_collation("aaa", Collation::Utf8AiCi),
                    DfValue::from_str_and_collation("bbb", Collation::Utf8AiCi),
                ])));
            let key2_values =
                DfValue::Array(std::sync::Arc::new(readyset_data::Array::from(vec![
                    DfValue::from_str_and_collation("AAA", Collation::Utf8AiCi),
                    DfValue::from_str_and_collation("zzz", Collation::Utf8AiCi),
                ])));

            let mut acc_data: AccumulatorData = (&op).into();
            acc_data.add_raw(&op, &key1_values).unwrap();
            acc_data.add_raw(&op, &key2_values).unwrap();

            // With Utf8AiCi, "aaa" and "AAA" are equal — BTreeMap merges them
            let result = op.apply(&acc_data).unwrap();
            let result_str = result.as_str().expect("should be text");
            let parts: Vec<&str> = result_str.split("::").collect();
            // Only 3 values: AAA was merged into aaa
            assert_eq!(parts.len(), 3);
            assert!(!parts.contains(&"AAA"), "AAA should be merged into aaa");
        }
    }

    mod array_agg_multidim_tests {
        use super::*;

        fn simple_array_agg() -> AccumulationOp {
            AccumulationOp::ArrayAgg {
                distinct: DistinctOption::NotDistinct,
                order_by: None,
            }
        }

        fn distinct_array_agg() -> AccumulationOp {
            AccumulationOp::ArrayAgg {
                distinct: DistinctOption::IsDistinct,
                order_by: Some((OrderType::OrderAscending, NullOrder::NullsFirst)),
            }
        }

        fn make_array_val(vals: Vec<i64>) -> DfValue {
            DfValue::Array(std::sync::Arc::new(readyset_data::Array::from(
                vals.into_iter().map(DfValue::from).collect::<Vec<_>>(),
            )))
        }

        #[test]
        fn apply_array_agg_on_array_columns() {
            let op = simple_array_agg();
            let mut data = AccumulatorData::from(&op);
            data.add(&op, make_array_val(vec![1, 2]));
            data.add(&op, make_array_val(vec![3, 4]));

            let result = op.apply(&data).unwrap();
            match &result {
                DfValue::Array(arr) => {
                    assert_eq!(arr.num_dimensions(), 2);
                    assert_eq!(arr.to_string(), "{{1,2},{3,4}}");
                }
                other => panic!("Expected Array, got: {:?}", other),
            }
        }

        #[test]
        fn apply_array_agg_mixed_types_errors() {
            let op = simple_array_agg();
            let mut data = AccumulatorData::from(&op);
            data.add(&op, make_array_val(vec![1, 2]));
            data.add(&op, DfValue::from(42));

            let result = op.apply(&data);
            assert!(result.is_err());
        }

        #[test]
        fn apply_array_agg_mismatched_shapes_errors() {
            let op = simple_array_agg();
            let mut data = AccumulatorData::from(&op);
            data.add(&op, make_array_val(vec![1, 2]));
            data.add(&op, make_array_val(vec![3]));

            let result = op.apply(&data);
            assert!(result.is_err());
        }

        #[test]
        fn split_roundtrip_multidim_array_agg() {
            // Build a 2D array via apply, then split it back, then re-apply.
            // The result should be identical (round-trip).
            let op = simple_array_agg();
            let mut data = AccumulatorData::from(&op);
            data.add(&op, make_array_val(vec![1, 2]));
            data.add(&op, make_array_val(vec![3, 4]));

            let original = op.apply(&data).unwrap();

            // Split and re-accumulate
            let parts = op.split(&original).unwrap();
            assert_eq!(parts.len(), 2);
            // Each part should be a DfValue::Array
            for p in &parts {
                assert!(matches!(p, DfValue::Array(_)));
            }

            let mut data2 = AccumulatorData::from(&op);
            for p in parts {
                data2.add(&op, p);
            }
            let roundtripped = op.apply(&data2).unwrap();
            assert_eq!(original, roundtripped);
        }

        #[test]
        fn split_roundtrip_1d_unchanged() {
            let op = simple_array_agg();
            let mut data = AccumulatorData::from(&op);
            data.add(&op, DfValue::from(1));
            data.add(&op, DfValue::from(2));
            data.add(&op, DfValue::from(3));

            let original = op.apply(&data).unwrap();
            let parts = op.split(&original).unwrap();
            assert_eq!(parts.len(), 3);
            assert_eq!(
                parts,
                vec![DfValue::from(1), DfValue::from(2), DfValue::from(3)]
            );
        }

        #[test]
        fn distinct_ordered_array_agg_on_array_columns() {
            let op = distinct_array_agg();
            let mut data = AccumulatorData::from(&op);
            data.add(&op, make_array_val(vec![3, 4]));
            data.add(&op, make_array_val(vec![1, 2]));
            data.add(&op, make_array_val(vec![3, 4])); // duplicate

            let result = op.apply(&data).unwrap();
            match &result {
                DfValue::Array(arr) => {
                    assert_eq!(arr.num_dimensions(), 2);
                    // Distinct: only {1,2} and {3,4}; ordered ascending
                    assert_eq!(arr.to_string(), "{{1,2},{3,4}}");
                }
                other => panic!("Expected Array, got: {:?}", other),
            }
        }
    }
}
