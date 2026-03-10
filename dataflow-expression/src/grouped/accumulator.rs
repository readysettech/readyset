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

use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt::{Display, Formatter, Result};
use std::sync::Arc;

use readyset_data::{Array, Collation, DfValue};
use readyset_errors::{internal, internal_err, unsupported, ReadySetError, ReadySetResult};
use readyset_sql::analysis::is_aggregate;
use readyset_sql::analysis::visit::{self, Visitor};
use readyset_sql::ast::{
    DistinctOption, Expr, FieldReference, FunctionExpr, NullOrder, OrderClause, OrderType,
};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
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

/// Convert a `&DfValue` to a `JsonValue` without cloning the DfValue.
///
/// This is a borrowing alternative to `TryFrom<DfValue> for JsonValue`, used on the apply() hot
/// path to avoid cloning every stored element.
fn dfvalue_to_json(val: &DfValue) -> ReadySetResult<JsonValue> {
    Ok(match val {
        DfValue::Int(i) => JsonValue::Number((*i).into()),
        DfValue::UnsignedInt(u) => JsonValue::Number((*u).into()),
        DfValue::Float(f) => serde_json::Number::from_f64(*f as f64)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        DfValue::Double(f) => serde_json::Number::from_f64(*f)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        DfValue::Text(t) => JsonValue::String(t.as_str().to_string()),
        DfValue::TinyText(t) => JsonValue::String(t.as_str().to_string()),
        DfValue::TimestampTz(ts) => JsonValue::String(ts.to_string()),
        DfValue::Time(t) => JsonValue::String(t.to_string()),
        DfValue::Numeric(d) => JsonValue::String(d.to_string()),
        DfValue::None | DfValue::Default | DfValue::Max | DfValue::PassThrough(_) => {
            JsonValue::Null
        }
        other => {
            return Err(internal_err!(
                "json_object_agg: cannot convert {:?} to JSON value",
                std::mem::discriminant(other)
            ))
        }
    })
}

/// Extract a key/value pair from a stored json_object_agg element, returning types ready for
/// JSON serialization. Borrows the key string directly from the stored DfValue.
///
/// Expects the **array format**: `DfValue::Array([key, value])` — borrows key as `&str`,
/// converts value to `JsonValue` without cloning. AccumulatorData is never persisted; it is
/// always rebuilt from replay, so all stored elements use the array format produced by
/// `json_kv_to_array()`.
fn extract_json_kv_for_serial(value: &DfValue) -> ReadySetResult<(Cow<'_, str>, JsonValue)> {
    let arr = match value {
        DfValue::Array(arr) => arr,
        _ => {
            return Err(internal_err!(
                "json_object_agg: expected Array, got {:?}",
                std::mem::discriminant(value)
            ))
        }
    };
    let mut vals = arr.values();
    let key = vals
        .next()
        .ok_or_else(|| internal_err!("json_object_agg: array missing key"))?;
    let val = vals
        .next()
        .ok_or_else(|| internal_err!("json_object_agg: array missing value"))?;
    debug_assert!(
        vals.next().is_none(),
        "json_object_agg: array has more than 2 elements"
    );
    let key_str =
        <&str>::try_from(key).map_err(|_| internal_err!("json_object_agg: key is not a string"))?;
    let val_json = dfvalue_to_json(val)?;
    Ok((Cow::Borrowed(key_str), val_json))
}

/// Parse a JSON object DfValue (like `{"key": value}`) into a `DfValue::Array([key, value])`.
///
/// Used by `add()` and `remove()` for `JsonObjectAgg` to store key/value pairs in a format that
/// avoids repeated JSON parsing in `apply_json_object_agg()`.
fn json_kv_to_array(value: DfValue) -> ReadySetResult<DfValue> {
    let json_obj = value
        .to_json()
        .map_err(|_| internal_err!("json_object_agg: failed to parse json"))?;
    let obj = match json_obj {
        JsonValue::Object(map) => map,
        _ => return Err(internal_err!("json_object_agg: value is not an object")),
    };
    if obj.len() != 1 {
        return Err(internal_err!(
            "json_object_agg: expected single-key object, got {} keys",
            obj.len()
        ));
    }
    // SAFETY of unwrap: checked len == 1 above
    #[allow(clippy::unwrap_used)]
    let (k, v) = obj.into_iter().next().unwrap();
    Ok(DfValue::Array(Arc::new(Array::from(vec![
        DfValue::from(k.as_str()),
        DfValue::from(v),
    ]))))
}

/// Finalize a raw `DfValue::Array` of `[key, value]` sub-arrays directly into a JSON string.
///
/// Bypasses intermediate `AccumulatorData` allocation. Used by the `finalize_raw` fast path
/// for single-group post-lookup aggregation.
pub fn finalize_raw_json(allow_duplicate_keys: bool, value: &DfValue) -> ReadySetResult<DfValue> {
    let arr = match value {
        DfValue::Array(arr) => arr,
        DfValue::None => return Ok(DfValue::None),
        other => {
            return Err(internal_err!(
                "finalize_raw_json: expected DfValue::Array, got {:?}",
                std::mem::discriminant(other)
            ))
        }
    };

    if arr.total_len() == 0 {
        return Ok(DfValue::None);
    }

    let json_str = if allow_duplicate_keys {
        let mut pairs: Vec<(Cow<'_, str>, JsonValue)> = Vec::with_capacity(arr.total_len());
        for sub in arr.values() {
            pairs.push(extract_json_kv_for_serial(sub)?);
        }
        let serializable = crate::utils::serialize_slice_as_map(&pairs);
        serde_json::to_string(&serializable)
    } else {
        let mut map: BTreeMap<Cow<'_, str>, JsonValue> = BTreeMap::new();
        for sub in arr.values() {
            let (k, v) = extract_json_kv_for_serial(sub)?;
            map.insert(k, v);
        }
        serde_json::to_string(&map)
    };

    json_str
        .map(DfValue::from)
        .map_err(|e| internal_err!("finalize_raw_json: serialization failed: {e}"))
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
        // PostgreSQL: ARRAY_AGG() over zero rows returns NULL, not an empty array.
        // (ARRAY(SELECT ...) constructors return empty arrays, but those are rewritten
        // as COALESCE(array_agg(...), ARRAY[]) which handles the NULL-to-empty conversion.)
        if data.is_empty() {
            return Ok(DfValue::None);
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

        Ok(DfValue::Array(Arc::new(arr)))
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

        if data.is_empty() {
            return Ok("{}".into());
        }

        // Serialize directly from stored [key, value] arrays to JSON string, bypassing
        // the intermediate DfValue Vecs, Array constructions, and re-iteration that
        // json_object_from_keys_and_values would require.
        let json_str = if allow_duplicate_keys {
            let mut pairs: Vec<(Cow<'_, str>, JsonValue)> = Vec::with_capacity(data.len());
            for value in data {
                pairs.push(extract_json_kv_for_serial(value)?);
            }
            let serializable = crate::utils::serialize_slice_as_map(&pairs);
            serde_json::to_string(&serializable)
        } else {
            let mut map: BTreeMap<Cow<'_, str>, JsonValue> = BTreeMap::new();
            for value in data {
                let (k, v) = extract_json_kv_for_serial(value)?;
                map.insert(k, v);
            }
            serde_json::to_string(&map)
        };

        json_str
            .map(|s| s.into())
            .map_err(|e| internal_err!("json_object_agg: serialization failed: {e}"))
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
            AccumulationOp::JsonObjectAgg {
                allow_duplicate_keys,
            } => {
                // serde_json::Map is BTreeMap-backed: it deduplicates and reorders keys.
                // json_object_agg (allow_duplicate_keys=true) can have duplicate keys and
                // preserves insertion order, so split() would silently lose data.
                // The raw path (emit_raw/add_raw) must be used instead.
                if *allow_duplicate_keys {
                    internal!(
                        "split() cannot round-trip json_object_agg with duplicate keys; \
                         use the raw path (skip_finalization + raw_values)"
                    )
                }
                // Parse the finalized JSON object string back into individual
                // single-key JSON objects (e.g. {"k1":"v1","k2":"v2"} -> [{"k1":"v1"}, {"k2":"v2"}]).
                // Each element is a DfValue string that `add()` will convert via json_kv_to_array().
                // Note: this round-trips through JSON serialization twice (here + json_kv_to_array),
                // but this is the non-raw fallback path and only applies to jsonb_object_agg.
                let json_str = value
                    .as_str()
                    .ok_or_else(|| internal_err!("json_object_agg split: expected string"))?;
                let obj: serde_json::Map<String, JsonValue> = serde_json::from_str(json_str)
                    .map_err(|e| internal_err!("json_object_agg split: invalid JSON: {e}"))?;
                obj.into_iter()
                    .map(|(k, v)| {
                        let single = serde_json::json!({ k: v });
                        DfValue::from(single.to_string())
                    })
                    .collect::<Vec<_>>()
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
        for v in op.split(&value)? {
            self.add(op, v)?;
        }
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

    pub fn add(&mut self, op: &AccumulationOp, value: DfValue) -> ReadySetResult<()> {
        match self {
            AccumulatorData::Simple(v) => {
                if op.is_distinct() {
                    return Err(internal_err!(
                        "AccumulatorData::Simple received a distinct op: {:?}",
                        <&str>::from(op)
                    ));
                }
                if matches!(op, AccumulationOp::JsonObjectAgg { .. }) {
                    // Parse the JSON string once on entry and store as a [key, value] array,
                    // avoiding repeated JSON parsing in apply_json_object_agg().
                    v.push(json_kv_to_array(value)?);
                } else {
                    v.push(value);
                }
            }
            AccumulatorData::DistinctOrdered(v) => {
                let order_by = op.order_by();
                let key = OrderableDfValue { value, order_by };
                v.entry(key).and_modify(|cnt| *cnt += 1).or_insert(1);
            }
        }
        Ok(())
    }

    pub fn remove(&mut self, op: &AccumulationOp, value: DfValue) -> ReadySetResult<()> {
        match self {
            AccumulatorData::Simple(v) => {
                let search_value = if matches!(op, AccumulationOp::JsonObjectAgg { .. }) {
                    // Convert the incoming JSON string to the same [key, value] array format
                    // used for storage, so the equality comparison finds the correct element.
                    json_kv_to_array(value)?
                } else {
                    value
                };
                // NOTE: rposition() is O(N) and Vec::remove() is O(N) for element shifting,
                // making N consecutive deletes O(N^2). swap_remove would be O(1) but cannot
                // be used because json_object_agg preserves insertion order.
                let item_pos = v
                    .iter()
                    .rposition(|x| x == &search_value)
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

    mod json_object_agg_tests {
        use super::*;

        fn make_json_obj_op(allow_duplicate_keys: bool) -> AccumulationOp {
            AccumulationOp::JsonObjectAgg {
                allow_duplicate_keys,
            }
        }

        /// Simulate what the upstream `json_build_object(key, value)` projection produces.
        fn make_json_object(key: &str, value: &str) -> DfValue {
            DfValue::from(format!("{{\"{key}\":\"{value}\"}}"))
        }

        fn make_json_object_int(key: &str, value: i64) -> DfValue {
            DfValue::from(format!("{{\"{key}\":{value}}}"))
        }

        #[test]
        fn add_stores_as_array() {
            let op = make_json_obj_op(true);
            let mut data = AccumulatorData::from(&op);
            data.add(&op, make_json_object("a", "1")).unwrap();

            match &data {
                AccumulatorData::Simple(v) => {
                    assert_eq!(v.len(), 1);
                    assert!(matches!(&v[0], DfValue::Array(_)));
                }
                _ => panic!("expected Simple"),
            }
        }

        #[test]
        fn add_and_apply_round_trip() {
            let op = make_json_obj_op(true);
            let mut data = AccumulatorData::from(&op);

            data.add(&op, make_json_object("a", "1")).unwrap();
            data.add(&op, make_json_object("b", "2")).unwrap();

            let result = op.apply(&data).unwrap();
            let result_str: String = (&result).try_into().unwrap();
            assert_eq!(result_str, r#"{"a":"1","b":"2"}"#);
        }

        #[test]
        fn add_remove_apply() {
            let op = make_json_obj_op(true);
            let mut data = AccumulatorData::from(&op);

            data.add(&op, make_json_object("a", "1")).unwrap();
            data.add(&op, make_json_object("b", "2")).unwrap();
            data.add(&op, make_json_object("c", "3")).unwrap();

            // Remove the middle element
            data.remove(&op, make_json_object("b", "2")).unwrap();

            let result = op.apply(&data).unwrap();
            let result_str: String = (&result).try_into().unwrap();
            assert_eq!(result_str, r#"{"a":"1","c":"3"}"#);
        }

        #[test]
        fn remove_nonexistent_returns_error() {
            let op = make_json_obj_op(true);
            let mut data = AccumulatorData::from(&op);

            data.add(&op, make_json_object("a", "1")).unwrap();
            let result = data.remove(&op, make_json_object("x", "9"));
            assert!(result.is_err());
        }

        #[test]
        fn jsonb_no_duplicate_keys() {
            let op = make_json_obj_op(false);
            let mut data = AccumulatorData::from(&op);

            data.add(&op, make_json_object("a", "1")).unwrap();
            data.add(&op, make_json_object("a", "2")).unwrap();

            let result = op.apply(&data).unwrap();
            let result_str: String = (&result).try_into().unwrap();
            // jsonb_object_agg deduplicates: last value wins
            assert_eq!(result_str, r#"{"a":"2"}"#);
        }

        #[test]
        fn integer_values() {
            let op = make_json_obj_op(true);
            let mut data = AccumulatorData::from(&op);

            data.add(&op, make_json_object_int("x", 42)).unwrap();
            data.add(&op, make_json_object_int("y", 99)).unwrap();

            let result = op.apply(&data).unwrap();
            let result_str: String = (&result).try_into().unwrap();
            assert_eq!(result_str, r#"{"x":42,"y":99}"#);
        }

        #[test]
        fn empty_data_produces_empty_object() {
            let op = make_json_obj_op(true);
            let data = AccumulatorData::from(&op);

            let result = op.apply(&data).unwrap();
            let result_str: String = (&result).try_into().unwrap();
            assert_eq!(result_str, "{}");
        }

        #[test]
        fn nested_object_values() {
            // Nested JSON objects are stored as serialized strings through the
            // DfValue round-trip (JsonValue::Object → DfValue::Text → JsonValue::String).
            // This means nested objects become double-serialized strings in the output.
            // This matches the behavior of the previous json_object_from_keys_and_values
            // code path and is a known limitation.
            let op = make_json_obj_op(true);
            let mut data = AccumulatorData::from(&op);

            data.add(&op, DfValue::from(r#"{"a":{"nested":true}}"#))
                .unwrap();

            let result = op.apply(&data).unwrap();
            let result_str: String = (&result).try_into().unwrap();
            assert_eq!(result_str, r#"{"a":"{\"nested\":true}"}"#);
        }

        #[test]
        fn null_json_values() {
            let op = make_json_obj_op(true);
            let mut data = AccumulatorData::from(&op);

            data.add(&op, DfValue::from(r#"{"a":null}"#)).unwrap();

            let result = op.apply(&data).unwrap();
            let result_str: String = (&result).try_into().unwrap();
            assert_eq!(result_str, r#"{"a":null}"#);
        }

        #[test]
        fn multi_key_input_rejected() {
            let op = make_json_obj_op(true);
            let mut data = AccumulatorData::from(&op);

            let result = data.add(&op, DfValue::from(r#"{"a":1,"b":2}"#));
            assert!(result.is_err());
        }

        #[test]
        fn non_array_in_apply_returns_error() {
            // If AccumulatorData somehow contains non-Array elements,
            // extract_json_kv_for_serial should return an error.
            let op = make_json_obj_op(true);
            let data = AccumulatorData::Simple(vec![DfValue::from(r#"{"a":"1"}"#)]);

            let result = op.apply(&data);
            assert!(result.is_err());
        }

        #[test]
        fn split_round_trips_through_add() {
            // split() should decompose a finalized JSON string into individual
            // single-key JSON objects that add() can re-ingest.
            let op = make_json_obj_op(false);
            let finalized = DfValue::from(r#"{"alpha":"one","beta":"two","gamma":"three"}"#);

            let parts = op.split(&finalized).unwrap();
            assert_eq!(parts.len(), 3);

            // Feed the split parts through add() to rebuild the accumulator
            let mut data = AccumulatorData::Simple(vec![]);
            for part in parts {
                data.add(&op, part).unwrap();
            }

            let result = op.apply(&data).unwrap();
            let result_str = result.as_str().expect("should be text");
            // BTreeMap ordering: alpha, beta, gamma
            assert_eq!(
                result_str,
                r#"{"alpha":"one","beta":"two","gamma":"three"}"#
            );
        }

        #[test]
        fn split_rejects_allow_duplicate_keys() {
            // json_object_agg (allow_duplicate_keys=true) cannot round-trip through
            // split() because serde_json::Map deduplicates keys. The raw path must be used.
            let op = make_json_obj_op(true);
            let finalized = DfValue::from(r#"{"a":"1","b":"2"}"#);

            let result = op.split(&finalized);
            assert!(result.is_err());
        }

        #[test]
        fn split_empty_object() {
            let op = make_json_obj_op(false);
            let finalized = DfValue::from("{}");

            let parts = op.split(&finalized).unwrap();
            assert!(parts.is_empty());
        }

        #[test]
        fn emit_raw_round_trips_through_add_raw() {
            // Simulates the post-lookup path: emit_raw -> add_raw -> apply
            let op = make_json_obj_op(false);
            let mut data = AccumulatorData::Simple(vec![]);
            for (k, v) in [("x", "1"), ("y", "2"), ("z", "3")] {
                data.add(&op, make_json_object(k, v)).unwrap();
            }

            // emit_raw produces a DfValue::Array of [key, value] arrays
            let raw = op.emit_raw(&data);

            // Simulate post-lookup: add_raw into a fresh accumulator, then apply
            let mut post_data = AccumulatorData::Simple(vec![]);
            post_data.add_raw(&op, &raw).unwrap();
            let result = op.apply(&post_data).unwrap();
            let result_str = result.as_str().expect("should be text");
            assert_eq!(result_str, r#"{"x":"1","y":"2","z":"3"}"#);
        }

        #[test]
        fn emit_raw_round_trips_allow_duplicate_keys() {
            // json_object_agg (allow_duplicate_keys=true) through the raw post-lookup path
            // should preserve duplicate keys across groups.
            let op = make_json_obj_op(true);

            let mut data1 = AccumulatorData::Simple(vec![]);
            data1.add(&op, make_json_object("a", "first")).unwrap();
            let raw1 = op.emit_raw(&data1);

            let mut data2 = AccumulatorData::Simple(vec![]);
            data2.add(&op, make_json_object("a", "second")).unwrap();
            let raw2 = op.emit_raw(&data2);

            let mut merged = AccumulatorData::Simple(vec![]);
            merged.add_raw(&op, &raw1).unwrap();
            merged.add_raw(&op, &raw2).unwrap();

            let result = op.apply(&merged).unwrap();
            let result_str = result.as_str().expect("should be text");
            // allow_duplicate_keys=true preserves both entries
            assert_eq!(result_str, r#"{"a":"first","a":"second"}"#);
        }

        #[test]
        fn finalize_raw_json_allow_duplicate_keys() {
            // finalize_raw_json with allow_duplicate_keys=true preserves duplicates
            let op = make_json_obj_op(true);
            let mut data = AccumulatorData::Simple(vec![]);
            data.add(&op, make_json_object("k", "one")).unwrap();
            data.add(&op, make_json_object("k", "two")).unwrap();
            let raw = op.emit_raw(&data);

            let result = finalize_raw_json(true, &raw).unwrap();
            let result_str = result.as_str().expect("should be text");
            assert_eq!(result_str, r#"{"k":"one","k":"two"}"#);
        }

        #[test]
        fn finalize_raw_json_none_returns_none() {
            let result = finalize_raw_json(false, &DfValue::None).unwrap();
            assert_eq!(result, DfValue::None);
        }

        #[test]
        fn emit_raw_merges_multiple_groups() {
            // Simulates post-lookup merging two groups' raw emissions
            let op = make_json_obj_op(false);

            let mut data1 = AccumulatorData::Simple(vec![]);
            data1.add(&op, make_json_object("a", "1")).unwrap();
            data1.add(&op, make_json_object("b", "2")).unwrap();
            let raw1 = op.emit_raw(&data1);

            let mut data2 = AccumulatorData::Simple(vec![]);
            data2.add(&op, make_json_object("c", "3")).unwrap();
            let raw2 = op.emit_raw(&data2);

            // Merge via add_raw
            let mut merged = AccumulatorData::Simple(vec![]);
            merged.add_raw(&op, &raw1).unwrap();
            merged.add_raw(&op, &raw2).unwrap();

            let result = op.apply(&merged).unwrap();
            let result_str = result.as_str().expect("should be text");
            assert_eq!(result_str, r#"{"a":"1","b":"2","c":"3"}"#);
        }

        mod proptests {
            use proptest::prelude::*;

            use super::*;

            /// Strategy for generating valid JSON keys (non-empty alphanumeric).
            fn json_key() -> impl Strategy<Value = String> {
                "[a-zA-Z][a-zA-Z0-9]{0,15}"
            }

            /// Strategy for generating JSON-serializable values of various types.
            fn json_value_str() -> impl Strategy<Value = String> {
                prop_oneof![
                    // String values
                    "[a-zA-Z0-9 ]{0,30}".prop_map(|s| { serde_json::json!(s).to_string() }),
                    // Integer values
                    any::<i32>().prop_map(|n| n.to_string()),
                    // Null
                    Just("null".to_string()),
                ]
            }

            /// Strategy for generating a single-key JSON object string like {"key":value}
            fn json_kv_pair() -> impl Strategy<Value = (String, String)> {
                (json_key(), json_value_str())
            }

            proptest! {
                #[test]
                fn split_round_trips_through_add_apply(
                    pairs in prop::collection::hash_map(json_key(), json_value_str(), 1..20)
                ) {
                    // Build the initial accumulator via add()
                    let op = make_json_obj_op(false); // jsonb: deduplicate keys
                    let mut data = AccumulatorData::Simple(vec![]);
                    for (k, v) in &pairs {
                        // Build {"key": <raw_value>} — value is already a JSON literal
                        let json_str = format!(r#"{{"{k}":{v}}}"#);
                        data.add(&op, DfValue::from(json_str)).unwrap();
                    }

                    // Finalize to JSON string
                    let finalized = op.apply(&data).unwrap();
                    let finalized_str = finalized.as_str().expect("should be text");

                    // split() it back
                    let parts = op.split(&finalized).unwrap();
                    prop_assert_eq!(parts.len(), pairs.len());

                    // Rebuild through add() and re-finalize
                    let mut rebuilt = AccumulatorData::Simple(vec![]);
                    for part in parts {
                        rebuilt.add(&op, part).unwrap();
                    }
                    let result = op.apply(&rebuilt).unwrap();
                    let result_str = result.as_str().expect("should be text");

                    // Round-trip should produce identical JSON
                    prop_assert_eq!(finalized_str, result_str);
                }

                #[test]
                fn emit_raw_round_trips_through_add_raw_apply(
                    pairs in prop::collection::vec(json_kv_pair(), 1..20)
                ) {
                    let op = make_json_obj_op(false);
                    let mut data = AccumulatorData::Simple(vec![]);
                    for (k, v) in &pairs {
                        let json_str = format!(r#"{{"{k}":{v}}}"#);
                        data.add(&op, DfValue::from(json_str)).unwrap();
                    }

                    // Finalize the original
                    let expected = op.apply(&data).unwrap();

                    // emit_raw -> add_raw -> apply
                    let raw = op.emit_raw(&data);
                    let mut rebuilt = AccumulatorData::Simple(vec![]);
                    rebuilt.add_raw(&op, &raw).unwrap();
                    let result = op.apply(&rebuilt).unwrap();

                    prop_assert_eq!(
                        expected.as_str().expect("text"),
                        result.as_str().expect("text")
                    );
                }
            }
        }
    }

    mod validate_no_nested_aggregates_tests {
        use super::*;

        #[test]
        fn test_window_function_rejected() {
            let expr = Expr::WindowFunction {
                function: FunctionExpr::RowNumber,
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
            data.add(&op, make_array_val(vec![1, 2])).unwrap();
            data.add(&op, make_array_val(vec![3, 4])).unwrap();

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
            data.add(&op, make_array_val(vec![1, 2])).unwrap();
            data.add(&op, DfValue::from(42)).unwrap();

            let result = op.apply(&data);
            assert!(result.is_err());
        }

        #[test]
        fn apply_array_agg_mismatched_shapes_errors() {
            let op = simple_array_agg();
            let mut data = AccumulatorData::from(&op);
            data.add(&op, make_array_val(vec![1, 2])).unwrap();
            data.add(&op, make_array_val(vec![3])).unwrap();

            let result = op.apply(&data);
            assert!(result.is_err());
        }

        #[test]
        fn split_roundtrip_multidim_array_agg() {
            // Build a 2D array via apply, then split it back, then re-apply.
            // The result should be identical (round-trip).
            let op = simple_array_agg();
            let mut data = AccumulatorData::from(&op);
            data.add(&op, make_array_val(vec![1, 2])).unwrap();
            data.add(&op, make_array_val(vec![3, 4])).unwrap();

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
                data2.add(&op, p).unwrap();
            }
            let roundtripped = op.apply(&data2).unwrap();
            assert_eq!(original, roundtripped);
        }

        #[test]
        fn split_roundtrip_1d_unchanged() {
            let op = simple_array_agg();
            let mut data = AccumulatorData::from(&op);
            data.add(&op, DfValue::from(1)).unwrap();
            data.add(&op, DfValue::from(2)).unwrap();
            data.add(&op, DfValue::from(3)).unwrap();

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
            data.add(&op, make_array_val(vec![3, 4])).unwrap();
            data.add(&op, make_array_val(vec![1, 2])).unwrap();
            data.add(&op, make_array_val(vec![3, 4])).unwrap(); // duplicate

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
