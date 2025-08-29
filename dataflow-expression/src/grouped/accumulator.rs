//! This module implements SQL aggregation functions that accumulate multiple input values
//! into a single output value. Currently only supports mysql's `GROUP_CONCAT`, postgres' `STRING_AGG`
//! and `ARRAY_AGG`, and the various JSON object aggregation functions.
use std::collections::BTreeMap;

use crate::eval::json;
use readyset_data::DfValue;
use readyset_errors::{internal, internal_err, ReadySetResult};
use readyset_sql::ast::DistinctOption;
use serde::{Deserialize, Serialize};

/// Supported accumulation operators.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AccumulationOp {
    /// Concatenate values into an array. Allows NULL values in the output array.
    ArrayAgg { distinct: DistinctOption },
    /// Concatenates using the given separator between values. The string result is with the concatenated non-NULL
    /// values from a group. It returns NULL if there are no non-NULL values.
    GroupConcat {
        separator: String,
        distinct: DistinctOption,
    },
    /// Aggregates key-value pairs into JSON object.
    JsonObjectAgg { allow_duplicate_keys: bool },
    /// Concatenates using the given separator between values. Postgres allows the separator
    /// to be `NULL` (yet it must be specified in the query string). The string result is the
    /// concatenated non-NULL values from a group. It returns NULL if there are no non-NULL values.
    StringAgg {
        separator: Option<String>,
        distinct: DistinctOption,
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
            Self::ArrayAgg { distinct }
            | Self::GroupConcat { distinct, .. }
            | Self::StringAgg { distinct, .. } => *distinct == DistinctOption::IsDistinct,
            // uniqueness is handled is slightly differently for the JSON aggregators
            Self::JsonObjectAgg { .. } => false,
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
                        std::iter::repeat_n(k.clone(), repeat_count)
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
    /// When we add support for `order by`, we'll need to do some surgery here anyway, so we can
    /// resolve the default NULL ordering then, as well. (Also, fwiw, if the user didn't specify an ordering
    /// via an `order by` clause, they get what they get and this "NULLs ordered first" is not a bug :shrug:).
    /// Further note: the `group_concat()` and `string_agg()` functions don't output NULLs, so really this
    /// only affects `array_agg()` which does output NULLs.
    DistinctOrdered(BTreeMap<DfValue, usize>),
}

impl AccumulatorData {
    pub fn add(&mut self, op: &AccumulationOp, value: DfValue) {
        match self {
            AccumulatorData::Simple(v) => {
                // check to make sure we've got the correct data structure
                assert!(!op.is_distinct());
                v.push(value);
            }
            AccumulatorData::DistinctOrdered(v) => {
                v.entry(value).and_modify(|cnt| *cnt += 1).or_insert(1);
            }
        }
    }

    pub fn remove(&mut self, value: DfValue) -> ReadySetResult<()> {
        match self {
            AccumulatorData::Simple(v) => {
                let item_pos = v
                    .iter()
                    .rposition(|x| x == &value)
                    .ok_or_else(|| internal_err!("accumulator couldn't remove value from data"))?;
                v.remove(item_pos);
            }
            AccumulatorData::DistinctOrdered(v) => {
                match v.get_mut(&value) {
                    Some(cnt) => {
                        if *cnt == 1 {
                            v.remove(&value);
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
            ArrayAgg { distinct } | GroupConcat { distinct, .. } | StringAgg { distinct, .. } => {
                if *distinct == DistinctOption::IsDistinct {
                    AccumulatorData::DistinctOrdered(Default::default())
                } else {
                    AccumulatorData::Simple(Default::default())
                }
            }
        }
    }
}
