//! This module implements SQL aggregation functions that accumulate multiple input values
//! into a single output value. Currently only supports mysql's `GROUP_CONCAT` and the various
//! JSON object aggregation functions.
use crate::eval::json;
use readyset_data::DfValue;
use readyset_errors::{internal_err, ReadySetResult};
use serde::{Deserialize, Serialize};

/// Supported accumulation operators.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AccumulationOp {
    /// Concatenates using the given separator between values.
    GroupConcat { separator: String },
    /// Aggregates key-value pairs into JSON object.
    JsonObjectAgg { allow_duplicate_keys: bool },
}

impl AccumulationOp {
    fn apply_group_concat(&self, data: &[DfValue], separator: &str) -> ReadySetResult<DfValue> {
        let out_str = data
            .iter()
            .map(|piece| piece.to_string())
            .collect::<Vec<_>>()
            .join(separator);
        Ok(out_str.into())
    }

    fn apply_json_object_agg(
        &self,
        data: &[DfValue],
        allow_duplicate_keys: bool,
    ) -> ReadySetResult<DfValue> {
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

    pub fn apply(&self, data: &[DfValue]) -> ReadySetResult<DfValue> {
        match self {
            AccumulationOp::GroupConcat { separator } => self.apply_group_concat(data, separator),
            AccumulationOp::JsonObjectAgg {
                allow_duplicate_keys,
            } => self.apply_json_object_agg(data, *allow_duplicate_keys),
        }
    }
}
