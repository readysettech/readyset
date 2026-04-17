use std::cmp::{self, Ordering};
use std::fmt::{self, Debug, Write};
use std::sync::Arc;

use partial_map::InsertionOrder;
use readyset_data::upstream_system_props::get_group_concat_max_len;
use readyset_data::DfValue;
use readyset_errors::{internal, ReadySetResult};
use readyset_sql::ast::{NullOrder, OrderType};
use serde::{Deserialize, Serialize};

use crate::grouped::accumulator::{
    finalize_raw_json, truncate_group_concat, AccumulationOp, AccumulatorData,
};

/// Representation of an aggregate function
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub enum PostLookupAggregateFunction {
    ArrayAgg {
        op: AccumulationOp,
    },
    /// Add together all the input numbers
    ///
    /// Note that this encapsulates both `SUM` *and* `COUNT` in base SQL, as re-aggregating counts
    /// is done by just summing numbers together
    Sum,
    /// Concatenate together all the input strings with the given separator
    GroupConcat {
        op: AccumulationOp,
    },
    /// Take the maximum input value
    Max,
    /// Take the minimum input value
    Min,
    /// Use specified Key-value pair to build a JSON Object
    JsonObjectAgg {
        op: AccumulationOp,
    },
    /// Concatenate together all the input strings with the given separator
    StringAgg {
        op: AccumulationOp,
    },
}

impl PostLookupAggregateFunction {
    pub fn is_accumulation(&self) -> bool {
        match self {
            PostLookupAggregateFunction::ArrayAgg { .. }
            | PostLookupAggregateFunction::GroupConcat { .. }
            | PostLookupAggregateFunction::JsonObjectAgg { .. }
            | PostLookupAggregateFunction::StringAgg { .. } => true,
            PostLookupAggregateFunction::Max
            | PostLookupAggregateFunction::Min
            | PostLookupAggregateFunction::Sum => false,
        }
    }

    /// Apply this aggregate function to the two input values
    ///
    /// This forms a semigroup.
    pub fn apply(&self, val1: &DfValue, val2: &DfValue) -> ReadySetResult<DfValue> {
        match self {
            PostLookupAggregateFunction::Sum => val1 + val2,
            PostLookupAggregateFunction::Max => Ok(cmp::max(val1, val2).clone()),
            PostLookupAggregateFunction::Min => Ok(cmp::min(val1, val2).clone()),
            PostLookupAggregateFunction::ArrayAgg { .. }
            | PostLookupAggregateFunction::GroupConcat { .. }
            | PostLookupAggregateFunction::JsonObjectAgg { .. }
            | PostLookupAggregateFunction::StringAgg { .. } => {
                internal!("Calling non-accumlate operation on a accumulating function")
            }
        }
    }

    /// Accumulate values.
    pub fn apply_accumulated(
        &self,
        data: &mut AccumulatorData,
        value: &DfValue,
    ) -> ReadySetResult<()> {
        match self {
            PostLookupAggregateFunction::ArrayAgg { op }
            | PostLookupAggregateFunction::GroupConcat { op }
            | PostLookupAggregateFunction::JsonObjectAgg { op }
            | PostLookupAggregateFunction::StringAgg { op } => {
                data.add_accummulated(op, value.clone())
            }
            PostLookupAggregateFunction::Max
            | PostLookupAggregateFunction::Min
            | PostLookupAggregateFunction::Sum => {
                internal!("Calling accumlate operation on a non-accumulating function")
            }
        }
    }

    pub fn finish(&self, data: &AccumulatorData) -> ReadySetResult<DfValue> {
        match self {
            PostLookupAggregateFunction::ArrayAgg { op }
            | PostLookupAggregateFunction::GroupConcat { op }
            | PostLookupAggregateFunction::JsonObjectAgg { op }
            | PostLookupAggregateFunction::StringAgg { op } => op.apply(data),
            PostLookupAggregateFunction::Max
            | PostLookupAggregateFunction::Min
            | PostLookupAggregateFunction::Sum => {
                internal!("Non-accumulating function with accumulated data")
            }
        }
    }

    pub fn create_accumulator_data(&self) -> Option<AccumulatorData> {
        match self {
            PostLookupAggregateFunction::ArrayAgg { op }
            | PostLookupAggregateFunction::GroupConcat { op }
            | PostLookupAggregateFunction::JsonObjectAgg { op }
            | PostLookupAggregateFunction::StringAgg { op } => Some(op.into()),
            PostLookupAggregateFunction::Max
            | PostLookupAggregateFunction::Min
            | PostLookupAggregateFunction::Sum => None,
        }
    }

    /// Accumulate raw array values directly, bypassing split().
    pub fn apply_raw_accumulated(
        &self,
        data: &mut AccumulatorData,
        value: &DfValue,
    ) -> ReadySetResult<()> {
        match self {
            PostLookupAggregateFunction::ArrayAgg { op }
            | PostLookupAggregateFunction::GroupConcat { op }
            | PostLookupAggregateFunction::JsonObjectAgg { op }
            | PostLookupAggregateFunction::StringAgg { op } => data.add_raw(op, value),
            PostLookupAggregateFunction::Max
            | PostLookupAggregateFunction::Min
            | PostLookupAggregateFunction::Sum => {
                internal!("Calling raw accumulate on a non-accumulating function")
            }
        }
    }

    /// Finalize a single raw `DfValue::Array` into the output format.
    ///
    /// For simple (non-distinct, non-ordered) cases, this takes fast paths that avoid
    /// intermediate `AccumulatorData` allocation and double iteration:
    /// - `ArrayAgg`: the raw array IS the output, so just clone the value.
    /// - `GroupConcat`/`StringAgg`: directly iterate the array and write to a `String`
    ///   with the separator, skipping the intermediate `Vec<String>` + `join()`.
    pub fn finalize_raw(&self, value: &DfValue) -> ReadySetResult<DfValue> {
        match self {
            PostLookupAggregateFunction::ArrayAgg { op } => {
                // Fast path: non-distinct, non-ordered ArrayAgg — the raw array is the output.
                // An empty array means zero input rows; PostgreSQL returns NULL in that case.
                if !op.is_distinct() && op.order_by().is_none() {
                    if let DfValue::Array(arr) = value {
                        if arr.is_empty() {
                            return Ok(DfValue::None);
                        }
                    }
                    return Ok(value.clone());
                }
                // Distinct/ordered: fall through to full accumulator path.
                let mut acc_data: AccumulatorData = op.into();
                acc_data.add_raw(op, value)?;
                op.apply(&acc_data)
            }
            PostLookupAggregateFunction::GroupConcat { op }
            | PostLookupAggregateFunction::StringAgg { op } => {
                // Fast path: non-distinct, non-ordered string concat — single-pass direct join.
                if !op.is_distinct() && op.order_by().is_none() {
                    return Self::finalize_raw_string_direct(self, value);
                }
                // Distinct/ordered: fall through to full accumulator path.
                let mut acc_data: AccumulatorData = op.into();
                acc_data.add_raw(op, value)?;
                op.apply(&acc_data)
            }
            PostLookupAggregateFunction::JsonObjectAgg { op } => {
                // json_object_agg is always non-distinct, non-ordered.
                // Fast path: serialize directly from the raw [k,v] sub-arrays,
                // skipping the intermediate AccumulatorData allocation.
                let AccumulationOp::JsonObjectAgg {
                    allow_duplicate_keys,
                } = op
                else {
                    internal!("JsonObjectAgg PostLookupAggregateFunction with non-JsonObjectAgg op")
                };
                finalize_raw_json(*allow_duplicate_keys, value)
            }
            PostLookupAggregateFunction::Max
            | PostLookupAggregateFunction::Min
            | PostLookupAggregateFunction::Sum => {
                internal!("finalize_raw called on non-accumulating function")
            }
        }
    }

    /// Direct single-pass string concatenation for non-distinct, non-ordered
    /// `GroupConcat` / `StringAgg`.
    ///
    /// Appends each element directly into a pre-allocated `String`,
    /// interleaving the separator. Uses `as_str()` for Text/TinyText values
    /// to avoid `Display` formatting overhead on the hot path.
    fn finalize_raw_string_direct(&self, value: &DfValue) -> ReadySetResult<DfValue> {
        let arr = match value {
            DfValue::Array(arr) => arr,
            DfValue::None => return Ok(DfValue::None),
            other => {
                return Err(readyset_errors::internal_err!(
                    "finalize_raw_string_direct: expected DfValue::Array, got {:?}",
                    other
                ));
            }
        };

        let num_elements = arr.total_len();
        if num_elements == 0 {
            return Ok(DfValue::None);
        }

        let separator = match self {
            PostLookupAggregateFunction::StringAgg {
                op: AccumulationOp::StringAgg { separator, .. },
            } => separator.as_deref().unwrap_or(""),
            PostLookupAggregateFunction::GroupConcat {
                op: AccumulationOp::GroupConcat { separator, .. },
            } => separator.as_str(),
            _ => {
                return Err(readyset_errors::internal_err!(
                    "finalize_raw_string_direct called on non-string-concat function"
                ));
            }
        };

        let max_len = if matches!(self, PostLookupAggregateFunction::GroupConcat { .. }) {
            Some(get_group_concat_max_len())
        } else {
            None
        };

        // Pre-allocate: estimate ~16 bytes per element + separators, capped at
        // max_len when set to avoid over-allocating for large result sets.
        let estimated_cap = num_elements * 16 + (num_elements.saturating_sub(1)) * separator.len();
        let estimated_cap = match max_len {
            Some(max) => estimated_cap.min(max),
            None => estimated_cap,
        };
        let mut result = String::with_capacity(estimated_cap);
        let mut first = true;
        for val in arr.values() {
            if first {
                first = false;
            } else {
                result.push_str(separator);
            }
            // Fast path: Text/TinyText values can be appended directly without
            // going through Display formatting and try_from overhead.
            if let Some(s) = val.as_str() {
                result.push_str(s);
            } else {
                write!(&mut result, "{}", val).map_err(|e: fmt::Error| {
                    readyset_errors::internal_err!("fmt::Write failed: {}", e)
                })?;
            }
            // Stop early once we've reached the truncation limit.
            if let Some(max) = max_len {
                if result.len() >= max {
                    return Ok(DfValue::from(truncate_group_concat(&result, max)));
                }
            }
        }

        Ok(DfValue::from(result))
    }
}

/// Representation of a single aggregate function to be performed on a column post-lookup
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct PostLookupAggregate<Column = usize> {
    /// The column index in the result set containing the already-aggregated values
    pub column: Column,
    /// The aggregate function to perform
    pub function: PostLookupAggregateFunction,
    /// When true, the column contains raw `DfValue::Array` values from a dataflow
    /// accumulator with `skip_finalization`, and should be consumed directly
    /// instead of going through split().
    #[serde(default)]
    pub raw_values: bool,
}

impl<Column> PostLookupAggregate<Column> {
    /// Transform all column references in self by applying a function
    pub fn map_columns<F, C2, E>(self, mut f: F) -> Result<PostLookupAggregate<C2>, E>
    where
        F: FnMut(Column) -> Result<C2, E>,
    {
        Ok(PostLookupAggregate {
            column: f(self.column)?,
            function: self.function,
            raw_values: self.raw_values,
        })
    }

    pub fn description(&self) -> String
    where
        Column: std::fmt::Display,
    {
        format!(
            "{}({})",
            match &self.function {
                PostLookupAggregateFunction::ArrayAgg { .. } => "ArrayAgg",
                PostLookupAggregateFunction::GroupConcat { .. } => "GC",
                PostLookupAggregateFunction::JsonObjectAgg { op } => {
                    let AccumulationOp::JsonObjectAgg {
                        allow_duplicate_keys,
                    } = op
                    else {
                        panic!("Should only contain JsonObjectAgg AccumulationOp");
                    };

                    if *allow_duplicate_keys {
                        "JsonbObjectAgg"
                    } else {
                        "JsonObjectAgg"
                    }
                }
                PostLookupAggregateFunction::Max => "Max",
                PostLookupAggregateFunction::Min => "Min",
                PostLookupAggregateFunction::Sum => "Σ",
                PostLookupAggregateFunction::StringAgg { .. } => "StringAgg",
            },
            &self.column
        )
    }
}

/// Representation of a set of multiple aggregate functions to be performed post-lookup
///
/// This is used for range queries, where lookups cover multiple grouped keys
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct PostLookupAggregates<Column = usize> {
    /// The set of column indices to group the aggregate by
    pub group_by: Vec<Column>,
    /// The aggregate functions to perform
    pub aggregates: Vec<PostLookupAggregate<Column>>,
}

impl<Column> PostLookupAggregates<Column> {
    /// Transform all column references in self by applying a function
    pub fn map_columns<F, C2, E>(self, mut f: F) -> Result<PostLookupAggregates<C2>, E>
    where
        F: FnMut(Column) -> Result<C2, E>,
    {
        Ok(PostLookupAggregates {
            group_by: self
                .group_by
                .into_iter()
                .map(&mut f)
                .collect::<Result<_, E>>()?,
            aggregates: self
                .aggregates
                .into_iter()
                .map(|agg| agg.map_columns(&mut f))
                .collect::<Result<_, E>>()?,
        })
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, Eq, PartialEq)]
/// Operations to perform on rows before insertion into a reader or after a lookup
pub struct ReaderProcessing {
    /// Pre processing on rows prior to insertion into a reader
    pub pre_processing: PreInsertion,
    /// Post processing on result sets after a lookup is finished
    pub post_processing: PostLookup,
}

impl ReaderProcessing {
    /// Constructs a new [`PostLookup`]
    pub fn new(
        order_by: Option<Vec<(usize, OrderType, NullOrder)>>,
        limit: Option<usize>,
        returned_cols: Option<Vec<usize>>,
        default_row: Option<Vec<DfValue>>,
        aggregates: Option<PostLookupAggregates>,
    ) -> ReadySetResult<Self> {
        if let Some(cols) = &returned_cols {
            if cols.iter().enumerate().any(|(i, v)| i != *v) {
                internal!("Returned columns must be projected in order");
            }
        }

        let post_processing = PostLookup {
            order_by,
            limit,
            returned_cols,
            default_row: default_row.map(|r| Arc::new(r.into_boxed_slice())),
            aggregates,
        };

        let pre_processing = PreInsertion {
            order_by: post_processing.order_by.clone(),
            group_by: post_processing
                .aggregates
                .as_ref()
                .map(|v| v.group_by.clone()),
        };

        Ok(ReaderProcessing {
            pre_processing,
            post_processing,
        })
    }
}

/// Operations to perform on the results of a lookup after it's loaded from the map in a
/// reader
///
/// Because of limitations in the data structures we use to store reader state, some operations in a
/// query can't be cached as part of that state, and need to be performed after the results for a
/// query are loaded. We extract these operations as part of migration, and store them on the reader
/// node in this struct.
///
/// A previous version provided these operations as part of [`ViewQuery`] rather than storing them
/// on the reader node - they've been moved here so that the post-lookup operations can be based on
/// the desugared query rather than the original query.
#[derive(Serialize, Deserialize, Debug, Clone, Default, Eq, PartialEq)]
pub struct PostLookup {
    /// Column indices to order by, and whether or not to reverse order on each index.
    ///
    /// If an empty `Vec` is specified, rows are sorted in lexicographic order.
    pub order_by: Option<Vec<(usize, OrderType, NullOrder)>>,
    /// Maximum number of records to return
    pub limit: Option<usize>,
    /// Indices of the columns requested in the query. Reader will filter out all other projected
    /// columns
    pub returned_cols: Option<Vec<usize>>,
    /// Default values to send back, for example if we're aggregating and no rows are found
    pub default_row: Option<Arc<Box<[DfValue]>>>,
    /// Aggregates to perform on the result set *after* it's retrieved from the reader.
    ///
    /// Note that currently these are only performed on each key individually, not the overall
    /// result set returned by all keys in a multi-key lookup
    pub aggregates: Option<PostLookupAggregates>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, Eq, PartialEq)]
/// Operations to perform on a row before it is stored in the map in a reader.
pub struct PreInsertion {
    /// Column indices to order by, and whether or not to reverse order on each index.
    ///
    /// If an empty `Vec` is specified, rows are sorted in lexicographic order.
    order_by: Option<Vec<(usize, OrderType, NullOrder)>>,
    /// The set of column indices to group the aggregate by, `group_by` takes precedence over
    /// `order_by` when determining row order, so that aggregates are processed one by one.
    group_by: Option<Vec<usize>>,
}

impl InsertionOrder<Box<[DfValue]>> for PreInsertion {
    fn cmp(&self, a: &Box<[DfValue]>, b: &Box<[DfValue]>) -> Ordering {
        if let Some(cols) = &self.group_by {
            cols.iter()
                .map(|&idx| a[idx].cmp(&b[idx]))
                .try_fold(Ordering::Equal, |acc, next| match acc {
                    Ordering::Equal => Ok(next),
                    ord => Err(ord),
                })
                .unwrap_or_else(|ord| ord)
                .then(a.cmp(b))
        } else if let Some(indices) = self.order_by.as_deref() {
            indices
                .iter()
                .map(|&(idx, order_type, null_order)| {
                    null_order
                        .apply(a[idx].is_none(), b[idx].is_none())
                        .then(order_type.apply(a[idx].cmp(&b[idx])))
                })
                .try_fold(Ordering::Equal, |acc, next| match acc {
                    Ordering::Equal => Ok(next),
                    ord => Err(ord),
                })
                .unwrap_or_else(|ord| ord)
                .then(a.cmp(b))
        } else {
            a.cmp(b)
        }
    }
}
