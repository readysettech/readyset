use std::cmp::{self, Ordering};
use std::convert::TryFrom;
use std::fmt::Debug;
use std::sync::Arc;

use nom_sql::OrderType;
use partial_map::InsertionOrder;
use readyset_data::DfValue;
use readyset_errors::{internal, ReadySetResult};
use serde::{Deserialize, Serialize};

/// Representation of an aggregate function
// TODO(grfn): It would be really nice to deduplicate this somehow with the grouped operator itself
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub enum PostLookupAggregateFunction {
    /// Add together all the input numbers
    ///
    /// Note that this encapsulates both `SUM` *and* `COUNT` in base SQL, as re-aggregating counts
    /// is done by just summing numbers together
    Sum,
    /// Multiply together all the input numbers
    Product,
    /// Concatenate together all the input strings with the given separator
    GroupConcat { separator: String },
    /// Take the maximum input value
    Max,
    /// Take the minimum input value
    Min,
}

impl PostLookupAggregateFunction {
    /// Apply this aggregate function to the two input values
    ///
    /// This forms a semigroup.
    pub fn apply(&self, val1: &DfValue, val2: &DfValue) -> ReadySetResult<DfValue> {
        match self {
            PostLookupAggregateFunction::Sum => val1 + val2,
            PostLookupAggregateFunction::Product => val1 * val2,
            PostLookupAggregateFunction::GroupConcat { separator } => Ok(format!(
                "{}{}{}",
                String::try_from(val1)?,
                separator,
                String::try_from(val2)?
            )
            .into()),
            PostLookupAggregateFunction::Max => Ok(cmp::max(val1, val2).clone()),
            PostLookupAggregateFunction::Min => Ok(cmp::min(val1, val2).clone()),
        }
    }
}

/// Representation of a single aggregate function to be performed on a column post-lookup
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct PostLookupAggregate<Column = usize> {
    /// The column index in the result set containing the already-aggregated values
    pub column: Column,
    /// The aggregate function to perform
    pub function: PostLookupAggregateFunction,
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
        })
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
        order_by: Option<Vec<(usize, OrderType)>>,
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
    pub order_by: Option<Vec<(usize, OrderType)>>,
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
    order_by: Option<Vec<(usize, OrderType)>>,
    /// The set of column indices to group the aggregate by, `group_by` takes precedence over
    /// `order_by` when determining row order, so that aggregates are processed one by one.
    group_by: Option<Vec<usize>>,
}

impl InsertionOrder<Box<[DfValue]>> for PreInsertion {
    fn get_insertion_order(
        &self,
        values: &[Box<[DfValue]>],
        elem: &Box<[DfValue]>,
    ) -> Result<usize, usize> {
        if let Some(cols) = &self.group_by {
            values.binary_search_by(|cur_row| {
                cols.iter()
                    .map(|&idx| cur_row[idx].cmp(&elem[idx]))
                    .try_fold(Ordering::Equal, |acc, next| match acc {
                        Ordering::Equal => Ok(next),
                        ord => Err(ord),
                    })
                    .unwrap_or_else(|ord| ord)
                    .then(cur_row.cmp(elem))
            })
        } else if let Some(indices) = self.order_by.as_deref() {
            values.binary_search_by(|cur_row| {
                indices
                    .iter()
                    .map(|&(idx, order_type)| order_type.apply(cur_row[idx].cmp(&elem[idx])))
                    .try_fold(Ordering::Equal, |acc, next| match acc {
                        Ordering::Equal => Ok(next),
                        ord => Err(ord),
                    })
                    .unwrap_or_else(|ord| ord)
                    .then(cur_row.cmp(elem))
            })
        } else {
            values.binary_search(elem)
        }
    }
}
