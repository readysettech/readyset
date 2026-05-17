//! Ordering applied to rows as they are inserted into a reader's underlying map.
//!
//! The reader map supports a custom insertion order so that rows for a given
//! lookup key are stored pre-sorted. Later, the k-way merge in
//! [`readyset_client::post_processing::post_lookup`] can produce globally
//! sorted output without re-sorting.

use std::cmp::Ordering;

use partial_map::InsertionOrder;
use readyset_client::post_processing::{PostLookup, PostLookupDistinct};
use readyset_data::DfValue;
use readyset_sql::ast::{NullOrder, OrderType};
use serde::{Deserialize, Serialize};

/// Operations to perform on a row before it is stored in the map in a reader.
#[derive(Serialize, Deserialize, Debug, Clone, Default, Eq, PartialEq)]
pub struct PreInsertion {
    /// Column indices to order by, and whether or not to reverse order on each index.
    ///
    /// If an empty `Vec` is specified, rows are sorted in lexicographic order.
    order_by: Option<Vec<(usize, OrderType, NullOrder)>>,
    /// The set of column indices to group the aggregate by, `group_by` takes precedence over
    /// `order_by` when determining row order, so that aggregates are processed one by one.
    group_by: Option<Vec<usize>>,
}

impl PreInsertion {
    /// Derive the pre-insertion ordering implied by a post-lookup spec.
    ///
    /// - When the post-lookup uses sorted DISTINCT dedup, rows must be stored
    ///   sorted by *all projected columns* so the k-way merge produces globally
    ///   sorted output suitable for streaming dedup.
    /// - Otherwise, rows are grouped by the aggregate's group-by columns so
    ///   aggregation can process groups one at a time.
    pub fn from_post_lookup(post: &PostLookup) -> Self {
        Self {
            order_by: post.order_by.clone(),
            group_by: match &post.distinct {
                PostLookupDistinct::Sorted { dedup_aggregates } => {
                    Some(dedup_aggregates.group_by.clone())
                }
                _ => post.aggregates.as_ref().map(|v| v.group_by.clone()),
            },
        }
    }
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
