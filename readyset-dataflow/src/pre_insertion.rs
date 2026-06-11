use std::cmp::Ordering;

use partial_map::InsertionOrder;
use readyset_data::DfValue;
use readyset_sql::ast::{NullOrder, OrderType};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, Default, Eq, PartialEq)]
/// Operations to perform on a row before it is stored in the map in a reader.
pub struct PreInsertion {
    /// Column indices to order by, and whether or not to reverse order on each index.
    ///
    /// If an empty `Vec` is specified, rows are sorted in lexicographic order.
    pub(crate) order_by: Option<Vec<(usize, OrderType, NullOrder)>>,
    /// The set of column indices to group the aggregate by, `group_by` takes precedence over
    /// `order_by` when determining row order, so that aggregates are processed one by one.
    pub(crate) group_by: Option<Vec<usize>>,
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
