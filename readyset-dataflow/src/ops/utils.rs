use std::cmp::Ordering;
use std::fmt::Display;

use itertools::Itertools;
use readyset_sql::ast::{NullOrder, OrderType};
use serde::{Deserialize, Serialize};

use crate::prelude::DfValue;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct Order(Vec<(usize, OrderType, NullOrder)>);

impl Order {
    pub(crate) fn cmp(&self, a: &[DfValue], b: &[DfValue]) -> Ordering {
        for &(c, ref order_type, ref null_order) in &self.0 {
            let result = null_order
                .apply(a[c].is_none(), b[c].is_none())
                .then(order_type.apply(a[c].cmp(&b[c])));

            if result != Ordering::Equal {
                return result;
            }
        }

        Ordering::Equal
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn columns(&self) -> Vec<usize> {
        self.0.iter().map(|&(c, _, _)| c).collect()
    }
}

impl From<Vec<(usize, OrderType, NullOrder)>> for Order {
    fn from(other: Vec<(usize, OrderType, NullOrder)>) -> Self {
        Order(other)
    }
}

impl Display for Order {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            self.0
                .iter()
                .map(|(c, dir, no)| {
                    format!(
                        "{}{}({})",
                        match dir {
                            OrderType::OrderAscending => "<",
                            OrderType::OrderDescending => ">",
                        },
                        c,
                        match no {
                            NullOrder::NullsFirst => "NULLS FIRST",
                            NullOrder::NullsLast => "NULLS LAST",
                        }
                    )
                })
                .join(", "),
        )
    }
}
