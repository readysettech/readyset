use std::cmp::Ordering;
use std::fmt::Display;

use itertools::Itertools;
use nom_sql::OrderType;
use serde::{Deserialize, Serialize};

use crate::prelude::DataType;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct Order(Vec<(usize, OrderType)>);
impl Order {
    pub(crate) fn cmp(&self, a: &[DataType], b: &[DataType]) -> Ordering {
        for &(c, ref order_type) in &self.0 {
            let result = match *order_type {
                OrderType::OrderAscending => a[c].cmp(&b[c]),
                OrderType::OrderDescending => b[c].cmp(&a[c]),
            };
            if result != Ordering::Equal {
                return result;
            }
        }
        Ordering::Equal
    }
}

impl From<Vec<(usize, OrderType)>> for Order {
    fn from(other: Vec<(usize, OrderType)>) -> Self {
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
                .map(|(c, dir)| {
                    format!(
                        "{}{}",
                        match dir {
                            OrderType::OrderAscending => "<",
                            OrderType::OrderDescending => ">",
                        },
                        c,
                    )
                })
                .join(", "),
        )
    }
}
