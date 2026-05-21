#![deny(macro_use_extern_crate)]

mod collections;
mod local;
mod metrics;
mod records;
pub mod ulimit;
pub mod worker;

use petgraph::prelude::*;
pub use readyset_client::internal::{Index, IndexType};
pub use readyset_data::DfValue;
use serde::{Deserialize, Serialize};

pub use self::local::*;
pub use self::metrics::LenMetric;
pub use self::records::*;

pub trait Len {
    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// A reference to a node, and potentially a partial index on that node
///
/// The index is only included if partial materialization is possible; if it's present, it
/// determines the index required on the node to make partial materialization work.

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct IndexRef {
    /// The index of the node being referenced.
    pub node: NodeIndex,
    /// If partial materialization is possible, the index required for partial materialization.
    pub index: Option<Index>,
}

impl IndexRef {
    /// Make a new `IndexRef` with a partial index.
    pub fn partial(node: NodeIndex, index: Index) -> Self {
        Self {
            node,
            index: Some(index),
        }
    }

    /// Make a new `IndexRef` with just a node.
    pub fn full(node: NodeIndex) -> Self {
        Self { node, index: None }
    }
}

#[cfg(test)]
mod tests {
    use std::mem::{size_of, size_of_val};

    use chrono::DateTime;
    use readyset_util::SizeOf;

    use super::*;

    #[test]
    fn data_type_mem_size() {
        const DFVALUE_SIZE: usize = size_of::<DfValue>();

        let s = "this needs to be longer than 14 chars to make it be a Text";
        let txt = DfValue::from(s);
        let time = DfValue::TimestampTz(
            DateTime::from_timestamp(0, 42_000_000)
                .unwrap()
                .naive_utc()
                .into(),
        );

        let rec = vec![DfValue::Int(5), "asdfasdfasdfasdf".into(), "asdf".into()];

        assert_eq!(
            txt.deep_size_of(),
            // DfValue + Arc's ptr + string
            DFVALUE_SIZE + 8 + s.len()
        );
        assert_eq!(size_of_val(&time), time.deep_size_of());

        assert_eq!(size_of_val(&rec), 24);
        assert_eq!(rec.deep_size_of(), 24 + 3 * DFVALUE_SIZE + (8 + 16));
    }
}
