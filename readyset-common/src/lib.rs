#![deny(macro_use_extern_crate)]

mod collections;
mod local;
mod metrics;
mod records;
pub mod ulimit;
pub mod worker;

use std::mem::{size_of, size_of_val};

use petgraph::prelude::*;
pub use readyset_client::internal::{Index, IndexType};
pub use readyset_data::DfValue;
use serde::{Deserialize, Serialize};

pub use self::local::*;
pub use self::metrics::LenMetric;
pub use self::records::*;

pub trait SizeOf {
    fn deep_size_of(&self) -> usize;
    fn is_empty(&self) -> bool;
}

pub trait Len {
    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl SizeOf for DfValue {
    fn deep_size_of(&self) -> usize {
        let inner = match self {
            DfValue::Text(t) => size_of_val(t) + t.as_bytes().len(),
            DfValue::BitVector(t) => size_of_val(t) + t.len().div_ceil(8),
            DfValue::ByteArray(t) => size_of_val(t) + t.len(),
            _ => 0,
        };

        size_of::<Self>() + inner
    }

    fn is_empty(&self) -> bool {
        false
    }
}

impl<T> SizeOf for Vec<T>
where
    T: SizeOf,
{
    fn deep_size_of(&self) -> usize {
        size_of_val(self) + self.iter().map(|x| x.deep_size_of()).sum::<usize>()
    }

    fn is_empty(&self) -> bool {
        false
    }
}

impl<T> SizeOf for Box<[T]>
where
    T: SizeOf,
{
    fn deep_size_of(&self) -> usize {
        size_of_val(self) + self.iter().map(|x| x.deep_size_of()).sum::<usize>() + 8
    }

    fn is_empty(&self) -> bool {
        false
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
    use super::*;

    #[test]
    fn data_type_mem_size() {
        use chrono::DateTime;

        let s = "this needs to be longer than 14 chars to make it be a Text";
        let txt = DfValue::from(s);
        let shrt = DfValue::Int(5);
        let time = DfValue::TimestampTz(
            DateTime::from_timestamp(0, 42_000_000)
                .unwrap()
                .naive_utc()
                .into(),
        );

        let rec = vec![DfValue::Int(5), "asdfasdfasdfasdf".into(), "asdf".into()];

        // DfValue should always use 16 bytes itself
        assert_eq!(size_of::<DfValue>(), 16);
        assert_eq!(size_of_val(&txt), 16);
        assert_eq!(
            txt.deep_size_of(),
            // DfValue + Arc's ptr + string
            16 + 8 + s.len()
        );
        assert_eq!(size_of_val(&shrt), 16);
        assert_eq!(size_of_val(&time), 16);
        assert_eq!(size_of_val(&time), time.deep_size_of());

        assert_eq!(size_of_val(&rec), 24);
        assert_eq!(rec.deep_size_of(), 24 + 3 * 16 + (8 + 16));
    }
}
