#![deny(macro_use_extern_crate)]
#![feature(bound_map)]

mod local;
mod records;
pub mod ulimit;

use petgraph::prelude::*;
pub use readyset_client::internal::{Index, IndexType};
pub use readyset_data::DfValue;
use serde::{Deserialize, Serialize};

pub use self::local::*;
pub use self::records::*;

pub trait SizeOf {
    fn deep_size_of(&self) -> u64;
    fn size_of(&self) -> u64;
    fn is_empty(&self) -> bool;
}

impl SizeOf for DfValue {
    fn deep_size_of(&self) -> u64 {
        use std::mem::size_of_val;

        let inner = match *self {
            DfValue::Text(ref t) => size_of_val(t) as u64 + t.as_bytes().len() as u64,
            DfValue::BitVector(ref t) => size_of_val(t) as u64 + (t.len() as u64 + 7) / 8,
            DfValue::ByteArray(ref t) => size_of_val(t) as u64 + t.len() as u64,
            _ => 0u64,
        };

        self.size_of() + inner
    }

    fn size_of(&self) -> u64 {
        use std::mem::size_of;

        // doesn't include data if stored externally
        size_of::<DfValue>() as u64
    }

    fn is_empty(&self) -> bool {
        false
    }
}

impl SizeOf for Vec<DfValue> {
    fn deep_size_of(&self) -> u64 {
        use std::mem::size_of_val;

        size_of_val(self) as u64 + self.iter().fold(0u64, |acc, d| acc + d.deep_size_of())
    }

    fn size_of(&self) -> u64 {
        use std::mem::{size_of, size_of_val};

        size_of_val(self) as u64 + size_of::<DfValue>() as u64 * self.len() as u64
    }

    fn is_empty(&self) -> bool {
        false
    }
}

impl SizeOf for Box<[DfValue]> {
    fn deep_size_of(&self) -> u64 {
        use std::mem::size_of_val;

        size_of_val(self) as u64 + self.iter().fold(0u64, |acc, d| acc + d.deep_size_of()) + 8
    }

    fn size_of(&self) -> u64 {
        use std::mem::{size_of, size_of_val};

        size_of_val(self) as u64 + size_of::<DfValue>() as u64 * self.len() as u64
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
    use std::convert::TryInto;

    use super::*;

    #[test]
    fn data_type_mem_size() {
        use std::mem::{size_of, size_of_val};

        use chrono::NaiveDateTime;

        let s = "this needs to be longer than 14 chars to make it be a Text";
        let txt: DfValue = DfValue::from(s);
        let shrt = DfValue::Int(5);
        let time = DfValue::TimestampTz(NaiveDateTime::from_timestamp(0, 42_000_000).into());

        let rec = vec![
            DfValue::Int(5),
            "asdfasdfasdfasdf".try_into().unwrap(),
            "asdf".try_into().unwrap(),
        ];

        // DfValue should always use 16 bytes itself
        assert_eq!(size_of::<DfValue>(), 16);
        assert_eq!(size_of_val(&txt), 16);
        assert_eq!(size_of_val(&txt) as u64, txt.size_of());
        assert_eq!(
            txt.deep_size_of(),
            // DfValue + Arc's ptr + string
            txt.size_of() + 8 + (s.len() as u64)
        );
        assert_eq!(size_of_val(&shrt), 16);
        assert_eq!(size_of_val(&time), 16);
        assert_eq!(size_of_val(&time) as u64, time.size_of());
        assert_eq!(time.deep_size_of(), 16); // DfValue + inline NaiveDateTime

        assert_eq!(size_of_val(&rec), 24);
        assert_eq!(rec.size_of(), 24 + 3 * 16);
        assert_eq!(rec.deep_size_of(), 24 + 3 * 16 + (8 + 16));
    }
}
