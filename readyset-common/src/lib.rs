#![deny(macro_use_extern_crate)]
#![feature(bound_map)]

mod local;
mod records;
pub mod ulimit;

use std::mem::{size_of, size_of_val};

use bit_vec::BitVec;
use petgraph::prelude::*;
pub use readyset_client::internal::{Index, IndexType};
pub use readyset_data::{Array, DfValue, PassThrough, Text, TinyText};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use vec1::Vec1;

pub use self::local::*;
pub use self::records::*;

/// Overhead of ArcInner<T> structure
/// for strong and weak counters
const ARC_INNER_SIZE: u64 = 2 * size_of::<usize>() as u64;

pub trait SizeOf {
    fn deep_size_of(&self) -> u64;
    fn size_of(&self) -> u64;
    fn is_empty(&self) -> bool;
}

impl SizeOf for DfValue {
    fn deep_size_of(&self) -> u64 {
        let inner = match *self {
            DfValue::Text(ref t) => t.deep_size_of(),
            DfValue::BitVector(ref t) => ARC_INNER_SIZE + t.deep_size_of(),
            DfValue::ByteArray(ref t) => {
                ARC_INNER_SIZE
                // to account for Vec<u8>
                + size_of::<Vec<u8>>() as u64
                // use capacity instead of length
                + t.capacity() as u64
            }
            DfValue::Numeric(_) => {
                ARC_INNER_SIZE
                // `Decimal` size - 16 bytes
                + size_of::<Decimal>() as u64
            }
            DfValue::Array(ref t) => {
                ARC_INNER_SIZE
                // SmallVec size
                + t.deep_size_of()
            }
            DfValue::PassThrough(ref t) => ARC_INNER_SIZE + t.deep_size_of(),
            _ => 0u64,
        };

        self.size_of() + inner
    }

    fn size_of(&self) -> u64 {
        // doesn't include data if stored externally
        size_of::<DfValue>() as u64
    }

    fn is_empty(&self) -> bool {
        false
    }
}

impl SizeOf for Vec<DfValue> {
    fn deep_size_of(&self) -> u64 {
        let mut size =
            size_of_val(self) as u64 + self.iter().fold(0u64, |acc, d| acc + d.deep_size_of());
        // To account for vector overallocation
        size += (self.capacity() - self.len()) as u64 * size_of::<DfValue>() as u64;
        size
    }

    fn size_of(&self) -> u64 {
        size_of_val(self) as u64 + size_of::<DfValue>() as u64 * self.len() as u64
    }

    fn is_empty(&self) -> bool {
        false
    }
}

impl SizeOf for Box<[DfValue]> {
    fn deep_size_of(&self) -> u64 {
        size_of_val(self) as u64 + self.iter().fold(0u64, |acc, d| acc + d.deep_size_of()) + 8
    }

    fn size_of(&self) -> u64 {
        size_of_val(self) as u64 + size_of::<DfValue>() as u64 * self.len() as u64
    }

    fn is_empty(&self) -> bool {
        false
    }
}

impl SizeOf for PassThrough {
    fn deep_size_of(&self) -> u64 {
        size_of::<PassThrough>() as u64 + size_of_val::<[u8]>(&self.data) as u64
    }

    fn size_of(&self) -> u64 {
        size_of::<PassThrough>() as u64
    }

    fn is_empty(&self) -> bool {
        false
    }
}

impl SizeOf for Array {
    fn deep_size_of(&self) -> u64 {
        size_of::<Array>() as u64
        // SmallVec size
        + 2 * size_of::<usize>() as u64
        + self.num_dimensions() as u64 * size_of::<i32>() as u64
        // estimate for ndarray data size
        + self.values().map(|d| d.deep_size_of()).sum::<u64>()
    }

    fn size_of(&self) -> u64 {
        size_of::<Array>() as u64
    }

    fn is_empty(&self) -> bool {
        false
    }
}

impl SizeOf for Text {
    fn deep_size_of(&self) -> u64 {
        // Size of triomphe::thin_arc::ThinArc
        size_of::<usize>() as u64
        // Size of triomphe::thin_arc::ArcInner - not public outside of the crate
        + 2 * size_of::<usize>() as u64
        // Size of triomphe::header::HeaderSliceWithLength - not public outside of a crate
        + (2 * size_of::<usize>() + 1) as u64
        // Text size
        + self.as_bytes().len() as u64
    }

    fn size_of(&self) -> u64 {
        size_of::<Text>() as u64
    }

    fn is_empty(&self) -> bool {
        false
    }
}

impl SizeOf for BitVec {
    fn deep_size_of(&self) -> u64 {
        // to account for BitVec size
        size_of::<BitVec>() as u64
        // get raw storage
        + size_of_val(self.storage()) as u64
    }

    fn size_of(&self) -> u64 {
        size_of::<BitVec>() as u64
    }

    fn is_empty(&self) -> bool {
        false
    }
}

impl<T: SizeOf> SizeOf for Vec1<T> {
    fn deep_size_of(&self) -> u64 {
        let mut size = size_of::<Vec1<T>>() as u64;
        size += self.iter().map(SizeOf::deep_size_of).sum::<u64>()
            + ((self.capacity() - self.len()) as u64) * (size_of::<T>() as u64);
        size
    }

    fn size_of(&self) -> u64 {
        size_of::<Vec1<T>>() as u64
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
            // DfValue + overhead + string
            txt.size_of() + 41 + (s.len() as u64)
        );
        assert_eq!(size_of_val(&shrt), 16);
        assert_eq!(size_of_val(&time), 16);
        assert_eq!(size_of_val(&time) as u64, time.size_of());
        assert_eq!(time.deep_size_of(), 16); // DfValue + inline NaiveDateTime

        assert_eq!(size_of_val(&rec), 24);
        assert_eq!(rec.size_of(), 24 + 3 * 16);
        assert_eq!(rec.deep_size_of(), 24 + 3 * 16 + (41 + 16));
    }
}
