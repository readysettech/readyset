#![warn(clippy::dbg_macro)]
#![deny(macro_use_extern_crate)]
#![feature(bound_map)]

mod local;
mod records;

pub use self::local::*;
pub use self::records::*;
pub use noria::internal::{Index, IndexType};
pub use noria_data::DataType;
use petgraph::prelude::*;
use serde::{Deserialize, Serialize};
use vec1::Vec1;

pub trait SizeOf {
    fn deep_size_of(&self) -> u64;
    fn size_of(&self) -> u64;
    fn is_empty(&self) -> bool;
}

impl SizeOf for DataType {
    fn deep_size_of(&self) -> u64 {
        use std::mem::size_of_val;

        let inner = match *self {
            DataType::Text(ref t) => size_of_val(t) as u64 + t.as_bytes().len() as u64,
            DataType::BitVector(ref t) => size_of_val(t) as u64 + (t.len() as u64 + 7) / 8,
            DataType::ByteArray(ref t) => size_of_val(t) as u64 + t.len() as u64,
            _ => 0u64,
        };

        self.size_of() + inner
    }

    fn size_of(&self) -> u64 {
        use std::mem::size_of;

        // doesn't include data if stored externally
        size_of::<DataType>() as u64
    }

    fn is_empty(&self) -> bool {
        false
    }
}

impl SizeOf for Vec<DataType> {
    fn deep_size_of(&self) -> u64 {
        use std::mem::size_of_val;

        size_of_val(self) as u64 + self.iter().fold(0u64, |acc, d| acc + d.deep_size_of())
    }

    fn size_of(&self) -> u64 {
        use std::mem::{size_of, size_of_val};

        size_of_val(self) as u64 + size_of::<DataType>() as u64 * self.len() as u64
    }

    fn is_empty(&self) -> bool {
        false
    }
}

/// A reference to a node, and potentially some columns.
///
/// The columns are only included if partial materialization is possible; if they're present,
/// they determine the index required on the node to make partial materialization work.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct OptColumnRef {
    /// The index of the node being referenced.
    pub node: NodeIndex,
    /// If partial materialization is possible, the index required for partial materialization.
    pub cols: Option<Vec1<usize>>,
}

impl OptColumnRef {
    /// Make a new `OptColumnRef` with a column index.
    pub fn partial(node: NodeIndex, cols: Vec1<usize>) -> Self {
        Self {
            node,
            cols: Some(cols),
        }
    }

    /// Make a new `OptColumnRef` with just a node.
    pub fn full(node: NodeIndex) -> Self {
        Self { node, cols: None }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::TryInto;

    #[test]
    fn data_type_mem_size() {
        use chrono::NaiveDateTime;
        use std::mem::{size_of, size_of_val};

        let s = "this needs to be longer than 14 chars to make it be a Text";
        let txt: DataType = DataType::from(s);
        let shrt = DataType::Int(5);
        let long = DataType::BigInt(5);
        let time = DataType::Timestamp(NaiveDateTime::from_timestamp(0, 42_000_000));

        let rec = vec![
            DataType::Int(5),
            "asdfasdfasdfasdf".try_into().unwrap(),
            "asdf".try_into().unwrap(),
        ];

        // DataType should always use 16 bytes itself
        assert_eq!(size_of::<DataType>(), 16);
        assert_eq!(size_of_val(&txt), 16);
        assert_eq!(size_of_val(&txt) as u64, txt.size_of());
        assert_eq!(
            txt.deep_size_of(),
            // DataType + Arc's ptr + string
            txt.size_of() + 8 + (s.len() as u64)
        );
        assert_eq!(size_of_val(&shrt), 16);
        assert_eq!(size_of_val(&long), 16);
        assert_eq!(size_of_val(&time), 16);
        assert_eq!(size_of_val(&time) as u64, time.size_of());
        assert_eq!(time.deep_size_of(), 16); // DataType + inline NaiveDateTime

        assert_eq!(size_of_val(&rec), 24);
        assert_eq!(rec.size_of(), 24 + 3 * 16);
        assert_eq!(rec.deep_size_of(), 24 + 3 * 16 + (8 + 16));
    }
}
