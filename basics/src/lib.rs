#![feature(try_from)]
#![deny(unused_extern_crates)]

extern crate arccstr;
extern crate chrono;
extern crate fnv;
extern crate nom_sql;
extern crate petgraph;
extern crate serde;
#[macro_use]
extern crate serde_derive;

pub mod addressing;
pub mod data;
pub mod external;
pub mod local;
pub mod map;

pub use addressing::{IndexPair, LocalNodeIndex};
pub use data::{DataType, Datas, Modification, Operation, Record, Records, TableOperation};
pub use external::{Link, MaterializationStatus};
pub use local::{DomainIndex, KeyType, Tag};
pub use map::Map;
pub use petgraph::graph::NodeIndex;

#[inline]
pub fn shard_by(dt: &DataType, shards: usize) -> usize {
    match *dt {
        DataType::Int(n) => n as usize % shards,
        DataType::BigInt(n) => n as usize % shards,
        DataType::Text(..) | DataType::TinyText(..) => {
            use std::borrow::Cow;
            use std::hash::Hasher;
            let mut hasher = fnv::FnvHasher::default();
            let s: Cow<str> = dt.into();
            hasher.write(s.as_bytes());
            hasher.finish() as usize % shards
        }
        // a bit hacky: send all NULL values to the first shard
        DataType::None => 0,
        ref x => {
            unimplemented!("asked to shard on value {:?}", x);
        }
    }
}
