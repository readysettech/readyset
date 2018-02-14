#![feature(box_syntax)]
#![feature(entry_or_default)]
#![feature(non_modrs_mods)]
#![feature(try_from)]
#![deny(unused_extern_crates)]

extern crate arccstr;
extern crate chrono;
extern crate fnv;
extern crate nom_sql;
extern crate petgraph;
extern crate rahashmap;
extern crate rand;
extern crate rusqlite;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;

pub mod addressing;
pub mod data;
pub mod local;
pub mod map;

pub use addressing::{IndexPair, LocalNodeIndex};
pub use data::{BaseOperation, DataType, Datas, Modification, Operation, Record, Records};
pub use local::{KeyType, LookupResult, MemoryState, Row, State, Tag};
pub use map::Map;
pub use petgraph::graph::NodeIndex;

pub type StateMap = map::Map<State>;
