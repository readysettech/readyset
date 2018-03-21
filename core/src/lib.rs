#![feature(box_syntax)]
#![feature(conservative_impl_trait)]
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
extern crate serde;
#[macro_use]
extern crate serde_derive;

pub mod addressing;
pub mod data;
pub mod local;
pub mod map;

pub use data::{DataType, Datas, Record, Records};
pub use addressing::{IndexPair, LocalNodeIndex};
pub use local::{KeyType, LookupResult, Row, State, Tag};
pub use map::Map;
pub use petgraph::graph::NodeIndex;

pub type StateMap = map::Map<State>;
