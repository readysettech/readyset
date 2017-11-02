#![feature(box_syntax)]
#![feature(conservative_impl_trait)]
#![feature(entry_or_default)]
#![feature(try_from)]

extern crate arccstr;
extern crate backtrace;
extern crate channel;
extern crate chrono;
extern crate fnv;
extern crate petgraph;
extern crate nom_sql;
extern crate regex;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate vec_map;

pub mod addressing;
pub mod data;
pub mod local;

pub use data::{DataType, Datas, Record, Records};
pub use addressing::{IndexPair, LocalNodeIndex};
pub use local::{KeyType, LookupResult, Map, Row, Tag};
pub use petgraph::graph::NodeIndex;

pub type State = local::State<DataType>;
pub type StateMap = local::Map<State>;
