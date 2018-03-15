#![feature(box_syntax)]
#![feature(entry_or_default)]
#![feature(non_modrs_mods)]
#![feature(try_from)]
#![deny(unused_extern_crates)]

extern crate arccstr;
extern crate bincode;
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

pub mod addressing;
pub mod data;
pub mod local;
pub mod map;

pub use addressing::{IndexPair, LocalNodeIndex};
pub use data::{BaseOperation, DataType, Datas, Modification, Operation, Record, Records};
pub use local::{KeyType, LookupResult, Row, State, Tag};
pub use map::Map;
pub use petgraph::graph::NodeIndex;

pub type StateMap = map::Map<State>;

/// Indicates to what degree updates should be persisted.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum DurabilityMode {
    /// Don't do any durability
    MemoryOnly,
    /// Delete any log files on exit. Useful mainly for tests.
    DeleteOnExit,
    /// Persist updates to disk, and don't delete them later.
    Permanent,
}
