#![feature(nll)]
#![feature(box_syntax)]
#![feature(box_patterns)]
#![feature(option_filter)]
#![feature(use_extern_macros)]
#![feature(entry_or_default)]
#![feature(if_while_or_patterns)]
#![feature(use_extern_macros)]
#![feature(duration_from_micros)]
#![feature(proc_macro_path_invoc)]
#![deny(unused_extern_crates)]

#[allow(unused_extern_crates)]
extern crate backtrace;
extern crate basics;
extern crate channel;
extern crate evmap;
extern crate fnv;
extern crate hyper;
extern crate itertools;
extern crate nom_sql;
extern crate petgraph;
extern crate rand;
extern crate regex;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
#[macro_use]
extern crate slog;
extern crate timekeeper;
extern crate tokio_core;
extern crate vec_map;

pub mod backlog;
pub mod debug;
pub mod node;
pub mod ops;
pub mod payload;
pub mod prelude;
pub mod statistics;

mod domain;
mod group_commit;
mod processing;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub type Readers = Arc<Mutex<HashMap<(basics::NodeIndex, usize), backlog::SingleReadHandle>>>;
pub type DomainConfig = domain::Config;

pub use domain::{Domain, DomainBuilder, Index};
pub use payload::{LocalBypass, Packet};

#[derive(Copy, Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum Sharding {
    None,
    ForcedNone,
    Random(usize),
    ByColumn(usize, usize),
}

impl Sharding {
    pub fn is_none(&self) -> bool {
        match *self {
            Sharding::None | Sharding::ForcedNone => true,
            _ => false,
        }
    }

    pub fn shards(&self) -> usize {
        match *self {
            Sharding::None | Sharding::ForcedNone => 1,
            Sharding::Random(shards) | Sharding::ByColumn(_, shards) => shards,
        }
    }
}

pub use basics::shard_by;
