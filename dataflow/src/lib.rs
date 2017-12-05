#![feature(box_syntax)]
#![feature(box_patterns)]
#![feature(use_extern_macros)]
#![feature(entry_or_default)]
#![feature(plugin, use_extern_macros)]
#![plugin(tarpc_plugins)]
#![deny(unused_extern_crates)]

#[allow(unused_extern_crates)]
extern crate backtrace;
extern crate buf_redux;
extern crate channel;
extern crate core;
extern crate evmap;
extern crate fnv;
extern crate itertools;
extern crate nom_sql;
extern crate petgraph;
extern crate regex;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
#[macro_use]
extern crate slog;
#[macro_use]
extern crate tarpc;
extern crate timekeeper;
extern crate tokio_core;
extern crate vec_map;
extern crate hyper;
extern crate futures;

pub mod backlog;
pub mod checktable;
pub mod debug;
pub mod node;
pub mod ops;
pub mod payload;
pub mod prelude;
pub mod statistics;

mod domain;
mod persistence;
mod processing;
mod transactions;

use std::sync::{Arc, Mutex};
use std::collections::HashMap;

use checktable::TokenGenerator;

pub type Readers = Arc<
    Mutex<HashMap<(core::NodeIndex, usize), (backlog::SingleReadHandle, Option<TokenGenerator>)>>,
>;
pub type PersistenceParameters = persistence::Parameters;
pub type DomainConfig = domain::Config;

pub use persistence::DurabilityMode;
pub use domain::{Domain, DomainBuilder, Index};
pub use payload::Packet;
pub use checktable::connect_thread_checktable;

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

#[inline]
pub fn shard_by(dt: &core::DataType, shards: usize) -> usize {
    match *dt {
        core::DataType::Int(n) => n as usize % shards,
        core::DataType::BigInt(n) => n as usize % shards,
        core::DataType::Text(..) | core::DataType::TinyText(..) => {
            use std::hash::Hasher;
            use std::borrow::Cow;
            let mut hasher = fnv::FnvHasher::default();
            let s: Cow<str> = dt.into();
            hasher.write(s.as_bytes());
            hasher.finish() as usize % shards
        }
        ref x => {
            println!("asked to shard on value {:?}", x);
            unimplemented!();
        }
    }
}
