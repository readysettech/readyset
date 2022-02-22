#![warn(
    clippy::unimplemented,
    clippy::unreachable,
    clippy::panic,
    clippy::todo
)]
#![feature(
    iter_order_by,
    binary_heap_retain,
    trait_alias,
    btree_drain_filter,
    option_result_contains,
    bound_as_ref,
    bound_map,
    stmt_expr_attributes,
    drain_filter,
    hash_drain_filter,
    option_get_or_insert_default
)]
// Only used in a `debug_assert!` in `ops/grouped/mod.rs` therefore I added it
// conditionally to avoid requiring another unstable feature for release builds.
#![cfg_attr(debug, feature(is_sorted))]
#![deny(unused_extern_crates, macro_use_extern_crate)]
#![allow(clippy::redundant_closure)]

pub(crate) mod backlog;
pub mod node;
pub mod ops;
pub mod payload; // it makes me _really_ sad that this has to be pub
pub mod prelude;
pub mod utils;

mod domain;
mod node_map;
mod processing;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use nom_sql::SqlIdentifier;
use serde::{Deserialize, Serialize};

pub use crate::backlog::{LookupError, SingleReadHandle};
pub type Readers = Arc<
    Mutex<HashMap<(petgraph::graph::NodeIndex, SqlIdentifier, usize), backlog::SingleReadHandle>>,
>;
pub type DomainConfig = domain::Config;

pub use dataflow_expression::{BuiltinFunction, Expression};
pub use dataflow_state::{DurabilityMode, PersistenceParameters};

pub use crate::domain::{Domain, DomainBuilder, DomainIndex};
pub use crate::node::special::reader::post_lookup;
pub use crate::node_map::NodeMap;
pub use crate::payload::{DomainRequest, Packet, PacketDiscriminants};
pub use crate::processing::LookupIndex;

#[derive(Copy, Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum Sharding {
    None,
    ForcedNone,
    Random(usize),
    ByColumn(usize, usize),
}

impl Sharding {
    pub fn is_none(&self) -> bool {
        matches!(*self, Sharding::None | Sharding::ForcedNone)
    }

    pub fn shards(&self) -> Option<usize> {
        match *self {
            Sharding::None | Sharding::ForcedNone => None,
            Sharding::Random(shards) | Sharding::ByColumn(_, shards) => Some(shards),
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, clap::ArgEnum)]
pub enum EvictionKind {
    Random,
    LRU,
    Generational,
}

impl Default for EvictionKind {
    fn default() -> Self {
        EvictionKind::Random
    }
}

pub use noria::shard_by;
