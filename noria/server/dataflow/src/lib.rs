#![warn(
    clippy::dbg_macro,
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
pub mod state; // pub for doctests

mod domain;
mod node_map;
mod processing;

use derivative::Derivative;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use thiserror::Error;

pub use crate::backlog::{LookupError, SingleReadHandle};
pub type Readers =
    Arc<Mutex<HashMap<(petgraph::graph::NodeIndex, String, usize), backlog::SingleReadHandle>>>;
pub type DomainConfig = domain::Config;

pub use crate::domain::{Domain, DomainBuilder, Index};
pub use crate::node::special::PostLookup;
pub use crate::node_map::NodeMap;
pub use crate::payload::{DomainRequest, Packet};
pub use crate::processing::SuggestedIndex;

pub use dataflow_expression::{BuiltinFunction, Expression};

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

#[derive(Debug, Error)]
#[error("Invalid durability mode; expected one of persistent, ephemeral, or memory")]
pub struct InvalidDurabilityMode;

impl FromStr for DurabilityMode {
    type Err = InvalidDurabilityMode;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "persistent" => Ok(Self::Permanent),
            "ephemeral" => Ok(Self::DeleteOnExit),
            "memory" => Ok(Self::MemoryOnly),
            _ => Err(InvalidDurabilityMode),
        }
    }
}

/// Parameters to control the operation of GroupCommitQueue.
#[derive(Clone, Debug, Serialize, Deserialize, Derivative)]
#[derivative(PartialEq)]
pub struct PersistenceParameters {
    /// Whether the output files should be deleted when the GroupCommitQueue is dropped.
    pub mode: DurabilityMode,
    /// Filename prefix for the RocksDB database folder
    pub db_filename_prefix: String,
    /// Number of background threads PersistentState can use (shared acrosss all worker threads).
    pub persistence_threads: i32,
}

impl Default for PersistenceParameters {
    fn default() -> Self {
        Self {
            mode: DurabilityMode::MemoryOnly,
            db_filename_prefix: String::from("soup"),
            persistence_threads: 1,
        }
    }
}

impl PersistenceParameters {
    /// Parameters to control the persistence mode, and parameters related to persistence.
    ///
    /// Three modes are available:
    ///
    ///  1. `DurabilityMode::Permanent`: all writes to base nodes should be written to disk.
    ///  2. `DurabilityMode::DeleteOnExit`: all writes to base nodes are written to disk, but the
    ///     persistent files are deleted once the `ControllerHandle` is dropped. Useful for tests.
    ///  3. `DurabilityMode::MemoryOnly`: no writes to disk, store all writes in memory.
    ///     Useful for baseline numbers.
    pub fn new(
        mode: DurabilityMode,
        db_filename_prefix: Option<String>,
        persistence_threads: i32,
    ) -> Self {
        // NOTE(fran): DO NOT impose a particular format on `db_filename_prefix`. If you need to, modify
        // it before use, but do not make assertions on it. The reason being, we use Noria's deployment
        // name as db filename prefix (which makes sense), and we don't want to impose any restriction
        // on it (since sometimes we automate the deployments and deployment name generation).
        let db_filename_prefix = db_filename_prefix.unwrap_or_else(|| String::from("soup"));

        Self {
            mode,
            db_filename_prefix,
            persistence_threads,
        }
    }
}

pub use noria::shard_by;
