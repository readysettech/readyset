#![warn(clippy::dbg_macro)]
#![feature(
    bound_cloned,
    iter_order_by,
    trait_alias,
    btree_drain_filter,
    or_patterns,
    option_result_contains
)]
// Only used in a `debug_assert!` in `ops/grouped/mod.rs` therefore I added it
// conditionally to avoid requiring another unstable feature for release builds.
#![cfg_attr(debug, feature(is_sorted))]
#![deny(unused_extern_crates)]
#![allow(clippy::redundant_closure)]

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate slog;
#[macro_use]
extern crate lazy_static;

pub(crate) mod backlog;
pub mod node;
pub mod ops;
pub mod payload; // it makes me _really_ sad that this has to be pub
pub mod prelude;
pub mod state; // pub for doctests

mod domain;
mod group_commit;
mod processing;

use derivative::Derivative;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time;

pub use crate::backlog::{LookupError, SingleReadHandle};
pub use msql_srv::MysqlTime;
pub type Readers =
    Arc<Mutex<HashMap<(petgraph::graph::NodeIndex, usize), backlog::SingleReadHandle>>>;
pub type DomainConfig = domain::Config;

pub use crate::domain::{Domain, DomainBuilder, Index, PollEvent, ProcessResult};
pub use crate::payload::{DomainRequest, Packet};

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

/// Parameters to control the operation of GroupCommitQueue.
#[derive(Clone, Debug, Serialize, Deserialize, Derivative)]
#[derivative(PartialEq)]
pub struct PersistenceParameters {
    /// Force a flush if packets have been in the base table queue for this long.
    pub flush_timeout: time::Duration,
    /// Whether the output files should be deleted when the GroupCommitQueue is dropped.
    pub mode: DurabilityMode,
    /// Filename prefix for persistent log entries.
    pub log_prefix: String,
    /// Absolute path where the log will be written. Defaults to the current directory.
    #[derivative(PartialEq = "ignore")]
    pub log_dir: Option<PathBuf>,
    /// Number of background threads PersistentState can use (shared acrosss all worker threads).
    pub persistence_threads: i32,
}

impl Default for PersistenceParameters {
    fn default() -> Self {
        Self {
            flush_timeout: time::Duration::new(0, 100_000),
            mode: DurabilityMode::MemoryOnly,
            log_prefix: String::from("soup"),
            log_dir: None,
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
        flush_timeout: time::Duration,
        log_prefix: Option<String>,
        persistence_threads: i32,
    ) -> Self {
        let log_prefix = log_prefix.unwrap_or_else(|| String::from("soup"));
        assert!(!log_prefix.contains('-'));

        Self {
            flush_timeout,
            mode,
            log_prefix,
            persistence_threads,
            ..Default::default()
        }
    }
}

pub use noria::shard_by;
