#![feature(nll)]
#![feature(box_syntax)]
#![feature(box_patterns)]
#![feature(duration_as_u128)]
#![feature(if_while_or_patterns)]
#![deny(unused_extern_crates)]

extern crate api;
#[cfg(debug_assertions)]
extern crate backtrace;
extern crate basics;
extern crate bincode;
extern crate channel;
extern crate evmap;
extern crate fnv;
extern crate futures;
extern crate hyper;
extern crate itertools;
extern crate nom_sql;
extern crate petgraph;
extern crate rahashmap;
extern crate rand;
extern crate regex;
extern crate rocksdb;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
#[macro_use]
extern crate slog;
extern crate stream_cancel;
extern crate tempfile;
extern crate timekeeper;
extern crate tokio;
extern crate vec_map;

pub mod backlog;
pub mod node;
pub mod ops;
pub mod payload;
pub mod prelude;
pub mod state;

mod domain;
mod group_commit;
mod processing;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time;

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
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct PersistenceParameters {
    /// Number of elements to buffer before flushing.
    pub queue_capacity: usize,
    /// Amount of time to wait before flushing despite not reaching `queue_capacity`.
    pub flush_timeout: time::Duration,
    /// Whether the output files should be deleted when the GroupCommitQueue is dropped.
    pub mode: DurabilityMode,
    /// Filename prefix for persistent log entries.
    pub log_prefix: String,
    /// Absolute path where the log will be written. Defaults to the current directory.
    pub log_dir: Option<PathBuf>,
    /// Number of background threads PersistentState can use (shared acrosss all worker threads).
    pub persistence_threads: i32,
}

impl Default for PersistenceParameters {
    fn default() -> Self {
        Self {
            queue_capacity: 256,
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
    ///
    /// `queue_capacity` indicates the number of packets that should be buffered until
    /// flushing, and `flush_timeout` indicates the length of time to wait before flushing
    /// anyway.
    pub fn new(
        mode: DurabilityMode,
        queue_capacity: usize,
        flush_timeout: time::Duration,
        log_prefix: Option<String>,
        persistence_threads: i32,
    ) -> Self {
        let log_prefix = log_prefix.unwrap_or(String::from("soup"));
        assert!(!log_prefix.contains("-"));

        Self {
            queue_capacity,
            flush_timeout,
            mode,
            log_prefix,
            persistence_threads,
            ..Default::default()
        }
    }
}

pub use basics::shard_by;
