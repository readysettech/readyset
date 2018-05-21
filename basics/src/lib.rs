#![feature(allow_fail)]
#![feature(box_syntax)]
#![feature(entry_or_default)]
#![feature(non_modrs_mods)]
#![feature(try_from)]
#![deny(unused_extern_crates)]

extern crate arccstr;
extern crate bincode;
extern crate chrono;
extern crate fnv;
extern crate itertools;
extern crate nom_sql;
extern crate petgraph;
extern crate rahashmap;
extern crate rand;
extern crate rocksdb;
extern crate serde;
#[macro_use]
extern crate serde_derive;

pub mod addressing;
pub mod api;
pub mod data;
pub mod local;
pub mod map;

use std::path::PathBuf;
use std::time;

pub use addressing::{IndexPair, LocalNodeIndex};
pub use api::{Input, Link};
pub use data::{DataType, Datas, Modification, Operation, Record, Records, TableOperation};
pub use local::{
    KeyType, LookupResult, MemoryState, PersistentState, RecordResult, Row, State, Tag,
};
pub use map::Map;
pub use petgraph::graph::NodeIndex;

#[inline]
pub fn shard_by(dt: &DataType, shards: usize) -> usize {
    match *dt {
        DataType::Int(n) => n as usize % shards,
        DataType::BigInt(n) => n as usize % shards,
        DataType::Text(..) | DataType::TinyText(..) => {
            use std::borrow::Cow;
            use std::hash::Hasher;
            let mut hasher = fnv::FnvHasher::default();
            let s: Cow<str> = dt.into();
            hasher.write(s.as_bytes());
            hasher.finish() as usize % shards
        }
        ref x => {
            unimplemented!("asked to shard on value {:?}", x);
        }
    }
}

pub type StateMap = map::Map<Box<State>>;

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
