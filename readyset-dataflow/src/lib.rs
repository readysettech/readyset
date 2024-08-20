#![warn(
    clippy::unimplemented,
    clippy::unreachable,
    clippy::panic,
    clippy::todo
)]
#![deny(unused_extern_crates, macro_use_extern_crate)]
#![allow(clippy::redundant_closure)]

#[macro_use]
mod processing;

pub(crate) mod backlog;
pub mod node;
pub mod ops;
pub mod payload; // it makes me _really_ sad that this has to be pub
pub mod prelude;
pub mod utils;

mod domain;
mod node_map;

use std::collections::{HashMap, VecDeque};
use std::fmt::{self, Display};
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};

use readyset_client::ReaderAddress;
use serde::{Deserialize, Serialize};

pub use crate::backlog::{LookupError, ReaderUpdatedNotifier, SingleReadHandle};

/// A [`ReaderMap`] maps a [`ReaderAddress`] to the [`SingleReadHandle`] to access the reader at
/// that address.
#[repr(transparent)]
#[derive(Default, Clone)]
pub struct ReaderMap(HashMap<ReaderAddress, SingleReadHandle>);
pub type Readers = Arc<Mutex<ReaderMap>>;

pub type DomainConfig = domain::Config;

pub use dataflow_expression::{
    BinaryOperator, BuiltinFunction, Expr, LowerContext, PostLookup, PostLookupAggregate,
    PostLookupAggregateFunction, PostLookupAggregates, ReaderProcessing,
};
pub use dataflow_state::{
    BaseTableState, DurabilityMode, MaterializedNodeState, PersistenceParameters, PersistentState,
};

pub use crate::domain::channel::{ChannelCoordinator, DomainReceiver, DomainSender, DualTcpStream};
use crate::domain::ReplicaAddress;
pub use crate::domain::{Domain, DomainBuilder, DomainIndex};
pub use crate::node_map::NodeMap;
pub use crate::payload::{DomainRequest, Packet, PacketDiscriminants};
use crate::prelude::Executor;
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

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, clap::ValueEnum, Default)]
pub enum EvictionKind {
    #[default]
    Random,
    LRU,
    Generational,
}

impl Display for EvictionKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Random => write!(f, "random"),
            Self::LRU => write!(f, "lru"),
            Self::Generational => write!(f, "generational"),
        }
    }
}

pub use readyset_client::shard_by;

impl Deref for ReaderMap {
    type Target = HashMap<ReaderAddress, SingleReadHandle>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for ReaderMap {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[derive(Default)]
pub struct Outboxes {
    /// messages for other domains
    domains: HashMap<ReplicaAddress, VecDeque<Packet>>,
}

impl Outboxes {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn have_messages(&self) -> bool {
        !self.domains.is_empty()
    }

    pub fn take_messages(&mut self) -> Vec<(ReplicaAddress, VecDeque<Packet>)> {
        self.domains.drain().collect()
    }
}

impl Executor for Outboxes {
    fn send(&mut self, dest: ReplicaAddress, m: Packet) {
        self.domains.entry(dest).or_default().push_back(m);
    }
}
