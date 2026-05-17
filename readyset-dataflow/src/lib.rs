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

pub mod domain;
mod node_map;
pub mod pre_insertion;

use std::collections::{HashMap, VecDeque};
use std::fmt::{self, Display};
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};

use readyset_client::ReaderAddress;
use readyset_data::DfValue;
use readyset_errors::{ReadySetResult, internal};
use readyset_sql::ast::{NullOrder, OrderType};
use serde::{Deserialize, Serialize};

use crate::prelude::Executor;

pub use dataflow_expression::{BinaryOperator, BuiltinFunction, Expr, LowerContext};
pub use dataflow_state::{
    BaseTableState, DurabilityMode, MaterializedNodeState, PersistenceParameters, PersistentState,
};
pub use readyset_client::post_processing::{
    PostLookup, PostLookupAggregate, PostLookupAggregateFunction, PostLookupAggregates,
    PostLookupDistinct,
};

pub use crate::backlog::{LookupError, ReaderUpdatedNotifier, SingleReadHandle};
pub use crate::domain::channel::{
    BaseWriteStream, ChannelCoordinator, DomainReceiver, DomainSender, ReplayReceiver, ReplaySender,
};
pub use crate::domain::{Domain, DomainBuilder, DomainIndex, ReplayPath, ReplayPathWithContext};
pub use crate::node_map::NodeMap;
pub use crate::payload::{
    DomainRequest, Packet, PacketDiscriminants, ReplayPathSegment, TriggerEndpoint,
};
pub use crate::pre_insertion::PreInsertion;
pub use crate::processing::LookupIndex;

/// A [`ReaderMap`] maps a [`ReaderAddress`] to the [`SingleReadHandle`] to access the reader at
/// that address.
#[repr(transparent)]
#[derive(Default, Clone)]
pub struct ReaderMap(HashMap<ReaderAddress, SingleReadHandle>);
pub type Readers = Arc<Mutex<ReaderMap>>;

pub type DomainConfig = domain::Config;

/// Operations to perform on rows before insertion into a reader or after a lookup.
///
/// Bundles pre-insertion ordering and post-lookup processing into a single spec
/// that travels with the reader node at migration time.
#[derive(Serialize, Deserialize, Debug, Clone, Default, Eq, PartialEq)]
pub struct ReaderProcessing {
    /// Pre processing on rows prior to insertion into a reader.
    pub pre_processing: PreInsertion,
    /// Post processing on result sets after a lookup is finished.
    pub post_processing: PostLookup,
}

impl ReaderProcessing {
    /// Constructs a new [`ReaderProcessing`].
    pub fn new(
        order_by: Option<Vec<(usize, OrderType, NullOrder)>>,
        limit: Option<usize>,
        returned_cols: Option<Vec<usize>>,
        default_row: Option<Vec<DfValue>>,
        aggregates: Option<PostLookupAggregates>,
        distinct: PostLookupDistinct,
    ) -> ReadySetResult<Self> {
        if let Some(cols) = &returned_cols
            && cols.iter().enumerate().any(|(i, v)| i != *v)
        {
            internal!("Returned columns must be projected in order");
        }

        if matches!(distinct, PostLookupDistinct::Sorted { .. }) && aggregates.is_some() {
            internal!(
                "Sorted DISTINCT must not be combined with real aggregates; use HashBased instead"
            );
        }

        let post_processing = PostLookup {
            order_by,
            limit,
            returned_cols,
            default_row: default_row.map(|r| Arc::new(r.into_boxed_slice())),
            aggregates,
            distinct,
        };

        let pre_processing = PreInsertion::from_post_lookup(&post_processing);

        Ok(ReaderProcessing {
            pre_processing,
            post_processing,
        })
    }
}

/// Kept only so older persisted `ControllerState` payloads (which include `Node.sharded_by`) can
/// still be deserialized. Never consulted at runtime — sharding was removed and the variants are
/// preserved purely so MessagePack-compact decoding of pre-removal graphs still works.
#[doc(hidden)]
#[deprecated(note = "kept only for persisted state compat; do not consult at runtime")]
#[allow(deprecated)]
#[derive(Copy, Clone, PartialEq, Eq, Debug, Serialize, Deserialize, Default)]
pub enum Sharding {
    #[default]
    None,
    ForcedNone,
    Random(usize),
    ByColumn(usize, usize),
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, clap::ValueEnum, Default)]
pub enum EvictionKind {
    // unused, kept for backward compatibility, synonym for lru
    Random,
    #[default]
    LRU,
}

impl Display for EvictionKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Random => write!(f, "lru (was random)"),
            Self::LRU => write!(f, "lru"),
        }
    }
}

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

/// A unit of barrier accounting returned to the worker's `BarrierManager`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BarrierCredit {
    pub id: u128,
    pub credits: u128,
}

#[derive(Default)]
pub struct Outboxes {
    /// messages for other domains
    domains: HashMap<DomainIndex, VecDeque<Packet>>,
    /// barrier credits to return to the worker's `BarrierManager`
    barrier_credits: Vec<BarrierCredit>,
    /// messages held temporarily that we will revise soon
    corked: Option<Vec<(DomainIndex, Packet)>>,
}

impl Outboxes {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn have_messages(&self) -> bool {
        !self.domains.is_empty()
    }

    pub fn have_barrier_credits(&self) -> bool {
        !self.barrier_credits.is_empty()
    }

    pub fn take_messages(&mut self) -> Vec<(DomainIndex, VecDeque<Packet>)> {
        self.domains.drain().collect()
    }

    pub fn take_barrier_credits(&mut self) -> Vec<BarrierCredit> {
        self.barrier_credits.drain(..).collect()
    }
}

impl Executor for Outboxes {
    fn send(&mut self, dest: DomainIndex, m: Packet) {
        if let Some(ref mut corked) = self.corked {
            corked.push((dest, m));
        } else {
            self.domains.entry(dest).or_default().push_back(m);
        }
    }

    fn barrier_credit(&mut self, credit: BarrierCredit) {
        // Barrier credits are only emitted after `uncork`; the cork holds back
        // downstream packets, not credit returns. Surface a future caller that
        // violates that ordering.
        debug_assert!(self.corked.is_none());
        self.barrier_credits.push(credit);
    }

    fn cork(&mut self) {
        self.corked = Some(Vec::new());
    }

    fn uncork(&mut self) -> Vec<(DomainIndex, Packet)> {
        self.corked.take().expect("can't uncork when not corked!")
    }
}
