use std::collections::HashSet;
use std::fmt::{self, Display};

use itertools::Itertools;
use nom_sql::SqlIdentifier;
use noria::internal::LocalOrNot;
use noria::{self, KeyComparison, PacketData, PacketTrace};
use serde::{Deserialize, Serialize};
use strum_macros::{EnumCount, EnumDiscriminants, EnumIter, IntoStaticStr};
use vec1::Vec1;

use crate::prelude::*;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReplayPathSegment {
    pub node: LocalNodeIndex,
    pub force_tag_to: Option<Tag>,
    pub partial_index: Option<Index>,
}

/// [`Display`] wrapper struct for a list of [`ReplayPathSegment`]s, to write them using a more
/// human-readable representation
pub struct PrettyReplayPath<'a>(pub &'a [ReplayPathSegment]);

impl<'a> Display for PrettyReplayPath<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (
            i,
            ReplayPathSegment {
                node,
                force_tag_to,
                partial_index,
            },
        ) in self.0.iter().enumerate()
        {
            if i != 0 {
                write!(f, " → ")?;
            }

            write!(f, "{}", node)?;
            if let Some(idx) = partial_index {
                write!(
                    f,
                    " ({:?}[{}])",
                    idx.index_type,
                    idx.columns.iter().join(", ")
                )?;
            }

            if let Some(tag) = force_tag_to {
                write!(f, " force: {:?}", tag)?;
            }
        }

        Ok(())
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum SourceSelection {
    /// Query only the shard of the source that matches the key.
    KeyShard {
        key_i_to_shard: usize,
        nshards: usize,
    },
    /// Query the same shard of the source as the destination.
    SameShard,
    /// Query all shards of the source.
    ///
    /// Value is the number of shards.
    AllShards(usize),
}

/// Representation for how to trigger replays for a partial replay path that touches a particular
/// domain
#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum TriggerEndpoint {
    None,
    /// This domain is the start of the replay path
    Start(Index),
    /// This domain is the end of the replay path, with the indicated source domain and how to
    /// query that domain's shards
    End(SourceSelection, DomainIndex),
    /// The replay path is contained entirely within this domain
    Local(Index),
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum InitialState {
    PartialLocal {
        strict: Vec<(Index, Vec<Tag>)>,
        weak: HashSet<Index>,
    },
    IndexedLocal {
        strict: HashSet<Index>,
        weak: HashSet<Index>,
    },
    PartialGlobal {
        gid: petgraph::graph::NodeIndex,
        cols: usize,
        index: Index,
        trigger_domain: (DomainIndex, usize),
    },
    Global {
        gid: petgraph::graph::NodeIndex,
        cols: usize,
        index: Index,
    },
}

#[derive(Clone, Serialize, Deserialize, PartialEq)]
pub enum ReplayPieceContext {
    Partial {
        for_keys: HashSet<KeyComparison>,
        requesting_shard: usize,
        unishard: bool,
    },
    Regular {
        last: bool,
    },
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq)]
pub struct SourceChannelIdentifier {
    pub token: u64,
    pub tag: u32,
}

/// A request issued to a domain through the worker RPC interface.
#[derive(Clone, Serialize, Deserialize, Debug)]
#[allow(clippy::large_enum_variant)]
pub enum DomainRequest {
    /// Request that a domain send usage statistics.
    GetStatistics,

    /// Add a new column to an existing `Base` node.
    AddBaseColumn {
        node: LocalNodeIndex,
        field: SqlIdentifier,
        default: DataType,
    },

    /// Drops an existing column from a `Base` node.
    DropBaseColumn { node: LocalNodeIndex, column: usize },

    /// Add a new node to this domain below the given parents.
    AddNode {
        node: Node,
        parents: Vec<LocalNodeIndex>,
    },

    /// Direct domain to remove some nodes.
    RemoveNodes { nodes: Vec<LocalNodeIndex> },

    /// Update Egress node.
    UpdateEgress {
        node: LocalNodeIndex,
        new_tx: Option<(NodeIndex, LocalNodeIndex, ReplicaAddr)>,
        new_tag: Option<(Tag, NodeIndex)>,
    },

    /// Add the target node to the list of nodes
    /// that should go through the filtering process
    /// at the given Egress node.
    AddEgressFilter {
        egress_node: LocalNodeIndex,
        target_node: NodeIndex,
    },

    /// Add a shard to a Sharder node.
    ///
    /// Note that this *must* be done *before* the sharder starts being used!
    UpdateSharder {
        node: LocalNodeIndex,
        new_txs: (LocalNodeIndex, Vec<ReplicaAddr>),
    },

    /// Set up a fresh, empty state for a node, indexed by a particular column.
    ///
    /// This is done in preparation of a subsequent state replay.
    PrepareState {
        node: LocalNodeIndex,
        state: InitialState,
    },

    /// Probe for the number of records in the given node's state
    StateSizeProbe { node: LocalNodeIndex },

    /// Ask domain to log its state size
    UpdateStateSize,

    /// Inform domain about a new replay path.
    SetupReplayPath {
        tag: Tag,
        source: Option<LocalNodeIndex>,
        path: Vec1<ReplayPathSegment>,
        partial_unicast_sharder: Option<NodeIndex>,
        notify_done: bool,
        trigger: TriggerEndpoint,
        raw_path: Vec<IndexRef>,
    },

    /// Instruct domain to replay the state of a particular node along an existing replay path,
    /// identified by `tag`.
    StartReplay { tag: Tag, from: LocalNodeIndex },

    /// Query whether a domain has finished replaying.
    QueryReplayDone,

    /// Sent to instruct a domain that a particular node should be considered ready to process
    /// updates.
    Ready {
        node: LocalNodeIndex,
        purge: bool,
        index: HashSet<Index>,
    },

    /// Request a map of all replication offsets of the base table nodes in the domain
    RequestReplicationOffsets,

    /// Request a list of base table nodes that are currently involved in snapshotting.
    RequestSnapshottingTables,

    /// Process the packet, as per usual
    Packet(Packet),

    /// Informs a domain that a given node's columns are generated, and upqueries from them
    /// should use `Ingredient::handle_upquery`.
    GeneratedColumns {
        node: LocalNodeIndex,
        cols: Vec<usize>,
    },
}

/// The primary unit of communication between nodes in the dataflow graph.
///
/// FIXME(grfn): This should be refactored to be an enum-of-enums so that the various parts of
/// dataflow code that only know how to handle one kind of packet don't have to panic if they
/// receive the wrong kind of packet. See
/// [ENG-455](https://readysettech.atlassian.net/browse/ENG-455)
#[derive(Clone, Serialize, Deserialize, PartialEq, EnumDiscriminants)]
#[strum_discriminants(derive(EnumIter, EnumCount, IntoStaticStr))]
#[allow(clippy::large_enum_variant)]
pub enum Packet {
    // Data messages
    /// A write received to the base table
    Input {
        inner: LocalOrNot<PacketData>,
        src: SourceChannelIdentifier,
    },

    /// Regular data-flow update.
    Message {
        link: Link,
        data: Records,
        trace: Option<PacketTrace>,
    },

    /// Update that is part of a tagged data-flow replay path.
    ReplayPiece {
        link: Link,
        tag: Tag,
        data: Records,
        context: ReplayPieceContext,
    },

    /// Trigger an eviction from the target node.
    Evict {
        node: Option<LocalNodeIndex>,
        num_bytes: usize,
    },

    /// Evict the indicated keys from the materialization targed by the replay path `tag` (along
    /// with any other materializations below it).
    EvictKeys {
        link: Link,
        tag: Tag,
        keys: Vec<KeyComparison>,
    },

    //
    // Internal control
    Finish(Tag, LocalNodeIndex),

    // Control messages
    /// Ask domain (nicely) to replay a particular set of keys.
    RequestPartialReplay {
        tag: Tag,
        keys: Vec<KeyComparison>,
        unishard: bool,
        requesting_shard: usize,
    },

    /// Ask domain (nicely) to replay a particular set of keys into a Reader.
    RequestReaderReplay {
        node: LocalNodeIndex,
        cols: Vec<usize>,
        keys: Vec<KeyComparison>,
    },

    /// A packet used solely to drive the event loop forward.
    Spin,

    /// Propagate updated timestamps for the set of base tables.
    Timestamp {
        link: Option<Link>,
        src: SourceChannelIdentifier,
        timestamp: LocalOrNot<PacketData>,
    },
}

// Getting rid of the various unreachables on the accessor functions in this impl requires
// refactoring Packet to be an enum-of-enums, and then moving the accessor functions themselves to
// the smaller enums (or having them return Options). This is scoped for a larger refactor - see
// https://readysettech.atlassian.net/browse/ENG-455.
#[allow(clippy::unreachable)]
impl Packet {
    pub(crate) fn src(&self) -> LocalNodeIndex {
        match *self {
            // inputs come "from" the base table too
            Packet::Input { ref inner, .. } => unsafe { inner.deref() }.dst,
            Packet::Message { ref link, .. } => link.src,
            Packet::ReplayPiece { ref link, .. } => link.src,
            // If link is not specified, then we are at a base table node. Use the packet data
            // to get the src (which is the base table node).
            Packet::Timestamp {
                ref link,
                ref timestamp,
                ..
            } => match link {
                Some(l) => l.src,
                None => unsafe { timestamp.deref() }.dst,
            },
            _ => unreachable!(),
        }
    }

    pub(crate) fn dst(&self) -> LocalNodeIndex {
        match *self {
            Packet::Input { ref inner, .. } => unsafe { inner.deref() }.dst,
            Packet::Message { ref link, .. } => link.dst,
            Packet::ReplayPiece { ref link, .. } => link.dst,
            // If link is not specified, then we are at a base table node. Use the packet data
            // to get the dst (which is the base table node).
            Packet::Timestamp {
                ref link,
                ref timestamp,
                ..
            } => match link {
                Some(l) => l.dst,
                None => unsafe { timestamp.deref() }.dst,
            },
            _ => unreachable!(),
        }
    }

    pub(crate) fn link_mut(&mut self) -> &mut Link {
        match *self {
            Packet::Message { ref mut link, .. } => link,
            Packet::ReplayPiece { ref mut link, .. } => link,
            Packet::EvictKeys { ref mut link, .. } => link,
            Packet::Timestamp { ref mut link, .. } => link.as_mut().unwrap(),
            _ => unreachable!(),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        match *self {
            Packet::Message { ref data, .. } => data.is_empty(),
            Packet::ReplayPiece { ref data, .. } => data.is_empty(),
            _ => unreachable!(),
        }
    }

    pub(crate) fn map_data<F>(&mut self, map: F)
    where
        F: FnOnce(&mut Records),
    {
        match *self {
            Packet::Message { ref mut data, .. } | Packet::ReplayPiece { ref mut data, .. } => {
                map(data);
            }
            _ => {
                unreachable!();
            }
        }
    }

    /// Perform a function on a packet's trace info if the packet has trace info and it is not
    /// None. Otherwise  this is a noop and the function is not called.
    pub(crate) fn handle_trace<F>(&mut self, map: F)
    where
        F: FnOnce(&PacketTrace),
    {
        if let Packet::Message {
            trace: Some(ref t), ..
        } = *self
        {
            map(t);
        }
    }

    pub(crate) fn is_regular(&self) -> bool {
        matches!(*self, Packet::Message { .. })
    }

    pub(crate) fn tag(&self) -> Option<Tag> {
        match *self {
            Packet::ReplayPiece { tag, .. } => Some(tag),
            Packet::EvictKeys { tag, .. } => Some(tag),
            _ => None,
        }
    }

    pub(crate) fn take_data(&mut self) -> Records {
        let inner = match *self {
            Packet::Message { ref mut data, .. } => data,
            Packet::ReplayPiece { ref mut data, .. } => data,
            _ => unreachable!(),
        };
        std::mem::take(inner)
    }

    pub(crate) fn clone_data(&self) -> Self {
        match *self {
            Packet::Message {
                link,
                ref data,
                ref trace,
            } => Packet::Message {
                link,
                data: data.clone(),
                trace: trace.clone(),
            },
            Packet::ReplayPiece {
                link,
                tag,
                ref data,
                ref context,
            } => Packet::ReplayPiece {
                link,
                tag,
                data: data.clone(),
                context: context.clone(),
            },
            Packet::Timestamp {
                ref timestamp,
                link,
                src,
            } => Packet::Timestamp {
                link,
                src,
                timestamp: timestamp.clone(),
            },
            _ => unreachable!(),
        }
    }
}

impl ToString for Packet {
    fn to_string(&self) -> String {
        match self {
            Packet::Input { .. } => "Input",
            Packet::Message { .. } => "Message",
            Packet::RequestReaderReplay { .. } => "RequestReaderReplay",
            Packet::RequestPartialReplay { .. } => "RequestPartialReplay",
            Packet::ReplayPiece { .. } => "ReplayPiece",
            Packet::EvictKeys { .. } => "EvictKeys",
            Packet::Timestamp { .. } => "Timestamp",
            Packet::Finish { .. } => "Finish",
            Packet::Spin { .. } => "Spin",
            Packet::Evict { .. } => "Evict",
        }
        .to_string()
    }
}

impl fmt::Debug for Packet {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Packet::Input { .. } => write!(f, "Packet::Input"),
            Packet::Message { ref link, .. } => write!(f, "Packet::Message({:?})", link),
            Packet::RequestReaderReplay { ref keys, .. } => {
                write!(f, "Packet::RequestReaderReplay({:?})", keys)
            }
            Packet::RequestPartialReplay { ref tag, .. } => {
                write!(f, "Packet::RequestPartialReplay({:?})", tag)
            }
            Packet::ReplayPiece {
                ref link,
                ref tag,
                ref data,
                ..
            } => write!(
                f,
                "Packet::ReplayPiece({:?}, tag {}, {} records)",
                link,
                tag,
                data.len()
            ),
            ref p => {
                use std::mem;
                write!(f, "Packet::Control({:?})", mem::discriminant(p))
            }
        }
    }
}
