use std::collections::HashSet;
use std::fmt::{self, Display};

use itertools::Itertools;
use noria::internal::LocalOrNot;
use noria::{self, KeyComparison, PacketData, PacketTrace};
use serde::{Deserialize, Serialize};
use strum_macros::{EnumCount, EnumDiscriminants, EnumIter, IntoStaticStr};
use vec1::Vec1;

use crate::node::Column;
use crate::prelude::*;

/// A single segment (node that is passed through) of a replay path within a particular domain
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReplayPathSegment {
    /// The index of the node this replay path passes through
    pub node: LocalNodeIndex,

    pub force_tag_to: Option<Tag>,

    /// If this node is partially materialized, the index we should mark filled when replays are
    /// executed through this node
    pub partial_index: Option<Index>,

    /// Is this replay path segment the *target* of the replay path?
    ///
    /// Replay paths may have at most one segment as their target, but may not have any, if the
    /// source node happens to be the same as the target node.
    ///
    /// Usually this will be true for the last segment of the path, but that may not be the case if
    /// this is an *extended* replay path
    ///
    /// See [the docs section on straddled joins][straddled-joins] for more information about
    /// extended replay paths
    ///
    /// [straddled-joins]: http://docs/dataflow/replay_paths.html#straddled-joins
    pub is_target: bool,
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
                is_target,
            },
        ) in self.0.iter().enumerate()
        {
            if i != 0 {
                write!(f, " → ")?;
            }

            if *is_target {
                write!(f, "◎ ")?;
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
        trigger_domain: DomainIndex,
        num_shards: usize,
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
        /// The index of the shard that originally requested the replay.
        requesting_shard: usize,
        /// The index of the replica that originally requested the replay.
        ///
        /// Only this replica will receive any replay piece packets.
        requesting_replica: usize,
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

/// Description for how a sender node (an [`Egress`] or a [`Sharder`]) should replicate the
/// messages that it sends
///
/// Currently, we're limited to either going from n replicas to n replicas, or going from 1 replica
/// to n replicas. If in the future that limitation is lifted, this type will have to change to
/// accommodate the different ways we can do n-to-m replication
///
/// [`Egress`]: crate::node::special::Egress
/// [`Sharder`]: crate::node::special::Sharder
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq)]
pub enum SenderReplication {
    /// Send all messages to the same replica index as the current domain.
    ///
    /// This is the case both when going from an unreplicated domain to an unreplicated domain, and
    /// when going from a replicated domain to a domain with the same number of replicas
    Same,

    /// Fan-out from an unreplicated domain to `num_replicas` replicas.
    ///
    /// For most messages, this just consists of duplicating the message from 0 to `num_replicas`
    /// replicas. The one exception is replay pieces, which we only want to send to the replica
    /// that requested the replay originally (since some other replica might have requested the
    /// same key, and we don't want to replay the same key twice to the same replica)
    Fanout { num_replicas: usize },
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
        column: Column,
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

    /// Tell an egress node about its corresponding ingress node in the next domain
    AddEgressTx {
        /// The local index of the egress node we're informing about changes
        egress_node: LocalNodeIndex,
        /// The global and local index of the corresponding ingress node in the target domain
        ingress_node: (NodeIndex, LocalNodeIndex),
        target_domain: DomainIndex,
        target_shard: usize,
        /// Description for how messages should be replicated when sending to the target domain
        replication: SenderReplication,
    },

    /// Tell an egress node about a new tag that will pass through it, and the ingress node in the
    /// next domain that will receive replays along that tag
    AddEgressTag {
        /// The local index of the egress node we're informing about changes
        egress_node: LocalNodeIndex,
        /// The tag for a replay path
        tag: Tag,
        /// The ingress node that replays along that path should be sent to
        ingress_node: NodeIndex,
    },

    /// Add the target node to the list of nodes that should go through the filtering process at
    /// the given Egress node.
    AddEgressFilter {
        egress_node: LocalNodeIndex,
        target_node: NodeIndex,
    },

    /// Tell a Sharder node about its corresponding ingress node in the next domain, and how it
    /// should shard messages when sending to shards of that domain.
    ///
    /// Note that this *must* be done *before* the sharder starts being used!
    AddSharderTx {
        /// The local index of the sharder node to update
        sharder_node: LocalNodeIndex,
        /// The local index of the ingress node in the target domain
        ingress_node: LocalNodeIndex,
        /// The index of the target domain
        target_domain: DomainIndex,
        /// The number of shards to send to in the target domain
        num_shards: usize,
        /// Description for how messages should be replicated when sending to the target domain
        replication: SenderReplication,
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
        source_index: Option<Index>,
        path: Vec1<ReplayPathSegment>,
        partial_unicast_sharder: Option<NodeIndex>,
        notify_done: bool,
        trigger: TriggerEndpoint,

        /// True if the domain at the source of the replay path is unreplicated, but this domain is
        /// replicated.
        ///
        /// This is used to select the replica index to send replay requests to - if this is
        /// `true`, all replay requests will go to replica index `0`, but if it's `false`
        /// all replay requests will go to the same replica as the requesting domain
        replica_fanout: bool,
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

    /// Request a map of node indexes to key counts
    RequestNodeKeyCounts,

    /// Process the packet, as per usual
    Packet(Packet),

    /// Informs a domain that a particular index in a node is generated, and upqueries from them
    /// should use `Ingredient::handle_upquery`.
    GeneratedColumns {
        node: LocalNodeIndex,
        /// The generated index itself
        index: Index,
        /// The Tag for the replay path that will be making upqueries *to* this generated index
        tag: Tag,
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
        requesting_replica: usize,
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
            Packet::ReplayPiece { tag, .. } | Packet::EvictKeys { tag, .. } => Some(tag),
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

    pub(crate) fn replay_piece_context(&self) -> Option<&ReplayPieceContext> {
        match self {
            Packet::ReplayPiece { context, .. } => Some(context),
            _ => None,
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
