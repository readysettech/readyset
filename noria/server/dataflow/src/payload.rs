use serde::{Deserialize, Serialize};

use crate::domain;
use crate::prelude::*;
use noria::{self, KeyComparison};
use noria::{internal::LocalOrNot, PacketData};

use std::collections::HashSet;
use std::fmt;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReplayPathSegment {
    pub node: LocalNodeIndex,
    pub force_tag_to: Option<Tag>,
    pub partial_key: Option<Vec<usize>>,
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

#[derive(Clone, Serialize, Deserialize)]
pub enum TriggerEndpoint {
    None,
    Start(Vec<usize>),
    End(SourceSelection, domain::Index),
    Local(Vec<usize>),
}

#[derive(Clone, Serialize, Deserialize)]
pub enum InitialState {
    PartialLocal(Vec<(Index, Vec<Tag>)>),
    IndexedLocal(HashSet<Index>),
    PartialGlobal {
        gid: petgraph::graph::NodeIndex,
        cols: usize,
        key: Vec<usize>,
        trigger_domain: (domain::Index, usize),
    },
    Global {
        gid: petgraph::graph::NodeIndex,
        cols: usize,
        key: Vec<usize>,
    },
}

#[derive(Clone, Serialize, Deserialize, PartialEq)]
pub enum ReplayPieceContext {
    Partial {
        for_keys: HashSet<KeyComparison>,
        requesting_shard: usize,
        unishard: bool,
        ignore: bool,
    },
    Regular {
        last: bool,
    },
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq)]
pub struct SourceChannelIdentifier {
    pub token: usize,
    pub epoch: usize,
    pub tag: u32,
}

/// A request issued to a domain through the worker RPC interface.
#[derive(Clone, Serialize, Deserialize)]
#[allow(clippy::large_enum_variant)]
pub enum DomainRequest {
    /// Request that a domain send usage statistics.
    GetStatistics,

    /// Add a new column to an existing `Base` node.
    AddBaseColumn {
        node: LocalNodeIndex,
        field: String,
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
        path: Vec<ReplayPathSegment>,
        partial_unicast_sharder: Option<NodeIndex>,
        notify_done: bool,
        trigger: TriggerEndpoint,
        raw_path: Vec<OptColumnRef>,
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

    /// Request the max replication offset of all the base table nodes in the domain
    RequestReplicationOffset,

    /// Process the packet, as per usual
    Packet(Packet),

    /// Informs a domain that a given node's columns are generated, and upqueries from them
    /// should use `Ingredient::handle_upquery`.
    GeneratedColumns {
        node: LocalNodeIndex,
        cols: Vec<usize>,
    },
}

#[derive(Clone, Serialize, Deserialize, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum Packet {
    // Data messages
    //
    Input {
        inner: LocalOrNot<PacketData>,
        src: Option<SourceChannelIdentifier>,
        senders: Vec<SourceChannelIdentifier>,
    },

    /// Regular data-flow update.
    Message {
        link: Link,
        data: Records,
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
    //
    Finish(Tag, LocalNodeIndex),

    // Control messages
    //
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
        src: Option<SourceChannelIdentifier>,
        timestamp: LocalOrNot<PacketData>,
    },
}

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
            Packet::Message { link, ref data } => Packet::Message {
                link,
                data: data.clone(),
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
