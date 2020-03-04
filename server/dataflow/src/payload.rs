use petgraph;
use serde::{Deserialize, Serialize};

use crate::domain;
use crate::node;
use crate::prelude::*;
use noria;
use noria::channel;
use noria::internal::LocalOrNot;

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::net::SocketAddr;
use std::time;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReplayPathSegment {
    pub node: LocalNodeIndex,
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
    PartialLocal(Vec<(Vec<usize>, Vec<Tag>)>),
    IndexedLocal(HashSet<Vec<usize>>),
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

#[derive(Clone, Serialize, Deserialize)]
pub enum ReplayPieceContext {
    Partial {
        for_keys: HashSet<Vec<DataType>>,
        unishard: bool,
        ignore: bool,
    },
    Regular {
        last: bool,
    },
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct SourceChannelIdentifier {
    pub token: usize,
    pub epoch: usize,
    pub tag: u32,
}

#[derive(Clone, Serialize, Deserialize)]
#[allow(clippy::large_enum_variant)]
pub enum Packet {
    // Data messages
    //
    Input {
        inner: LocalOrNot<Input>,
        src: Option<SourceChannelIdentifier>,
        senders: Vec<SourceChannelIdentifier>,
    },

    /// Regular data-flow update.
    Message {
        link: Link,
        data: Records,
        tracer: Tracer,
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
        keys: Vec<Vec<DataType>>,
    },

    //
    // Internal control
    //
    Finish(Tag, LocalNodeIndex),

    // Control messages
    //
    /// Add a new node to this domain below the given parents.
    AddNode {
        node: Node,
        parents: Vec<LocalNodeIndex>,
    },

    /// Direct domain to remove some nodes.
    RemoveNodes {
        nodes: Vec<LocalNodeIndex>,
    },

    /// Add a new column to an existing `Base` node.
    AddBaseColumn {
        node: LocalNodeIndex,
        field: String,
        default: DataType,
    },

    /// Drops an existing column from a `Base` node.
    DropBaseColumn {
        node: LocalNodeIndex,
        column: usize,
    },

    /// Update Egress node.
    UpdateEgress {
        node: LocalNodeIndex,
        new_tx: Option<(NodeIndex, LocalNodeIndex, ReplicaAddr)>,
        new_tag: Option<(Tag, NodeIndex)>,
    },

    /// Add a shard to a Sharder node.
    ///
    /// Note that this *must* be done *before* the sharder starts being used!
    UpdateSharder {
        node: LocalNodeIndex,
        new_txs: (LocalNodeIndex, Vec<ReplicaAddr>),
    },

    /// Add a streamer to an existing reader node.
    AddStreamer {
        node: LocalNodeIndex,
        new_streamer: channel::StreamSender<Vec<node::StreamUpdate>>,
    },

    /// Set up a fresh, empty state for a node, indexed by a particular column.
    ///
    /// This is done in preparation of a subsequent state replay.
    PrepareState {
        node: LocalNodeIndex,
        state: InitialState,
    },

    /// Probe for the number of records in the given node's state
    StateSizeProbe {
        node: LocalNodeIndex,
    },

    /// Inform domain about a new replay path.
    SetupReplayPath {
        tag: Tag,
        source: Option<LocalNodeIndex>,
        path: Vec<ReplayPathSegment>,
        notify_done: bool,
        trigger: TriggerEndpoint,
    },

    /// Ask domain (nicely) to replay a particular set of keys.
    RequestPartialReplay {
        tag: Tag,
        keys: Vec<Vec<DataType>>,
        unishard: bool,
    },

    /// Ask domain (nicely) to replay a particular set of keys into a Reader.
    RequestReaderReplay {
        node: LocalNodeIndex,
        cols: Vec<usize>,
        keys: Vec<Vec<DataType>>,
    },

    /// Instruct domain to replay the state of a particular node along an existing replay path.
    StartReplay {
        tag: Tag,
        from: LocalNodeIndex,
    },

    /// Sent to instruct a domain that a particular node should be considered ready to process
    /// updates.
    Ready {
        node: LocalNodeIndex,
        purge: bool,
        index: HashSet<Vec<usize>>,
    },

    /// Notification from Blender for domain to terminate
    Quit,

    /// A packet used solely to drive the event loop forward.
    Spin,

    /// Request that a domain send usage statistics on the control reply channel.
    /// Argument specifies if we wish to get the full state size or just the partial nodes.
    GetStatistics,

    /// Ask domain to log its state size
    UpdateStateSize,
}

impl Packet {
    pub(crate) fn src(&self) -> LocalNodeIndex {
        match *self {
            Packet::Input { ref inner, .. } => {
                // inputs come "from" the base table too
                unsafe { inner.deref() }.dst
            }
            Packet::Message { ref link, .. } => link.src,
            Packet::ReplayPiece { ref link, .. } => link.src,
            _ => unreachable!(),
        }
    }

    pub(crate) fn dst(&self) -> LocalNodeIndex {
        match *self {
            Packet::Input { ref inner, .. } => unsafe { inner.deref() }.dst,
            Packet::Message { ref link, .. } => link.dst,
            Packet::ReplayPiece { ref link, .. } => link.dst,
            _ => unreachable!(),
        }
    }

    pub(crate) fn link_mut(&mut self) -> &mut Link {
        match *self {
            Packet::Message { ref mut link, .. } => link,
            Packet::ReplayPiece { ref mut link, .. } => link,
            Packet::EvictKeys { ref mut link, .. } => link,
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
        match *self {
            Packet::Message { .. } => true,
            _ => false,
        }
    }

    pub(crate) fn tag(&self) -> Option<Tag> {
        match *self {
            Packet::ReplayPiece { tag, .. } => Some(tag),
            Packet::EvictKeys { tag, .. } => Some(tag),
            _ => None,
        }
    }

    pub(crate) fn data(&self) -> &Records {
        match *self {
            Packet::Message { ref data, .. } => data,
            Packet::ReplayPiece { ref data, .. } => data,
            _ => unreachable!(),
        }
    }

    pub(crate) fn take_data(&mut self) -> Records {
        use std::mem;
        let inner = match *self {
            Packet::Message { ref mut data, .. } => data,
            Packet::ReplayPiece { ref mut data, .. } => data,
            _ => unreachable!(),
        };
        mem::replace(inner, Records::default())
    }

    pub(crate) fn clone_data(&self) -> Self {
        match *self {
            Packet::Message {
                link,
                ref data,
                ref tracer,
            } => Packet::Message {
                link,
                data: data.clone(),
                tracer: tracer.clone(),
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
            _ => unreachable!(),
        }
    }

    pub(crate) fn trace(&self, event: PacketEvent) {
        if let Packet::Message {
            tracer: Some((tag, Some(ref sender))),
            ..
        } = *self
        {
            use noria::debug::trace::{Event, EventType};
            sender
                .send(Event {
                    instant: time::Instant::now(),
                    event: EventType::PacketEvent(event, tag),
                })
                .unwrap();
        }
    }

    pub(crate) fn tracer(&mut self) -> Option<&mut Tracer> {
        match *self {
            Packet::Message { ref mut tracer, .. } => Some(tracer),
            _ => None,
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
                tag.id(),
                data.len()
            ),
            ref p => {
                use std::mem;
                write!(f, "Packet::Control({:?})", mem::discriminant(p))
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ControlReplyPacket {
    Ack(()),
    /// (number of rows, size in bytes)
    StateSize(usize, u64),
    Statistics(
        noria::debug::stats::DomainStats,
        HashMap<petgraph::graph::NodeIndex, noria::debug::stats::NodeStats>,
    ),
    Booted(usize, SocketAddr),
}

impl ControlReplyPacket {
    pub(crate) fn ack() -> ControlReplyPacket {
        ControlReplyPacket::Ack(())
    }
}
