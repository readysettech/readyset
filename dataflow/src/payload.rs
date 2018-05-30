use petgraph;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use api;
#[cfg(debug_assertions)]
use backtrace::Backtrace;
use channel;
use domain;
use node;
use prelude::*;

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::net::SocketAddr;
use std::time;

pub struct LocalBypass<T>(*mut T);

impl<T> LocalBypass<T> {
    pub fn make(t: Box<T>) -> Self {
        LocalBypass(Box::into_raw(t))
    }

    pub unsafe fn deref(&self) -> &T {
        &*self.0
    }

    pub unsafe fn take(self) -> Box<T> {
        Box::from_raw(self.0)
    }
}

unsafe impl<T> Send for LocalBypass<T> {}
impl<T> Serialize for LocalBypass<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        (self.0 as usize).serialize(serializer)
    }
}
impl<'de, T> Deserialize<'de> for LocalBypass<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        usize::deserialize(deserializer).map(|p| LocalBypass(p as *mut T))
    }
}
impl<T> Clone for LocalBypass<T> {
    fn clone(&self) -> LocalBypass<T> {
        panic!("LocalBypass types cannot be cloned");
    }
}

#[derive(Serialize, Deserialize)]
pub enum LocalOrNot<T> {
    Local(LocalBypass<T>),
    Not(T),
}

impl<T> LocalOrNot<T> {
    pub fn is_local(&self) -> bool {
        if let LocalOrNot::Local(..) = *self {
            true
        } else {
            false
        }
    }

    pub fn make(t: T, local: bool) -> Self {
        if local {
            LocalOrNot::Local(LocalBypass::make(Box::new(t)))
        } else {
            LocalOrNot::Not(t)
        }
    }

    pub unsafe fn take(self) -> T {
        match self {
            LocalOrNot::Local(l) => *l.take(),
            LocalOrNot::Not(t) => t,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReplayPathSegment {
    pub node: LocalNodeIndex,
    pub partial_key: Option<Vec<usize>>,
}

#[derive(Clone, Serialize, Deserialize)]
pub enum TriggerEndpoint {
    None,
    Start(Vec<usize>),
    End(bool, domain::Index, usize),
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
        ignore: bool,
    },
    Regular {
        last: bool,
    },
}

#[derive(Clone, Copy, Serialize, Deserialize)]
pub struct SourceChannelIdentifier {
    pub token: usize,
}

#[derive(Clone, Serialize, Deserialize)]
pub enum Packet {
    // Data messages
    //
    Input {
        inner: Input,
        src: Option<SourceChannelIdentifier>,
        senders: Vec<SourceChannelIdentifier>,
    },
    /// Regular data-flow update.
    Message {
        link: Link,
        src: Option<SourceChannelIdentifier>,
        data: Records,
        tracer: Tracer,
        senders: Vec<SourceChannelIdentifier>,
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

    /// Ask domain (nicely) to replay a particular key.
    RequestPartialReplay {
        tag: Tag,
        key: Vec<DataType>,
    },

    /// Ask domain (nicely) to replay a particular key.
    RequestReaderReplay {
        node: LocalNodeIndex,
        cols: Vec<usize>,
        key: Vec<DataType>,
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

    /// The packet is being sent locally, so a pointer is sent to avoid
    /// serialization/deserialization costs.
    Local(LocalBypass<Packet>),
}

impl Packet {
    pub fn link(&self) -> &Link {
        match *self {
            Packet::Input {
                inner: Input { ref link, .. },
                ..
            } => link,
            Packet::Message { ref link, .. } => link,
            Packet::ReplayPiece { ref link, .. } => link,
            _ => unreachable!(),
        }
    }

    pub fn link_mut(&mut self) -> &mut Link {
        match *self {
            Packet::Message { ref mut link, .. } => link,
            Packet::ReplayPiece { ref mut link, .. } => link,
            Packet::EvictKeys { ref mut link, .. } => link,
            _ => unreachable!(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match *self {
            Packet::Message { ref data, .. } => data.is_empty(),
            Packet::ReplayPiece { ref data, .. } => data.is_empty(),
            _ => unreachable!(),
        }
    }

    pub fn map_data<F>(&mut self, map: F)
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

    pub fn is_regular(&self) -> bool {
        match *self {
            Packet::Message { .. } => true,
            _ => false,
        }
    }

    pub fn tag(&self) -> Option<Tag> {
        match *self {
            Packet::ReplayPiece { tag, .. } => Some(tag),
            Packet::EvictKeys { tag, .. } => Some(tag),
            _ => None,
        }
    }

    pub fn data(&self) -> &Records {
        match *self {
            Packet::Message { ref data, .. } => data,
            Packet::ReplayPiece { ref data, .. } => data,
            _ => unreachable!(),
        }
    }

    pub fn swap_data(&mut self, new_data: Records) -> Records {
        use std::mem;
        let inner = match *self {
            Packet::Message { ref mut data, .. } => data,
            Packet::ReplayPiece { ref mut data, .. } => data,
            _ => unreachable!(),
        };
        mem::replace(inner, new_data)
    }

    pub fn take_data(&mut self) -> Records {
        use std::mem;
        let inner = match *self {
            Packet::Message { ref mut data, .. } => data,
            Packet::ReplayPiece { ref mut data, .. } => data,
            _ => unreachable!(),
        };
        mem::replace(inner, Records::default())
    }

    pub fn clone_data(&self) -> Self {
        match *self {
            Packet::Message {
                ref link,
                src: _,
                ref data,
                ref tracer,
                ref senders,
            } => Packet::Message {
                link: link.clone(),
                src: None,
                data: data.clone(),
                tracer: tracer.clone(),
                senders: senders.clone(),
            },
            Packet::ReplayPiece {
                ref link,
                ref tag,
                ref data,
                ref context,
            } => Packet::ReplayPiece {
                link: link.clone(),
                tag: tag.clone(),
                data: data.clone(),
                context: context.clone(),
            },
            _ => unreachable!(),
        }
    }

    pub fn trace(&self, event: PacketEvent) {
        match *self {
            Packet::Message {
                tracer: Some((tag, Some(ref sender))),
                ..
            } => {
                use api::debug::trace::{Event, EventType};
                sender
                    .send(Event {
                        instant: time::Instant::now(),
                        event: EventType::PacketEvent(event, tag),
                    })
                    .unwrap();
            }
            _ => {}
        }
    }

    pub fn tracer(&mut self) -> Option<&mut Tracer> {
        match *self {
            Packet::Message { ref mut tracer, .. } => Some(tracer),
            _ => None,
        }
    }

    /// If self is `Packet::Local` then replace with the packet pointed to.
    pub fn extract_local(&mut self) -> Option<Box<Self>> {
        if let Packet::Local(..) = *self {
            use std::mem;
            if let Packet::Local(local) = mem::replace(self, Packet::Spin) {
                Some(unsafe { local.take() })
            } else {
                unreachable!();
            }
        } else {
            None
        }
    }

    pub fn is_local(&self) -> bool {
        if let Packet::Local(..) = *self {
            true
        } else {
            false
        }
    }

    pub fn make_local(self: Box<Self>) -> Box<Self> {
        Box::new(Packet::Local(LocalBypass::make(self)))
    }
}

impl fmt::Debug for Packet {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Packet::Message { ref link, .. } => write!(f, "Packet::Message({:?})", link),
            Packet::ReplayPiece {
                ref link,
                ref tag,
                ref data,
                ..
            } => write!(
                f,
                "Packet::ReplayPiece({:?}, {}, {} records)",
                link,
                tag.id(),
                data.len()
            ),
            Packet::Local(ref lp) => {
                let lp = unsafe { lp.deref() };
                let s = write!(f, "local {:?}", lp)?;
                Ok(s)
            }
            ref p => {
                use std::mem;
                write!(f, "Packet::Control({:?})", mem::discriminant(p))
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ControlReplyPacket {
    #[cfg(debug_assertions)]
    Ack(Backtrace),
    #[cfg(not(debug_assertions))]
    Ack(()),
    /// (number of rows, size in bytes)
    StateSize(usize, u64),
    Statistics(
        api::debug::stats::DomainStats,
        HashMap<petgraph::graph::NodeIndex, api::debug::stats::NodeStats>,
    ),
    Booted(usize, SocketAddr),
}

impl ControlReplyPacket {
    #[cfg(debug_assertions)]
    pub fn ack() -> ControlReplyPacket {
        ControlReplyPacket::Ack(Backtrace::new())
    }

    #[cfg(not(debug_assertions))]
    pub fn ack() -> ControlReplyPacket {
        ControlReplyPacket::Ack(())
    }
}
