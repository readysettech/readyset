use petgraph;

use checktable;
use flow::domain;
use flow::node;
use flow::statistics;
use flow::prelude::*;

use std::fmt;
use std::sync::mpsc;
use std::collections::HashMap;

use std::time;

#[derive(Clone)]
pub struct Link {
    pub src: NodeAddress,
    pub dst: NodeAddress,
}

impl Link {
    pub fn new(src: NodeAddress, dst: NodeAddress) -> Self {
        Link { src: src, dst: dst }
    }
}

impl fmt::Debug for Link {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?} -> {:?}", self.src, self.dst)
    }
}
pub enum TriggerEndpoint {
    None,
    Start(Vec<usize>),
    End(mpsc::Sender<Box<Packet>>),
    Local(Vec<usize>),
}

pub enum InitialState {
    PartialLocal(usize),
    IndexedLocal(Vec<Vec<usize>>),
    PartialGlobal {
        gid: petgraph::graph::NodeIndex,
        cols: usize,
        key: usize,
        tag: Tag,
        trigger_tx: mpsc::SyncSender<Box<Packet>>,
    },
    Global {
        gid: petgraph::graph::NodeIndex,
        cols: usize,
        key: usize,
    },
}

#[derive(Clone)]
pub enum ReplayPieceContext {
    Partial {
        for_key: Vec<DataType>,
        ignore: bool,
    },
    Regular { last: bool },
}

#[derive(Clone)]
pub enum TransactionState {
    Committed(i64, petgraph::graph::NodeIndex, Option<Box<HashMap<domain::Index, i64>>>),
    Pending(checktable::Token, mpsc::Sender<Result<i64, ()>>),
    WillCommit,
}

#[derive(Clone)]
pub struct ReplayTransactionState {
    pub ts: i64,
    pub prevs: Option<Box<HashMap<domain::Index, i64>>>,
}

/// Different events that can occur as a packet is being processed.
#[derive(Clone, Debug)]
pub enum PacketEvent {
    /// The packet has been pulled off the input channel.
    ExitInputChannel,
    /// The packet has been received by some domain, and is being handled.
    Handle,
    /// The packet is being processed at some node.
    Process,
    /// The packet has reached some reader node.
    ReachedReader,
}

pub type Tracer = Option<mpsc::Sender<(time::Instant, PacketEvent)>>;

//#[warn(variant_size_differences)]
pub enum Packet {
    // Data messages
    //
    /// Regular data-flow update.
    Message {
        link: Link,
        data: Records,
        tracer: Tracer,
    },

    /// Transactional data-flow update.
    Transaction {
        link: Link,
        data: Records,
        state: TransactionState,
        tracer: Tracer,
    },

    /// Update that is part of a tagged data-flow replay path.
    FullReplay { link: Link, tag: Tag, state: State },

    /// Update that is part of a tagged data-flow replay path.
    ReplayPiece {
        link: Link,
        tag: Tag,
        data: Records,
        context: ReplayPieceContext,
        transaction_state: Option<ReplayTransactionState>,
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

    /// Add a new column to an existing `Base` node.
    AddBaseColumn {
        node: LocalNodeIndex,
        field: String,
        default: DataType,
        ack: mpsc::SyncSender<()>,
    },

    /// Drops an existing column from a `Base` node.
    DropBaseColumn {
        node: LocalNodeIndex,
        column: usize,
        ack: mpsc::SyncSender<()>,
    },

    /// Update Egress node.
    UpdateEgress {
        node: LocalNodeIndex,
        new_tx: Option<(NodeAddress, NodeAddress, mpsc::SyncSender<Box<Packet>>)>,
        new_tag: Option<(Tag, NodeAddress)>,
    },

    /// Add a shard to a Sharder node.
    ///
    /// Note that this *must* be done *before* the sharder starts being used!
    UpdateSharder {
        node: LocalNodeIndex,
        new_tx: (NodeAddress, mpsc::SyncSender<Box<Packet>>),
    },

    /// Add a streamer to an existing reader node.
    AddStreamer {
        node: LocalNodeIndex,
        new_streamer: mpsc::Sender<Vec<node::StreamUpdate>>,
    },

    /// Request a handle to an unbounded channel to this domain.
    ///
    /// We need these channels to send replay requests, as using the bounded channels could easily
    /// result in a deadlock. Since the unbounded channel is only used for requests as a result of
    /// processing, it is essentially self-clocking.
    RequestUnboundedTx(mpsc::Sender<mpsc::Sender<Box<Packet>>>),

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
        ack: mpsc::SyncSender<usize>,
    },

    /// Inform domain about a new replay path.
    SetupReplayPath {
        tag: Tag,
        source: Option<NodeAddress>,
        path: Vec<(NodeAddress, Option<usize>)>,
        done_tx: Option<mpsc::SyncSender<()>>,
        trigger: TriggerEndpoint,
        ack: mpsc::SyncSender<()>,
    },

    /// Ask domain (nicely) to replay a particular key.
    RequestPartialReplay { tag: Tag, key: Vec<DataType> },

    /// Instruct domain to replay the state of a particular node along an existing replay path.
    StartReplay {
        tag: Tag,
        from: NodeAddress,
        ack: mpsc::SyncSender<()>,
    },

    /// Sent to instruct a domain that a particular node should be considered ready to process
    /// updates.
    Ready {
        node: LocalNodeIndex,
        index: Vec<Vec<usize>>,
        ack: mpsc::SyncSender<()>,
    },

    /// Notification from Blender for domain to terminate
    Quit,

    // Transaction time messages
    //
    /// Instruct domain to flush pending transactions and notify upon completion. `prev_ts` is the
    /// timestamp of the last transaction sent to the domain prior to at.
    ///
    /// This allows a migration to ensure all transactions happen strictly *before* or *after* a
    /// migration in timestamp order.
    StartMigration {
        at: i64,
        prev_ts: i64,
        ack: mpsc::SyncSender<()>,
    },

    /// Notify a domain about a completion timestamp for an ongoing migration.
    ///
    /// Once this message is received, the domain may continue processing transactions with
    /// timestamps following the given one.
    ///
    /// The update also includes the new ingress_from_base counts the domain should use going
    /// forward.
    CompleteMigration {
        at: i64,
        ingress_from_base: HashMap<petgraph::graph::NodeIndex, usize>,
    },

    /// Request that a domain send usage statistics on the given sender.
    GetStatistics(mpsc::SyncSender<(statistics::DomainStats,
                                     HashMap<petgraph::graph::NodeIndex, statistics::NodeStats>)>),

    /// The packet was captured awaiting the receipt of other replays.
    Captured,
}

impl Packet {
    pub fn link(&self) -> &Link {
        match *self {
            Packet::Message { ref link, .. } => link,
            Packet::Transaction { ref link, .. } => link,
            Packet::FullReplay { ref link, .. } => link,
            Packet::ReplayPiece { ref link, .. } => link,
            _ => unreachable!(),
        }
    }

    pub fn link_mut(&mut self) -> &mut Link {
        match *self {
            Packet::Message { ref mut link, .. } => link,
            Packet::Transaction { ref mut link, .. } => link,
            Packet::FullReplay { ref mut link, .. } => link,
            Packet::ReplayPiece { ref mut link, .. } => link,
            _ => unreachable!(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match *self {
            Packet::Message { ref data, .. } => data.is_empty(),
            Packet::Transaction { ref data, .. } => data.is_empty(),
            Packet::FullReplay { .. } => false,
            Packet::ReplayPiece { ref data, .. } => data.is_empty(),
            _ => unreachable!(),
        }
    }

    pub fn map_data<F>(&mut self, map: F)
        where F: FnOnce(&mut Records)
    {
        match *self {
            Packet::Message { ref mut data, .. } |
            Packet::Transaction { ref mut data, .. } |
            Packet::ReplayPiece { ref mut data, .. } => {
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
            Packet::Transaction { .. } => true,
            _ => false,
        }
    }

    pub fn tag(&self) -> Option<Tag> {
        match *self {
            Packet::FullReplay { tag, .. } => Some(tag),
            Packet::ReplayPiece { tag, .. } => Some(tag),
            _ => None,
        }
    }

    pub fn data(&self) -> &Records {
        match *self {
            Packet::Message { ref data, .. } => data,
            Packet::Transaction { ref data, .. } => data,
            Packet::ReplayPiece { ref data, .. } => data,
            _ => unreachable!(),
        }
    }

    pub fn take_data(&mut self) -> Records {
        use std::mem;
        let inner = match *self {
            Packet::Message { ref mut data, .. } => data,
            Packet::Transaction { ref mut data, .. } => data,
            Packet::ReplayPiece { ref mut data, .. } => data,
            _ => unreachable!(),
        };
        mem::replace(inner, Records::default())
    }

    pub fn clone_data(&self) -> Self {
        match *self {
            Packet::Message {
                ref link,
                ref data,
                ref tracer,
            } => {
                Packet::Message {
                    link: link.clone(),
                    data: data.clone(),
                    tracer: tracer.clone(),
                }
            }
            Packet::Transaction {
                ref link,
                ref data,
                ref state,
                ref tracer,
            } => {
                Packet::Transaction {
                    link: link.clone(),
                    data: data.clone(),
                    state: state.clone(),
                    tracer: tracer.clone(),
                }
            }
            Packet::ReplayPiece {
                ref link,
                ref tag,
                ref data,
                context: ref context @ ReplayPieceContext::Regular { .. },
                ref transaction_state,
            } => {
                Packet::ReplayPiece {
                    link: link.clone(),
                    tag: tag.clone(),
                    data: data.clone(),
                    context: context.clone(),
                    transaction_state: transaction_state.clone(),
                }
            }
            _ => unreachable!(),
        }
    }

    pub fn trace(&self, event: PacketEvent) {
        match *self {
            Packet::Message { tracer: Some(ref sender), .. } |
            Packet::Transaction { tracer: Some(ref sender), .. } => {
                let _ = sender.send((time::Instant::now(), event));
            }
            _ => {}
        }
    }

    pub fn tracer(&mut self) -> Option<&mut Tracer> {
        match *self {
            Packet::Message { ref mut tracer, .. } |
            Packet::Transaction { ref mut tracer, .. } => Some(tracer),
            _ => None,
        }
    }
}

impl fmt::Debug for Packet {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Packet::Message { ref link, .. } => write!(f, "Packet::Message({:?})", link),
            Packet::Transaction {
                ref link,
                ref state,
                ..
            } => {
                match *state {
                    TransactionState::Committed(ts, ..) => {
                        write!(f, "Packet::Transaction({:?}, {})", link, ts)
                    }
                    TransactionState::Pending(..) => {
                        write!(f, "Packet::Transaction({:?}, pending)", link)
                    }
                    TransactionState::WillCommit => write!(f, "Packet::Transaction({:?}, ?)", link),
                }
            }
            Packet::ReplayPiece {
                ref link,
                ref tag,
                ref data,
                ..
            } => {
                write!(f,
                       "Packet::ReplayPiece({:?}, {}, {} records)",
                       link,
                       tag.id(),
                       data.len())
            }
            Packet::FullReplay {
                ref link,
                ref tag,
                ref state,
            } => {
                write!(f,
                       "Packet::FullReplay({:?}, {}, {} row state)",
                       link,
                       tag.id(),
                       state.len())
            }
            _ => write!(f, "Packet::Control"),
        }
    }
}
