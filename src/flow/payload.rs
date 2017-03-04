use petgraph;

use flow;
use checktable;
use flow::domain;
use flow::statistics;
use flow::prelude::*;

use std::fmt;
use std::sync::mpsc;
use std::collections::HashMap;

#[derive(Clone)]
pub struct Link {
    pub src: NodeAddress,
    pub dst: NodeAddress,
}

impl Link {
    pub fn new(src: NodeAddress, dst: NodeAddress) -> Self {
        Link {
            src: src,
            dst: dst,
        }
    }
}

impl fmt::Debug for Link {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?} -> {:?}", self.src, self.dst)
    }
}
pub enum ReplayData {
    Records(Records),
    StateCopy(State),
}

#[derive(Clone)]
pub enum TransactionState {
    Committed(i64, petgraph::graph::NodeIndex),
    Pending(checktable::Token, mpsc::Sender<checktable::TransactionResult>),
}

pub enum Packet {
    //
    // Data messages
    //
    /// Regular data-flow update.
    Message { link: Link, data: Records },

    /// Transactional data-flow update.
    Transaction {
        link: Link,
        data: Records,
        state: TransactionState,
    },

    /// Update that is part of a tagged data-flow replay path.
    Replay {
        link: Link,
        tag: Tag,
        last: bool,
        data: ReplayData,
    },

    //
    // Control messages
    //
    /// Add a new node to this domain below the given parents.
    AddNode {
        node: domain::NodeDescriptor,
        parents: Vec<flow::LocalNodeIndex>,
    },

    /// Set up a fresh, empty state for a node, indexed by a particular column.
    ///
    /// This is done in preparation of a subsequent state replay.
    PrepareState {
        node: flow::LocalNodeIndex,
        index: Vec<Vec<usize>>,
    },

    /// Inform domain about a new replay path.
    SetupReplayPath {
        tag: Tag,
        path: Vec<NodeAddress>,
        done_tx: Option<mpsc::SyncSender<()>>,
        ack: mpsc::SyncSender<()>,
    },

    /// Instruct domain to replay the state of a particular node along an existing replay path.
    StartReplay {
        tag: Tag,
        from: NodeAddress,
        ack: mpsc::SyncSender<()>,
    },

    /// Sent to instruct a domain that a particular node should be considered ready to process
    /// updates.
    Ready {
        node: flow::LocalNodeIndex,
        index: Vec<Vec<usize>>,
        ack: mpsc::SyncSender<()>,
    },

    /// Notification from Blender for domain to terminate
    Quit,

    //
    // Transaction time messages
    //
    /// Instruct domain to flush pending transactions and notify upon completion.
    ///
    /// This allows a migration to ensure all transactions happen strictly *before* or *after* a
    /// migration in timestamp order.
    StartMigration { at: i64, ack: mpsc::SyncSender<()> },

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

    /// Notify a domain about a timestamp it would otherwise have missed.
    ///
    /// This message will be sent to domains from transactional base nodes with no connection to
    /// a particular domain.
    Timestamp(i64),

    None,
}

impl Packet {
    pub fn link(&self) -> &Link {
        match *self {
            Packet::Message { ref link, .. } => link,
            Packet::Transaction { ref link, .. } => link,
            Packet::Replay { ref link, .. } => link,
            _ => unreachable!(),
        }
    }

    pub fn link_mut(&mut self) -> &mut Link {
        match *self {
            Packet::Message { ref mut link, .. } => link,
            Packet::Transaction { ref mut link, .. } => link,
            Packet::Replay { ref mut link, .. } => link,
            _ => unreachable!(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match *self {
            Packet::Message { ref data, .. } => data.is_empty(),
            Packet::Transaction { ref data, .. } => data.is_empty(),
            Packet::Replay { ref data, .. } => {
                match *data {
                    ReplayData::StateCopy(..) => false,
                    ReplayData::Records(ref data) => data.is_empty(),
                }
            }
            Packet::None => true,
            _ => unreachable!(),
        }
    }

    pub fn map_data<F>(&mut self, map: F)
        where F: FnOnce(Records) -> Records
    {
        use std::mem;
        let m = match mem::replace(self, Packet::Timestamp(0)) {
            Packet::Message { link, data } => {
                Packet::Message {
                    link: link,
                    data: map(data),
                }
            }
            Packet::Transaction { link, data, state } => {
                Packet::Transaction {
                    link: link,
                    data: map(data),
                    state: state,
                }
            }
            Packet::Replay { link, tag, last, data: ReplayData::Records(data) } => {
                Packet::Replay {
                    link: link,
                    tag: tag,
                    last: last,
                    data: ReplayData::Records(map(data)),
                }
            }
            _ => {
                unreachable!();
            }
        };
        mem::replace(self, m);
    }

    pub fn data(&self) -> &Records {
        match *self {
            Packet::Message { ref data, .. } => data,
            Packet::Transaction { ref data, .. } => data,
            Packet::Replay { data: ReplayData::Records(ref data), .. } => data,
            _ => unreachable!(),
        }
    }

    pub fn take_data(self) -> Records {
        match self {
            Packet::Message { data, .. } => data,
            Packet::Transaction { data, .. } => data,
            Packet::Replay { data: ReplayData::Records(data), .. } => data,
            _ => unreachable!(),
        }
    }

    pub fn clone_data(&self) -> Self {
        match *self {
            Packet::Message { ref link, ref data } => {
                Packet::Message {
                    link: link.clone(),
                    data: data.clone(),
                }
            }
            Packet::Transaction { ref link, ref data, ref state } => {
                Packet::Transaction {
                    link: link.clone(),
                    data: data.clone(),
                    state: state.clone(),
                }
            }
            _ => unreachable!(),
        }
    }
}

impl fmt::Debug for Packet {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Packet::Message { ref link, .. } => write!(f, "Packet::Message({:?})", link),
            Packet::Transaction { ref link, ref state, .. } => {
                match *state {
                    TransactionState::Committed(ts, _) => {
                        write!(f, "Packet::Transaction({:?}, {})", link, ts)
                    }
                    TransactionState::Pending(..) => {
                        write!(f, "Packet::Transaction({:?}, pending)", link)
                    }
                }
            }
            Packet::Replay { ref link, ref tag, ref data, .. } => {
                match *data {
                    ReplayData::Records(ref data) => {
                        write!(f,
                               "Packet::Replay({:?}, {}, {} records)",
                               link,
                               tag.id(),
                               data.len())
                    }
                    ReplayData::StateCopy(ref data) => {
                        write!(f,
                               "Packet::Replay({:?}, {}, {}r state)",
                               link,
                               tag.id(),
                               data.len())
                    }
                }
            }
            Packet::Timestamp(ts) => write!(f, "Packet::Timestamp({})", ts),
            Packet::None => write!(f, "Packet::Node"),
            _ => write!(f, "Packet::Control"),
        }
    }
}
