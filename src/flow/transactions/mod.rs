use petgraph::graph::NodeIndex;
use std::collections::BinaryHeap;
use std::sync::{Arc, Mutex};
use std::sync::mpsc;

use std::cmp::Ordering;

use std::mem;

use flow::prelude::*;
use flow::payload::{TransactionState, ReplayTransactionState, IngressFromBase, EgressForBase};
use flow::domain;

use checktable;


enum BufferedTransaction {
    Transaction(NodeIndex, Box<Packet>),
    MigrationStart(mpsc::SyncSender<()>),
    MigrationEnd(IngressFromBase, EgressForBase),
    Replay(Box<Packet>),
    SeedReplay(Tag, Vec<DataType>, ReplayTransactionState),
}

struct BufferEntry {
    ts: i64,
    prev_ts: i64,
    transaction: BufferedTransaction,
}
impl Ord for BufferEntry {
    fn cmp(&self, other: &BufferEntry) -> Ordering {
        // The "larger" BufferEntry is the one with the smallest timestamp. This is necessary so
        // that transactions with earlier timestamps will be removed from the max-heap before later
        // ones.
        other.ts.cmp(&self.ts)
    }
}
impl PartialOrd for BufferEntry {
    fn partial_cmp(&self, other: &BufferEntry) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl PartialEq for BufferEntry {
    fn eq(&self, other: &BufferEntry) -> bool {
        self.ts == other.ts
    }
}
impl Eq for BufferEntry {}

enum Bundle {
    Empty,
    Messages(usize, Vec<Box<Packet>>),
    MigrationStart(mpsc::SyncSender<()>),
    MigrationEnd(IngressFromBase, EgressForBase),
    Replay(Box<Packet>),
    SeedReplay(Tag, Vec<DataType>, ReplayTransactionState),
}

pub enum Event {
    Transaction(Vec<Box<Packet>>),
    StartMigration,
    CompleteMigration,
    Replay(Box<Packet>),
    SeedReplay(Tag, Vec<DataType>, ReplayTransactionState),
    None,
}

pub struct DomainState {
    domain_index: domain::Index,

    checktable: Arc<Mutex<checktable::CheckTable>>,
    buffer: BinaryHeap<BufferEntry>,

    next_transaction: Bundle,

    /// Number of ingress nodes in the domain that receive updates from each base node. Base nodes
    /// that are only connected by timestamp ingress nodes are not included.
    ingress_from_base: Vec<usize>,

    /// Reachable egress (or rather, output) nodes from a given base inside this domain.
    egress_for_base: EgressForBase,

    /// Timestamp that the domain has seen all transactions up to.
    ts: i64,
}

impl DomainState {
    pub fn new(domain_index: domain::Index,
               checktable: Arc<Mutex<checktable::CheckTable>>,
               ts: i64)
               -> Self {

        Self {
            domain_index: domain_index,
            checktable: checktable,
            buffer: BinaryHeap::new(),
            next_transaction: Bundle::Empty,
            ingress_from_base: Vec::new(),
            egress_for_base: Default::default(),
            ts: ts,
        }
    }

    pub fn egress_for(&self, base: NodeIndex) -> &[NodeAddress] {
        &self.egress_for_base[&base][..]
    }

    fn assign_ts(&mut self, packet: &mut Box<Packet>, nodes: &DomainNodes) -> bool {
        // all this is for #16223
        let mut unsafe_state_ref_mut = None;
        if let Packet::Transaction { ref mut state, .. } = **packet {
            match *state {
                TransactionState::Committed(..) => {}
                _ => {
                    unsafe_state_ref_mut = Some(state as *mut _);
                }
            }
        }

        match **packet {
            Packet::Transaction { state: TransactionState::Committed(..), .. } => true,
            Packet::Transaction { ref link, ref data, .. } => {
                let state: &mut TransactionState = unsafe { &mut *unsafe_state_ref_mut.unwrap() };
                let empty = TransactionState::Committed(0, 0.into(), None);
                let pending = ::std::mem::replace(state, empty);
                let base_node = nodes[link.dst.as_local()]
                    .borrow()
                    .global_addr()
                    .as_global()
                    .clone();
                match pending {
                    TransactionState::Pending(token, send) => {
                        let result = self.checktable
                            .lock()
                            .unwrap()
                            .apply(&token, base_node, data);
                        match result {
                            checktable::TransactionResult::Committed(ts, prevs) => {
                                let _ = send.send(Ok(ts));
                                ::std::mem::replace(state,
                                                    TransactionState::Committed(ts,
                                                                                base_node,
                                                                                prevs));
                                true
                            }
                            checktable::TransactionResult::Aborted => {
                                let _ = send.send(Err(()));
                                false
                            }
                        }
                    }
                    TransactionState::WillCommit => {
                        let (ts, prevs) = self.checktable
                            .lock()
                            .unwrap()
                            .apply_unconditional(base_node, data);
                        ::std::mem::replace(state,
                                            TransactionState::Committed(ts, base_node, prevs));
                        true
                    }
                    TransactionState::Committed(..) => unreachable!(),
                }
            }
            _ => true,
        }
    }

    fn buffer_transaction(&mut self, m: Box<Packet>) {
        let (ts, base, prev_ts) = match *m {
            Packet::Transaction {
                state: TransactionState::Committed(ts, base, ref prevs), ..
            } => {
                if self.ts == ts - 1 {
                    (ts, Some(base), ts - 1)
                } else {
                    let prev_ts = prevs
                        .as_ref()
                        .and_then(|p| p.get(&self.domain_index))
                        .cloned()
                        .unwrap_or(ts - 1);

                    (ts, Some(base), prev_ts)
                }
            }
            Packet::StartMigration { at, prev_ts, .. } => (at, None, prev_ts),
            Packet::CompleteMigration { at, .. } => (at, None, at - 1),
            Packet::ReplayPiece {
                transaction_state: Some(ReplayTransactionState { ts, ref prevs }), ..
            } => {
                if self.ts == ts - 1 {
                    (ts, None, ts - 1)
                } else {
                    let prev_ts = prevs
                        .as_ref()
                        .and_then(|p| p.get(&self.domain_index))
                        .cloned()
                        .unwrap_or(ts - 1);

                    (ts, None, prev_ts)
                }
            }
            _ => unreachable!(),
        };

        if self.ts == prev_ts {
            self.ts = ts - 1;

            match self.next_transaction {
                Bundle::Empty => {
                    let bundle = match m {
                        box Packet::Transaction { .. } => {
                            let count = base.map(|b| self.ingress_from_base[b.index()])
                                .unwrap_or(1);
                            if count == 0 {
                                println!("{:?} got transaction from base {:?}, which it shouldn't",
                                         self.domain_index,
                                         base);
                                unreachable!();
                            }
                            Bundle::Messages(count, vec![m])
                        }
                        box Packet::StartMigration { ack, .. } => Bundle::MigrationStart(ack),
                        box Packet::CompleteMigration { .. } => {
                            let m = *m; // workaround for #16223
                            if let Packet::CompleteMigration {
                                       ingress_from_base,
                                       egress_for_base,
                                       ..
                                   } = m {
                                Bundle::MigrationEnd(ingress_from_base, egress_for_base)
                            } else {
                                unreachable!()
                            }
                        }
                        box Packet::ReplayPiece { .. } => Bundle::Replay(m),
                        _ => unreachable!(),
                    };

                    mem::replace(&mut self.next_transaction, bundle);
                }
                Bundle::Messages(_, ref mut packets) => packets.push(m),
                _ => unreachable!(),
            }
        } else {
            let transaction = match m {
                box Packet::Transaction { .. } => {
                    BufferedTransaction::Transaction(base.unwrap(), m)
                }
                box Packet::StartMigration { ack, .. } => BufferedTransaction::MigrationStart(ack),
                box Packet::CompleteMigration { .. } => {
                    let m = *m; // workaround for #16223
                    if let Packet::CompleteMigration {
                               ingress_from_base,
                               egress_for_base,
                               ..
                           } = m {
                        BufferedTransaction::MigrationEnd(ingress_from_base, egress_for_base)
                    } else {
                        unreachable!()
                    }
                }
                box Packet::ReplayPiece { .. } => BufferedTransaction::Replay(m),
                _ => unreachable!(),
            };
            let entry = BufferEntry {
                ts: ts,
                prev_ts: prev_ts,
                transaction: transaction,
            };
            self.buffer.push(entry);
        }
    }

    pub fn handle(&mut self, mut m: Box<Packet>, nodes: &DomainNodes) {
        if self.assign_ts(&mut m, nodes) {
            self.buffer_transaction(m);
        }
    }

    fn update_next_transaction(&mut self) {
        let has_next = self.buffer
            .peek()
            .map(|e| e.prev_ts == self.ts)
            .unwrap_or(false);

        if has_next {
            let entry = self.buffer.pop().unwrap();
            let ts = entry.ts;

            match entry.transaction {
                BufferedTransaction::Transaction(base, p) => {
                    let mut messages = vec![p];
                    while self.buffer.peek().map(|e| e.ts == ts).unwrap_or(false) {
                        let e = self.buffer.pop().unwrap();
                        if let BufferedTransaction::Transaction(_, p) = e.transaction {
                            messages.push(p);
                        } else {
                            unreachable!(); // Different transaction types at same timestamp
                        }
                    }

                    self.next_transaction = Bundle::Messages(self.ingress_from_base[base.index()],
                                                             messages);
                }
                BufferedTransaction::MigrationStart(sender) => {
                    self.next_transaction = Bundle::MigrationStart(sender)
                }
                BufferedTransaction::MigrationEnd(ingress_from_base, egress_for_base) => {
                    self.next_transaction = Bundle::MigrationEnd(ingress_from_base,
                                                                 egress_for_base);
                }
                BufferedTransaction::Replay(packet) => {
                    self.next_transaction = Bundle::Replay(packet);
                }
                BufferedTransaction::SeedReplay(tag, key, rts) => {
                    self.next_transaction = Bundle::SeedReplay(tag, key, rts);
                }
            }
        }
    }

    pub fn get_next_event(&mut self) -> Event {
        if let Bundle::Messages(count, ref v) = self.next_transaction {
            if v.len() < count {
                return Event::None;
            }
        }

        match mem::replace(&mut self.next_transaction, Bundle::Empty) {
            Bundle::MigrationStart(channel) => {
                channel.send(()).unwrap();
                self.ts += 1;
                self.update_next_transaction();
                Event::StartMigration
            }
            Bundle::MigrationEnd(ingress_from_base, egress_for_base) => {
                let max_index = ingress_from_base
                    .keys()
                    .map(|ni| ni.index())
                    .max()
                    .unwrap_or(0);
                self.ingress_from_base = vec![0; max_index + 1];
                for (ni, count) in ingress_from_base.into_iter() {
                    self.ingress_from_base[ni.index()] = count;
                }
                self.egress_for_base = egress_for_base;

                self.ts += 1;
                self.update_next_transaction();
                Event::CompleteMigration
            }
            Bundle::Messages(count, v) => {
                assert_eq!(v.len(), count);

                self.ts += 1;
                self.update_next_transaction();
                Event::Transaction(v)
            }
            Bundle::Replay(packet) => {
                self.ts += 1;
                self.update_next_transaction();
                Event::Replay(packet)
            }
            Bundle::SeedReplay(tag, key, rts) => {
                self.ts += 1;
                self.update_next_transaction();
                Event::SeedReplay(tag, key, rts)
            }
            Bundle::Empty => Event::None,
        }
    }

    pub fn schedule_replay(&mut self, tag: Tag, key: Vec<DataType>) {
        let (ts, prevs) = self.checktable.lock().unwrap().claim_replay_timestamp(&tag);

        let prev_ts = if self.ts == ts - 1 {
            ts - 1
        } else {
            prevs
                .as_ref()
                .and_then(|p| p.get(&self.domain_index))
                .cloned()
                .unwrap_or(ts - 1)
        };

        let rts = ReplayTransactionState {
            ts: ts,
            prevs: prevs,
        };

        if self.ts == prev_ts {
            self.ts = ts - 1;

            if let Bundle::Empty = self.next_transaction {
                mem::replace(&mut self.next_transaction,
                             Bundle::SeedReplay(tag, key, rts));
            } else {
                unreachable!();
            }
        } else {
            self.buffer.push(BufferEntry {
                                 ts: ts,
                                 prev_ts: prev_ts,
                                 transaction: BufferedTransaction::SeedReplay(tag, key, rts),
                             });
        }
    }
}
