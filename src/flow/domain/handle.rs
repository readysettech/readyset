use channel::ChannelSender;
use flow::prelude::*;
use flow::domain;
use flow::checktable;
use flow::persistence;
use petgraph::graph::NodeIndex;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::cell;
use std::thread;
use slog::Logger;

pub struct DomainHandle {
    idx: domain::Index,

    txs: Vec<mpsc::SyncSender<Box<Packet>>>,
    in_txs: Vec<mpsc::SyncSender<Box<Packet>>>,

    // used during booting
    threads: Vec<thread::JoinHandle<()>>,
    rxs: Vec<(mpsc::Receiver<Box<Packet>>, mpsc::Receiver<Box<Packet>>)>,

    // used during operation
    tx_buf: Option<Box<Packet>>,
}

impl Clone for DomainHandle {
    /// Note that a `DomainHandle` clone does not also track the underlying threads.
    fn clone(&self) -> Self {
        DomainHandle {
            idx: self.idx,
            txs: self.txs.clone(),
            in_txs: self.in_txs.clone(),
            threads: Vec::new(),
            rxs: Vec::new(),
            tx_buf: None,
        }
    }
}

impl DomainHandle {
    pub fn new(domain: domain::Index, sharded_by: Sharding) -> Self {
        let mut txs = Vec::new();
        let mut in_txs = Vec::new();
        let mut rxs = Vec::new();
        {
            let mut add = || {
                let (in_tx, in_rx) = mpsc::sync_channel(256);
                let (tx, rx) = mpsc::sync_channel(1);
                txs.push(tx);
                in_txs.push(in_tx);
                rxs.push((rx, in_rx));
            };
            add();
            match sharded_by {
                Sharding::None => {}
                _ => {
                    // NOTE: warning to future self
                    // the code currently relies on the fact that the domains that are sharded by
                    // the same key *also* have the same number of shards. if this no longer holds,
                    // we actually need to do a shuffle, otherwise writes will end up on the wrong
                    // shard. keep that in mind.
                    for _ in 1..::SHARDS {
                        add();
                    }
                }
            }
        }
        DomainHandle {
            txs,
            in_txs,
            rxs,
            idx: domain,
            tx_buf: None,
            threads: Vec::new(),
        }
    }

    pub fn get_txs(&self) -> Vec<mpsc::SyncSender<Box<Packet>>> {
        self.txs.clone()
    }

    pub fn shards(&self) -> usize {
        self.txs.len()
    }

    fn build_descriptors(graph: &mut Graph, nodes: Vec<(NodeIndex, bool)>) -> DomainNodes {
        nodes
            .into_iter()
            .map(|(ni, _)| {
                let node = graph.node_weight_mut(ni).unwrap().take();
                node.finalize(graph)
            })
            .map(|nd| (*nd.local_addr(), cell::RefCell::new(nd)))
            .collect()
    }

    pub fn boot(
        &mut self,
        log: &Logger,
        graph: &mut Graph,
        nodes: Vec<(NodeIndex, bool)>,
        persistence_params: &persistence::Parameters,
        checktable: &Arc<Mutex<checktable::CheckTable>>,
        channel_coordinator: &Arc<ChannelCoordinator>,
        ts: i64,
    ) {
        for (i, (tx, in_tx)) in self.txs.iter().zip(self.in_txs.iter()).enumerate() {
            channel_coordinator.insert_tx(
                (self.idx, i),
                ChannelSender::LocalSync(tx.clone()),
                ChannelSender::LocalSync(in_tx.clone()),
            );
        }

        let mut nodes = Some(Self::build_descriptors(graph, nodes));
        let n = self.rxs.len();
        for (i, (rx, in_rx)) in self.rxs.drain(..).enumerate() {
            let logger = if n == 1 {
                log.new(o!("domain" => self.idx.index()))
            } else {
                log.new(o!("domain" => format!("{}.{}", self.idx.index(), i)))
            };
            let nodes = if i == n - 1 {
                nodes.take().unwrap()
            } else {
                nodes.clone().unwrap()
            };
            let domain = domain::Domain::new(
                logger,
                self.idx,
                i,
                n,
                nodes,
                persistence_params.clone(),
                checktable.clone(),
                channel_coordinator.clone(),
                ts,
            );
            self.threads.push(domain.boot(rx, in_rx));
        }
    }

    pub fn wait(&mut self) {
        for t in self.threads.drain(..) {
            t.join().unwrap();
        }
    }

    #[inline]
    fn nextp(&mut self, i: usize, of: usize) -> Box<Packet> {
        assert!(self.tx_buf.is_some());
        if i == of - 1 {
            return self.tx_buf.take().unwrap();
        }

        // DomainHandles are only used by Blender and its derivatives, never internally in the
        // graph. Because of this, we know that we can only receive one of a small set of Packet
        // types (all of which are clone-able). We deal with those here:
        let p = self.tx_buf.as_ref().unwrap();
        match **p {
            Packet::Message { .. } => box p.clone_data(),
            Packet::Transaction { .. } => box p.clone_data(),
            Packet::AddNode {
                ref node,
                ref parents,
            } => {
                box Packet::AddNode {
                    node: node.clone(),
                    parents: parents.clone(),
                }
            }
            Packet::AddBaseColumn { .. } |
            Packet::DropBaseColumn { .. } => unreachable!("sharded base node"),
            Packet::UpdateEgress {
                ref node,
                ref new_tx,
                ref new_tag,
            } => {
                box Packet::UpdateEgress {
                    node: node.clone(),
                    new_tx: new_tx.clone(),
                    new_tag: new_tag.clone(),
                }
            }
            Packet::UpdateSharder {
                ref node,
                ref new_txs,
            } => {
                box Packet::UpdateSharder {
                    node: node.clone(),
                    new_txs: new_txs.clone(),
                }
            }
            Packet::AddStreamer {
                ref node,
                ref new_streamer,
            } => {
                box Packet::AddStreamer {
                    node: node.clone(),
                    new_streamer: new_streamer.clone(),
                }
            }
            Packet::RequestUnboundedTx(ref tx) => box Packet::RequestUnboundedTx(tx.clone()),
            Packet::PrepareState {
                ref node,
                ref state,
            } => {
                box Packet::PrepareState {
                    node: node.clone(),
                    state: state.clone(),
                }
            }
            Packet::StateSizeProbe { ref node, ref ack } => {
                box Packet::StateSizeProbe {
                    node: node.clone(),
                    ack: ack.clone(),
                }
            }
            Packet::SetupReplayPath {
                ref tag,
                ref source,
                ref path,
                ref done_tx,
                ref trigger,
                ref ack,
            } => {
                box Packet::SetupReplayPath {
                    tag: tag.clone(),
                    source: source.clone(),
                    path: path.clone(),
                    done_tx: done_tx.clone(),
                    trigger: trigger.clone(),
                    ack: ack.clone(),
                }
            }
            Packet::RequestPartialReplay { ref tag, ref key } => {
                box Packet::RequestPartialReplay {
                    tag: tag.clone(),
                    key: key.clone(),
                }
            }
            Packet::StartReplay { ref tag, ref from } => {
                box Packet::StartReplay {
                    tag: tag.clone(),
                    from: from.clone(),
                }
            }
            Packet::Ready {
                ref node,
                ref index,
                ref ack,
            } => {
                box Packet::Ready {
                    node: node.clone(),
                    index: index.clone(),
                    ack: ack.clone(),
                }
            }
            Packet::Quit => box Packet::Quit,
            Packet::StartMigration {
                ref at,
                ref prev_ts,
                ref ack,
            } => {
                box Packet::StartMigration {
                    at: at.clone(),
                    prev_ts: prev_ts.clone(),
                    ack: ack.clone(),
                }
            }
            Packet::CompleteMigration {
                ref at,
                ref ingress_from_base,
                ref egress_for_base,
            } => {
                box Packet::CompleteMigration {
                    at: at.clone(),
                    ingress_from_base: ingress_from_base.clone(),
                    egress_for_base: egress_for_base.clone(),
                }
            }
            Packet::GetStatistics(..) => unimplemented!(),
            _ => unreachable!(),
        }
    }

    pub fn send(&mut self, p: Box<Packet>) -> Result<(), mpsc::SendError<Box<Packet>>> {
        self.tx_buf = Some(p);
        let txs = self.txs.len();
        for i in 0..txs {
            let p = self.nextp(i, txs);
            self.txs[i].send(p)?;
        }
        Ok(())
    }

    pub fn send_to_shard(
        &mut self,
        i: usize,
        p: Box<Packet>,
    ) -> Result<(), mpsc::SendError<Box<Packet>>> {
        self.txs[i].send(p)
    }

    pub fn base_send(
        &mut self,
        p: Box<Packet>,
        key: &[usize],
    ) -> Result<(), mpsc::SendError<Box<Packet>>> {
        if self.txs.len() == 1 {
            self.in_txs[0].send(p)
        } else {
            if key.is_empty() {
                unreachable!("sharded base without a key?");
            }
            if key.len() != 1 {
                // base sharded by complex key
                unimplemented!();
            }
            let key_col = key[0];
            let shard = {
                let key = match p.data()[0] {
                    Record::Positive(ref r) |
                    Record::Negative(ref r) => &r[key_col],
                    Record::DeleteRequest(ref k) => &k[0],
                };
                if !p.data().iter().all(|r| match *r {
                    Record::Positive(ref r) |
                    Record::Negative(ref r) => &r[key_col] == key,
                    Record::DeleteRequest(ref k) => k.len() == 1 && &k[0] == key,
                })
                {
                    // batch with different keys to sharded base
                    unimplemented!();
                }
                ::shard_by(key, self.txs.len())
            };
            self.in_txs[shard].send(p)
        }
    }
}
