use channel::ChannelSender;
use flow;
use flow::payload::ControlReplyPacket;
use flow::prelude::*;
use flow::debug;
use flow::domain;
use flow::checktable;
use flow::persistence;
use petgraph::graph::NodeIndex;
use std::collections::HashMap;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::cell;
use std::thread;
use slog::Logger;
use flow::statistics::{DomainStats, NodeStats};

#[derive(Debug)]
pub enum WaitError {
    WrongReply(ControlReplyPacket),
    RecvError(mpsc::RecvError),
}

#[derive(Clone)]
pub struct DomainInputHandle(Vec<mpsc::SyncSender<Box<Packet>>>);

impl DomainInputHandle {
    pub fn base_send(
        &mut self,
        p: Box<Packet>,
        key: &[usize],
    ) -> Result<(), mpsc::SendError<Box<Packet>>> {
        if self.0.len() == 1 {
            self.0[0].send(p)
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
                    Record::Positive(ref r) | Record::Negative(ref r) => &r[key_col],
                    Record::DeleteRequest(ref k) => &k[0],
                };
                if !p.data().iter().all(|r| match *r {
                    Record::Positive(ref r) | Record::Negative(ref r) => &r[key_col] == key,
                    Record::DeleteRequest(ref k) => k.len() == 1 && &k[0] == key,
                }) {
                    // batch with different keys to sharded base
                    unimplemented!();
                }
                ::shard_by(key, self.0.len())
            };
            self.0[shard].send(p)
        }
    }
}

pub struct DomainHandle {
    idx: domain::Index,

    txs: Vec<mpsc::SyncSender<Box<Packet>>>,
    in_txs: Vec<mpsc::SyncSender<Box<Packet>>>,
    cr_rxs: Vec<mpsc::Receiver<ControlReplyPacket>>,
    back_txs: Vec<mpsc::Sender<Box<Packet>>>,

    // used during booting
    threads: Vec<thread::JoinHandle<()>>,
    boot_args: Vec<
        (
            mpsc::Receiver<Box<Packet>>,
            mpsc::Receiver<Box<Packet>>,
            mpsc::Receiver<Box<Packet>>,
            mpsc::SyncSender<ControlReplyPacket>,
        ),
    >,

    // used during operation
    tx_buf: Option<Box<Packet>>,
}

impl DomainHandle {
    pub fn new(domain: domain::Index, sharded_by: Sharding) -> Self {
        let mut txs = Vec::new();
        let mut in_txs = Vec::new();
        let mut back_txs = Vec::new();
        let mut cr_rxs = Vec::new();
        let mut boot_args = Vec::new();
        {
            let mut add = || {
                let (in_tx, in_rx) = mpsc::sync_channel(512);
                let (tx, rx) = mpsc::sync_channel(1);
                let (cr_tx, cr_rx) = mpsc::sync_channel(1);
                let (back_tx, back_rx) = mpsc::channel();

                txs.push(tx);
                in_txs.push(in_tx);
                back_txs.push(back_tx);
                cr_rxs.push(cr_rx);
                boot_args.push((rx, in_rx, back_rx, cr_tx));
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
            back_txs,
            cr_rxs,
            idx: domain,
            tx_buf: None,
            threads: Vec::new(),
            boot_args,
        }
    }

    pub fn get_input_handle(&self) -> DomainInputHandle {
        DomainInputHandle(self.in_txs.clone())
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
        readers: &flow::Readers,
        nodes: Vec<(NodeIndex, bool)>,
        persistence_params: &persistence::Parameters,
        checktable: &Arc<Mutex<checktable::CheckTable>>,
        channel_coordinator: &Arc<ChannelCoordinator>,
        debug_tx: &Option<mpsc::Sender<debug::DebugEvent>>,
        ts: i64,
    ) {
        for (i, ((tx, in_tx), back_tx)) in self.txs
            .iter()
            .zip(self.in_txs.iter())
            .zip(self.back_txs.iter())
            .enumerate()
        {
            channel_coordinator.insert_tx(
                (self.idx, i),
                ChannelSender::LocalSync(tx.clone()),
                ChannelSender::LocalSync(in_tx.clone()),
                ChannelSender::Local(back_tx.clone()),
            );
        }

        let mut nodes = Some(Self::build_descriptors(graph, nodes));
        let n = self.boot_args.len();
        for (i, (rx, in_rx, back_rx, cr_tx)) in self.boot_args.drain(..).enumerate() {
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
                readers,
                persistence_params.clone(),
                checktable.clone(),
                channel_coordinator.clone(),
                ts,
            );
            self.threads
                .push(domain.boot(rx, in_rx, back_rx, cr_tx, debug_tx.clone()));
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
            } => box Packet::AddNode {
                node: node.clone(),
                parents: parents.clone(),
            },
            Packet::AddBaseColumn { .. } | Packet::DropBaseColumn { .. } => {
                unreachable!("sharded base node")
            }
            Packet::UpdateEgress {
                ref node,
                ref new_tx,
                ref new_tag,
            } => box Packet::UpdateEgress {
                node: node.clone(),
                new_tx: new_tx.clone(),
                new_tag: new_tag.clone(),
            },
            Packet::UpdateSharder {
                ref node,
                ref new_txs,
            } => box Packet::UpdateSharder {
                node: node.clone(),
                new_txs: new_txs.clone(),
            },
            Packet::AddStreamer {
                ref node,
                ref new_streamer,
            } => box Packet::AddStreamer {
                node: node.clone(),
                new_streamer: new_streamer.clone(),
            },
            Packet::PrepareState {
                ref node,
                ref state,
            } => box Packet::PrepareState {
                node: node.clone(),
                state: state.clone(),
            },
            Packet::StateSizeProbe { ref node } => {
                box Packet::StateSizeProbe { node: node.clone() }
            }
            Packet::SetupReplayPath {
                ref tag,
                ref source,
                ref path,
                notify_done,
                ref trigger,
            } => box Packet::SetupReplayPath {
                tag: tag.clone(),
                source: source.clone(),
                path: path.clone(),
                notify_done,
                trigger: trigger.clone(),
            },
            Packet::RequestPartialReplay { ref tag, ref key } => box Packet::RequestPartialReplay {
                tag: tag.clone(),
                key: key.clone(),
            },
            Packet::StartReplay { ref tag, ref from } => box Packet::StartReplay {
                tag: tag.clone(),
                from: from.clone(),
            },
            Packet::Ready {
                ref node,
                ref index,
            } => box Packet::Ready {
                node: node.clone(),
                index: index.clone(),
            },
            Packet::Quit => box Packet::Quit,
            Packet::StartMigration {
                ref at,
                ref prev_ts,
            } => box Packet::StartMigration {
                at: at.clone(),
                prev_ts: prev_ts.clone(),
            },
            Packet::CompleteMigration {
                ref at,
                ref ingress_from_base,
                ref egress_for_base,
            } => box Packet::CompleteMigration {
                at: at.clone(),
                ingress_from_base: ingress_from_base.clone(),
                egress_for_base: egress_for_base.clone(),
            },
            Packet::GetStatistics => box Packet::GetStatistics,
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

    pub fn wait_for_ack(&self) -> Result<(), WaitError> {
        for rx in &self.cr_rxs {
            match rx.recv() {
                Ok(ControlReplyPacket::Ack(_)) => {}
                Ok(r) => return Err(WaitError::WrongReply(r)),
                Err(e) => return Err(WaitError::RecvError(e)),
            }
        }
        Ok(())
    }

    pub fn wait_for_state_size(&self) -> Result<usize, WaitError> {
        let mut size = 0;
        for rx in &self.cr_rxs {
            match rx.recv() {
                Ok(ControlReplyPacket::StateSize(s)) => size += s,
                Ok(r) => return Err(WaitError::WrongReply(r)),
                Err(e) => return Err(WaitError::RecvError(e)),
            }
        }
        Ok(size)
    }

    pub fn wait_for_statistics(
        &self,
    ) -> Result<Vec<(DomainStats, HashMap<NodeIndex, NodeStats>)>, WaitError> {
        let mut stats = Vec::with_capacity(self.cr_rxs.len());
        for rx in &self.cr_rxs {
            match rx.recv() {
                Ok(ControlReplyPacket::Statistics(d, s)) => stats.push((d, s)),
                Ok(r) => return Err(WaitError::WrongReply(r)),
                Err(e) => return Err(WaitError::RecvError(e)),
            }
        }
        Ok(stats)
    }
}
