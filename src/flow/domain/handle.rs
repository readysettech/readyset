use channel::{tcp, TcpSender, TcpReceiver};
use channel::poll::{PollingLoop, PollEvent, KeepPolling, StopPolling};

use flow;
use flow::domain;
use flow::payload::ControlReplyPacket;
use flow::persistence;
use flow::prelude::*;
use flow::statistics::{DomainStats, NodeStats};

use std;
use std::cell;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::thread;

use mio;
use petgraph::graph::NodeIndex;
use slog::Logger;

#[derive(Debug)]
pub enum WaitError {
    WrongReply(ControlReplyPacket),
}

pub struct DomainInputHandle {
    txs: Vec<TcpSender<Box<Packet>>>,
    tx_reply: PollingLoop<Result<i64, ()>>,
    tx_reply_addr: SocketAddr,
}

impl DomainInputHandle {
    pub fn base_send(&mut self, p: Box<Packet>, key: &[usize]) -> Result<(), tcp::Error> {
        if self.txs.len() == 1 {
            self.txs[0].send(p)
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
                ::shard_by(key, self.txs.len())
            };
            self.txs[shard].send(p)
        }
    }

    pub fn receive_transaction_result(&mut self) -> Result<i64, ()> {
        let mut result = Err(());
        self.tx_reply.run_polling_loop(|event| match event {
            PollEvent::ResumePolling(_) => KeepPolling,
            PollEvent::Process(r) => {
                result = r;
                StopPolling
            }
            PollEvent::Timeout => unreachable!(),
        });
        result
    }

    pub fn tx_reply_addr(&self) -> SocketAddr {
        self.tx_reply_addr.clone()
    }
}

pub struct DomainHandle {
    idx: domain::Index,

    cr_poll: PollingLoop<ControlReplyPacket>,
    txs: Vec<TcpSender<Box<Packet>>>,

    // used during booting
    threads: Vec<thread::JoinHandle<()>>,
}

impl DomainHandle {
    pub fn new(
        idx: domain::Index,
        sharded_by: Sharding,
        log: &Logger,
        graph: &mut Graph,
        readers: &flow::Readers,
        nodes: Vec<(NodeIndex, bool)>,
        persistence_params: &persistence::Parameters,
        checktable_addr: &SocketAddr,
        channel_coordinator: &Arc<ChannelCoordinator>,
        debug_addr: &Option<SocketAddr>,
        ts: i64,
    ) -> Self {
        // NOTE: warning to future self...
        // the code currently relies on the fact that the domains that are sharded by the same key
        // *also* have the same number of shards. if this no longer holds, we actually need to do a
        // shuffle, otherwise writes will end up on the wrong shard. keep that in mind.
        let num_shards = match sharded_by {
            Sharding::None => 1,
            _ => ::SHARDS,
        };

        let mut txs = Vec::new();
        let mut cr_rxs = Vec::new();
        let mut threads = Vec::new();
        let mut nodes = Some(Self::build_descriptors(graph, nodes));

        for i in 0..num_shards {

            let logger = if num_shards == 1 {
                log.new(o!("domain" => idx.index()))
            } else {
                log.new(o!("domain" => format!("{}.{}", idx.index(), i)))
            };
            let nodes = if i == num_shards - 1 {
                nodes.take().unwrap()
            } else {
                nodes.clone().unwrap()
            };

            let control_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let domain = domain::DomainBuilder {
                index: idx,
                shard: i,
                nshards: num_shards,
                nodes,
                persistence_parameters: persistence_params.clone(),
                ts,
                control_addr: control_listener.local_addr().unwrap(),
                checktable_addr: checktable_addr.clone(),
                debug_addr: debug_addr.clone(),
            };

            threads.push(
                domain.boot(logger, readers.clone(), channel_coordinator.clone()),
            );

            let stream =
                mio::net::TcpStream::from_stream(control_listener.accept().unwrap().0).unwrap();
            cr_rxs.push(TcpReceiver::new(stream));
        }

        let mut cr_poll = PollingLoop::from_receivers(cr_rxs);
        cr_poll.run_polling_loop(|event| match event {
            PollEvent::ResumePolling(_) => KeepPolling,
            PollEvent::Process(ControlReplyPacket::Booted(shard, addr)) => {
                channel_coordinator.insert_addr((idx, shard), addr.clone());
                txs.push(channel_coordinator.get_tx(&(idx, shard)).unwrap());

                if txs.len() == num_shards {
                    StopPolling
                } else {
                    KeepPolling
                }
            }
            PollEvent::Process(_) => unreachable!(),
            PollEvent::Timeout => unreachable!(),
        });

        DomainHandle {
            idx,
            threads,
            cr_poll,
            txs,
        }
    }

    pub fn get_input_handle(
        &self,
        channel_coordinator: &Arc<ChannelCoordinator>,
    ) -> DomainInputHandle {
        let tx_reply = PollingLoop::new();
        let tx_reply_addr = tx_reply.get_listener_addr().unwrap();

        DomainInputHandle {
            txs: (0..self.shards())
                .map(|i| {
                    channel_coordinator.get_input_tx(&(self.idx, i)).unwrap()
                })
                .collect(),
            tx_reply,
            tx_reply_addr,
        }
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

    pub fn wait(&mut self) {
        for t in self.threads.drain(..) {
            t.join().unwrap();
        }
    }

    pub fn send(&mut self, p: Box<Packet>) -> Result<(), tcp::Error> {
        for tx in self.txs.iter_mut() {
            tx.send_ref(&p)?;
        }
        Ok(())
    }

    pub fn send_to_shard(&mut self, i: usize, p: Box<Packet>) -> Result<(), tcp::Error> {
        self.txs[i].send(p)
    }

    fn wait_for_next_reply(&mut self) -> ControlReplyPacket {
        let mut reply = None;
        self.cr_poll.run_polling_loop(|event| match event {
            PollEvent::Process(packet) => {
                reply = Some(packet);
                StopPolling
            }
            PollEvent::ResumePolling(_) => KeepPolling,
            PollEvent::Timeout => unreachable!(),
        });
        reply.unwrap()
    }

    pub fn wait_for_ack(&mut self) -> Result<(), WaitError> {
        for _ in 0..self.shards() {
            match self.wait_for_next_reply() {
                ControlReplyPacket::Ack => {}
                r => return Err(WaitError::WrongReply(r)),
            }
        }
        Ok(())
    }

    pub fn wait_for_state_size(&mut self) -> Result<usize, WaitError> {
        let mut size = 0;
        for _ in 0..self.shards() {
            match self.wait_for_next_reply() {
                ControlReplyPacket::StateSize(s) => size += s,
                r => return Err(WaitError::WrongReply(r)),
            }
        }
        Ok(size)
    }

    pub fn wait_for_statistics(
        &mut self,
    ) -> Result<Vec<(DomainStats, HashMap<NodeIndex, NodeStats>)>, WaitError> {
        let mut stats = Vec::with_capacity(self.shards());
        for _ in 0..self.shards() {
            match self.wait_for_next_reply() {
                ControlReplyPacket::Statistics(d, s) => stats.push((d, s)),
                r => return Err(WaitError::WrongReply(r)),
            }
        }
        Ok(stats)
    }
}
