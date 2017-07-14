use channel::{tcp, TcpSender, TcpReceiver};

use flow;
use flow::checktable;
use flow::domain;
use flow::payload::ControlReplyPacket;
use flow::persistence;
use flow::prelude::*;
use flow::statistics::{DomainStats, NodeStats};

use std;
use std::cell;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::thread;

use mio;
use petgraph::graph::NodeIndex;
use slog::Logger;

#[derive(Debug)]
pub enum WaitError {
    WrongReply(ControlReplyPacket),
    RecvError(tcp::RecvError),
}

pub struct DomainInputHandle(Vec<TcpSender<Box<Packet>>>);

impl DomainInputHandle {
    pub fn base_send(&mut self, p: Box<Packet>, key: &[usize]) -> Result<(), tcp::Error> {
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
                    Record::Positive(ref r) |
                    Record::Negative(ref r) => &r[key_col],
                    Record::DeleteRequest(ref k) => &k[0],
                };
                if !p.data().iter().all(|r| match *r {
                    Record::Positive(ref r) |
                    Record::Negative(ref r) => &r[key_col] == key,
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

    addrs: Vec<SocketAddr>,
    cr_rxs: Vec<TcpReceiver<ControlReplyPacket>>,
    txs: Vec<TcpSender<Box<Packet>>>,

    // used during booting
    threads: Vec<thread::JoinHandle<()>>,
    boot_args: Vec<mio::net::TcpListener>,
}

impl DomainHandle {
    pub fn new(domain: domain::Index, sharded_by: Sharding) -> Self {
        let mut addrs = Vec::new();
        let mut boot_args = Vec::new();
        {
            let mut add = || {
                let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
                let addr = listener.local_addr().unwrap();
                addrs.push(addr.clone());
                boot_args.push(
                    mio::net::TcpListener::from_listener(listener, &addr).unwrap(),
                );
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
            idx: domain,
            threads: Vec::new(),
            cr_rxs: Vec::new(),
            txs: Vec::new(),
            boot_args,
            addrs,
        }
    }

    pub fn get_input_handle(
        &self,
        channel_coordinator: &Arc<ChannelCoordinator>,
    ) -> DomainInputHandle {
        DomainInputHandle(
            (0..self.shards())
                .map(|i| {
                    channel_coordinator.get_input_tx(&(self.idx, i)).unwrap()
                })
                .collect(),
        )
    }

    pub fn shards(&self) -> usize {
        self.addrs.len()
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
        debug_addr: &Option<SocketAddr>,
        ts: i64,
    ) {
        for (i, addr) in self.addrs.iter().enumerate() {
            channel_coordinator.insert_addr((self.idx, i), addr.clone());
        }

        let mut nodes = Some(Self::build_descriptors(graph, nodes));
        let n = self.boot_args.len();
        for (i, listener) in self.boot_args.drain(..).enumerate() {
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

            let control_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            self.threads.push(domain.boot(
                listener,
                control_listener.local_addr().unwrap(),
                debug_addr.clone(),
            ));
            self.cr_rxs.push(TcpReceiver::new(
                mio::net::TcpStream::from_stream(control_listener.accept().unwrap().0).unwrap(),
            ));
            self.txs
                .push(channel_coordinator.get_tx(&(self.idx, i)).unwrap());
        }
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

    pub fn wait_for_ack(&mut self) -> Result<(), WaitError> {
        for rx in self.cr_rxs.iter_mut() {
            match rx.recv() {
                Ok(ControlReplyPacket::Ack) => {}
                Ok(r) => return Err(WaitError::WrongReply(r)),
                Err(e) => return Err(WaitError::RecvError(e)),
            }
        }
        Ok(())
    }

    pub fn wait_for_state_size(&mut self) -> Result<usize, WaitError> {
        let mut size = 0;
        for rx in self.cr_rxs.iter_mut() {
            match rx.recv() {
                Ok(ControlReplyPacket::StateSize(s)) => size += s,
                Ok(r) => return Err(WaitError::WrongReply(r)),
                Err(e) => return Err(WaitError::RecvError(e)),
            }
        }
        Ok(size)
    }

    pub fn wait_for_statistics(
        &mut self,
    ) -> Result<Vec<(DomainStats, HashMap<NodeIndex, NodeStats>)>, WaitError> {
        let mut stats = Vec::with_capacity(self.cr_rxs.len());
        for rx in self.cr_rxs.iter_mut() {
            match rx.recv() {
                Ok(ControlReplyPacket::Statistics(d, s)) => stats.push((d, s)),
                Ok(r) => return Err(WaitError::WrongReply(r)),
                Err(e) => return Err(WaitError::RecvError(e)),
            }
        }
        Ok(stats)
    }
}
