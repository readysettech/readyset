use buf_redux::BufWriter;
use buf_redux::strategy::WhenFull;

use serde_json;

use std::fs;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::time;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::rc::Rc;

use debug::DebugEventType;
use domain;
use prelude::*;
use checktable;

use channel::TcpSender;

/// Indicates to what degree updates should be persisted.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum DurabilityMode {
    /// Don't do any durability
    MemoryOnly,
    /// Delete any log files on exit. Useful mainly for tests.
    DeleteOnExit,
    /// Persist updates to disk, and don't delete them later.
    Permanent,
}

/// Parameters to control the operation of GroupCommitQueue.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Parameters {
    /// Number of elements to buffer before flushing.
    pub queue_capacity: usize,
    /// Amount of time to wait before flushing despite not reaching `queue_capacity`.
    pub flush_timeout: time::Duration,
    /// Whether the output files should be deleted when the GroupCommitQueue is dropped.
    pub mode: DurabilityMode,
}

impl Default for Parameters {
    fn default() -> Self {
        Self {
            queue_capacity: 256,
            flush_timeout: time::Duration::from_millis(1),
            mode: DurabilityMode::MemoryOnly,
        }
    }
}

impl Parameters {
    /// Parameters to control the persistence mode, and parameters related to persistence.
    ///
    /// Three modes are available:
    ///
    ///  1. `DurabilityMode::Permanent`: all writes to base nodes should be written to disk.
    ///  2. `DurabilityMode::DeleteOnExit`: all writes are written to disk, but the log is
    ///     deleted once the `Blender` is dropped. Useful for tests.
    ///  3. `DurabilityMode::MemoryOnly`: no writes to disk, store all writes in memory.
    ///     Useful for baseline numbers.
    ///
    /// `queue_capacity` indicates the number of packets that should be buffered until
    /// flushing, and `flush_timeout` indicates the length of time to wait before flushing
    /// anyway.
    pub fn new(mode: DurabilityMode, queue_capacity: usize, flush_timeout: time::Duration) -> Self {
        Self {
            queue_capacity,
            flush_timeout,
            mode,
        }
    }
}

pub struct GroupCommitQueueSet {
    /// Packets that are queued to be persisted.
    pending_packets: Map<Vec<Box<Packet>>>,

    /// Time when the first packet was inserted into pending_packets, or none if pending_packets is
    /// empty. A flush should occur on or before wait_start + timeout.
    wait_start: Map<time::Instant>,

    /// Name of, and handle to the files that packets should be persisted to.
    files: Map<(PathBuf, BufWriter<File, WhenFull>)>,

    transaction_reply_txs: HashMap<SocketAddr, TcpSender<Result<i64, ()>>>,

    domain_index: domain::Index,
    domain_shard: usize,
    timeout: time::Duration,
    capacity: usize,
    durability_mode: DurabilityMode,

    checktable: Rc<checktable::CheckTableClient>,
}

impl GroupCommitQueueSet {
    /// Create a new `GroupCommitQueue`.
    pub fn new(
        domain_index: domain::Index,
        domain_shard: usize,
        params: &Parameters,
        checktable: Rc<checktable::CheckTableClient>,
    ) -> Self {
        assert!(params.queue_capacity > 0);

        Self {
            pending_packets: Map::default(),
            wait_start: Map::default(),
            files: Map::default(),

            domain_index,
            domain_shard,
            timeout: params.flush_timeout,
            capacity: params.queue_capacity,
            durability_mode: params.mode.clone(),
            transaction_reply_txs: HashMap::new(),
            checktable,
        }
    }

    fn create_file(&self, node: &LocalNodeIndex) -> (PathBuf, BufWriter<File, WhenFull>) {
        let filename = format!(
            "soup-log-{}_{}-{}.json",
            self.domain_index.index(),
            self.domain_shard,
            node.id()
        );

        // TODO(jmftrindade): Current semantics is to overwrite an existing log.
        // Once we have recovery code, we obviously do not want to overwrite this
        // log before recovering.
        let file = OpenOptions::new()
            .read(false)
            .append(false)
            .write(true)
            .create(true)
            .open(PathBuf::from(&filename))
            .unwrap();

        (
            PathBuf::from(filename),
            BufWriter::with_capacity(self.capacity * 1024, file),
        )
    }

    /// Returns None for packet types not relevant to persistence, and the node the packet was
    /// directed to otherwise.
    fn packet_destination(p: &Box<Packet>) -> Option<LocalNodeIndex> {
        match **p {
            Packet::Message { ref link, .. } | Packet::Transaction { ref link, .. } => {
                Some(link.dst)
            }
            _ => None,
        }
    }

    /// Returns whether the given packet should be persisted.
    pub fn should_append(&self, p: &Box<Packet>, nodes: &DomainNodes) -> bool {
        match Self::packet_destination(p) {
            Some(n) => {
                let node = &nodes[&n].borrow();
                node.is_internal() && node.get_base().is_some()
            }
            None => false,
        }
    }

    /// Find the first queue that has timed out waiting for more packets, and flush it to disk.
    pub fn flush_if_necessary(&mut self, nodes: &DomainNodes) -> Option<Box<Packet>> {
        let mut needs_flush = None;
        for (node, wait_start) in self.wait_start.iter() {
            if wait_start.elapsed() >= self.timeout {
                needs_flush = Some(node);
                break;
            }
        }

        needs_flush.and_then(|node| self.flush_internal(&node, nodes))
    }

    /// Flush any pending packets for node to disk (if applicable), and return a merged packet.
    fn flush_internal(
        &mut self,
        node: &LocalNodeIndex,
        nodes: &DomainNodes,
    ) -> Option<Box<Packet>> {
        match self.durability_mode {
            DurabilityMode::DeleteOnExit | DurabilityMode::Permanent => {
                if !self.files.contains_key(node) {
                    let file = self.create_file(node);
                    self.files.insert(node.clone(), file);
                }

                let mut file = &mut self.files[node].1;
                {
                    let data_to_flush: Vec<_> = self.pending_packets[&node]
                        .iter()
                        .map(|p| match **p {
                            Packet::Transaction { ref data, .. } |
                            Packet::Message { ref data, .. } => data,
                            _ => unreachable!(),
                        })
                        .collect();
                    serde_json::to_writer(&mut file, &data_to_flush).unwrap();
                }

                file.flush().unwrap();
                file.get_mut().sync_data().unwrap();
            }
            DurabilityMode::MemoryOnly => {}
        }

        self.wait_start.remove(node);
        Self::merge_packets(
            &mut self.pending_packets[node],
            nodes,
            &self.checktable,
            &mut self.transaction_reply_txs,
        )
    }

    /// Add a new packet to be persisted, and if this triggered a flush return an iterator over the
    /// packets that were written.
    pub fn append<'a>(&mut self, p: Box<Packet>, nodes: &DomainNodes) -> Option<Box<Packet>> {
        let node = Self::packet_destination(&p).unwrap();
        if !self.pending_packets.contains_key(&node) {
            self.pending_packets
                .insert(node.clone(), Vec::with_capacity(self.capacity));
        }

        self.pending_packets[&node].push(p);
        if self.pending_packets[&node].len() >= self.capacity {
            return self.flush_internal(&node, nodes);
        } else if !self.wait_start.contains_key(&node) {
            self.wait_start.insert(node, time::Instant::now());
        }
        None
    }

    /// Returns how long until a flush should occur.
    pub fn duration_until_flush(&self) -> Option<time::Duration> {
        self.wait_start
            .values()
            .map(|i| {
                self.timeout
                    .checked_sub(i.elapsed())
                    .unwrap_or(time::Duration::from_millis(0))
            })
            .min()
    }

    fn merge_committed_packets<I>(
        packets: I,
        transaction_state: Option<TransactionState>,
    ) -> Option<Box<Packet>>
    where
        I: Iterator<Item = Box<Packet>>,
    {
        let mut packets = packets.peekable();
        let merged_link = match **packets.peek().as_mut().unwrap() {
            box Packet::Message { ref link, .. } | box Packet::Transaction { ref link, .. } => {
                link.clone()
            }
            _ => unreachable!(),
        };
        let mut merged_tracer: Tracer = None;

        let merged_data = packets.fold(Records::default(), |mut acc, p| {
            match (p,) {
                (box Packet::Message {
                    ref link,
                    ref mut data,
                    ref mut tracer,
                },) |
                (box Packet::Transaction {
                    ref link,
                    ref mut data,
                    ref mut tracer,
                    ..
                },) => {
                    assert_eq!(merged_link, *link);
                    acc.append(data);

                    match (&merged_tracer, tracer) {
                        (&Some((mtag, _)), &mut Some((tag, Some(ref sender)))) => {
                            sender
                                .send(DebugEvent {
                                    instant: time::Instant::now(),
                                    event: DebugEventType::PacketEvent(
                                        PacketEvent::Merged(mtag),
                                        tag,
                                    ),
                                })
                                .unwrap();
                        }
                        (_, tracer @ &mut Some(_)) => {
                            merged_tracer = tracer.take();
                        }
                        _ => {}
                    }
                }
                _ => unreachable!(),
            }
            acc
        });

        match transaction_state {
            Some(merged_state) => Some(Box::new(Packet::Transaction {
                link: merged_link,
                data: merged_data,
                tracer: merged_tracer,
                state: merged_state,
            })),
            None => Some(Box::new(Packet::Message {
                link: merged_link,
                data: merged_data,
                tracer: merged_tracer,
            })),
        }
    }

    /// Merge the contents of packets into a single packet, emptying packets in the process. Panics
    /// if every packet in packets is not a `Packet::Transaction`, or if packets is empty.
    fn merge_transactional_packets(
        packets: &mut Vec<Box<Packet>>,
        nodes: &DomainNodes,
        checktable: &Rc<checktable::CheckTableClient>,
        transaction_reply_txs: &mut HashMap<SocketAddr, TcpSender<Result<i64, ()>>>,
    ) -> Option<Box<Packet>> {
        let mut send_reply = |addr: SocketAddr, reply| {
            transaction_reply_txs
                .entry(addr.clone())
                .or_insert_with(|| TcpSender::connect(&addr, None).unwrap())
                .send(reply)
                .unwrap()
        };

        let base = if let box Packet::Transaction { ref link, .. } = packets[0] {
            nodes[&link.dst].borrow().global_addr()
        } else {
            unreachable!()
        };

        let reply = {
            let transactions = packets
                .iter()
                .enumerate()
                .map(|(i, p)| {
                    let id = checktable::TransactionId(i as u64);
                    if let box Packet::Transaction {
                        ref data,
                        ref state,
                        ..
                    } = *p
                    {
                        match *state {
                            TransactionState::Pending(ref token, _) => {
                                (id, data.clone(), Some(token.clone()))
                            }
                            TransactionState::WillCommit => (id, data.clone(), None),
                            TransactionState::Committed(..) => unreachable!(),
                        }
                    } else {
                        unreachable!();
                    }
                })
                .collect();

            let request = checktable::service::TimestampRequest { transactions, base };
            match checktable.apply_batch(request).unwrap() {
                None => {
                    for packet in packets.drain(..) {
                        if let (box Packet::Transaction {
                            state: TransactionState::Pending(_, ref mut addr),
                            ..
                        },) = (packet,)
                        {
                            send_reply(addr.clone(), Err(()));
                        } else {
                            unreachable!();
                        }
                    }
                    return None;
                }
                Some(mut reply) => {
                    reply.committed_transactions.sort();
                    reply
                }
            }
        };

        let prevs = reply.prevs;
        let timestamp = reply.timestamp;
        let committed_transactions = reply.committed_transactions;

        // TODO: persist list of committed transacions.

        let committed_packets = packets.drain(..).enumerate().filter_map(|(i, mut packet)| {
            if let box Packet::Transaction {
                state: TransactionState::Pending(_, ref mut addr),
                ..
            } = packet
            {
                if committed_transactions
                    .binary_search(&checktable::TransactionId(i as u64))
                    .is_err()
                {
                    send_reply(addr.clone(), Err(()));
                    return None;
                }
                send_reply(addr.clone(), Ok(timestamp));
            }
            Some(packet)
        });

        Self::merge_committed_packets(
            committed_packets,
            Some(TransactionState::Committed(reply.timestamp, base, prevs)),
        )
    }

    /// Merge the contents of packets into a single packet, emptying packets in the process.
    fn merge_packets(
        packets: &mut Vec<Box<Packet>>,
        nodes: &DomainNodes,
        checktable: &Rc<checktable::CheckTableClient>,
        transaction_reply_txs: &mut HashMap<SocketAddr, TcpSender<Result<i64, ()>>>,
    ) -> Option<Box<Packet>> {
        if packets.is_empty() {
            return None;
        }

        match packets[0] {
            box Packet::Message { .. } => Self::merge_committed_packets(packets.drain(..), None),
            box Packet::Transaction { .. } => {
                Self::merge_transactional_packets(packets, nodes, checktable, transaction_reply_txs)
            }
            _ => unreachable!(),
        }
    }
}

impl Drop for GroupCommitQueueSet {
    fn drop(&mut self) {
        if let DurabilityMode::DeleteOnExit = self.durability_mode {
            for &(ref filename, _) in self.files.values() {
                fs::remove_file(filename).unwrap();
            }
        }
    }
}
