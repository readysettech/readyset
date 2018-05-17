use std::time;

use checktable;
use debug::DebugEventType;
use prelude::*;

pub struct GroupCommitQueueSet {
    /// Packets that are queued to be persisted.
    pending_packets: Map<Vec<Box<Packet>>>,

    /// Time when the first packet was inserted into pending_packets, or none if pending_packets is
    /// empty. A flush should occur on or before wait_start + timeout.
    wait_start: Map<time::Instant>,

    params: PersistenceParameters,
}

impl GroupCommitQueueSet {
    /// Create a new `GroupCommitQueue`.
    pub fn new(params: &PersistenceParameters) -> Self {
        assert!(params.queue_capacity > 0);

        Self {
            pending_packets: Map::default(),
            wait_start: Map::default(),

            params: params.clone(),
        }
    }

    /// Returns whether the given packet should be persisted.
    pub fn should_append(&self, p: &Box<Packet>, nodes: &DomainNodes) -> bool {
        if let Packet::Input { .. } = **p {
            assert!(nodes[&p.link().dst].borrow().is_base());
            true
        } else {
            false
        }
    }

    /// Find the first queue that has timed out waiting for more packets, and flush it to disk.
    pub fn flush_if_necessary(
        &mut self,
        nodes: &DomainNodes,
        ex: &Executor,
    ) -> Option<Box<Packet>> {
        let mut needs_flush = None;
        for (node, wait_start) in self.wait_start.iter() {
            if wait_start.elapsed() >= self.params.flush_timeout {
                needs_flush = Some(node);
                break;
            }
        }

        needs_flush.and_then(|node| self.flush_internal(&node, nodes, ex))
    }

    /// Merge any pending packets.
    fn flush_internal(
        &mut self,
        node: &LocalNodeIndex,
        nodes: &DomainNodes,
        ex: &Executor,
    ) -> Option<Box<Packet>> {
        self.wait_start.remove(node);
        Self::merge_packets(&mut self.pending_packets[node], nodes, ex)
    }

    /// Add a new packet to be persisted, and if this triggered a flush return an iterator over the
    /// packets that were written.
    pub fn append<'a>(
        &mut self,
        p: Box<Packet>,
        nodes: &DomainNodes,
        ex: &Executor,
    ) -> Option<Box<Packet>> {
        let node = p.link().dst;
        if !self.pending_packets.contains_key(&node) {
            self.pending_packets
                .insert(node.clone(), Vec::with_capacity(self.params.queue_capacity));
        }

        self.pending_packets[&node].push(p);
        if self.pending_packets[&node].len() >= self.params.queue_capacity {
            return self.flush_internal(&node, nodes, ex);
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
                self.params
                    .flush_timeout
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
        let merged_link = packets.peek().as_mut().unwrap().link().clone();
        let mut merged_tracer: Tracer = None;

        let mut all_senders = vec![];
        let merged_data = packets.fold(Vec::new(), |mut acc, p| {
            match *p {
                Packet::Input {
                    inner: Input { link, data },
                    src,
                    tracer,
                    senders,
                    txn,
                } => {
                    assert_eq!(senders.len(), 0);
                    assert_eq!(merged_link, link);
                    assert!(txn.is_none() || txn.unwrap().is_committed());
                    acc.extend(data);

                    if let Some(src) = src {
                        all_senders.push(src);
                    }

                    match (&merged_tracer, tracer) {
                        (&Some((mtag, _)), Some((tag, Some(sender)))) => {
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
                        (_, mut tracer @ Some(_)) => {
                            merged_tracer = tracer.take();
                        }
                        _ => {}
                    }
                }
                _ => unreachable!(),
            }
            acc
        });

        Some(Box::new(Packet::Input {
            inner: Input {
                link: merged_link,
                data: merged_data,
            },
            src: None,
            tracer: merged_tracer,
            senders: all_senders,
            txn: transaction_state,
        }))
    }

    /// Merge the contents of packets into a single packet, emptying packets in the process. Panics
    /// if every packet in packets is not a `Packet::Transaction`, or if packets is empty.
    fn merge_transactional_packets(
        packets: &mut Vec<Box<Packet>>,
        nodes: &DomainNodes,
        ex: &Executor,
    ) -> Option<Box<Packet>> {
        let send_reply = |p: &Packet, reply| {
            let src = match *p {
                Packet::Transaction { src: Some(src), .. } => src,
                _ => unreachable!(),
            };
            ex.send_back(src, reply);
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
                            TransactionState::Pending(ref token, ..) => {
                                // NOTE: this data.clone() seems sad?
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
            match checktable::with_checktable(|ct| ct.apply_batch(request).unwrap()) {
                None => {
                    for packet in packets.drain(..) {
                        if let Packet::Transaction {
                            state: TransactionState::Pending(..),
                            ..
                        } = *packet
                        {
                            send_reply(&packet, Err(()));
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
        let committed_transactions = reply.committed_transactions;

        // TODO: persist list of committed transacions.

        let committed_packets = packets.drain(..).enumerate().filter_map(|(i, packet)| {
            if let box Packet::Transaction {
                state: TransactionState::Pending(..),
                ..
            } = packet
            {
                if committed_transactions
                    .binary_search(&checktable::TransactionId(i as u64))
                    .is_err()
                {
                    send_reply(&packet, Err(()));
                    return None;
                }
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
        ex: &Executor,
    ) -> Option<Box<Packet>> {
        if packets.is_empty() {
            return None;
        }

        match *packets[0] {
            Packet::Input { txn: None, .. } => {
                Self::merge_committed_packets(packets.drain(..), None)
            }
            Packet::Input { txn: Some(..), .. } => {
                Self::merge_transactional_packets(packets, nodes, ex)
            }
            _ => unreachable!(),
        }
    }
}
