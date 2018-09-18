use api::LocalOrNot;
use prelude::*;
use std::time;

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
    pub fn flush_if_necessary(&mut self) -> Option<Box<Packet>> {
        let mut needs_flush = None;
        for (node, wait_start) in self.wait_start.iter() {
            if wait_start.elapsed() >= self.params.flush_timeout {
                needs_flush = Some(node);
                break;
            }
        }

        needs_flush.and_then(|node| self.flush_internal(&node))
    }

    /// Merge any pending packets.
    fn flush_internal(&mut self, node: &LocalNodeIndex) -> Option<Box<Packet>> {
        self.wait_start.remove(node);
        Self::merge_packets(&mut self.pending_packets[node])
    }

    /// Add a new packet to be persisted, and if this triggered a flush return an iterator over the
    /// packets that were written.
    pub fn append<'a>(&mut self, p: Box<Packet>) -> Option<Box<Packet>> {
        let node = p.link().dst;
        if !self.pending_packets.contains_key(&node) {
            self.pending_packets
                .insert(node.clone(), Vec::with_capacity(self.params.queue_capacity));
        }

        self.pending_packets[&node].push(p);
        if self.pending_packets[&node].len() >= self.params.queue_capacity {
            return self.flush_internal(&node);
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
            }).min()
    }

    fn merge_committed_packets<I>(packets: I) -> Option<Box<Packet>>
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
                    inner,
                    src,
                    senders,
                } => {
                    let Input { link, data, tracer } = unsafe { inner.take() };

                    assert_eq!(senders.len(), 0);
                    assert_eq!(merged_link, link);
                    acc.extend(data);

                    if let Some(src) = src {
                        all_senders.push(src);
                    }

                    match (&merged_tracer, tracer) {
                        (&Some((mtag, _)), Some((tag, Some(sender)))) => {
                            use api::debug::trace::*;
                            sender
                                .send(Event {
                                    instant: time::Instant::now(),
                                    event: EventType::PacketEvent(PacketEvent::Merged(mtag), tag),
                                }).unwrap();
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
            inner: LocalOrNot::new(Input {
                link: merged_link,
                data: merged_data,
                tracer: merged_tracer,
            }),
            src: None,
            senders: all_senders,
        }))
    }

    /// Merge the contents of packets into a single packet, emptying packets in the process.
    fn merge_packets(packets: &mut Vec<Box<Packet>>) -> Option<Box<Packet>> {
        if packets.is_empty() {
            return None;
        }

        Self::merge_committed_packets(packets.drain(..))
    }
}
