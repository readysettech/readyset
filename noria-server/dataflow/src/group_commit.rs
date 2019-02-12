use noria::internal::LocalOrNot;
use prelude::*;
use std::time;

pub struct GroupCommitQueueSet {
    /// Packets that are queued to be persisted.
    #[allow(clippy::vec_box)]
    pending_packets: Map<(time::Instant, Vec<Box<Packet>>)>,
    params: PersistenceParameters,
}

impl GroupCommitQueueSet {
    /// Create a new `GroupCommitQueue`.
    pub fn new(params: &PersistenceParameters) -> Self {
        Self {
            pending_packets: Map::default(),
            params: params.clone(),
        }
    }

    /// Returns whether the given packet should be persisted.
    pub fn should_append(&self, p: &Packet, nodes: &DomainNodes) -> bool {
        if let Packet::Input { .. } = *p {
            assert!(nodes[p.dst()].borrow().is_base());
            true
        } else {
            false
        }
    }

    /// Find the first queue that has timed out waiting for more packets, and flush it to disk.
    pub fn flush_if_necessary(&mut self) -> Option<Box<Packet>> {
        let to = self.params.flush_timeout;
        let node = self
            .pending_packets
            .iter()
            .find(|(_, &(ref first, ref ps))| first.elapsed() >= to && !ps.is_empty())
            .map(|(n, _)| n);

        if let Some(node) = node {
            self.flush_internal(node)
        } else {
            None
        }
    }

    /// Merge any pending packets.
    fn flush_internal(&mut self, node: LocalNodeIndex) -> Option<Box<Packet>> {
        Self::merge_packets(&mut self.pending_packets[node].1)
    }

    /// Add a new packet to be persisted, and if this triggered a flush return an iterator over the
    /// packets that were written.
    pub fn append(&mut self, p: Box<Packet>) -> Option<Box<Packet>> {
        let node = p.dst();
        let pp = self
            .pending_packets
            .entry(node)
            .or_insert_with(|| (time::Instant::now(), Vec::new()));

        if pp.1.is_empty() {
            pp.0 = time::Instant::now();
        }

        pp.1.push(p);
        if pp.0.elapsed() >= self.params.flush_timeout {
            self.flush_internal(node)
        } else {
            None
        }
    }

    /// Returns how long until a flush should occur.
    pub fn duration_until_flush(&self) -> Option<time::Duration> {
        self.pending_packets
            .values()
            .map(|p| {
                self.params
                    .flush_timeout
                    .checked_sub(p.0.elapsed())
                    .unwrap_or(time::Duration::from_millis(0))
            })
            .min()
    }

    fn merge_committed_packets<I>(packets: I) -> Option<Box<Packet>>
    where
        I: Iterator<Item = Box<Packet>>,
    {
        let mut packets = packets.peekable();
        let merged_dst = packets.peek().as_mut().unwrap().dst();
        let mut merged_tracer: Tracer = None;

        let mut all_senders = vec![];
        let merged_data = packets.fold(Vec::new(), |mut acc, p| {
            match *p {
                Packet::Input {
                    inner,
                    src,
                    senders,
                } => {
                    let Input { dst, data, tracer } = unsafe { inner.take() };

                    assert_eq!(senders.len(), 0);
                    assert_eq!(merged_dst, dst);
                    acc.extend(data);

                    if let Some(src) = src {
                        all_senders.push(src);
                    }

                    match (&merged_tracer, tracer) {
                        (&Some((mtag, _)), Some((tag, Some(sender)))) => {
                            use noria::debug::trace::*;
                            sender
                                .send(Event {
                                    instant: time::Instant::now(),
                                    event: EventType::PacketEvent(PacketEvent::Merged(mtag), tag),
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
            inner: LocalOrNot::new(Input {
                dst: merged_dst,
                data: merged_data,
                tracer: merged_tracer,
            }),
            src: None,
            senders: all_senders,
        }))
    }

    /// Merge the contents of packets into a single packet, emptying packets in the process.
    #[allow(clippy::vec_box)]
    fn merge_packets(packets: &mut Vec<Box<Packet>>) -> Option<Box<Packet>> {
        if packets.is_empty() {
            return None;
        }

        Self::merge_committed_packets(packets.drain(..))
    }
}
