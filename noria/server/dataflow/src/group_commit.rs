use crate::prelude::*;
use core::convert::TryInto;
use noria::errors::{internal_err, ReadySetResult};
use noria::{internal, internal::LocalOrNot, invariant_eq, PacketData, TableOperation};
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
    pub fn flush_if_necessary(&mut self) -> ReadySetResult<Option<Box<Packet>>> {
        let now = time::Instant::now();
        let to = self.params.flush_timeout;
        let node = self
            .pending_packets
            .iter()
            .find(|(_, &(first, ref ps))| now.duration_since(first) >= to && !ps.is_empty())
            .map(|(n, _)| n);

        Ok(if let Some(node) = node {
            self.flush_internal(node)?
        } else {
            None
        })
    }

    /// Merge any pending packets.
    fn flush_internal(&mut self, node: LocalNodeIndex) -> ReadySetResult<Option<Box<Packet>>> {
        Self::merge_packets(&mut self.pending_packets[node].1)
    }

    /// Add a new packet to be persisted, and if this triggered a flush return an iterator over the
    /// packets that were written.
    pub fn append(&mut self, p: Box<Packet>) -> ReadySetResult<Option<Box<Packet>>> {
        let node = p.dst();
        let pp = self
            .pending_packets
            .entry(node)
            .or_insert_with(|| (time::Instant::now(), Vec::new()));

        if pp.1.is_empty() {
            pp.0 = time::Instant::now();
        }

        pp.1.push(p);
        Ok(if pp.0.elapsed() >= self.params.flush_timeout {
            self.flush_internal(node)?
        } else {
            None
        })
    }

    /// Returns how long until a flush should occur.
    pub fn duration_until_flush(&self) -> Option<time::Duration> {
        self.pending_packets
            .values()
            .filter(|(_, ps)| !ps.is_empty())
            .map(|p| {
                self.params
                    .flush_timeout
                    .checked_sub(p.0.elapsed())
                    .unwrap_or_else(|| time::Duration::from_millis(0))
            })
            .min()
    }

    fn merge_committed_packets<I>(packets: I) -> ReadySetResult<Option<Box<Packet>>>
    where
        I: Iterator<Item = Box<Packet>>,
    {
        let mut packets = packets.peekable();
        let merged_dst = if let Some(packet) = packets.peek().as_mut() {
            packet.dst()
        } else {
            return Ok(None);
        };

        let mut all_senders = vec![];
        let merged_data = packets.try_fold(Vec::new(), |mut acc, p| -> ReadySetResult<_> {
            match *p {
                Packet::Input {
                    inner,
                    src,
                    senders,
                } => {
                    // SAFETY: inner is local to this node and can be unwrapped.
                    let p = unsafe { inner.take() };
                    let data: Vec<TableOperation> = p
                        .data
                        .try_into()
                        .map_err(|_| internal_err("Input packet data was not of Input type."))?;
                    invariant_eq!(senders.len(), 0);
                    invariant_eq!(merged_dst, p.dst);
                    acc.extend(data);

                    if let Some(src) = src {
                        all_senders.push(src);
                    }
                }
                _ => internal!(),
            }
            Ok(acc)
        })?;

        Ok(Some(Box::new(Packet::Input {
            inner: LocalOrNot::new(PacketData {
                dst: merged_dst,
                data: PacketPayload::Input(merged_data),
            }),
            src: None,
            senders: all_senders,
        })))
    }

    /// Merge the contents of packets into a single packet, emptying packets in the process.
    #[allow(clippy::vec_box)]
    fn merge_packets(packets: &mut Vec<Box<Packet>>) -> ReadySetResult<Option<Box<Packet>>> {
        if packets.is_empty() {
            return Ok(None);
        }

        Self::merge_committed_packets(packets.drain(..))
    }
}
