
use buf_redux::BufWriter;
use buf_redux::strategy::WhenFull;

use serde_json;

use std::fs;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::mem;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time;
use std::vec::Drain;


use flow::domain;
use flow::prelude::*;
use checktable;

/// Indicates to what degree updates should be persisted.
#[derive(Clone)]
pub enum DurabilityMode {
    /// Don't do any durability
    MemoryOnly,
    /// Delete any log files on exit. Useful mainly for tests.
    DeleteOnExit,
    /// Persist updates to disk, and don't delete them later.
    Permanent,
}

/// Parameters to control the operation of GroupCommitQueue.
#[derive(Clone)]
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
            queue_capacity: 1,
            flush_timeout: time::Duration::from_millis(0),
            mode: DurabilityMode::MemoryOnly,
        }
    }
}

pub struct GroupCommitQueueSet {
    /// Packets that are queued to be persisted.
    pending_packets: Map<Vec<Box<Packet>>>,

    /// Time when the first packet was inserted into pending_packets, or none if pending_packets is
    /// empty. A flush should occur on or before wait_start + timeout.
    wait_start: Map<time::Instant>,

    /// Packets that have already been persisted, and should now be handled by the domain. This Vec
    /// is drained immediately after it is filled, so it should be empty any time method is called
    /// on GroupCommitqueueSet.
    durable_packets: Vec<Box<Packet>>,

    /// Passed to the checktable to learn which packets committed. Stored here to avoid an
    /// allocation on the critical path.
    commit_decisions: Vec<bool>,

    /// Name of, and handle to the files that packets should be persisted to.
    files: Map<(PathBuf, BufWriter<File, WhenFull>)>,

    domain_index: domain::Index,
    timeout: time::Duration,
    capacity: usize,
    durability_mode: DurabilityMode,
}

impl GroupCommitQueueSet {
    /// Create a new `GroupCommitQueue`.
    pub fn new(domain_index: domain::Index, params: &Parameters) -> Self {
        assert!(params.queue_capacity > 0);

        Self {
            pending_packets: Map::default(),
            durable_packets: Vec::with_capacity(params.queue_capacity),
            commit_decisions: Vec::with_capacity(params.queue_capacity),
            wait_start: Map::default(),
            files: Map::default(),

            domain_index,
            timeout: params.flush_timeout,
            capacity: params.queue_capacity,
            durability_mode: params.mode.clone(),
        }
    }

    fn create_file(&self, node: &LocalNodeIndex) -> (PathBuf, BufWriter<File, WhenFull>) {
        let filename = format!("soup-log-{}-{}.json", self.domain_index.index(), node.id());

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

        (PathBuf::from(filename), BufWriter::with_capacity(self.capacity * 1024, file))
    }

    /// Returns None for packet types not relevant to persistence, and the node the packet was
    /// directed to otherwise.
    fn packet_destination(p: &Box<Packet>) -> Option<LocalNodeIndex> {
        match **p {
            Packet::Message { ref link, .. } |
            Packet::Transaction { ref link, .. } => Some(link.dst.as_local().clone()),
            _ => None,
        }
    }

    /// Returns whether the given packet should be persisted.
    pub fn destination_is_base(&self, p: &Box<Packet>, nodes: &DomainNodes) -> bool {
        match Self::packet_destination(p) {
            Some(n) => {
                let node = &nodes[&n].borrow();
                node.is_internal() && node.get_base().is_some()
            }
            None => false,
        }
    }

    /// Find the first queue that has timed out waiting for more packets, and flush it to disk.
    pub fn flush(&mut self) -> Drain<Box<Packet>> {
        assert_eq!(self.durable_packets.len(), 0);

        let mut needs_flush = None;
        for (node, wait_start) in self.wait_start.iter() {
            if wait_start.elapsed() >= self.timeout {
                needs_flush = Some(node.as_local().clone());
                break;
            }
        }

        if let Some(node) = needs_flush {
            self.flush_internal(&node);
        }

        self.durable_packets.drain(..)
    }

    /// Flush any pending packets for node to disk (if applicable), moving the packets to self.durable_packets.
    fn flush_internal(&mut self, node: &LocalNodeIndex) {
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
        mem::swap(&mut self.pending_packets[&node], &mut self.durable_packets);
    }

    /// Add a new packet to be persisted, and if this triggered a flush return an iterator over the
    /// packets that were written.
    pub fn append(&mut self, p: Box<Packet>) -> Drain<Box<Packet>> {
        assert_eq!(self.durable_packets.len(), 0);

        let node = Self::packet_destination(&p).unwrap();
        if !self.pending_packets.contains_key(&node) {
            self.pending_packets
                .insert(node.clone(), Vec::with_capacity(self.capacity));
        }

        self.pending_packets[&node].push(p);
        if self.pending_packets[&node].len() >= self.capacity {
            self.flush_internal(&node);
        } else if !self.wait_start.contains_key(&node) {
            self.wait_start.insert(node, time::Instant::now());
        }

        self.durable_packets.drain(..)
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

    fn merge_committed_packets<I>(packets: I) -> Option<Box<Packet>>
        where I: Iterator<Item = Box<Packet>>
    {
        packets.fold(None, |mut acc, p| {
            if acc.is_none() {
                return Some(p);
            }

            match (acc.as_mut().unwrap(), p) {
                (&mut box Packet::Message {
                     link: ref acc_link,
                     data: ref mut acc_data,
                     tracer: ref mut acc_tracer,
                 },
                 box Packet::Message {
                     link: ref p_link,
                     data: ref mut p_data,
                     tracer: ref mut p_tracer,
                 }) |
                (&mut box Packet::Transaction {
                     link: ref acc_link,
                     data: ref mut acc_data,
                     tracer: ref mut acc_tracer,
                     ..
                 },
                 box Packet::Transaction {
                     link: ref p_link,
                     data: ref mut p_data,
                     tracer: ref mut p_tracer,
                     ..
                 }) => {
                    assert_eq!(*acc_link, *p_link);
                    acc_data.append(p_data);

                    if acc_tracer.is_some() && p_tracer.is_some() {
                        p_tracer
                            .as_mut()
                            .unwrap()
                            .send((time::Instant::now(), PacketEvent::Merged))
                            .unwrap();
                    } else if p_tracer.is_some() {
                        *acc_tracer = p_tracer.take();
                    }
                }
                _ => unreachable!(),
            }
            acc
        })
    }

    fn merge_transactional_packets(&mut self,
                                   packets: &mut Vec<Box<Packet>>,
                                   nodes: &DomainNodes,
                                   checktable: &Arc<Mutex<checktable::CheckTable>>)
                                   -> Option<Box<Packet>> {
        let base = if let box Packet::Transaction { ref link, .. } = packets[0] {
            nodes[&link.dst.as_local()]
                .borrow()
                .global_addr()
                .as_global()
                .clone()
        } else {
            unreachable!()
        };

        let (ts, prevs) = {
            let mut checktable = checktable.lock().unwrap();
            match checktable.apply_batch(base, packets, &mut self.commit_decisions) {
                checktable::TransactionResult::Aborted => return None,
                checktable::TransactionResult::Committed(ts, prevs) => (ts, prevs),
            }
        };

        let committed_packets = packets
            .drain(..)
            .zip(self.commit_decisions.iter())
            .map(|(mut packet, committed)| {
                if let box Packet::Transaction {
                           state: TransactionState::Pending(_, ref mut sender), ..
                       } = packet {
                    if *committed {
                        sender.send(Ok(ts)).unwrap();
                    } else {
                        sender.send(Err(())).unwrap();
                    }
                }
                (packet, committed)
            })
            .filter(|&(_, committed)| *committed)
            .map(|(packet, _)| packet);

        let mut merged = Self::merge_committed_packets(committed_packets);
        if let Some(&mut box Packet::Transaction { ref mut state, .. }) = merged.as_mut() {
            *state = TransactionState::Committed(ts, base, prevs);
        } else {
            unreachable!();
        }
        merged
    }

    fn merge_packets(&mut self,
                     packets: &mut Vec<Box<Packet>>,
                     nodes: &DomainNodes,
                     checktable: &Arc<Mutex<checktable::CheckTable>>)
                     -> Option<Box<Packet>> {
        if packets.is_empty() {
            return None;
        }

        match packets[0] {
            box Packet::Message { .. } => Self::merge_committed_packets(packets.drain(..)),
            box Packet::Transaction { .. } => {
                self.merge_transactional_packets(packets, nodes, checktable)
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
