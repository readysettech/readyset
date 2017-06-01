
use buf_redux::BufWriter;
use buf_redux::strategy::WhenFull;

use serde_json;

use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::mem;
use std::path::PathBuf;
use std::time;

use flow::prelude::*;

pub struct GroupCommitQueue {
    /// Ingress nodes whose incoming packets should be persisted. These should be the ingress nodes
    /// that are above base nodes.
    persisted_ingress: HashSet<NodeAddress>,

    pending_packets: Vec<Box<Packet>>,
    /// Time when the first packet was inserted into pending_packets, or none if pending_packets is
    /// empty. A flush should occur on or before wait_start + timeout.
    wait_start: Option<time::Instant>,

    durable_log: Option<BufWriter<File, WhenFull>>,

    timeout: time::Duration,
    capacity: usize,
}

impl GroupCommitQueue {
    /// Create a new `GroupCommitQueue`.
    pub fn new(log_filename: &str, ingress_above_base: HashSet<NodeAddress>) -> Self {
        let capacity = 128;
        let durable_log_path = PathBuf::from(&log_filename);

        // TODO(jmftrindade): Current semantics is to overwrite an existing log.
        // Once we have recovery code, we obviously do not want to overwrite this
        // log before recovering.
        let file = match OpenOptions::new()
                  .read(false)
                  .append(false)
                  .write(true)
                  .create(true)
                  .open(durable_log_path) {
            Err(reason) => {
                panic!("Unable to open durable log file {}, reason: {}",
                       log_filename,
                       reason)
            }
            Ok(file) => file,
        };

        let durable_log = BufWriter::with_capacity_and_strategy(capacity * 1024, file, WhenFull);

        Self {
            persisted_ingress: ingress_above_base,

            pending_packets: Vec::with_capacity(capacity),
            wait_start: None,

            durable_log: Some(durable_log),

            timeout: time::Duration::from_millis(10),
            capacity,
        }
    }

    /// Sets the capacity to the indicated size.
    pub fn set_capacity(&mut self, capacity: usize) {
        self.capacity = capacity;
    }

    /// Sets the timeout to the indicated length.
    pub fn set_timeout(&mut self, timeout: time::Duration) {
        self.timeout = timeout;
    }

    /// Returns whether the given packet should be persisted.
    pub fn should_persist(&self, p: &Box<Packet>) -> bool {
        match **p {
            Packet::Message {ref link, ..} |
            Packet::Transaction {ref link, ..} => {
                self.persisted_ingress.contains(&link.dst)
            }
            _ => false,
        }
    }

    fn write_packets(&mut self) {
        let data_to_flush: Vec<_> = self.pending_packets
            .iter()
            .map(|p| match **p {
                     Packet::Transaction { ref data, ref link, .. } |
                     Packet::Message { ref data, ref link, .. } => (link, data),
                     _ => unreachable!(),
                 })
            .collect();
        serde_json::to_writer(&mut self.durable_log.as_mut().unwrap(), &data_to_flush)
            .unwrap();
    }

    /// Flush any pending packets to disk, and return an iterator over the packets that were
    /// written.
    pub fn flush(&mut self) -> Vec<Box<Packet>> {
        if !self.pending_packets.is_empty() {
            self.write_packets();
            let file = self.durable_log.take().unwrap().into_inner().unwrap();
            file.sync_data().unwrap();
            self.durable_log =
                Some(BufWriter::with_capacity_and_strategy(self.capacity * 1024, file, WhenFull));
            self.wait_start = None;
        }
        mem::replace(&mut self.pending_packets, Vec::with_capacity(self.capacity))
    }

    /// Add a new packet to be persisted, and if this triggered a flush return an iterator over the
    /// packets that were written.
    pub fn append(&mut self, p: Box<Packet>) -> Vec<Box<Packet>> {
        self.pending_packets.push(p);

        if self.pending_packets.len() >= self.capacity {
            return self.flush();
        }

        if self.wait_start.is_none() {
            self.wait_start = Some(time::Instant::now());
        }

        Vec::new()
    }

    /// Returns how long until a flush should occur.
    pub fn duration_until_flush(&self) -> Option<time::Duration> {
        self.wait_start.map(|i| {
                                self.timeout
                                    .checked_sub(i.elapsed())
                                    .unwrap_or(time::Duration::from_millis(0))
                            })
    }
}
