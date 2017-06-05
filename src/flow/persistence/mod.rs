
use buf_redux::BufWriter;
use buf_redux::strategy::WhenFull;

use serde_json;

use std::fs;
use std::fs::{File, OpenOptions};
use std::mem;
use std::path::PathBuf;
use std::time;

use flow::prelude::*;

/// Parameters to control the operation of GroupCommitQueue.
pub struct Parameters {
    /// Number of elements to buffer before flushing.
    pub queue_capacity: usize,
    /// Amount of time to wait before flushing despite not reaching `queue_capacity`.
    pub flush_timeout: time::Duration,
    /// Whether the output files should be deleted when the GroupCommitQueue is dropped.
    pub delete_on_drop: bool,
}

pub struct GroupCommitQueue {
    /// Packets that are queued to be persisted.
    pending_packets: Vec<Box<Packet>>,

    /// Time when the first packet was inserted into pending_packets, or none if pending_packets is
    /// empty. A flush should occur on or before wait_start + timeout.
    wait_start: Option<time::Instant>,

    /// Handle to the file that packets should be written to. Outside of flush() it should never be
    /// None.
    durable_log: Option<BufWriter<File, WhenFull>>,
    durable_log_path: PathBuf,

    timeout: time::Duration,
    capacity: usize,
    delete_on_drop: bool,
}

impl GroupCommitQueue {
    /// Create a new `GroupCommitQueue`.
    pub fn new(log_filename: &str, params: &Parameters) -> Self {
        let durable_log_path = PathBuf::from(&log_filename);

        // TODO(jmftrindade): Current semantics is to overwrite an existing log.
        // Once we have recovery code, we obviously do not want to overwrite this
        // log before recovering.
        let file = match OpenOptions::new()
                  .read(false)
                  .append(false)
                  .write(true)
                  .create(true)
                  .open(durable_log_path.clone()) {
            Err(reason) => {
                panic!("Unable to open durable log file {}, reason: {}",
                       log_filename,
                       reason)
            }
            Ok(file) => file,
        };

        let durable_log =
            BufWriter::with_capacity_and_strategy(params.queue_capacity * 1024, file, WhenFull);

        Self {
            pending_packets: Vec::with_capacity(params.queue_capacity),
            wait_start: None,

            durable_log: Some(durable_log),
            durable_log_path,

            timeout: params.flush_timeout,
            capacity: params.queue_capacity,
            delete_on_drop: params.delete_on_drop,
        }
    }

    /// Returns whether the given packet should be persisted.
    pub fn should_persist(&self, p: &Box<Packet>, nodes: &DomainNodes) -> bool {
        match **p {
            Packet::Message { ref link, .. } |
            Packet::Transaction { ref link, .. } => {
                let node = &nodes[&link.dst.as_local()].borrow();
                node.is_internal() && node.get_base().is_some()
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
    pub fn flush(&mut self, durable_packets: &mut Vec<Box<Packet>>) {
        assert_eq!(durable_packets.len(), 0);

        if !self.pending_packets.is_empty() {
            self.write_packets();
            let file = self.durable_log.take().unwrap().into_inner().unwrap();
            file.sync_data().unwrap();
            self.durable_log =
                Some(BufWriter::with_capacity_and_strategy(self.capacity * 1024, file, WhenFull));
            self.wait_start = None;
            mem::swap(&mut self.pending_packets, durable_packets)
        }
    }

    /// Add a new packet to be persisted, and if this triggered a flush return an iterator over the
    /// packets that were written.
    pub fn append(&mut self, p: Box<Packet>, durable_packets: &mut Vec<Box<Packet>>) {
        self.pending_packets.push(p);

        if self.pending_packets.len() >= self.capacity {
            self.flush(durable_packets);
        } else if self.wait_start.is_none() {
            self.wait_start = Some(time::Instant::now());
        }
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

impl Drop for GroupCommitQueue {
    fn drop(&mut self) {
        if self.delete_on_drop {
            fs::remove_file(self.durable_log_path.clone()).unwrap();
        }
    }
}
