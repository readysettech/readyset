use crate::controller::{Worker, WorkerIdentifier};
use dataflow::prelude::*;
use noria::channel::tcp;
use slog::Logger;
use std::collections::HashMap;
use std::io;

pub(super) struct DomainShardHandle {
    pub(super) worker: WorkerIdentifier,
    pub(super) tx: Box<dyn noria::channel::Sender<Item = Box<Packet>> + Send>,
}

/// A `DomainHandle` is a handle that allows communicating with all of the shards of a given
/// domain.
pub(super) struct DomainHandle {
    pub(super) idx: DomainIndex,
    pub(super) shards: Vec<DomainShardHandle>,
    pub(super) log: Logger,
}

impl DomainHandle {
    pub(super) fn index(&self) -> DomainIndex {
        self.idx
    }

    pub(super) fn shards(&self) -> usize {
        self.shards.len()
    }

    pub(super) fn assignment(&self, shard: usize) -> WorkerIdentifier {
        self.shards[shard].worker
    }

    pub(super) fn assigned_to_worker(&self, worker: &WorkerIdentifier) -> bool {
        self.shards.iter().any(|s| s.worker == *worker)
    }

    pub(super) fn send_to_healthy(
        &mut self,
        p: Box<Packet>,
        workers: &HashMap<WorkerIdentifier, Worker>,
    ) -> Result<(), tcp::SendError> {
        for shard in self.shards.iter_mut() {
            if workers[&shard.worker].healthy {
                shard.tx.send(p.clone())?;
            } else {
                error!(
                    self.log,
                    "Tried to send packet to failed worker {:?}; ignoring!", shard.worker
                );
                return Err(io::Error::new(io::ErrorKind::BrokenPipe, "worker failed").into());
            }
        }
        Ok(())
    }

    pub(super) fn send_to_healthy_shard(
        &mut self,
        i: usize,
        p: Box<Packet>,
        workers: &HashMap<WorkerIdentifier, Worker>,
    ) -> Result<(), tcp::SendError> {
        if workers[&self.shards[i].worker].healthy {
            self.shards[i].tx.send(p)?;
        } else {
            error!(
                self.log,
                "Tried to send packet to failed worker {:?}; ignoring!", &self.shards[i].worker
            );
            return Err(io::Error::new(io::ErrorKind::BrokenPipe, "worker failed").into());
        }
        Ok(())
    }
}
