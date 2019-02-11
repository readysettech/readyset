use super::{Worker, WorkerIdentifier};
use dataflow::prelude::*;
use noria::channel::tcp;
use slog::Logger;
use std::collections::HashMap;
use std::io;

pub(crate) struct DomainShardHandle {
    pub(crate) worker: WorkerIdentifier,
    pub(crate) tx: Box<dyn noria::channel::Sender<Item = Box<Packet>> + Send>,
}

/// A `DomainHandle` is a handle that allows communicating with all of the shards of a given
/// domain.
pub(crate) struct DomainHandle {
    pub(crate) idx: DomainIndex,
    pub(crate) shards: Vec<DomainShardHandle>,
    pub(crate) log: Logger,
}

impl DomainHandle {
    pub fn index(&self) -> DomainIndex {
        self.idx
    }

    pub fn shards(&self) -> usize {
        self.shards.len()
    }

    pub fn assignment(&self, shard: usize) -> WorkerIdentifier {
        self.shards[shard].worker.clone()
    }

    pub fn assigned_to_worker(&self, worker: &WorkerIdentifier) -> bool {
        self.shards.iter().any(|s| s.worker == *worker)
    }

    pub(crate) fn send_to_healthy(
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

    pub(crate) fn send_to_healthy_shard(
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
