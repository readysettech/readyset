use crate::controller::{Worker, WorkerIdentifier};
use crate::worker::WorkerRequestKind;
use dataflow::prelude::*;
use dataflow::DomainRequest;
use serde::de::DeserializeOwned;
use slog::Logger;
use std::collections::HashMap;

/// A `DomainHandle` is a handle that allows communicating with all of the shards of a given
/// domain.
pub(super) struct DomainHandle {
    pub(super) idx: DomainIndex,
    pub(super) shards: Vec<WorkerIdentifier>,
    pub(super) log: Logger,
}

impl DomainHandle {
    pub(super) fn index(&self) -> DomainIndex {
        self.idx
    }

    pub(super) fn shards(&self) -> usize {
        self.shards.len()
    }

    pub(super) fn assignment(&self, shard: usize) -> ReadySetResult<WorkerIdentifier> {
        self.shards
            .get(shard)
            .ok_or_else(|| ReadySetError::NoSuchDomain {
                domain_index: self.idx.index(),
                shard,
            })
            .cloned()
    }

    pub(super) fn assigned_to_worker(&self, worker: &WorkerIdentifier) -> bool {
        self.shards.iter().any(|s| s == worker)
    }

    pub(super) async fn send_to_healthy_shard<T: DeserializeOwned>(
        &self,
        i: usize,
        req: DomainRequest,
        workers: &HashMap<WorkerIdentifier, Worker>,
    ) -> ReadySetResult<T> {
        let addr = self.assignment(i)?;
        let worker =
            workers
                .get(&addr)
                .ok_or_else(|| ReadySetError::UnmappableWorkerIdentifier {
                    ident: addr.to_string(),
                })?;
        if worker.healthy {
            Ok(worker
                .rpc(WorkerRequestKind::DomainRequest {
                    target_idx: self.idx,
                    target_shard: i,
                    request: Box::new(req.clone()),
                })
                .await
                .map_err(|e| {
                    rpc_err_no_downcast(format!("domain request to {}.{}", self.idx.index(), i), e)
                })?)
        } else {
            error!(
                self.log,
                "tried to send domain request to failed worker at {}", addr
            );
            Err(ReadySetError::WorkerFailed { uri: addr.clone() })
        }
    }

    pub(super) async fn send_to_healthy<T: DeserializeOwned>(
        &self,
        req: DomainRequest,
        workers: &HashMap<WorkerIdentifier, Worker>,
    ) -> ReadySetResult<Vec<T>> {
        let mut ret = Vec::with_capacity(self.shards.len());
        for shard in 0..(self.shards.len()) {
            ret.push(
                self.send_to_healthy_shard(shard, req.clone(), workers)
                    .await?,
            );
        }

        Ok(ret)
    }

    pub(super) fn send_to_healthy_blocking<T: DeserializeOwned>(
        &mut self,
        req: DomainRequest,
        workers: &HashMap<WorkerIdentifier, Worker>,
    ) -> ReadySetResult<Vec<T>> {
        tokio::runtime::Handle::current().block_on(self.send_to_healthy(req, workers))
    }

    pub(super) fn send_to_healthy_shard_blocking<T: DeserializeOwned>(
        &mut self,
        i: usize,
        req: DomainRequest,
        workers: &HashMap<WorkerIdentifier, Worker>,
    ) -> ReadySetResult<T> {
        tokio::runtime::Handle::current().block_on(self.send_to_healthy_shard(i, req, workers))
    }
}
