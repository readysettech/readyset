use std::collections::HashMap;

use dataflow::prelude::*;
use dataflow::DomainRequest;
use serde::de::DeserializeOwned;
use tracing::error;

use crate::controller::{Worker, WorkerIdentifier};
use crate::worker::WorkerRequestKind;

/// A `DomainHandle` is a handle that allows communicating with all of the shards of a given
/// domain.
#[derive(Clone)]
pub(super) struct DomainHandle {
    pub(super) idx: DomainIndex,
    pub(super) shards: Vec<WorkerIdentifier>,
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
        shard: usize,
        req: DomainRequest,
        workers: &HashMap<WorkerIdentifier, Worker>,
    ) -> ReadySetResult<T> {
        let addr = self.assignment(shard)?;
        if let Some(worker) = workers.get(&addr) {
            let req = req.clone();
            let replica_address = ReplicaAddress {
                domain_index: self.idx,
                shard,
            };
            Ok(worker
                .rpc(WorkerRequestKind::DomainRequest {
                    replica_address,
                    request: Box::new(req),
                })
                .await
                .map_err(|e| {
                    rpc_err_no_downcast(format!("domain request to {}", replica_address), e)
                })?)
        } else {
            error!(%addr, ?req, "tried to send domain request to failed worker");
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
            let request = req.clone();
            ret.push(self.send_to_healthy_shard(shard, request, workers).await?);
        }

        Ok(ret)
    }
}
