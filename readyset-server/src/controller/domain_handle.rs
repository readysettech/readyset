use std::collections::HashMap;

use dataflow::prelude::*;
use dataflow::DomainRequest;
use serde::de::DeserializeOwned;
use tracing::error;

use crate::controller::{Worker, WorkerIdentifier};
use crate::worker::WorkerRequestKind;

/// A `DomainHandle` is a handle that allows communicating with a domain. With standalone-only
/// Readyset there is exactly one worker, so a domain is either placed on that worker or
/// unscheduled.
#[derive(Clone)]
pub(super) struct DomainHandle {
    idx: DomainIndex,
    /// The worker running this domain, or [`None`] if it has not been scheduled yet.
    worker: Option<WorkerIdentifier>,
}

impl DomainHandle {
    pub fn new(idx: DomainIndex, worker: Option<WorkerIdentifier>) -> Self {
        Self { idx, worker }
    }

    pub(super) fn index(&self) -> DomainIndex {
        self.idx
    }

    /// The worker this domain is running on, if any.
    pub(super) fn worker(&self) -> Option<&WorkerIdentifier> {
        self.worker.as_ref()
    }

    /// Has this domain been placed onto a worker?
    pub(super) fn is_placed(&self) -> bool {
        self.worker.is_some()
    }

    /// Take the worker assignment from `other` if this handle is unassigned.
    pub(super) fn merge(&mut self, other: DomainHandle) {
        debug_assert_eq!(other.idx, self.idx);
        if other.worker.is_some() {
            self.worker = other.worker;
        }
    }

    /// The replica address of this domain's single replica.
    fn replica_address(&self) -> ReplicaAddress {
        ReplicaAddress {
            domain_index: self.idx,
            shard: 0,
            replica: 0,
        }
    }

    /// Iterator over the assigned worker for this domain, yielding zero or one entry.
    pub(super) fn assignments(&self) -> impl Iterator<Item = (ReplicaAddress, &WorkerIdentifier)> {
        let addr = self.replica_address();
        self.worker.as_ref().map(move |wi| (addr, wi)).into_iter()
    }

    pub(super) fn is_assigned_to_worker(&self, worker: &WorkerIdentifier) -> bool {
        self.worker.as_ref() == Some(worker)
    }

    /// Clear the worker assignment if it matches `wi`, returning the replica address that was
    /// previously assigned (if any).
    pub(crate) fn remove_worker(&mut self, wi: &WorkerIdentifier) -> Option<ReplicaAddress> {
        if self.worker.as_ref() == Some(wi) {
            let addr = self.replica_address();
            self.worker = None;
            Some(addr)
        } else {
            None
        }
    }

    /// Drop the worker assignment, if any.
    pub(crate) fn clear_assignment(&mut self) {
        self.worker = None;
    }

    /// Send `req` to the worker running this domain, if one is assigned. Returns [`None`] if no
    /// worker is assigned.
    pub(super) async fn try_send<R>(
        &self,
        req: DomainRequest,
        workers: &HashMap<WorkerIdentifier, Worker>,
    ) -> ReadySetResult<Option<R>>
    where
        R: DeserializeOwned,
    {
        let Some(addr) = self.worker.as_ref() else {
            return Ok(None);
        };
        let Some(worker) = workers.get(addr) else {
            error!(%addr, ?req, "tried to send domain request to failed worker");
            return Err(ReadySetError::WorkerFailed { uri: addr.clone() });
        };
        let replica_address = self.replica_address();
        Ok(Some(
            worker
                .rpc(WorkerRequestKind::DomainRequest {
                    replica_address,
                    request: Box::new(req),
                })
                .await
                .map_err(|e| {
                    rpc_err_no_downcast(format!("domain request to {replica_address}"), e)
                })?,
        ))
    }

    /// Like [`Self::try_send`], but returns an error if no worker has been assigned.
    pub(super) async fn send<R>(
        &self,
        req: DomainRequest,
        workers: &HashMap<WorkerIdentifier, Worker>,
    ) -> ReadySetResult<R>
    where
        R: DeserializeOwned,
    {
        self.try_send(req, workers)
            .await?
            .ok_or_else(|| ReadySetError::NoSuchReplica {
                domain_index: self.idx.into(),
                shard: 0,
                replica: 0,
            })
    }
}
