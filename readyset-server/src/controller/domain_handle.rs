use std::collections::HashMap;

use array2::Array2;
use dataflow::prelude::*;
use dataflow::DomainRequest;
use futures::{stream, StreamExt, TryStreamExt};
use serde::de::DeserializeOwned;
use tracing::error;

use crate::controller::{Worker, WorkerIdentifier};
use crate::worker::WorkerRequestKind;

/// A `DomainHandle` is a handle that allows communicating with all of the replicas and shards of a
/// given domain.
#[derive(Clone)]
pub(super) struct DomainHandle {
    idx: DomainIndex,
    /// Maps from shard index, to replica index, to (optional) address of the worker running that
    /// replica of that shard of the domain
    shards: Array2<Option<WorkerIdentifier>>,
}

impl DomainHandle {
    pub fn new(idx: DomainIndex, shards: Array2<Option<WorkerIdentifier>>) -> Self {
        Self { idx, shards }
    }

    pub(super) fn index(&self) -> DomainIndex {
        self.idx
    }

    pub(super) fn shards(&self) -> impl Iterator<Item = &[Option<WorkerIdentifier>]> {
        self.shards.rows()
    }

    /// Returns the number of times this domain is sharded
    pub(super) fn num_shards(&self) -> usize {
        self.shards.num_rows()
    }

    /// Returns the number of times this domain is replicated
    pub(super) fn num_replicas(&self) -> usize {
        self.shards.row_size()
    }

    /// Have all replicas of all shards of this domain been placed onto a worker?
    pub(super) fn all_replicas_placed(&self) -> bool {
        self.shards.cells().iter().all(|addr| addr.is_some())
    }

    /// Merge all addresses from the given [`DomainHandle`] into this [`DomainHandle`]
    pub(super) fn merge(&mut self, other: DomainHandle) {
        debug_assert_eq!(other.idx, self.idx);
        for (pos, wid) in other.shards.into_entries() {
            if wid.is_some() {
                self.shards[pos] = wid;
            }
        }
    }

    /// Look up which worker the given shard/replica pair is assigned to
    ///
    /// Returns [`None`] if the replica has not been assigned to a worker.
    pub(super) fn assignment(&self, shard: usize, replica: usize) -> Option<&WorkerIdentifier> {
        self.shards.get((shard, replica)).and_then(|r| r.as_ref())
    }

    pub(super) fn is_assigned_to_worker(&self, worker: &WorkerIdentifier) -> bool {
        self.shards
            .cells()
            .iter()
            .flat_map(|s| s.as_ref())
            .any(|s| s == worker)
    }

    /// Remove the given worker identifier from all shard/replica assignments of this domain
    pub(crate) fn remove_worker(&mut self, wi: &WorkerIdentifier) {
        for assignment in self.shards.cells_mut() {
            if assignment.as_ref() == Some(wi) {
                *assignment = None;
            }
        }
    }

    pub(super) async fn send_to_healthy_shard_replica<R>(
        &self,
        shard: usize,
        replica: usize,
        req: DomainRequest,
        workers: &HashMap<WorkerIdentifier, Worker>,
    ) -> ReadySetResult<Option<R>>
    where
        R: DeserializeOwned,
    {
        let Some(addr) = self.assignment(shard, replica) else {
            return Ok(None);
        };
        if let Some(worker) = workers.get(addr) {
            let req = req.clone();
            let replica_address = ReplicaAddress {
                domain_index: self.idx,
                shard,
                replica,
            };
            Ok(Some(
                worker
                    .rpc(WorkerRequestKind::DomainRequest {
                        replica_address,
                        request: Box::new(req),
                    })
                    .await
                    .map_err(|e| {
                        rpc_err_no_downcast(format!("domain request to {}", replica_address), e)
                    })?,
            ))
        } else {
            error!(%addr, ?req, "tried to send domain request to failed worker");
            Err(ReadySetError::WorkerFailed { uri: addr.clone() })
        }
    }

    pub(super) async fn send_to_healthy_shard<R>(
        &self,
        shard: usize,
        req: DomainRequest,
        workers: &HashMap<WorkerIdentifier, Worker>,
    ) -> ReadySetResult<Vec<Option<R>>>
    where
        R: DeserializeOwned,
    {
        stream::iter(0..self.num_replicas())
            .then(move |replica| {
                self.send_to_healthy_shard_replica(shard, replica, req.clone(), workers)
            })
            .try_collect()
            .await
    }

    /// Send the given [`req`] to the given *healthy* replicas of all shards of this domain. Returns
    /// a 2-dimensional array, indexed in shard, replica order, where the order of replicas matches
    /// the order of replica indexes given, of the results of sending each request. Each result
    /// which was sent to a shard-replica which has not yet been placed onto a worker will be
    /// [`None`]
    pub(super) async fn send_to_healthy_replicas<R, I>(
        &self,
        req: DomainRequest,
        replicas: I,
        workers: &HashMap<WorkerIdentifier, Worker>,
    ) -> ReadySetResult<Array2<Option<R>>>
    where
        R: DeserializeOwned,
        I: IntoIterator<Item = usize> + Clone,
    {
        let results = stream::iter(0..self.num_shards())
            .then(move |shard| {
                let req = req.clone();
                let replicas = replicas.clone();
                stream::iter(replicas)
                    .then(move |replica| {
                        self.send_to_healthy_shard_replica(shard, replica, req.clone(), workers)
                    })
                    .try_collect()
            })
            .try_collect::<Vec<_>>()
            .await?;

        Ok(Array2::from_rows(results))
    }

    /// Send the given [`req`] to all *healthy* shard replicas of this domain. Returns a
    /// 2-dimensional array, indexed in shard, replica order, of the results of sending each
    /// request. Each result which was sent to a domain which has not yet been placed onto a worker
    /// will be [`None`]
    pub(super) async fn send_to_healthy<R>(
        &self,
        req: DomainRequest,
        workers: &HashMap<WorkerIdentifier, Worker>,
    ) -> ReadySetResult<Array2<Option<R>>>
    where
        R: DeserializeOwned,
    {
        self.send_to_healthy_replicas(req, 0..self.num_replicas(), workers)
            .await
    }

    /// Send the given [`req`] to *all* shard replicas of this domain. Returns a 2-dimensional
    /// array, indexed in shard, replica order, of the results of sending each request. If any of
    /// the replicas of the domain have not yet been placed onto a worker, this function will return
    /// an error.
    pub(super) async fn send_to_all<R>(
        &self,
        req: DomainRequest,
        workers: &HashMap<WorkerIdentifier, Worker>,
    ) -> ReadySetResult<Array2<R>>
    where
        R: DeserializeOwned,
    {
        self.send_to_healthy(req, workers)
            .await?
            .try_map_cells(|((shard, replica), r)| {
                r.ok_or_else(|| ReadySetError::NoSuchReplica {
                    domain_index: self.index().into(),
                    shard,
                    replica,
                })
            })
    }

    /// Returns a 2-dimensional array mapping shard index, to replica index, to whether that shard
    /// replica has been placed onto a worker
    pub(crate) fn placed_shard_replicas(&self) -> Array2<bool> {
        self.shards.map(|addr| addr.is_some())
    }
}
