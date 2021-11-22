//! Scheduling which domains (actually shards of domains) run on which workers
//!
//! The domain scheduling algorithm, which is currently quite simplistic, works as follows:
//!
//! 1. We filter the set of workers in the cluster by two criteria:
//!    a. The worker must be healhty, and
//!    b. The worker can be [configured to only run reader nodes][reader_only], in which case only
//!       domains that contain a reader node can run on that worker
//! 2. Migrations can optionally [be restricted to a single worker][worker] - if so, all
//!    shards of all domains within the migration will be scheduled to that worker, *if* it's valid
//! 3. Otherwise, for each shard in the domain (which is just the list of natural numbers from 0 to the
//!    number of shards exclusive) we either:
//!    a. Run the domain shard on the worker matching its [placement restrictions][], if it has any, or
//!    b. Run it on the worker that has the smallest number of domain shards scheduled onto it.
//!
//! [reader_only]: Worker::reader_only
//! [worker]: Migration::worker
//! [placement restrictions]: DomainPlacementRestriction

use std::collections::HashMap;

use dataflow::prelude::*;
use noria::internal::DomainIndex;

use crate::controller::inner::Leader;
use crate::controller::{DomainPlacementRestriction, NodeRestrictionKey, Worker, WorkerIdentifier};

/// Verifies that the worker `worker` meets the domain placement restrictions of all dataflow nodes
/// that will be placed in a new domain on the worker.  If the set of restrictions in this domain
/// are too stringent, no worker may be able to satisfy the domain placement.
fn worker_meets_restrictions(
    worker: &Worker,
    restrictions: &[&DomainPlacementRestriction],
) -> bool {
    restrictions
        .iter()
        .all(|r| r.worker_volume == worker.volume_id)
}

/// A short-lived struct holding all the information necessary to assign domain shards to workers.
pub(crate) struct Scheduler<'leader, 'migration> {
    valid_workers: Vec<(&'leader WorkerIdentifier, &'leader Worker)>,
    node_restrictions: &'leader HashMap<NodeRestrictionKey, DomainPlacementRestriction>,
    worker_domain_shards: HashMap<&'leader WorkerIdentifier, Vec<(usize, DomainIndex)>>,
    ingredients: &'migration Graph,
}

impl<'leader, 'migration> Scheduler<'leader, 'migration> {
    /// Create a new scheduler, taking information from the given `leader`, optionally restricted to
    /// the given `worker`, and assigning nodes from the given graph of `ingredients`.
    pub(crate) fn new(
        leader: &'leader Leader,
        worker: &'migration Option<WorkerIdentifier>,
        ingredients: &'migration Graph,
    ) -> ReadySetResult<Self> {
        let valid_workers = leader
            .workers
            .iter()
            .filter(|(_, w)| w.healthy)
            .filter(|(wi, _)| worker.iter().all(|target_worker| *target_worker == **wi))
            .collect();

        let mut worker_domain_shards: HashMap<&WorkerIdentifier, Vec<(usize, DomainIndex)>> =
            HashMap::new();
        for (di, dh) in &leader.domains {
            for (shard_i, wi) in dh.shards.iter().enumerate() {
                worker_domain_shards
                    .entry(wi)
                    .or_default()
                    .push((shard_i, *di));
            }
        }

        Ok(Self {
            valid_workers,
            node_restrictions: &leader.node_restrictions,
            worker_domain_shards,
            ingredients,
        })
    }

    /// Decide which workers the shards of the given `domain` (with the given list of `nodes`)
    /// should run on
    ///
    /// Returns a vector of `WorkerIdentifier` to schedule the domain's shards onto, where each
    /// index is a shard index.
    ///
    /// # Invariants
    ///
    /// * `nodes` cannot be empty
    /// * All the nodes in `nodes` must exist in `self.ingredients`
    #[allow(clippy::indexing_slicing)] // documented invariant
    pub(crate) fn schedule_domain(
        &mut self,
        domain_index: DomainIndex,
        nodes: &[(NodeIndex, bool)],
    ) -> ReadySetResult<Vec<WorkerIdentifier>> {
        let num_shards = self.ingredients[nodes[0].0]
            .sharded_by()
            .shards()
            .unwrap_or(1);
        let is_reader_domain = nodes.iter().any(|(n, _)| self.ingredients[*n].is_reader());

        let workers = self
            .valid_workers
            .iter()
            .filter(|(_, worker)| !worker.reader_only || is_reader_domain);

        let mut res = Vec::with_capacity(num_shards);
        for shard in 0..num_shards {
            // Shards of certain dataflow nodes may have restrictions that
            // limit the workers they are placed upon.
            let dataflow_node_restrictions = nodes
                .iter()
                .filter_map(|(n, _)| {
                    let node_name = self.ingredients[*n].name();
                    self.node_restrictions.get(&NodeRestrictionKey {
                        node_name: node_name.into(),
                        shard,
                    })
                })
                .collect::<Vec<_>>();

            let worker_id = if dataflow_node_restrictions.is_empty() {
                // If there are no placement restrictions, pick the worker running the smallest
                // number of domain shards
                workers.clone().min_by_key(|(wi, _)| {
                    self.worker_domain_shards
                        .get(wi)
                        .map(|domain_shards| domain_shards.len())
                        .unwrap_or(0)
                })
            } else {
                // Otherwise, if there are placement restrictions, we select the first worker that
                // meets the placement restrictions. This can lead to imbalance in the number of
                // dataflow nodes placed on each server.
                workers.clone().find(|(_, worker)| {
                    worker_meets_restrictions(worker, &dataflow_node_restrictions)
                })
            }
            .map(|(wi, _)| *wi)
            .ok_or(ReadySetError::NoAvailableWorkers {
                domain_index: domain_index.index(),
                shard,
            })?;

            self.worker_domain_shards
                .entry(worker_id)
                .or_default()
                .push((shard, domain_index));
            res.push(worker_id.clone());
        }

        Ok(res)
    }
}
