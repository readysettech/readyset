//! Scheduling which domains (actually shards of domains) run on which workers
//!
//! Currently our domain scheduling algorithm is quite naive - essentially for each domain we
//! round-robin assign the shards of that domain to workers, iterating in an effectively random
//! order (because we iterate over a HashMap of workers)

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

        Ok(Self {
            valid_workers,
            node_restrictions: &leader.node_restrictions,
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

        let mut round_robin = workers.clone().cycle();

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

            // If there are placement restrictions we select the first worker
            // that meets the placement restrictions. This can lead to imbalance
            // in the number of dataflow nodes placed on each server.
            let worker_id = if !dataflow_node_restrictions.is_empty() {
                round_robin.next()
            } else {
                workers.clone().find(|(_, worker)| {
                    worker_meets_restrictions(worker, &dataflow_node_restrictions)
                })
            }
            .map(|(wi, _)| (*wi).clone())
            .ok_or(ReadySetError::NoAvailableWorkers {
                domain_index: domain_index.index(),
                shard,
            })?;

            res.push(worker_id.clone());
        }

        Ok(res)
    }
}
