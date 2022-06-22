//! Scheduling which domain replicas run on which workers
//!
//! The domain scheduling algorithm, which is currently quite simplistic, works as follows:
//!
//! 1. We filter the set of workers in the cluster by two criteria:
//!    a. The worker must be healhty, and
//!    b. The worker can be [configured to only run reader nodes][reader_only], in which case only
//!       domains that contain a reader node can run on that worker
//! 2. Migrations can optionally [be restricted to a single worker][worker] - if so, all
//!    replicas of all shards of all domains within the migration will be scheduled to that worker,
//!    *if* it's valid
//! 3. Otherwise, for each replica of each shard in the domain, we first filter the set of workers
//!    down to only workers that aren't running a different replica of the same domain shard, then
//!    either:
//!    a. Run the domain shard on the worker matching its [placement restrictions][], if it has any,
//!       or
//!    b. If the domain contains base tables, run it on the worker running the smallest number of
//!       other base tables, or otherwise
//!    c. Run it on the worker that has the smallest number of domain shards scheduled onto it
//!
//! [reader_only]: Worker::reader_only
//! [worker]: Migration::worker
//! [placement restrictions]: DomainPlacementRestriction

use std::collections::{HashMap, HashSet};

use dataflow::prelude::*;
use noria::internal::DomainIndex;

use crate::controller::state::DataflowState;
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

/// Statistics about the domains scheduled onto a worker
#[derive(Default, Clone, Copy)]
struct WorkerStats {
    /// The number of replicas of domain shards that are running in this worker.
    num_domain_shard_replicas: usize,
    /// The number of replicas of shards of domains with base tables that are running in this
    /// worker.
    ///
    /// Currently there will only be 1 replica of each base table shard (because we don't have the
    /// ability to replicate base tables yet) but this is intended to still work once we lift that
    /// limitation.
    num_base_table_domain_shard_replicas: usize,
}

/// A short-lived struct holding all the information necessary to assign domain shards to workers.
pub(crate) struct Scheduler<'state> {
    valid_workers: Vec<(&'state WorkerIdentifier, &'state Worker)>,
    worker_stats: HashMap<&'state WorkerIdentifier, WorkerStats>,
    scheduled_shards: HashMap<&'state WorkerIdentifier, HashSet<(DomainIndex, usize)>>,
    dataflow_state: &'state DataflowState,
}

impl<'state> Scheduler<'state> {
    /// Create a new [`Scheduler`], optionally restricted to the given `worker`.
    pub(crate) fn new(
        dataflow_state: &'state DataflowState,
        worker: &Option<WorkerIdentifier>,
    ) -> ReadySetResult<Self> {
        let valid_workers = dataflow_state
            .workers
            .iter()
            .filter(|(_, w)| w.healthy)
            .filter(|(wi, _)| worker.iter().all(|target_worker| *target_worker == **wi))
            .collect();

        let mut worker_stats: HashMap<&WorkerIdentifier, WorkerStats> = HashMap::new();
        let mut scheduled_shards: HashMap<&WorkerIdentifier, HashSet<(DomainIndex, usize)>> =
            HashMap::new();

        for (di, dh) in &dataflow_state.domains {
            let is_base_table_domain = dataflow_state.domain_nodes[di]
                .values()
                .any(|ni| dataflow_state.ingredients[*ni].is_base());
            for (shard, replicas) in dh.shards().iter().enumerate() {
                for wi in replicas {
                    let stats = worker_stats.entry(wi).or_default();
                    stats.num_domain_shard_replicas += 1;
                    if is_base_table_domain {
                        stats.num_base_table_domain_shard_replicas += 1;
                    }

                    scheduled_shards.entry(wi).or_default().insert((*di, shard));
                }
            }
        }

        Ok(Self {
            valid_workers,
            worker_stats,
            scheduled_shards,
            dataflow_state,
        })
    }

    /// Decide which workers the shards of the given `domain` (with the given list of `nodes`)
    /// should run on
    ///
    /// Returns a 2-dimensional vector of `WorkerIdentifier` to schedule the domain's shards onto,
    /// indexed by shard index first and replica index second
    ///
    /// # Invariants
    ///
    /// * `nodes` cannot be empty
    /// * All the nodes in `nodes` must exist in `self.dataflow_state.ingredients`
    #[allow(clippy::indexing_slicing)] // documented invariant
    pub(crate) fn schedule_domain(
        &mut self,
        domain_index: DomainIndex,
        nodes: &[NodeIndex],
    ) -> ReadySetResult<Vec<Vec<WorkerIdentifier>>> {
        let num_shards = self.dataflow_state.ingredients[nodes[0]]
            .sharded_by()
            .shards()
            .unwrap_or(1);
        let num_replicas = self
            .dataflow_state
            .replication_strategy
            .replicate_domain(&self.dataflow_state.ingredients, nodes);

        let is_reader_domain = nodes
            .iter()
            .any(|n| self.dataflow_state.ingredients[*n].is_reader());
        let is_base_table_domain = nodes
            .iter()
            .any(|n| self.dataflow_state.ingredients[*n].is_base());

        if is_base_table_domain {
            invariant_eq!(num_replicas, 1);
        }

        let workers = self
            .valid_workers
            .iter()
            .filter(|(_, worker)| !worker.reader_only || is_reader_domain);

        let mut res = Vec::with_capacity(num_shards);
        for shard in 0..num_shards {
            let mut replicas = Vec::with_capacity(num_replicas);
            // Filter out any workers that have a different replica of the same domain shard, to
            // avoid scheduling two replicas of the same shard onto the same worker
            let available_workers = workers
                .clone()
                .filter(|(wi, _)| {
                    self.scheduled_shards
                        .get(wi)
                        .map_or(true, |shards| !shards.contains(&(domain_index, shard)))
                })
                .collect::<Vec<_>>();
            for _replica in 0..num_replicas {
                // Shards of certain dataflow nodes may have restrictions that
                // limit the workers they are placed upon.
                let dataflow_node_restrictions = nodes
                    .iter()
                    .filter_map(|n| {
                        let node_name = self.dataflow_state.ingredients[*n].name();
                        self.dataflow_state
                            .node_restrictions
                            .get(&NodeRestrictionKey {
                                node_name: node_name.clone(),
                                shard,
                            })
                    })
                    .collect::<Vec<_>>();

                let worker_id = if dataflow_node_restrictions.is_empty() {
                    // If there are no placement restrictions, pick the node based on load-balancing
                    // heuristics
                    available_workers.iter().min_by_key(|(wi, _)| {
                        let stats = self.worker_stats.get(wi).copied().unwrap_or_default();

                        if is_base_table_domain {
                            // If there are base tables in the domain, find the worker running the
                            // smallest number of base table domain shards
                            stats.num_base_table_domain_shard_replicas
                        } else {
                            // Otherwise, find the worker running the smallest number of domain
                            // shards overall
                            stats.num_domain_shard_replicas
                        }
                    })
                } else {
                    // Otherwise, if there are placement restrictions, we select the first worker
                    // that meets the placement restrictions. This can lead to
                    // imbalance in the number of dataflow nodes placed on each
                    // server.
                    available_workers.iter().find(|(_, worker)| {
                        worker_meets_restrictions(worker, &dataflow_node_restrictions)
                    })
                }
                .map(|(wi, _)| *wi)
                .ok_or(ReadySetError::NoAvailableWorkers {
                    domain_index: domain_index.index(),
                    shard,
                })?;

                replicas.push(worker_id.clone());

                self.scheduled_shards
                    .entry(worker_id)
                    .or_default()
                    .insert((domain_index, shard));

                let stats = self.worker_stats.entry(worker_id).or_default();
                stats.num_domain_shard_replicas += 1;
                if is_base_table_domain {
                    stats.num_base_table_domain_shard_replicas += 1;
                }
            }
            res.push(replicas);
        }

        Ok(res)
    }
}
