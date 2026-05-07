//! Scheduling which domain replicas run on which workers.
//!
//! Standalone Readyset only ever has one worker, so scheduling reduces to:
//!
//! 1. Pick the single healthy worker, optionally [restricted to a specific worker][worker] by the
//!    migration. The worker may also be [configured to only run reader nodes][reader_only], in
//!    which case it can only host domains that contain a reader node.
//! 2. Place that worker on the first replica slot per shard that isn't already assigned, leaving
//!    the remaining replica slots empty (since one worker can only host one replica of any given
//!    shard).
//!
//! [reader_only]: Worker::reader_only
//! [worker]: Migration::worker

use array2::Array2;
use dataflow::prelude::*;
use readyset_client::consensus::NodeTypeSchedulingRestriction;
use readyset_client::internal::DomainIndex;
use tracing::trace;

use crate::controller::state::DfState;
use crate::controller::WorkerIdentifier;

/// Decide which workers the shards of `domain_index` (with the given list of `nodes`) should run
/// on, optionally restricted to a single `worker_filter`.
///
/// Returns a 2-dimensional vector, indexed by shard index first and replica index second, of
/// `Option<WorkerIdentifier>`. A cell is `None` if that domain/shard/replica cannot be scheduled
/// or has already been scheduled.
///
/// # Invariants
///
/// * `nodes` cannot be empty
/// * All the nodes in `nodes` must exist in `dataflow_state.ingredients`
pub(crate) fn schedule_domain(
    dataflow_state: &DfState,
    worker_filter: &Option<WorkerIdentifier>,
    domain_index: DomainIndex,
    nodes: &[NodeIndex],
) -> ReadySetResult<Array2<Option<WorkerIdentifier>>> {
    let num_shards = 1;
    let num_replicas = dataflow_state
        .replication_strategy
        .replicate_domain(&dataflow_state.ingredients, nodes);

    let is_reader_domain = nodes
        .iter()
        .any(|n| dataflow_state.ingredients[*n].is_reader());
    let is_base_table_domain = nodes
        .iter()
        .any(|n| dataflow_state.ingredients[*n].is_base());
    trace!(is_reader_domain, is_base_table_domain);

    if is_base_table_domain {
        invariant_eq!(num_replicas, 1);
    }

    let eligible_worker = dataflow_state
        .workers
        .iter()
        .find(|(wi, w)| {
            w.healthy
                && worker_filter.iter().all(|target| target == *wi)
                && match w.domain_scheduling_config.reader_nodes {
                    NodeTypeSchedulingRestriction::None => true,
                    NodeTypeSchedulingRestriction::OnlyWithNodeType => is_reader_domain,
                    NodeTypeSchedulingRestriction::NeverWithNodeType => !is_reader_domain,
                }
        })
        .map(|(wi, _)| wi.clone());

    let existing_dh = dataflow_state.domains.get(&domain_index);

    let mut res = Vec::with_capacity(num_shards);
    for shard in 0..num_shards {
        // A single worker can host at most one replica of any given shard, so if a prior placement
        // already used the worker for this shard we can't place additional replicas on it.
        let mut worker_available = match (&eligible_worker, existing_dh) {
            (Some(w), Some(dh)) => {
                !(0..dh.num_replicas()).any(|r| dh.assignment(shard, r) == Some(w))
            }
            _ => eligible_worker.is_some(),
        };

        let mut replicas = Vec::with_capacity(num_replicas);
        for replica in 0..num_replicas {
            // Don't reschedule a replica that's already placed (eg in another run of recovery).
            let already_placed = existing_dh
                .and_then(|dh| dh.assignment(shard, replica))
                .is_some();

            let assignment = if already_placed {
                None
            } else if worker_available {
                worker_available = false;
                let w = eligible_worker.clone();
                if let Some(ref wid) = w {
                    trace!(%shard, %replica, worker_id = %wid, "Scheduled replica");
                }
                w
            } else {
                trace!(%shard, %replica, "Failed to schedule replica");
                None
            };
            replicas.push(assignment);
        }
        res.push(replicas);
    }

    Ok(Array2::from_rows(res))
}
