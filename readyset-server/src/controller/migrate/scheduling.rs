//! Scheduling which domain runs on which worker.
//!
//! Standalone Readyset only ever has one worker, so scheduling reduces to: pick the single
//! healthy worker, optionally [restricted to a specific worker][worker] by the migration. The
//! worker may also be [configured to only run reader nodes][reader_only], in which case it can
//! only host domains that contain a reader node.
//!
//! [reader_only]: Worker::reader_only
//! [worker]: Migration::worker

use dataflow::prelude::*;
use readyset_client::consensus::NodeTypeSchedulingRestriction;
use readyset_client::internal::DomainIndex;
use tracing::trace;

use crate::controller::state::DfState;
use crate::controller::WorkerIdentifier;

/// Decide which worker the given `domain_index` (with the given list of `nodes`) should run on,
/// optionally restricted to a single `worker_filter`. Returns [`None`] if no worker is eligible
/// or the domain is already placed.
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
) -> ReadySetResult<Option<WorkerIdentifier>> {
    let is_reader_domain = nodes
        .iter()
        .any(|n| dataflow_state.ingredients[*n].is_reader());
    trace!(is_reader_domain);

    // Don't reschedule a domain that's already placed.
    if dataflow_state
        .domains
        .get(&domain_index)
        .is_some_and(|dh| dh.is_placed())
    {
        return Ok(None);
    }

    let worker = dataflow_state
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

    match &worker {
        Some(wi) => trace!(worker_id = %wi, "Scheduled domain"),
        None => trace!("Failed to schedule domain"),
    }

    Ok(worker)
}
