//! Scheduling which domain runs on which worker.
//!
//! Standalone Readyset only ever has one worker, so scheduling reduces to: pick the single
//! healthy worker, optionally [restricted to a specific worker][worker] by the migration.
//!
//! [worker]: Migration::worker

use dataflow::prelude::*;
use readyset_client::internal::DomainIndex;
use tracing::trace;

use crate::controller::state::DfState;
use crate::controller::WorkerIdentifier;

/// Decide which worker the given `domain_index` should run on, optionally restricted to a single
/// `worker_filter`. Returns [`None`] if no worker is eligible or the domain is already placed.
pub(crate) fn schedule_domain(
    dataflow_state: &DfState,
    worker_filter: &Option<WorkerIdentifier>,
    domain_index: DomainIndex,
) -> ReadySetResult<Option<WorkerIdentifier>> {
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
        .find(|(wi, w)| w.healthy && worker_filter.iter().all(|target| target == *wi))
        .map(|(wi, _)| wi.clone());

    match &worker {
        Some(wi) => trace!(worker_id = %wi, "Scheduled domain"),
        None => trace!("Failed to schedule domain"),
    }

    Ok(worker)
}
