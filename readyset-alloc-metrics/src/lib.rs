pub(crate) mod recorded;

use std::{
    collections::{HashMap, HashSet, hash_map::Entry},
    time::Duration,
};

use metrics::{Counter, Gauge};
use readyset_alloc::{AllocThreadStats, fetch_all_memory_stats};
use readyset_util::shutdown::ShutdownReceiver;
use tokio::select;
use tracing::info;

use crate::recorded::*;

const REPORTING_INTERVAL: Duration = Duration::from_secs(2);

pub async fn report_allocator_metrics(mut shutdown_rx: ShutdownReceiver) {
    let mut interval = tokio::time::interval(REPORTING_INTERVAL);
    let mut reporter = AllocatorMetricsReporter::new();

    loop {
        select! {
            _ = interval.tick() => reporter.report_metrics(),
            _ = shutdown_rx.recv() => break,
        }
    }
}

struct AllocatorMetricsReporter {
    allocated: Gauge,
    active: Gauge,
    metadata: Gauge,
    resident: Gauge,
    mapped: Gauge,
    retained: Gauge,
    dirty: Gauge,
    fragmented: Gauge,
    per_thread_bytes: HashMap<String, (Counter, Counter)>,
}

impl AllocatorMetricsReporter {
    fn new() -> Self {
        Self {
            allocated: metrics::gauge!(ALLOCATED_BYTES),
            active: metrics::gauge!(ACTIVE_BYTES),
            metadata: metrics::gauge!(METADATA_BYTES),
            resident: metrics::gauge!(RESIDENT_BYTES),
            mapped: metrics::gauge!(MAPPED_BYTES),
            retained: metrics::gauge!(RETAINED_BYTES),
            dirty: metrics::gauge!(DIRTY_BYTES),
            fragmented: metrics::gauge!(FRAGMENTED_BYTES),
            per_thread_bytes: HashMap::new(),
        }
    }

    fn report_metrics(&mut self) {
        match fetch_all_memory_stats() {
            Ok((alloc_stats, thread_memory_stats)) => {
                self.allocated.set(alloc_stats.allocated as f64);
                self.active.set(alloc_stats.active as f64);
                self.metadata.set(alloc_stats.metadata as f64);
                self.resident.set(alloc_stats.resident as f64);
                self.mapped.set(alloc_stats.mapped as f64);
                self.retained.set(alloc_stats.retained as f64);
                self.dirty.set(alloc_stats.dirty as f64);
                self.fragmented.set(alloc_stats.fragmentation as f64);
                let seen_threads = thread_memory_stats
                    .iter()
                    .map(|stats| &stats.thread_name)
                    .collect::<HashSet<_>>();
                self.per_thread_bytes
                    .retain(|thread_name, _| seen_threads.contains(thread_name));
                let mut thread_totals: HashMap<String, (u64, u64)> = HashMap::new();
                for AllocThreadStats {
                    thread_name,
                    allocated,
                    deallocated,
                } in thread_memory_stats
                {
                    thread_totals
                        .entry(thread_name)
                        .and_modify(|(a, d)| {
                            *a += allocated;
                            *d += deallocated;
                        })
                        .or_insert((allocated, deallocated));
                }
                for (thread_name, (allocated, deallocated)) in thread_totals {
                    let mut entry = self.per_thread_bytes.entry(thread_name);
                    let (allocated_counter, deallocated_counter) = match entry {
                        Entry::Occupied(ref mut entry) => entry.get_mut(),
                        Entry::Vacant(entry) => {
                            let labels = [("thread_name", entry.key().clone())];
                            entry.insert((
                                metrics::counter!(ALLOCATED_BYTES_PER_THREAD, &labels),
                                metrics::counter!(DEALLOCATED_BYTES_PER_THREAD, &labels),
                            ))
                        }
                    };
                    allocated_counter.absolute(allocated);
                    deallocated_counter.absolute(deallocated);
                }
            }
            Err(e) => {
                // not sure what else to do but log, at a low level :shrug:
                info!("Failed to fetch memory allocator stats: {:?}", e);
            }
        }
    }
}
