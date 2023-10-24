pub(crate) mod recorded;

use std::time::Duration;

use metrics::Gauge;
use readyset_alloc::fetch_stats;
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
}

impl AllocatorMetricsReporter {
    fn new() -> Self {
        Self {
            allocated: metrics::register_gauge!(ALLOCATED_BYTES),
            active: metrics::register_gauge!(ACTIVE_BYTES),
            metadata: metrics::register_gauge!(METADATA_BYTES),
            resident: metrics::register_gauge!(RESIDENT_BYTES),
            mapped: metrics::register_gauge!(MAPPED_BYTES),
            retained: metrics::register_gauge!(RETAINED_BYTES),
            dirty: metrics::register_gauge!(DIRTY_BYTES),
            fragmented: metrics::register_gauge!(FRAGMENTED_BYTES),
        }
    }

    fn report_metrics(&mut self) {
        // Note: we could call alloc::jemalloc::iterate_thread_allocation_stats() to get the
        // per-thread dump. `fetch_stats` is sufficient for now.
        // Additional note: iterate_thread_allocation_stats() doesn't automatically bump the
        // it's epoch, so you'll need to do that (just read the code if you are interested).

        match fetch_stats() {
            Ok(stats) => {
                self.allocated.set(stats.allocated as f64);
                self.active.set(stats.active as f64);
                self.metadata.set(stats.metadata as f64);
                self.resident.set(stats.resident as f64);
                self.mapped.set(stats.mapped as f64);
                self.retained.set(stats.retained as f64);
                self.dirty.set(stats.dirty as f64);
                self.fragmented.set(stats.fragmentation as f64);
            }
            Err(e) => {
                // not sure what else to do but log, at a low level :shrug:
                info!("Failed to fetch memory allocator stats: {:?}", e);
            }
        }
    }
}
