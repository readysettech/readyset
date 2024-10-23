use std::thread::JoinHandle;
use std::time::Duration;

use metrics::{gauge, Gauge};
use rocksdb::perf::get_memory_usage_stats;
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};

use crate::persistent_state::recorded;
use crate::persistent_state::PersistentStateHandle;

const REPORTING_INTERVAL: Duration = Duration::from_secs(5);

pub(crate) struct MetricsReporterStop {
    tx: SyncSender<()>,
    thread: JoinHandle<()>,
}

pub(crate) struct MetricsReporter {
    state_handle: PersistentStateHandle,
    stop_rx: Receiver<()>,

    /// Approximate memory usage of all the mem-tables
    mem_table_total: Gauge,
    /// Approximate memory usage of un-flushed mem-tables
    mem_table_unflushed: Gauge,
    /// Approximate memory usage of all the table readers
    mem_table_readers_total: Gauge,
    /// Approximate memory usage by cache
    cache_total: Gauge,
    /// Memory used by block indexes and filters, as loaded from each sstable
    block_indexes_filters_total: Gauge,
}

impl MetricsReporterStop {
    fn new(tx: SyncSender<()>, thread: JoinHandle<()>) -> Self {
        Self { tx, thread }
    }

    pub(crate) fn stop(self) {
        self.tx.send(()).unwrap();
        self.thread.join().unwrap();
    }
}

impl MetricsReporter {
    pub(crate) fn start(name: String, state_handle: PersistentStateHandle) -> MetricsReporterStop {
        let mem_table_total = gauge!(recorded::MEM_TABLE_TOTAL, "rocksdb" => name.clone());
        let mem_table_unflushed = gauge!(recorded::MEM_TABLE_UNFLUSHED, "rocksdb" => name.clone());
        let readers_total = gauge!(recorded::READERS_TOTAL, "rocksdb" => name.clone());
        let cache_total = gauge!(recorded::CACHE_TOTAL, "rocksdb" => name.clone());
        let index_filters_total =
            gauge!(recorded::BLOCK_INDEXES_FILTERS_TOTAL, "rocksdb" => name.clone());
        let (tx, rx) = sync_channel(1);

        let new = Self {
            state_handle,
            stop_rx: rx,
            mem_table_total,
            mem_table_unflushed,
            mem_table_readers_total: readers_total,
            cache_total,
            block_indexes_filters_total: index_filters_total,
        };

        MetricsReporterStop::new(tx, new.spawn())
    }

    fn spawn(self) -> JoinHandle<()> {
        std::thread::spawn(move || loop {
            if let Ok(()) = self.stop_rx.recv_timeout(REPORTING_INTERVAL) {
                break;
            }
            self.memory_stats();
        })
    }

    fn memory_stats(&self) {
        let inner = self.state_handle.inner();
        let db = inner.db;
        if let Ok(stats) = get_memory_usage_stats(Some(&[&db]), None) {
            self.mem_table_total.set(stats.mem_table_total as f64);
            self.mem_table_unflushed
                .set(stats.mem_table_unflushed as f64);
            self.mem_table_readers_total
                .set(stats.mem_table_readers_total as f64);
            self.cache_total.set(stats.cache_total as f64);
        }

        // sum up the values across each column family to derive a meaningful value for a metric.

        // https://github.com/facebook/rocksdb/wiki/Memory-usage-in-RocksDB
        let mut block_indexes_filters_count = 0;
        for index in &inner.shared_state.indices {
            if let Some(cf) = db.cf_handle(&index.column_family) {
                if let Ok(v) = db.property_int_value_cf(&cf, "rocksdb.estimate-table-readers-mem") {
                    block_indexes_filters_count += v.unwrap_or(0);
                }
            }
        }
        self.block_indexes_filters_total
            .set(block_indexes_filters_count as f64);
    }
}
