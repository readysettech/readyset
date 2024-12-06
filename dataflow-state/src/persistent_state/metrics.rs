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
    /// Memory used by block indexes and filters, as loaded from each sstable.
    /// https://github.com/facebook/rocksdb/wiki/Memory-usage-in-RocksDB
    block_indexes_filters_total: Gauge,

    /// returns 1 if at least one compaction is pending; otherwise, returns 0
    compaction_pending: Gauge,
    /// the estimated number of bytes that will be written during pending compactions
    estimate_pending_compaction_bytes: Gauge,
    /// the number of currently running compactions.
    num_running_compactions: Gauge,
    /// the number of currently running flushes.
    num_running_flushes: Gauge,

    total_sst_files_size: Gauge,
    /// total size (bytes) of all SST files belong to the latest LSM tree
    live_sst_files_size: Gauge,
    /// the total capacity of the block cache
    block_cache_capacity: Gauge,
    /// the memory size for the entries residing in block cache
    block_cache_usage: Gauge,
    /// the memory size for the entries being pinned
    block_cache_pinned_usage: Gauge,
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
        let index_filters_total =
            gauge!(recorded::BLOCK_INDEXES_FILTERS_TOTAL, "rocksdb" => name.clone());
        let compaction_pending = gauge!(recorded::COMPACTION_PENDING, "rocksdb" => name.clone());
        let estimate_pending_compaction_bytes =
            gauge!(recorded::ESTIMATE_PENDING_COMPACTION_BYTES, "rocksdb" => name.clone());
        let num_running_compactions =
            gauge!(recorded::NUM_RUNNING_COMPACTIONS, "rocksdb" => name.clone());
        let num_running_flushes = gauge!(recorded::NUM_RUNNING_FLUSHES, "rocksdb" => name.clone());
        let total_sst_files_size =
            gauge!(recorded::TOTAL_SST_FILES_SIZE, "rocksdb" => name.clone());
        let live_sst_files_size = gauge!(recorded::LIVE_SST_FILES_SIZE, "rocksdb" => name.clone());
        let block_cache_capacity =
            gauge!(recorded::BLOCK_CACHE_CAPACITY, "rocksdb" => name.clone());
        let block_cache_usage = gauge!(recorded::BLOCK_CACHE_USAGE, "rocksdb" => name.clone());
        let block_cache_pinned_usage =
            gauge!(recorded::BLOCK_CACHE_PINNED_USAGE, "rocksdb" => name.clone());

        let (tx, rx) = sync_channel(1);

        let new = Self {
            state_handle,
            stop_rx: rx,
            mem_table_total,
            mem_table_unflushed,
            mem_table_readers_total: readers_total,
            block_indexes_filters_total: index_filters_total,
            compaction_pending,
            estimate_pending_compaction_bytes,
            num_running_compactions,
            num_running_flushes,
            total_sst_files_size,
            live_sst_files_size,
            block_cache_capacity,
            block_cache_usage,
            block_cache_pinned_usage,
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
        // We don't have references to the caches, so we can't report their memory usage from
        // the `get_memory_usage_stats()` function.
        if let Ok(stats) = get_memory_usage_stats(Some(&[&db]), None) {
            self.mem_table_total.set(stats.mem_table_total as f64);
            self.mem_table_unflushed
                .set(stats.mem_table_unflushed as f64);
            self.mem_table_readers_total
                .set(stats.mem_table_readers_total as f64);
        }

        // sum up the values across each column family to derive a meaningful value for a metric.

        // macro to add the value of a property to a target variable.
        macro_rules! add_property_value {
            ($cf:expr, $property:expr, $target:expr) => {
                if let Ok(v) = db.property_int_value_cf($cf, $property) {
                    $target += v.unwrap_or(0);
                }
            };
        }

        let mut block_indexes_filters_count = 0;
        let mut compaction_pending = 0;
        let mut estimate_pending_compaction_bytes = 0;
        let mut num_running_compactions = 0;
        let mut num_running_flushes = 0;
        let mut total_sst_files_size = 0;
        let mut live_sst_files_size = 0;
        let mut block_cache_capacity = 0;
        let mut block_cache_usage = 0;
        let mut block_cache_pinned_usage = 0;

        // we need to iterate over each column family as, if we don't do this, we will only get
        // the value for the default column family. This is not obvious from the docs.
        for index in &inner.shared_state.indices {
            if let Some(cf) = db.cf_handle(&index.column_family) {
                // readers-related metrics
                add_property_value!(
                    &cf,
                    "rocksdb.estimate-table-readers-mem",
                    block_indexes_filters_count
                );

                // compaction-related metrics
                add_property_value!(&cf, "rocksdb.compaction-pending", compaction_pending);
                add_property_value!(
                    &cf,
                    "rocksdb.estimate-pending-compaction-bytes",
                    estimate_pending_compaction_bytes
                );
                add_property_value!(
                    &cf,
                    "rocksdb.num-running-compactions",
                    num_running_compactions
                );

                // flush-related metrics
                add_property_value!(&cf, "rocksdb.num-running-flushes", num_running_flushes);

                // sst-file-related metrics
                add_property_value!(&cf, "rocksdb.total-sst-files-size", total_sst_files_size);
                add_property_value!(&cf, "rocksdb.live-sst-files-size", live_sst_files_size);

                // block-cache-related metrics
                add_property_value!(&cf, "rocksdb.block-cache-capacity", block_cache_capacity);
                add_property_value!(&cf, "rocksdb.block-cache-usage", block_cache_usage);
                add_property_value!(
                    &cf,
                    "rocksdb.block-cache-pinned-usage",
                    block_cache_pinned_usage
                );
            }
        }
        self.block_indexes_filters_total
            .set(block_indexes_filters_count as f64);
        self.compaction_pending.set(compaction_pending as f64);
        self.estimate_pending_compaction_bytes
            .set(estimate_pending_compaction_bytes as f64);
        self.num_running_compactions
            .set(num_running_compactions as f64);
        self.num_running_flushes.set(num_running_flushes as f64);
        self.total_sst_files_size.set(total_sst_files_size as f64);
        self.live_sst_files_size.set(live_sst_files_size as f64);
        self.block_cache_capacity.set(block_cache_capacity as f64);
        self.block_cache_usage.set(block_cache_usage as f64);
        self.block_cache_pinned_usage
            .set(block_cache_pinned_usage as f64);
    }
}
