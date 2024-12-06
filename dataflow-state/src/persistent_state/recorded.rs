/// Gauge: Approximate memory usage of all the mem-tables.
pub const MEM_TABLE_TOTAL: &str = "readyset_rocksdb_mem_table_total_bytes";

/// Gauge: Approximate memory usage of un-flushed mem-tables.
pub const MEM_TABLE_UNFLUSHED: &str = "readyset_rocksdb_mem_table_unflushed_bytes";

/// Gauge: Approximate memory usage of all the table readers.
pub const READERS_TOTAL: &str = "readyset_rocksdb_readers_total_bytes";

/// Gauge: Total memory used by block index and filter, as loaded from each sstable.
pub const BLOCK_INDEXES_FILTERS_TOTAL: &str = "readyset_rocksdb_block_index_filters_total_bytes";

/// Gauge: Number of compactions pending.
pub const COMPACTION_PENDING: &str = "readyset_rocksdb_compaction_pending";

/// Gauge: Estimate of pending compaction bytes.
pub const ESTIMATE_PENDING_COMPACTION_BYTES: &str =
    "readyset_rocksdb_estimate_pending_compaction_bytes";

/// Gauge: Number of running compactions.
pub const NUM_RUNNING_COMPACTIONS: &str = "readyset_rocksdb_num_running_compactions";

/// Gauge: Number of running flushes.
pub const NUM_RUNNING_FLUSHES: &str = "readyset_rocksdb_num_running_flushes";

/// Gauge: Total size of all SST files.
pub const TOTAL_SST_FILES_SIZE: &str = "readyset_rocksdb_total_sst_files_size";

/// Gauge: Live size of all SST files.
pub const LIVE_SST_FILES_SIZE: &str = "readyset_rocksdb_live_sst_files_size";

/// Gauge: Capacity of the block cache.
pub const BLOCK_CACHE_CAPACITY: &str = "readyset_rocksdb_block_cache_capacity";

/// Gauge: Usage of the block cache.
pub const BLOCK_CACHE_USAGE: &str = "readyset_rocksdb_block_cache_usage";

/// Gauge: Pinned usage of the block cache.
pub const BLOCK_CACHE_PINNED_USAGE: &str = "readyset_rocksdb_block_cache_pinned_usage";
