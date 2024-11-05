/// Gauge: Approximate memory usage of all the mem-tables.
pub const MEM_TABLE_TOTAL: &str = "readyset_rocksdb_mem_table_total_bytes";

/// Gauge: Approximate memory usage of un-flushed mem-tables.
pub const MEM_TABLE_UNFLUSHED: &str = "readyset_rocksdb_mem_table_unflushed_bytes";

/// Gauge: Approximate memory usage of all the table readers.
pub const READERS_TOTAL: &str = "readyset_rocksdb_readers_total_bytes";

/// Gauge: Approximate memory usage by cache.
pub const CACHE_TOTAL: &str = "readyset_rocksdb_cache_total_bytes";

/// Gauge: Total memory used by block index and filter, as loaded from each sstable.
pub const BLOCK_INDEXES_FILTERS_TOTAL: &str = "readyset_rocksdb_block_index_filters_total_bytes";
