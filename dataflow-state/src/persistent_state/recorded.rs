/// Gauge: Approximate memory usage of all the mem-tables.
pub const MEM_TABLE_TOTAL: &str = "readyset_rocksdb_mem_table_total";

/// Gauge: Approximate memory usage of un-flushed mem-tables.
pub const MEM_TABLE_UNFLUSHED: &str = "readyset_rocksdb_mem_table_unflushed";

/// Gauge: Approximate memory usage of all the table readers.
pub const READERS_TOTAL: &str = "readyset_rocksdb_readers_total";

/// Gauge: Approximate memory usage by cache.
pub const CACHE_TOTAL: &str = "readyset_rocksdb_cache_total";
