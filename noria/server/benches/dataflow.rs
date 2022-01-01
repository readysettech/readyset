use criterion::{criterion_group, criterion_main};
use dataflow::node::bench::unique_misses;
use dataflow::state::bench::{
    rocksdb_get_primary_key, rocksdb_get_secondary_key, rocksdb_get_secondary_unique_key,
    rocksdb_range_lookup_large_strings,
};

criterion_group!(
    benches,
    rocksdb_get_primary_key,
    rocksdb_get_secondary_key,
    rocksdb_get_secondary_unique_key,
    rocksdb_range_lookup_large_strings,
    unique_misses
);
criterion_main!(benches);
