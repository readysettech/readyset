use criterion::{criterion_group, criterion_main};
use dataflow::state::bench::{rocksdb_get_primary_key, rocksdb_get_secondary_key};

criterion_group!(benches, rocksdb_get_primary_key, rocksdb_get_secondary_key);
criterion_main!(benches);
