use criterion::{criterion_group, criterion_main};
use dataflow::node::bench::unique_misses;

criterion_group!(benches, unique_misses);
criterion_main!(benches);
