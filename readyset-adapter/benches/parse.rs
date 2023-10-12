use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use lru::LruCache;
use rand::distributions::Alphanumeric;
use rand::Rng;
use readyset_client::query::QueryId;
use readyset_util::hash::hash;

fn random_string(len: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}

// Benchmarks to see if having an LruCache<String, SqlQuery> or an LruCache<QueryId, SqlQuery> is
// faster for `parsed_query_cache`
fn lru_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("LruCache Benchmarks");

    for len in [100, 1000, 10000].iter() {
        let mut lru_cache_str = LruCache::<String, String>::new(10000.try_into().expect("nonzero"));
        let queries: Vec<String> = (0..100).map(|_| random_string(*len)).collect();
        group.bench_with_input(
            BenchmarkId::new("LruCache<String, String>", len),
            len,
            |b, &_| {
                b.iter(|| {
                    for q in queries.iter() {
                        lru_cache_str.put(black_box(q.clone()), black_box(q.clone()));
                    }
                });
            },
        );

        let mut lru_cache_hash =
            LruCache::<QueryId, String>::new(10000.try_into().expect("nonzero"));
        group.bench_with_input(
            BenchmarkId::new("LruCache<QueryId, String>", len),
            len,
            |b, &_| {
                b.iter(|| {
                    for q in queries.iter() {
                        let id = QueryId::new(hash(&q));
                        lru_cache_hash.put(black_box(id), black_box(q.clone()));
                    }
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, lru_benchmarks);
criterion_main!(benches);
