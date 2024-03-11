use criterion::{black_box, criterion_group, criterion_main, Criterion};
use pprof::criterion::{Output, PProfProfiler};
use rand::distributions::Alphanumeric;
use rand::Rng;
use readyset_data::DfValue;

fn cmp(c: &mut Criterion) {
    let mut group = c.benchmark_group("dfvalue cmp");

    let data = (0..100000)
        .map(|_| {
            let s: String = rand::thread_rng()
                .sample_iter(&Alphanumeric)
                .take(7)
                .map(char::from)
                .collect();
            DfValue::Text(s.as_str().into())
        })
        .collect::<Vec<DfValue>>();

    group.bench_function("cmp", |b| {
        let mut i = 0;
        let mut j = 0;

        b.iter(|| {
            black_box(black_box(&data[i % 1000]).cmp(black_box(&data[j % 1000])));

            i += 1;
            j += 2;
        })
    });
}

fn flamegraphs_profiler() -> Criterion {
    return Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
}

criterion_group!(
    name = benches;
    config = flamegraphs_profiler();
    targets = cmp
);
criterion_main!(benches);
