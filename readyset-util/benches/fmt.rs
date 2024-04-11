use std::fmt::Write;

use bytes::BytesMut;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use pprof::criterion::{Output, PProfProfiler};
use rand::Rng;
use readyset_util::fmt::write_padded_u32;

const NUMBERS_TO_GENERATE: usize = 1_000_000;

fn bench_write_padded_u32(c: &mut Criterion) {
    let mut group = c.benchmark_group("write a padded u32");
    let mut rng = rand::thread_rng();
    let numbers = (0..NUMBERS_TO_GENERATE)
        .map(|_| (rng.gen_range(u32::MIN..=u32::MAX), rng.gen_range(0..12)))
        .collect::<Vec<(u32, u32)>>();

    group.bench_function("custom formatter", |b| {
        let mut bytes = BytesMut::new();
        let mut iter = 0usize;

        b.iter(|| {
            let (num, width) = black_box(numbers[iter % NUMBERS_TO_GENERATE]);
            write_padded_u32(num, width, black_box(&mut bytes));
            iter += 1;
        })
    });

    group.bench_function("default formatter", |b| {
        let mut bytes = BytesMut::new();
        let mut iter = 0usize;

        b.iter(|| {
            let (num, width) = black_box(numbers[iter % NUMBERS_TO_GENERATE]);
            write!(
                black_box(&mut bytes),
                "{:0width$}",
                num,
                width = width as usize
            )
            .unwrap();
            iter += 1;
        })
    });
}

fn flamegraphs_profiler() -> Criterion {
    return Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
}

criterion_group!(
    name = benches;
    config = flamegraphs_profiler();
    targets = bench_write_padded_u32
);
criterion_main!(benches);
