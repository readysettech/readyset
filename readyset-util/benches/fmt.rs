use std::fmt::Write;

use bytes::BytesMut;
use chrono::{DateTime, FixedOffset, TimeZone};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use pprof::criterion::{Output, PProfProfiler};
use rand::Rng;
use readyset_util::fmt;

const NUMBERS_TO_GENERATE: usize = 1_000_000;

fn write_timestamptz(c: &mut Criterion) {
    let mut group = c.benchmark_group("write a timestamptz");

    group.bench_function("write_padded_u32", |b| {
        let mut bytes = BytesMut::new();
        let mut iter = 0usize;
        let mut rng = rand::thread_rng();
        let numbers = (0..NUMBERS_TO_GENERATE)
            .map(|_| {
                FixedOffset::west_opt(18_000)
                    .unwrap()
                    .with_ymd_and_hms(2020, rng.gen_range(1..12), rng.gen_range(1..28), 12, 30, 45)
                    .single()
                    .unwrap()
            })
            .collect::<Vec<DateTime<FixedOffset>>>();

        b.iter(|| {
            fmt::write_timestamp_tz(
                black_box(&mut bytes),
                black_box(numbers[iter % NUMBERS_TO_GENERATE]),
            );
            iter += 1;
        })
    });

    group.bench_function("rust_formatter", |b| {
        const TIMESTAMP_TZ_FORMAT: &str = "%Y-%m-%d %H:%M:%S%.f %:z";

        let mut bytes = BytesMut::new();
        let mut iter = 0usize;
        let mut rng = rand::thread_rng();
        let numbers = (0..NUMBERS_TO_GENERATE)
            .map(|_| {
                FixedOffset::west_opt(18_000)
                    .unwrap()
                    .with_ymd_and_hms(2020, rng.gen_range(1..12), rng.gen_range(1..28), 12, 30, 45)
                    .single()
                    .unwrap()
            })
            .collect::<Vec<DateTime<FixedOffset>>>();

        b.iter(|| {
            let ts = numbers[iter % NUMBERS_TO_GENERATE];
            write!(
                black_box(&mut bytes),
                "{}",
                black_box(ts.format(TIMESTAMP_TZ_FORMAT))
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
    targets = write_timestamptz
);
criterion_main!(benches);
