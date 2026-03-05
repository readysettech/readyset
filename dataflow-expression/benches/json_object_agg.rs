//! Microbenchmarks for `json_object_agg` accumulator performance.
//!
//! Measures the cost of `apply()` (which rebuilds the JSON output) and `add()`/`remove()` on the
//! accumulator data. The main bottleneck being targeted is the O(N) JSON parse per element in
//! `apply_json_object_agg()` — each mutation triggers a full rebuild that parses every stored
//! element's JSON string.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use dataflow_expression::grouped::accumulator::{AccumulationOp, AccumulatorData};
use readyset_data::DfValue;

/// Simulate what the upstream `json_build_object(key, value)` projection produces:
/// a serialized JSON object with a single key-value pair.
fn make_json_object(key: &str, value: &str) -> DfValue {
    DfValue::from(format!("{{\"{key}\":\"{value}\"}}"))
}

fn make_json_object_int(key: &str, value: i64) -> DfValue {
    DfValue::from(format!("{{\"{key}\":{value}}}"))
}

/// Build an AccumulatorData with `n` elements already added.
fn build_accumulator(n: usize) -> (AccumulationOp, AccumulatorData, Vec<DfValue>) {
    let op = AccumulationOp::JsonObjectAgg {
        allow_duplicate_keys: true,
    };
    let mut data = AccumulatorData::from(&op);
    let mut values = Vec::with_capacity(n);

    for i in 0..n {
        let v = make_json_object(&format!("key_{i}"), &format!("value_{i}"));
        values.push(v.clone());
        data.add(&op, v).expect("add failed");
    }

    (op, data, values)
}

/// Benchmark `apply()` — rebuilds the entire JSON output from stored data.
/// This is the hot path: called after every add/remove.
fn bench_apply(c: &mut Criterion) {
    let mut group = c.benchmark_group("json_object_agg/apply");

    for n in [10, 25, 50, 100, 200] {
        let (op, data, _) = build_accumulator(n);

        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| op.apply(&data).expect("apply failed"));
        });
    }

    group.finish();
}

/// Benchmark a single `add()` on an accumulator with `n` existing elements.
fn bench_add(c: &mut Criterion) {
    let mut group = c.benchmark_group("json_object_agg/add");

    for n in [10, 50, 100] {
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_batched(
                || {
                    let (op, data, _) = build_accumulator(n);
                    let new_val = make_json_object("new_key", "new_value");
                    (op, data, new_val)
                },
                |(op, mut data, val)| {
                    data.add(&op, val).expect("add failed");
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

/// Benchmark a single `remove()` on an accumulator with `n` existing elements.
/// Removes the first element (worst case for `rposition` — scans all elements).
fn bench_remove(c: &mut Criterion) {
    let mut group = c.benchmark_group("json_object_agg/remove");

    for n in [10, 50, 100] {
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_batched(
                || {
                    let (op, data, values) = build_accumulator(n);
                    let remove_val = values[0].clone();
                    (op, data, remove_val)
                },
                |(op, mut data, val)| {
                    data.remove(&op, val).expect("remove failed");
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

/// Benchmark the full delete-all-rows scenario from the ticket:
/// accumulator starts with `n` rows, then each row is deleted one by one,
/// with `apply()` called after each deletion (as happens in the real dataflow).
fn bench_delete_all_rows(c: &mut Criterion) {
    let mut group = c.benchmark_group("json_object_agg/delete_all");

    for n in [10, 25, 55] {
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_batched(
                || build_accumulator(n),
                |(op, mut data, values)| {
                    for v in &values {
                        data.remove(&op, v.clone()).expect("remove failed");
                        // apply() is called after each mutation in the real dataflow
                        op.apply(&data).expect("apply failed");
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

/// Benchmark apply with integer values (different JSON serialization path).
fn bench_apply_int_values(c: &mut Criterion) {
    let mut group = c.benchmark_group("json_object_agg/apply_int");

    for n in [10, 50, 100] {
        let op = AccumulationOp::JsonObjectAgg {
            allow_duplicate_keys: true,
        };
        let mut data = AccumulatorData::from(&op);
        for i in 0..n {
            data.add(&op, make_json_object_int(&format!("k{i}"), i as i64))
                .expect("add failed");
        }

        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| op.apply(&data).expect("apply failed"));
        });
    }

    group.finish();
}

/// Generate a large string value (~32KB, matching the Antithesis fuzz test from REA-6280).
fn make_large_value(seed: usize) -> String {
    // Real values were ~32KB avg of repeated characters; use a pattern that's
    // representative of the serialization cost without being literally 'aaa...'.
    let base = format!("value_{seed}_");
    base.repeat(32768 / base.len())
}

/// Build an accumulator with large (~32KB) key and value strings, matching the
/// Antithesis test that found REA-6280 (55 rows, ~67KB per json_build_object output).
fn build_large_accumulator(n: usize) -> (AccumulationOp, AccumulatorData, Vec<DfValue>) {
    let op = AccumulationOp::JsonObjectAgg {
        allow_duplicate_keys: true,
    };
    let mut data = AccumulatorData::from(&op);
    let mut values = Vec::with_capacity(n);

    for i in 0..n {
        let key = make_large_value(i * 2);
        let val = make_large_value(i * 2 + 1);
        let v = DfValue::from(format!("{{\"{}\":\"{}\"}}", key, val));
        values.push(v.clone());
        data.add(&op, v).expect("add failed");
    }

    (op, data, values)
}

/// Benchmark apply() with large values (~32KB per key/value).
fn bench_apply_large(c: &mut Criterion) {
    let mut group = c.benchmark_group("json_object_agg/apply_large");

    for n in [10, 25, 55] {
        let (op, data, _) = build_large_accumulator(n);

        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| op.apply(&data).expect("apply failed"));
        });
    }

    group.finish();
}

/// Benchmark the full delete-all scenario with large values (the actual REA-6280 scenario).
fn bench_delete_all_large(c: &mut Criterion) {
    let mut group = c.benchmark_group("json_object_agg/delete_all_large");

    for n in [10, 25, 55] {
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_batched(
                || build_large_accumulator(n),
                |(op, mut data, values)| {
                    for v in &values {
                        data.remove(&op, v.clone()).expect("remove failed");
                        op.apply(&data).expect("apply failed");
                    }
                },
                criterion::BatchSize::LargeInput,
            );
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_apply,
    bench_add,
    bench_remove,
    bench_delete_all_rows,
    bench_apply_int_values,
    bench_apply_large,
    bench_delete_all_large,
);
criterion_main!(benches);
