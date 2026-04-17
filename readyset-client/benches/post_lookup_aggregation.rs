use criterion::{black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use dataflow_expression::grouped::accumulator::AccumulationOp;
use dataflow_expression::{
    PostLookup, PostLookupAggregate, PostLookupAggregateFunction, PostLookupAggregates,
};
use readyset_client::results::{ResultIterator, SharedResults};
use readyset_data::DfValue;
use readyset_sql::ast::DistinctOption;
use smallvec::SmallVec;

// ---------------------------------------------------------------------------
// Data generators
// ---------------------------------------------------------------------------

/// Build SharedResults where each key has one row: [group_id, finalized_string].
/// The finalized string simulates dataflow output (values joined by separator).
fn make_string_agg_data(num_keys: usize, values_per_key: usize, separator: &str) -> SharedResults {
    (0..num_keys)
        .map(|key_id| {
            let values: Vec<String> = (0..values_per_key)
                .map(|v| format!("val_{}_{}", key_id, v))
                .collect();
            let finalized = values.join(separator);
            let row: Box<[DfValue]> =
                vec![DfValue::from(key_id as i64), DfValue::from(finalized)].into();
            triomphe::Arc::new(SmallVec::from_vec(vec![row]))
        })
        .collect()
}

/// Build SharedResults where each key has one row: [group_id, DfValue::Array].
fn make_array_agg_data(num_keys: usize, values_per_key: usize) -> SharedResults {
    (0..num_keys)
        .map(|key_id| {
            let values: Vec<DfValue> = (0..values_per_key)
                .map(|v| DfValue::from(format!("val_{}_{}", key_id, v)))
                .collect();
            let array = DfValue::Array(std::sync::Arc::new(readyset_data::Array::from(values)));
            let row: Box<[DfValue]> = vec![DfValue::from(key_id as i64), array].into();
            triomphe::Arc::new(SmallVec::from_vec(vec![row]))
        })
        .collect()
}

/// Build SharedResults with longer values (~100 chars each).
fn make_string_agg_long_data(
    num_keys: usize,
    values_per_key: usize,
    separator: &str,
) -> SharedResults {
    (0..num_keys)
        .map(|key_id| {
            let values: Vec<String> = (0..values_per_key)
                .map(|v| {
                    format!(
                        "longer_value_for_key_{}_item_{}_with_padding_{:0>60}",
                        key_id, v, v
                    )
                })
                .collect();
            let finalized = values.join(separator);
            let row: Box<[DfValue]> =
                vec![DfValue::from(key_id as i64), DfValue::from(finalized)].into();
            triomphe::Arc::new(SmallVec::from_vec(vec![row]))
        })
        .collect()
}

/// Build SharedResults for group-by merging: num_keys lookup results that collapse
/// into num_groups groups (group_id = key_id % num_groups).
fn make_grouped_string_agg_data(
    num_keys: usize,
    values_per_key: usize,
    num_groups: usize,
    separator: &str,
) -> SharedResults {
    (0..num_keys)
        .map(|key_id| {
            let group_id = key_id % num_groups;
            let values: Vec<String> = (0..values_per_key)
                .map(|v| format!("val_{}_{}", key_id, v))
                .collect();
            let finalized = values.join(separator);
            let row: Box<[DfValue]> =
                vec![DfValue::from(group_id as i64), DfValue::from(finalized)].into();
            triomphe::Arc::new(SmallVec::from_vec(vec![row]))
        })
        .collect()
}

/// Build SharedResults with longer DfValue::Array values (~100 chars each).
fn make_array_agg_long_data(num_keys: usize, values_per_key: usize) -> SharedResults {
    (0..num_keys)
        .map(|key_id| {
            let values: Vec<DfValue> = (0..values_per_key)
                .map(|v| {
                    DfValue::from(format!(
                        "longer_value_for_key_{}_item_{}_with_padding_{:0>60}",
                        key_id, v, v
                    ))
                })
                .collect();
            let array = DfValue::Array(std::sync::Arc::new(readyset_data::Array::from(values)));
            let row: Box<[DfValue]> = vec![DfValue::from(key_id as i64), array].into();
            triomphe::Arc::new(SmallVec::from_vec(vec![row]))
        })
        .collect()
}

/// Build SharedResults for group-by merging with raw arrays.
fn make_grouped_array_agg_data(
    num_keys: usize,
    values_per_key: usize,
    num_groups: usize,
) -> SharedResults {
    (0..num_keys)
        .map(|key_id| {
            let group_id = key_id % num_groups;
            let values: Vec<DfValue> = (0..values_per_key)
                .map(|v| DfValue::from(format!("val_{}_{}", key_id, v)))
                .collect();
            let array = DfValue::Array(std::sync::Arc::new(readyset_data::Array::from(values)));
            let row: Box<[DfValue]> = vec![DfValue::from(group_id as i64), array].into();
            triomphe::Arc::new(SmallVec::from_vec(vec![row]))
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build a PostLookup with the given aggregate function.
/// Column 1 is the aggregated column; column 0 is optionally a group-by column.
fn make_post_lookup(
    agg_function: PostLookupAggregateFunction,
    has_group_by: bool,
    raw_values: bool,
) -> PostLookup {
    PostLookup {
        order_by: None,
        limit: None,
        returned_cols: None,
        default_row: None,
        aggregates: Some(PostLookupAggregates {
            group_by: if has_group_by { vec![0] } else { vec![] },
            aggregates: vec![PostLookupAggregate {
                column: 1,
                function: agg_function,
                raw_values,
            }],
        }),
    }
}

fn make_string_agg_op(separator: &str, distinct: DistinctOption) -> PostLookupAggregateFunction {
    PostLookupAggregateFunction::StringAgg {
        op: AccumulationOp::StringAgg {
            separator: Some(separator.to_string()),
            distinct,
            order_by: None,
        },
    }
}

fn make_group_concat_op(separator: &str) -> PostLookupAggregateFunction {
    PostLookupAggregateFunction::GroupConcat {
        op: AccumulationOp::GroupConcat {
            separator: separator.to_string(),
            distinct: DistinctOption::NotDistinct,
            order_by: None,
        },
    }
}

fn make_array_agg_op() -> PostLookupAggregateFunction {
    PostLookupAggregateFunction::ArrayAgg {
        op: AccumulationOp::ArrayAgg {
            distinct: DistinctOption::NotDistinct,
            order_by: None,
        },
    }
}

// ---------------------------------------------------------------------------
// Old path benchmarks (split-based, raw_values: false)
// ---------------------------------------------------------------------------

fn bench_string_agg(c: &mut Criterion) {
    let mut group = c.benchmark_group("string_agg");

    for &num_keys in &[10, 50, 100] {
        for &values_per_key in &[5, 20, 50] {
            let data = make_string_agg_data(num_keys, values_per_key, ",");
            let post_lookup = make_post_lookup(
                make_string_agg_op(",", DistinctOption::NotDistinct),
                false,
                false,
            );

            group.bench_with_input(
                BenchmarkId::new(format!("keys_{}", num_keys), values_per_key),
                &values_per_key,
                |b, _| {
                    b.iter_batched(
                        || data.clone(),
                        |d| {
                            black_box(
                                ResultIterator::new(d, &post_lookup, None, None, None).into_vec(),
                            )
                        },
                        BatchSize::SmallInput,
                    )
                },
            );
        }
    }

    group.finish();
}

fn bench_array_agg(c: &mut Criterion) {
    let mut group = c.benchmark_group("array_agg");

    for &num_keys in &[10, 50, 100] {
        for &values_per_key in &[5, 20, 50] {
            let data = make_array_agg_data(num_keys, values_per_key);
            let post_lookup = make_post_lookup(make_array_agg_op(), false, false);

            group.bench_with_input(
                BenchmarkId::new(format!("keys_{}", num_keys), values_per_key),
                &values_per_key,
                |b, _| {
                    b.iter_batched(
                        || data.clone(),
                        |d| {
                            black_box(
                                ResultIterator::new(d, &post_lookup, None, None, None).into_vec(),
                            )
                        },
                        BatchSize::SmallInput,
                    )
                },
            );
        }
    }

    group.finish();
}

fn bench_group_concat(c: &mut Criterion) {
    let mut group = c.benchmark_group("group_concat");

    let data = make_string_agg_data(50, 20, ",");
    let post_lookup = make_post_lookup(make_group_concat_op(","), false, false);

    group.bench_function("50_keys_20_vals", |b| {
        b.iter_batched(
            || data.clone(),
            |d| black_box(ResultIterator::new(d, &post_lookup, None, None, None).into_vec()),
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn bench_string_agg_with_group_by(c: &mut Criterion) {
    let mut group = c.benchmark_group("string_agg_group_by");

    let data = make_string_agg_data(50, 20, ",");
    let post_lookup = make_post_lookup(
        make_string_agg_op(",", DistinctOption::NotDistinct),
        true,
        false,
    );

    group.bench_function("50_keys_20_vals", |b| {
        b.iter_batched(
            || data.clone(),
            |d| black_box(ResultIterator::new(d, &post_lookup, None, None, None).into_vec()),
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

/// Single-key lookup (data.len() == 1): hits the shortcircuit path.
fn bench_string_agg_single_key(c: &mut Criterion) {
    let mut group = c.benchmark_group("string_agg_single_key");

    for &values_per_key in &[5, 20, 50] {
        let data = make_string_agg_data(1, values_per_key, ",");
        let post_lookup = make_post_lookup(
            make_string_agg_op(",", DistinctOption::NotDistinct),
            false,
            false,
        );

        group.bench_with_input(
            BenchmarkId::from_parameter(values_per_key),
            &values_per_key,
            |b, _| {
                b.iter_batched(
                    || data.clone(),
                    |d| {
                        black_box(ResultIterator::new(d, &post_lookup, None, None, None).into_vec())
                    },
                    BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

/// Distinct string_agg: exercises BTreeSet accumulator path.
fn bench_string_agg_distinct(c: &mut Criterion) {
    let mut group = c.benchmark_group("string_agg_distinct");

    for &num_keys in &[10, 50] {
        let data = make_string_agg_data(num_keys, 20, ",");
        let post_lookup = make_post_lookup(
            make_string_agg_op(",", DistinctOption::IsDistinct),
            false,
            false,
        );

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("keys_{}", num_keys)),
            &num_keys,
            |b, _| {
                b.iter_batched(
                    || data.clone(),
                    |d| {
                        black_box(ResultIterator::new(d, &post_lookup, None, None, None).into_vec())
                    },
                    BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

/// Longer value strings (~100 chars each): split() cost scales with string length.
fn bench_string_agg_long_values(c: &mut Criterion) {
    let mut group = c.benchmark_group("string_agg_long_values");

    for &num_keys in &[10, 50] {
        let data = make_string_agg_long_data(num_keys, 20, ",");
        let post_lookup = make_post_lookup(
            make_string_agg_op(",", DistinctOption::NotDistinct),
            false,
            false,
        );

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("keys_{}", num_keys)),
            &num_keys,
            |b, _| {
                b.iter_batched(
                    || data.clone(),
                    |d| {
                        black_box(ResultIterator::new(d, &post_lookup, None, None, None).into_vec())
                    },
                    BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

/// Group-by with actual merging: 50 keys collapse into 5 groups.
fn bench_string_agg_group_by_merge(c: &mut Criterion) {
    let mut group = c.benchmark_group("string_agg_group_by_merge");

    let data = make_grouped_string_agg_data(50, 20, 5, ",");
    let post_lookup = make_post_lookup(
        make_string_agg_op(",", DistinctOption::NotDistinct),
        true,
        false,
    );

    group.bench_function("50_keys_5_groups_20_vals", |b| {
        b.iter_batched(
            || data.clone(),
            |d| black_box(ResultIterator::new(d, &post_lookup, None, None, None).into_vec()),
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// New path benchmarks (raw arrays, raw_values: true)
// ---------------------------------------------------------------------------

fn bench_string_agg_raw(c: &mut Criterion) {
    let mut group = c.benchmark_group("string_agg_raw");

    for &num_keys in &[10, 50, 100] {
        for &values_per_key in &[5, 20, 50] {
            let data = make_array_agg_data(num_keys, values_per_key);
            let post_lookup = make_post_lookup(
                make_string_agg_op(",", DistinctOption::NotDistinct),
                false,
                true,
            );

            group.bench_with_input(
                BenchmarkId::new(format!("keys_{}", num_keys), values_per_key),
                &values_per_key,
                |b, _| {
                    b.iter_batched(
                        || data.clone(),
                        |d| {
                            black_box(
                                ResultIterator::new(d, &post_lookup, None, None, None).into_vec(),
                            )
                        },
                        BatchSize::SmallInput,
                    )
                },
            );
        }
    }

    group.finish();
}

fn bench_array_agg_raw(c: &mut Criterion) {
    let mut group = c.benchmark_group("array_agg_raw");

    for &num_keys in &[10, 50, 100] {
        for &values_per_key in &[5, 20, 50] {
            let data = make_array_agg_data(num_keys, values_per_key);
            let post_lookup = make_post_lookup(make_array_agg_op(), false, true);

            group.bench_with_input(
                BenchmarkId::new(format!("keys_{}", num_keys), values_per_key),
                &values_per_key,
                |b, _| {
                    b.iter_batched(
                        || data.clone(),
                        |d| {
                            black_box(
                                ResultIterator::new(d, &post_lookup, None, None, None).into_vec(),
                            )
                        },
                        BatchSize::SmallInput,
                    )
                },
            );
        }
    }

    group.finish();
}

fn bench_group_concat_raw(c: &mut Criterion) {
    let mut group = c.benchmark_group("group_concat_raw");

    let data = make_array_agg_data(50, 20);
    let post_lookup = make_post_lookup(make_group_concat_op(","), false, true);

    group.bench_function("50_keys_20_vals", |b| {
        b.iter_batched(
            || data.clone(),
            |d| black_box(ResultIterator::new(d, &post_lookup, None, None, None).into_vec()),
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn bench_string_agg_with_group_by_raw(c: &mut Criterion) {
    let mut group = c.benchmark_group("string_agg_group_by_raw");

    let data = make_array_agg_data(50, 20);
    let post_lookup = make_post_lookup(
        make_string_agg_op(",", DistinctOption::NotDistinct),
        true,
        true,
    );

    group.bench_function("50_keys_20_vals", |b| {
        b.iter_batched(
            || data.clone(),
            |d| black_box(ResultIterator::new(d, &post_lookup, None, None, None).into_vec()),
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

/// Single-key raw: hits the finalize_raw_single_key shortcircuit.
fn bench_string_agg_single_key_raw(c: &mut Criterion) {
    let mut group = c.benchmark_group("string_agg_single_key_raw");

    for &values_per_key in &[5, 20, 50] {
        let data = make_array_agg_data(1, values_per_key);
        let post_lookup = make_post_lookup(
            make_string_agg_op(",", DistinctOption::NotDistinct),
            false,
            true,
        );

        group.bench_with_input(
            BenchmarkId::from_parameter(values_per_key),
            &values_per_key,
            |b, _| {
                b.iter_batched(
                    || data.clone(),
                    |d| {
                        black_box(ResultIterator::new(d, &post_lookup, None, None, None).into_vec())
                    },
                    BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

/// Distinct raw: exercises BTreeSet accumulator with raw arrays.
fn bench_string_agg_distinct_raw(c: &mut Criterion) {
    let mut group = c.benchmark_group("string_agg_distinct_raw");

    for &num_keys in &[10, 50] {
        let data = make_array_agg_data(num_keys, 20);
        let post_lookup = make_post_lookup(
            make_string_agg_op(",", DistinctOption::IsDistinct),
            false,
            true,
        );

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("keys_{}", num_keys)),
            &num_keys,
            |b, _| {
                b.iter_batched(
                    || data.clone(),
                    |d| {
                        black_box(ResultIterator::new(d, &post_lookup, None, None, None).into_vec())
                    },
                    BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

/// Longer values raw: should show larger improvement since split() cost grows with length.
fn bench_string_agg_long_values_raw(c: &mut Criterion) {
    let mut group = c.benchmark_group("string_agg_long_values_raw");

    for &num_keys in &[10, 50] {
        let data = make_array_agg_long_data(num_keys, 20);
        let post_lookup = make_post_lookup(
            make_string_agg_op(",", DistinctOption::NotDistinct),
            false,
            true,
        );

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("keys_{}", num_keys)),
            &num_keys,
            |b, _| {
                b.iter_batched(
                    || data.clone(),
                    |d| {
                        black_box(ResultIterator::new(d, &post_lookup, None, None, None).into_vec())
                    },
                    BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

/// Group-by merge raw: 50 keys collapse into 5 groups with raw arrays.
fn bench_string_agg_group_by_merge_raw(c: &mut Criterion) {
    let mut group = c.benchmark_group("string_agg_group_by_merge_raw");

    let data = make_grouped_array_agg_data(50, 20, 5);
    let post_lookup = make_post_lookup(
        make_string_agg_op(",", DistinctOption::NotDistinct),
        true,
        true,
    );

    group.bench_function("50_keys_5_groups_20_vals", |b| {
        b.iter_batched(
            || data.clone(),
            |d| black_box(ResultIterator::new(d, &post_lookup, None, None, None).into_vec()),
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

criterion_group!(
    benches,
    // Old path (split-based)
    bench_string_agg,
    bench_array_agg,
    bench_group_concat,
    bench_string_agg_with_group_by,
    bench_string_agg_single_key,
    bench_string_agg_distinct,
    bench_string_agg_long_values,
    bench_string_agg_group_by_merge,
    // New path (raw arrays)
    bench_string_agg_raw,
    bench_array_agg_raw,
    bench_group_concat_raw,
    bench_string_agg_with_group_by_raw,
    bench_string_agg_single_key_raw,
    bench_string_agg_distinct_raw,
    bench_string_agg_long_values_raw,
    bench_string_agg_group_by_merge_raw,
);
criterion_main!(benches);
