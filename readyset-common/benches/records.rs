use std::sync::Arc;

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use readyset_common::{DfValue, Record, Records};

criterion_group!(benches, remove_deleted);
criterion_main!(benches);

fn row(id: usize, v: &str) -> Vec<DfValue> {
    vec![DfValue::from(id as i64), DfValue::from(v)]
}

/// A batch of replicated updates as a base table emits them: the old row, then the new row.
fn updates(n: usize) -> Records {
    (0..n)
        .flat_map(|id| {
            [
                Record::Negative(row(id, "old")),
                Record::Positive(row(id, "new")),
            ]
        })
        .collect::<Vec<_>>()
        .into()
}

fn inserts(n: usize) -> Records {
    (0..n)
        .map(|id| Record::Positive(row(id, "new")))
        .collect::<Vec<_>>()
        .into()
}

fn deletes(n: usize) -> Records {
    (0..n).map(|id| Record::Negative(row(id, "old"))).collect()
}

fn cancellations(n: usize) -> Records {
    (0..n)
        .flat_map(|id| {
            [
                Record::Positive(row(id, "same")),
                Record::Negative(row(id, "same")),
            ]
        })
        .collect()
}

fn numeric_updates(n: usize) -> Records {
    (0..n)
        .flat_map(|id| {
            let key = DfValue::Numeric(Arc::new(
                (999_999_999_999_900_000u64 + id as u64)
                    .to_string()
                    .parse()
                    .expect("benchmark key is a valid decimal"),
            ));
            [
                Record::Negative(vec![key.clone(), "old".into()]),
                Record::Positive(vec![key, "new".into()]),
            ]
        })
        .collect()
}

fn remove_deleted(c: &mut Criterion) {
    let mut group = c.benchmark_group("Records::remove_deleted");
    group.sample_size(10);

    for n in [1_000, 10_000, 50_000] {
        for (name, records) in [
            ("updates", updates(n)),
            ("inserts", inserts(n)),
            ("deletes", deletes(n)),
            ("cancellations", cancellations(n)),
            ("numeric_updates", numeric_updates(n)),
        ] {
            group.bench_with_input(BenchmarkId::new(name, n), &records, |b, records| {
                b.iter_batched(
                    || records.clone(),
                    |mut records| records.remove_deleted(),
                    BatchSize::LargeInput,
                )
            });
        }
    }

    group.finish();
}
