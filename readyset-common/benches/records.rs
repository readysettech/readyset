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

fn remove_deleted(c: &mut Criterion) {
    let mut group = c.benchmark_group("Records::remove_deleted");
    group.sample_size(10);

    for n in [1_000, 10_000, 50_000] {
        for (name, records) in [("updates", updates(n)), ("inserts", inserts(n))] {
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
