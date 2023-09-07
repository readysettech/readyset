use common::{Index, IndexType, Record, Records};
use criterion::{black_box, criterion_main, Criterion, Throughput};
use dataflow_state::{
    DurabilityMode, PersistenceParameters, PersistentState, State, WalPersistence,
};
use itertools::Itertools;
use rand::distributions::Alphanumeric;
use rand::Rng;

const BATCH_SIZES: [usize; 5] = [1, 10, 100, 1_000, 10_000];
const NUMBER_OF_STRINGS: usize = 3;
const STRING_SIZE_BYTES: usize = 100;
const NUMBER_OF_DISTINCT_BATCHES: usize = 1000;

fn benchmark_writes(c: &mut Criterion, wal_persistence: WalPersistence) {
    let mut state = PersistentState::new(
        format!("bench_{wal_persistence:?}"),
        vec![&[0usize][..], &[3][..]],
        &PersistenceParameters {
            mode: DurabilityMode::DeleteOnExit,
            persistence_threads: 6,
            wal_persistence,
            ..PersistenceParameters::default()
        },
    )
    .unwrap();

    state.add_key(Index::new(IndexType::HashMap, vec![0]), None);
    state.add_key(Index::new(IndexType::HashMap, vec![1]), None);
    state.add_key(Index::new(IndexType::HashMap, vec![3]), None);

    state.add_key(Index::new(IndexType::BTreeMap, vec![1]), None);

    let mut group = c.benchmark_group(format!("RocksDB writes: {wal_persistence:?}"));

    let strings = (0..NUMBER_OF_STRINGS)
        .map(|_| {
            rand::thread_rng()
                .sample_iter(&Alphanumeric)
                .take(STRING_SIZE_BYTES)
                .map(char::from)
                .collect()
        })
        .collect::<Vec<String>>();

    for batch_size in BATCH_SIZES {
        group.throughput(Throughput::Elements(batch_size as u64));
        group.bench_with_input(
            format!("{batch_size} elements per batch"),
            &batch_size,
            |b, &batch_size| {
                let bs = (0..NUMBER_OF_DISTINCT_BATCHES * batch_size)
                    .map(|i| {
                        Record::from(vec![
                            i.into(),
                            strings[i % NUMBER_OF_STRINGS].clone().into(),
                            (i % 99).into(),
                            i.into(),
                        ])
                    })
                    .chunks(batch_size);

                let mut batches = Vec::with_capacity(NUMBER_OF_DISTINCT_BATCHES);

                for b in &bs {
                    batches.push(Records::from(b.collect::<Vec<Record>>()));
                }

                let mut n = 0;

                b.iter(|| {
                    black_box(state.process_records(
                        &mut batches[n % NUMBER_OF_DISTINCT_BATCHES],
                        None,
                        None,
                    ))
                    .unwrap();
                    n += 1;
                })
            },
        );
    }

    group.finish();
}

fn benchmark_writes_synced(c: &mut Criterion) {
    benchmark_writes(c, WalPersistence::Sync);
}

fn benchmark_writes_flushed(c: &mut Criterion) {
    benchmark_writes(c, WalPersistence::Flush);
}
fn benchmark_writes_neither(c: &mut Criterion) {
    benchmark_writes(c, WalPersistence::Memory);
}

criterion::criterion_group! {
    name = benches;
    config = criterion::Criterion::default();
    targets = benchmark_writes_synced, benchmark_writes_flushed, benchmark_writes_neither
}
criterion_main!(benches);
