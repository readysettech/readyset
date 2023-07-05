use std::ops::Bound;

use clap::Parser;
use common::{Index, IndexType};
use criterion::{black_box, Criterion};
use dataflow_state::{
    DurabilityMode, PersistenceParameters, PersistentState, PointKey, RangeKey, SnapshotMode, State,
};
use itertools::Itertools;
use readyset_data::DfValue;

const UNIQUE_ENTRIES: usize = 100000;

lazy_static::lazy_static! {
    static ref LARGE_STRINGS: Vec<String> = ["a", "b", "c"]
        .iter()
        .map(|s| std::iter::once(s).cycle().take(10000).join(""))
        .collect::<Vec<_>>();
}

pub fn rocksdb_get_primary_key(c: &mut Criterion, state: &PersistentState) {
    let mut group = c.benchmark_group("RockDB get primary key");
    let n = UNIQUE_ENTRIES / 1000;
    group.bench_function("lookup_multi", |b| {
        let mut iter = 0usize;
        b.iter(|| {
            black_box(state.lookup_multi(
                &[0],
                &[
                    PointKey::Single(iter.into()),
                    PointKey::Single((iter + n).into()),
                    PointKey::Single((iter + n * 2).into()),
                    PointKey::Single((iter + n * 3).into()),
                    PointKey::Single((iter + n * 4).into()),
                    PointKey::Single((iter + n * 5).into()),
                    PointKey::Single((iter + n * 6).into()),
                    PointKey::Single((iter + n * 7).into()),
                    PointKey::Single((iter + n * 8).into()),
                    PointKey::Single((iter + n * 9).into()),
                ],
            ));
            iter = (iter + 1) % (UNIQUE_ENTRIES - n * 10);
        })
    });

    group.bench_function("lookup", |b| {
        let mut iter = 0usize;
        b.iter(|| {
            black_box((
                state.lookup(&[0], &PointKey::Single(iter.into())),
                state.lookup(&[0], &PointKey::Single((iter + n).into())),
                state.lookup(&[0], &PointKey::Single((iter + n * 2).into())),
                state.lookup(&[0], &PointKey::Single((iter + n * 3).into())),
                state.lookup(&[0], &PointKey::Single((iter + n * 4).into())),
                state.lookup(&[0], &PointKey::Single((iter + n * 5).into())),
                state.lookup(&[0], &PointKey::Single((iter + n * 6).into())),
                state.lookup(&[0], &PointKey::Single((iter + n * 7).into())),
                state.lookup(&[0], &PointKey::Single((iter + n * 8).into())),
                state.lookup(&[0], &PointKey::Single((iter + n * 9).into())),
            ));
            iter = (iter + 1) % (UNIQUE_ENTRIES - n * 10);
        })
    });

    group.finish();
}

pub fn rocksdb_get_secondary_key(c: &mut Criterion, state: &PersistentState) {
    let mut group = c.benchmark_group("RockDB get secondary key");
    group.bench_function("lookup_multi", |b| {
        b.iter(|| {
            black_box(state.lookup_multi(
                &[1, 2],
                &[
                    PointKey::Double(("Dog".into(), 1.into())),
                    PointKey::Double(("Cat".into(), 2.into())),
                ],
            ));
        })
    });

    group.bench_function("lookup", |b| {
        b.iter(|| {
            black_box((
                state.lookup(&[1, 2], &PointKey::Double(("Dog".into(), 1.into()))),
                state.lookup(&[1, 2], &PointKey::Double(("Cat".into(), 2.into()))),
            ))
        })
    });

    group.finish();
}

pub fn rocksdb_get_secondary_unique_key(c: &mut Criterion, state: &PersistentState) {
    let mut group = c.benchmark_group("RockDB get secondary unique key");
    let n = UNIQUE_ENTRIES / 1000;
    group.bench_function("lookup_multi", |b| {
        let mut iter = 0usize;
        b.iter(|| {
            black_box(state.lookup_multi(
                &[3],
                &[
                    PointKey::Single(iter.into()),
                    PointKey::Single((iter + n).into()),
                    PointKey::Single((iter + n * 2).into()),
                    PointKey::Single((iter + n * 3).into()),
                    PointKey::Single((iter + n * 4).into()),
                    PointKey::Single((iter + n * 5).into()),
                    PointKey::Single((iter + n * 6).into()),
                    PointKey::Single((iter + n * 7).into()),
                    PointKey::Single((iter + n * 8).into()),
                    PointKey::Single((iter + n * 9).into()),
                ],
            ));
            iter = (iter + 1) % (UNIQUE_ENTRIES - n * 10);
        })
    });

    group.bench_function("lookup", |b| {
        let mut iter = 0usize;
        b.iter(|| {
            black_box((
                state.lookup(&[3], &PointKey::Single(iter.into())),
                state.lookup(&[3], &PointKey::Single((iter + n).into())),
                state.lookup(&[3], &PointKey::Single((iter + n * 2).into())),
                state.lookup(&[3], &PointKey::Single((iter + n * 3).into())),
                state.lookup(&[3], &PointKey::Single((iter + n * 4).into())),
                state.lookup(&[3], &PointKey::Single((iter + n * 5).into())),
                state.lookup(&[3], &PointKey::Single((iter + n * 6).into())),
                state.lookup(&[3], &PointKey::Single((iter + n * 7).into())),
                state.lookup(&[3], &PointKey::Single((iter + n * 8).into())),
                state.lookup(&[3], &PointKey::Single((iter + n * 9).into())),
            ));
            iter = (iter + 1) % (UNIQUE_ENTRIES - n * 10);
        })
    });

    group.finish();
}

pub fn rocksdb_range_lookup(c: &mut Criterion, state: &PersistentState) {
    let key = DfValue::from("D");

    let mut group = c.benchmark_group("RocksDB lookup_range");
    group.bench_function("lookup_range", |b| {
        b.iter(|| {
            black_box(state.lookup_range(
                &[1],
                &RangeKey::Single((Bound::Included(key.clone()), Bound::Unbounded)),
            ));
        })
    });
    group.finish();
}

pub fn rocksdb_range_lookup_large_strings(c: &mut Criterion, state: &PersistentState) {
    let key = DfValue::from(LARGE_STRINGS[0].clone());

    let mut group = c.benchmark_group("RocksDB with large strings");
    group.bench_function("lookup_range", |b| {
        b.iter(|| {
            black_box(state.lookup_range(
                &[1],
                &RangeKey::Single((Bound::Included(key.clone()), Bound::Unbounded)),
            ));
        })
    });
    group.finish();
}

#[derive(Parser, Debug)]
struct PersistentStateBenchArgs {
    /// If specified, only run benches containing this string in their names
    // This argument is the first argument passed by `cargo bench`
    #[clap(index(1))]
    benchname: Option<String>,
    /// Names an explicit baseline and enables overwriting the previous results.
    #[clap(long)]
    save_baseline: Option<String>,
    /// Is present when executed with `cargo bench`
    #[clap(long, hide(true))]
    bench: bool,
    #[clap(long, hide(true))]
    /// Is present when executed with `cargo test`
    test: bool,
    /// If this value is set to "persistent", data from these benchmarks will be persisted in the
    /// current directory instead of in a tmp directory.
    #[clap(long, default_value = "memory")]
    durability_mode: DurabilityMode,
}

fn initialize_state(name: String, mode: DurabilityMode) -> PersistentState {
    let mut state = PersistentState::new(
        name,
        vec![&[0usize][..], &[3][..]],
        &PersistenceParameters {
            mode,
            persistence_threads: 6,
            ..PersistenceParameters::default()
        },
    )
    .unwrap();

    state.add_key(Index::new(IndexType::HashMap, vec![0]), None);
    state.add_key(Index::new(IndexType::HashMap, vec![1, 2]), None);
    state.add_key(Index::new(IndexType::HashMap, vec![3]), None);

    state.add_key(Index::new(IndexType::BTreeMap, vec![1]), None);

    state.set_snapshot_mode(SnapshotMode::SnapshotModeEnabled);

    let animals = ["Cat", "Dog", "Bat"];

    for i in 0..UNIQUE_ENTRIES {
        let rec: Vec<DfValue> = vec![i.into(), animals[i % 3].into(), (i % 99).into(), i.into()];
        state
            .process_records(&mut vec![rec].into(), None, None)
            .unwrap();
    }

    state.set_snapshot_mode(SnapshotMode::SnapshotModeDisabled);

    state
}

fn initialize_large_strings_state(name: String, mode: DurabilityMode) -> PersistentState {
    let mut state = PersistentState::new(
        name,
        vec![&[0usize][..], &[3][..]],
        &PersistenceParameters {
            mode,
            persistence_threads: 6,
            ..PersistenceParameters::default()
        },
    )
    .unwrap();

    state.set_snapshot_mode(SnapshotMode::SnapshotModeEnabled);

    state.add_key(Index::new(IndexType::HashMap, vec![0]), None);
    state.add_key(Index::new(IndexType::HashMap, vec![1]), None);
    state.add_key(Index::new(IndexType::HashMap, vec![3]), None);

    state.add_key(Index::new(IndexType::BTreeMap, vec![1]), None);

    for i in 0..UNIQUE_ENTRIES {
        let rec: Vec<DfValue> = vec![
            i.into(),
            LARGE_STRINGS[i % 3].clone().into(),
            (i % 99).into(),
            i.into(),
        ];
        state
            .process_records(&mut vec![rec].into(), None, None)
            .unwrap();
    }

    state.set_snapshot_mode(SnapshotMode::SnapshotModeDisabled);

    state
}

fn main() -> anyhow::Result<()> {
    let mut args = PersistentStateBenchArgs::parse();

    if args.test {
        // Move along citizen, no tests here
        return Ok(());
    }

    let mut criterion = Criterion::default();
    if let Some(ref filter) = args.benchname {
        criterion = criterion.with_filter(filter);
    }
    if let Some(baseline) = args.save_baseline.take() {
        criterion = criterion.save_baseline(baseline);
    }

    let state = initialize_state(format!("bench_{UNIQUE_ENTRIES}"), args.durability_mode);
    let large_strings_state = initialize_large_strings_state(
        format!("bench_{UNIQUE_ENTRIES}_large_strings"),
        args.durability_mode,
    );

    rocksdb_get_primary_key(&mut criterion, &state);
    rocksdb_get_secondary_key(&mut criterion, &state);
    rocksdb_get_secondary_unique_key(&mut criterion, &state);
    rocksdb_range_lookup(&mut criterion, &state);
    rocksdb_range_lookup_large_strings(&mut criterion, &large_strings_state);

    criterion.final_summary();

    Ok(())
}
