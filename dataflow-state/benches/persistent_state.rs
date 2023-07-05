use std::ops::Bound;

use clap::Parser;
use common::{Index, IndexType};
use criterion::{black_box, Criterion};
use dataflow_state::{
    DurabilityMode, PersistenceParameters, PersistentState, PointKey, RangeKey, SnapshotMode, State,
};
use itertools::Itertools;
use readyset_data::DfValue;

lazy_static::lazy_static! {
    static ref LARGE_STRINGS: Vec<String> = ["a", "b", "c"]
        .iter()
        .map(|s| std::iter::once(s).cycle().take(10000).join(""))
        .collect::<Vec<_>>();
}

pub fn rocksdb_get_primary_key(c: &mut Criterion, state: &PersistentState, unique_entries: usize) {
    let mut group = c.benchmark_group("RockDB get primary key");
    let n = unique_entries / 1000;
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
            iter = (iter + 1) % (unique_entries - n * 10);
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
            iter = (iter + 1) % (unique_entries - n * 10);
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

pub fn rocksdb_get_secondary_unique_key(
    c: &mut Criterion,
    state: &PersistentState,
    unique_entries: usize,
) {
    let mut group = c.benchmark_group("RockDB get secondary unique key");
    let n = unique_entries / 1000;
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
            iter = (iter + 1) % (unique_entries - n * 10);
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
            iter = (iter + 1) % (unique_entries - n * 10);
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
    /// Specifies the number of entries that should be included in the database
    #[clap(long, default_value = "100000")]
    unique_entries: usize,
    /// If this value is set to "persistent", data from these benchmarks will be persisted in the
    /// current directory instead of in a tmp directory.
    #[clap(long, default_value = "memory")]
    durability_mode: DurabilityMode,
    /// If this setting is true, the benchmarks will try to use a database that already exists on
    /// disk instead of creating and seeding a new database. Databases are unique to the value
    /// specified by `unique_entries`. In other words, with the durability mode set to
    /// "persistent", if the benchmarks are run with the number of unique entries set to 100,000
    /// and then again with the number of unique entries set to 10,000, two separate databases will
    /// be created. You can only reuse a given database if the benchmarks have already been run
    /// persistently with the same number of unique entries.
    ///
    /// It is an error to use this flag with `--durability-mode` set to any value other than
    /// "persistent".
    ///
    /// **NOTE:** Only set this to true if you are **certain** that the previous run of these
    /// benchmarks finished seeding the database correctly, *including finishing compaction*. If
    /// the data is incomplete or corrupt, the benchmarks may fail to run or may return
    /// inaccurate results.
    #[clap(long, default_value = "false")]
    reuse_persistence: bool,
    /// If this setting is true, the benchmarks will seed a database with the given number of
    /// unique entries and exit without running the benchmarks.
    ///
    /// It is an error to use this flag with `--durability-mode` set to any value other than
    /// "persistent".
    #[clap(long, default_value = "false")]
    seed_only: bool,
}

impl PersistentStateBenchArgs {
    fn initialize_state(&self) -> PersistentState {
        let mut state = PersistentState::new(
            format!("bench_{}", self.unique_entries),
            vec![&[0usize][..], &[3][..]],
            &PersistenceParameters {
                mode: self.durability_mode,
                persistence_threads: 6,
                ..PersistenceParameters::default()
            },
        )
        .unwrap();

        // If `reuse_persistence` is false, seed the database with the appropriate data. If it's
        // true, we assume that the state already contains the proper data from a previous run of
        // the benchmarks.
        if !self.reuse_persistence {
            state.add_key(Index::new(IndexType::HashMap, vec![0]), None);
            state.add_key(Index::new(IndexType::HashMap, vec![1, 2]), None);
            state.add_key(Index::new(IndexType::HashMap, vec![3]), None);

            state.add_key(Index::new(IndexType::BTreeMap, vec![1]), None);

            state.set_snapshot_mode(SnapshotMode::SnapshotModeEnabled);

            let animals = ["Cat", "Dog", "Bat"];

            for i in 0..self.unique_entries {
                let rec: Vec<DfValue> =
                    vec![i.into(), animals[i % 3].into(), (i % 99).into(), i.into()];
                state
                    .process_records(&mut vec![rec].into(), None, None)
                    .unwrap();
            }

            state.set_snapshot_mode(SnapshotMode::SnapshotModeDisabled);

            // Wait for compaction to finish
            state.compaction_finished();
        }

        state
    }

    fn initialize_large_strings_state(&self) -> PersistentState {
        let mut state = PersistentState::new(
            format!("bench_{}_large_strings", self.unique_entries),
            vec![&[0usize][..], &[3][..]],
            &PersistenceParameters {
                mode: self.durability_mode,
                persistence_threads: 6,
                ..PersistenceParameters::default()
            },
        )
        .unwrap();

        // If `reuse_persistence` is false, seed the database with the appropriate data. If it's
        // true, we assume that the state already contains the proper data from a previous run of
        // the benchmarks.
        if !self.reuse_persistence {
            state.set_snapshot_mode(SnapshotMode::SnapshotModeEnabled);

            state.add_key(Index::new(IndexType::HashMap, vec![0]), None);
            state.add_key(Index::new(IndexType::HashMap, vec![1]), None);
            state.add_key(Index::new(IndexType::HashMap, vec![3]), None);

            state.add_key(Index::new(IndexType::BTreeMap, vec![1]), None);

            for i in 0..self.unique_entries {
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

            // Wait for compaction to finish
            state.wait_for_compaction();
        }

        state
    }
}

fn main() -> anyhow::Result<()> {
    let mut args = PersistentStateBenchArgs::parse();

    if args.test {
        // Move along citizen, no tests here
        return Ok(());
    }

    if !matches!(args.durability_mode, DurabilityMode::Permanent) && args.reuse_persistence {
        anyhow::bail!(
            "It only makes sense to reuse persistence if --durability-mode is set to \"permanent\""
        );
    }

    if !matches!(args.durability_mode, DurabilityMode::Permanent) && args.seed_only {
        anyhow::bail!(
            "It only makes sense to seed a database if --durability-mode is set to \"permanent\""
        );
    }

    let state = args.initialize_state();
    let large_strings_state = args.initialize_large_strings_state();

    if !args.seed_only {
        let mut criterion = Criterion::default();
        if let Some(ref filter) = args.benchname {
            criterion = criterion.with_filter(filter);
        }
        if let Some(baseline) = args.save_baseline.take() {
            criterion = criterion.save_baseline(baseline);
        }

        rocksdb_get_primary_key(&mut criterion, &state, args.unique_entries);
        rocksdb_get_secondary_key(&mut criterion, &state);
        rocksdb_get_secondary_unique_key(&mut criterion, &state, args.unique_entries);
        rocksdb_range_lookup(&mut criterion, &state);
        rocksdb_range_lookup_large_strings(&mut criterion, &large_strings_state);

        criterion.final_summary();
    }

    Ok(())
}
