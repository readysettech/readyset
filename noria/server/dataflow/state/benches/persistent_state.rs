use std::ops::Bound;

use common::{Index, IndexType, KeyType, RangeKey};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use dataflow_state::{PersistenceParameters, PersistentState, State};
use itertools::Itertools;
use noria_data::DataType;

const UNIQUE_ENTIRES: usize = 100000;

lazy_static::lazy_static! {
    static ref STATE: PersistentState = {
        let mut state = PersistentState::new(
            String::from("bench"),
            vec![&[0usize][..], &[3][..]],
            &PersistenceParameters::default(),
        );

        state.add_key(Index::new(IndexType::HashMap, vec![0]), None);
        state.add_key(Index::new(IndexType::HashMap, vec![1, 2]), None);
        state.add_key(Index::new(IndexType::HashMap, vec![3]), None);

        let animals = ["Cat", "Dog", "Bat"];

        for i in 0..UNIQUE_ENTIRES {
            let rec: Vec<DataType> = vec![
                i.into(),
                animals[i % 3].into(),
                (i % 99).into(),
                i.into(),
            ];
            state.process_records(&mut vec![rec].into(), None, None);
        }

        state
    };

    static ref LARGE_STRINGS: Vec<String> = ["a", "b", "c"].iter().map(|s| {
        std::iter::once(s).cycle().take(10000).join("")
    }).collect::<Vec<_>>();

    static ref STATE_LARGE_STRINGS: PersistentState = {
        let mut state = PersistentState::new(
            String::from("bench"),
            vec![&[0usize][..], &[3][..]],
            &PersistenceParameters::default(),
        );

        state.add_key(Index::new(IndexType::HashMap, vec![0]), None);
        state.add_key(Index::new(IndexType::HashMap, vec![1]), None);
        state.add_key(Index::new(IndexType::HashMap, vec![3]), None);

        for i in 0..UNIQUE_ENTIRES {
            let rec: Vec<DataType> = vec![
                i.into(),
                LARGE_STRINGS[i % 3].clone().into(),
                (i % 99).into(),
                i.into(),
            ];
            state.process_records(&mut vec![rec].into(), None, None);
        }

        state
    };
}

pub fn rocksdb_get_primary_key(c: &mut Criterion) {
    let state = &*STATE;

    let mut group = c.benchmark_group("RockDB get primary key");
    group.bench_function("lookup_multi", |b| {
        let mut iter = 0usize;
        b.iter(|| {
            black_box(state.lookup_multi(
                &[0],
                &[
                    KeyType::Single(&iter.into()),
                    KeyType::Single(&(iter + 100).into()),
                    KeyType::Single(&(iter + 200).into()),
                    KeyType::Single(&(iter + 300).into()),
                    KeyType::Single(&(iter + 400).into()),
                    KeyType::Single(&(iter + 500).into()),
                    KeyType::Single(&(iter + 600).into()),
                    KeyType::Single(&(iter + 700).into()),
                    KeyType::Single(&(iter + 800).into()),
                    KeyType::Single(&(iter + 900).into()),
                ],
            ));
            iter = (iter + 1) % (UNIQUE_ENTIRES - 1000);
        })
    });

    group.bench_function("lookup", |b| {
        let mut iter = 0usize;
        b.iter(|| {
            black_box({
                state.lookup(&[0], &KeyType::Single(&iter.into()));
                state.lookup(&[0], &KeyType::Single(&(iter + 100).into()));
                state.lookup(&[0], &KeyType::Single(&(iter + 200).into()));
                state.lookup(&[0], &KeyType::Single(&(iter + 300).into()));
                state.lookup(&[0], &KeyType::Single(&(iter + 400).into()));
                state.lookup(&[0], &KeyType::Single(&(iter + 500).into()));
                state.lookup(&[0], &KeyType::Single(&(iter + 600).into()));
                state.lookup(&[0], &KeyType::Single(&(iter + 700).into()));
                state.lookup(&[0], &KeyType::Single(&(iter + 800).into()));
                state.lookup(&[0], &KeyType::Single(&(iter + 900).into()));
            });
            iter = (iter + 1) % (UNIQUE_ENTIRES - 1000);
        })
    });

    group.finish();
}

pub fn rocksdb_get_secondary_key(c: &mut Criterion) {
    let state = &*STATE;

    let mut group = c.benchmark_group("RockDB get secondary key");
    group.bench_function("lookup_multi", |b| {
        b.iter(|| {
            black_box(state.lookup_multi(
                &[1, 2],
                &[
                    KeyType::Double(("Dog".into(), 1.into())),
                    KeyType::Double(("Cat".into(), 2.into())),
                ],
            ));
        })
    });

    group.bench_function("lookup", |b| {
        b.iter(|| {
            black_box({
                state.lookup(&[1, 2], &KeyType::Double(("Dog".into(), 1.into())));
                state.lookup(&[1, 2], &KeyType::Double(("Cat".into(), 2.into())));
            })
        })
    });

    group.finish();
}

pub fn rocksdb_get_secondary_unique_key(c: &mut Criterion) {
    let state = &*STATE;

    let mut group = c.benchmark_group("RockDB get secondary unique key");
    group.bench_function("lookup_multi", |b| {
        let mut iter = 0usize;
        b.iter(|| {
            black_box(state.lookup_multi(
                &[3],
                &[
                    KeyType::Single(&iter.into()),
                    KeyType::Single(&(iter + 100).into()),
                    KeyType::Single(&(iter + 200).into()),
                    KeyType::Single(&(iter + 300).into()),
                    KeyType::Single(&(iter + 400).into()),
                    KeyType::Single(&(iter + 500).into()),
                    KeyType::Single(&(iter + 600).into()),
                    KeyType::Single(&(iter + 700).into()),
                    KeyType::Single(&(iter + 800).into()),
                    KeyType::Single(&(iter + 900).into()),
                ],
            ));
            iter = (iter + 1) % (UNIQUE_ENTIRES - 1000);
        })
    });

    group.bench_function("lookup", |b| {
        let mut iter = 0usize;
        b.iter(|| {
            black_box({
                state.lookup(&[3], &KeyType::Single(&iter.into()));
                state.lookup(&[3], &KeyType::Single(&(iter + 100).into()));
                state.lookup(&[3], &KeyType::Single(&(iter + 200).into()));
                state.lookup(&[3], &KeyType::Single(&(iter + 300).into()));
                state.lookup(&[3], &KeyType::Single(&(iter + 400).into()));
                state.lookup(&[3], &KeyType::Single(&(iter + 500).into()));
                state.lookup(&[3], &KeyType::Single(&(iter + 600).into()));
                state.lookup(&[3], &KeyType::Single(&(iter + 700).into()));
                state.lookup(&[3], &KeyType::Single(&(iter + 800).into()));
                state.lookup(&[3], &KeyType::Single(&(iter + 900).into()));
            });
            iter = (iter + 1) % (UNIQUE_ENTIRES - 1000);
        })
    });

    group.finish();
}

pub fn rocksdb_range_lookup_large_strings(c: &mut Criterion) {
    let state = &*STATE_LARGE_STRINGS;
    let key = DataType::from(LARGE_STRINGS[0].clone());

    let mut group = c.benchmark_group("RocksDB with large strings");
    group.bench_function("lookup_range", |b| {
        b.iter(|| {
            black_box(state.lookup_range(
                &[1],
                &RangeKey::Single((Bound::Included(&key), Bound::Unbounded)),
            ));
        })
    });
    group.finish();
}

criterion_group!(
    benches,
    rocksdb_get_primary_key,
    rocksdb_get_secondary_key,
    rocksdb_get_secondary_unique_key,
    rocksdb_range_lookup_large_strings,
);
criterion_main!(benches);
