extern crate reader_map;

use std::cmp::Ord;
use std::collections::{BTreeMap, HashSet};
use std::fmt::Debug;
use std::hash::{BuildHasher, Hash};
use std::ops::Range;

use proptest::collection::size_range;
use proptest::prelude::*;
use reader_map::handles::{ReadHandle, WriteHandle};
use reader_map::DefaultInsertionOrder;
use readyset_util::ranges::{Bound, RangeBounds};
use test_strategy::{proptest, Arbitrary};
use test_utils::tags;

const LARGE_VEC_RANGE: Range<usize> = 10..1000;

fn set<'a, T, I>(iter: I) -> HashSet<T>
where
    I: IntoIterator<Item = &'a T>,
    T: Copy + Hash + Eq + 'a,
{
    iter.into_iter().cloned().collect()
}

#[tags(no_retry)]
#[proptest]
fn contains(insert: Vec<u32>) {
    let (mut w, r) = reader_map::new();
    for &key in &insert {
        w.insert(key, ());
    }
    w.publish();

    assert!(insert.iter().all(|&key| r.get(&key).unwrap().is_some()));
}

#[tags(no_retry)]
#[proptest]
fn contains_not(insert: Vec<u8>, not: Vec<u8>) {
    let (mut w, r) = reader_map::new();
    for &key in &insert {
        w.insert(key, ());
    }
    w.publish();

    let nots = &set(&not) - &set(&insert);
    assert!(nots.iter().all(|&key| r.get(&key).unwrap().is_none()));
}

#[tags(no_retry)]
#[proptest]
fn insert_empty(insert: Vec<u8>, remove: Vec<u8>) {
    let (mut w, r) = reader_map::new();
    for &key in &insert {
        w.insert(key, ());
    }
    w.publish();
    for &key in &remove {
        w.remove_entry(key);
    }
    w.publish();
    let elements = &set(&insert) - &set(&remove);

    assert_eq!(r.len(), elements.len());
    assert_eq!(
        r.enter().iter().flat_map(|r| r.keys()).count(),
        elements.len()
    );
    assert!(elements.iter().all(|k| r.get(k).unwrap().is_some()));
}

use Op::*;
#[derive(Arbitrary, Copy, Clone, Debug)]
enum Op<K: Ord + Clone, V> {
    Add(K, V),
    Remove(K),
    RemoveValue(K, V),
    RemoveRange(#[strategy(arbitrary_range())] (Bound<K>, Bound<K>)),
    Refresh,
}

fn arbitrary_range<T: Arbitrary + Ord + Clone + 'static>(
) -> impl Strategy<Value = (Bound<T>, Bound<T>)> {
    use Bound::*;

    (any::<Bound<T>>(), any::<Bound<T>>())
        .prop_filter("bounds cannot be equal", |(b1, b2)| match (b1, b2) {
            (Included(b1), Excluded(b2))
            | (Excluded(b1), Included(b2))
            | (Excluded(b1), Excluded(b2)) => b1 != b2,
            _ => true,
        })
        .prop_map(|(b1, b2)| {
            fn inner<T>(bound: Bound<&T>) -> Option<&T> {
                match bound {
                    Bound::Included(b) => Some(b),
                    Bound::Excluded(b) => Some(b),
                }
            }

            match (inner(b1.as_ref()), inner(b2.as_ref())) {
                (Some(b1_inner), Some(b2_inner)) => {
                    if b1_inner <= b2_inner {
                        (b1, b2)
                    } else {
                        (b2, b1)
                    }
                }
                (_, _) => (b1, b2),
            }
        })
}

fn do_ops<K, V, S>(
    ops: &[Op<K, V>],
    reader_map: &mut WriteHandle<K, V, DefaultInsertionOrder, (), S>,
    write_ref: &mut BTreeMap<K, Vec<V>>,
    read_ref: &mut BTreeMap<K, Vec<V>>,
) where
    K: Ord + Clone + Hash,
    V: Clone + Ord,
    S: BuildHasher + Clone,
{
    for op in ops {
        match *op {
            Add(ref k, ref v) => {
                reader_map.insert(k.clone(), v.clone());
                write_ref.entry(k.clone()).or_default().push(v.clone());
            }
            Remove(ref k) => {
                reader_map.remove_entry(k.clone());
                write_ref.remove(k);
            }
            RemoveValue(ref k, ref v) => {
                reader_map.remove_value(k.clone(), v.clone());
                write_ref.get_mut(k).and_then(|values| {
                    values
                        .iter_mut()
                        .position(|value| value == v)
                        .map(|pos| values.swap_remove(pos))
                });
            }
            RemoveRange(ref range) => {
                reader_map.remove_range(range.clone());
                write_ref.retain(|k, _| !range.contains(k));
            }
            Refresh => {
                reader_map.publish();
                *read_ref = write_ref.clone();
            }
        }
    }
}

fn assert_maps_equivalent<K, V, S>(
    a: &ReadHandle<K, V, DefaultInsertionOrder, (), S>,
    b: &BTreeMap<K, Vec<V>>,
) -> bool
where
    K: Clone + Ord + Debug + Hash,
    V: Hash + Eq + Debug + Ord + Copy,
    S: BuildHasher,
{
    assert_eq!(a.len(), b.len());
    for key in a.enter().iter().flat_map(|r| r.keys()) {
        assert!(b.contains_key(key), "b does not contain {key:?}");
    }
    for key in b.keys() {
        assert!(a.get(key).unwrap().is_some(), "a does not contain {key:?}");
    }
    let guard = if let Ok(guard) = a.enter() {
        guard
    } else {
        // Reference was empty, ReadHandle was destroyed, so all is well. Maybe.
        return true;
    };
    for key in guard.keys() {
        let mut ev_map_values: Vec<V> = guard.get(key).unwrap().iter().copied().collect();
        ev_map_values.sort();
        let mut map_values = b[key].clone();
        map_values.sort();
        assert_eq!(ev_map_values, map_values);
    }
    true
}

#[tags(no_retry)]
#[proptest]
fn operations_i8(#[any(size_range(LARGE_VEC_RANGE).lift())] ops: Vec<Op<i8, i8>>) {
    let (mut w, r) = reader_map::new();
    let mut write_ref = BTreeMap::new();
    let mut read_ref = BTreeMap::new();
    do_ops(&ops, &mut w, &mut write_ref, &mut read_ref);
    assert_maps_equivalent(&r, &read_ref);

    w.publish();
    assert_maps_equivalent(&r, &write_ref);
}

#[tags(no_retry)]
#[proptest]
fn operations_string(ops: Vec<Op<String, i8>>) {
    let (mut w, r) = reader_map::new();
    let mut write_ref = BTreeMap::new();
    let mut read_ref = BTreeMap::new();
    do_ops(&ops, &mut w, &mut write_ref, &mut read_ref);
    assert_maps_equivalent(&r, &read_ref);

    w.publish();
    assert_maps_equivalent(&r, &write_ref);
}

#[tags(no_retry)]
#[proptest]
fn keys_values(#[any(size_range(LARGE_VEC_RANGE).lift())] ops: Vec<Op<i8, i8>>) {
    let (mut w, r) = reader_map::new();
    let mut write_ref = BTreeMap::new();
    let mut read_ref = BTreeMap::new();
    do_ops(&ops, &mut w, &mut write_ref, &mut read_ref);

    if let Ok(read_guard) = r.enter() {
        let (mut w_visit, r_visit) = reader_map::new();
        for (k, v_set) in read_guard.keys().zip(read_guard.values()) {
            assert!(read_guard[k].iter().all(|v| v_set.contains(v)));
            assert!(v_set.iter().all(|v| read_guard[k].contains(v)));

            assert!(!r_visit.contains_key(k));

            // If the value set is empty, we still need to add the key.
            // But we need to add the key with an empty value bag.
            // Just add something and then remove the value, but leave the bag.
            if v_set.is_empty() {
                w_visit.insert(*k, 0);
                w_visit.remove_value(*k, 0);
            } else {
                for value in v_set.iter() {
                    w_visit.insert(*k, *value);
                }
            }
            w_visit.publish();
        }
        assert_eq!(r_visit.len(), read_ref.len());
    };
}
