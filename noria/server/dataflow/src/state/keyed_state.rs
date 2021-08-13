use indexmap::IndexMap;
use std::iter;
use std::ops::{Bound, RangeBounds};
use std::rc::Rc;
use tuple::Map;
use tuple::TupleElements;
use vec1::Vec1;

use super::mk_key::MakeKey;
use super::partial_map::{self, PartialMap};
use super::Misses;
use crate::prelude::*;
use common::SizeOf;
use launchpad::intervals::into_bound_endpoint;

/// A map containing a single index into the state of a node.
///
/// KeyedStates are associative (key-value) maps from lists of [`DataType`]s of length of at least 1
/// to [lists of reference-counted pointers to rows](Rows), and can be backed by either a
/// [`BTreeMap`](std::collections::BTreeMap) or an [`IndexMap`], according to an
/// [`IndexType`](noria::IndexType).
///
/// Any operations on a KeyedState that are unsupported by the index type, such as inserting or
/// looking up ranges in a HashMap, will panic.
#[allow(clippy::type_complexity)]
pub(super) enum KeyedState {
    SingleBTree(PartialMap<DataType, Rows>),
    DoubleBTree(PartialMap<(DataType, DataType), Rows>),
    TriBTree(PartialMap<(DataType, DataType, DataType), Rows>),
    QuadBTree(PartialMap<(DataType, DataType, DataType, DataType), Rows>),
    QuinBTree(PartialMap<(DataType, DataType, DataType, DataType, DataType), Rows>),
    SexBTree(PartialMap<(DataType, DataType, DataType, DataType, DataType, DataType), Rows>),
    // the `usize` parameter is the length of the Vec.
    MultiBTree(PartialMap<Vec<DataType>, Rows>, usize),

    SingleHash(IndexMap<DataType, Rows, ahash::RandomState>),
    DoubleHash(IndexMap<(DataType, DataType), Rows, ahash::RandomState>),
    TriHash(IndexMap<(DataType, DataType, DataType), Rows, ahash::RandomState>),
    QuadHash(IndexMap<(DataType, DataType, DataType, DataType), Rows, ahash::RandomState>),
    QuinHash(
        IndexMap<(DataType, DataType, DataType, DataType, DataType), Rows, ahash::RandomState>,
    ),
    SexHash(
        IndexMap<
            (DataType, DataType, DataType, DataType, DataType, DataType),
            Rows,
            ahash::RandomState,
        >,
    ),
    // ♪ multi-hash ♪ https://www.youtube.com/watch?v=bEtDVy55shI
    // (`usize` parameter as in `MultiBTree`)
    MultiHash(IndexMap<Vec<DataType>, Rows>, usize),
}

impl KeyedState {
    /// Look up all the rows corresponding to the given `key` and return them, or return None if no
    /// rows exist for the given key
    ///
    /// # Panics
    ///
    /// Panics if the length of `key` is different than the length of this `KeyedState`
    pub(super) fn lookup<'a>(&'a self, key: &KeyType) -> Option<&'a Rows> {
        match (self, key) {
            (&KeyedState::SingleBTree(ref m), &KeyType::Single(k)) => m.get(k),
            (&KeyedState::DoubleBTree(ref m), &KeyType::Double(ref k)) => m.get(k),
            (&KeyedState::TriBTree(ref m), &KeyType::Tri(ref k)) => m.get(k),
            (&KeyedState::QuadBTree(ref m), &KeyType::Quad(ref k)) => m.get(k),
            (&KeyedState::QuinBTree(ref m), &KeyType::Quin(ref k)) => m.get(k),
            (&KeyedState::SexBTree(ref m), &KeyType::Sex(ref k)) => m.get(k),
            (&KeyedState::MultiBTree(ref m, len), &KeyType::Multi(ref k)) if k.len() == len => {
                m.get(k)
            }
            (&KeyedState::SingleHash(ref m), &KeyType::Single(k)) => m.get(k),
            (&KeyedState::DoubleHash(ref m), &KeyType::Double(ref k)) => m.get(k),
            (&KeyedState::TriHash(ref m), &KeyType::Tri(ref k)) => m.get(k),
            (&KeyedState::QuadHash(ref m), &KeyType::Quad(ref k)) => m.get(k),
            (&KeyedState::QuinHash(ref m), &KeyType::Quin(ref k)) => m.get(k),
            (&KeyedState::SexHash(ref m), &KeyType::Sex(ref k)) => m.get(k),
            (&KeyedState::MultiHash(ref m, len), &KeyType::Multi(ref k)) if k.len() == len => {
                m.get(k)
            }
            _ =>
            #[allow(clippy::panic)] // documented invariant
            {
                panic!(
                    "Invalid key type for KeyedState, got key of length {}",
                    key.len()
                )
            }
        }
    }

    /// Insert the given `row` into this `KeyedState`, using the column indices in `key_cols` to
    /// derive the key, and return whether or not the row was actually inserted
    ///
    /// If `partial` is `true`, and the key is not present, the row will not be inserted and
    /// `insert` will return `false`.
    ///
    /// # Invariants
    ///
    /// * The length of `key_cols` must be equal to the length of the key of this KeyedState
    /// * All column indices in `key_cols` must be in-bounds for `row`
    pub(super) fn insert(&mut self, key_cols: &[usize], row: Row, partial: bool) -> bool {
        macro_rules! single_insert {
            ($map: ident, $key_cols: expr, $row: expr, $partial: expr) => {{
                // treat this specially to avoid the extra Vec
                debug_assert_eq!($key_cols.len(), 1);
                // i *wish* we could use the entry API here, but it would mean an extra clone
                // in the common case of an entry already existing for the given key...
                let key = &row[key_cols[0]];
                if let Some(ref mut rs) = $map.get_mut(key) {
                    rs.insert(row);
                    return true;
                } else if $partial {
                    // trying to insert a record into partial materialization hole!
                    return false;
                }

                $map.insert(key.clone(), iter::once(row).collect());
            }};
        }

        macro_rules! multi_insert {
            ($map: ident, $key_cols: expr, $row:expr, $partial: expr, $entry:path) => {{
                let key = MakeKey::from_row($key_cols, &*$row);
                use $entry as Entry;
                match $map.entry(key) {
                    Entry::Occupied(mut rs) => {
                        rs.get_mut().insert($row);
                    }
                    Entry::Vacant(..) if $partial => return false,
                    rs @ Entry::Vacant(..) => {
                        rs.or_default().insert($row);
                    }
                }
            }};
        }

        match self {
            KeyedState::SingleBTree(map) => single_insert!(map, key_cols, row, partial),
            KeyedState::DoubleBTree(map) => {
                multi_insert!(map, key_cols, row, partial, partial_map::Entry)
            }
            KeyedState::TriBTree(map) => {
                multi_insert!(map, key_cols, row, partial, partial_map::Entry)
            }
            KeyedState::QuadBTree(map) => {
                multi_insert!(map, key_cols, row, partial, partial_map::Entry)
            }
            KeyedState::QuinBTree(map) => {
                multi_insert!(map, key_cols, row, partial, partial_map::Entry)
            }
            KeyedState::SexBTree(map) => {
                multi_insert!(map, key_cols, row, partial, partial_map::Entry)
            }
            KeyedState::MultiBTree(map, len) => {
                debug_assert_eq!(key_cols.len(), *len);
                multi_insert!(map, key_cols, row, partial, partial_map::Entry)
            }
            KeyedState::SingleHash(map) => single_insert!(map, key_cols, row, partial),
            KeyedState::DoubleHash(map) => {
                multi_insert!(map, key_cols, row, partial, indexmap::map::Entry)
            }
            KeyedState::TriHash(map) => {
                multi_insert!(map, key_cols, row, partial, indexmap::map::Entry)
            }
            KeyedState::QuadHash(map) => {
                multi_insert!(map, key_cols, row, partial, indexmap::map::Entry)
            }
            KeyedState::QuinHash(map) => {
                multi_insert!(map, key_cols, row, partial, indexmap::map::Entry)
            }
            KeyedState::SexHash(map) => {
                multi_insert!(map, key_cols, row, partial, indexmap::map::Entry)
            }
            KeyedState::MultiHash(map, len) => {
                debug_assert_eq!(key_cols.len(), *len);
                multi_insert!(map, key_cols, row, partial, indexmap::map::Entry)
            }
        }

        true
    }

    /// Remove one instance of the given `row` from this `KeyedState`, using the column indices in
    /// `key_cols` to derive the key, and return the row itself.
    ///
    /// If given, `hit` will be set to `true` if the key exists in `self` (but not necessarily if
    /// the row was found!)
    ///
    /// # Invariants
    ///
    /// * The length of `key_cols` must be equal to the length of the key of this KeyedState
    /// * All column indices in `key_cols` must be in-bounds for `row`
    pub(super) fn remove(
        &mut self,
        key_cols: &[usize],
        row: &[DataType],
        hit: Option<&mut bool>,
    ) -> Option<Row> {
        let do_remove = |rs: &mut Rows| -> Option<Row> {
            if let Some(hit) = hit {
                *hit = true;
            }
            let rm = if rs.len() == 1 {
                // it *should* be impossible to get a negative for a record that we don't have,
                // so let's avoid hashing + eqing if we don't need to
                let left = rs.drain().next().unwrap();
                debug_assert_eq!(left.1, 1);
                debug_assert_eq!(&left.0[..], row);
                Some(left.0)
            } else {
                match rs.try_take(row) {
                    Ok(row) => Some(row),
                    Err(None) => None,
                    Err(Some((row, _))) => {
                        // there are still copies of the row left in rs
                        Some(row.clone())
                    }
                }
            };
            rm
        };

        macro_rules! single_remove {
            ($map: ident, $key_cols: expr, $row: expr) => {{
                if let Some(rs) = $map.get_mut(&row[$key_cols[0]]) {
                    return do_remove(rs);
                }
            }};
        }

        macro_rules! multi_remove {
            ($map: ident, $key_cols: expr, $row: expr) => {
                multi_remove!($map, $key_cols, $row, _)
            };
            ($map: ident, $key_cols: expr, $row: expr, $hint: ty) => {{
                let key = <$hint as MakeKey<_>>::from_row(&$key_cols, $row);
                if let Some(rs) = $map.get_mut(&key) {
                    return do_remove(rs);
                }
            }};
        }

        match self {
            KeyedState::SingleBTree(map) => single_remove!(map, key_cols, row),
            KeyedState::DoubleBTree(map) => multi_remove!(map, key_cols, row),
            KeyedState::TriBTree(map) => multi_remove!(map, key_cols, row),
            KeyedState::QuadBTree(map) => multi_remove!(map, key_cols, row),
            KeyedState::QuinBTree(map) => multi_remove!(map, key_cols, row),
            KeyedState::SexBTree(map) => multi_remove!(map, key_cols, row),
            KeyedState::MultiBTree(map, len) => {
                debug_assert_eq!(key_cols.len(), *len);
                multi_remove!(map, key_cols, row, Vec<_>);
            }
            KeyedState::SingleHash(map) => single_remove!(map, key_cols, row),
            KeyedState::DoubleHash(map) => multi_remove!(map, key_cols, row, (_, _)),
            KeyedState::TriHash(map) => multi_remove!(map, key_cols, row, (_, _, _)),
            KeyedState::QuadHash(map) => multi_remove!(map, key_cols, row, (_, _, _, _)),
            KeyedState::QuinHash(map) => multi_remove!(map, key_cols, row, (_, _, _, _, _)),
            KeyedState::SexHash(map) => multi_remove!(map, key_cols, row, (_, _, _, _, _, _)),
            KeyedState::MultiHash(map, len) => {
                debug_assert_eq!(key_cols.len(), *len);
                multi_remove!(map, key_cols, row, Vec<_>);
            }
        }

        None
    }

    /// Mark the given range of keys as filled
    ///
    /// # Panics
    ///
    /// Panics if this `KeyedState` is backed by a HashMap index
    pub(super) fn insert_range(&mut self, range: (Bound<Vec1<DataType>>, Bound<Vec1<DataType>>)) {
        match self {
            KeyedState::SingleBTree(ref mut map) => map.insert_range((
                range.0.map(|k| k.split_off_first().0),
                range.1.map(|k| k.split_off_first().0),
            )),
            KeyedState::DoubleBTree(ref mut map) => {
                map.insert_range(<(DataType, _) as MakeKey<_>>::from_range(&range))
            }
            KeyedState::TriBTree(ref mut map) => {
                map.insert_range(<(DataType, _, _) as MakeKey<_>>::from_range(&range))
            }
            KeyedState::QuadBTree(ref mut map) => {
                map.insert_range(<(DataType, _, _, _) as MakeKey<_>>::from_range(&range))
            }
            KeyedState::QuinBTree(ref mut map) => {
                map.insert_range(<(DataType, _, _, _, _) as MakeKey<_>>::from_range(&range))
            }
            KeyedState::SexBTree(ref mut map) => map.insert_range(
                <(DataType, _, _, _, _, _) as MakeKey<_>>::from_range(&range),
            ),
            // This is unwieldy, but allowing callers to insert the wrong length of Vec into us would
            // be very bad!
            KeyedState::MultiBTree(ref mut map, len)
                if (into_bound_endpoint(range.0.as_ref()).map_or(true, |x| x.len() == *len)
                    && into_bound_endpoint(range.1.as_ref()).map_or(true, |x| x.len() == *len)) =>
            {
                map.insert_range((range.0.map(Vec1::into_vec), range.1.map(Vec1::into_vec)))
            }
            _ =>
            #[allow(clippy::panic)] // documented invariant
            {
                panic!("insert_range called on a HashMap KeyedState")
            }
        };
    }

    /// Look up all the keys in the given range `key`, and return either iterator over all the rows
    /// or a set of [`Misses`] indicating that some keys are not present
    ///
    /// # Panics
    ///
    /// * Panics if the length of `key` is different than the length of this `KeyedState`
    /// * Panics if this `KeyedState` is backed by a HashMap index
    pub(super) fn lookup_range<'a>(
        &'a self,
        key: &RangeKey,
    ) -> Result<Box<dyn Iterator<Item = &'a Row> + 'a>, Misses> {
        fn to_misses<K: TupleElements<Element = DataType>>(
            misses: Vec<(Bound<K>, Bound<K>)>,
        ) -> Misses {
            misses
                .into_iter()
                .map(|(lower, upper)| {
                    (
                        lower.map(|k| k.into_elements().collect()),
                        upper.map(|k| k.into_elements().collect()),
                    )
                })
                .collect()
        }

        fn flatten_rows<'a, K: 'a, I: Iterator<Item = (&'a K, &'a Rows)> + 'a>(
            r: I,
        ) -> Box<dyn Iterator<Item = &'a Row> + 'a> {
            Box::new(r.flat_map(|(_, rows)| rows))
        }

        macro_rules! full_range {
            ($m: expr) => {
                $m.range(..).map(flatten_rows).map_err(to_misses)
            };
        }

        macro_rules! range {
            ($m: expr, $range: ident) => {
                $m.range((
                    $range.0.map(|k| k.map(Clone::clone)),
                    $range.1.map(|k| k.map(Clone::clone)),
                ))
                .map(flatten_rows)
                .map_err(to_misses)
            };
        }

        match (self, key) {
            (&KeyedState::SingleBTree(ref m), &RangeKey::Unbounded) => m
                .range(..)
                .map_err(|misses| {
                    misses
                        .into_iter()
                        .map(|(lower, upper)| (lower.map(|k| vec![k]), upper.map(|k| vec![k])))
                        .collect()
                })
                .map(flatten_rows),
            (&KeyedState::DoubleBTree(ref m), &RangeKey::Unbounded) => full_range!(m),
            (&KeyedState::TriBTree(ref m), &RangeKey::Unbounded) => full_range!(m),
            (&KeyedState::QuadBTree(ref m), &RangeKey::Unbounded) => full_range!(m),
            (&KeyedState::SexBTree(ref m), &RangeKey::Unbounded) => full_range!(m),
            (&KeyedState::SingleBTree(ref m), &RangeKey::Single(range)) => {
                m.range(range).map(flatten_rows).map_err(|misses| {
                    misses
                        .into_iter()
                        .map(|(lower, upper)| (lower.map(|k| vec![k]), upper.map(|k| vec![k])))
                        .collect()
                })
            }
            (&KeyedState::DoubleBTree(ref m), &RangeKey::Double(range)) => range!(m, range),
            (&KeyedState::TriBTree(ref m), &RangeKey::Tri(range)) => range!(m, range),
            (&KeyedState::QuadBTree(ref m), &RangeKey::Quad(range)) => range!(m, range),
            (&KeyedState::SexBTree(ref m), &RangeKey::Sex(range)) => range!(m, range),
            (&KeyedState::MultiBTree(ref m, _), &RangeKey::Multi(range)) => m
                .range((range.0.map(|x| x.to_owned()), range.1.map(|x| x.to_owned())))
                .map(flatten_rows),
            (
                KeyedState::SingleHash(_)
                | KeyedState::DoubleHash(_)
                | KeyedState::TriHash(_)
                | KeyedState::QuadHash(_)
                | KeyedState::SexHash(_)
                | KeyedState::MultiHash(..),
                _,
            ) =>
            #[allow(clippy::panic)] // documented invariant
            {
                panic!("lookup_range called on a HashMap KeyedState")
            }
            _ =>
            #[allow(clippy::panic)] // documented invariant
            {
                panic!(
                    "Invalid key type for KeyedState, got key of length {:?}",
                    key.len()
                )
            }
        }
    }

    /// Remove all rows for a randomly chosen key seeded by `seed`, returning that key along with
    /// the number of bytes freed. Returns `None` if map is empty.
    pub(super) fn evict_with_seed(&mut self, seed: usize) -> Option<(u64, Vec<DataType>)> {
        let (rs, key) = match *self {
            KeyedState::SingleHash(ref mut m) => {
                let index = seed % m.len();
                m.swap_remove_index(index).map(|(k, rs)| (rs, vec![k]))
            }
            KeyedState::DoubleHash(ref mut m) => {
                let index = seed % m.len();
                m.swap_remove_index(index)
                    .map(|(k, rs)| (rs, k.into_elements().collect()))
            }
            KeyedState::TriHash(ref mut m) => {
                let index = seed % m.len();
                m.swap_remove_index(index)
                    .map(|(k, rs)| (rs, k.into_elements().collect()))
            }
            KeyedState::QuadHash(ref mut m) => {
                let index = seed % m.len();
                m.swap_remove_index(index)
                    .map(|(k, rs)| (rs, k.into_elements().collect()))
            }
            KeyedState::SexHash(ref mut m) => {
                let index = seed % m.len();
                m.swap_remove_index(index)
                    .map(|(k, rs)| (rs, k.into_elements().collect()))
            }
            KeyedState::MultiHash(ref mut m, _) => {
                let index = seed % m.len();
                m.swap_remove_index(index).map(|(k, rs)| (rs, k))
            }

            // TODO(grfn): This way of evicting (which also happens in reader_map) is pretty icky - we
            // have to iterate the sequence of keys, *and* we have to clone out the keys themselves! we
            // should find a better way to do that.
            // https://app.clubhouse.io/readysettech/story/154
            KeyedState::SingleBTree(ref mut m) if !m.is_empty() => {
                let index = seed % m.len();
                let key = m.keys().nth(index).unwrap().clone();
                m.remove_entry(&key).map(|(k, rs)| (rs, vec![k]))
            }
            KeyedState::DoubleBTree(ref mut m) if !m.is_empty() => {
                let index = seed % m.len();
                let key = m.keys().nth(index).unwrap().clone();
                m.remove_entry(&key).map(|(k, rs)| (rs, vec![k.0, k.1]))
            }
            KeyedState::TriBTree(ref mut m) if !m.is_empty() => {
                let index = seed % m.len();
                let key = m.keys().nth(index).unwrap().clone();
                m.remove_entry(&key)
                    .map(|(k, rs)| (rs, vec![k.0, k.1, k.2]))
            }
            KeyedState::QuadBTree(ref mut m) if !m.is_empty() => {
                let index = seed % m.len();
                let key = m.keys().nth(index).unwrap().clone();
                m.remove_entry(&key)
                    .map(|(k, rs)| (rs, vec![k.0, k.1, k.2, k.3]))
            }
            KeyedState::QuinBTree(ref mut m) if !m.is_empty() => {
                let index = seed % m.len();
                let key = m.keys().nth(index).unwrap().clone();
                m.remove_entry(&key)
                    .map(|(k, rs)| (rs, vec![k.0, k.1, k.2, k.3, k.4]))
            }
            KeyedState::SexBTree(ref mut m) if !m.is_empty() => {
                let index = seed % m.len();
                let key = m.keys().nth(index).unwrap().clone();
                m.remove_entry(&key)
                    .map(|(k, rs)| (rs, vec![k.0, k.1, k.2, k.3, k.4, k.5]))
            }
            KeyedState::MultiBTree(ref mut m, _) if !m.is_empty() => {
                let index = seed % m.len();
                let key = m.keys().nth(index).unwrap().clone();
                m.remove_entry(&key).map(|(k, rs)| (rs, k))
            }
            _ => {
                // map must be empty, so no point in trying to evict from it.
                return None;
            }
        }?;
        Some((
            rs.iter()
                .filter(|r| Rc::strong_count(&r.0) == 1)
                .map(SizeOf::deep_size_of)
                .sum(),
            key,
        ))
    }

    /// Remove all rows for the given key, returning the evicted rows.
    pub(super) fn evict(&mut self, key: &[DataType]) -> Option<Rows> {
        match *self {
            KeyedState::SingleBTree(ref mut m) => m.remove(&(key[0])),
            KeyedState::DoubleBTree(ref mut m) => m.remove(&MakeKey::from_key(key)),
            KeyedState::TriBTree(ref mut m) => m.remove(&MakeKey::from_key(key)),
            KeyedState::QuadBTree(ref mut m) => m.remove(&MakeKey::from_key(key)),
            KeyedState::QuinBTree(ref mut m) => m.remove(&MakeKey::from_key(key)),
            KeyedState::SexBTree(ref mut m) => m.remove(&MakeKey::from_key(key)),
            // FIXME(eta): this clones unnecessarily, given we could make PartialMap do the Borrow thing.
            // That requres making the unbounded-interval-tree crate do that as well, though, and that's painful.
            // (also everything else in here clones -- I do wonder what the perf impacts of that are)
            KeyedState::MultiBTree(ref mut m, _) => m.remove(&key.to_owned()),

            KeyedState::SingleHash(ref mut m) => m.remove(&(key[0])),
            KeyedState::DoubleHash(ref mut m) => m.remove::<(DataType, _)>(&MakeKey::from_key(key)),
            KeyedState::TriHash(ref mut m) => m.remove::<(DataType, _, _)>(&MakeKey::from_key(key)),
            KeyedState::QuadHash(ref mut m) => {
                m.remove::<(DataType, _, _, _)>(&MakeKey::from_key(key))
            }
            KeyedState::QuinHash(ref mut m) => {
                m.remove::<(DataType, _, _, _, _)>(&MakeKey::from_key(key))
            }
            KeyedState::SexHash(ref mut m) => {
                m.remove::<(DataType, _, _, _, _, _)>(&MakeKey::from_key(key))
            }
            KeyedState::MultiHash(ref mut m, _) => m.remove(&key.to_owned()),
        }
    }

    /// Evict all rows in the given range of keys from this KeyedState, and return the removed rows
    ///
    /// # Panics
    ///
    /// Panics if this `KeyedState` is backed by a HashMap index
    pub(super) fn evict_range<R>(&mut self, range: &R) -> Rows
    where
        R: RangeBounds<Vec1<DataType>>,
    {
        macro_rules! do_evict_range {
            ($m: expr, $range: expr, $hint: ty) => {
                $m.remove_range(<$hint as MakeKey<DataType>>::from_range($range))
                    .flat_map(|(_, rows)| rows.into_iter().map(|(r, _)| r))
                    .collect()
            };
        }

        match self {
            KeyedState::SingleBTree(m) => do_evict_range!(m, range, DataType),
            KeyedState::DoubleBTree(m) => do_evict_range!(m, range, (DataType, _)),
            KeyedState::TriBTree(m) => do_evict_range!(m, range, (DataType, _, _)),
            KeyedState::QuadBTree(m) => do_evict_range!(m, range, (DataType, _, _, _)),
            KeyedState::QuinBTree(m) => do_evict_range!(m, range, (DataType, _, _, _, _)),
            KeyedState::SexBTree(m) => do_evict_range!(m, range, (DataType, _, _, _, _, _)),
            KeyedState::MultiBTree(m, _) => m
                .remove_range((
                    range.start_bound().map(Vec1::as_vec),
                    range.end_bound().map(Vec1::as_vec),
                ))
                .flat_map(|(_, rows)| rows.into_iter().map(|(r, _)| r))
                .collect(),
            _ => {
                #[allow(clippy::panic)] // documented invariant
                {
                    panic!("evict_range called on a HashMap KeyedState")
                }
            }
        }
    }
}

impl From<&Index> for KeyedState {
    fn from(index: &Index) -> Self {
        use IndexType::*;
        match (index.len(), &index.index_type) {
            (1, BTreeMap) => KeyedState::SingleBTree(Default::default()),
            (2, BTreeMap) => KeyedState::DoubleBTree(Default::default()),
            (3, BTreeMap) => KeyedState::TriBTree(Default::default()),
            (4, BTreeMap) => KeyedState::QuadBTree(Default::default()),
            (5, BTreeMap) => KeyedState::QuinBTree(Default::default()),
            (6, BTreeMap) => KeyedState::SexBTree(Default::default()),
            (1, HashMap) => KeyedState::SingleHash(Default::default()),
            (2, HashMap) => KeyedState::DoubleHash(Default::default()),
            (3, HashMap) => KeyedState::TriHash(Default::default()),
            (4, HashMap) => KeyedState::QuadHash(Default::default()),
            (5, HashMap) => KeyedState::QuinHash(Default::default()),
            (6, HashMap) => KeyedState::SexHash(Default::default()),
            (x, HashMap) => KeyedState::MultiHash(Default::default(), x),
            (x, BTreeMap) => KeyedState::MultiBTree(Default::default(), x),
        }
    }
}
