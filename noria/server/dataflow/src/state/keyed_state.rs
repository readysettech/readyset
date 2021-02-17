use indexmap::IndexMap;
use std::ops::{Bound, RangeBounds};
use std::rc::Rc;
use tuple::Map;
use tuple::TupleElements;
use vec1::Vec1;

use super::mk_key::MakeKey;
use super::partial_map::PartialMap;
use super::Misses;
use crate::prelude::*;
use common::SizeOf;
use launchpad::intervals::BoundFunctor;

/// A map containing a single index into the state of a node.
///
/// KeyedStates are associative (key-value) maps from lists of [`DataType`]s of length between 1 and
/// 6 inclusive to [lists of reference-counted pointers to rows](Rows), and can be backed by either
/// a [`BTreeMap`](std::collections::BTreeMap) or an [`IndexMap`], according to an
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
}

impl KeyedState {
    pub(super) fn lookup<'a>(&'a self, key: &KeyType) -> Option<&'a Rows> {
        match (self, key) {
            (&KeyedState::SingleBTree(ref m), &KeyType::Single(k)) => m.get(k),
            (&KeyedState::DoubleBTree(ref m), &KeyType::Double(ref k)) => m.get(k),
            (&KeyedState::TriBTree(ref m), &KeyType::Tri(ref k)) => m.get(k),
            (&KeyedState::QuadBTree(ref m), &KeyType::Quad(ref k)) => m.get(k),
            (&KeyedState::QuinBTree(ref m), &KeyType::Quin(ref k)) => m.get(k),
            (&KeyedState::SexBTree(ref m), &KeyType::Sex(ref k)) => m.get(k),
            (&KeyedState::SingleHash(ref m), &KeyType::Single(k)) => m.get(k),
            (&KeyedState::DoubleHash(ref m), &KeyType::Double(ref k)) => m.get(k),
            (&KeyedState::TriHash(ref m), &KeyType::Tri(ref k)) => m.get(k),
            (&KeyedState::QuadHash(ref m), &KeyType::Quad(ref k)) => m.get(k),
            (&KeyedState::QuinHash(ref m), &KeyType::Quin(ref k)) => m.get(k),
            (&KeyedState::SexHash(ref m), &KeyType::Sex(ref k)) => m.get(k),
            _ => panic!("Invalid key type for KeyedState, got: {:?}", key),
        }
    }

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
            _ => panic!("insert_range called on a HashMap KeyedState"),
        };
    }

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
            (
                KeyedState::SingleHash(_)
                | KeyedState::DoubleHash(_)
                | KeyedState::TriHash(_)
                | KeyedState::QuadHash(_)
                | KeyedState::SexHash(_),
                _,
            ) => panic!("lookup_range called on a HashMap KeyedState"),
            _ => panic!("Invalid key type for KeyedState, got: {:?}", key),
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

    /// Remove all rows for the given key, returning the number of bytes freed.
    pub(super) fn evict(&mut self, key: &[DataType]) -> u64 {
        match *self {
            KeyedState::SingleBTree(ref mut m) => m.remove(&(key[0])),
            KeyedState::DoubleBTree(ref mut m) => m.remove(&MakeKey::from_key(key)),
            KeyedState::TriBTree(ref mut m) => m.remove(&MakeKey::from_key(key)),
            KeyedState::QuadBTree(ref mut m) => m.remove(&MakeKey::from_key(key)),
            KeyedState::QuinBTree(ref mut m) => m.remove(&MakeKey::from_key(key)),
            KeyedState::SexBTree(ref mut m) => m.remove(&MakeKey::from_key(key)),

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
        }
        .map(|rows| {
            rows.iter()
                .filter(|r| Rc::strong_count(&r.0) == 1)
                .map(SizeOf::deep_size_of)
                .sum()
        })
        .unwrap_or(0)
    }

    pub(super) fn evict_range<R>(&mut self, range: &R) -> u64
    where
        R: RangeBounds<Vec1<DataType>>,
    {
        macro_rules! do_evict_range {
            ($m: expr, $range: expr, $hint: ty) => {
                $m.remove_range(<$hint as MakeKey<DataType>>::from_range($range))
                    .map(|(_, rows)| -> u64 {
                        rows.iter()
                            .filter(|r| Rc::strong_count(&r.0) == 1)
                            .map(SizeOf::deep_size_of)
                            .sum()
                    })
                    .sum()
            };
        }

        match self {
            KeyedState::SingleBTree(m) => do_evict_range!(m, range, DataType),
            KeyedState::DoubleBTree(m) => do_evict_range!(m, range, (DataType, _)),
            KeyedState::TriBTree(m) => do_evict_range!(m, range, (DataType, _, _)),
            KeyedState::QuadBTree(m) => do_evict_range!(m, range, (DataType, _, _, _)),
            KeyedState::QuinBTree(m) => do_evict_range!(m, range, (DataType, _, _, _, _)),
            KeyedState::SexBTree(m) => do_evict_range!(m, range, (DataType, _, _, _, _, _)),
            _ => panic!("evict_range called on a HashMap KeyedState"),
        }
    }
}

impl From<&Index> for KeyedState {
    fn from(index: &Index) -> Self {
        use IndexType::*;
        match (index.key_length(), &index.index_type) {
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
            (x, _) => panic!("invalid compound key of length: {}", x),
        }
    }
}
