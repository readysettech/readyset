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

#[allow(clippy::type_complexity)]
pub(super) enum KeyedState {
    Single(PartialMap<DataType, Rows>),
    Double(PartialMap<(DataType, DataType), Rows>),
    Tri(PartialMap<(DataType, DataType, DataType), Rows>),
    Quad(PartialMap<(DataType, DataType, DataType, DataType), Rows>),
    Quin(PartialMap<(DataType, DataType, DataType, DataType, DataType), Rows>),
    Sex(PartialMap<(DataType, DataType, DataType, DataType, DataType, DataType), Rows>),
}

impl KeyedState {
    pub(super) fn lookup<'a>(&'a self, key: &KeyType) -> Option<&'a Rows> {
        match (self, key) {
            (&KeyedState::Single(ref m), &KeyType::Single(k)) => m.get(k),
            (&KeyedState::Double(ref m), &KeyType::Double(ref k)) => m.get(k),
            (&KeyedState::Tri(ref m), &KeyType::Tri(ref k)) => m.get(k),
            (&KeyedState::Quad(ref m), &KeyType::Quad(ref k)) => m.get(k),
            (&KeyedState::Quin(ref m), &KeyType::Quin(ref k)) => m.get(k),
            (&KeyedState::Sex(ref m), &KeyType::Sex(ref k)) => m.get(k),
            _ => panic!("Invalid key type for KeyedState, got: {:?}", key),
        }
    }

    pub(super) fn insert_range(&mut self, range: (Bound<Vec1<DataType>>, Bound<Vec1<DataType>>)) {
        match self {
            KeyedState::Single(ref mut map) => map.insert_range((
                range.0.map(|k| k.split_off_first().0),
                range.1.map(|k| k.split_off_first().0),
            )),
            KeyedState::Double(ref mut map) => {
                map.insert_range(<(DataType, _) as MakeKey<_>>::from_range(&range))
            }
            KeyedState::Tri(ref mut map) => {
                map.insert_range(<(DataType, _, _) as MakeKey<_>>::from_range(&range))
            }
            KeyedState::Quad(ref mut map) => {
                map.insert_range(<(DataType, _, _, _) as MakeKey<_>>::from_range(&range))
            }
            KeyedState::Quin(ref mut map) => {
                map.insert_range(<(DataType, _, _, _, _) as MakeKey<_>>::from_range(&range))
            }
            KeyedState::Sex(ref mut map) => map.insert_range(
                <(DataType, _, _, _, _, _) as MakeKey<_>>::from_range(&range),
            ),
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
            (&KeyedState::Single(ref m), &RangeKey::Unbounded) => m
                .range(..)
                .map_err(|misses| {
                    misses
                        .into_iter()
                        .map(|(lower, upper)| (lower.map(|k| vec![k]), upper.map(|k| vec![k])))
                        .collect()
                })
                .map(flatten_rows),
            (&KeyedState::Double(ref m), &RangeKey::Unbounded) => full_range!(m),
            (&KeyedState::Tri(ref m), &RangeKey::Unbounded) => full_range!(m),
            (&KeyedState::Quad(ref m), &RangeKey::Unbounded) => full_range!(m),
            (&KeyedState::Sex(ref m), &RangeKey::Unbounded) => full_range!(m),
            (&KeyedState::Single(ref m), &RangeKey::Single(range)) => {
                m.range(range).map(flatten_rows).map_err(|misses| {
                    misses
                        .into_iter()
                        .map(|(lower, upper)| (lower.map(|k| vec![k]), upper.map(|k| vec![k])))
                        .collect()
                })
            }
            (&KeyedState::Double(ref m), &RangeKey::Double(range)) => range!(m, range),
            (&KeyedState::Tri(ref m), &RangeKey::Tri(range)) => range!(m, range),
            (&KeyedState::Quad(ref m), &RangeKey::Quad(range)) => range!(m, range),
            (&KeyedState::Sex(ref m), &RangeKey::Sex(range)) => range!(m, range),
            _ => panic!("Invalid key type for KeyedState, got: {:?}", key),
        }
    }

    /// Remove all rows for a randomly chosen key seeded by `seed`, returning that key along with
    /// the number of bytes freed. Returns `None` if map is empty.
    pub(super) fn evict_with_seed(&mut self, seed: usize) -> Option<(u64, Vec<DataType>)> {
        // TODO(grfn): This way of evicting (which also happens in reader_map) is pretty icky - we
        // have to iterate the sequence of keys, *and* we have to clone out the keys themselves! we
        // should find a better way to do that.
        // https://app.clubhouse.io/readysettech/story/154
        let (rs, key) = match *self {
            KeyedState::Single(ref mut m) if !m.is_empty() => {
                let index = seed % m.len();
                let key = m.keys().nth(index).unwrap().clone();
                m.remove_entry(&key).map(|(k, rs)| (rs, vec![k]))
            }
            KeyedState::Double(ref mut m) if !m.is_empty() => {
                let index = seed % m.len();
                let key = m.keys().nth(index).unwrap().clone();
                m.remove_entry(&key).map(|(k, rs)| (rs, vec![k.0, k.1]))
            }
            KeyedState::Tri(ref mut m) if !m.is_empty() => {
                let index = seed % m.len();
                let key = m.keys().nth(index).unwrap().clone();
                m.remove_entry(&key)
                    .map(|(k, rs)| (rs, vec![k.0, k.1, k.2]))
            }
            KeyedState::Quad(ref mut m) if !m.is_empty() => {
                let index = seed % m.len();
                let key = m.keys().nth(index).unwrap().clone();
                m.remove_entry(&key)
                    .map(|(k, rs)| (rs, vec![k.0, k.1, k.2, k.3]))
            }
            KeyedState::Quin(ref mut m) if !m.is_empty() => {
                let index = seed % m.len();
                let key = m.keys().nth(index).unwrap().clone();
                m.remove_entry(&key)
                    .map(|(k, rs)| (rs, vec![k.0, k.1, k.2, k.3, k.4]))
            }
            KeyedState::Sex(ref mut m) if !m.is_empty() => {
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
            KeyedState::Single(ref mut m) => m.remove(&(key[0])),
            KeyedState::Double(ref mut m) => m.remove(&MakeKey::from_key(key)),
            KeyedState::Tri(ref mut m) => m.remove(&MakeKey::from_key(key)),
            KeyedState::Quad(ref mut m) => m.remove(&MakeKey::from_key(key)),
            KeyedState::Quin(ref mut m) => m.remove(&MakeKey::from_key(key)),

            KeyedState::Sex(ref mut m) => m.remove(&MakeKey::from_key(key)),
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
            KeyedState::Single(m) => do_evict_range!(m, range, DataType),
            KeyedState::Double(m) => do_evict_range!(m, range, (DataType, _)),
            KeyedState::Tri(m) => do_evict_range!(m, range, (DataType, _, _)),
            KeyedState::Quad(m) => do_evict_range!(m, range, (DataType, _, _, _)),
            KeyedState::Quin(m) => do_evict_range!(m, range, (DataType, _, _, _, _)),
            KeyedState::Sex(m) => do_evict_range!(m, range, (DataType, _, _, _, _, _)),
        }
    }
}

impl<'a> Into<KeyedState> for &'a [usize] {
    fn into(self) -> KeyedState {
        match self.len() {
            0 => unreachable!(),
            1 => KeyedState::Single(PartialMap::default()),
            2 => KeyedState::Double(PartialMap::default()),
            3 => KeyedState::Tri(PartialMap::default()),
            4 => KeyedState::Quad(PartialMap::default()),
            5 => KeyedState::Quin(PartialMap::default()),
            6 => KeyedState::Sex(PartialMap::default()),
            x => panic!("invalid compound key of length: {}", x),
        }
    }
}
