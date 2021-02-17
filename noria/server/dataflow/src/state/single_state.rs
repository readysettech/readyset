use super::mk_key::MakeKey;
use super::{partial_map, RangeLookupResult};
use crate::prelude::*;
use crate::state::keyed_state::KeyedState;
use common::SizeOf;
use noria::KeyComparison;
use rand::prelude::*;
use std::ops::Bound;
use std::rc::Rc;
use vec1::Vec1;

pub(super) struct SingleState {
    key: Vec<usize>,
    state: KeyedState,
    partial: bool,
    rows: usize,
}

impl SingleState {
    pub(super) fn new(index: &Index, partial: bool) -> Self {
        let mut state = KeyedState::from(index);
        if !partial && index.index_type == IndexType::BTreeMap {
            state.insert_range((Bound::Unbounded, Bound::Unbounded))
        }
        Self {
            key: index.columns.clone(),
            state,
            partial,
            rows: 0,
        }
    }

    /// Inserts the given record, or returns false if a hole was encountered (and the record hence
    /// not inserted).
    pub(super) fn insert_row(&mut self, r: Row) -> bool {
        macro_rules! insert_row_match_impl {
            ($self:ident, $r:ident, $map:ident, $entry:path) => {{
                use $entry as Entry;
                let key = MakeKey::from_row(&$self.key, &*$r);
                match $map.entry(key) {
                    Entry::Occupied(mut rs) => {
                        rs.get_mut().insert($r);
                    }
                    Entry::Vacant(..) if $self.partial => return false,
                    rs @ Entry::Vacant(..) => {
                        rs.or_default().insert($r);
                    }
                }
            }};
        }

        match self.state {
            KeyedState::SingleBTree(ref mut map) => {
                // treat this specially to avoid the extra Vec
                debug_assert_eq!(self.key.len(), 1);
                // i *wish* we could use the entry API here, but it would mean an extra clone
                // in the common case of an entry already existing for the given key...
                if let Some(ref mut rs) = map.get_mut(&r[self.key[0]]) {
                    self.rows += 1;
                    rs.insert(r);
                    return true;
                } else if self.partial {
                    // trying to insert a record into partial materialization hole!
                    return false;
                }
                map.insert(r[self.key[0]].clone(), std::iter::once(r).collect());
            }
            KeyedState::DoubleBTree(ref mut map) => {
                insert_row_match_impl!(self, r, map, partial_map::Entry)
            }
            KeyedState::TriBTree(ref mut map) => {
                insert_row_match_impl!(self, r, map, partial_map::Entry)
            }
            KeyedState::QuadBTree(ref mut map) => {
                insert_row_match_impl!(self, r, map, partial_map::Entry)
            }
            KeyedState::QuinBTree(ref mut map) => {
                insert_row_match_impl!(self, r, map, partial_map::Entry)
            }
            KeyedState::SexBTree(ref mut map) => {
                insert_row_match_impl!(self, r, map, partial_map::Entry)
            }
            KeyedState::DoubleHash(ref mut map) => {
                insert_row_match_impl!(self, r, map, indexmap::map::Entry)
            }
            KeyedState::SingleHash(ref mut map) => {
                // treat this specially to avoid the extra Vec
                debug_assert_eq!(self.key.len(), 1);
                // i *wish* we could use the entry API here, but it would mean an extra clone
                // in the common case of an entry already existing for the given key...
                if let Some(ref mut rs) = map.get_mut(&r[self.key[0]]) {
                    self.rows += 1;
                    rs.insert(r);
                    return true;
                } else if self.partial {
                    // trying to insert a record into partial materialization hole!
                    return false;
                }
                map.insert(r[self.key[0]].clone(), std::iter::once(r).collect());
            }
            KeyedState::TriHash(ref mut map) => {
                insert_row_match_impl!(self, r, map, indexmap::map::Entry)
            }
            KeyedState::QuadHash(ref mut map) => {
                insert_row_match_impl!(self, r, map, indexmap::map::Entry)
            }
            KeyedState::QuinHash(ref mut map) => {
                insert_row_match_impl!(self, r, map, indexmap::map::Entry)
            }
            KeyedState::SexHash(ref mut map) => {
                insert_row_match_impl!(self, r, map, indexmap::map::Entry)
            }
        }

        self.rows += 1;
        true
    }

    /// Attempt to remove row `r`.
    pub(super) fn remove_row(&mut self, r: &[DataType], hit: &mut bool) -> Option<Row> {
        let mut do_remove = |self_rows: &mut usize, rs: &mut Rows| -> Option<Row> {
            *hit = true;
            let rm = if rs.len() == 1 {
                // it *should* be impossible to get a negative for a record that we don't have,
                // so let's avoid hashing + eqing if we don't need to
                let left = rs.drain().next().unwrap();
                debug_assert_eq!(left.1, 1);
                debug_assert_eq!(&left.0[..], r);
                Some(left.0)
            } else {
                match rs.try_take(r) {
                    Ok(row) => Some(row),
                    Err(None) => None,
                    Err(Some((row, _))) => {
                        // there are still copies of the row left in rs
                        Some(row.clone())
                    }
                }
            };

            if rm.is_some() {
                *self_rows = self_rows.checked_sub(1).unwrap();
            }
            rm
        };

        macro_rules! remove_row_match_impl {
            ($self:ident, $r:ident, $map:ident) => {
                remove_row_match_impl!($self, $r, $map, _)
            };
            ($self:ident, $r:ident, $map:ident, $hint:ty) => {{
                let key = <$hint as MakeKey<_>>::from_row(&$self.key, $r);
                if let Some(ref mut rs) = $map.get_mut(&key) {
                    return do_remove(&mut $self.rows, rs);
                }
            }};
        }

        match self.state {
            KeyedState::SingleBTree(ref mut map) => {
                if let Some(ref mut rs) = map.get_mut(&r[self.key[0]]) {
                    return do_remove(&mut self.rows, rs);
                }
            }
            KeyedState::DoubleBTree(ref mut map) => {
                remove_row_match_impl!(self, r, map)
            }
            KeyedState::TriBTree(ref mut map) => {
                remove_row_match_impl!(self, r, map)
            }
            KeyedState::QuadBTree(ref mut map) => {
                remove_row_match_impl!(self, r, map)
            }
            KeyedState::QuinBTree(ref mut map) => {
                remove_row_match_impl!(self, r, map)
            }
            KeyedState::SexBTree(ref mut map) => {
                remove_row_match_impl!(self, r, map)
            }
            KeyedState::SingleHash(ref mut map) => {
                if let Some(ref mut rs) = map.get_mut(&r[self.key[0]]) {
                    return do_remove(&mut self.rows, rs);
                }
            }
            KeyedState::DoubleHash(ref mut map) => {
                remove_row_match_impl!(self, r, map, (DataType, _))
            }
            KeyedState::TriHash(ref mut map) => {
                remove_row_match_impl!(self, r, map, (DataType, _, _))
            }
            KeyedState::QuadHash(ref mut map) => {
                remove_row_match_impl!(self, r, map, (DataType, _, _, _))
            }
            KeyedState::QuinHash(ref mut map) => {
                remove_row_match_impl!(self, r, map, (DataType, _, _, _, _))
            }
            KeyedState::SexHash(ref mut map) => {
                remove_row_match_impl!(self, r, map, (DataType, _, _, _, _, _))
            }
        }
        None
    }

    fn mark_point_filled(&mut self, key: Vec1<DataType>) {
        let mut key = key.into_iter();
        let replaced = match self.state {
            KeyedState::SingleBTree(ref mut map) => {
                map.insert(key.next().unwrap(), Rows::default())
            }
            KeyedState::DoubleBTree(ref mut map) => {
                map.insert((key.next().unwrap(), key.next().unwrap()), Rows::default())
            }
            KeyedState::TriBTree(ref mut map) => map.insert(
                (
                    key.next().unwrap(),
                    key.next().unwrap(),
                    key.next().unwrap(),
                ),
                Rows::default(),
            ),
            KeyedState::QuadBTree(ref mut map) => map.insert(
                (
                    key.next().unwrap(),
                    key.next().unwrap(),
                    key.next().unwrap(),
                    key.next().unwrap(),
                ),
                Rows::default(),
            ),
            KeyedState::QuinBTree(ref mut map) => map.insert(
                (
                    key.next().unwrap(),
                    key.next().unwrap(),
                    key.next().unwrap(),
                    key.next().unwrap(),
                    key.next().unwrap(),
                ),
                Rows::default(),
            ),
            KeyedState::SexBTree(ref mut map) => map.insert(
                (
                    key.next().unwrap(),
                    key.next().unwrap(),
                    key.next().unwrap(),
                    key.next().unwrap(),
                    key.next().unwrap(),
                    key.next().unwrap(),
                ),
                Rows::default(),
            ),
            KeyedState::SingleHash(ref mut map) => map.insert(key.next().unwrap(), Rows::default()),
            KeyedState::DoubleHash(ref mut map) => {
                map.insert((key.next().unwrap(), key.next().unwrap()), Rows::default())
            }
            KeyedState::TriHash(ref mut map) => map.insert(
                (
                    key.next().unwrap(),
                    key.next().unwrap(),
                    key.next().unwrap(),
                ),
                Rows::default(),
            ),
            KeyedState::QuadHash(ref mut map) => map.insert(
                (
                    key.next().unwrap(),
                    key.next().unwrap(),
                    key.next().unwrap(),
                    key.next().unwrap(),
                ),
                Rows::default(),
            ),
            KeyedState::QuinHash(ref mut map) => map.insert(
                (
                    key.next().unwrap(),
                    key.next().unwrap(),
                    key.next().unwrap(),
                    key.next().unwrap(),
                    key.next().unwrap(),
                ),
                Rows::default(),
            ),
            KeyedState::SexHash(ref mut map) => map.insert(
                (
                    key.next().unwrap(),
                    key.next().unwrap(),
                    key.next().unwrap(),
                    key.next().unwrap(),
                    key.next().unwrap(),
                    key.next().unwrap(),
                ),
                Rows::default(),
            ),
        };
        assert!(replaced.is_none());
    }

    fn mark_range_filled(&mut self, range: (Bound<Vec1<DataType>>, Bound<Vec1<DataType>>)) {
        self.state.insert_range(range);
    }

    pub(super) fn mark_filled(&mut self, key: KeyComparison) {
        match key {
            KeyComparison::Equal(k) => self.mark_point_filled(k),
            KeyComparison::Range(range) => self.mark_range_filled(range),
        }
    }

    pub(super) fn mark_hole(&mut self, key: &KeyComparison) -> u64 {
        let removed: Box<dyn Iterator<Item = (Row, usize)>> = match key {
            KeyComparison::Equal(key) => match self.state {
                KeyedState::SingleBTree(ref mut m) => {
                    Box::new(m.remove(&(key[0])).into_iter().flatten())
                }
                KeyedState::DoubleBTree(ref mut m) => {
                    Box::new(m.remove(&MakeKey::from_key(key)).into_iter().flatten())
                }
                KeyedState::TriBTree(ref mut m) => {
                    Box::new(m.remove(&MakeKey::from_key(key)).into_iter().flatten())
                }
                KeyedState::QuadBTree(ref mut m) => {
                    Box::new(m.remove(&MakeKey::from_key(key)).into_iter().flatten())
                }
                KeyedState::QuinBTree(ref mut m) => {
                    Box::new(m.remove(&MakeKey::from_key(key)).into_iter().flatten())
                }
                KeyedState::SexBTree(ref mut m) => {
                    Box::new(m.remove(&MakeKey::from_key(key)).into_iter().flatten())
                }
                KeyedState::SingleHash(ref mut m) => {
                    Box::new(m.remove(&(key[0])).into_iter().flatten())
                }
                KeyedState::DoubleHash(ref mut m) => Box::new(
                    m.remove::<(DataType, _)>(&MakeKey::from_key(key))
                        .into_iter()
                        .flatten(),
                ),
                KeyedState::TriHash(ref mut m) => Box::new(
                    m.remove::<(DataType, _, _)>(&MakeKey::from_key(key))
                        .into_iter()
                        .flatten(),
                ),
                KeyedState::QuadHash(ref mut m) => Box::new(
                    m.remove::<(DataType, _, _, _)>(&MakeKey::from_key(key))
                        .into_iter()
                        .flatten(),
                ),
                KeyedState::QuinHash(ref mut m) => Box::new(
                    m.remove::<(DataType, _, _, _, _)>(&MakeKey::from_key(key))
                        .into_iter()
                        .flatten(),
                ),
                KeyedState::SexHash(ref mut m) => Box::new(
                    m.remove::<(DataType, _, _, _, _, _)>(&MakeKey::from_key(key))
                        .into_iter()
                        .flatten(),
                ),
            },
            KeyComparison::Range(range) => {
                macro_rules! remove_range {
                    ($m: expr, $range: expr, $hint: ty) => {
                        Box::new(
                            $m.remove_range(<$hint as MakeKey<DataType>>::from_range(range))
                                .flat_map(|(_, rows)| rows),
                        )
                    };
                }

                match self.state {
                    KeyedState::SingleBTree(ref mut m) => remove_range!(m, range, DataType),
                    KeyedState::DoubleBTree(ref mut m) => remove_range!(m, range, (DataType, _)),
                    KeyedState::TriBTree(ref mut m) => remove_range!(m, range, (DataType, _, _)),
                    KeyedState::QuadBTree(ref mut m) => {
                        remove_range!(m, range, (DataType, _, _, _))
                    }
                    KeyedState::QuinBTree(ref mut m) => {
                        remove_range!(m, range, (DataType, _, _, _, _))
                    }
                    KeyedState::SexBTree(ref mut m) => {
                        remove_range!(m, range, (DataType, _, _, _, _, _))
                    }
                    _ => panic!("mark_hole with a range key called on a HashMap SingleState"),
                }
            }
        };

        removed
            .filter(|(r, _)| Rc::strong_count(&r.0) == 1)
            .map(|(r, count)| SizeOf::deep_size_of(&r) * (count as u64))
            .sum()
    }

    pub(super) fn clear(&mut self) {
        self.rows = 0;
        match self.state {
            KeyedState::SingleBTree(ref mut map) => map.clear(),
            KeyedState::DoubleBTree(ref mut map) => map.clear(),
            KeyedState::TriBTree(ref mut map) => map.clear(),
            KeyedState::QuadBTree(ref mut map) => map.clear(),
            KeyedState::QuinBTree(ref mut map) => map.clear(),
            KeyedState::SexBTree(ref mut map) => map.clear(),
            KeyedState::SingleHash(ref mut map) => map.clear(),
            KeyedState::DoubleHash(ref mut map) => map.clear(),
            KeyedState::TriHash(ref mut map) => map.clear(),
            KeyedState::QuadHash(ref mut map) => map.clear(),
            KeyedState::QuinHash(ref mut map) => map.clear(),
            KeyedState::SexHash(ref mut map) => map.clear(),
        };
    }

    /// Evict `count` randomly selected keys from state and return them along with the number of
    /// bytes freed.
    pub(super) fn evict_random_keys(
        &mut self,
        count: usize,
        rng: &mut ThreadRng,
    ) -> (u64, Vec<Vec<DataType>>) {
        let mut bytes_freed = 0;
        let mut keys = Vec::with_capacity(count);
        for _ in 0..count {
            if let Some((n, key)) = self.state.evict_with_seed(rng.gen()) {
                bytes_freed += n;
                keys.push(key);
            } else {
                break;
            }
        }
        (bytes_freed, keys)
    }

    /// Evicts a specified key from this state, returning the number of bytes freed.
    pub(super) fn evict_keys(&mut self, keys: &[KeyComparison]) -> u64 {
        keys.iter()
            .map(|k| match k {
                KeyComparison::Equal(equal) => self.state.evict(equal),
                KeyComparison::Range(range) => self.state.evict_range(range),
            })
            .sum()
    }

    pub(super) fn values<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Rows> + 'a> {
        match self.state {
            KeyedState::SingleBTree(ref map) => Box::new(map.values()),
            KeyedState::DoubleBTree(ref map) => Box::new(map.values()),
            KeyedState::TriBTree(ref map) => Box::new(map.values()),
            KeyedState::QuadBTree(ref map) => Box::new(map.values()),
            KeyedState::QuinBTree(ref map) => Box::new(map.values()),
            KeyedState::SexBTree(ref map) => Box::new(map.values()),
            KeyedState::SingleHash(ref map) => Box::new(map.values()),
            KeyedState::DoubleHash(ref map) => Box::new(map.values()),
            KeyedState::TriHash(ref map) => Box::new(map.values()),
            KeyedState::QuadHash(ref map) => Box::new(map.values()),
            KeyedState::QuinHash(ref map) => Box::new(map.values()),
            KeyedState::SexHash(ref map) => Box::new(map.values()),
        }
    }

    pub(super) fn key(&self) -> &[usize] {
        &self.key
    }
    pub(super) fn partial(&self) -> bool {
        self.partial
    }
    pub(super) fn rows(&self) -> usize {
        self.rows
    }
    pub(super) fn is_empty(&self) -> bool {
        self.rows == 0
    }

    pub(super) fn lookup<'a>(&'a self, key: &KeyType) -> LookupResult<'a> {
        if let Some(rs) = self.state.lookup(key) {
            LookupResult::Some(RecordResult::Borrowed(rs))
        } else if self.partial() {
            // partially materialized, so this is a hole (empty results would be vec![])
            LookupResult::Missing
        } else {
            LookupResult::Some(RecordResult::Owned(vec![]))
        }
    }

    pub(super) fn lookup_range<'a>(&'a self, key: &RangeKey) -> RangeLookupResult<'a> {
        match self.state.lookup_range(key) {
            Ok(rs) => RangeLookupResult::Some(RecordResult::References(rs.collect())),
            Err(misses) if self.partial() => RangeLookupResult::Missing(misses),
            _ => RangeLookupResult::Some(RecordResult::Owned(vec![])),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Bound;

    use super::*;
    use vec1::vec1;

    #[test]
    fn mark_filled_point() {
        let mut state = SingleState::new(&Index::new(IndexType::BTreeMap, vec![0]), true);
        state.mark_filled(KeyComparison::Equal(vec1![0.into()]));
        assert!(state.lookup(&KeyType::from(&[0.into()])).is_some())
    }

    #[test]
    fn mark_filled_range() {
        let mut state = SingleState::new(&Index::new(IndexType::BTreeMap, vec![0]), true);
        state.mark_filled(KeyComparison::Range((
            Bound::Included(vec1![0.into()]),
            Bound::Excluded(vec1![5.into()]),
        )));
        assert!(state.lookup(&KeyType::from(&[0.into()])).is_some());
        assert!(state
            .lookup_range(&RangeKey::from(&(vec1![0.into()]..vec1![5.into()])))
            .is_some());
    }

    mod evict_keys {
        use super::*;
        use vec1::vec1;

        #[test]
        fn equal() {
            let mut state = SingleState::new(&Index::new(IndexType::BTreeMap, vec![0]), true);
            let key = KeyComparison::Equal(vec1![0.into()]);
            state.mark_filled(key.clone());
            state.insert_row(vec![0.into(), 1.into()].into());
            state.evict_keys(&[key]);
            assert!(state.lookup(&KeyType::from(&[0.into()])).is_missing())
        }

        #[test]
        fn range() {
            let mut state = SingleState::new(&Index::new(IndexType::BTreeMap, vec![0]), true);
            let key =
                KeyComparison::from_range(&(vec1![DataType::from(0)]..vec1![DataType::from(10)]));
            state.mark_filled(key.clone());
            assert!(state
                .lookup_range(&RangeKey::from(
                    &(vec1![DataType::from(0)]..vec1![DataType::from(10)])
                ))
                .is_some());

            state.insert_row(vec![0.into(), 1.into()].into());
            state.evict_keys(&[key]);
            assert!(state.lookup(&KeyType::from(&[0.into()])).is_missing());
            assert!(state
                .lookup_range(&RangeKey::from(
                    &(vec1![DataType::from(0)]..vec1![DataType::from(10)])
                ))
                .is_missing())
        }
    }
}
