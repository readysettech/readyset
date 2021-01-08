use super::mk_key::MakeKey;
use super::RangeLookupResult;
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
    pub(super) fn new(columns: &[usize], partial: bool) -> Self {
        let mut state: KeyedState = columns.into();
        if !partial {
            state.insert_range((Bound::Unbounded, Bound::Unbounded))
        }
        Self {
            key: Vec::from(columns),
            state,
            partial,
            rows: 0,
        }
    }

    /// Inserts the given record, or returns false if a hole was encountered (and the record hence
    /// not inserted).
    pub(super) fn insert_row(&mut self, r: Row) -> bool {
        macro_rules! insert_row_match_impl {
            ($self:ident, $r:ident, $map:ident) => {{
                use super::partial_map::Entry;
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
            KeyedState::Single(ref mut map) => {
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
            KeyedState::Double(ref mut map) => insert_row_match_impl!(self, r, map),
            KeyedState::Tri(ref mut map) => insert_row_match_impl!(self, r, map),
            KeyedState::Quad(ref mut map) => insert_row_match_impl!(self, r, map),
            KeyedState::Quin(ref mut map) => insert_row_match_impl!(self, r, map),
            KeyedState::Sex(ref mut map) => insert_row_match_impl!(self, r, map),
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
            ($self:ident, $r:ident, $map:ident) => {{
                let key = MakeKey::from_row(&$self.key, $r);
                if let Some(ref mut rs) = $map.get_mut(&key) {
                    return do_remove(&mut $self.rows, rs);
                }
            }};
        }

        match self.state {
            KeyedState::Single(ref mut map) => {
                if let Some(ref mut rs) = map.get_mut(&r[self.key[0]]) {
                    return do_remove(&mut self.rows, rs);
                }
            }
            KeyedState::Double(ref mut map) => {
                remove_row_match_impl!(self, r, map)
            }
            KeyedState::Tri(ref mut map) => {
                remove_row_match_impl!(self, r, map)
            }
            KeyedState::Quad(ref mut map) => {
                remove_row_match_impl!(self, r, map)
            }
            KeyedState::Quin(ref mut map) => {
                remove_row_match_impl!(self, r, map)
            }
            KeyedState::Sex(ref mut map) => {
                remove_row_match_impl!(self, r, map)
            }
        }
        None
    }

    fn mark_point_filled(&mut self, key: Vec1<DataType>) {
        let mut key = key.into_iter();
        let replaced = match self.state {
            KeyedState::Single(ref mut map) => map.insert(key.next().unwrap(), Rows::default()),
            KeyedState::Double(ref mut map) => {
                map.insert((key.next().unwrap(), key.next().unwrap()), Rows::default())
            }
            KeyedState::Tri(ref mut map) => map.insert(
                (
                    key.next().unwrap(),
                    key.next().unwrap(),
                    key.next().unwrap(),
                ),
                Rows::default(),
            ),
            KeyedState::Quad(ref mut map) => map.insert(
                (
                    key.next().unwrap(),
                    key.next().unwrap(),
                    key.next().unwrap(),
                    key.next().unwrap(),
                ),
                Rows::default(),
            ),
            KeyedState::Quin(ref mut map) => map.insert(
                (
                    key.next().unwrap(),
                    key.next().unwrap(),
                    key.next().unwrap(),
                    key.next().unwrap(),
                    key.next().unwrap(),
                ),
                Rows::default(),
            ),
            KeyedState::Sex(ref mut map) => map.insert(
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
                KeyedState::Single(ref mut m) => {
                    Box::new(m.remove(&(key[0])).into_iter().flatten())
                }
                KeyedState::Double(ref mut m) => {
                    Box::new(m.remove(&MakeKey::from_key(key)).into_iter().flatten())
                }
                KeyedState::Tri(ref mut m) => {
                    Box::new(m.remove(&MakeKey::from_key(key)).into_iter().flatten())
                }
                KeyedState::Quad(ref mut m) => {
                    Box::new(m.remove(&MakeKey::from_key(key)).into_iter().flatten())
                }
                KeyedState::Quin(ref mut m) => {
                    Box::new(m.remove(&MakeKey::from_key(key)).into_iter().flatten())
                }
                KeyedState::Sex(ref mut m) => {
                    Box::new(m.remove(&MakeKey::from_key(key)).into_iter().flatten())
                }
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
                    KeyedState::Single(ref mut m) => remove_range!(m, range, DataType),
                    KeyedState::Double(ref mut m) => remove_range!(m, range, (DataType, _)),
                    KeyedState::Tri(ref mut m) => remove_range!(m, range, (DataType, _, _)),
                    KeyedState::Quad(ref mut m) => remove_range!(m, range, (DataType, _, _, _)),
                    KeyedState::Quin(ref mut m) => remove_range!(m, range, (DataType, _, _, _, _)),
                    KeyedState::Sex(ref mut m) => {
                        remove_range!(m, range, (DataType, _, _, _, _, _))
                    }
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
            KeyedState::Single(ref mut map) => map.clear(),
            KeyedState::Double(ref mut map) => map.clear(),
            KeyedState::Tri(ref mut map) => map.clear(),
            KeyedState::Quad(ref mut map) => map.clear(),
            KeyedState::Quin(ref mut map) => map.clear(),
            KeyedState::Sex(ref mut map) => map.clear(),
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
            KeyedState::Single(ref map) => Box::new(map.values()),
            KeyedState::Double(ref map) => Box::new(map.values()),
            KeyedState::Tri(ref map) => Box::new(map.values()),
            KeyedState::Quad(ref map) => Box::new(map.values()),
            KeyedState::Quin(ref map) => Box::new(map.values()),
            KeyedState::Sex(ref map) => Box::new(map.values()),
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
        let mut state = SingleState::new(&[0], true);
        state.mark_filled(KeyComparison::Equal(vec1![0.into()]));
        assert!(state.lookup(&KeyType::from(&[0.into()])).is_some())
    }

    #[test]
    fn mark_filled_range() {
        let mut state = SingleState::new(&[0], true);
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
            let mut state = SingleState::new(&[0], true);
            let key = KeyComparison::Equal(vec1![0.into()]);
            state.mark_filled(key.clone());
            state.insert_row(vec![0.into(), 1.into()].into());
            state.evict_keys(&[key]);
            assert!(state.lookup(&KeyType::from(&[0.into()])).is_missing())
        }

        #[test]
        fn range() {
            let mut state = SingleState::new(&[0], true);
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
