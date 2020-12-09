use super::mk_key::MakeKey;
use crate::prelude::*;
use crate::state::keyed_state::KeyedState;
use common::SizeOf;
use rand::prelude::*;
use std::rc::Rc;

pub(super) struct SingleState {
    key: Vec<usize>,
    state: KeyedState,
    partial: bool,
    rows: usize,
}

macro_rules! insert_row_match_impl {
    ($self:ident, $r:ident, $map:ident) => {{
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

macro_rules! remove_row_match_impl {
    ($self:ident, $r:ident, $do_remove:ident, $map:ident, $($hint:tt)*) => {{
        // TODO: can we avoid the Clone here?
        let key = MakeKey::from_row(&$self.key, $r);
        if let Some(ref mut rs) = $map.get_mut::<$($hint)*>(&key) {
            return $do_remove(&mut $self.rows, rs);
        }
    }};
}

impl SingleState {
    pub(super) fn new(columns: &[usize], partial: bool) -> Self {
        Self {
            key: Vec::from(columns),
            state: columns.into(),
            partial,
            rows: 0,
        }
    }

    /// Inserts the given record, or returns false if a hole was encountered (and the record hence
    /// not inserted).
    pub(super) fn insert_row(&mut self, r: Row) -> bool {
        use indexmap::map::Entry;
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

        match self.state {
            KeyedState::Single(ref mut map) => {
                if let Some(ref mut rs) = map.get_mut(&r[self.key[0]]) {
                    return do_remove(&mut self.rows, rs);
                }
            }
            KeyedState::Double(ref mut map) => {
                remove_row_match_impl!(self, r, do_remove, map, (DataType, _))
            }
            KeyedState::Tri(ref mut map) => {
                remove_row_match_impl!(self, r, do_remove, map, (DataType, _, _))
            }
            KeyedState::Quad(ref mut map) => {
                remove_row_match_impl!(self, r, do_remove, map, (DataType, _, _, _))
            }
            KeyedState::Quin(ref mut map) => {
                remove_row_match_impl!(self, r, do_remove, map, (DataType, _, _, _, _))
            }
            KeyedState::Sex(ref mut map) => {
                remove_row_match_impl!(self, r, do_remove, map, (DataType, _, _, _, _, _))
            }
        }
        None
    }

    pub(super) fn mark_filled(&mut self, key: Vec<DataType>) {
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

    pub(super) fn mark_hole(&mut self, key: &[DataType]) -> u64 {
        let removed = match self.state {
            KeyedState::Single(ref mut m) => m.swap_remove(&(key[0])),
            KeyedState::Double(ref mut m) => {
                m.swap_remove::<(DataType, _)>(&MakeKey::from_key(key))
            }
            KeyedState::Tri(ref mut m) => {
                m.swap_remove::<(DataType, _, _)>(&MakeKey::from_key(key))
            }
            KeyedState::Quad(ref mut m) => {
                m.swap_remove::<(DataType, _, _, _)>(&MakeKey::from_key(key))
            }
            KeyedState::Quin(ref mut m) => {
                m.swap_remove::<(DataType, _, _, _, _)>(&MakeKey::from_key(key))
            }
            KeyedState::Sex(ref mut m) => {
                m.swap_remove::<(DataType, _, _, _, _, _)>(&MakeKey::from_key(key))
            }
        };
        // mark_hole should only be called on keys we called mark_filled on
        removed
            .unwrap()
            .iter()
            .filter(|r| Rc::strong_count(&r.0) == 1)
            .map(SizeOf::deep_size_of)
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
    pub(super) fn evict_keys(&mut self, keys: &[Vec<DataType>]) -> u64 {
        keys.iter().map(|k| self.state.evict(k)).sum()
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
}
