use rand::{Rng, ThreadRng};
use std::rc::Rc;

use common::SizeOf;
use prelude::*;
use state::keyed_state::KeyedState;

pub(super) struct SingleState {
    key: Vec<usize>,
    state: KeyedState,
    partial: bool,
    rows: usize,
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
        use rahashmap::Entry;
        match self.state {
            KeyedState::Single(ref mut map) => {
                // treat this specially to avoid the extra Vec
                debug_assert_eq!(self.key.len(), 1);
                // i *wish* we could use the entry API here, but it would mean an extra clone
                // in the common case of an entry already existing for the given key...
                if let Some(ref mut rs) = map.get_mut(&r[self.key[0]]) {
                    self.rows += 1;
                    rs.push(r);
                    return true;
                } else if self.partial {
                    // trying to insert a record into partial materialization hole!
                    return false;
                }
                map.insert(r[self.key[0]].clone(), vec![r]);
            }
            KeyedState::Double(ref mut map) => {
                let key = (r[self.key[0]].clone(), r[self.key[1]].clone());
                match map.entry(key) {
                    Entry::Occupied(mut rs) => rs.get_mut().push(r),
                    Entry::Vacant(..) if self.partial => return false,
                    rs @ Entry::Vacant(..) => rs.or_default().push(r),
                }
            }
            KeyedState::Tri(ref mut map) => {
                let key = (
                    r[self.key[0]].clone(),
                    r[self.key[1]].clone(),
                    r[self.key[2]].clone(),
                );
                match map.entry(key) {
                    Entry::Occupied(mut rs) => rs.get_mut().push(r),
                    Entry::Vacant(..) if self.partial => return false,
                    rs @ Entry::Vacant(..) => rs.or_default().push(r),
                }
            }
            KeyedState::Quad(ref mut map) => {
                let key = (
                    r[self.key[0]].clone(),
                    r[self.key[1]].clone(),
                    r[self.key[2]].clone(),
                    r[self.key[3]].clone(),
                );
                match map.entry(key) {
                    Entry::Occupied(mut rs) => rs.get_mut().push(r),
                    Entry::Vacant(..) if self.partial => return false,
                    rs @ Entry::Vacant(..) => rs.or_default().push(r),
                }
            }
            KeyedState::Quin(ref mut map) => {
                let key = (
                    r[self.key[0]].clone(),
                    r[self.key[1]].clone(),
                    r[self.key[2]].clone(),
                    r[self.key[3]].clone(),
                    r[self.key[4]].clone(),
                );
                match map.entry(key) {
                    Entry::Occupied(mut rs) => rs.get_mut().push(r),
                    Entry::Vacant(..) if self.partial => return false,
                    rs @ Entry::Vacant(..) => rs.or_default().push(r),
                }
            }
            KeyedState::Sex(ref mut map) => {
                let key = (
                    r[self.key[0]].clone(),
                    r[self.key[1]].clone(),
                    r[self.key[2]].clone(),
                    r[self.key[3]].clone(),
                    r[self.key[4]].clone(),
                    r[self.key[5]].clone(),
                );
                match map.entry(key) {
                    Entry::Occupied(mut rs) => rs.get_mut().push(r),
                    Entry::Vacant(..) if self.partial => return false,
                    rs @ Entry::Vacant(..) => rs.or_default().push(r),
                }
            }
        }

        self.rows += 1;
        true
    }

    /// Attempt to remove row `r`.
    pub(super) fn remove_row(&mut self, r: &[DataType], hit: &mut bool) -> Option<Row> {
        let mut do_remove = |self_rows: &mut usize, rs: &mut Vec<Row>| -> Option<Row> {
            *hit = true;
            let rm = if rs.len() == 1 {
                // it *should* be impossible to get a negative for a record that we don't have
                debug_assert_eq!(r, &rs[0][..]);
                Some(rs.swap_remove(0))
            } else if let Some(i) = rs.iter().position(|rsr| &rsr[..] == r) {
                Some(rs.swap_remove(i))
            } else {
                None
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
                // TODO: can we avoid the Clone here?
                let key = (r[self.key[0]].clone(), r[self.key[1]].clone());
                if let Some(ref mut rs) = map.get_mut(&key) {
                    return do_remove(&mut self.rows, rs);
                }
            }
            KeyedState::Tri(ref mut map) => {
                let key = (
                    r[self.key[0]].clone(),
                    r[self.key[1]].clone(),
                    r[self.key[2]].clone(),
                );
                if let Some(ref mut rs) = map.get_mut(&key) {
                    return do_remove(&mut self.rows, rs);
                }
            }
            KeyedState::Quad(ref mut map) => {
                let key = (
                    r[self.key[0]].clone(),
                    r[self.key[1]].clone(),
                    r[self.key[2]].clone(),
                    r[self.key[3]].clone(),
                );
                if let Some(ref mut rs) = map.get_mut(&key) {
                    return do_remove(&mut self.rows, rs);
                }
            }
            KeyedState::Quin(ref mut map) => {
                let key = (
                    r[self.key[0]].clone(),
                    r[self.key[1]].clone(),
                    r[self.key[2]].clone(),
                    r[self.key[3]].clone(),
                    r[self.key[4]].clone(),
                );
                if let Some(ref mut rs) = map.get_mut(&key) {
                    return do_remove(&mut self.rows, rs);
                }
            }
            KeyedState::Sex(ref mut map) => {
                let key = (
                    r[self.key[0]].clone(),
                    r[self.key[1]].clone(),
                    r[self.key[2]].clone(),
                    r[self.key[3]].clone(),
                    r[self.key[4]].clone(),
                    r[self.key[5]].clone(),
                );
                if let Some(ref mut rs) = map.get_mut(&key) {
                    return do_remove(&mut self.rows, rs);
                }
            }
        }
        None
    }

    pub(super) fn mark_filled(&mut self, key: Vec<DataType>) {
        let mut key = key.into_iter();
        let replaced = match self.state {
            KeyedState::Single(ref mut map) => map.insert(key.next().unwrap(), Vec::new()),
            KeyedState::Double(ref mut map) => {
                map.insert((key.next().unwrap(), key.next().unwrap()), Vec::new())
            }
            KeyedState::Tri(ref mut map) => map.insert(
                (
                    key.next().unwrap(),
                    key.next().unwrap(),
                    key.next().unwrap(),
                ),
                Vec::new(),
            ),
            KeyedState::Quad(ref mut map) => map.insert(
                (
                    key.next().unwrap(),
                    key.next().unwrap(),
                    key.next().unwrap(),
                    key.next().unwrap(),
                ),
                Vec::new(),
            ),
            KeyedState::Quin(ref mut map) => map.insert(
                (
                    key.next().unwrap(),
                    key.next().unwrap(),
                    key.next().unwrap(),
                    key.next().unwrap(),
                    key.next().unwrap(),
                ),
                Vec::new(),
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
                Vec::new(),
            ),
        };
        assert!(replaced.is_none());
    }

    pub(super) fn mark_hole(&mut self, key: &[DataType]) -> u64 {
        let removed = match self.state {
            KeyedState::Single(ref mut map) => map.remove(&key[0]),
            KeyedState::Double(ref mut map) => map.remove(&(key[0].clone(), key[1].clone())),
            KeyedState::Tri(ref mut map) => {
                map.remove(&(key[0].clone(), key[1].clone(), key[2].clone()))
            }
            KeyedState::Quad(ref mut map) => map.remove(&(
                key[0].clone(),
                key[1].clone(),
                key[2].clone(),
                key[3].clone(),
            )),
            KeyedState::Quin(ref mut map) => map.remove(&(
                key[0].clone(),
                key[1].clone(),
                key[2].clone(),
                key[3].clone(),
                key[4].clone(),
            )),
            KeyedState::Sex(ref mut map) => map.remove(&(
                key[0].clone(),
                key[1].clone(),
                key[2].clone(),
                key[3].clone(),
                key[4].clone(),
                key[5].clone(),
            )),
        };
        // mark_hole should only be called on keys we called mark_filled on
        removed
            .unwrap()
            .iter()
            .filter(|r| Rc::strong_count(&r.0) == 1)
            .map(SizeOf::deep_size_of)
            .sum()
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
            if let Some((n, key)) = self.state.evict_at_index(rng.gen()) {
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

    pub(super) fn values<'a>(&'a self) -> Box<Iterator<Item = &'a Vec<Row>> + 'a> {
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
    pub(super) fn lookup<'a>(&'a self, key: &KeyType) -> LookupResult<'a> {
        if let Some(rs) = self.state.lookup(key) {
            LookupResult::Some(RecordResult::Borrowed(&rs[..]))
        } else if self.partial() {
            // partially materialized, so this is a hole (empty results would be vec![])
            LookupResult::Missing
        } else {
            LookupResult::Some(RecordResult::Owned(vec![]))
        }
    }
}
