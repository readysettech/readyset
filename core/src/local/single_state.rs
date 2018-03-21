use rand::{Rng, ThreadRng};
use local::keyed_state::KeyedState;
use local::Row;
use DataType;

pub struct SingleState {
    pub key: Vec<usize>,
    pub state: KeyedState,
    pub partial: bool,
}
impl SingleState {
    /// Inserts the given record, or returns false if a hole was encountered (and the record hence
    /// not inserted).
    pub fn insert(&mut self, r: Row) -> bool {
        use rahashmap::Entry;
        match self.state {
            KeyedState::Single(ref mut map) => {
                // treat this specially to avoid the extra Vec
                debug_assert_eq!(self.key.len(), 1);
                // i *wish* we could use the entry API here, but it would mean an extra clone
                // in the common case of an entry already existing for the given key...
                if let Some(ref mut rs) = map.get_mut(&r[self.key[0]]) {
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

        true
    }

    /// Evict `count` randomly selected keys from state and return them along with the number of
    /// bytes freed.
    pub fn evict_random_keys(
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
    pub fn evict_keys(&mut self, keys: &[Vec<DataType>]) -> u64 {
        keys.iter().map(|k| self.state.evict(k)).sum()
    }
}
