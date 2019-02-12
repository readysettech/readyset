use fnv::FnvBuildHasher;
use rahashmap::HashMap as RaHashMap;
use std::rc::Rc;

use common::SizeOf;
use prelude::*;

type FnvHashMap<K, V> = RaHashMap<K, V, FnvBuildHasher>;

#[allow(clippy::type_complexity)]
pub(super) enum KeyedState {
    Single(FnvHashMap<DataType, Vec<Row>>),
    Double(FnvHashMap<(DataType, DataType), Vec<Row>>),
    Tri(FnvHashMap<(DataType, DataType, DataType), Vec<Row>>),
    Quad(FnvHashMap<(DataType, DataType, DataType, DataType), Vec<Row>>),
    Quin(FnvHashMap<(DataType, DataType, DataType, DataType, DataType), Vec<Row>>),
    Sex(FnvHashMap<(DataType, DataType, DataType, DataType, DataType, DataType), Vec<Row>>),
}

impl KeyedState {
    pub(super) fn lookup<'a>(&'a self, key: &KeyType) -> Option<&'a Vec<Row>> {
        match (self, key) {
            (&KeyedState::Single(ref m), &KeyType::Single(k)) => m.get(k),
            (&KeyedState::Double(ref m), &KeyType::Double(ref k)) => m.get(k),
            (&KeyedState::Tri(ref m), &KeyType::Tri(ref k)) => m.get(k),
            (&KeyedState::Quad(ref m), &KeyType::Quad(ref k)) => m.get(k),
            (&KeyedState::Quin(ref m), &KeyType::Quin(ref k)) => m.get(k),
            (&KeyedState::Sex(ref m), &KeyType::Sex(ref k)) => m.get(k),
            _ => unreachable!(),
        }
    }

    /// Remove all rows for the first key at or after `index`, returning that key along with the
    /// number of bytes freed. Returns None if already empty.
    pub(super) fn evict_at_index(&mut self, index: usize) -> Option<(u64, Vec<DataType>)> {
        let (rs, key) = match *self {
            KeyedState::Single(ref mut m) => m.remove_at_index(index).map(|(k, rs)| (rs, vec![k])),
            KeyedState::Double(ref mut m) => {
                m.remove_at_index(index).map(|(k, rs)| (rs, vec![k.0, k.1]))
            }
            KeyedState::Tri(ref mut m) => m
                .remove_at_index(index)
                .map(|(k, rs)| (rs, vec![k.0, k.1, k.2])),
            KeyedState::Quad(ref mut m) => m
                .remove_at_index(index)
                .map(|(k, rs)| (rs, vec![k.0, k.1, k.2, k.3])),
            KeyedState::Quin(ref mut m) => m
                .remove_at_index(index)
                .map(|(k, rs)| (rs, vec![k.0, k.1, k.2, k.3, k.4])),
            KeyedState::Sex(ref mut m) => m
                .remove_at_index(index)
                .map(|(k, rs)| (rs, vec![k.0, k.1, k.2, k.3, k.4, k.5])),
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
            KeyedState::Double(ref mut m) => m.remove(&(key[0].clone(), key[1].clone())),
            KeyedState::Tri(ref mut m) => {
                m.remove(&(key[0].clone(), key[1].clone(), key[2].clone()))
            }
            KeyedState::Quad(ref mut m) => m.remove(&(
                key[0].clone(),
                key[1].clone(),
                key[2].clone(),
                key[3].clone(),
            )),
            KeyedState::Quin(ref mut m) => m.remove(&(
                key[0].clone(),
                key[1].clone(),
                key[2].clone(),
                key[3].clone(),
                key[4].clone(),
            )),
            KeyedState::Sex(ref mut m) => m.remove(&(
                key[0].clone(),
                key[1].clone(),
                key[2].clone(),
                key[3].clone(),
                key[4].clone(),
                key[5].clone(),
            )),
        }
        .map(|rows| {
            rows.iter()
                .filter(|r| Rc::strong_count(&r.0) == 1)
                .map(SizeOf::deep_size_of)
                .sum()
        })
        .unwrap_or(0)
    }
}

impl<'a> Into<KeyedState> for &'a [usize] {
    fn into(self) -> KeyedState {
        match self.len() {
            0 => unreachable!(),
            1 => KeyedState::Single(FnvHashMap::default()),
            2 => KeyedState::Double(FnvHashMap::default()),
            3 => KeyedState::Tri(FnvHashMap::default()),
            4 => KeyedState::Quad(FnvHashMap::default()),
            5 => KeyedState::Quin(FnvHashMap::default()),
            6 => KeyedState::Sex(FnvHashMap::default()),
            x => panic!("invalid compound key of length: {}", x),
        }
    }
}
