use ahash::RandomState;
use indexmap::IndexMap;
use std::rc::Rc;

use super::mk_key::MakeKey;
use crate::prelude::*;
use common::SizeOf;

type HashMap<K, V> = IndexMap<K, V, RandomState>;

#[allow(clippy::type_complexity)]
pub(super) enum KeyedState {
    Single(HashMap<DataType, Rows>),
    Double(HashMap<(DataType, DataType), Rows>),
    Tri(HashMap<(DataType, DataType, DataType), Rows>),
    Quad(HashMap<(DataType, DataType, DataType, DataType), Rows>),
    Quin(HashMap<(DataType, DataType, DataType, DataType, DataType), Rows>),
    Sex(HashMap<(DataType, DataType, DataType, DataType, DataType, DataType), Rows>),
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
            _ => unreachable!(),
        }
    }

    /// Remove all rows for a randomly chosen key seeded by `seed`, returning that key along with
    /// the number of bytes freed. Returns `None` if map is empty.
    pub(super) fn evict_with_seed(&mut self, seed: usize) -> Option<(u64, Vec<DataType>)> {
        let (rs, key) = match *self {
            KeyedState::Single(ref mut m) if !m.is_empty() => {
                let index = seed % m.len();
                m.swap_remove_index(index).map(|(k, rs)| (rs, vec![k]))
            }
            KeyedState::Double(ref mut m) if !m.is_empty() => {
                let index = seed % m.len();
                m.swap_remove_index(index)
                    .map(|(k, rs)| (rs, vec![k.0, k.1]))
            }
            KeyedState::Tri(ref mut m) if !m.is_empty() => {
                let index = seed % m.len();
                m.swap_remove_index(index)
                    .map(|(k, rs)| (rs, vec![k.0, k.1, k.2]))
            }
            KeyedState::Quad(ref mut m) if !m.is_empty() => {
                let index = seed % m.len();
                m.swap_remove_index(index)
                    .map(|(k, rs)| (rs, vec![k.0, k.1, k.2, k.3]))
            }
            KeyedState::Quin(ref mut m) if !m.is_empty() => {
                let index = seed % m.len();
                m.swap_remove_index(index)
                    .map(|(k, rs)| (rs, vec![k.0, k.1, k.2, k.3, k.4]))
            }
            KeyedState::Sex(ref mut m) if !m.is_empty() => {
                let index = seed % m.len();
                m.swap_remove_index(index)
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
            1 => KeyedState::Single(HashMap::default()),
            2 => KeyedState::Double(HashMap::default()),
            3 => KeyedState::Tri(HashMap::default()),
            4 => KeyedState::Quad(HashMap::default()),
            5 => KeyedState::Quin(HashMap::default()),
            6 => KeyedState::Sex(HashMap::default()),
            x => panic!("invalid compound key of length: {}", x),
        }
    }
}
