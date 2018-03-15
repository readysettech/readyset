use ::*;
use std::hash::Hash;
use fnv::FnvBuildHasher;
use rahashmap::HashMap as RaHashMap;

type FnvHashMap<K, V> = RaHashMap<K, V, FnvBuildHasher>;

pub enum KeyedState<T: Eq + Hash> {
    Single(FnvHashMap<T, Vec<Row<Vec<T>>>>),
    Double(FnvHashMap<(T, T), Vec<Row<Vec<T>>>>),
    Tri(FnvHashMap<(T, T, T), Vec<Row<Vec<T>>>>),
    Quad(FnvHashMap<(T, T, T, T), Vec<Row<Vec<T>>>>),
    Quin(FnvHashMap<(T, T, T, T, T), Vec<Row<Vec<T>>>>),
    Sex(FnvHashMap<(T, T, T, T, T, T), Vec<Row<Vec<T>>>>),
}

impl<T: Eq + Hash> KeyedState<T> {
    pub fn is_empty(&self) -> bool {
        match *self {
            KeyedState::Single(ref m) => m.is_empty(),
            KeyedState::Double(ref m) => m.is_empty(),
            KeyedState::Tri(ref m) => m.is_empty(),
            KeyedState::Quad(ref m) => m.is_empty(),
            KeyedState::Quin(ref m) => m.is_empty(),
            KeyedState::Sex(ref m) => m.is_empty(),
        }
    }

    pub fn len(&self) -> usize {
        match *self {
            KeyedState::Single(ref m) => m.len(),
            KeyedState::Double(ref m) => m.len(),
            KeyedState::Tri(ref m) => m.len(),
            KeyedState::Quad(ref m) => m.len(),
            KeyedState::Quin(ref m) => m.len(),
            KeyedState::Sex(ref m) => m.len(),
        }
    }

    pub fn lookup<'a>(&'a self, key: &KeyType<T>) -> Option<&'a Vec<Row<Vec<T>>>> {
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

    pub fn remove_at_index(&mut self, index: usize) -> Option<Vec<Row<Vec<T>>>> {
        match *self {
            KeyedState::Single(ref mut m) => m.remove_at_index(index).map(|(_, rs)| rs),
            KeyedState::Double(ref mut m) => m.remove_at_index(index).map(|(_, rs)| rs),
            KeyedState::Tri(ref mut m) => m.remove_at_index(index).map(|(_, rs)| rs),
            KeyedState::Quad(ref mut m) => m.remove_at_index(index).map(|(_, rs)| rs),
            KeyedState::Quin(ref mut m) => m.remove_at_index(index).map(|(_, rs)| rs),
            KeyedState::Sex(ref mut m) => m.remove_at_index(index).map(|(_, rs)| rs),
        }
    }
}

impl<'a, T: Eq + Hash> Into<KeyedState<T>> for &'a [usize] {
    fn into(self) -> KeyedState<T> {
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
