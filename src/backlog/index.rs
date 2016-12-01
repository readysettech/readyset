use shortcut;
use std::hash::Hash;
use fnv::FnvHashMap;

#[derive(Clone)]
pub struct FnvHashIndex<K: Eq + Hash> {
    num: usize,
    map: FnvHashMap<K, Vec<usize>>,
}

impl<K: Eq + Hash> FnvHashIndex<K> {
    /// Allocate a new `HashIndex`.
    pub fn new() -> FnvHashIndex<K> {
        FnvHashIndex {
            map: FnvHashMap::default(),
            num: 0,
        }
    }
}

impl<T: Eq + Hash> shortcut::EqualityIndex<T> for FnvHashIndex<T> {
    fn lookup<'a>(&'a self, key: &T) -> Box<Iterator<Item = usize> + 'a> {
        match self.map.get(key) {
            Some(ref v) => Box::new(v.iter().map(|row| *row)),
            None => Box::new(None.into_iter()),
        }
    }

    fn index(&mut self, key: T, row: usize) {
        self.map.entry(key).or_insert_with(Vec::new).push(row);
        self.num += 1;
    }

    fn undex(&mut self, key: &T, row: usize) {
        let mut empty = false;
        if let Some(mut l) = self.map.get_mut(key) {
            empty = {
                match l.iter().position(|&r| r == row) {
                    Some(i) => {
                        l.swap_remove(i);
                    }
                    None => unreachable!(),
                }
                l.is_empty()
            };
        }
        if empty {
            self.map.remove(key);
        }
    }

    fn estimate(&self) -> usize {
        let len = self.map.len();
        if len > 0 {
            self.num / self.map.len()
        } else {
            0
        }
    }
}

impl<T: Eq + Hash + 'static + Send + Sync> Into<shortcut::Index<T>> for FnvHashIndex<T> {
    fn into(self) -> shortcut::Index<T> {
        shortcut::Index::Equality(Box::new(self))
    }
}
