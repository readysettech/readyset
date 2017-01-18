use flow::prelude::*;
use std::ops::Index;
use std::iter::FromIterator;

pub struct Map<T> {
    things: Vec<Option<T>>,
}

impl<T> Default for Map<T> {
    fn default() -> Self {
        Map { things: Vec::default() }
    }
}

impl<T> Map<T> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, addr: LocalNodeIndex, value: T) -> Option<T> {
        let i = addr.id();

        if i >= self.things.len() {
            let diff = i - self.things.len() + 1;
            self.things.reserve(diff);
            for _ in 0..diff {
                self.things.push(None);
            }
        }

        let old = self.things[i].take();
        self.things[i] = Some(value);
        old
    }

    pub fn get(&self, addr: &LocalNodeIndex) -> Option<&T> {
        self.things.get(addr.id()).and_then(|v| v.as_ref())
    }

    pub fn get_mut(&mut self, addr: &LocalNodeIndex) -> Option<&mut T> {
        self.things.get_mut(addr.id()).and_then(|v| v.as_mut())
    }

    pub fn contains_key(&self, addr: &LocalNodeIndex) -> bool {
        self.things.get(addr.id()).map(|v| v.is_some()).unwrap_or(false)
    }

    pub fn remove(&mut self, addr: &LocalNodeIndex) -> Option<T> {
        let i = addr.id();
        if i >= self.things.len() {
            return None;
        }

        self.things[i].take()
    }

    pub fn iter<'a>(&'a self) -> Box<Iterator<Item = &'a T> + 'a> {
        Box::new(self.things.iter().filter_map(|t| t.as_ref()))
    }
}

impl<'a, T> Index<&'a LocalNodeIndex> for Map<T> {
    type Output = T;
    fn index(&self, index: &LocalNodeIndex) -> &Self::Output {
        self.get(index).unwrap()
    }
}

impl<T> FromIterator<(LocalNodeIndex, T)> for Map<T> {
    fn from_iter<I>(iter: I) -> Self
        where I: IntoIterator<Item = (LocalNodeIndex, T)>
    {
        use std::collections::BTreeMap;

        // we've got to be a bit careful here, as the nodes may come in any order
        // we therefore sort them first
        let sorted = BTreeMap::from_iter(iter.into_iter().map(|(ni, v)| (ni.id(), v)));

        // no entries -- fine
        if sorted.is_empty() {
            return Map::default();
        }

        let end = sorted.keys().last().unwrap() + 1;
        let mut vs = Vec::with_capacity(end);
        for (i, v) in sorted {
            for _ in vs.len()..i {
                vs.push(None);
            }
            vs.push(Some(v));
        }

        Map { things: vs }
    }
}

use std::collections::hash_map;
use fnv::FnvHashMap;
use std::hash::Hash;
use std::sync::Arc;

#[derive(Clone)]
pub struct State<T: Hash + Eq + Clone> {
    pkey: usize,
    state: FnvHashMap<T, Vec<Arc<Vec<T>>>>,
}

impl<T: Hash + Eq + Clone> Default for State<T> {
    fn default() -> Self {
        State {
            pkey: usize::max_value(),
            state: FnvHashMap::default(),
        }
    }
}

impl<T: Hash + Eq + Clone> State<T> {
    pub fn set_pkey(&mut self, key: usize) {
        if self.pkey != usize::max_value() && self.pkey != key {
            unreachable!("asked to index {} when already indexing {}", key, self.pkey);
        }
        self.pkey = key;
    }

    pub fn get_pkey(&self) -> usize {
        self.pkey
    }

    pub fn is_useful(&self) -> bool {
        self.pkey != usize::max_value()
    }

    pub fn insert(&mut self, r: Arc<Vec<T>>) {
        let k = self.pkey;
        // i *wish* we could use the entry API here, but it would mean an extra clone in the common
        // case of an entry already existing for the given key...
        if let Some(ref mut rs) = self.state.get_mut(&r[k]) {
            rs.push(r);
            return;
        }
        self.state.insert(r[k].clone(), vec![r]);
    }

    pub fn remove(&mut self, r: &[T]) {
        let k = self.pkey;
        if let Some(ref mut rs) = self.state.get_mut(&r[k]) {
            rs.retain(|rsr| &rsr[..] != r);
        }
    }

    pub fn iter(&self) -> hash_map::Values<T, Vec<Arc<Vec<T>>>> {
        self.state.values()
    }

    pub fn lookup(&self, key: usize, value: &T) -> &[Arc<Vec<T>>] {
        debug_assert_ne!(self.pkey,
                         usize::max_value(),
                         "lookup on uninitialized index");
        debug_assert_eq!(key, self.pkey);
        if let Some(rs) = self.state.get(value) {
            rs
        } else {
            &[]
        }
    }
}
