#![allow(missing_docs)]
use crate::internal::*;
use std::iter::FromIterator;
use std::ops::{Index, IndexMut};

#[derive(Serialize, Deserialize)]
pub struct Map<T> {
    n: usize,
    things: Vec<Option<T>>,
}

impl<T> Default for Map<T> {
    fn default() -> Self {
        Map {
            n: 0,
            things: Vec::default(),
        }
    }
}

impl<T: Clone> Clone for Map<T> {
    fn clone(&self) -> Self {
        Map {
            n: self.n,
            things: self.things.clone(),
        }
    }
}

pub enum Entry<'a, V> {
    Vacant(VacantEntry<'a, V>),
    Occupied(OccupiedEntry<'a, V>),
}

pub struct VacantEntry<'a, V> {
    map: &'a mut Map<V>,
    index: LocalNodeIndex,
}

pub struct OccupiedEntry<'a, V> {
    map: &'a mut Map<V>,
    index: LocalNodeIndex,
}

impl<'a, V> Entry<'a, V> {
    pub fn or_insert(self, default: V) -> &'a mut V {
        match self {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => entry.insert(default),
        }
    }
    pub fn or_default(self) -> &'a mut V
    where
        V: Default,
    {
        match self {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => entry.insert(V::default()),
        }
    }
    pub fn or_insert_with<F: FnOnce() -> V>(self, default: F) -> &'a mut V {
        match self {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => entry.insert(default()),
        }
    }
}

impl<'a, V> VacantEntry<'a, V> {
    pub fn insert(self, value: V) -> &'a mut V {
        self.map.insert(self.index, value);
        &mut self.map[self.index]
    }
}

impl<'a, V> OccupiedEntry<'a, V> {
    pub fn get(&self) -> &V {
        &self.map[self.index]
    }
    pub fn get_mut(&mut self) -> &mut V {
        &mut self.map[self.index]
    }
    pub fn into_mut(self) -> &'a mut V {
        &mut self.map[self.index]
    }
    pub fn insert(&mut self, value: V) -> V {
        self.map.insert(self.index, value).unwrap()
    }
    pub fn remove(self) -> V {
        self.map.remove(self.index).unwrap()
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
        if old.is_none() {
            self.n += 1;
        }
        old
    }

    pub fn get(&self, addr: LocalNodeIndex) -> Option<&T> {
        self.things.get(addr.id()).and_then(Option::as_ref)
    }

    pub fn get_mut(&mut self, addr: LocalNodeIndex) -> Option<&mut T> {
        self.things.get_mut(addr.id()).and_then(Option::as_mut)
    }

    pub fn contains_key(&self, addr: LocalNodeIndex) -> bool {
        self.things
            .get(addr.id())
            .map(Option::is_some)
            .unwrap_or(false)
    }

    pub fn remove(&mut self, addr: LocalNodeIndex) -> Option<T> {
        let i = addr.id();
        if i >= self.things.len() {
            return None;
        }

        let ret = self.things[i].take();
        if ret.is_some() {
            self.n -= 1;
        }
        ret
    }

    pub fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = (LocalNodeIndex, &'a T)> + 'a> {
        Box::new(self.things.iter().enumerate().filter_map(|(i, t)| {
            t.as_ref()
                .map(|v| (unsafe { LocalNodeIndex::make(i as u32) }, v))
        }))
    }

    pub fn iter_mut<'a>(
        &'a mut self,
    ) -> Box<dyn Iterator<Item = (LocalNodeIndex, &'a mut T)> + 'a> {
        Box::new(self.things.iter_mut().enumerate().filter_map(|(i, t)| {
            t.as_mut()
                .map(|v| (unsafe { LocalNodeIndex::make(i as u32) }, v))
        }))
    }

    pub fn values<'a>(&'a self) -> Box<dyn Iterator<Item = &'a T> + 'a> {
        Box::new(self.things.iter().filter_map(Option::as_ref))
    }

    pub fn len(&self) -> usize {
        self.n
    }

    pub fn is_empty(&self) -> bool {
        self.n == 0
    }

    pub fn entry(&mut self, key: LocalNodeIndex) -> Entry<'_, T> {
        if self.contains_key(key) {
            Entry::Occupied(OccupiedEntry {
                map: self,
                index: key,
            })
        } else {
            Entry::Vacant(VacantEntry {
                map: self,
                index: key,
            })
        }
    }
}

use std::fmt;
impl<T> fmt::Debug for Map<T>
where
    T: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map().entries(self.iter()).finish()
    }
}

impl<T> Index<LocalNodeIndex> for Map<T> {
    type Output = T;
    fn index(&self, index: LocalNodeIndex) -> &Self::Output {
        self.get(index).unwrap()
    }
}
impl<T> IndexMut<LocalNodeIndex> for Map<T> {
    fn index_mut(&mut self, index: LocalNodeIndex) -> &mut Self::Output {
        self.get_mut(index).unwrap()
    }
}

impl<T> FromIterator<(LocalNodeIndex, T)> for Map<T> {
    fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = (LocalNodeIndex, T)>,
    {
        use std::collections::BTreeMap;

        // we've got to be a bit careful here, as the nodes may come in any order
        // we therefore sort them first
        let sorted = BTreeMap::from_iter(iter.into_iter().map(|(ni, v)| (ni.id(), v)));

        // no entries -- fine
        if sorted.is_empty() {
            return Map::default();
        }

        let n = sorted.len();
        let end = sorted.keys().last().unwrap() + 1;
        let mut vs = Vec::with_capacity(end);
        for (i, v) in sorted {
            for _ in vs.len()..i {
                vs.push(None);
            }
            vs.push(Some(v));
        }

        Map { n, things: vs }
    }
}

impl<T: 'static> IntoIterator for Map<T> {
    type Item = (LocalNodeIndex, T);
    type IntoIter = Box<dyn Iterator<Item = Self::Item>>;
    fn into_iter(self) -> Self::IntoIter {
        Box::new(
            self.things
                .into_iter()
                .enumerate()
                .filter_map(|(i, v)| v.map(|v| (unsafe { LocalNodeIndex::make(i as u32) }, v))),
        )
    }
}
