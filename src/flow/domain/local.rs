use flow::prelude::*;
use std::collections::HashMap;
use std::ops::Index;
use std::iter::FromIterator;

pub struct Map<T> {
    things: HashMap<LocalNodeIndex, T>,
}

impl<T> Default for Map<T> {
    fn default() -> Self {
        Map { things: HashMap::default() }
    }
}

impl<T> Map<T> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, addr: LocalNodeIndex, value: T) -> Option<T> {
        self.things.insert(addr, value)
    }

    pub fn get(&self, addr: &LocalNodeIndex) -> Option<&T> {
        self.things.get(addr)
    }

    pub fn get_mut(&mut self, addr: &LocalNodeIndex) -> Option<&mut T> {
        self.things.get_mut(addr)
    }

    pub fn contains_key(&self, addr: &LocalNodeIndex) -> bool {
        self.things.contains_key(addr)
    }
}

impl<'a, T> Index<&'a LocalNodeIndex> for Map<T> {
    type Output = T;
    fn index(&self, index: &LocalNodeIndex) -> &Self::Output {
        &self.things[index]
    }
}

impl<T> FromIterator<(LocalNodeIndex, T)> for Map<T> {
    fn from_iter<I>(iter: I) -> Self
        where I: IntoIterator<Item = (LocalNodeIndex, T)>
    {
        Map { things: HashMap::from_iter(iter) }
    }
}
