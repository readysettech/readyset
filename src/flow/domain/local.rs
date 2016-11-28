use flow::prelude::*;
use std::collections::HashMap;
use std::ops::Index;
use std::iter::FromIterator;

pub struct Map<T> {
    things: HashMap<NodeAddress, T>,
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

    pub fn insert(&mut self, addr: NodeAddress, value: T) -> Option<T> {
        self.things.insert(addr, value)
    }

    pub fn get(&self, addr: &NodeAddress) -> Option<&T> {
        self.things.get(addr)
    }

    pub fn get_mut(&mut self, addr: &NodeAddress) -> Option<&mut T> {
        self.things.get_mut(addr)
    }

    pub fn contains_key(&self, addr: &NodeAddress) -> bool {
        self.things.contains_key(addr)
    }
}

impl<'a, T> Index<&'a NodeAddress> for Map<T> {
    type Output = T;
    fn index(&self, index: &NodeAddress) -> &Self::Output {
        &self.things[index]
    }
}

impl<T> FromIterator<(NodeAddress, T)> for Map<T> {
    fn from_iter<I>(iter: I) -> Self
        where I: IntoIterator<Item = (NodeAddress, T)>
    {
        Map { things: HashMap::from_iter(iter) }
    }
}
