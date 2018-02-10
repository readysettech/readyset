use rand::{self, Rng};
use std::collections::HashMap;
use std::rc::Rc;

use ::*;
use data::SizeOf;
use local::single_state::SingleState;

pub enum State {
    InMemory(MemoryState),
}

impl State {
    pub fn default() -> Self {
        State::InMemory(MemoryState::default())
    }

    pub fn base() -> Self {
        State::InMemory(MemoryState::default())
    }

    /// Add an index keyed by the given columns and replayed to by the given partial tags.
    pub fn add_key(&mut self, columns: &[usize], partial: Option<Vec<Tag>>) {
        match *self {
            State::InMemory(ref mut memory_state) => memory_state.add_key(columns, partial),
            _ => unreachable!(),
        }
    }

    pub fn keys(&self) -> Vec<Vec<usize>> {
        match *self {
            State::InMemory(ref memory_state) => memory_state.keys(),
            _ => unreachable!(),
        }
    }

    pub fn is_useful(&self) -> bool {
        match *self {
            State::InMemory(ref memory_state) => memory_state.is_useful(),
            _ => unreachable!(),
        }
    }

    pub fn is_partial(&self) -> bool {
        match *self {
            State::InMemory(ref memory_state) => memory_state.is_partial(),
            _ => unreachable!(),
        }
    }

    pub fn insert(&mut self, r: Vec<DataType>, partial_tag: Option<Tag>) -> bool {
        match *self {
            State::InMemory(ref mut memory_state) => memory_state.insert(r, partial_tag),
            _ => unreachable!(),
        }
    }

    pub fn remove(&mut self, r: &[DataType]) -> bool {
        match *self {
            State::InMemory(ref mut memory_state) => memory_state.remove(r),
            _ => unreachable!(),
        }
    }

    pub fn mark_hole(&mut self, key: &[DataType], tag: &Tag) {
        match *self {
            State::InMemory(ref mut memory_state) => memory_state.mark_hole(key, tag),
            _ => unreachable!(),
        }
    }

    pub fn mark_filled(&mut self, key: Vec<DataType>, tag: &Tag) {
        match *self {
            State::InMemory(ref mut memory_state) => memory_state.mark_filled(key, tag),
            _ => unreachable!(),
        }
    }

    pub fn lookup<'a>(&'a self, columns: &[usize], key: &KeyType) -> LookupResult<'a> {
        match *self {
            State::InMemory(ref memory_state) => memory_state.lookup(columns, key),
            _ => unreachable!(),
        }
    }

    pub fn rows(&self) -> usize {
        match *self {
            State::InMemory(ref memory_state) => memory_state.rows(),
            _ => unreachable!(),
        }
    }

    pub fn cloned_records(&self) -> Vec<Vec<DataType>> {
        match *self {
            State::InMemory(ref memory_state) => memory_state.cloned_records(),
            _ => unreachable!(),
        }
    }

    pub fn clear(&mut self) {
        match *self {
            State::InMemory(ref mut memory_state) => memory_state.clear(),
            _ => unreachable!(),
        }
    }

    pub fn evict_random_keys(&mut self, count: usize) -> (&[usize], Vec<Vec<DataType>>, u64) {
        match *self {
            State::InMemory(ref mut memory_state) => memory_state.evict_random_keys(count),
            _ => unreachable!(),
        }
    }

    /// Evict the listed keys from the materialization targeted by `tag`.
    pub fn evict_keys(&mut self, tag: &Tag, keys: &[Vec<DataType>]) -> (&[usize], u64) {
        match *self {
            State::InMemory(ref mut memory_state) => memory_state.evict_keys(tag, keys),
            _ => unreachable!(),
        }
    }
}

#[derive(Default)]
pub struct MemoryState {
    state: Vec<SingleState>,
    by_tag: HashMap<Tag, usize>,
    mem_size: u64,
}

impl MemoryState {
    /// Returns the index in `self.state` of the index keyed on `cols`, or None if no such index
    /// exists.
    fn state_for(&self, cols: &[usize]) -> Option<usize> {
        self.state.iter().position(|s| s.key() == cols)
    }

    fn add_key(&mut self, columns: &[usize], partial: Option<Vec<Tag>>) {
        let (i, exists) = if let Some(i) = self.state_for(columns) {
            // already keyed by this key; just adding tags
            (i, true)
        } else {
            // will eventually be assigned
            (self.state.len(), false)
        };

        if let Some(ref p) = partial {
            for &tag in p {
                self.by_tag.insert(tag, i);
            }
        }

        if exists {
            return;
        }

        self.state
            .push(SingleState::new(columns, partial.is_some()));

        if !self.state.is_empty() && partial.is_none() {
            // we need to *construct* the index!
            let (new, old) = self.state.split_last_mut().unwrap();

            if !old.is_empty() {
                assert!(!old[0].partial());
                for rs in old[0].values() {
                    for r in rs {
                        new.insert_row(Row(r.0.clone()));
                    }
                }
            }
        }
    }

    /// Returns a Vec of all keys that currently exist on this materialization.
    fn keys(&self) -> Vec<Vec<usize>> {
        self.state.iter().map(|s| s.key().to_vec()).collect()
    }

    /// Returns whether this state is currently keyed on anything. If not, then it cannot store any
    /// infromation and is thus "not useful".
    fn is_useful(&self) -> bool {
        !self.state.is_empty()
    }

    fn is_partial(&self) -> bool {
        self.state.iter().any(|s| s.partial())
    }

    fn insert(&mut self, r: Vec<DataType>, partial_tag: Option<Tag>) -> bool {
        let r = Rc::new(r);

        if let Some(tag) = partial_tag {
            let i = match self.by_tag.get(&tag) {
                Some(i) => *i,
                None => {
                    // got tagged insert for unknown tag. this will happen if a node on an old
                    // replay path is now materialized. must return true to avoid any records
                    // (which are destined for a downstream materialization) from being pruned.
                    return true;
                }
            };
            self.mem_size += r.deep_size_of();
            self.state[i].insert_row(Row(r))
        } else {
            let mut hit_any = false;
            for i in 0..self.state.len() {
                hit_any |= self.state[i].insert_row(Row(r.clone()));
            }
            if hit_any {
                self.mem_size += r.deep_size_of();
            }
            hit_any
        }
    }

    fn remove(&mut self, r: &[DataType]) -> bool {
        let mut hit = false;
        for s in &mut self.state {
            if let Some(row) = s.remove_row(r, &mut hit) {
                if Rc::strong_count(&row.0) == 1 {
                    self.mem_size = self.mem_size.checked_sub(row.deep_size_of()).unwrap();
                }
            }
        }

        hit
    }

    fn rows(&self) -> usize {
        self.state.iter().map(|s| s.rows()).sum()
    }

    fn mark_filled(&mut self, key: Vec<DataType>, tag: &Tag) {
        debug_assert!(!self.state.is_empty(), "filling uninitialized index");
        let index = self.by_tag[tag];
        self.state[index].mark_filled(key);
    }

    fn mark_hole(&mut self, key: &[DataType], tag: &Tag) {
        debug_assert!(!self.state.is_empty(), "filling uninitialized index");
        let index = self.by_tag[tag];
        self.state[index].mark_hole(key);
    }

    fn lookup<'a>(&'a self, columns: &[usize], key: &KeyType) -> LookupResult<'a> {
        debug_assert!(!self.state.is_empty(), "lookup on uninitialized index");
        let index = self.state_for(columns)
            .expect("lookup on non-indexed column set");
        self.state[index].lookup(key)
    }

    /// Return a copy of all records. Panics if the state is only partially materialized.
    fn cloned_records(&self) -> Vec<Vec<DataType>> {
        fn fix<'a>(rs: &'a Vec<Row>) -> impl Iterator<Item = Vec<DataType>> + 'a {
            rs.iter().map(|r| Vec::clone(&**r))
        }

        assert!(!self.state[0].partial());
        self.state[0].values().flat_map(fix).collect()
    }

    fn clear(&mut self) {
        for s in &mut self.state {
            s.clear();
        }
        self.mem_size = 0;
    }

    /// Evict `count` randomly selected keys, returning key colunms of the index chosen to evict
    /// from along with the keys evicted and the number of bytes evicted.
    fn evict_random_keys(&mut self, count: usize) -> (&[usize], Vec<Vec<DataType>>, u64) {
        let mut rng = rand::thread_rng();
        let index = rng.gen_range(0, self.state.len());
        let (bytes_freed, keys) = self.state[index].evict_random_keys(count, &mut rng);
        self.mem_size = self.mem_size.saturating_sub(bytes_freed);
        (self.state[index].key(), keys, bytes_freed)
    }

    /// Evict the listed keys from the materialization targeted by `tag`, returning the key columns
    /// of the index that was evicted from and the number of bytes evicted.
    fn evict_keys(&mut self, tag: &Tag, keys: &[Vec<DataType>]) -> (&[usize], u64) {
        let index = self.by_tag[tag];
        let bytes = self.state[index].evict_keys(keys);
        self.mem_size = self.mem_size.saturating_sub(bytes);
        (self.state[index].key(), bytes)
    }
}

impl SizeOf for State {
    fn size_of(&self) -> u64 {
        use std::mem::size_of;

        size_of::<Self>() as u64
    }

    fn deep_size_of(&self) -> u64 {
        match *self {
            State::InMemory(ref memory_state) => memory_state.mem_size,
            _ => unreachable!(),
        }
    }
}
