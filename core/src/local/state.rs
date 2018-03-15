use ::*;
use std::collections::HashMap;
use std::rc::Rc;

use local::keyed_state::KeyedState;
use local::single_state::SingleState;

pub struct State {
    state: Vec<SingleState>,
    by_tag: HashMap<Tag, usize>,
    rows: usize,
}

impl Default for State {
    fn default() -> Self {
        State {
            state: Vec::new(),
            by_tag: HashMap::new(),
            rows: 0,
        }
    }
}

impl State {
    /// Construct base materializations differently (potentially)
    pub fn base() -> Self {
        Self::default()
    }

    /// Returns the index in `self.state` of the index keyed on `cols`, or None if no such index
    /// exists.
    fn state_for(&self, cols: &[usize]) -> Option<usize> {
        self.state.iter().position(|s| &s.key[..] == cols)
    }

    pub fn add_key(&mut self, columns: &[usize], partial: Option<Vec<Tag>>) {
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

        self.state.push(SingleState {
            key: Vec::from(columns),
            state: columns.into(),
            partial: partial.is_some(),
        });

        if !self.is_empty() && partial.is_none() {
            // we need to *construct* the index!
            let (new, old) = self.state.split_last_mut().unwrap();
            let mut insert = move |rs: &Vec<Row>| {
                for r in rs {
                    new.insert(Row(r.0.clone()));
                }
            };
            match old[0].state {
                KeyedState::Single(ref map) => for rs in map.values() {
                    insert(rs);
                },
                KeyedState::Double(ref map) => for rs in map.values() {
                    insert(rs);
                },
                KeyedState::Tri(ref map) => for rs in map.values() {
                    insert(rs);
                },
                KeyedState::Quad(ref map) => for rs in map.values() {
                    insert(rs);
                },
                KeyedState::Quin(ref map) => for rs in map.values() {
                    insert(rs);
                },
                KeyedState::Sex(ref map) => for rs in map.values() {
                    insert(rs);
                },
            }
        }
    }

    /// Returns a Vec of all keys that currently exist on this materialization.
    pub fn keys(&self) -> Vec<Vec<usize>> {
        self.state.iter().map(|s| &s.key).cloned().collect()
    }

    /// Returns whether this state is currently keyed on anything. If not, then it cannot store any
    /// infromation and is thus "not useful".
    pub fn is_useful(&self) -> bool {
        !self.state.is_empty()
    }

    pub fn is_partial(&self) -> bool {
        self.state.iter().any(|s| s.partial)
    }

    pub fn insert(&mut self, r: Vec<DataType>, partial_tag: Option<Tag>) -> bool {
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
            // FIXME: self.rows += ?
            self.state[i].insert(Row(r))
        } else {
            let mut hit_any = true;
            self.rows = self.rows.saturating_add(1);
            for i in 0..self.state.len() {
                hit_any = self.state[i].insert(Row(r.clone())) || hit_any;
            }
            hit_any
        }
    }

    pub fn remove(&mut self, r: &[DataType]) -> bool {
        let mut hit = false;
        let mut removed = false;
        let fix = |removed: &mut bool, rs: &mut Vec<Row>| {
            // rustfmt
            if let Some(i) = rs.iter().position(|rsr| &rsr[..] == r) {
                rs.swap_remove(i);
                *removed = true;
            }
        };

        for s in &mut self.state {
            match s.state {
                KeyedState::Single(ref mut map) => {
                    if let Some(ref mut rs) = map.get_mut(&r[s.key[0]]) {
                        fix(&mut removed, rs);
                        hit = true;
                    }
                }
                KeyedState::Double(ref mut map) => {
                    // TODO: can we avoid the Clone here?
                    let key = (r[s.key[0]].clone(), r[s.key[1]].clone());
                    if let Some(ref mut rs) = map.get_mut(&key) {
                        fix(&mut removed, rs);
                        hit = true;
                    }
                }
                KeyedState::Tri(ref mut map) => {
                    let key = (
                        r[s.key[0]].clone(),
                        r[s.key[1]].clone(),
                        r[s.key[2]].clone(),
                    );
                    if let Some(ref mut rs) = map.get_mut(&key) {
                        fix(&mut removed, rs);
                        hit = true;
                    }
                }
                KeyedState::Quad(ref mut map) => {
                    let key = (
                        r[s.key[0]].clone(),
                        r[s.key[1]].clone(),
                        r[s.key[2]].clone(),
                        r[s.key[3]].clone(),
                    );
                    if let Some(ref mut rs) = map.get_mut(&key) {
                        fix(&mut removed, rs);
                        hit = true;
                    }
                }
                KeyedState::Quin(ref mut map) => {
                    let key = (
                        r[s.key[0]].clone(),
                        r[s.key[1]].clone(),
                        r[s.key[2]].clone(),
                        r[s.key[3]].clone(),
                        r[s.key[4]].clone(),
                    );
                    if let Some(ref mut rs) = map.get_mut(&key) {
                        fix(&mut removed, rs);
                        hit = true;
                    }
                }
                KeyedState::Sex(ref mut map) => {
                    let key = (
                        r[s.key[0]].clone(),
                        r[s.key[1]].clone(),
                        r[s.key[2]].clone(),
                        r[s.key[3]].clone(),
                        r[s.key[4]].clone(),
                        r[s.key[5]].clone(),
                    );
                    if let Some(ref mut rs) = map.get_mut(&key) {
                        fix(&mut removed, rs);
                        hit = true;
                    }
                }
            }
        }

        if removed {
            self.rows = self.rows.saturating_sub(1);
        }

        hit
    }

    pub fn iter(&self) -> rahashmap::Values<DataType, Vec<Row>> {
        for index in &self.state {
            if let KeyedState::Single(ref map) = index.state {
                if index.partial {
                    unimplemented!();
                }
                return map.values();
            }
        }
        // TODO: allow iter without single key (breaks return type)
        unimplemented!();
    }

    pub fn is_empty(&self) -> bool {
        self.state.is_empty() || self.state[0].state.is_empty()
    }

    pub fn len(&self) -> usize {
        self.rows
    }

    pub fn nkeys(&self) -> usize {
        if self.state.is_empty() {
            0
        } else {
            self.state[0].state.len()
        }
    }

    pub fn mark_filled(&mut self, key: Vec<DataType>, tag: &Tag) {
        debug_assert!(!self.state.is_empty(), "filling uninitialized index");
        let i = self.by_tag[tag];
        let index = &mut self.state[i];
        let mut key = key.into_iter();
        let replaced = match index.state {
            KeyedState::Single(ref mut map) => map.insert(key.next().unwrap(), Vec::new()),
            KeyedState::Double(ref mut map) => {
                map.insert((key.next().unwrap(), key.next().unwrap()), Vec::new())
            }
            KeyedState::Tri(ref mut map) => map.insert(
                (
                    key.next().unwrap(),
                    key.next().unwrap(),
                    key.next().unwrap(),
                ),
                Vec::new(),
            ),
            KeyedState::Quad(ref mut map) => map.insert(
                (
                    key.next().unwrap(),
                    key.next().unwrap(),
                    key.next().unwrap(),
                    key.next().unwrap(),
                ),
                Vec::new(),
            ),
            KeyedState::Quin(ref mut map) => map.insert(
                (
                    key.next().unwrap(),
                    key.next().unwrap(),
                    key.next().unwrap(),
                    key.next().unwrap(),
                    key.next().unwrap(),
                ),
                Vec::new(),
            ),
            KeyedState::Sex(ref mut map) => map.insert(
                (
                    key.next().unwrap(),
                    key.next().unwrap(),
                    key.next().unwrap(),
                    key.next().unwrap(),
                    key.next().unwrap(),
                    key.next().unwrap(),
                ),
                Vec::new(),
            ),
        };
        assert!(replaced.is_none());
    }

    pub fn mark_hole(&mut self, key: &[DataType], tag: &Tag) {
        debug_assert!(!self.state.is_empty(), "filling uninitialized index");
        let i = self.by_tag[tag];
        let index = &mut self.state[i];
        let removed = match index.state {
            KeyedState::Single(ref mut map) => map.remove(&key[0]),
            KeyedState::Double(ref mut map) => map.remove(&(key[0].clone(), key[1].clone())),
            KeyedState::Tri(ref mut map) => {
                map.remove(&(key[0].clone(), key[1].clone(), key[2].clone()))
            }
            KeyedState::Quad(ref mut map) => map.remove(&(
                key[0].clone(),
                key[1].clone(),
                key[2].clone(),
                key[3].clone(),
            )),
            KeyedState::Quin(ref mut map) => map.remove(&(
                key[0].clone(),
                key[1].clone(),
                key[2].clone(),
                key[3].clone(),
                key[4].clone(),
            )),
            KeyedState::Sex(ref mut map) => map.remove(&(
                key[0].clone(),
                key[1].clone(),
                key[2].clone(),
                key[3].clone(),
                key[4].clone(),
                key[5].clone(),
            )),
        };
        // mark_hole should only be called on keys we called mark_filled on
        assert!(removed.is_some());
    }

    pub fn lookup<'a>(&'a self, columns: &[usize], key: &KeyType) -> LookupResult<'a> {
        debug_assert!(!self.state.is_empty(), "lookup on uninitialized index");
        let index = &self.state[self.state_for(columns)
                                    .expect("lookup on non-indexed column set")];
        if let Some(rs) = index.state.lookup(key) {
            LookupResult::Some(&rs[..])
        } else {
            if index.partial {
                // partially materialized, so this is a hole (empty results would be vec![])
                LookupResult::Missing
            } else {
                LookupResult::Some(&[])
            }
        }
    }

    /// Return a copy of all records. Panics if the state is only partially materialized.
    pub fn cloned_records(&self) -> Vec<Vec<DataType>> {
        fn fix<'a>(rs: &'a Vec<Row>) -> impl Iterator<Item = Vec<DataType>> + 'a {
            rs.iter().map(|r| Vec::clone(&**r))
        }

        match self.state[0].state {
            KeyedState::Single(ref map) => map.values().flat_map(fix).collect(),
            KeyedState::Double(ref map) => map.values().flat_map(fix).collect(),
            KeyedState::Tri(ref map) => map.values().flat_map(fix).collect(),
            KeyedState::Quad(ref map) => map.values().flat_map(fix).collect(),
            KeyedState::Quin(ref map) => map.values().flat_map(fix).collect(),
            KeyedState::Sex(ref map) => map.values().flat_map(fix).collect(),
        }
    }

    pub fn clear(&mut self) {
        self.rows = 0;
        for s in &mut self.state {
            match s.state {
                KeyedState::Single(ref mut map) => map.clear(),
                KeyedState::Double(ref mut map) => map.clear(),
                KeyedState::Tri(ref mut map) => map.clear(),
                KeyedState::Quad(ref mut map) => map.clear(),
                KeyedState::Quin(ref mut map) => map.clear(),
                KeyedState::Sex(ref mut map) => map.clear(),
            }
        }
    }

    fn unalias_for_state(&mut self) {
        let left = self.state.drain(..).last();
        if let Some(left) = left {
            self.state.push(left);
        }
    }
}

impl<'a> Drop for State {
    fn drop(&mut self) {
        self.unalias_for_state();
        self.clear();
    }
}
