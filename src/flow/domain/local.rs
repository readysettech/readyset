use flow::prelude::*;
use std::ops::{Deref, Index, IndexMut};
use std::iter::FromIterator;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::rc::Rc;

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

    pub fn get(&self, addr: &LocalNodeIndex) -> Option<&T> {
        self.things.get(addr.id()).and_then(|v| v.as_ref())
    }

    pub fn get_mut(&mut self, addr: &LocalNodeIndex) -> Option<&mut T> {
        self.things.get_mut(addr.id()).and_then(|v| v.as_mut())
    }

    pub fn contains_key(&self, addr: &LocalNodeIndex) -> bool {
        self.things
            .get(addr.id())
            .map(|v| v.is_some())
            .unwrap_or(false)
    }

    pub fn remove(&mut self, addr: &LocalNodeIndex) -> Option<T> {
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

    pub fn iter<'a>(&'a self) -> Box<Iterator<Item = (LocalNodeIndex, &'a T)> + 'a> {
        Box::new(self.things.iter().enumerate().filter_map(|(i, t)| {
            t.as_ref()
                .map(|v| (unsafe { LocalNodeIndex::make(i as u32) }, v))
        }))
    }

    pub fn iter_mut<'a>(&'a mut self) -> Box<Iterator<Item = (LocalNodeIndex, &'a mut T)> + 'a> {
        Box::new(self.things.iter_mut().enumerate().filter_map(|(i, t)| {
            t.as_mut()
                .map(|v| (unsafe { LocalNodeIndex::make(i as u32) }, v))
        }))
    }

    pub fn values<'a>(&'a self) -> Box<Iterator<Item = &'a T> + 'a> {
        Box::new(self.things.iter().filter_map(|t| t.as_ref()))
    }

    pub fn len(&self) -> usize {
        self.n
    }
}

use std::fmt;
impl<T> fmt::Debug for Map<T>
where
    T: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_map().entries(self.iter()).finish()
    }
}

impl<'a, T> Index<&'a LocalNodeIndex> for Map<T> {
    type Output = T;
    fn index(&self, index: &LocalNodeIndex) -> &Self::Output {
        self.get(index).unwrap()
    }
}
impl<'a, T> IndexMut<&'a LocalNodeIndex> for Map<T> {
    fn index_mut(&mut self, index: &LocalNodeIndex) -> &mut Self::Output {
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

        Map { n: n, things: vs }
    }
}

impl<T: 'static> IntoIterator for Map<T> {
    type Item = (LocalNodeIndex, T);
    type IntoIter = Box<Iterator<Item = Self::Item>>;
    fn into_iter(self) -> Self::IntoIter {
        Box::new(self.things.into_iter().enumerate().filter_map(|(i, v)| {
            v.map(|v| (unsafe { LocalNodeIndex::make(i as u32) }, v))
        }))
    }
}

use std::collections::hash_map;
use fnv::FnvHashMap;
use std::hash::Hash;

pub struct Row<T>(Rc<T>);

unsafe impl<T> Send for Row<T> {}

impl<T> Deref for Row<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}

#[derive(Clone)]
pub enum KeyType<'a, T: 'a> {
    Single(&'a T),
    Double((T, T)),
    Tri((T, T, T)),
    Quad((T, T, T, T)),
}

enum KeyedState<T: Eq + Hash> {
    Single(FnvHashMap<T, Vec<Row<Vec<T>>>>),
    Double(FnvHashMap<(T, T), Vec<Row<Vec<T>>>>),
    Tri(FnvHashMap<(T, T, T), Vec<Row<Vec<T>>>>),
    Quad(FnvHashMap<(T, T, T, T), Vec<Row<Vec<T>>>>),
}

impl<'a, T: 'static + Eq + Hash + Clone> From<&'a [T]> for KeyType<'a, T> {
    fn from(other: &'a [T]) -> Self {
        match other.len() {
            0 => unreachable!(),
            1 => KeyType::Single(&other[0]),
            2 => KeyType::Double((other[0].clone(), other[1].clone())),
            3 => KeyType::Tri((other[0].clone(), other[1].clone(), other[2].clone())),
            4 => KeyType::Quad((
                other[0].clone(),
                other[1].clone(),
                other[2].clone(),
                other[3].clone(),
            )),
            _ => unimplemented!(),
        }
    }
}

impl<'a, T: 'static + Eq + Hash + Clone> From<&'a [&'a T]> for KeyType<'a, T> {
    fn from(other: &'a [&'a T]) -> Self {
        match other.len() {
            0 => unreachable!(),
            1 => KeyType::Single(other[0]),
            2 => KeyType::Double((other[0].clone(), other[1].clone())),
            3 => KeyType::Tri((other[0].clone(), other[1].clone(), other[2].clone())),
            4 => KeyType::Quad((
                other[0].clone(),
                other[1].clone(),
                other[2].clone(),
                other[3].clone(),
            )),
            _ => unimplemented!(),
        }
    }
}

impl<T: Eq + Hash> KeyedState<T> {
    pub fn is_empty(&self) -> bool {
        match *self {
            KeyedState::Single(ref m) => m.is_empty(),
            KeyedState::Double(ref m) => m.is_empty(),
            KeyedState::Tri(ref m) => m.is_empty(),
            KeyedState::Quad(ref m) => m.is_empty(),
        }
    }

    pub fn len(&self) -> usize {
        match *self {
            KeyedState::Single(ref m) => m.len(),
            KeyedState::Double(ref m) => m.len(),
            KeyedState::Tri(ref m) => m.len(),
            KeyedState::Quad(ref m) => m.len(),
        }
    }

    pub fn lookup<'a>(&'a self, key: &KeyType<T>) -> Option<&'a Vec<Row<Vec<T>>>> {
        match (self, key) {
            (&KeyedState::Single(ref m), &KeyType::Single(k)) => m.get(k),
            (&KeyedState::Double(ref m), &KeyType::Double(ref k)) => m.get(k),
            (&KeyedState::Tri(ref m), &KeyType::Tri(ref k)) => m.get(k),
            (&KeyedState::Quad(ref m), &KeyType::Quad(ref k)) => m.get(k),
            _ => unreachable!(),
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
            x => panic!("invalid compound key of length: {}", x),
        }
    }
}

pub enum LookupResult<'a, T: 'a> {
    Some(&'a [Row<Vec<T>>]),
    Missing,
}

struct SingleState<T: Hash + Eq + Clone + 'static> {
    key: Vec<usize>,
    state: KeyedState<T>,
    partial: Option<Vec<Tag>>,
}

pub struct State<T: Hash + Eq + Clone + 'static> {
    state: Vec<SingleState<T>>,
    by_tag: HashMap<Tag, usize>,
    rows: usize,
}

impl<T: Hash + Eq + Clone + 'static> Default for State<T> {
    fn default() -> Self {
        State {
            state: Vec::new(),
            by_tag: HashMap::new(),
            rows: 0,
        }
    }
}

impl<T: Hash + Eq + Clone + 'static> State<T> {
    /// Construct base materializations differently (potentially)
    pub fn base() -> Self {
        Self::default()
    }

    fn state_for(&self, cols: &[usize]) -> Option<usize> {
        self.state.iter().position(|s| &s.key[..] == cols)
    }

    pub fn add_key(&mut self, columns: &[usize], partial: Option<Vec<Tag>>) {
        if self.state_for(columns).is_some() {
            // already keyed
            return;
        }

        let is_partial = partial.is_some();
        if let Some(ref p) = partial {
            let i = self.state.len();
            for &tag in p {
                self.by_tag.insert(tag, i);
            }
        }

        self.state.push(SingleState {
            key: Vec::from(columns),
            state: columns.into(),
            partial: partial,
        });

        if !self.is_empty() {
            // we need to *construct* the index!
            if is_partial {
                // would require multi-index partial view support
                unimplemented!();
            }

            let (new, old) = self.state.split_last_mut().unwrap();
            let mut insert = move |rs: &Vec<Row<Vec<T>>>| for r in rs {
                State::insert_into(new, Row(r.0.clone()));
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
            }
        }
    }

    pub fn keys(&self) -> Vec<Vec<usize>> {
        self.state.iter().map(|s| &s.key).cloned().collect()
    }

    pub fn is_useful(&self) -> bool {
        !self.state.is_empty()
    }

    pub fn is_partial(&self) -> bool {
        self.state.iter().any(|s| s.partial.is_some())
    }

    /// Insert the given record into the given state.
    ///
    /// Returns false if a hole was encountered (and the record hence not inserted).
    fn insert_into(s: &mut SingleState<T>, r: Row<Vec<T>>) -> bool {
        match s.state {
            KeyedState::Single(ref mut map) => {
                // treat this specially to avoid the extra Vec
                debug_assert_eq!(s.key.len(), 1);
                // i *wish* we could use the entry API here, but it would mean an extra clone
                // in the common case of an entry already existing for the given key...
                if let Some(ref mut rs) = map.get_mut(&r[s.key[0]]) {
                    rs.push(r);
                    return true;
                } else if s.partial.is_some() {
                    // trying to insert a record into partial materialization hole!
                    return false;
                }
                map.insert(r[s.key[0]].clone(), vec![r]);
            }
            _ => match s.state {
                KeyedState::Double(ref mut map) => {
                    let key = (r[s.key[0]].clone(), r[s.key[1]].clone());
                    match map.entry(key) {
                        Entry::Occupied(mut rs) => rs.get_mut().push(r),
                        Entry::Vacant(..) if s.partial.is_some() => return false,
                        rs @ Entry::Vacant(..) => rs.or_insert_with(Vec::new).push(r),
                    }
                }
                KeyedState::Tri(ref mut map) => {
                    let key = (
                        r[s.key[0]].clone(),
                        r[s.key[1]].clone(),
                        r[s.key[2]].clone(),
                    );
                    match map.entry(key) {
                        Entry::Occupied(mut rs) => rs.get_mut().push(r),
                        Entry::Vacant(..) if s.partial.is_some() => return false,
                        rs @ Entry::Vacant(..) => rs.or_insert_with(Vec::new).push(r),
                    }
                }
                KeyedState::Quad(ref mut map) => {
                    let key = (
                        r[s.key[0]].clone(),
                        r[s.key[1]].clone(),
                        r[s.key[2]].clone(),
                        r[s.key[3]].clone(),
                    );
                    match map.entry(key) {
                        Entry::Occupied(mut rs) => rs.get_mut().push(r),
                        Entry::Vacant(..) if s.partial.is_some() => return false,
                        rs @ Entry::Vacant(..) => rs.or_insert_with(Vec::new).push(r),
                    }
                }
                KeyedState::Single(..) => unreachable!(),
            },
        }

        true
    }

    pub fn insert(&mut self, r: Vec<T>, partial_tag: Option<Tag>) -> bool {
        let r = Rc::new(r);

        if let Some(tag) = partial_tag {
            let i = *self.by_tag
                .get(&tag)
                .expect("got tagged insert for unknown tag");
            // FIXME: self.rows += ?
            State::insert_into(&mut self.state[i], Row(r))
        } else {
            let mut hit_any = true;
            self.rows = self.rows.saturating_add(1);
            for i in 0..self.state.len() {
                hit_any = State::insert_into(&mut self.state[i], Row(r.clone())) || hit_any;
            }
            hit_any
        }
    }

    pub fn remove(&mut self, r: &[T]) -> bool {
        let mut hit = false;
        let mut removed = false;
        let fix = |removed: &mut bool, rs: &mut Vec<Row<Vec<T>>>| {
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
                _ => {
                    match s.state {
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
                        KeyedState::Single(..) => unreachable!(),
                    }
                }
            }
        }

        if removed {
            self.rows = self.rows.saturating_sub(1);
        }

        hit
    }

    pub fn iter(&self) -> hash_map::Values<T, Vec<Row<Vec<T>>>> {
        for index in &self.state {
            if let KeyedState::Single(ref map) = index.state {
                if index.partial.is_some() {
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

    pub fn mark_filled(&mut self, key: Vec<T>, tag: &Tag) {
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
        };
        assert!(replaced.is_none());
    }

    pub fn mark_hole(&mut self, key: &[T], tag: &Tag) {
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
        };
        assert!(removed.is_some());
        assert!(removed.unwrap().is_empty());
    }

    pub fn lookup<'a>(&'a self, columns: &[usize], key: &KeyType<T>) -> LookupResult<'a, T> {
        debug_assert!(!self.state.is_empty(), "lookup on uninitialized index");
        let index = &self.state[self.state_for(columns)
                                    .expect("lookup on non-indexed column set")];
        if let Some(rs) = index.state.lookup(key) {
            LookupResult::Some(&rs[..])
        } else {
            if index.partial.is_some() {
                // partially materialized, so this is a hole (empty results would be vec![])
                LookupResult::Missing
            } else {
                LookupResult::Some(&[])
            }
        }
    }

    fn fix<'a>(rs: &'a Vec<Row<Vec<T>>>) -> impl Iterator<Item = Vec<T>> + 'a {
        rs.iter().map(|r| Vec::clone(&**r))
    }

    pub fn cloned_records(&self) -> Vec<Vec<T>> {
        match self.state[0].state {
            KeyedState::Single(ref map) => map.values().flat_map(State::fix).collect(),
            KeyedState::Double(ref map) => map.values().flat_map(State::fix).collect(),
            KeyedState::Tri(ref map) => map.values().flat_map(State::fix).collect(),
            KeyedState::Quad(ref map) => map.values().flat_map(State::fix).collect(),
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
            }
        }
    }
}

impl<'a, T: Eq + Hash + Clone + 'static> State<T> {
    fn unalias_for_state(&mut self) {
        let left = self.state.drain(..).last();
        if let Some(left) = left {
            self.state.push(left);
        }
    }
}

impl<'a, T: Eq + Hash + Clone + 'static> Drop for State<T> {
    fn drop(&mut self) {
        self.unalias_for_state();
        self.clear();
    }
}

impl<T: Hash + Eq + Clone + 'static> IntoIterator for State<T> {
    type Item = Vec<Vec<T>>;
    type IntoIter = Box<Iterator<Item = Self::Item>>;
    fn into_iter(mut self) -> Self::IntoIter {
        // we need to make sure that the records eventually get dropped, so we need to ensure there
        // is only one index left (which therefore owns the records), and then cast back to the
        // original boxes.
        self.unalias_for_state();
        let own = |rs: Vec<Row<Vec<T>>>| match rs.into_iter()
            .map(|r| Rc::try_unwrap(r.0))
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(rs) => rs,
            Err(_) => unreachable!("rc still not owned after unaliasing"),
        };
        self.state
            .drain(..)
            .last()
            .map(move |index| -> Self::IntoIter {
                match index.state {
                    KeyedState::Single(map) => Box::new(map.into_iter().map(move |(_, v)| own(v))),
                    KeyedState::Double(map) => Box::new(map.into_iter().map(move |(_, v)| own(v))),
                    KeyedState::Tri(map) => Box::new(map.into_iter().map(move |(_, v)| own(v))),
                    KeyedState::Quad(map) => Box::new(map.into_iter().map(move |(_, v)| own(v))),
                }
            })
            .unwrap()
    }
}
