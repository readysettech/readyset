use flow::prelude::*;
use std::ops::{Index, IndexMut};
use std::iter::FromIterator;
use std::collections::hash_map::Entry;

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

    pub fn iter<'a>(&'a self) -> Box<Iterator<Item = (NodeAddress, &'a T)> + 'a> {
        Box::new(self.things.iter().enumerate().filter_map(|(i, t)| {
                                                               t.as_ref().map(|v| {
                (unsafe { NodeAddress::make_local(i as u32) }, v)
            })
                                                           }))
    }

    pub fn iter_mut<'a>(&'a mut self) -> Box<Iterator<Item = (NodeAddress, &'a mut T)> + 'a> {
        Box::new(self.things.iter_mut().enumerate().filter_map(|(i, t)| {
                                                                   t.as_mut().map(|v| {
                (unsafe { NodeAddress::make_local(i as u32) }, v)
            })
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
    where T: fmt::Debug
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
    type Item = (NodeAddress, T);
    type IntoIter = Box<Iterator<Item = Self::Item>>;
    fn into_iter(self) -> Self::IntoIter {
        Box::new(self.things.into_iter().enumerate().filter_map(|(i, v)| {
                                                                    v.map(|v| {
                (unsafe { NodeAddress::make_local(i as u32) }, v)
            })
                                                                }))
    }
}

use std::collections::hash_map;
use fnv::FnvHashMap;
use std::hash::Hash;
use std::sync::Arc;

#[derive(Clone)]
pub enum KeyType<'a, T: 'a> {
    Single(&'a T),
    Double((T, T)),
    Tri((T, T, T)),
    Quad((T, T, T, T)),
}

#[derive(Clone)]
enum KeyedState<T: Eq + Hash> {
    Single(FnvHashMap<T, Vec<Arc<Vec<T>>>>),
    Double(FnvHashMap<(T, T), Vec<Arc<Vec<T>>>>),
    Tri(FnvHashMap<(T, T, T), Vec<Arc<Vec<T>>>>),
    Quad(FnvHashMap<(T, T, T, T), Vec<Arc<Vec<T>>>>),
}

impl<'a, T: 'static + Eq + Hash + Clone> From<&'a [T]> for KeyType<'a, T> {
    fn from(other: &'a [T]) -> Self {
        match other.len() {
            0 => unreachable!(),
            1 => KeyType::Single(&other[0]),
            2 => KeyType::Double((other[0].clone(), other[1].clone())),
            3 => KeyType::Tri((other[0].clone(), other[1].clone(), other[2].clone())),
            4 => {
                KeyType::Quad((other[0].clone(),
                               other[1].clone(),
                               other[2].clone(),
                               other[3].clone()))
            }
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
            4 => {
                KeyType::Quad((other[0].clone(),
                               other[1].clone(),
                               other[2].clone(),
                               other[3].clone()))
            }
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

    pub fn lookup<'a>(&'a self, key: &KeyType<T>) -> Option<&'a Vec<Arc<Vec<T>>>> {
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
    Some(&'a [Arc<Vec<T>>]),
    Missing,
}

#[derive(Clone)]
pub struct State<T: Hash + Eq + Clone> {
    state: Vec<(Vec<usize>, KeyedState<T>, bool)>,
    rows: usize,
}

impl<T: Hash + Eq + Clone> Default for State<T> {
    fn default() -> Self {
        State {
            state: Vec::new(),
            rows: 0,
        }
    }
}

impl<T: Hash + Eq + Clone> State<T> {
    /// Construct base materializations differently (potentially)
    pub fn base() -> Self {
        Self::default()
    }

    fn state_for(&self, cols: &[usize]) -> Option<usize> {
        self.state.iter().position(|s| &s.0[..] == cols)
    }

    pub fn add_key(&mut self, columns: &[usize], partial: bool) {
        if self.state_for(columns).is_some() {
            // already keyed
            return;
        }

        if !self.state.is_empty() && !self.state[0].1.is_empty() {
            // we'd need to *construct* the index!
            unimplemented!();
        }

        self.state
            .push((Vec::from(columns), columns.into(), partial));
    }

    pub fn keys(&self) -> Vec<Vec<usize>> {
        self.state.iter().map(|s| &s.0).cloned().collect()
    }

    pub fn is_useful(&self) -> bool {
        !self.state.is_empty()
    }

    pub fn is_partial(&self) -> bool {
        self.state.iter().any(|s| s.2)
    }

    pub fn insert(&mut self, r: Arc<Vec<T>>) {
        let mut rclones = Vec::with_capacity(self.state.len());
        rclones.extend((0..(self.state.len() - 1)).into_iter().map(|_| r.clone()));
        rclones.push(r);

        self.rows = self.rows.saturating_add(1);
        for s in &mut self.state {
            let r = rclones.swap_remove(0);

            match s.1 {
                KeyedState::Single(ref mut map) => {
                    // treat this specially to avoid the extra Vec
                    debug_assert_eq!(s.0.len(), 1);
                    // i *wish* we could use the entry API here, but it would mean an extra clone
                    // in the common case of an entry already existing for the given key...
                    if let Some(ref mut rs) = map.get_mut(&r[s.0[0]]) {
                        rs.push(r);
                        return;
                    } else if s.2 {
                        // trying to insert a record into partial materialization hole!
                        unimplemented!();
                    }
                    map.insert(r[s.0[0]].clone(), vec![r]);
                }
                _ => {
                    match s.1 {
                        KeyedState::Double(ref mut map) => {
                            let key = (r[s.0[0]].clone(), r[s.0[1]].clone());
                            match map.entry(key) {
                                Entry::Occupied(mut rs) => rs.get_mut().push(r),
                                Entry::Vacant(..) if s.2 => unimplemented!(),
                                rs @ Entry::Vacant(..) => rs.or_insert_with(Vec::new).push(r),
                            }
                        }
                        KeyedState::Tri(ref mut map) => {
                            let key = (r[s.0[0]].clone(), r[s.0[1]].clone(), r[s.0[2]].clone());
                            match map.entry(key) {
                                Entry::Occupied(mut rs) => rs.get_mut().push(r),
                                Entry::Vacant(..) if s.2 => unimplemented!(),
                                rs @ Entry::Vacant(..) => rs.or_insert_with(Vec::new).push(r),
                            }
                        }
                        KeyedState::Quad(ref mut map) => {
                            let key = (r[s.0[0]].clone(),
                                       r[s.0[1]].clone(),
                                       r[s.0[2]].clone(),
                                       r[s.0[3]].clone());
                            match map.entry(key) {
                                Entry::Occupied(mut rs) => rs.get_mut().push(r),
                                Entry::Vacant(..) if s.2 => unimplemented!(),
                                rs @ Entry::Vacant(..) => rs.or_insert_with(Vec::new).push(r),
                            }
                        }
                        KeyedState::Single(..) => unreachable!(),
                    }
                }
            }
        }
    }

    pub fn remove(&mut self, r: &[T]) {
        let mut removed = 0;
        for s in &mut self.state {
            removed = 0; // otherwise we'd count every removal multiple times
            let keep = |rsr: &Arc<Vec<T>>| if &rsr[..] == r {
                removed += 1;
                false
            } else {
                true
            };

            match s.1 {
                KeyedState::Single(ref mut map) => {
                    if let Some(ref mut rs) = map.get_mut(&r[s.0[0]]) {
                        rs.retain(keep);
                    }
                }
                _ => {
                    match s.1 {
                        KeyedState::Double(ref mut map) => {
                            // TODO: can we avoid the Clone here?
                            let key = (r[s.0[0]].clone(), r[s.0[1]].clone());
                            if let Some(ref mut rs) = map.get_mut(&key) {
                                rs.retain(keep);
                            }
                        }
                        KeyedState::Tri(ref mut map) => {
                            let key = (r[s.0[0]].clone(), r[s.0[1]].clone(), r[s.0[2]].clone());
                            if let Some(ref mut rs) = map.get_mut(&key) {
                                rs.retain(keep);
                            }
                        }
                        KeyedState::Quad(ref mut map) => {
                            let key = (r[s.0[0]].clone(),
                                       r[s.0[1]].clone(),
                                       r[s.0[2]].clone(),
                                       r[s.0[3]].clone());
                            if let Some(ref mut rs) = map.get_mut(&key) {
                                rs.retain(keep);
                            }
                        }
                        KeyedState::Single(..) => unreachable!(),
                    }
                }
            }
        }
        self.rows = self.rows.saturating_sub(removed);
    }

    pub fn iter(&self) -> hash_map::Values<T, Vec<Arc<Vec<T>>>> {
        for &(_, ref state, partial) in &self.state {
            if let KeyedState::Single(ref map) = *state {
                if partial {
                    unimplemented!();
                }
                return map.values();
            }
        }
        // TODO: allow iter without single key (breaks return type)
        unimplemented!();
    }

    pub fn is_empty(&self) -> bool {
        self.state.is_empty() || self.state[0].1.is_empty()
    }

    pub fn len(&self) -> usize {
        self.rows
    }

    pub fn nkeys(&self) -> usize {
        if self.state.is_empty() {
            0
        } else {
            self.state[0].1.len()
        }
    }

    pub fn mark_filled(&mut self, key: Vec<T>) {
        debug_assert!(!self.state.is_empty(), "filling uninitialized index");
        assert!(self.state.len() == 1,
                "partially materializing to multi-index materialization");
        let state = &mut self.state[0];
        let mut key = key.into_iter();
        let replaced = match state.1 {
            KeyedState::Single(ref mut map) => map.insert(key.next().unwrap(), Vec::new()),
            KeyedState::Double(ref mut map) => {
                map.insert((key.next().unwrap(), key.next().unwrap()), Vec::new())
            }
            KeyedState::Tri(ref mut map) => {
                map.insert((key.next().unwrap(), key.next().unwrap(), key.next().unwrap()),
                           Vec::new())
            }
            KeyedState::Quad(ref mut map) => {
                map.insert((key.next().unwrap(),
                            key.next().unwrap(),
                            key.next().unwrap(),
                            key.next().unwrap()),
                           Vec::new())
            }
        };
        assert!(replaced.is_none());
    }

    pub fn mark_hole(&mut self, key: &[T]) {
        debug_assert!(!self.state.is_empty(), "filling uninitialized index");
        assert!(self.state.len() == 1,
                "partially materializing to multi-index materialization");
        let state = &mut self.state[0];
        let removed = match state.1 {
            KeyedState::Single(ref mut map) => map.remove(&key[0]),
            KeyedState::Double(ref mut map) => map.remove(&(key[0].clone(), key[1].clone())),
            KeyedState::Tri(ref mut map) => {
                map.remove(&(key[0].clone(), key[1].clone(), key[2].clone()))
            }
            KeyedState::Quad(ref mut map) => {
                map.remove(&(key[0].clone(), key[1].clone(), key[2].clone(), key[3].clone()))
            }
        };
        assert!(removed.is_some());
        assert!(removed.unwrap().is_empty());
    }

    pub fn hits_hole(&self, r: &[T]) -> Option<(&[usize])> {
        for s in &self.state {
            if !s.2 {
                continue;
            }

            match s.1 {
                KeyedState::Single(ref map) => {
                    if !map.contains_key(&r[s.0[0]]) {
                        return Some(&s.0[..]);
                    }
                }
                KeyedState::Double(ref map) => {
                    let key = (r[s.0[0]].clone(), r[s.0[1]].clone());
                    if !map.contains_key(&key) {
                        return Some(&s.0[..]);
                    }
                }
                KeyedState::Tri(ref map) => {
                    let key = (r[s.0[0]].clone(), r[s.0[1]].clone(), r[s.0[2]].clone());
                    if !map.contains_key(&key) {
                        return Some(&s.0[..]);
                    }
                }
                KeyedState::Quad(ref map) => {
                    let key = (r[s.0[0]].clone(),
                               r[s.0[1]].clone(),
                               r[s.0[2]].clone(),
                               r[s.0[3]].clone());
                    if !map.contains_key(&key) {
                        return Some(&s.0[..]);
                    }
                }
            }
        }

        None
    }

    pub fn lookup<'a>(&'a self, columns: &[usize], key: &KeyType<T>) -> LookupResult<'a, T> {
        debug_assert!(!self.state.is_empty(), "lookup on uninitialized index");
        let state = &self.state[self.state_for(columns)
            .expect("lookup on non-indexed column set")];
        if let Some(rs) = state.1.lookup(key) {
            LookupResult::Some(&rs[..])
        } else {
            if state.2 {
                // partially materialized, so this is a hole (empty results would be vec![])
                LookupResult::Missing
            } else {
                LookupResult::Some(&[])
            }
        }
    }

    pub fn clear(&mut self) {
        self.rows = 0;
        for s in &mut self.state {
            match s.1 {
                KeyedState::Single(ref mut map) => map.clear(),
                KeyedState::Double(ref mut map) => map.clear(),
                KeyedState::Tri(ref mut map) => map.clear(),
                KeyedState::Quad(ref mut map) => map.clear(),
            }
        }
    }
}

impl<T: Hash + Eq + Clone + 'static> IntoIterator for State<T> {
    type Item = Vec<Arc<Vec<T>>>;
    type IntoIter = Box<Iterator<Item = Self::Item>>;
    fn into_iter(self) -> Self::IntoIter {
        self.state
            .into_iter()
            .find(|&(_, _, partial)| !partial)
            .map(|(_, state, _)| -> Self::IntoIter {
                     match state {
                         KeyedState::Single(map) => Box::new(map.into_iter().map(|(_, v)| v)),
                         KeyedState::Double(map) => Box::new(map.into_iter().map(|(_, v)| v)),
                         KeyedState::Tri(map) => Box::new(map.into_iter().map(|(_, v)| v)),
                         KeyedState::Quad(map) => Box::new(map.into_iter().map(|(_, v)| v)),
                     }
                 })
            .unwrap()
    }
}
