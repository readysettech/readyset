use std::collections::BTreeMap;
use std::rc::Rc;
use tuple::Map;

use super::mk_key::MakeKey;
use super::Misses;
use crate::prelude::*;
use common::SizeOf;
use noria::util::BoundFunctor;

#[allow(clippy::type_complexity)]
pub(super) enum KeyedState {
    Single(BTreeMap<DataType, Rows>),
    Double(BTreeMap<(DataType, DataType), Rows>),
    Tri(BTreeMap<(DataType, DataType, DataType), Rows>),
    Quad(BTreeMap<(DataType, DataType, DataType, DataType), Rows>),
    Quin(BTreeMap<(DataType, DataType, DataType, DataType, DataType), Rows>),
    Sex(BTreeMap<(DataType, DataType, DataType, DataType, DataType, DataType), Rows>),
}

impl KeyedState {
    pub(super) fn lookup<'a>(&'a self, key: &KeyType) -> Option<&'a Rows> {
        match (self, key) {
            (&KeyedState::Single(ref m), &KeyType::Single(k)) => m.get(k),
            (&KeyedState::Double(ref m), &KeyType::Double(ref k)) => m.get(k),
            (&KeyedState::Tri(ref m), &KeyType::Tri(ref k)) => m.get(k),
            (&KeyedState::Quad(ref m), &KeyType::Quad(ref k)) => m.get(k),
            (&KeyedState::Quin(ref m), &KeyType::Quin(ref k)) => m.get(k),
            (&KeyedState::Sex(ref m), &KeyType::Sex(ref k)) => m.get(k),
            _ => panic!("Invalid key type for KeyedState, got: {:?}", key),
        }
    }

    pub(super) fn lookup_range<'a>(
        &'a self,
        key: &RangeKey,
    ) -> Result<Box<dyn Iterator<Item = &'a Row> + 'a>, Misses<'a>> {
        macro_rules! full_range {
            ($m: expr) => {
                Ok(Box::new($m.range(..).flat_map(|(_, rows)| rows)))
            };
        }

        macro_rules! range {
            ($m: expr, $range: ident) => {
                Ok(Box::new(
                    $m.range((
                        $range.0.map(|k| k.map(Clone::clone)),
                        $range.1.map(|k| k.map(Clone::clone)),
                    ))
                    .flat_map(|(_, rows)| rows),
                ))
            };
        }

        match (self, key) {
            (&KeyedState::Single(ref m), &RangeKey::Unbounded) => full_range!(m),
            (&KeyedState::Double(ref m), &RangeKey::Unbounded) => full_range!(m),
            (&KeyedState::Tri(ref m), &RangeKey::Unbounded) => full_range!(m),
            (&KeyedState::Quad(ref m), &RangeKey::Unbounded) => full_range!(m),
            (&KeyedState::Sex(ref m), &RangeKey::Unbounded) => full_range!(m),
            (&KeyedState::Single(ref m), &RangeKey::Single(range)) => {
                Ok(Box::new(m.range(range).flat_map(|(_, rows)| rows)))
            }
            (&KeyedState::Double(ref m), &RangeKey::Double(range)) => range!(m, range),
            (&KeyedState::Tri(ref m), &RangeKey::Tri(range)) => range!(m, range),
            (&KeyedState::Quad(ref m), &RangeKey::Quad(range)) => range!(m, range),
            (&KeyedState::Sex(ref m), &RangeKey::Sex(range)) => range!(m, range),
            _ => panic!("Invalid key type for KeyedState, got: {:?}", key),
        }
    }

    /// Remove all rows for a randomly chosen key seeded by `seed`, returning that key along with
    /// the number of bytes freed. Returns `None` if map is empty.
    pub(super) fn evict_with_seed(&mut self, seed: usize) -> Option<(u64, Vec<DataType>)> {
        // TODO(grfn): This way of evicting (which also happens in evbtree) is pretty icky - we have
        // to iterate the sequence of keys, *and* we have to clone out the keys themselves! we
        // should find a better way to do that.
        // https://app.clubhouse.io/readysettech/story/154
        let (rs, key) = match *self {
            KeyedState::Single(ref mut m) if !m.is_empty() => {
                let index = seed % m.len();
                let key = m.keys().nth(index).unwrap().clone();
                m.remove_entry(&key).map(|(k, rs)| (rs, vec![k]))
            }
            KeyedState::Double(ref mut m) if !m.is_empty() => {
                let index = seed % m.len();
                let key = m.keys().nth(index).unwrap().clone();
                m.remove_entry(&key).map(|(k, rs)| (rs, vec![k.0, k.1]))
            }
            KeyedState::Tri(ref mut m) if !m.is_empty() => {
                let index = seed % m.len();
                let key = m.keys().nth(index).unwrap().clone();
                m.remove_entry(&key)
                    .map(|(k, rs)| (rs, vec![k.0, k.1, k.2]))
            }
            KeyedState::Quad(ref mut m) if !m.is_empty() => {
                let index = seed % m.len();
                let key = m.keys().nth(index).unwrap().clone();
                m.remove_entry(&key)
                    .map(|(k, rs)| (rs, vec![k.0, k.1, k.2, k.3]))
            }
            KeyedState::Quin(ref mut m) if !m.is_empty() => {
                let index = seed % m.len();
                let key = m.keys().nth(index).unwrap().clone();
                m.remove_entry(&key)
                    .map(|(k, rs)| (rs, vec![k.0, k.1, k.2, k.3, k.4]))
            }
            KeyedState::Sex(ref mut m) if !m.is_empty() => {
                let index = seed % m.len();
                let key = m.keys().nth(index).unwrap().clone();
                m.remove_entry(&key)
                    .map(|(k, rs)| (rs, vec![k.0, k.1, k.2, k.3, k.4, k.5]))
            }
            _ => {
                // map must be empty, so no point in trying to evict from it.
                return None;
            }
        }?;
        Some((
            rs.iter()
                .filter(|r| Rc::strong_count(&r.0) == 1)
                .map(SizeOf::deep_size_of)
                .sum(),
            key,
        ))
    }

    /// Remove all rows for the given key, returning the number of bytes freed.
    pub(super) fn evict(&mut self, key: &[DataType]) -> u64 {
        match *self {
            KeyedState::Single(ref mut m) => m.remove(&(key[0])),
            KeyedState::Double(ref mut m) => m.remove::<(DataType, _)>(&MakeKey::from_key(key)),
            KeyedState::Tri(ref mut m) => m.remove::<(DataType, _, _)>(&MakeKey::from_key(key)),
            KeyedState::Quad(ref mut m) => m.remove::<(DataType, _, _, _)>(&MakeKey::from_key(key)),
            KeyedState::Quin(ref mut m) => {
                m.remove::<(DataType, _, _, _, _)>(&MakeKey::from_key(key))
            }
            KeyedState::Sex(ref mut m) => {
                m.remove::<(DataType, _, _, _, _, _)>(&MakeKey::from_key(key))
            }
        }
        .map(|rows| {
            rows.iter()
                .filter(|r| Rc::strong_count(&r.0) == 1)
                .map(SizeOf::deep_size_of)
                .sum()
        })
        .unwrap_or(0)
    }
}

impl<'a> Into<KeyedState> for &'a [usize] {
    fn into(self) -> KeyedState {
        match self.len() {
            0 => unreachable!(),
            1 => KeyedState::Single(BTreeMap::default()),
            2 => KeyedState::Double(BTreeMap::default()),
            3 => KeyedState::Tri(BTreeMap::default()),
            4 => KeyedState::Quad(BTreeMap::default()),
            5 => KeyedState::Quin(BTreeMap::default()),
            6 => KeyedState::Sex(BTreeMap::default()),
            x => panic!("invalid compound key of length: {}", x),
        }
    }
}
