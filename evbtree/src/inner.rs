use std::collections::BTreeMap;
use std::fmt;
use std::hash::BuildHasher;

use crate::values::ValuesInner;
use left_right::aliasing::DropBehavior;

pub(crate) struct Inner<K, V, M, S, D = crate::aliasing::NoDrop>
where
    K: Ord,
    S: BuildHasher,
    D: DropBehavior,
{
    pub(crate) data: BTreeMap<K, ValuesInner<V, S, D>>,
    pub(crate) meta: M,
    pub(crate) ready: bool,
    pub(crate) hasher: S,
}

impl<K, V, M, S> fmt::Debug for Inner<K, V, M, S>
where
    K: Ord + fmt::Debug,
    S: BuildHasher,
    V: fmt::Debug,
    M: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Inner")
            .field("data", &self.data)
            .field("meta", &self.meta)
            .field("ready", &self.ready)
            .finish()
    }
}

impl<K, V, M, S> Clone for Inner<K, V, M, S>
where
    K: Ord + Clone,
    S: BuildHasher + Clone,
    M: Clone,
{
    fn clone(&self) -> Self {
        assert!(self.data.is_empty());
        Inner {
            data: BTreeMap::new(),
            meta: self.meta.clone(),
            ready: self.ready,
            hasher: self.hasher.clone(),
        }
    }
}

impl<K, V, M, S> Inner<K, V, M, S>
where
    K: Ord,
    S: BuildHasher,
{
    pub(crate) fn with_hasher(meta: M, hasher: S) -> Self {
        Inner {
            data: BTreeMap::default(),
            meta,
            ready: false,
            hasher,
        }
    }
}
