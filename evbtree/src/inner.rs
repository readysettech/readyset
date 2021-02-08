use std::collections::BTreeMap;
use std::fmt;
use std::hash::BuildHasher;
use unbounded_interval_tree::IntervalTree;

use crate::values::ValuesInner;
use left_right::aliasing::DropBehavior;

pub(crate) struct Inner<K, V, M, T, S, D = crate::aliasing::NoDrop>
where
    K: Ord + Clone,
    S: BuildHasher,
    D: DropBehavior,
    T: Clone,
{
    pub(crate) data: BTreeMap<K, ValuesInner<V, S, D>>,
    pub(crate) tree: IntervalTree<K>,
    pub(crate) meta: M,
    pub(crate) timestamp: T,
    pub(crate) ready: bool,
    pub(crate) hasher: S,
}

impl<K, V, M, T, S> fmt::Debug for Inner<K, V, M, T, S>
where
    K: Ord + Clone + fmt::Debug,
    S: BuildHasher,
    V: fmt::Debug,
    M: fmt::Debug,
    T: Clone + fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Inner")
            .field("data", &self.data)
            .field("tree", &self.tree)
            .field("meta", &self.meta)
            .field("ready", &self.ready)
            .field("timestamp", &self.timestamp)
            .finish()
    }
}

impl<K, V, M, T, S> Clone for Inner<K, V, M, T, S>
where
    K: Ord + Clone,
    S: BuildHasher + Clone,
    M: Clone,
    T: Clone,
{
    fn clone(&self) -> Self {
        assert!(self.data.is_empty());
        Inner {
            data: BTreeMap::new(),
            tree: IntervalTree::default(),
            meta: self.meta.clone(),
            timestamp: self.timestamp.clone(),
            ready: self.ready,
            hasher: self.hasher.clone(),
        }
    }
}

impl<K, V, M, T, S> Inner<K, V, M, T, S>
where
    K: Ord + Clone,
    S: BuildHasher,
    T: Clone,
{
    pub(crate) fn with_hasher(meta: M, timestamp: T, hasher: S) -> Self {
        Inner {
            data: BTreeMap::default(),
            tree: IntervalTree::default(),
            meta,
            timestamp,
            ready: false,
            hasher,
        }
    }
}
