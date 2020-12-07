use std::fmt;
use std::hash::{BuildHasher, Hash};

#[cfg(feature = "indexed")]
pub(crate) use indexmap::IndexMap as MapImpl;
#[cfg(not(feature = "indexed"))]
pub(crate) use std::collections::HashMap as MapImpl;

use crate::values::ValuesInner;
use left_right::aliasing::DropBehavior;

pub(crate) struct Inner<K, V, M, S, D = crate::aliasing::NoDrop>
where
    K: Eq + Hash,
    S: BuildHasher,
    D: DropBehavior,
{
    pub(crate) data: MapImpl<K, ValuesInner<V, S, D>, S>,
    pub(crate) meta: M,
    pub(crate) ready: bool,
}

impl<K, V, M, S> fmt::Debug for Inner<K, V, M, S>
where
    K: Eq + Hash + fmt::Debug,
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
    K: Eq + Hash + Clone,
    S: BuildHasher + Clone,
    M: Clone,
{
    fn clone(&self) -> Self {
        assert!(self.data.is_empty());
        Inner {
            data: MapImpl::with_capacity_and_hasher(
                self.data.capacity(),
                self.data.hasher().clone(),
            ),
            meta: self.meta.clone(),
            ready: self.ready,
        }
    }
}

impl<K, V, M, S> Inner<K, V, M, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    pub(crate) fn with_hasher(m: M, hash_builder: S) -> Self {
        Inner {
            data: MapImpl::with_hasher(hash_builder),
            meta: m,
            ready: false,
        }
    }

    pub(crate) fn with_capacity_and_hasher(m: M, capacity: usize, hash_builder: S) -> Self {
        Inner {
            data: MapImpl::with_capacity_and_hasher(capacity, hash_builder),
            meta: m,
            ready: false,
        }
    }
}
