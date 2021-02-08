use super::ReadHandle;
use crate::inner::Inner;
use std::collections::hash_map::RandomState;
use std::fmt;
use std::hash::BuildHasher;

/// A type that is both `Sync` and `Send` and lets you produce new [`ReadHandle`] instances.
///
/// This serves as a handy way to distribute read handles across many threads without requiring
/// additional external locking to synchronize access to the non-`Sync` [`ReadHandle`] type. Note
/// that this _internally_ takes a lock whenever you call [`ReadHandleFactory::handle`], so
/// you should not expect producing new handles rapidly to scale well.
pub struct ReadHandleFactory<K, V, M, T, S = RandomState>
where
    K: Ord + Clone,
    S: BuildHasher,
    T: Clone,
{
    pub(super) factory: left_right::ReadHandleFactory<Inner<K, V, M, T, S>>,
}

impl<K, V, M, T, S> fmt::Debug for ReadHandleFactory<K, V, M, T, S>
where
    K: Ord + Clone,
    S: BuildHasher,
    T: Clone,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadHandleFactory")
            .field("factory", &self.factory)
            .finish()
    }
}

impl<K, V, M, T, S> Clone for ReadHandleFactory<K, V, M, T, S>
where
    K: Ord + Clone,
    S: BuildHasher,
    T: Clone,
{
    fn clone(&self) -> Self {
        Self {
            factory: self.factory.clone(),
        }
    }
}

impl<K, V, M, T, S> ReadHandleFactory<K, V, M, T, S>
where
    K: Ord + Clone,
    S: BuildHasher,
    T: Clone,
{
    /// Produce a new [`ReadHandle`] to the same left-right data structure as this factory was
    /// originally produced from.
    pub fn handle(&self) -> ReadHandle<K, V, M, T, S> {
        ReadHandle {
            handle: self.factory.handle(),
        }
    }
}
