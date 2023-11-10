use std::fmt;
use std::iter::FusedIterator;
use std::ops::Deref;

use smallvec::SmallVec;
use triomphe::Arc;

use crate::eviction::EvictionMeta;

/// A sorted vector of values for a given key in the map with access metadata for eviction
#[derive(Clone)]
pub struct Values<T> {
    eviction_meta: EvictionMeta,
    values: ValuesInner<T>,
}

impl<T> Default for Values<T> {
    fn default() -> Self {
        Values {
            eviction_meta: Default::default(),
            values: ValuesInner::new(),
        }
    }
}

/// A sorted vector of values for a given key in the map.
#[repr(transparent)]
#[derive(Default, Clone)]
pub(crate) struct ValuesInner<T>(Arc<SmallVec<[T; 1]>>);

/// An iterator over Values
pub struct ValuesIter<'a, T>(std::slice::Iter<'a, T>);

impl<T> fmt::Debug for Values<T>
where
    T: fmt::Debug,
{
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.values.fmt(fmt)
    }
}

impl<T> fmt::Debug for ValuesInner<T>
where
    T: fmt::Debug,
{
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_set().entries(self.0.iter()).finish()
    }
}

impl<T> ValuesInner<T> {
    fn new() -> Self {
        ValuesInner(Arc::new(SmallVec::new()))
    }
}

impl<T> Deref for Values<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        self.values.0.as_slice()
    }
}

impl<T> AsRef<Arc<SmallVec<[T; 1]>>> for Values<T> {
    fn as_ref(&self) -> &Arc<SmallVec<[T; 1]>> {
        &self.values.0
    }
}

impl<T> Values<T> {
    pub(crate) fn new(eviction_meta: EvictionMeta) -> Self {
        Values {
            eviction_meta,
            values: ValuesInner(Arc::new(smallvec::SmallVec::new())),
        }
    }

    /// Returns the number of values.
    pub fn len(&self) -> usize {
        self.values.0.len()
    }

    /// Returns true if holds no values.
    pub fn is_empty(&self) -> bool {
        self.values.0.is_empty()
    }

    /// Returns the number of values that can be held without reallocating.
    pub fn capacity(&self) -> usize {
        self.values.0.capacity()
    }

    /// An iterator visiting all elements in arbitrary order.
    ///
    /// The iterator element type is &T.
    pub fn iter(&self) -> ValuesIter<'_, T> {
        ValuesIter(self.values.0.iter())
    }

    /// Returns a guarded reference to _one_ value corresponding to the key.
    ///
    /// This is mostly intended for use when you are working with no more than one value per key.
    /// If there are multiple values stored for this key, the smallest one is returned
    pub fn first(&self) -> Option<&T> {
        self.values.0.first()
    }

    /// Get the eviction metadata associated with that value set
    pub fn eviction_meta(&self) -> &EvictionMeta {
        &self.eviction_meta
    }

    /// Inserts an element at position index within the vector, shifting all elements after it to
    /// the right.
    pub(crate) fn insert(&mut self, index: usize, element: T)
    where
        T: Clone,
    {
        Arc::make_mut(&mut self.values.0).insert(index, element);
    }

    /// Removes the element at position index within the vector, shifting all elements after it to
    /// the left.
    ///
    /// Note: Because this shifts over the remaining elements, it has a worst-case
    /// performance of O(n).
    pub(crate) fn remove(&mut self, index: usize)
    where
        T: PartialEq + Clone,
    {
        Arc::make_mut(&mut self.values.0).remove(index);
    }

    pub(crate) fn clear(&mut self)
    where
        T: Clone,
    {
        Arc::make_mut(&mut self.values.0).clear()
    }

    pub(crate) fn retain<F>(&mut self, f: F)
    where
        T: Clone,
        F: FnMut(&mut T) -> bool,
    {
        Arc::make_mut(&mut self.values.0).retain(f)
    }
}

impl<'a, T: 'a> IntoIterator for &'a Values<T> {
    type IntoIter = ValuesIter<'a, T>;
    type Item = &'a T;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<T> ValuesInner<T> {}

impl<'a, T> fmt::Debug for ValuesIter<'a, T>
where
    T: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.0.clone()).finish()
    }
}

impl<'a, T> Iterator for ValuesIter<'a, T> {
    type Item = &'a T;
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

impl<'a, T> ExactSizeIterator for ValuesIter<'a, T> {}

impl<'a, T> FusedIterator for ValuesIter<'a, T> {}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! assert_empty {
        ($x:expr) => {
            assert_eq!($x.len(), 0);
            assert!($x.is_empty());
            assert_eq!($x.iter().count(), 0);
            assert_eq!($x.into_iter().count(), 0);
            assert_eq!($x.first(), None);
        };
    }

    macro_rules! assert_len {
        ($x:expr, $n:expr) => {
            assert_eq!($x.len(), $n);
            assert!(!$x.is_empty());
            assert_eq!($x.iter().count(), $n);
            assert_eq!($x.into_iter().count(), $n);
        };
    }

    #[test]
    fn sensible_default() {
        let v: Values<i32> = Values::default();
        assert_eq!(v.capacity(), 1);
        assert_empty!(v);
    }

    #[test]
    fn long_values() {
        let mut v: Values<i32> = Values::default();

        let values = 0..1000;
        let len = values.clone().count();
        for (i, e) in values.clone().enumerate() {
            v.insert(i, e);
        }

        for i in values.clone() {
            assert!(v.contains(&i));
        }
        assert_len!(v, len);
        assert!(values.contains(v.first().unwrap()));

        v.clear();

        assert_empty!(v);

        // clear() should not affect capacity or value type!
        assert!(v.capacity() > 1);
    }
}
