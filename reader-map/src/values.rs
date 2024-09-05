use std::fmt;
use std::time::{Duration, Instant};

use partial_map::InsertionOrder;
use smallvec::SmallVec;
use triomphe::Arc;

use crate::eviction::EvictionMeta;

#[derive(Clone, Default)]
pub(crate) struct Metrics {
    /// The timestamp when a value was first inserted into this `Values`.
    created: Option<Instant>,
    /// The timestamp of the most recent update to this entry.
    updated: Option<Instant>,
    /// The previous update timestamp; used to calculate the time interval between the most recent
    /// updates.
    prev_updated: Option<Instant>,
}

impl Metrics {
    fn update(&mut self, next_ts: Instant) {
        if self.created.is_none() {
            self.created = Some(next_ts);
        }

        self.prev_updated = self.updated;
        self.updated = Some(next_ts);
    }

    // The amount of time between the last two updates.
    #[allow(dead_code)]
    pub(crate) fn last_update_interval(&self) -> Option<Duration> {
        if self.prev_updated.is_some() {
            // just checked `prev_updated`, and it's only set when `updated` has a value
            return Some(self.updated.unwrap() - self.prev_updated.unwrap());
        }
        None
    }

    // The amount of time since created.
    #[allow(dead_code)]
    pub(crate) fn lifetime(&self) -> Option<Duration> {
        self.created.map(|created| created.elapsed())
    }
}

/// A sorted vector of values for a given key in the map with access metadata for eviction
#[derive(Clone)]
pub struct Values<T, I> {
    values: ValuesInner<T>,
    #[allow(dead_code)]
    order: Option<I>,
    eviction_meta: EvictionMeta,
    metrics: Metrics,
}

impl<T, I> Default for Values<T, I>
where
    I: InsertionOrder<T>,
{
    fn default() -> Self {
        Values {
            values: ValuesInner::new(),
            order: None,
            eviction_meta: Default::default(),
            metrics: Default::default(),
        }
    }
}

/// Values for a given key in the map.
#[derive(Clone, Debug)]
pub(crate) enum ValuesInner<T> {
    SmallVec(Arc<SmallVec<[T; 1]>>),
}

impl<T, I> fmt::Debug for Values<T, I>
where
    T: fmt::Debug,
{
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.values.fmt(fmt)
    }
}

impl<T> ValuesInner<T> {
    fn new() -> Self {
        ValuesInner::SmallVec(Arc::new(SmallVec::new()))
    }
}

impl<T, I> Values<T, I>
where
    I: InsertionOrder<T>,
{
    pub(crate) fn new(eviction_meta: EvictionMeta, order: Option<I>) -> Self {
        Values {
            values: ValuesInner::SmallVec(Arc::new(smallvec::SmallVec::new())),
            order,
            eviction_meta,
            metrics: Default::default(),
        }
    }

    /// Returns the number of values.
    pub fn len(&self) -> usize {
        match self.values {
            ValuesInner::SmallVec(ref v) => v.len(),
        }
    }

    /// Returns true if holds no values.
    pub fn is_empty(&self) -> bool {
        match self.values {
            ValuesInner::SmallVec(ref v) => v.is_empty(),
        }
    }

    /// An iterator visiting all elements in arbitrary order.
    ///
    /// The iterator element type is &T.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = &T> {
        match self.values {
            ValuesInner::SmallVec(ref v) => v.iter(),
        }
    }

    /// Returns a guarded reference to _one_ value corresponding to the key.
    ///
    /// This is mostly intended for use when you are working with no more than one value per key.
    /// If there are multiple values stored for this key, the smallest one is returned
    pub fn first(&self) -> Option<&T> {
        match self.values {
            ValuesInner::SmallVec(ref v) => v.first(),
        }
    }

    /// Get the eviction metadata associated with that value set
    pub fn eviction_meta(&self) -> &EvictionMeta {
        &self.eviction_meta
    }

    fn find(&self, value: &T, order: &Option<I>, cache: &mut Option<usize>, insert: bool)
    where
        T: Ord + Clone,
    {
        let i = if let Some(cache) = cache {
            Ok(*cache) // cached from first time
        } else if let Some(order) = order {
            match self.values {
                ValuesInner::SmallVec(ref v) => v.binary_search_by(|x| order.cmp(x, value)),
            }
        } else {
            match self.values {
                ValuesInner::SmallVec(ref v) => v.binary_search(value),
            }
        };

        *cache = if insert {
            Some(i.unwrap_or_else(|x| x))
        } else if let Ok(x) = i {
            Some(x)
        } else {
            // Option<usize> doesn't permit us to encode for the second side "we searched for
            // this the first time, but didn't find it," so if the workload deletes non-
            // existent keys, we will repeat the search in vain the second time.  But since
            // that's not the workload, we're okay here.  (But proptests do generate deletions
            // of non-existent values.)
            None
        }
    }

    /// Checks if the value is present.  Used in tests only.
    pub fn contains(&self, value: &T) -> bool
    where
        T: PartialEq,
    {
        match self.values {
            ValuesInner::SmallVec(ref v) => v.contains(value),
        }
    }

    /// Inserts an element at position index within the vector, shifting all elements after it to
    /// the right.
    pub(crate) fn insert(
        &mut self,
        value: T,
        order: &Option<I>,
        index: &mut Option<usize>,
        timestamp: Instant,
    ) where
        T: Ord + Clone,
    {
        // Always insert values in sorted order, even if no ordering method is provided,
        // otherwise it will require a linear scan to remove a value
        self.find(&value, order, index, true);
        match self.values {
            ValuesInner::SmallVec(ref mut v) => Arc::make_mut(v).insert(index.unwrap(), value),
        }
        self.metrics.update(timestamp);
    }

    /// Removes the element at position index within the vector, shifting all elements after it to
    /// the left.
    ///
    /// Note: Because this shifts over the remaining elements, it has a worst-case
    /// performance of O(n).
    pub(crate) fn remove(
        &mut self,
        value: &T,
        order: &Option<I>,
        index: &mut Option<usize>,
        timestamp: Instant,
    ) where
        T: Ord + Clone,
    {
        self.find(value, order, index, false);
        if let Some(index) = *index {
            match self.values {
                ValuesInner::SmallVec(ref mut v) => Arc::make_mut(v).remove(index),
            };
        }
        self.metrics.update(timestamp);
    }

    pub(crate) fn clear(&mut self)
    where
        T: Clone,
    {
        match self.values {
            ValuesInner::SmallVec(ref mut v) => Arc::make_mut(v).clear(),
        }
    }

    pub(crate) fn retain<F>(&mut self, f: F)
    where
        T: Clone,
        F: FnMut(&mut T) -> bool,
    {
        match self.values {
            ValuesInner::SmallVec(ref mut v) => Arc::make_mut(v).retain(f),
        }
    }

    pub(crate) fn metrics(&self) -> &Metrics {
        &self.metrics
    }

    /// Returns the values as a SmallVec.  If the internal storage is a SmallVec, this merely
    /// wraps the internal storage in an Arc.  If not, the stored values are copied into a
    /// new SmallVec.
    pub fn to_shared_smallvec(&self) -> Arc<SmallVec<[T; 1]>> {
        match self.values {
            ValuesInner::SmallVec(ref v) => Arc::clone(v),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DefaultInsertionOrder;

    macro_rules! assert_empty {
        ($x:expr) => {
            assert_eq!($x.len(), 0);
            assert!($x.is_empty());
            assert_eq!($x.iter().count(), 0);
            assert_eq!($x.first(), None);
        };
    }

    macro_rules! assert_len {
        ($x:expr, $n:expr) => {
            assert_eq!($x.len(), $n);
            assert!(!$x.is_empty());
            assert_eq!($x.iter().count(), $n);
        };
    }

    #[test]
    fn sensible_default() {
        let v: Values<i32, DefaultInsertionOrder> = Values::default();
        match v.values {
            ValuesInner::SmallVec(ref v) => assert_eq!(v.capacity(), 1),
        }
        assert_empty!(v);
    }

    #[test]
    fn long_values() {
        let mut v: Values<i32, DefaultInsertionOrder> = Values::default();

        let values = 0..1000;
        let len = values.clone().count();
        for (i, e) in values.clone().enumerate() {
            v.insert(
                e,
                &None::<DefaultInsertionOrder>,
                &mut Some(i),
                Instant::now(),
            );
        }

        for i in values.clone() {
            assert!(v.contains(&i));
        }
        assert_len!(v, len);
        assert!(values.contains(v.first().unwrap()));

        v.clear();

        assert_empty!(v);
    }
}
