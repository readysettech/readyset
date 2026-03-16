use std::fmt::{self, Debug};
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
        if let Some(prev_updated) = self.prev_updated {
            // just checked `prev_updated`, and it's only set when `updated` has a value
            return Some(self.updated.unwrap() - prev_updated);
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
    values: Arc<SmallVec<[T; 1]>>,
    order: I,
    eviction_meta: EvictionMeta,
    metrics: Metrics,
}

impl<T, I> Default for Values<T, I>
where
    I: InsertionOrder<T>,
{
    fn default() -> Self {
        Values {
            values: Arc::new(SmallVec::new()),
            order: Default::default(),
            eviction_meta: Default::default(),
            metrics: Default::default(),
        }
    }
}

impl<T, I> Debug for Values<T, I>
where
    T: Debug,
{
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("Values")
            .field("values", &*self.values)
            .field("eviction_meta", &self.eviction_meta)
            .finish_non_exhaustive()
    }
}

impl<T, I> Values<T, I>
where
    I: InsertionOrder<T>,
{
    pub(crate) fn new(eviction_meta: EvictionMeta, order: I) -> Self {
        Values {
            values: Default::default(),
            order,
            eviction_meta,
            metrics: Default::default(),
        }
    }

    /// Get the eviction metadata associated with the value set
    pub fn eviction_meta(&self) -> &EvictionMeta {
        &self.eviction_meta
    }

    /// Gets the metrics associated with the value set
    pub(crate) fn metrics(&self) -> &Metrics {
        &self.metrics
    }

    /// Returns the number of values.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Returns true if holds no values.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Assigns new InsertionOrder
    pub fn set_order(&mut self, order: I) {
        self.order = order;
    }

    /// An iterator visiting all elements in sorted order.
    pub fn iter(&self) -> std::slice::Iter<'_, T> {
        self.values.iter()
    }

    fn find(
        values: &SmallVec<[T; 1]>,
        order: &I,
        value: &T,
        cache: &mut Option<usize>,
        insert: bool,
    ) where
        T: Ord + Clone,
    {
        let i = if let Some(cache) = cache {
            Ok(*cache) // cached from first time
        } else {
            values.binary_search_by(|x| order.cmp(x, value))
        };

        *cache = if insert {
            Some(i.unwrap_or_else(|x| x))
        } else {
            // Option<usize> doesn't permit us to encode for the second side "we searched for
            // this the first time, but didn't find it," so if the workload deletes non-
            // existent keys, we will repeat the search in vain the second time.  But since
            // that's not the workload, we're okay here.  (But proptests do generate deletions
            // of non-existent values.)
            i.ok()
        };
    }

    /// Checks if the value is present.  Used in tests only.
    pub fn contains(&self, value: &T) -> bool
    where
        T: Clone + Ord + PartialEq,
    {
        self.values.contains(value)
    }

    /// Inserts an element at position index within the vector, shifting all elements after it
    /// to the right.
    pub(crate) fn insert(&mut self, value: T, index: &mut Option<usize>, timestamp: Instant)
    where
        T: Ord + Clone,
    {
        Self::find(&self.values, &self.order, &value, index, true);
        Arc::make_mut(&mut self.values).insert(index.unwrap(), value);
        self.metrics.update(timestamp);
    }

    /// Removes the element at position index within the vector, shifting all elements after
    /// it to the left.
    pub(crate) fn remove(&mut self, value: &T, index: &mut Option<usize>, timestamp: Instant)
    where
        T: Ord + Clone,
    {
        Self::find(&self.values, &self.order, value, index, false);
        if let Some(index) = *index {
            Arc::make_mut(&mut self.values).remove(index);
        }
        self.metrics.update(timestamp);
    }

    pub(crate) fn clear(&mut self)
    where
        T: Clone,
    {
        self.values = Default::default();
    }

    /// Returns the values as a shared Arc over the SmallVec.
    pub fn to_shared_smallvec(&self) -> Arc<SmallVec<[T; 1]>>
    where
        T: Clone,
    {
        Arc::clone(&self.values)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DefaultInsertionOrder;

    type TestValues = Values<i32, DefaultInsertionOrder>;

    macro_rules! assert_empty {
        ($x:expr_2021) => {
            assert_eq!($x.len(), 0);
            assert!($x.is_empty());
            assert_eq!($x.iter().count(), 0);
        };
    }

    macro_rules! assert_len {
        ($x:expr_2021, $n:expr_2021) => {
            assert_eq!($x.len(), $n);
            assert!(!$x.is_empty());
            assert_eq!($x.iter().count(), $n);
        };
    }

    #[test]
    fn sensible_default() {
        let v: Values<i32, DefaultInsertionOrder> = Values::default();
        assert_eq!(v.values.capacity(), 1);
        assert_empty!(v);
    }

    #[test]
    fn long_values() {
        let mut v: Values<i32, DefaultInsertionOrder> = Values::default();

        let values = 0..1000;
        let len = values.clone().count();
        for (i, e) in values.clone().enumerate() {
            v.insert(e, &mut Some(i), Instant::now());
        }

        for i in values.clone() {
            assert!(v.contains(&i));
        }
        assert_len!(v, len);

        v.clear();
        assert_empty!(v);
    }

    #[test]
    fn duplicate_values() {
        const ROWS: usize = 100;

        let mut v = TestValues::default();
        for _ in 0..ROWS {
            v.insert(1, &mut None, Instant::now());
        }
        assert_eq!(v.len(), ROWS);
        assert_eq!(v.to_shared_smallvec().len(), ROWS);

        v.remove(&1, &mut None, Instant::now());
        assert_eq!(v.to_shared_smallvec().len(), ROWS - 1);
    }

    #[derive(Debug, Default, Clone)]
    struct Backwards;

    impl<T> InsertionOrder<T> for Backwards
    where
        T: Ord,
    {
        fn cmp(&self, a: &T, b: &T) -> std::cmp::Ordering {
            a.cmp(b).reverse()
        }
    }

    #[test]
    fn backwards() {
        let mut v: Values<u64, Backwards> = Values::default();
        for i in 0..12 {
            v.insert(i, &mut None, Instant::now());
        }

        let expect = vec![11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0];
        assert_eq!(v.iter().cloned().collect::<Vec<_>>(), expect);

        v.remove(&11, &mut None, Instant::now());
        assert_eq!(v.iter().cloned().collect::<Vec<_>>(), &expect[1..]);
    }
}
