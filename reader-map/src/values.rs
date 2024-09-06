use std::collections::BTreeMap;
use std::fmt::{self, Debug};
use std::time::{Duration, Instant};

use itertools::Either;
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

#[derive(Clone)]
pub(crate) struct BTreeValue<T, I> {
    value: T,
    order: I,
}

impl<T, I> BTreeValue<T, I> {
    fn new(value: T, order: I) -> Self {
        Self { value, order }
    }
}

impl<T, I> Debug for BTreeValue<T, I>
where
    T: Debug,
{
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> std::result::Result<(), fmt::Error> {
        self.value.fmt(fmt)
    }
}

impl<T, I> PartialEq for BTreeValue<T, I>
where
    T: PartialEq,
    I: InsertionOrder<T>,
{
    fn eq(&self, other: &BTreeValue<T, I>) -> bool {
        self.order.cmp(&self.value, &other.value) == std::cmp::Ordering::Equal
    }
}

impl<T, I> Eq for BTreeValue<T, I>
where
    T: Eq,
    I: InsertionOrder<T>,
{
}

impl<T, I> PartialOrd for BTreeValue<T, I>
where
    T: PartialOrd,
    I: InsertionOrder<T>,
{
    fn partial_cmp(&self, other: &BTreeValue<T, I>) -> Option<std::cmp::Ordering> {
        Some(self.order.cmp(&self.value, &other.value))
    }
}

impl<T, I> Ord for BTreeValue<T, I>
where
    T: Ord,
    I: InsertionOrder<T>,
{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.order.cmp(&self.value, &other.value)
    }
}

impl<T, I> std::borrow::Borrow<T> for BTreeValue<T, I> {
    fn borrow(&self) -> &T {
        &self.value
    }
}

#[derive(Debug)]
pub struct BTreeUnwrapIterator<'a, T, I> {
    inner: std::collections::btree_map::Iter<'a, BTreeValue<T, I>, usize>,
    current: Option<&'a T>,
    count: usize,
}

impl<'a, T, I> BTreeUnwrapIterator<'a, T, I> {
    fn new(inner: std::collections::btree_map::Iter<'a, BTreeValue<T, I>, usize>) -> Self {
        Self {
            inner,
            current: None,
            count: 0,
        }
    }
}

impl<'a, T, I> Iterator for BTreeUnwrapIterator<'a, T, I> {
    type Item = &'a T;

    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        if self.count == 0 {
            let (wrapped, count) = self.inner.next()?;
            self.current = Some(&wrapped.value);
            self.count = *count;
            self.next()
        } else {
            self.count -= 1;
            self.current
        }
    }
}

/// Values for a given key in the map.
#[derive(Clone)]
pub(crate) enum ValuesInner<T, I> {
    SmallVec(Arc<SmallVec<[T; 1]>>),
    BTreeMap {
        map: BTreeMap<BTreeValue<T, I>, usize>, // value -> count of duplicates
        len: usize,                             // including duplicates
    },
}

impl<T, I> Debug for ValuesInner<T, I>
where
    T: Debug,
{
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ValuesInner::SmallVec(v) => v.fmt(fmt),
            ValuesInner::BTreeMap { map, .. } => map.fmt(fmt),
        }
    }
}

impl<T, I> ValuesInner<T, I> {
    fn new() -> Self {
        ValuesInner::SmallVec(Arc::new(SmallVec::new()))
    }

    fn new_btree() -> Self {
        ValuesInner::BTreeMap {
            map: Default::default(),
            len: 0,
        }
    }

    fn smallvec_iter(&self) -> std::slice::Iter<'_, T> {
        match self {
            ValuesInner::SmallVec(v) => v.iter(),
            ValuesInner::BTreeMap { .. } => unreachable!(),
        }
    }
}

/// A sorted vector of values for a given key in the map with access metadata for eviction
#[derive(Clone)]
pub struct Values<T, I> {
    values: ValuesInner<T, I>,
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
            values: ValuesInner::new(),
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
        self.values.fmt(fmt)
    }
}

impl<T, I> Values<T, I>
where
    I: InsertionOrder<T>,
{
    const VEC_MAX: usize = 10;

    pub(crate) fn new(eviction_meta: EvictionMeta, order: I) -> Self {
        Values {
            values: ValuesInner::new(),
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
        match self.values {
            ValuesInner::SmallVec(ref v) => v.len(),
            ValuesInner::BTreeMap { len, .. } => len,
        }
    }

    /// Returns true if holds no values.
    pub fn is_empty(&self) -> bool {
        match self.values {
            ValuesInner::SmallVec(ref v) => v.is_empty(),
            ValuesInner::BTreeMap { ref map, .. } => map.is_empty(),
        }
    }

    /// An iterator visiting all elements in arbitrary order.
    pub fn iter(&self) -> Either<std::slice::Iter<'_, T>, BTreeUnwrapIterator<'_, T, I>> {
        match self.values {
            ValuesInner::SmallVec(ref v) => Either::Left(v.iter()),
            ValuesInner::BTreeMap { ref map, .. } => {
                Either::Right(BTreeUnwrapIterator::new(map.iter()))
            }
        }
    }

    /// Returns a guarded reference to _one_ value corresponding to the key.
    ///
    /// This is mostly intended for use when you are working with no more than one value per key.
    /// If there are multiple values stored for this key, the smallest one is returned
    pub fn first(&self) -> Option<&T>
    where
        T: Ord,
    {
        match self.values {
            ValuesInner::SmallVec(ref v) => v.first(),
            ValuesInner::BTreeMap { ref map, .. } => map.first_key_value().map(|(wr, _)| &wr.value),
        }
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
        T: Clone + Ord + PartialEq,
    {
        match self.values {
            ValuesInner::SmallVec(ref v) => v.contains(value),
            ValuesInner::BTreeMap { ref map, .. } => map.contains_key(value),
        }
    }

    /// Inserts an element at position index within the vector, shifting all elements after it to
    /// the right.
    pub(crate) fn insert(&mut self, value: T, index: &mut Option<usize>, timestamp: Instant)
    where
        T: Ord + Clone,
    {
        // Always insert values in sorted order, even if no ordering method is provided,
        // otherwise it will require a linear scan to remove a value
        let Self { values, order, .. } = self;

        match values {
            ValuesInner::SmallVec(ref mut v) if v.len() < Self::VEC_MAX => {
                Self::find(v, order, &value, index, true);
                Arc::make_mut(v).insert(index.unwrap(), value);
            }
            ValuesInner::SmallVec(_) => {
                // Switch to btree to limit the O(n^2) inserts to the sorted vector.
                let mut vals = ValuesInner::new_btree();
                std::mem::swap(&mut vals, values);

                // collect would drop duplicates
                for val in vals.smallvec_iter() {
                    self.insert(val.clone(), index, timestamp);
                }

                self.insert(value, index, timestamp);
            }
            ValuesInner::BTreeMap {
                ref mut map,
                ref mut len,
            } => {
                match map.get_mut(&value) {
                    None => {
                        map.insert(BTreeValue::new(value, self.order.clone()), 1);
                    }
                    Some(count) => *count += 1,
                }
                *len += 1;
            }
        }
        self.metrics.update(timestamp);
    }

    /// Removes the element at position index within the vector, shifting all elements after it to
    /// the left.
    ///
    /// Note: Because this shifts over the remaining elements, it has a worst-case
    /// performance of O(n).
    pub(crate) fn remove(&mut self, value: &T, index: &mut Option<usize>, timestamp: Instant)
    where
        T: Ord + Clone,
    {
        let Self { values, order, .. } = self;

        match values {
            ValuesInner::SmallVec(ref mut v) => {
                Self::find(v, order, value, index, false);
                if let Some(index) = *index {
                    Arc::make_mut(v).remove(index);
                }
            }
            ValuesInner::BTreeMap {
                ref mut map,
                ref mut len,
            } => match map.get_mut(value) {
                None => (),
                Some(1) => {
                    map.remove(value);
                    *len -= 1;
                }
                Some(count) => {
                    *count -= 1;
                    *len -= 1;
                }
            },
        };
        self.metrics.update(timestamp);
    }

    pub(crate) fn clear(&mut self)
    where
        T: Clone,
    {
        match self.values {
            ValuesInner::SmallVec(ref mut v) => Arc::make_mut(v).clear(),
            ValuesInner::BTreeMap {
                ref mut map,
                ref mut len,
            } => {
                map.clear();
                *len = 0;
            }
        }
    }

    /// Returns the values as a SmallVec.  If the internal storage is a SmallVec, this merely
    /// clones an Arc.  If not, the stored values are copied into a new SmallVec.
    pub fn to_shared_smallvec(&self) -> Arc<SmallVec<[T; 1]>>
    where
        T: Clone,
    {
        match self.values {
            ValuesInner::SmallVec(ref v) => Arc::clone(v),
            ValuesInner::BTreeMap { ref map, .. } => Arc::new(
                map.iter()
                    .flat_map(|(wrapped, count)| {
                        std::iter::repeat(wrapped.value.clone()).take(*count)
                    })
                    .collect(),
            ),
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
            ValuesInner::BTreeMap { .. } => unreachable!(),
        }
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
        assert!(values.contains(v.first().unwrap()));

        v.clear();

        assert_empty!(v);
    }

    #[test]
    fn duplicate_values_btree() {
        const ROWS: usize = 10 * TestValues::VEC_MAX;

        type TestValues = Values<i32, DefaultInsertionOrder>;

        let mut v = TestValues::default();
        for _ in 0..ROWS {
            v.insert(1, &mut None, Instant::now());
        }
        assert_eq!(v.to_shared_smallvec().len(), ROWS);

        v.remove(&1, &mut None, Instant::now());
        assert_eq!(v.to_shared_smallvec().len(), ROWS - 1);
    }
}
