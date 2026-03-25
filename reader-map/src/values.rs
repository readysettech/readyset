use std::cmp::Ordering;
use std::fmt::{self, Debug};
use std::mem::ManuallyDrop;
use std::ptr;
use std::time::{Duration, Instant};

use partial_map::InsertionOrder;
use smallvec::SmallVec;
use triomphe::Arc;

use crate::eviction::EvictionMeta;

/// When the ratio of batch size to existing values is below this threshold,
/// use binary search + insert/remove (O(k log n) comparisons + O(kn) memcpy)
/// instead of merge (O(n) pointer-chasing comparisons). The former is faster
/// when k << n because memcpy is cache-friendly while merge comparisons chase
/// heap pointers.
const MERGE_THRESHOLD: usize = 32;

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

    /// Backward merge of sorted `adds` into the existing sorted values, in-place.
    /// Uses `Arc::make_mut` to avoid allocation when the Arc is uniquely owned.
    fn do_merge_adds(&mut self, adds: Vec<T>, timestamp: Instant)
    where
        T: Ord + Clone,
    {
        let mut adds = ManuallyDrop::new(adds);
        let values = Arc::make_mut(&mut self.values);
        let old_len = values.len();
        let add_len = adds.len();
        let new_len = old_len + add_len;

        values.reserve(add_len);

        let base = values.as_mut_ptr();
        let adds_base = adds.as_ptr();
        let mut old = old_len.checked_sub(1);
        let mut add = add_len.checked_sub(1);

        // SAFETY: Backward merge filling each slot in [old_len..new_len] exactly once.
        // Existing elements are moved rightward via ptr::read/ptr::write (non-overlapping
        // because dst >= old always holds: the gap starts at add_len and shrinks by 1 only
        // when an add element is placed).  Add elements are moved from the adds buffer,
        // which is wrapped in ManuallyDrop to prevent double-drop.  When all adds are placed,
        // the remaining existing elements are already in position.
        #[allow(clippy::multiple_unsafe_ops_per_block)]
        unsafe {
            for dst in (0..new_len).rev() {
                let Some(a) = add else { break };
                match old {
                    Some(o)
                        if self.order.cmp(&*base.add(o), &*adds_base.add(a))
                            == Ordering::Greater =>
                    {
                        ptr::write(base.add(dst), ptr::read(base.add(o)));
                        old = o.checked_sub(1);
                    }
                    _ => {
                        ptr::write(base.add(dst), ptr::read(adds_base.add(a)));
                        add = a.checked_sub(1);
                    }
                }
            }
            values.set_len(new_len);

            // Free the adds buffer without dropping the elements (they were moved out above).
            adds.set_len(0);
            ManuallyDrop::drop(&mut adds);
        }

        self.metrics.update(timestamp);
    }

    /// In-place compaction that removes matching values from the existing sorted values.
    /// Uses `Arc::make_mut` to avoid allocation when the Arc is uniquely owned.
    fn do_merge_removes(&mut self, removes: &[T], timestamp: Instant)
    where
        T: Ord + Clone,
    {
        let values = Arc::make_mut(&mut self.values);
        let mut rm = 0;
        let mut write = 0;

        'outer: for read in 0..values.len() {
            while rm < removes.len() {
                match self.order.cmp(&values[read], &removes[rm]) {
                    Ordering::Greater => {
                        rm += 1;
                    }
                    Ordering::Equal => {
                        rm += 1;
                        continue 'outer;
                    }
                    Ordering::Less => break,
                }
            }
            values.swap(write, read);
            write += 1;
        }

        values.truncate(write);
        self.metrics.update(timestamp);
    }

    /// Insert sorted values using binary search + insert for each element.
    /// O(k * n) memcpy but only O(k * log n) comparisons — faster than merge when k << n
    /// because the shifts are sequential memcpy (cache-friendly) while merge comparisons
    /// chase pointers through heap-allocated values.
    fn do_individual_adds(&mut self, adds: &[T], timestamp: Instant)
    where
        T: Ord + Clone,
    {
        let values = Arc::make_mut(&mut self.values);
        for value in adds {
            let pos = values
                .binary_search_by(|x| self.order.cmp(x, value))
                .unwrap_or_else(|x| x);
            values.insert(pos, value.clone());
        }
        self.metrics.update(timestamp);
    }

    /// Remove sorted values using binary search for each element.
    /// O(k * n) memcpy but only O(k * log n) comparisons.
    fn do_individual_removes(&mut self, removes: &[T], timestamp: Instant)
    where
        T: Ord + Clone,
    {
        let values = Arc::make_mut(&mut self.values);
        for value in removes {
            if let Ok(pos) = values.binary_search_by(|x| self.order.cmp(x, value)) {
                values.remove(pos);
            }
        }
        self.metrics.update(timestamp);
    }

    /// Merge sorted adds into the existing values.
    /// Uses merge for large batches, binary search + insert for small batches.
    /// Accepts `&[T]` (clones into a Vec) or `Vec<T>` (no-op move).
    pub(crate) fn merge_sorted_adds(&mut self, sorted: impl Into<Vec<T>>, timestamp: Instant)
    where
        T: Ord + Clone,
    {
        let sorted = sorted.into();
        if sorted.is_empty() {
            return;
        }
        // When adds are small relative to existing values, binary search + insert is faster:
        // merge does O(n) pointer-chasing comparisons, while individual inserts do
        // O(k * log n) comparisons + O(k * n) cache-friendly memcpy.
        if !self.values.is_empty() && sorted.len() * MERGE_THRESHOLD < self.values.len() {
            self.do_individual_adds(&sorted, timestamp);
        } else {
            self.do_merge_adds(sorted, timestamp);
        }
    }

    /// Remove sorted values from the existing values.
    /// Uses merge for large batches, binary search + remove for small batches.
    /// Accepts `&[T]` (clones into a Vec) or `Vec<T>` (no-op move).
    pub(crate) fn merge_sorted_removes(&mut self, sorted: impl Into<Vec<T>>, timestamp: Instant)
    where
        T: Ord + Clone,
    {
        let sorted = sorted.into();
        if sorted.is_empty() {
            return;
        }
        if !self.values.is_empty() && sorted.len() * MERGE_THRESHOLD < self.values.len() {
            self.do_individual_removes(&sorted, timestamp);
        } else {
            self.do_merge_removes(&sorted, timestamp);
        }
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
