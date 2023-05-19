use std::collections::hash_map::RandomState;
use std::fmt;
use std::hash::{BuildHasher, Hash};
use std::ops::{Bound, RangeBounds};

use left_right::Absorb;
use partial_map::InsertionOrder;
use readyset_client::internal::IndexType;

use crate::eviction::EvictionMeta;
use crate::inner::Inner;
use crate::read::ReadHandle;
use crate::values::Values;

/// A representation of how many keys to evict from the map.
#[derive(Debug)]
pub enum EvictionQuantity {
    /// The number of keys to evict
    Quantity(usize),
    /// The ratio of keys to evict on [0, 1]
    Ratio(f64),
}

/// A handle that may be used to modify the eventually consistent map.
///
/// Note that any changes made to the map will not be made visible to readers until
/// [`publish`](Self::publish) is called.
///
/// When the `WriteHandle` is dropped, the map is immediately (but safely) taken away from all
/// readers, causing all future lookups to return `None`.
///
/// # Examples
/// ```
/// let x = ('x', 42);
///
/// let (mut w, r) = reader_map::new();
///
/// // the map is uninitialized, so all lookups should return Err(NotPublished)
/// assert_eq!(r.get(&x.0).err().unwrap(), reader_map::Error::NotPublished);
///
/// w.publish();
///
/// // after the first publish, it is empty, but ready
/// assert!(r.get(&x.0).unwrap().is_none());
///
/// w.insert(x.0, x);
///
/// // it is empty even after an add (we haven't publish yet)
/// assert!(r.get(&x.0).unwrap().is_none());
///
/// w.publish();
///
/// // but after the swap, the record is there!
/// assert_eq!(r.get(&x.0).unwrap().map(|rs| rs.len()), Some(1));
/// assert_eq!(
///     r.get(&x.0)
///         .unwrap()
///         .map(|rs| rs.iter().any(|v| v.0 == x.0 && v.1 == x.1)),
///     Some(true)
/// );
/// ```
pub struct WriteHandle<K, V, I, M = (), T = (), S = RandomState>
where
    K: Ord + Hash + Clone,
    S: BuildHasher + Clone,
    V: Ord + Clone,
    M: 'static + Clone,
    T: Clone,
    I: InsertionOrder<V>,
{
    handle: left_right::WriteHandle<Inner<K, V, M, T, S, I>, Operation<K, V, M, T>>,
    r_handle: ReadHandle<K, V, I, M, T, S>,
}

impl<K, V, I, M, T, S> fmt::Debug for WriteHandle<K, V, I, M, T, S>
where
    K: Ord + Hash + Clone + fmt::Debug,
    S: BuildHasher + Clone + fmt::Debug,
    V: Ord + Clone + fmt::Debug,
    M: 'static + Clone + fmt::Debug,
    T: Clone + fmt::Debug,
    I: InsertionOrder<V>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WriteHandle")
            .field("handle", &self.handle)
            .finish()
    }
}

impl<K, V, I, M, T, S> WriteHandle<K, V, I, M, T, S>
where
    K: Ord + Hash + Clone,
    S: BuildHasher + Clone,
    V: Ord + Clone,
    M: 'static + Clone,
    T: Clone,
    I: InsertionOrder<V>,
{
    pub(crate) fn new(
        handle: left_right::WriteHandle<Inner<K, V, M, T, S, I>, Operation<K, V, M, T>>,
    ) -> Self {
        let r_handle = ReadHandle::new(left_right::ReadHandle::clone(&*handle));
        Self { handle, r_handle }
    }

    /// Returns the base size of inner data associated with this write handle.
    pub fn base_value_size(&self) -> usize {
        std::mem::size_of::<Values<V>>()
    }

    /// Publish all changes since the last call to `publish` to make them visible to readers.
    ///
    /// This can take some time, especially if readers are executing slow operations, or if there
    /// are many of them.
    pub fn publish(&mut self) -> &mut Self {
        self.handle.publish();
        self
    }

    /// Returns true if there are changes to the map that have not yet been exposed to readers.
    pub fn has_pending(&self) -> bool {
        self.handle.has_pending_operations()
    }

    /// Set the metadata.
    ///
    /// Will only be visible to readers after the next call to [`publish`](Self::publish).
    pub fn set_meta(&mut self, meta: M) {
        self.add_op(Operation::SetMeta(meta));
    }

    /// Set the timestamp
    ///
    /// Will only be visible to readesr after the next call to ['publish']
    pub fn set_timestamp(&mut self, timestamp: T) {
        self.add_op(Operation::SetTimestamp(timestamp));
    }

    fn add_ops<IT>(&mut self, ops: IT) -> &mut Self
    where
        IT: IntoIterator<Item = Operation<K, V, M, T>>,
    {
        self.handle.extend(ops);
        self
    }

    fn add_op(&mut self, op: Operation<K, V, M, T>) -> &mut Self {
        self.add_ops(vec![op])
    }

    /// Add the given value to the value-bag of the given key.
    ///
    /// The updated value-bag will only be visible to readers after the next call to
    /// [`publish`](Self::publish).
    pub fn insert(&mut self, k: K, v: V) -> &mut Self {
        self.add_op(Operation::Add(k, v, None, None))
    }

    /// Add the list of `records` to the value-set, which are assumed to have a key part of the
    /// `range`. The `range` is then also inserted to the underlying interval tree, to keep
    /// track of which intervals are covered by the evmap.
    ///
    /// The update value-set will only be visible to readers after the next call to `refresh()`.
    pub fn insert_range<R>(&mut self, range: R) -> &mut Self
    where
        R: RangeBounds<K>,
    {
        self.add_op(Operation::AddRange((
            range.start_bound().cloned(),
            range.end_bound().cloned(),
        )))
    }

    /// Clear the value-bag of the given key, without removing it.
    ///
    /// If a value-bag already exists, this will clear it but leave the
    /// allocated memory intact for reuse, or if no associated value-bag exists
    /// an empty value-bag will be created for the given key.
    ///
    /// The new value will only be visible to readers after the next call to
    /// [`publish`](Self::publish).
    pub fn clear(&mut self, k: K) -> &mut Self {
        self.add_op(Operation::Clear(k, None))
    }

    /// Remove the given value from the value-bag of the given key.
    ///
    /// The updated value-bag will only be visible to readers after the next call to
    /// [`publish`](Self::publish).
    pub fn remove_value(&mut self, k: K, v: V) -> &mut Self {
        self.add_op(Operation::RemoveValue(k, v, None))
    }

    /// Remove the value-bag for the given key.
    ///
    /// The value-bag will only disappear from readers after the next call to
    /// [`publish`](Self::publish).
    #[deprecated(since = "11.0.0", note = "Renamed to remove_entry")]
    pub fn empty(&mut self, k: K) -> &mut Self {
        self.remove_entry(k)
    }

    /// Remove the value-bag for the given key.
    ///
    /// The value-bag will only disappear from readers after the next call to
    /// [`publish`](Self::publish).
    pub fn remove_entry(&mut self, k: K) -> &mut Self {
        self.add_op(Operation::RemoveEntry(k))
    }

    /// Remove all the entries for keys in the given range.
    ///
    /// The entries will only disappear from readers after the next call to
    /// [`publish`](Self::publish).
    pub fn remove_range(&mut self, range: (Bound<K>, Bound<K>)) -> &mut Self {
        self.add_op(Operation::RemoveRange(range))
    }

    /// Purge all value-bags from the map.
    ///
    /// The map will only appear empty to readers after the next call to
    /// [`publish`](Self::publish).
    ///
    /// Note that this will iterate once over all the keys internally.
    pub fn purge(&mut self) -> &mut Self {
        self.add_op(Operation::Purge)
    }

    /// Retain elements for the given key using the provided predicate function.
    ///
    /// The removed  values will only become inaccesible to readers after the next call to
    /// [`publish`](Self::publish)
    pub fn retain<F>(&mut self, k: K, f: F) -> &mut Self
    where
        F: FnMut(&V, bool) -> bool + 'static + Send,
    {
        self.add_op(Operation::Retain(k, Predicate(Box::new(f))))
    }

    /// Remove the value-bag for randomly chosen keys given an `EvictionQuantity` to evict.
    ///
    /// This method immediately calls [`publish`](Self::publish) to ensure that the keys and values
    /// it returns match the elements that will be emptied on the next call to
    /// [`publish`](Self::publish). The values will be submitted for eviction, but the result will
    /// only be visible to all readers after a following call to publish is made. The method returns
    /// the amount of memory freed, computed using the provided closure on each (K,V) pair.
    pub fn evict_keys<'a, F>(&'a mut self, keys_to_evict: EvictionQuantity, mut mem_cnt: F) -> u64
    where
        F: FnMut(&K, &Values<V>) -> u64,
    {
        self.publish();

        let inner = self
            .r_handle
            .handle
            .raw_handle()
            .expect("WriteHandle has not been dropped");
        // safety: the writer cannot publish until 'a ends, so we know that reading from the read
        // map is safe for the duration of 'a.
        let inner: &'a Inner<K, V, M, T, S, I> =
            unsafe { std::mem::transmute::<&Inner<K, V, M, T, S, I>, _>(inner.as_ref()) };

        let keys_to_evict = match keys_to_evict {
            EvictionQuantity::Ratio(ratio) => (inner.data.len() as f64 * ratio) as usize,
            EvictionQuantity::Quantity(keys) => keys,
        }
        .min(inner.data.len());

        let mut mem_freed = 0;

        match inner.data.index_type() {
            IndexType::BTreeMap => {
                let mut range_iterator = inner
                    .eviction_strategy
                    .pick_ranges_to_evict(&inner.data, keys_to_evict);

                while let Some(subrange_iter) = range_iterator.next_range() {
                    let mut subrange_iter = subrange_iter.map(|(k, v)| {
                        mem_freed += mem_cnt(k, v);
                        (k, v)
                    });

                    let (start, _) = subrange_iter.next().expect("Subrange can't be empty");
                    let end = match subrange_iter.last() {
                        None => start,
                        Some((end, _)) => end,
                    };

                    self.add_op(Operation::RemoveRange((
                        Bound::Included(start.clone()),
                        Bound::Included(end.clone()),
                    )));
                }
            }
            IndexType::HashMap => {
                let kvs = inner
                    .eviction_strategy
                    .pick_keys_to_evict(&inner.data, keys_to_evict);

                for (k, v) in kvs {
                    self.add_op(Operation::RemoveEntry(k.clone()));
                    mem_freed += mem_cnt(k, v);
                }
            }
        }

        mem_freed
    }
}

impl<K, V, M, T, S, I> Absorb<Operation<K, V, M, T>> for Inner<K, V, M, T, S, I>
where
    K: Ord + Hash + Clone,
    S: BuildHasher + Clone,
    V: Ord + Clone,
    M: 'static + Clone,
    T: Clone,
    I: InsertionOrder<V>,
{
    /// Apply ops in such a way that no values are dropped, only forgotten
    fn absorb_first(&mut self, op: &mut Operation<K, V, M, T>, other: &Self) {
        match op {
            Operation::Add(key, value, eviction_meta, idx) => {
                let values = self.data_entry(key.clone(), eviction_meta);
                // Always insert values in sorted order, even if no ordering method is provided,
                // otherwise it will require a linear scan to remove a value
                let insert_idx = if let Some(insertion_order) = &other.insertion_order {
                    insertion_order.get_insertion_order(values, value)
                } else {
                    values.binary_search(value)
                }
                .unwrap_or_else(|i| i);
                values.insert(insert_idx, value.clone());
                *idx = Some(insert_idx);
            }
            Operation::RemoveValue(key, value, idx) => {
                // Because elements are always in sorted order, it is possible to remove the element
                // using binary search
                if let Some(e) = self.data.get_mut(key) {
                    let remove_idx = if let Some(insertion_order) = &other.insertion_order {
                        insertion_order.get_insertion_order(e, value)
                    } else {
                        e.binary_search(value)
                    };
                    if let Ok(remove_idx) = remove_idx {
                        e.remove(remove_idx);
                        *idx = Some(remove_idx);
                    }
                }
            }
            Operation::AddRange(range) => self.data.add_range(range.clone()),
            Operation::Clear(key, eviction_meta) => {
                self.data_entry(key.clone(), eviction_meta).clear()
            }
            Operation::RemoveEntry(key) => {
                self.data.remove(key);
            }
            Operation::Purge => self.data.clear(),
            Operation::RemoveRange(range) => self.data.remove_range(range.clone()),
            Operation::Retain(key, predicate) => {
                if let Some(e) = self.data.get_mut(key) {
                    let mut first = true;
                    e.retain(move |v| {
                        let retain = predicate.eval(v, first);
                        first = false;
                        retain
                    });
                }
            }
            Operation::MarkReady => {
                self.ready = true;
            }
            Operation::SetMeta(m) => {
                self.meta = m.clone();
            }
            Operation::SetTimestamp(t) => {
                self.timestamp = t.clone();
            }
        }
    }

    /// Apply operations while allowing dropping of values
    fn absorb_second(&mut self, op: Operation<K, V, M, T>, other: &Self) {
        match op {
            Operation::Add(key, value, mut eviction_meta, idx) => {
                let values = self.data_entry(key, &mut eviction_meta);
                let insert_idx = match idx {
                    // In `absorb_second` there is no need to do binary search again if it was
                    // already passed over by `absorb_first`
                    Some(idx) => idx,
                    // This only happens before the initial publish
                    None => match &other.insertion_order {
                        Some(order) => order.get_insertion_order(values, &value),
                        None => values.binary_search(&value),
                    }
                    .unwrap_or_else(|i| i),
                };
                values.insert(insert_idx, value);
            }
            Operation::RemoveValue(key, value, idx) => {
                if let Some(e) = self.data.get_mut(&key) {
                    let remove_idx = match idx {
                        // In `absorb_second` there is no need to do binary search again if it was
                        // already passed over by `absorb_first`
                        Some(idx) => Ok(idx),
                        // This only happens before the initial publish or if value is not present,
                        // but in real world usage value is always present
                        None => match &other.insertion_order {
                            Some(order) => order.get_insertion_order(e, &value),
                            None => e.binary_search(&value),
                        },
                    };
                    if let Ok(remove_idx) = remove_idx {
                        e.remove(remove_idx);
                    }
                }
            }
            Operation::AddRange(range) => self.data.add_range(range),
            Operation::Clear(key, mut eviction_meta) => {
                self.data_entry(key, &mut eviction_meta).clear()
            }
            Operation::RemoveEntry(key) => {
                self.data.remove(&key);
            }
            Operation::RemoveRange(range) => self.data.remove_range(range),
            Operation::Purge => self.data.clear(),
            Operation::Retain(key, mut predicate) => {
                if let Some(e) = self.data.get_mut(&key) {
                    let mut first = true;
                    e.retain(move |v| {
                        let retain = predicate.eval(&*v, first);
                        first = false;
                        retain
                    });
                }
            }
            Operation::MarkReady => {
                self.ready = true;
            }
            Operation::SetMeta(m) => {
                self.meta = m;
            }
            Operation::SetTimestamp(t) => {
                self.timestamp = t;
            }
        }
    }

    fn sync_with(&mut self, first: &Self) {
        self.data = first.data.clone();
        self.ready = first.ready;
    }
}

// allow using write handle for reads
use std::ops::Deref;
impl<K, V, I, M, T, S> Deref for WriteHandle<K, V, I, M, T, S>
where
    K: Ord + Clone + Hash,
    S: BuildHasher + Clone,
    V: Ord + Clone,
    M: 'static + Clone,
    T: Clone,
    I: InsertionOrder<V>,
{
    type Target = ReadHandle<K, V, I, M, T, S>;
    fn deref(&self) -> &Self::Target {
        &self.r_handle
    }
}

/// A pending map operation.
#[non_exhaustive]
pub(super) enum Operation<K, V, M, T> {
    /// Add this value to the set of entries for this key. Last element of the tuple is the
    /// insertion index for [`absorb_second`] and computed in [`absorb_first`]
    Add(K, V, Option<EvictionMeta>, Option<usize>),
    /// Add an interval to the list of filled intervals
    AddRange((Bound<K>, Bound<K>)),
    /// Remove this value from the set of entries for this key. Last element of the tuple is the
    /// removal index for [`absorb_second`] and computed in [`absorb_first`]
    RemoveValue(K, V, Option<usize>),
    /// Remove the value set for this key.
    RemoveEntry(K),
    /// Remove all entries in the given range
    RemoveRange((Bound<K>, Bound<K>)),
    /// Remove all values in the value set for this key.
    Clear(K, Option<EvictionMeta>),
    /// Remove all values for all keys.
    ///
    /// Note that this will iterate once over all the keys internally.
    Purge,
    /// Retains all values matching the given predicate.
    Retain(K, Predicate<V>),
    /// Mark the map as ready to be consumed for readers.
    MarkReady,
    /// Set the value of the map meta.
    SetMeta(M),
    /// Set the value of the timestamp of the current values in the map.
    SetTimestamp(T),
}

impl<K, V, M, T> fmt::Debug for Operation<K, V, M, T>
where
    K: fmt::Debug,
    V: fmt::Debug,
    M: fmt::Debug,
    T: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Operation::Add(a, b, c, _) => f.debug_tuple("Add").field(a).field(b).field(c).finish(),
            Operation::AddRange(a) => f.debug_tuple("AddRange").field(a).finish(),
            Operation::RemoveValue(a, b, _) => {
                f.debug_tuple("RemoveValue").field(a).field(b).finish()
            }
            Operation::RemoveRange(range) => f.debug_tuple("RemoveRange").field(range).finish(),
            Operation::RemoveEntry(a) => f.debug_tuple("RemoveEntry").field(a).finish(),
            Operation::Clear(a, _) => f.debug_tuple("Clear").field(a).finish(),
            Operation::Purge => f.debug_tuple("Purge").finish(),
            Operation::Retain(a, b) => f.debug_tuple("Retain").field(a).field(b).finish(),
            Operation::MarkReady => f.debug_tuple("MarkReady").finish(),
            Operation::SetMeta(a) => f.debug_tuple("SetMeta").field(a).finish(),
            Operation::SetTimestamp(a) => f.debug_tuple("SetTimestamp").field(a).finish(),
        }
    }
}

/// Unary predicate used to retain elements.
///
/// The predicate function is called once for each distinct value, and `true` if this is the
/// _first_ call to the predicate on the _second_ application of the operation.
pub(super) struct Predicate<V: ?Sized>(Box<dyn FnMut(&V, bool) -> bool + Send>);

impl<V: ?Sized> Predicate<V> {
    /// Evaluate the predicate for the given element
    #[inline]
    fn eval(&mut self, value: &V, reset: bool) -> bool {
        (*self.0)(value, reset)
    }
}

impl<V: ?Sized> PartialEq for Predicate<V> {
    #[inline]
    #[allow(clippy::ptr_eq)]
    fn eq(&self, other: &Self) -> bool {
        // only compare data, not vtable: https://stackoverflow.com/q/47489449/472927
        &*self.0 as *const _ as *const () == &*other.0 as *const _ as *const ()
    }
}

impl<V: ?Sized> Eq for Predicate<V> {}

impl<V: ?Sized> fmt::Debug for Predicate<V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Predicate")
            .field(&format_args!("{:p}", &*self.0 as *const _))
            .finish()
    }
}
