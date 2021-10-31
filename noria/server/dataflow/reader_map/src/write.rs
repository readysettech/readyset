use crate::inner::{Entry, Inner};
use crate::read::ReadHandle;
use crate::values::ValuesInner;
use left_right::{aliasing::Aliased, Absorb};

use rand::prelude::IteratorRandom;
use std::collections::hash_map::RandomState;
use std::fmt;
use std::hash::{BuildHasher, Hash};
use std::ops::{Bound, RangeBounds};

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
/// // the map is uninitialized, so all lookups should return None
/// assert_eq!(r.get(&x.0).map(|rs| rs.len()), None);
///
/// w.publish();
///
/// // after the first publish, it is empty, but ready
/// assert_eq!(r.get(&x.0).map(|rs| rs.len()), None);
///
/// w.insert(x.0, x);
///
/// // it is empty even after an add (we haven't publish yet)
/// assert_eq!(r.get(&x.0).map(|rs| rs.len()), None);
///
/// w.publish();
///
/// // but after the swap, the record is there!
/// assert_eq!(r.get(&x.0).map(|rs| rs.len()), Some(1));
/// assert_eq!(r.get(&x.0).map(|rs| rs.iter().any(|v| v.0 == x.0 && v.1 == x.1)), Some(true));
/// ```
pub struct WriteHandle<K, V, M = (), T = (), S = RandomState>
where
    K: Ord + Clone + Hash,
    S: BuildHasher + Clone,
    V: Eq + Hash,
    M: 'static + Clone,
    T: Clone,
{
    handle: left_right::WriteHandle<Inner<K, V, M, T, S>, Operation<K, V, M, T>>,
    r_handle: ReadHandle<K, V, M, T, S>,

    /// If Some, write directly to the write handle map, since no publish has happened.
    /// Some(false) indicates that the necessary `Operation::JustCloneRHandle` has not
    /// yet been appended to the oplog for when a publish does happen.
    direct_write: Option<bool>,
}

impl<K, V, M, T, S> fmt::Debug for WriteHandle<K, V, M, T, S>
where
    K: Ord + Clone + Hash + fmt::Debug,
    S: BuildHasher + Clone,
    V: Eq + Hash + fmt::Debug,
    M: 'static + Clone + fmt::Debug,
    T: Clone + fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WriteHandle")
            .field("handle", &self.handle)
            .field("direct_write", &self.direct_write)
            .finish()
    }
}

impl<K, V, M, T, S> WriteHandle<K, V, M, T, S>
where
    K: Ord + Clone + Hash,
    S: BuildHasher + Clone,
    V: Eq + Hash,
    M: 'static + Clone,
    T: Clone,
{
    pub(crate) fn new(
        handle: left_right::WriteHandle<Inner<K, V, M, T, S>, Operation<K, V, M, T>>,
    ) -> Self {
        let r_handle = ReadHandle::new(left_right::ReadHandle::clone(&*handle));
        Self {
            handle,
            r_handle,
            direct_write: Some(false),
        }
    }

    /// Returns the base size of inner data associated with this write handle.
    pub fn base_value_size(&self) -> usize {
        std::mem::size_of::<ValuesInner<V, S, crate::aliasing::NoDrop>>()
    }

    /// Publish all changes since the last call to `publish` to make them visible to readers.
    ///
    /// This can take some time, especially if readers are executing slow operations, or if there
    /// are many of them.
    pub fn publish(&mut self) -> &mut Self {
        self.handle.publish();
        self.direct_write = None;
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

    fn add_ops<I>(&mut self, ops: I) -> &mut Self
    where
        I: IntoIterator<Item = Operation<K, V, M, T>>,
    {
        if let Some(ref mut queued_clone) = self.direct_write {
            {
                // Safety: we know there are no outstanding w_handle readers, since we haven't
                // refreshed ever before, so we can modify it directly!
                let mut w_inner = self.handle.raw_write_handle();
                let w_inner = unsafe { w_inner.as_mut() };
                let r_handle = self.handle.enter().expect("map has not yet been destroyed");
                // Because we are operating directly on the map, and nothing is aliased, we do want
                // to perform drops, so we invoke absorb_second.
                for op in ops {
                    Absorb::absorb_second(w_inner, op, &*r_handle);
                }
            }

            if !*queued_clone {
                // NOTE: since we didn't record this in the oplog, r_handle *must* clone w_handle
                self.handle.append(Operation::JustCloneRHandle);
                *queued_clone = true;
            }
        } else {
            // TODO(grfn): Once https://github.com/jonhoo/rust-evmap/pull/86 is merged this can use
            // `extend`
            for op in ops {
                self.handle.append(op);
            }
        }
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
        self.add_op(Operation::Add(k, Aliased::from(v)))
    }

    /// Add the list of `records` to the value-set, which are assumed to have a key part of the `range`.
    /// The `range` is then also inserted to the underlying interval tree, to keep track
    /// of which intervals are covered by the evmap.
    ///
    /// The update value-set will only be visible to readers after the next call to `refresh()`.
    pub fn insert_range<R>(&mut self, records: Vec<(K, V)>, range: R) -> &mut Self
    where
        R: RangeBounds<K>,
    {
        self.add_op(Operation::AddRange(
            records
                .into_iter()
                .map(|(k, v)| (k, Aliased::from(v)))
                .collect(),
            (range.start_bound().cloned(), range.end_bound().cloned()),
        ))
    }

    /// Replace the value-set of the given key with the given value.
    ///
    /// Replacing the value will automatically deallocate any heap storage and place the new value
    /// back into the `SmallVec` inline storage. This can improve cache locality for common
    /// cases where the value-bag is only ever a single element.
    ///
    /// See [the doc section on this](./index.html#small-vector-optimization) for more information.
    ///
    /// The new value will only be visible to readers after the next call to
    /// [`publish`](Self::publish).
    pub fn update(&mut self, k: K, v: V) -> &mut Self {
        self.add_op(Operation::Replace(k, Aliased::from(v)))
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
        self.add_op(Operation::Clear(k))
    }

    /// Remove the given value from the value-bag of the given key.
    ///
    /// The updated value-bag will only be visible to readers after the next call to
    /// [`publish`](Self::publish).
    #[deprecated(since = "11.0.0", note = "Renamed to remove_value")]
    pub fn remove(&mut self, k: K, v: V) -> &mut Self {
        self.remove_value(k, v)
    }

    /// Remove the given value from the value-bag of the given key.
    ///
    /// The updated value-bag will only be visible to readers after the next call to
    /// [`publish`](Self::publish).
    pub fn remove_value(&mut self, k: K, v: V) -> &mut Self {
        self.add_op(Operation::RemoveValue(k, v))
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
    pub fn remove_range<R>(&mut self, range: R) -> &mut Self
    where
        R: RangeBounds<K>,
    {
        self.add_op(Operation::RemoveRange((
            range.start_bound().cloned(),
            range.end_bound().cloned(),
        )))
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
    /// The remaining value-bag will only be visible to readers after the next call to
    /// [`publish`](Self::publish)
    ///
    /// # Safety
    ///
    /// The given closure is called _twice_ for each element, once when called, and once
    /// on swap. It _must_ retain the same elements each time, otherwise a value may exist in one
    /// map, but not the other, leaving the two maps permanently out-of-sync. This is _really_ bad,
    /// as values are aliased between the maps, and are assumed safe to free when they leave the
    /// map during a `publish`. Returning `true` when `retain` is first called for a value, and
    /// `false` the second time would free the value, but leave an aliased pointer to it in the
    /// other side of the map.
    ///
    /// The arguments to the predicate function are the current value in the value-bag, and `true`
    /// if this is the first value in the value-bag on the second map, or `false` otherwise. Use
    /// the second argument to know when to reset any closure-local state to ensure deterministic
    /// operation.
    ///
    /// So, stated plainly, the given closure _must_ return the same order of true/false for each
    /// of the two iterations over the value-bag. That is, the sequence of returned booleans before
    /// the second argument is true must be exactly equal to the sequence of returned booleans
    /// at and beyond when the second argument is true.
    pub unsafe fn retain<F>(&mut self, k: K, f: F) -> &mut Self
    where
        F: FnMut(&V, bool) -> bool + 'static + Send,
    {
        self.add_op(Operation::Retain(k, Predicate(Box::new(f))))
    }

    /// Shrinks a value-bag to it's minimum necessary size, freeing memory
    /// and potentially improving cache locality by switching to inline storage.
    ///
    /// The optimized value-bag will only be visible to readers after the next call to
    /// [`publish`](Self::publish)
    pub fn fit(&mut self, k: K) -> &mut Self {
        self.add_op(Operation::Fit(Some(k)))
    }

    /// Like [`WriteHandle::fit`](#method.fit), but shrinks <b>all</b> value-bags in the map.
    ///
    /// The optimized value-bags will only be visible to readers after the next call to
    /// [`publish`](Self::publish)
    pub fn fit_all(&mut self) -> &mut Self {
        self.add_op(Operation::Fit(None))
    }

    /// Reserves capacity for some number of additional elements in a value-bag,
    /// or creates an empty value-bag for this key with the given capacity if
    /// it doesn't already exist.
    ///
    /// Readers are unaffected by this operation, but it can improve performance
    /// by pre-allocating space for large value-bags.
    pub fn reserve(&mut self, k: K, additional: usize) -> &mut Self {
        self.add_op(Operation::Reserve(k, additional))
    }

    /// Remove the value-bag for randomly chosen keys in an attempt to evict `bytes`.
    ///
    /// This method immediately calls [`publish`](Self::publish) to ensure that the keys and values
    /// it returns match the elements that will be emptied on the next call to
    /// [`publish`](Self::publish). The value-bags will only disappear from readers after the next
    /// call to [`publish`](Self::publish).
    pub fn empty_random<'a>(
        &'a mut self,
        rng: &mut impl rand::Rng,
        bytes: usize,
    ) -> impl ExactSizeIterator<Item = (&'a K, &'a crate::values::Values<V, S>)> {
        // force a publish so that our view into self.r_handle matches the indices we choose.
        // if we didn't do this, the `i`th element of r_handle may be a completely different
        // element than the one that _will_ be evicted when `EmptyAt([.. i ..])` is applied.
        // this would be bad since we are telling the caller which elements we are evicting!
        // note also that we _must_ use `r_handle`, not `w_handle`, since `w_handle` may have
        // pending operations even after a publish!
        self.publish();

        let inner = self
            .r_handle
            .handle
            .raw_handle()
            .expect("WriteHandle has not been dropped");
        // safety: the writer cannot publish until 'a ends, so we know that reading from the read
        // map is safe for the duration of 'a.
        let inner: &'a Inner<K, V, M, T, S> =
            unsafe { std::mem::transmute::<&Inner<K, V, M, T, S>, _>(inner.as_ref()) };
        let inner = &inner.data;

        // Base value size is always greater than 0 so this cannot panic. Round this value
        // up using mod to avoid float conversion.
        let num_keys =
            bytes / self.base_value_size() + (bytes % self.base_value_size() != 0) as usize;
        let n = num_keys.min(inner.len());
        // let's pick some (distinct) indices to evict!
        let keys: Vec<&K> = inner.keys().choose_multiple(rng, n);

        self.add_ops(keys.iter().map(|k| Operation::RemoveEntry((*k).clone())));

        keys.into_iter().map(move |k| {
            let (k, vs) = inner.get_key_value(k).expect("yielded by keys()");
            (k, vs.as_ref())
        })
    }
}

impl<K, V, M, T, S> Absorb<Operation<K, V, M, T>> for Inner<K, V, M, T, S>
where
    K: Ord + Clone + Hash,
    S: BuildHasher + Clone,
    V: Eq + Hash,
    M: 'static + Clone,
    T: Clone,
{
    /// Apply ops in such a way that no values are dropped, only forgotten
    fn absorb_first(&mut self, op: &mut Operation<K, V, M, T>, other: &Self) {
        // Safety note for calls to .alias():
        //
        //   it is safe to alias this value here because if it is ever removed, one alias is always
        //   first dropped with NoDrop (in absorb_first), and _then_ the other (and only remaining)
        //   alias is dropped with DoDrop (in absorb_second). we won't drop the aliased value until
        //   _after_ absorb_second is called on this operation, so leaving an alias in the oplog is
        //   also safe.

        let hasher = &other.hasher;
        match *op {
            Operation::Replace(ref key, ref mut value) => {
                let vs = self
                    .data
                    .entry(key.clone())
                    .or_insert_with(ValuesInner::new);

                // truncate vector
                vs.clear();

                // implicit shrink_to_fit on replace op
                // so it will switch back to inline allocation for the subsequent push.
                vs.shrink_to_fit();

                vs.push(unsafe { value.alias() }, hasher);
            }
            Operation::Clear(ref key) => {
                self.data
                    .entry(key.clone())
                    .or_insert_with(ValuesInner::new)
                    .clear();
            }
            Operation::Add(ref key, ref mut value) => {
                self.data
                    .entry(key.clone())
                    .or_insert_with(ValuesInner::new)
                    .push(unsafe { value.alias() }, hasher);
            }
            Operation::AddRange(ref mut records, ref range) => {
                for (ref key, value) in records {
                    self.data
                        .entry(key.clone())
                        .or_insert_with(ValuesInner::new)
                        .push(unsafe { value.alias() }, hasher);
                }

                self.data.add_range(range.clone());
            }
            Operation::RemoveEntry(ref key) => {
                self.data.remove(key);
            }
            Operation::Purge => {
                self.data.clear();
            }
            Operation::RemoveValue(ref key, ref value) => {
                if let Some(e) = self.data.get_mut(key) {
                    e.swap_remove(value);
                }
            }
            Operation::RemoveRange(ref range) => {
                self.data.remove_range(range);
            }
            Operation::Retain(ref key, ref mut predicate) => {
                if let Some(e) = self.data.get_mut(key) {
                    let mut first = true;
                    e.retain(move |v| {
                        let retain = predicate.eval(v, first);
                        first = false;
                        retain
                    });
                }
            }
            Operation::Fit(ref key) => match key {
                Some(ref key) => {
                    if let Some(e) = self.data.get_mut(key) {
                        e.shrink_to_fit();
                    }
                }
                None => {
                    for value_set in self.data.values_mut() {
                        value_set.shrink_to_fit();
                    }
                }
            },
            Operation::Reserve(ref key, additional) => match self.data.entry(key.clone()) {
                Entry::Occupied(mut entry) => {
                    entry.get_mut().reserve(additional, hasher);
                }
                Entry::Vacant(entry) => {
                    entry.insert(ValuesInner::with_capacity_and_hasher(additional, hasher));
                }
            },
            Operation::MarkReady => {
                self.ready = true;
            }
            Operation::SetMeta(ref m) => {
                self.meta = m.clone();
            }
            Operation::SetTimestamp(ref t) => {
                self.timestamp = t.clone();
            }
            Operation::JustCloneRHandle => {
                // This is applying the operation to the original write handle,
                // which we already applied the first batch of operations to.
            }
        }
    }

    /// Apply operations while allowing dropping of values
    fn absorb_second(&mut self, op: Operation<K, V, M, T>, other: &Self) {
        // # Safety (for cast):
        //
        // See the module-level documentation for left_right::aliasing.
        // NoDrop and DoDrop are both private, therefore this cast is (likely) sound.
        //
        // # Safety (for NoDrop -> DoDrop):
        //
        // It is safe for us to drop values the second time each operation has been
        // performed, since if they are dropped here, they were also dropped in the first
        // application of the operation, which removed the only other alias.
        //
        // FIXME: This is where the non-determinism of Hash and PartialEq hits us (#78).
        let inner: &mut Inner<K, V, M, T, S, crate::aliasing::DoDrop> =
            unsafe { &mut *(self as *mut _ as *mut _) };

        // Safety note for calls to .change_drop():
        //
        //   we're turning a NoDrop into DoDrop, so we must be prepared for a drop.
        //   if absorb_first dropped its alias, then `value` is the only alias
        //   if absorb_first did not drop its alias, then `value` will not be dropped here either,
        //   and at the end of scope we revert to `NoDrop`, so all is well.
        let hasher = &other.hasher;
        match op {
            Operation::Replace(key, value) => {
                let v = inner.data.entry(key).or_insert_with(ValuesInner::new);
                v.clear();
                v.shrink_to_fit();

                v.push(unsafe { value.change_drop() }, hasher);
            }
            Operation::Clear(key) => {
                inner
                    .data
                    .entry(key)
                    .or_insert_with(ValuesInner::new)
                    .clear();
            }
            Operation::Add(key, value) => {
                // safety (below):
                //   we're turning a NoDrop into DoDrop, so we must be prepared for a drop.
                //   if absorb_first dropped the value, then `value` is the only alias
                //   if absorb_first did not drop the value, then `value` will not be dropped here
                //   either, and at the end of scope we revert to `NoDrop`, so all is well.
                inner
                    .data
                    .entry(key)
                    .or_insert_with(ValuesInner::new)
                    .push(unsafe { value.change_drop() }, hasher);
            }
            Operation::AddRange(records, range) => {
                for (key, value) in records {
                    inner
                        .data
                        .entry(key)
                        .or_insert_with(ValuesInner::new)
                        .push(unsafe { value.change_drop() }, hasher);
                }

                inner.data.add_range(range);
            }
            Operation::RemoveEntry(key) => {
                inner.data.remove(&key);
            }
            Operation::RemoveRange(range) => {
                inner.data.remove_range(&range);
            }
            Operation::Purge => {
                inner.data.clear();
            }
            Operation::RemoveValue(key, value) => {
                if let Some(e) = inner.data.get_mut(&key) {
                    // find the first entry that matches all fields
                    e.swap_remove(&value);
                }
            }
            Operation::Retain(key, mut predicate) => {
                if let Some(e) = inner.data.get_mut(&key) {
                    let mut first = true;
                    e.retain(move |v| {
                        let retain = predicate.eval(&*v, first);
                        first = false;
                        retain
                    });
                }
            }
            Operation::Fit(key) => match key {
                Some(ref key) => {
                    if let Some(e) = inner.data.get_mut(key) {
                        e.shrink_to_fit();
                    }
                }
                None => {
                    for value_set in inner.data.values_mut() {
                        value_set.shrink_to_fit();
                    }
                }
            },
            Operation::Reserve(key, additional) => match inner.data.entry(key) {
                Entry::Occupied(mut entry) => {
                    entry.get_mut().reserve(additional, hasher);
                }
                Entry::Vacant(entry) => {
                    entry.insert(ValuesInner::with_capacity_and_hasher(additional, hasher));
                }
            },
            Operation::MarkReady => {
                inner.ready = true;
            }
            Operation::SetMeta(m) => {
                inner.meta = m;
            }
            Operation::SetTimestamp(t) => {
                inner.timestamp = t;
            }
            Operation::JustCloneRHandle => {
                // This is applying the operation to the original read handle,
                // which is empty, and needs to copy over all data from the
                // write handle that we wrote to directly.

                // XXX: it really is too bad that we can't just .clone() the data here and save
                // ourselves a lot of re-hashing, re-bucketization, etc.
                inner.data.extend(other.data.iter().map(|(k, vs)| {
                    // # Safety (for aliasing):
                    //
                    // We are aliasing every value in the read map, and the oplog has no other
                    // pending operations (by the semantics of JustCloneRHandle). For any of the
                    // values we alias to be dropped, the operation that drops it must first be
                    // enqueued to the oplog, at which point it will _first_ go through
                    // absorb_first, which will remove the alias and leave only one alias left.
                    // Only after that, when that operation eventually goes through absorb_second,
                    // will the alias be dropped, and by that time it is the only value.
                    //
                    // # Safety (for NoDrop -> DoDrop):
                    //
                    // The oplog has only this one operation in it for the first call to `publish`,
                    // so we are about to turn the alias back into NoDrop.
                    (k.clone(), unsafe { ValuesInner::alias(vs, &other.hasher) })
                }));
                inner.data.clone_intervals_from(&other.data);
            }
        }
    }

    fn drop_first(self: Box<Self>) {
        // since the two copies are exactly equal, we need to make sure that we *don't* call the
        // destructors of any of the values that are in our map, as they'll all be called when the
        // last read handle goes out of scope. that's easy enough since none of them will be
        // dropped by default.
    }

    fn drop_second(self: Box<Self>) {
        // when the second copy is dropped is where we want to _actually_ drop all the values in
        // the map. we do this by setting the generic type to the one that causes drops to happen.
        //
        // safety: since we're going second, we know that all the aliases in the first map have
        // gone away, so all of our aliases must be the only ones.
        let inner: Box<Inner<K, V, M, T, S, crate::aliasing::DoDrop>> =
            unsafe { Box::from_raw(Box::into_raw(self) as *mut _ as *mut _) };
        drop(inner);
    }
}

impl<K, V, M, T, S> Extend<(K, V)> for WriteHandle<K, V, M, T, S>
where
    K: Ord + Clone + Hash,
    S: BuildHasher + Clone,
    V: Eq + Hash,
    M: 'static + Clone,
    T: Clone,
{
    fn extend<I: IntoIterator<Item = (K, V)>>(&mut self, iter: I) {
        for (k, v) in iter {
            self.insert(k, v);
        }
    }
}

// allow using write handle for reads
use std::ops::Deref;
impl<K, V, M, T, S> Deref for WriteHandle<K, V, M, T, S>
where
    K: Ord + Clone + Hash,
    S: BuildHasher + Clone,
    V: Eq + Hash,
    M: 'static + Clone,
    T: Clone,
{
    type Target = ReadHandle<K, V, M, T, S>;
    fn deref(&self) -> &Self::Target {
        &self.r_handle
    }
}

/// A pending map operation.
#[non_exhaustive]
pub(super) enum Operation<K, V, M, T> {
    /// Replace the set of entries for this key with this value.
    Replace(K, Aliased<V, crate::aliasing::NoDrop>),
    /// Add this value to the set of entries for this key.
    Add(K, Aliased<V, crate::aliasing::NoDrop>),
    /// Add a list of values for the given range of keys.
    AddRange(
        Vec<(K, Aliased<V, crate::aliasing::NoDrop>)>,
        (Bound<K>, Bound<K>),
    ),
    /// Remove this value from the set of entries for this key.
    RemoveValue(K, V),
    /// Remove the value set for this key.
    RemoveEntry(K),
    /// Remove all entries in the given range
    RemoveRange((Bound<K>, Bound<K>)),
    /// Remove all values in the value set for this key.
    Clear(K),
    /// Remove all values for all keys.
    ///
    /// Note that this will iterate once over all the keys internally.
    Purge,
    /// Retains all values matching the given predicate.
    Retain(K, Predicate<V>),
    /// Shrinks [`Values`] to their minimum necessary size, freeing memory
    /// and potentially improving cache locality.
    ///
    /// If no key is given, all `Values` will shrink to fit.
    Fit(Option<K>),
    /// Reserves capacity for some number of additional elements in [`Values`]
    /// for the given key. If the given key does not exist, allocate an empty
    /// `Values` with the given capacity.
    ///
    /// This can improve performance by pre-allocating space for large bags of values.
    Reserve(K, usize),
    /// Mark the map as ready to be consumed for readers.
    MarkReady,
    /// Set the value of the map meta.
    SetMeta(M),
    /// Set the value of the timestamp of the current values in the map.
    SetTimestamp(T),
    /// Copy over the contents of the read map wholesale as the write map is empty.
    JustCloneRHandle,
}

impl<K, V, M, T> fmt::Debug for Operation<K, V, M, T>
where
    K: fmt::Debug,
    V: fmt::Debug,
    M: fmt::Debug,
    T: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Operation::Replace(ref a, ref b) => f.debug_tuple("Replace").field(a).field(b).finish(),
            Operation::Add(ref a, ref b) => f.debug_tuple("Add").field(a).field(b).finish(),
            Operation::AddRange(ref a, ref b) => {
                f.debug_tuple("AddRange").field(a).field(b).finish()
            }
            Operation::RemoveValue(ref a, ref b) => {
                f.debug_tuple("RemoveValue").field(a).field(b).finish()
            }
            Operation::RemoveRange(ref range) => f.debug_tuple("RemoveRange").field(range).finish(),
            Operation::RemoveEntry(ref a) => f.debug_tuple("RemoveEntry").field(a).finish(),
            Operation::Clear(ref a) => f.debug_tuple("Clear").field(a).finish(),
            Operation::Purge => f.debug_tuple("Purge").finish(),
            Operation::Retain(ref a, ref b) => f.debug_tuple("Retain").field(a).field(b).finish(),
            Operation::Fit(ref a) => f.debug_tuple("Fit").field(a).finish(),
            Operation::Reserve(ref a, ref b) => f.debug_tuple("Reserve").field(a).field(b).finish(),
            Operation::MarkReady => f.debug_tuple("MarkReady").finish(),
            Operation::SetMeta(ref a) => f.debug_tuple("SetMeta").field(a).finish(),
            Operation::SetTimestamp(ref a) => f.debug_tuple("SetTimestamp").field(a).finish(),
            Operation::JustCloneRHandle => f.debug_tuple("JustCloneRHandle").finish(),
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
