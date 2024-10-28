use std::borrow::Cow;
use std::cmp::Ordering;
use std::sync::Arc;

use ahash::RandomState;
use common::SizeOf;
use dataflow_expression::{PostLookup, ReaderProcessing};
use reader_map::{EvictionQuantity, EvictionStrategy};
use readyset_client::consistency::Timestamp;
use readyset_client::results::SharedResults;
use readyset_client::KeyComparison;
use readyset_data::Bound;
use vec1::Vec1;

pub use self::multir::LookupError;
use crate::prelude::*;

/// The kind of reader update notification, currently the eviction epoch of the writer
pub(crate) type ReaderNotification = usize;
/// The type we can await for changes in the reader for
pub type ReaderUpdatedNotifier = tokio::sync::broadcast::Receiver<ReaderNotification>;
/// The type we can send reader update notifications
pub(crate) type ReaderUpdatedSender = tokio::sync::broadcast::Sender<ReaderNotification>;

/// Allocate a new end-user facing result table.
///
/// # Invariants:
///
/// * index must be non-empty, or we hit an unimplemented!
pub(crate) fn new(
    cols: usize,
    index: Index,
    reader_processing: ReaderProcessing,
    node_index: NodeIndex,
) -> (SingleReadHandle, WriteHandle) {
    new_inner(
        cols,
        index,
        None,
        EvictionKind::default(),
        reader_processing,
        node_index,
    )
}

/// Allocate a new partially materialized end-user facing result table.
///
/// Misses in this table will call `upquery` to populate the entry, and retry until successful.
///
/// # Arguments
///
/// * `cols` - the number of columns in this table
/// * `index` - the index for the reader
/// * `upquery` - function to call to trigger an upquery and replay
///
/// # Invariants:
///
/// * key must be non-empty, or we hit an unimplemented!
pub(crate) fn new_partial<F>(
    cols: usize,
    index: Index,
    upquery: F,
    eviction_kind: EvictionKind,
    reader_processing: ReaderProcessing,
    node_index: NodeIndex,
) -> (SingleReadHandle, WriteHandle)
where
    F: Fn(&mut dyn Iterator<Item = KeyComparison>) -> bool + 'static + Send + Sync,
{
    new_inner(
        cols,
        index,
        Some(Arc::new(upquery)),
        eviction_kind,
        reader_processing,
        node_index,
    )
}

// # Invariants:
//
// * key must be non-empty, or we hit an unimplemented!
#[allow(clippy::type_complexity)]
fn new_inner(
    cols: usize,
    index: Index,
    upquery: Option<
        Arc<dyn Fn(&mut dyn Iterator<Item = KeyComparison>) -> bool + 'static + Send + Sync>,
    >,
    eviction_kind: EvictionKind,
    reader_processing: ReaderProcessing,
    node_index: NodeIndex,
) -> (SingleReadHandle, WriteHandle) {
    let contiguous = {
        let mut contiguous = true;
        let mut last = None;
        for &k in &index.columns {
            if let Some(last) = last {
                if k != last + 1 {
                    contiguous = false;
                    break;
                }
            }
            last = Some(k);
        }
        contiguous
    };

    let eviction_strategy = match eviction_kind {
        EvictionKind::Random => EvictionStrategy::new_lru(),
        EvictionKind::LRU => EvictionStrategy::new_lru(),
    };

    let ReaderProcessing {
        pre_processing,
        post_processing,
    } = reader_processing;

    macro_rules! make {
        ($variant:tt) => {{
            let (mut w, r) = reader_map::Options::default()
                .with_meta(-1)
                .with_node_index(node_index)
                .with_timestamp(Timestamp::default())
                .with_hasher(RandomState::default())
                .with_index_type(index.index_type)
                .with_eviction_strategy(eviction_strategy)
                .with_insertion_order(Some(pre_processing.clone()))
                .construct();
            // If we're fully materialized, we never miss, so we can insert a single interval to
            // cover the full range of keys
            // PERF: this is likely not the most efficient way to do this - at some point we likely
            // want to pass whether we're fully materialized down into the reader_map and skip
            // inserting into the interval tree entirely (maybe make it an option?) if so
            if upquery.is_none() {
                w.insert_full_range();
            }
            (multiw::Handle::$variant(w), multir::Handle::$variant(r))
        }};
    }

    let (w, r) = match index.len() {
        0 => unreachable!(),
        1 => make!(Single),
        _ => make!(Many),
    };

    let (notifier, receiver) = tokio::sync::broadcast::channel(1);
    let partial = upquery.is_some();
    let w = WriteHandle {
        partial,
        replay_done: partial,
        handle: w,
        index: index.clone(),
        cols,
        contiguous,
        mem_size: 0,
        notifier,
        eviction_epoch: 0,
    };

    let r = SingleReadHandle {
        handle: r,
        upquery,
        index,
        post_lookup: post_processing,
        receiver,
        eviction_epoch: 0,
    };

    (r, w)
}

mod multir;
mod multiw;

fn key_to_single(k: Key) -> Cow<DfValue> {
    assert_eq!(k.len(), 1);
    match k {
        Cow::Owned(mut k) => Cow::Owned(k.swap_remove(0)),
        Cow::Borrowed(k) => Cow::Borrowed(&k[0]),
    }
}

pub(crate) struct WriteHandle {
    handle: multiw::Handle,
    partial: bool,
    /// If this reeader is fully materialized, has it received a complete full replay yet?
    replay_done: bool,
    cols: usize,
    index: Index,
    contiguous: bool,
    mem_size: usize,
    /// The notifier can be used to notify readers that are waiting on changes
    notifier: ReaderUpdatedSender,
    /// How many eviction rounds this handle had
    eviction_epoch: usize,
}

type Key<'a> = Cow<'a, [DfValue]>;

pub(crate) struct MutWriteHandleEntry<'a> {
    handle: &'a mut WriteHandle,
    key: Key<'a>,
}

impl MutWriteHandleEntry<'_> {
    pub(crate) fn key_value_size(&self, key: &Key) -> usize {
        self.handle.handle.base_value_size()
            + key.iter().map(SizeOf::deep_size_of).sum::<u64>() as usize
    }
}

impl MutWriteHandleEntry<'_> {
    pub(crate) fn mark_filled(self) -> ReadySetResult<()> {
        if self
            .handle
            .handle
            .read()
            .get(&self.key)
            .map(|rs| rs.is_empty())
            .err()
            .iter()
            .any(LookupError::is_miss)
        {
            // TODO(ENG-726): Trying to introspect how much memory these data structures
            // are using for storing key value pairs can provide a poor estimate. Handling
            // memory tracking closer to where the data is stored will be beneficial.
            self.handle.mem_size += self.key_value_size(&self.key);
            self.handle.handle.clear(self.key);
            Ok(())
        } else {
            Err(ReadySetError::KeyAlreadyFilled)
        }
    }

    /// Clear this entry and return the number of bytes freed
    pub(crate) fn mark_hole(self) -> u64 {
        // Do not account for the key if we miss on the lookup
        let size = self
            .handle
            .handle
            .read()
            .get(&self.key)
            .map(|rs| {
                rs.iter().map(SizeOf::deep_size_of).sum::<u64>() as usize
                    + self.key_value_size(&self.key)
            })
            .unwrap_or(0);
        self.handle.mem_size = self.handle.mem_size.saturating_sub(size);
        self.handle.handle.empty(self.key);
        size as u64
    }
}

impl WriteHandle {
    pub(crate) fn mut_with_key<'a, K>(&'a mut self, key: K) -> MutWriteHandleEntry<'a>
    where
        K: Into<Key<'a>>,
    {
        MutWriteHandleEntry {
            handle: self,
            key: key.into(),
        }
    }

    pub(crate) fn contains(&self, key: &KeyComparison) -> reader_map::Result<bool> {
        match key {
            KeyComparison::Equal(k) => self.handle.read().contains_key(k),
            KeyComparison::Range((start, end)) => self.handle.read().contains_range(&(
                start.as_ref().map(Vec1::as_vec),
                end.as_ref().map(Vec1::as_vec),
            )),
        }
    }

    pub(crate) fn contains_record(&self, rec: &[DfValue]) -> reader_map::Result<bool> {
        let key_cols = self.index.columns.as_slice();
        if self.contiguous {
            self.contains_key(&rec[key_cols[0]..(key_cols[0] + key_cols.len())])
        } else {
            self.contains_key(&key_cols.iter().map(|c| rec[*c].clone()).collect::<Vec<_>>())
        }
    }

    pub(crate) fn interval_difference(&self, key: KeyComparison) -> Option<Vec<KeyComparison>> {
        match self.handle.read().get_multi(&[key]) {
            Err(LookupError::Miss((misses, _))) => {
                Some(misses.into_iter().map(|c| c.into_owned()).collect())
            }
            _ => None,
        }
    }

    pub(super) fn contains_key(&self, key: &[DfValue]) -> reader_map::Result<bool> {
        self.handle.read().contains_key(key)
    }

    pub(crate) fn publish(&mut self) {
        self.handle.publish();
    }

    pub(crate) fn len(&self) -> usize {
        self.handle.read().len()
    }

    /// Add a new set of records to the backlog.
    ///
    /// These will be made visible to readers after the next call to `publish()`.
    pub(crate) fn add<I>(&mut self, rs: I)
    where
        I: IntoIterator<Item = Record>,
    {
        let mem_delta = self.handle.add(&self.index.columns, self.cols, rs);
        match mem_delta.cmp(&0) {
            Ordering::Greater => {
                self.mem_size += mem_delta as usize;
            }
            Ordering::Less => {
                self.mem_size = self.mem_size.saturating_sub(-mem_delta as usize);
            }
            _ => {}
        }
    }
    pub(crate) fn set_timestamp(&mut self, t: Timestamp) {
        self.handle.set_timestamp(t);
    }

    pub(crate) fn is_partial(&self) -> bool {
        self.partial
    }

    /// Evict from state according to the [`EvictionQuantity`]. Returns the number of bytes freed
    /// and if the request is EvictionQuantity::SingleKey, returns the key that was evicted.
    fn evict_inner(&mut self, request: EvictionQuantity) -> (u64, Option<Vec<DfValue>>) {
        let (bytes_to_be_freed, eviction) = if self.mem_size > 0 {
            debug_assert!(
                !self.handle.is_empty(),
                "mem size is {}, but map is empty",
                self.mem_size
            );

            self.handle.evict(request)
        } else {
            (0, None)
        };

        self.mem_size = self.mem_size.saturating_sub(bytes_to_be_freed as usize);
        (bytes_to_be_freed, eviction)
    }

    /// Attempt to evict `bytes` from state. This approximates the number of keys to evict,
    /// these keys may not have exactly `bytes` worth of state.
    pub(crate) fn evict_bytes(&mut self, bytes: usize) -> u64 {
        let request = EvictionQuantity::Ratio(bytes as f64 / self.mem_size as f64);
        self.evict_inner(request).0
    }

    /// Evict a single key from state
    pub(crate) fn evict_random(&mut self) -> (u64, Option<Vec<DfValue>>) {
        let request = EvictionQuantity::SingleKey;
        self.evict_inner(request)
    }

    pub(crate) fn mark_hole(&mut self, key: &KeyComparison) -> ReadySetResult<u64> {
        invariant_eq!(key.len(), self.index.len());

        match key {
            KeyComparison::Equal(k) => Ok(self.mut_with_key(k.as_vec()).mark_hole()),
            KeyComparison::Range((start, end)) => {
                let start = start.clone();
                let end = end.clone();
                // We don't want to clone things more than once, so construct the range key, then
                // deconstruct it again
                let range_key = KeyComparison::Range((start, end));
                let size = self
                    .handle
                    .read()
                    .get_multi(std::slice::from_ref(&range_key))
                    .map(|rs| {
                        rs.iter()
                            .flat_map(|rs| rs.iter().map(SizeOf::deep_size_of))
                            .sum::<u64>()
                    })
                    .unwrap_or(0);

                self.mem_size = self.mem_size.saturating_sub(size as usize);
                if let KeyComparison::Range(range) = range_key {
                    self.handle
                        .empty_range((range.0.map(Vec1::into_vec), range.1.map(Vec1::into_vec)));
                }
                Ok(size)
            }
        }
    }

    pub(crate) fn mark_filled(&mut self, key: KeyComparison) -> ReadySetResult<()> {
        invariant_eq!(key.len(), self.index.len());

        let range = match (self.index.index_type, &key) {
            (IndexType::HashMap, KeyComparison::Equal(equal)) => {
                return self.mut_with_key(equal.as_vec()).mark_filled();
            }
            (IndexType::HashMap, KeyComparison::Range(_)) => {
                unreachable!("Range key with a HashMap index")
            }
            (IndexType::BTreeMap, KeyComparison::Equal(equal)) => (
                Bound::Included(equal.as_vec()),
                Bound::Included(equal.as_vec()),
            ),
            (IndexType::BTreeMap, KeyComparison::Range((start, end))) => (
                start.as_ref().map(Vec1::as_vec),
                end.as_ref().map(Vec1::as_vec),
            ),
        };

        if self.handle.read().overlaps_range(&range).unwrap_or(false) {
            return Err(ReadySetError::RangeAlreadyFilled);
        }

        self.handle.insert_range(range);
        Ok(())
    }

    /// Increment the eviction epoch, and notify readers
    pub(crate) fn notify_readers_of_eviction(&mut self) -> ReadySetResult<()> {
        self.eviction_epoch += 1;
        self.notify_readers()
    }

    /// Notify readers with the current eviction epoch
    pub(crate) fn notify_readers(&mut self) -> ReadySetResult<()> {
        self.notifier
            .send(self.eviction_epoch)
            .map_err(|_| ReadySetError::ReaderNotFound)?;
        Ok(())
    }

    /// The index type of the underlying state
    pub(crate) fn index_type(&self) -> IndexType {
        self.index.index_type
    }

    pub(crate) fn set_replay_done(&mut self, replay_done: bool) {
        debug_assert!(!self.is_partial());
        self.replay_done = replay_done;
    }

    pub(crate) fn replay_done(&self) -> bool {
        self.replay_done
    }
}

impl SizeOf for WriteHandle {
    fn size_of(&self) -> u64 {
        std::mem::size_of::<Self>() as u64
    }

    fn deep_size_of(&self) -> u64 {
        self.mem_size as u64
    }

    fn is_empty(&self) -> bool {
        self.handle.is_empty()
    }
}

/// Handle to get the state of a single shard of a reader.
#[allow(clippy::type_complexity)]
pub struct SingleReadHandle {
    handle: multir::Handle,
    upquery: Option<
        Arc<dyn Fn(&mut dyn Iterator<Item = KeyComparison>) -> bool + 'static + Send + Sync>,
    >,
    index: Index,
    pub post_lookup: PostLookup,
    /// Receives a notification whenever the [`WriteHandle`] is updated after filling an upquery
    receiver: ReaderUpdatedNotifier,
    /// Caches the eviction epoch of the associated [`WriteHandle`]
    eviction_epoch: usize,
}

impl Clone for SingleReadHandle {
    fn clone(&self) -> Self {
        Self {
            handle: self.handle.clone(),
            upquery: self.upquery.clone(),
            index: self.index.clone(),
            post_lookup: self.post_lookup.clone(),
            receiver: self.receiver.resubscribe(),
            eviction_epoch: self.eviction_epoch,
        }
    }
}

impl std::fmt::Debug for SingleReadHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SingleReadHandle")
            .field("handle", &self.handle)
            .field("has_upquery", &self.upquery.is_some())
            .field("index", &self.index)
            .finish()
    }
}

impl SingleReadHandle {
    /// Trigger a replay of a missing key from a partially materialized view.
    pub fn upquery<I>(&self, mut keys: I) -> bool
    where
        I: Iterator<Item = KeyComparison>,
    {
        let Some(ref t) = self.upquery else {
            panic!("tried to trigger a replay for a fully materialized view");
        };
        t(&mut keys)
    }

    /// Returns None if this handle is not ready, Some(true) if this handle fully contains the
    /// given key comparison, Some(false) if any of the keys miss
    pub fn contains(&self, key: &KeyComparison) -> reader_map::Result<bool> {
        match key {
            KeyComparison::Equal(k) => self.handle.contains_key(k),
            KeyComparison::Range((start, end)) => self.handle.contains_range(&(
                start.as_ref().map(Vec1::as_vec),
                end.as_ref().map(Vec1::as_vec),
            )),
        }
    }

    /// Look up a list of keys under the same reader guard
    pub fn get_multi<'a>(
        &self,
        keys: &'a [KeyComparison],
    ) -> Result<SharedResults, LookupError<'a>> {
        match self.handle.get_multi(keys) {
            Err(e) if e.is_miss() && self.upquery.is_none() => Ok(SharedResults::default()),
            r => r,
        }
    }

    /// Look up a list of keys under the same reader guard. If missed, will include a notifier
    /// that can tell us when a new hole was filled in the map.
    pub fn get_multi_with_notifier<'a>(
        &self,
        keys: &'a [KeyComparison],
    ) -> Result<SharedResults, LookupError<'a, ReaderUpdatedNotifier>> {
        match self
            .handle
            .get_multi_and_map_error(keys, || self.receiver.resubscribe())
        {
            Err(e) if e.is_miss() && self.upquery.is_none() => Ok(SharedResults::default()),
            r => r,
        }
    }

    pub fn len(&self) -> usize {
        self.handle.len()
    }

    pub fn is_empty(&self) -> bool {
        self.handle.len() == 0
    }

    pub fn keys(&self) -> Vec<Vec<DfValue>> {
        self.handle.keys()
    }

    pub fn timestamp(&self) -> Option<Timestamp> {
        self.handle.timestamp()
    }

    /// Returns true if the corresponding write handle to our read handle has been dropped
    pub fn was_dropped(&self) -> bool {
        self.handle.was_dropped()
    }

    pub fn eviction_epoch(&mut self) -> usize {
        while !self.receiver.is_empty() {
            if let Ok(epoch) = self.receiver.try_recv() {
                self.eviction_epoch = epoch
            }
        }

        self.eviction_epoch
    }
}

#[cfg(test)]
mod tests {
    use readyset_client::results::SharedRows;
    use readyset_data::Bound;

    use super::*;

    impl SingleReadHandle {
        fn get<'a>(&self, key: &'a [DfValue]) -> Result<SharedRows, LookupError<'a>> {
            match self.handle.get(key) {
                Err(e) if e.is_miss() && self.upquery.is_none() => Ok(SharedRows::default()),
                r => r,
            }
        }
    }

    #[test]
    fn store_works() {
        let a = vec![1i32.into(), "a".into()].into_boxed_slice();

        let (r, mut w) = new(
            2,
            Index::hash_map(vec![0]),
            ReaderProcessing::default(),
            Default::default(),
        );

        w.publish();

        // after first publish, it is empty, but ready
        assert_eq!(r.get(&a[0..1]).unwrap().len(), 0);

        w.add(vec![Record::Positive(a.to_vec())]);

        // it is empty even after an add (we haven't published yet)
        assert_eq!(r.get(&a[0..1]).unwrap().len(), 0);

        w.publish();

        // but after the publish, the record is there!
        assert_eq!(r.get(&a[0..1]).unwrap().len(), 1);
        assert_eq!(r.get(&a[0..1]).unwrap()[0], a);
    }

    #[test]
    fn busybusybusy() {
        use std::thread;

        let n = 1_000;
        let (r, mut w) = new(
            1,
            Index::hash_map(vec![0]),
            ReaderProcessing::default(),
            Default::default(),
        );
        let jh = thread::spawn(move || {
            for i in 0..n {
                w.add(vec![Record::Positive(vec![i.into()])]);
                w.publish();
            }
            // important that we don't drop w here, or the loop below never exits
            w
        });

        for i in 0..n {
            let i = &[i.into()];
            loop {
                match r.get(i) {
                    Ok(rs) if rs.len() == 1 => break,
                    Ok(rs) => assert_ne!(rs.len(), 1),
                    Err(_) => continue,
                }
            }
        }

        jh.join().unwrap();
    }

    #[test]
    fn minimal_query() {
        let a = vec![1i32.into(), "a".into()].into_boxed_slice();
        let b = vec![1i32.into(), "b".into()].into_boxed_slice();

        let (r, mut w) = new(
            2,
            Index::hash_map(vec![0]),
            ReaderProcessing::default(),
            Default::default(),
        );
        w.add(vec![Record::Positive(a.to_vec())]);
        w.publish();
        w.add(vec![Record::Positive(b.to_vec())]);

        assert_eq!(r.get(&a[0..1]).unwrap().len(), 1);
        assert_eq!(r.get(&a[0..1]).unwrap()[0], a);
    }

    #[test]
    fn non_minimal_query() {
        let a = vec![1i32.into(), "a".into()].into_boxed_slice();
        let b = vec![1i32.into(), "b".into()].into_boxed_slice();
        let c = vec![1i32.into(), "c".into()].into_boxed_slice();

        let (r, mut w) = new(
            2,
            Index::hash_map(vec![0]),
            ReaderProcessing::default(),
            Default::default(),
        );
        w.add(vec![Record::Positive(a.to_vec())]);
        w.add(vec![Record::Positive(b.to_vec())]);
        w.publish();
        w.add(vec![Record::Positive(c.to_vec())]);

        assert_eq!(r.get(&a[0..1]).unwrap().len(), 2);
        assert_eq!(r.get(&a[0..1]).unwrap()[0], a);
        assert_eq!(r.get(&a[0..1]).unwrap()[1], b);
    }

    #[test]
    fn absorb_negative_immediate() {
        let a = vec![1i32.into(), "a".into()].into_boxed_slice();
        let b = vec![1i32.into(), "b".into()].into_boxed_slice();

        let (r, mut w) = new(
            2,
            Index::hash_map(vec![0]),
            ReaderProcessing::default(),
            Default::default(),
        );
        w.add(vec![Record::Positive(a.to_vec())]);
        w.add(vec![Record::Positive(b.to_vec())]);
        w.add(vec![Record::Negative(a.to_vec())]);
        w.publish();

        assert_eq!(r.get(&a[0..1]).unwrap().len(), 1);
        assert_eq!(r.get(&a[0..1]).unwrap()[0], b);
    }

    #[test]
    fn absorb_negative_later() {
        let a = vec![1i32.into(), "a".into()].into_boxed_slice();
        let b = vec![1i32.into(), "b".into()].into_boxed_slice();

        let (r, mut w) = new(
            2,
            Index::hash_map(vec![0]),
            ReaderProcessing::default(),
            Default::default(),
        );
        w.add(vec![Record::Positive(a.to_vec())]);
        w.add(vec![Record::Positive(b.to_vec())]);
        w.publish();
        w.add(vec![Record::Negative(a.to_vec())]);
        w.publish();

        assert_eq!(r.get(&a[0..1]).unwrap().len(), 1);
        assert_eq!(r.get(&a[0..1]).unwrap()[0], b);
    }

    #[test]
    fn absorb_multi() {
        let a = vec![1i32.into(), "a".into()].into_boxed_slice();
        let b = vec![1i32.into(), "b".into()].into_boxed_slice();
        let c = vec![1i32.into(), "c".into()].into_boxed_slice();

        let (r, mut w) = new(
            2,
            Index::hash_map(vec![0]),
            ReaderProcessing::default(),
            Default::default(),
        );
        w.add(vec![
            Record::Positive(a.to_vec()),
            Record::Positive(b.to_vec()),
        ]);
        w.publish();

        assert_eq!(r.get(&a[0..1]).unwrap().len(), 2);
        assert_eq!(r.get(&a[0..1]).unwrap()[0], a);
        assert_eq!(r.get(&a[0..1]).unwrap()[1], b);

        w.add(vec![
            Record::Negative(a.to_vec()),
            Record::Positive(c.to_vec()),
            Record::Negative(c.to_vec()),
        ]);
        w.publish();

        assert_eq!(r.get(&a[0..1]).unwrap().len(), 1);
        assert_eq!(r.get(&a[0..1]).unwrap()[0], b);
    }

    #[test]
    fn find_missing_partial() {
        let (r, mut w) = new_partial(
            1,
            Index::hash_map(vec![0]),
            |_: &mut dyn Iterator<Item = KeyComparison>| true,
            EvictionKind::default(),
            ReaderProcessing::default(),
            Default::default(),
        );
        w.publish();

        match r.get(&[1.into()]) {
            Err(LookupError::Miss((mut misses, _))) => {
                assert_eq!(
                    misses.pop().unwrap().into_owned().equal().unwrap(),
                    &vec1![1.into()]
                );
            }
            _ => panic!("Should have missed"),
        }
    }

    mod mark_filled {
        use super::*;

        #[test]
        fn point() {
            let (r, mut w) = new_partial(
                1,
                Index::hash_map(vec![0]),
                |_: &mut dyn Iterator<Item = KeyComparison>| true,
                EvictionKind::default(),
                ReaderProcessing::default(),
                Default::default(),
            );
            w.publish();

            let key = vec1![DfValue::from(0)];
            assert!(r.get(&key).err().unwrap().is_miss());

            w.mark_filled(key.clone().into()).unwrap();
            w.publish();
            r.get(&key).unwrap();
        }

        #[test]
        fn range() {
            let (r, mut w) = new_partial(
                1,
                Index::btree_map(vec![0]),
                |_: &mut dyn Iterator<Item = KeyComparison>| true,
                EvictionKind::default(),
                ReaderProcessing::default(),
                Default::default(),
            );
            w.publish();

            let range_key = &[KeyComparison::Range((
                Bound::Included(vec1![DfValue::from(0)]),
                Bound::Excluded(vec1![DfValue::from(10)]),
            ))];

            assert!(r.get_multi(range_key).err().unwrap().is_miss());

            w.mark_filled(KeyComparison::from_range(
                &(vec1![DfValue::from(0)]..vec1![DfValue::from(10)]),
            ))
            .unwrap();
            w.publish();
            r.get_multi(range_key).unwrap();
        }
    }

    mod mark_hole {
        use super::*;

        #[test]
        fn point() {
            let (r, mut w) = new_partial(
                1,
                Index::btree_map(vec![0]),
                |_: &mut dyn Iterator<Item = KeyComparison>| true,
                EvictionKind::default(),
                ReaderProcessing::default(),
                Default::default(),
            );
            w.publish();

            let key = vec1![DfValue::from(0)];
            w.mark_filled(key.clone().into()).unwrap();
            w.publish();
            r.get(&key).unwrap();

            w.mark_hole(&key.clone().into()).unwrap();
            w.publish();
            assert!(r.get(&key).err().unwrap().is_miss());
        }

        #[test]
        fn range() {
            let (r, mut w) = new_partial(
                1,
                Index::btree_map(vec![0]),
                |_: &mut dyn Iterator<Item = KeyComparison>| true,
                EvictionKind::default(),
                ReaderProcessing::default(),
                Default::default(),
            );
            w.publish();

            let range_key = &[KeyComparison::Range((
                Bound::Included(vec1![DfValue::from(0)]),
                Bound::Excluded(vec1![DfValue::from(10)]),
            ))];

            w.mark_filled(KeyComparison::from_range(
                &(vec1![DfValue::from(0)]..vec1![DfValue::from(10)]),
            ))
            .unwrap();
            w.publish();
            r.get_multi(range_key).unwrap();

            w.mark_hole(&KeyComparison::from_range(
                &(vec1![DfValue::from(0)]..vec1![DfValue::from(10)]),
            ))
            .unwrap();
            w.publish();
            assert!(r.get_multi(range_key).err().unwrap().is_miss());
        }
    }
}
