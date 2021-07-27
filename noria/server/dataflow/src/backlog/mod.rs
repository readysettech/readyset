pub use self::multir::{LookupError, LookupResult};
use crate::{prelude::*, PostLookup};
use ahash::RandomState;
use common::SizeOf;
use noria::consistency::Timestamp;
use noria::KeyComparison;
use rand::prelude::*;
use reader_map::refs::Values;
use std::borrow::Cow;
use std::cmp::Ordering;
use std::ops::RangeBounds;
use std::sync::Arc;
use vec1::Vec1;

pub(crate) trait Trigger =
    Fn(&mut dyn Iterator<Item = &KeyComparison>) -> bool + 'static + Send + Sync;

/// Allocate a new end-user facing result table.
///
/// # Invariants:
///
/// * key must be non-empty, or we hit an unimplemented!
pub(crate) fn new(cols: usize, key: &[usize]) -> (SingleReadHandle, WriteHandle) {
    new_inner(cols, key, None)
}

/// Allocate a new partially materialized end-user facing result table.
///
/// Misses in this table will call `trigger` to populate the entry, and retry until successful.
///
/// # Arguments
///
/// * `cols` - the number of columns in this table
/// * `key` - the column indices for the lookup key for this table
/// * `trigger` - function to call to trigger an upquery and replay
///
/// # Invariants:
///
/// * key must be non-empty, or we hit an unimplemented!
pub(crate) fn new_partial<F>(
    cols: usize,
    key: &[usize],
    trigger: F,
) -> (SingleReadHandle, WriteHandle)
where
    F: Trigger,
{
    new_inner(cols, key, Some(Arc::new(trigger)))
}

// # Invariants:
//
// * key must be non-empty, or we hit an unimplemented!
fn new_inner(
    cols: usize,
    key: &[usize],
    trigger: Option<Arc<dyn Trigger>>,
) -> (SingleReadHandle, WriteHandle) {
    let contiguous = {
        let mut contiguous = true;
        let mut last = None;
        for &k in key {
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

    macro_rules! make {
        ($variant:tt) => {{
            use reader_map;
            let (mut w, r) = reader_map::Options::default()
                .with_meta(-1)
                .with_timestamp(Timestamp::default())
                .with_hasher(RandomState::default())
                .construct();
            // If we're fully materialized, we never miss, so we can insert a single interval to
            // cover the full range of keys
            // PERF: this is likely not the most efficient way to do this - at some point we likely
            // want to pass whether we're fully materialized down into the reader_map and skip
            // inserting into the interval tree entirely (maybe make it an option?) if so
            if trigger.is_none() {
                w.insert_range(vec![], ..);
            }
            (multiw::Handle::$variant(w), multir::Handle::$variant(r))
        }};
    }

    #[allow(clippy::unreachable)] // Documented invariant.
    let (w, r) = match key.len() {
        0 => unreachable!(),
        1 => make!(Single),
        2 => make!(Double),
        _ => make!(Many),
    };

    let w = WriteHandle {
        partial: trigger.is_some(),
        handle: w,
        key: Vec::from(key),
        cols,
        contiguous,
        mem_size: 0,
    };
    let r = SingleReadHandle {
        handle: r,
        trigger,
        key: Vec::from(key),
        post_lookup: Default::default(),
    };

    (r, w)
}

mod multir;
mod multiw;

fn key_to_single(k: Key) -> Cow<DataType> {
    assert_eq!(k.len(), 1);
    match k {
        Cow::Owned(mut k) => Cow::Owned(k.swap_remove(0)),
        Cow::Borrowed(k) => Cow::Borrowed(&k[0]),
    }
}

fn key_to_double(k: Key) -> Cow<(DataType, DataType)> {
    assert_eq!(k.len(), 2);
    match k {
        Cow::Owned(k) => {
            let mut k = k.into_iter();
            let k1 = k.next().unwrap();
            let k2 = k.next().unwrap();
            Cow::Owned((k1, k2))
        }
        Cow::Borrowed(k) => Cow::Owned((k[0].clone(), k[1].clone())),
    }
}

pub(crate) struct WriteHandle {
    handle: multiw::Handle,
    partial: bool,
    cols: usize,
    key: Vec<usize>,
    contiguous: bool,
    mem_size: usize,
}

type Key<'a> = Cow<'a, [DataType]>;
pub(crate) struct MutWriteHandleEntry<'a> {
    handle: &'a mut WriteHandle,
    key: Key<'a>,
}
pub(crate) struct WriteHandleEntry<'a> {
    handle: &'a WriteHandle,
    key: Key<'a>,
}

impl<'a> WriteHandleEntry<'a> {
    pub(crate) fn try_find_and<F, T>(self, mut then: F) -> LookupResult<T>
    where
        F: FnMut(&reader_map::refs::Values<Vec<DataType>, RandomState>) -> T,
    {
        self.handle.handle.read().meta_get_and(&self.key, &mut then)
    }
}

impl<'a> MutWriteHandleEntry<'a> {
    pub(crate) fn mark_filled(self) {
        if self
            .handle
            .handle
            .read()
            .meta_get_and(&self.key, |rs| rs.is_empty())
            .err()
            .iter()
            .any(LookupError::is_miss)
        {
            self.handle.handle.clear(self.key)
        } else {
            unreachable!("attempted to fill already-filled key");
        }
    }

    pub(crate) fn mark_hole(self) {
        let size = self
            .handle
            .handle
            .read()
            .meta_get_and(&self.key, |rs| rs.iter().map(SizeOf::deep_size_of).sum())
            .map(|(size, _)| size)
            .unwrap_or(0);
        self.handle.mem_size = self.handle.mem_size.saturating_sub(size as usize);
        self.handle.handle.empty(self.key)
    }
}

fn key_from_record<'a, R>(key: &[usize], contiguous: bool, record: R) -> Key<'a>
where
    R: Into<Cow<'a, [DataType]>>,
{
    match record.into() {
        Cow::Owned(mut record) => {
            let mut i = 0;
            let mut keep = key.iter().peekable();
            record.retain(|_| {
                i += 1;
                if let Some(&&next) = keep.peek() {
                    if next != i - 1 {
                        return false;
                    }
                } else {
                    return false;
                }

                assert_eq!(*keep.next().unwrap(), i - 1);
                true
            });
            Cow::Owned(record)
        }
        Cow::Borrowed(record) if contiguous => Cow::Borrowed(&record[key[0]..(key[0] + key.len())]),
        Cow::Borrowed(record) => Cow::Owned(key.iter().map(|&i| &record[i]).cloned().collect()),
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

    pub(crate) fn with_key<'a, K>(&'a self, key: K) -> WriteHandleEntry<'a>
    where
        K: Into<Key<'a>>,
    {
        WriteHandleEntry {
            handle: self,
            key: key.into(),
        }
    }

    pub(crate) fn contains(&self, key: &KeyComparison) -> Option<bool> {
        match key {
            KeyComparison::Equal(k) => self.handle.read().contains_key(k),
            KeyComparison::Range((start, end)) => self.handle.read().contains_range(&(
                start.as_ref().map(Vec1::as_vec),
                end.as_ref().map(Vec1::as_vec),
            )),
        }
    }

    #[allow(dead_code)]
    fn mut_entry_from_record<'a, R>(&'a mut self, record: R) -> MutWriteHandleEntry<'a>
    where
        R: Into<Cow<'a, [DataType]>>,
    {
        let key = key_from_record(&self.key[..], self.contiguous, record);
        self.mut_with_key(key)
    }

    pub(crate) fn entry_from_record<'a, R>(&'a self, record: R) -> WriteHandleEntry<'a>
    where
        R: Into<Cow<'a, [DataType]>>,
    {
        let key = key_from_record(&self.key[..], self.contiguous, record);
        self.with_key(key)
    }

    pub(crate) fn swap(&mut self) {
        self.handle.refresh();
    }

    /// Add a new set of records to the backlog.
    ///
    /// These will be made visible to readers after the next call to `swap()`.
    pub(crate) fn add<I>(&mut self, rs: I)
    where
        I: IntoIterator<Item = Record>,
    {
        let mem_delta = self.handle.add(&self.key[..], self.cols, rs);
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

    /// Evict `count` randomly selected keys from state and return them along with the number of
    /// bytes that will be freed once the underlying `reader_map` applies the operation.
    pub(crate) fn evict_random_keys(&mut self, rng: &mut ThreadRng, mut n: usize) -> u64 {
        let mut bytes_to_be_freed = 0;
        if self.mem_size > 0 {
            debug_assert!(
                !self.handle.is_empty(),
                "mem size is {}, but map is empty",
                self.mem_size
            );

            self.handle.empty_random_for_each(rng, n, |vs| {
                let size: u64 = vs.iter().map(|r| r.deep_size_of() as u64).sum();
                bytes_to_be_freed += size;
                n -= 1;
            });
        }

        self.mem_size = self.mem_size.saturating_sub(bytes_to_be_freed as usize);
        bytes_to_be_freed
    }

    pub(crate) fn mark_hole(&mut self, key: &KeyComparison) {
        match key {
            KeyComparison::Equal(k) => self.mut_with_key(k.as_vec()).mark_hole(),
            KeyComparison::Range((start, end)) => {
                let range = (
                    start.as_ref().map(Vec1::as_vec),
                    end.as_ref().map(Vec1::as_vec),
                );
                let size = self
                    .handle
                    .read()
                    .meta_get_range_and(&range, |rs| {
                        rs.iter().map(SizeOf::deep_size_of).sum::<u64>()
                    })
                    .map(|(sizes, _)| sizes.iter().sum())
                    .unwrap_or(0);
                self.mem_size = self.mem_size.saturating_sub(size as usize);
                self.handle.empty_range(range)
            }
        }
    }

    pub(crate) fn mark_filled(&mut self, key: KeyComparison) {
        match key {
            KeyComparison::Equal(equal) => self.mut_with_key(equal.as_vec()).mark_filled(),
            KeyComparison::Range((start, end)) => self.handle.insert_range((
                start.as_ref().map(Vec1::as_vec),
                end.as_ref().map(Vec1::as_vec),
            )),
        }
    }
}

impl SizeOf for WriteHandle {
    fn size_of(&self) -> u64 {
        use std::mem::size_of;

        size_of::<Self>() as u64
    }

    fn deep_size_of(&self) -> u64 {
        self.mem_size as u64
    }

    fn is_empty(&self) -> bool {
        self.handle.is_empty()
    }
}

/// Handle to get the state of a single shard of a reader.
#[derive(Clone)]
pub struct SingleReadHandle {
    handle: multir::Handle,
    trigger: Option<Arc<dyn Trigger>>,
    key: Vec<usize>,
    pub post_lookup: PostLookup,
}

impl std::fmt::Debug for SingleReadHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SingleReadHandle")
            .field("handle", &self.handle)
            .field("has_trigger", &self.trigger.is_some())
            .field("key", &self.key)
            .finish()
    }
}

impl SingleReadHandle {
    /// Trigger a replay of a missing key from a partially materialized view.
    pub fn trigger<'a, I>(&self, keys: I) -> bool
    where
        I: Iterator<Item = &'a KeyComparison>,
    {
        assert!(
            self.trigger.is_some(),
            "tried to trigger a replay for a fully materialized view"
        );

        let mut it = keys;

        // trigger a replay to populate
        (*self.trigger.as_ref().unwrap())(&mut it)
    }

    /// Returns None if this handle is not ready, Some(true) if this handle fully contains the given
    /// key comparison, Some(false) if any of the keys miss
    pub fn contains(&self, key: &KeyComparison) -> Option<bool> {
        match key {
            KeyComparison::Equal(k) => self.handle.contains_key(k),
            KeyComparison::Range((start, end)) => self.handle.contains_range(&(
                start.as_ref().map(Vec1::as_vec),
                end.as_ref().map(Vec1::as_vec),
            )),
        }
    }

    /// Find all entries that matched the given conditions.
    ///
    /// Returned records are passed to `then` before being returned.
    ///
    /// Note that not all writes will be included with this read -- only those that have been
    /// swapped in by the writer.
    ///
    /// Holes in partially materialized state are returned as `Ok((None, _))`.
    pub fn try_find_and<F, T>(&self, key: &[DataType], mut then: F) -> Result<(T, i64), LookupError>
    where
        F: FnMut(&Values<Vec<DataType>, RandomState>) -> T,
    {
        match self.handle.meta_get_and(key, &mut then) {
            Err(e) if e.is_miss() && self.trigger.is_none() => {
                Ok((then(&Values::default()), e.meta().unwrap()))
            }
            r => r,
        }
    }

    /// Look up the entries whose keys are in `range`, pass each to `then`, and return them
    pub fn try_find_range_and<F, T, R>(
        &self,
        range: &R,
        mut then: F,
    ) -> Result<(Vec<T>, i64), LookupError>
    where
        F: FnMut(&Values<Vec<DataType>, RandomState>) -> T,
        R: RangeBounds<Vec<common::DataType>>,
    {
        match self.handle.meta_get_range_and(range, &mut then) {
            Err(e) if e.is_miss() && self.trigger.is_none() => {
                Ok((vec![then(&Values::default())], e.meta().unwrap()))
            }
            r => r,
        }
    }

    pub fn len(&self) -> usize {
        self.handle.len()
    }

    pub fn is_empty(&self) -> bool {
        self.handle.len() == 0
    }

    pub fn keys(&self) -> Vec<Vec<DataType>> {
        self.handle.keys()
    }

    pub fn timestamp(&self) -> Option<Timestamp> {
        self.handle.timestamp()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::TryInto;

    #[test]
    fn store_works() {
        let a = vec![1.into(), "a".try_into().unwrap()];

        let (r, mut w) = new(2, &[0]);

        w.swap();

        // after first swap, it is empty, but ready
        assert_eq!(r.try_find_and(&a[0..1], |rs| rs.len()), Ok((0, -1)));

        w.add(vec![Record::Positive(a.clone())]);

        // it is empty even after an add (we haven't swapped yet)
        assert_eq!(r.try_find_and(&a[0..1], |rs| rs.len()), Ok((0, -1)));

        w.swap();

        // but after the swap, the record is there!
        assert_eq!(r.try_find_and(&a[0..1], |rs| rs.len()).unwrap().0, 1);
        assert!(
            r.try_find_and(&a[0..1], |rs| rs
                .iter()
                .any(|r| r[0] == a[0] && r[1] == a[1]))
                .unwrap()
                .0
        );
    }

    #[test]
    fn busybusybusy() {
        use std::thread;

        let n = 1_000;
        let (r, mut w) = new(1, &[0]);
        let jh = thread::spawn(move || {
            for i in 0..n {
                w.add(vec![Record::Positive(vec![i.into()])]);
                w.swap();
            }
            // important that we don't drop w here, or the loop below never exits
            w
        });

        for i in 0..n {
            let i = &[i.into()];
            loop {
                match r.try_find_and(i, |rs| rs.len()) {
                    Ok((1, _)) => break,
                    Ok((i, _)) => assert_ne!(i, 1),
                    Err(_) => continue,
                }
            }
        }

        jh.join().unwrap();
    }

    #[test]
    fn minimal_query() {
        let a = vec![1.into(), "a".try_into().unwrap()];
        let b = vec![1.into(), "b".try_into().unwrap()];

        let (r, mut w) = new(2, &[0]);
        w.add(vec![Record::Positive(a.clone())]);
        w.swap();
        w.add(vec![Record::Positive(b)]);

        assert_eq!(r.try_find_and(&a[0..1], |rs| rs.len()).unwrap().0, 1);
        assert!(
            r.try_find_and(&a[0..1], |rs| rs
                .iter()
                .any(|r| r[0] == a[0] && r[1] == a[1]))
                .unwrap()
                .0
        );
    }

    #[test]
    fn non_minimal_query() {
        let a = vec![1.into(), "a".try_into().unwrap()];
        let b = vec![1.into(), "b".try_into().unwrap()];
        let c = vec![1.into(), "c".try_into().unwrap()];

        let (r, mut w) = new(2, &[0]);
        w.add(vec![Record::Positive(a.clone())]);
        w.add(vec![Record::Positive(b.clone())]);
        w.swap();
        w.add(vec![Record::Positive(c)]);

        assert_eq!(r.try_find_and(&a[0..1], |rs| rs.len()).unwrap().0, 2);
        assert!(
            r.try_find_and(&a[0..1], |rs| rs
                .iter()
                .any(|r| r[0] == a[0] && r[1] == a[1]))
                .unwrap()
                .0
        );
        assert!(
            r.try_find_and(&a[0..1], |rs| rs
                .iter()
                .any(|r| r[0] == b[0] && r[1] == b[1]))
                .unwrap()
                .0
        );
    }

    #[test]
    fn absorb_negative_immediate() {
        let a = vec![1.into(), "a".try_into().unwrap()];
        let b = vec![1.into(), "b".try_into().unwrap()];

        let (r, mut w) = new(2, &[0]);
        w.add(vec![Record::Positive(a.clone())]);
        w.add(vec![Record::Positive(b.clone())]);
        w.add(vec![Record::Negative(a.clone())]);
        w.swap();

        assert_eq!(r.try_find_and(&a[0..1], |rs| rs.len()).unwrap().0, 1);
        assert!(
            r.try_find_and(&a[0..1], |rs| rs
                .iter()
                .any(|r| r[0] == b[0] && r[1] == b[1]))
                .unwrap()
                .0
        );
    }

    #[test]
    fn absorb_negative_later() {
        let a = vec![1.into(), "a".try_into().unwrap()];
        let b = vec![1.into(), "b".try_into().unwrap()];

        let (r, mut w) = new(2, &[0]);
        w.add(vec![Record::Positive(a.clone())]);
        w.add(vec![Record::Positive(b.clone())]);
        w.swap();
        w.add(vec![Record::Negative(a.clone())]);
        w.swap();

        assert_eq!(r.try_find_and(&a[0..1], |rs| rs.len()).unwrap().0, 1);
        assert!(
            r.try_find_and(&a[0..1], |rs| rs
                .iter()
                .any(|r| r[0] == b[0] && r[1] == b[1]))
                .unwrap()
                .0
        );
    }

    #[test]
    fn absorb_multi() {
        let a = vec![1.into(), "a".try_into().unwrap()];
        let b = vec![1.into(), "b".try_into().unwrap()];
        let c = vec![1.into(), "c".try_into().unwrap()];

        let (r, mut w) = new(2, &[0]);
        w.add(vec![
            Record::Positive(a.clone()),
            Record::Positive(b.clone()),
        ]);
        w.swap();

        assert_eq!(r.try_find_and(&a[0..1], |rs| rs.len()).unwrap().0, 2);
        assert!(
            r.try_find_and(&a[0..1], |rs| rs
                .iter()
                .any(|r| r[0] == a[0] && r[1] == a[1]))
                .unwrap()
                .0
        );
        assert!(
            r.try_find_and(&a[0..1], |rs| rs
                .iter()
                .any(|r| r[0] == b[0] && r[1] == b[1]))
                .unwrap()
                .0
        );

        w.add(vec![
            Record::Negative(a.clone()),
            Record::Positive(c.clone()),
            Record::Negative(c),
        ]);
        w.swap();

        assert_eq!(r.try_find_and(&a[0..1], |rs| rs.len()).unwrap().0, 1);
        assert!(
            r.try_find_and(&a[0..1], |rs| rs
                .iter()
                .any(|r| r[0] == b[0] && r[1] == b[1]))
                .unwrap()
                .0
        );
    }

    #[test]
    fn find_missing_partial() {
        let (r, mut w) = new_partial(1, &[0], |_: &mut dyn Iterator<Item = &KeyComparison>| true);
        w.swap();

        assert_eq!(
            r.try_find_and(&[1.into()], |rs| rs.len()),
            Err(LookupError::MissPointSingle(1.into(), -1))
        );
    }

    mod mark_filled {
        use super::*;
        use vec1::vec1;

        #[test]
        fn point() {
            let (r, mut w) =
                new_partial(1, &[0], |_: &mut dyn Iterator<Item = &KeyComparison>| true);
            w.swap();

            let key = vec1![DataType::from(0)];
            assert!(r.try_find_and(&key, |_| ()).err().unwrap().is_miss());

            w.mark_filled(key.clone().into());
            w.swap();
            assert!(r.try_find_and(&key, |_| ()).is_ok());
        }

        #[test]
        fn range() {
            let (r, mut w) =
                new_partial(1, &[0], |_: &mut dyn Iterator<Item = &KeyComparison>| true);
            w.swap();

            let range = vec![DataType::from(0)]..vec![DataType::from(10)];
            assert!(r
                .try_find_range_and(&range, |_| ())
                .err()
                .unwrap()
                .is_miss());

            w.mark_filled(KeyComparison::from_range(
                &(vec1![DataType::from(0)]..vec1![DataType::from(10)]),
            ));
            w.swap();
            assert!(r.try_find_range_and(&range, |_| ()).is_ok());
        }
    }

    mod mark_hole {
        use super::*;
        use vec1::vec1;

        #[test]
        fn point() {
            let (r, mut w) =
                new_partial(1, &[0], |_: &mut dyn Iterator<Item = &KeyComparison>| true);
            w.swap();

            let key = vec1![DataType::from(0)];
            w.mark_filled(key.clone().into());
            w.swap();
            assert!(r.try_find_and(&key, |_| ()).is_ok());

            w.mark_hole(&key.clone().into());
            w.swap();
            assert!(r.try_find_and(&key, |_| ()).err().unwrap().is_miss());
        }

        #[test]
        fn range() {
            let (r, mut w) =
                new_partial(1, &[0], |_: &mut dyn Iterator<Item = &KeyComparison>| true);
            w.swap();

            let range = vec![DataType::from(0)]..vec![DataType::from(10)];
            w.mark_filled(KeyComparison::from_range(
                &(vec1![DataType::from(0)]..vec1![DataType::from(10)]),
            ));
            w.swap();
            assert!(r.try_find_range_and(&range, |_| ()).is_ok());

            w.mark_hole(&KeyComparison::from_range(
                &(vec1![DataType::from(0)]..vec1![DataType::from(10)]),
            ));
            w.swap();
            assert!(r
                .try_find_range_and(&range, |_| ())
                .err()
                .unwrap()
                .is_miss());
        }
    }
}
