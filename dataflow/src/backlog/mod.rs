use core::{DataType, Record};
use core::data::SizeOf;
use fnv::FnvBuildHasher;
use std::borrow::Cow;
use evmap;

use rand::{Rng, ThreadRng};
use std::sync::Arc;

/// Allocate a new end-user facing result table.
pub(crate) fn new(cols: usize, key: &[usize]) -> (SingleReadHandle, WriteHandle) {
    new_inner(cols, key, None)
}

/// Allocate a new partially materialized end-user facing result table.
///
/// Misses in this table will call `trigger` to populate the entry, and retry until successful.
pub(crate) fn new_partial<F>(
    cols: usize,
    key: &[usize],
    trigger: F,
) -> (SingleReadHandle, WriteHandle)
where
    F: Fn(&[DataType]) + 'static + Send + Sync,
{
    new_inner(cols, key, Some(Arc::new(trigger)))
}

fn new_inner(
    cols: usize,
    key: &[usize],
    trigger: Option<Arc<Fn(&[DataType]) + Send + Sync>>,
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
            use evmap;
            let (r, w) = evmap::Options::default()
                .with_meta(-1)
                .with_hasher(FnvBuildHasher::default())
                .construct();

            (multir::Handle::$variant(r), multiw::Handle::$variant(w))
        }};
    }

    let (r, w) = match key.len() {
        0 => unreachable!(),
        1 => make!(Single),
        2 => make!(Double),
        _ => make!(Many),
    };

    let w = WriteHandle {
        partial: trigger.is_some(),
        handle: w,
        key: Vec::from(key),
        cols: cols,
        contiguous,
        mem_size: 0,
    };
    let r = SingleReadHandle {
        handle: r,
        trigger: trigger,
        key: Vec::from(key),
    };

    (r, w)
}

mod multir;
mod multiw;

fn key_to_single<'a>(k: Key<'a>) -> Cow<'a, DataType> {
    assert_eq!(k.len(), 1);
    match k {
        Cow::Owned(mut k) => Cow::Owned(k.swap_remove(0)),
        Cow::Borrowed(k) => Cow::Borrowed(&k[0]),
    }
}

fn key_to_double<'a>(k: Key<'a>) -> Cow<'a, (DataType, DataType)> {
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
    handle: &'a mut multiw::Handle,
    key: Key<'a>,
}
pub(crate) struct WriteHandleEntry<'a> {
    handle: &'a multiw::Handle,
    key: Key<'a>,
}

impl<'a> MutWriteHandleEntry<'a> {
    pub fn mark_filled(self) {
        self.handle.clear(self.key)
    }

    pub fn mark_hole(self) {
        self.handle.empty(self.key)
    }
}

impl<'a> WriteHandleEntry<'a> {
    pub(crate) fn try_find_and<F, T>(self, mut then: F) -> Result<(Option<T>, i64), ()>
    where
        F: FnMut(&[Vec<DataType>]) -> T,
    {
        self.handle.meta_get_and(self.key, &mut then).ok_or(())
    }
}

fn key_from_record<'a, R>(key: &[usize], contiguous: bool, record: R) -> Key<'a>
where
    R: Into<Cow<'a, [DataType]>>,
{
    match record.into() {
        Cow::Owned(mut record) => {
            let mut i = 0;
            let mut keep = key.into_iter().peekable();
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
            handle: &mut self.handle,
            key: key.into(),
        }
    }

    pub(crate) fn with_key<'a, K>(&'a self, key: K) -> WriteHandleEntry<'a>
    where
        K: Into<Key<'a>>,
    {
        WriteHandleEntry {
            handle: &self.handle,
            key: key.into(),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn mut_entry_from_record<'a, R>(&'a mut self, record: R) -> MutWriteHandleEntry<'a>
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
        if mem_delta > 0 {
            self.mem_size += mem_delta as usize;
        } else if mem_delta < 0 {
            self.mem_size = self.mem_size.checked_sub(mem_delta as usize).unwrap();
        }
    }

    pub(crate) fn update_ts(&mut self, ts: i64) {
        self.handle.set_meta(ts);
    }

    pub(crate) fn is_partial(&self) -> bool {
        self.partial
    }

    /// Evict `count` randomly selected keys from state and return them along with the number of
    /// bytes freed.
    pub fn evict_random_keys(&mut self, count: usize, rng: &mut ThreadRng) -> u64 {
        let mut bytes_freed = 0;
        for _ in 0..count {
            match self.handle.empty_at_index(rng.gen()) {
                None => continue,
                Some((_k, vs)) => {
                    let size: u64 = vs.into_iter().map(|r| r.deep_size_of() as u64).sum();
                    bytes_freed += size;
                }
            }
        }
        bytes_freed
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
}

/// Handle to get the state of a single shard of a reader.
#[derive(Clone)]
pub struct SingleReadHandle {
    handle: multir::Handle,
    trigger: Option<Arc<Fn(&[DataType]) + Send + Sync>>,
    key: Vec<usize>,
}

impl SingleReadHandle {
    /// Find all entries that matched the given conditions.
    ///
    /// Returned records are passed to `then` before being returned.
    ///
    /// Note that not all writes will be included with this read -- only those that have been
    /// swapped in by the writer.
    ///
    /// If `block` is set, this call will block if it encounters a hole in partially materialized
    /// state.
    pub fn find_and<F, T>(
        &self,
        key: &[DataType],
        mut then: F,
        block: bool,
    ) -> Result<(Option<T>, i64), ()>
    where
        F: FnMut(&[Vec<DataType>]) -> T,
    {
        match self.try_find_and(key, &mut then) {
            Ok((None, ts)) if self.trigger.is_some() => {
                if let Some(ref trigger) = self.trigger {
                    use std::thread;

                    // trigger a replay to populate
                    (*trigger)(key);

                    if block {
                        // wait for result to come through
                        loop {
                            thread::yield_now();
                            match self.try_find_and(key, &mut then) {
                                Ok((None, _)) => {}
                                r => return r,
                            }
                        }
                    } else {
                        Ok((None, ts))
                    }
                } else {
                    unreachable!()
                }
            }
            r => r,
        }
    }

    pub(crate) fn try_find_and<F, T>(
        &self,
        key: &[DataType],
        mut then: F,
    ) -> Result<(Option<T>, i64), ()>
    where
        F: FnMut(&[Vec<DataType>]) -> T,
    {
        self.handle.meta_get_and(key, &mut then).ok_or(())
    }

    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.handle.len()
    }

    /// Count the number of rows in the reader.
    /// This is a potentially very costly operation, since it will
    /// hold up writers until all rows are iterated through.
    pub fn count_rows(&self) -> usize {
        let mut nrows = 0;
        self.handle.for_each(|v| nrows += v.len());
        nrows
    }
}

#[derive(Clone)]
pub enum ReadHandle {
    Sharded(Vec<Option<SingleReadHandle>>),
    Singleton(Option<SingleReadHandle>),
}

impl ReadHandle {
    /// Find all entries that matched the given conditions.
    ///
    /// Returned records are passed to `then` before being returned.
    ///
    /// Note that not all writes will be included with this read -- only those that have been
    /// swapped in by the writer.
    ///
    /// If `block` is set, this call will block if it encounters a hole in partially materialized
    /// state.
    pub fn find_and<F, T>(
        &self,
        key: &[DataType],
        then: F,
        block: bool,
    ) -> Result<(Option<T>, i64), ()>
    where
        F: FnMut(&[Vec<DataType>]) -> T,
    {
        match *self {
            ReadHandle::Sharded(ref shards) => {
                assert_eq!(key.len(), 1);
                shards[::shard_by(&key[0], shards.len())]
                    .as_ref()
                    .unwrap()
                    .find_and(key, then, block)
            }
            ReadHandle::Singleton(ref srh) => srh.as_ref().unwrap().find_and(key, then, block),
        }
    }

    #[allow(dead_code)]
    pub fn try_find_and<F, T>(&self, key: &[DataType], then: F) -> Result<(Option<T>, i64), ()>
    where
        F: FnMut(&[Vec<DataType>]) -> T,
    {
        match *self {
            ReadHandle::Sharded(ref shards) => {
                assert_eq!(key.len(), 1);
                shards[::shard_by(&key[0], shards.len())]
                    .as_ref()
                    .unwrap()
                    .try_find_and(key, then)
            }
            ReadHandle::Singleton(ref srh) => srh.as_ref().unwrap().try_find_and(key, then),
        }
    }

    pub fn len(&self) -> usize {
        match *self {
            ReadHandle::Sharded(ref shards) => {
                shards.iter().map(|s| s.as_ref().unwrap().len()).sum()
            }
            ReadHandle::Singleton(ref srh) => srh.as_ref().unwrap().len(),
        }
    }

    pub fn set_single_handle(&mut self, shard: Option<usize>, handle: SingleReadHandle) {
        match (self, shard) {
            (&mut ReadHandle::Singleton(ref mut srh), None) => {
                *srh = Some(handle);
            }
            (&mut ReadHandle::Sharded(ref mut rhs), None) => {
                // when ::SHARDS == 1, sharded domains think they're unsharded
                assert_eq!(rhs.len(), 1);
                let srh = rhs.get_mut(0).unwrap();
                assert!(srh.is_none());
                *srh = Some(handle)
            }
            (&mut ReadHandle::Sharded(ref mut rhs), Some(shard)) => {
                let srh = rhs.get_mut(shard).unwrap();
                assert!(srh.is_none());
                *srh = Some(handle)
            }
            _ => unreachable!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn store_works() {
        let a = vec![1.into(), "a".into()];

        let (r, mut w) = new(2, &[0]);

        // initially, store is uninitialized
        assert_eq!(r.find_and(&a[0..1], |rs| rs.len(), true), Err(()));

        w.swap();

        // after first swap, it is empty, but ready
        assert_eq!(r.find_and(&a[0..1], |rs| rs.len(), true), Ok((None, -1)));

        w.add(vec![Record::Positive(a.clone())]);

        // it is empty even after an add (we haven't swapped yet)
        assert_eq!(r.find_and(&a[0..1], |rs| rs.len(), true), Ok((None, -1)));

        w.swap();

        // but after the swap, the record is there!
        assert_eq!(
            r.find_and(&a[0..1], |rs| rs.len(), true).unwrap().0,
            Some(1)
        );
        assert!(
            r.find_and(
                &a[0..1],
                |rs| rs.iter().any(|r| r[0] == a[0] && r[1] == a[1]),
                true
            ).unwrap()
                .0
                .unwrap()
        );
    }

    #[test]
    fn busybusybusy() {
        use std::thread;

        let n = 10000;
        let (r, mut w) = new(1, &[0]);
        thread::spawn(move || {
            for i in 0..n {
                w.add(vec![Record::Positive(vec![i.into()])]);
                w.swap();
            }
        });

        for i in 0..n {
            let i = &[i.into()];
            loop {
                match r.find_and(i, |rs| rs.len(), true) {
                    Ok((None, _)) => continue,
                    Ok((Some(1), _)) => break,
                    Ok((Some(i), _)) => assert_ne!(i, 1),
                    Err(()) => continue,
                }
            }
        }
    }

    #[test]
    fn minimal_query() {
        let a = vec![1.into(), "a".into()];
        let b = vec![1.into(), "b".into()];

        let (r, mut w) = new(2, &[0]);
        w.add(vec![Record::Positive(a.clone())]);
        w.swap();
        w.add(vec![Record::Positive(b.clone())]);

        assert_eq!(
            r.find_and(&a[0..1], |rs| rs.len(), true).unwrap().0,
            Some(1)
        );
        assert!(
            r.find_and(
                &a[0..1],
                |rs| rs.iter().any(|r| r[0] == a[0] && r[1] == a[1]),
                true
            ).unwrap()
                .0
                .unwrap()
        );
    }

    #[test]
    fn non_minimal_query() {
        let a = vec![1.into(), "a".into()];
        let b = vec![1.into(), "b".into()];
        let c = vec![1.into(), "c".into()];

        let (r, mut w) = new(2, &[0]);
        w.add(vec![Record::Positive(a.clone())]);
        w.add(vec![Record::Positive(b.clone())]);
        w.swap();
        w.add(vec![Record::Positive(c.clone())]);

        assert_eq!(
            r.find_and(&a[0..1], |rs| rs.len(), true).unwrap().0,
            Some(2)
        );
        assert!(
            r.find_and(
                &a[0..1],
                |rs| rs.iter().any(|r| r[0] == a[0] && r[1] == a[1]),
                true
            ).unwrap()
                .0
                .unwrap()
        );
        assert!(
            r.find_and(
                &a[0..1],
                |rs| rs.iter().any(|r| r[0] == b[0] && r[1] == b[1]),
                true
            ).unwrap()
                .0
                .unwrap()
        );
    }

    #[test]
    fn absorb_negative_immediate() {
        let a = vec![1.into(), "a".into()];
        let b = vec![1.into(), "b".into()];

        let (r, mut w) = new(2, &[0]);
        w.add(vec![Record::Positive(a.clone())]);
        w.add(vec![Record::Positive(b.clone())]);
        w.add(vec![Record::Negative(a.clone())]);
        w.swap();

        assert_eq!(
            r.find_and(&a[0..1], |rs| rs.len(), true).unwrap().0,
            Some(1)
        );
        assert!(
            r.find_and(
                &a[0..1],
                |rs| rs.iter().any(|r| r[0] == b[0] && r[1] == b[1]),
                true
            ).unwrap()
                .0
                .unwrap()
        );
    }

    #[test]
    fn absorb_negative_later() {
        let a = vec![1.into(), "a".into()];
        let b = vec![1.into(), "b".into()];

        let (r, mut w) = new(2, &[0]);
        w.add(vec![Record::Positive(a.clone())]);
        w.add(vec![Record::Positive(b.clone())]);
        w.swap();
        w.add(vec![Record::Negative(a.clone())]);
        w.swap();

        assert_eq!(
            r.find_and(&a[0..1], |rs| rs.len(), true).unwrap().0,
            Some(1)
        );
        assert!(
            r.find_and(
                &a[0..1],
                |rs| rs.iter().any(|r| r[0] == b[0] && r[1] == b[1]),
                true
            ).unwrap()
                .0
                .unwrap()
        );
    }

    #[test]
    fn absorb_multi() {
        let a = vec![1.into(), "a".into()];
        let b = vec![1.into(), "b".into()];
        let c = vec![1.into(), "c".into()];

        let (r, mut w) = new(2, &[0]);
        w.add(vec![
            Record::Positive(a.clone()),
            Record::Positive(b.clone()),
        ]);
        w.swap();

        assert_eq!(
            r.find_and(&a[0..1], |rs| rs.len(), true).unwrap().0,
            Some(2)
        );
        assert!(
            r.find_and(
                &a[0..1],
                |rs| rs.iter().any(|r| r[0] == a[0] && r[1] == a[1]),
                true
            ).unwrap()
                .0
                .unwrap()
        );
        assert!(
            r.find_and(
                &a[0..1],
                |rs| rs.iter().any(|r| r[0] == b[0] && r[1] == b[1]),
                true
            ).unwrap()
                .0
                .unwrap()
        );

        w.add(vec![
            Record::Negative(a.clone()),
            Record::Positive(c.clone()),
            Record::Negative(c.clone()),
        ]);
        w.swap();

        assert_eq!(
            r.find_and(&a[0..1], |rs| rs.len(), true).unwrap().0,
            Some(1)
        );
        assert!(
            r.find_and(
                &a[0..1],
                |rs| rs.iter().any(|r| r[0] == b[0] && r[1] == b[1]),
                true
            ).unwrap()
                .0
                .unwrap()
        );
    }
}
