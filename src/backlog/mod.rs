use flow::core::{DataType, Record};
use fnv::FnvBuildHasher;
use evmap;
use arrayvec::ArrayVec;

use std::sync::Arc;

/// Allocate a new end-user facing result table.
pub fn new(cols: usize, key: usize) -> (SingleReadHandle, WriteHandle) {
    new_inner(cols, key, None)
}

/// Allocate a new partially materialized end-user facing result table.
///
/// Misses in this table will call `trigger` to populate the entry, and retry until successful.
pub fn new_partial<F>(cols: usize, key: usize, trigger: F) -> (SingleReadHandle, WriteHandle)
where
    F: Fn(&DataType) + 'static + Send + Sync,
{
    new_inner(cols, key, Some(Arc::new(trigger)))
}

fn new_inner(
    cols: usize,
    key: usize,
    trigger: Option<Arc<Fn(&DataType) + Send + Sync>>,
) -> (SingleReadHandle, WriteHandle) {
    let (r, w) = evmap::Options::default()
        .with_meta(-1)
        .with_hasher(FnvBuildHasher::default())
        .construct();
    let w = WriteHandle {
        partial: trigger.is_some(),
        handle: w,
        key: key,
        cols: cols,
    };
    let r = SingleReadHandle {
        handle: r,
        trigger: trigger,
        key: key,
    };
    (r, w)
}

pub struct WriteHandle {
    handle: evmap::WriteHandle<DataType, Arc<Vec<DataType>>, i64, FnvBuildHasher>,
    partial: bool,
    cols: usize,
    key: usize,
}

impl WriteHandle {
    pub fn swap(&mut self) {
        self.handle.refresh();
    }

    /// Add a new set of records to the backlog.
    ///
    /// These will be made visible to readers after the next call to `swap()`.
    pub fn add<I>(&mut self, rs: I)
    where
        I: IntoIterator<Item = Record>,
    {
        for r in rs {
            debug_assert_eq!(r.len(), self.cols);
            let key = r[self.key].clone();
            match r {
                Record::Positive(r) => {
                    self.handle.insert(key, Arc::new(r));
                }
                Record::Negative(r) => {
                    // TODO: evmap will remove the empty vec for a key if we remove the last
                    // record. this means that future lookups will fail, and cause a replay, which
                    // will produce an empty result. this will work, but is somewhat inefficient.
                    self.handle.remove(key, Arc::new(r));
                }
                Record::DeleteRequest(..) => unreachable!(),
            }
        }
    }

    pub fn update_ts(&mut self, ts: i64) {
        self.handle.set_meta(ts);
    }

    pub fn mark_filled(&mut self, key: &DataType) {
        self.handle.clear(key.clone());
    }

    pub fn mark_hole(&mut self, key: &DataType) {
        self.handle.empty(key.clone());
    }

    pub fn try_find_and<F, T>(&self, key: &DataType, mut then: F) -> Result<(Option<T>, i64), ()>
    where
        F: FnMut(&[Arc<Vec<DataType>>]) -> T,
    {
        self.handle.meta_get_and(key, &mut then).ok_or(())
    }

    pub fn key(&self) -> usize {
        self.key
    }

    pub fn len(&self) -> usize {
        self.handle.len()
    }

    pub fn is_partial(&self) -> bool {
        self.partial
    }
}

/// Handle to get the state of a single shard of a reader.
#[derive(Clone)]
pub struct SingleReadHandle {
    handle: evmap::ReadHandle<DataType, Arc<Vec<DataType>>, i64, FnvBuildHasher>,
    trigger: Option<Arc<Fn(&DataType) + Send + Sync>>,
    key: usize,
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
        key: &DataType,
        mut then: F,
        block: bool,
    ) -> Result<(Option<T>, i64), ()>
    where
        F: FnMut(&[Arc<Vec<DataType>>]) -> T,
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
        key: &DataType,
        mut then: F,
    ) -> Result<(Option<T>, i64), ()>
    where
        F: FnMut(&[Arc<Vec<DataType>>]) -> T,
    {
        self.handle.meta_get_and(key, &mut then).ok_or(())
    }

    #[allow(dead_code)]
    pub(crate) fn len(&self) -> usize {
        self.handle.len()
    }
}

#[derive(Clone)]
pub enum ReadHandle {
    Sharded(ArrayVec<[Option<SingleReadHandle>; ::SHARDS]>),
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
        key: &DataType,
        then: F,
        block: bool,
    ) -> Result<(Option<T>, i64), ()>
    where
        F: FnMut(&[Arc<Vec<DataType>>]) -> T,
    {
        match *self {
            ReadHandle::Sharded(ref shards) => shards[::shard_by(key, shards.len())]
                .as_ref()
                .unwrap()
                .find_and(key, then, block),
            ReadHandle::Singleton(ref srh) => srh.as_ref().unwrap().find_and(key, then, block),
        }
    }

    #[allow(dead_code)]
    pub fn try_find_and<F, T>(&self, key: &DataType, then: F) -> Result<(Option<T>, i64), ()>
    where
        F: FnMut(&[Arc<Vec<DataType>>]) -> T,
    {
        match *self {
            ReadHandle::Sharded(ref shards) => shards[::shard_by(key, shards.len())]
                .as_ref()
                .unwrap()
                .try_find_and(key, then),
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

        let (r, mut w) = new(2, 0);

        // initially, store is uninitialized
        assert_eq!(r.find_and(&a[0], |rs| rs.len(), true), Err(()));

        w.swap();

        // after first swap, it is empty, but ready
        assert_eq!(r.find_and(&a[0], |rs| rs.len(), true), Ok((None, -1)));

        w.add(vec![Record::Positive(a.clone())]);

        // it is empty even after an add (we haven't swapped yet)
        assert_eq!(r.find_and(&a[0], |rs| rs.len(), true), Ok((None, -1)));

        w.swap();

        // but after the swap, the record is there!
        assert_eq!(r.find_and(&a[0], |rs| rs.len(), true).unwrap().0, Some(1));
        assert!(
            r.find_and(
                &a[0],
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
        let (r, mut w) = new(1, 0);
        thread::spawn(move || for i in 0..n {
            w.add(vec![Record::Positive(vec![i.into()])]);
            w.swap();
        });

        for i in 0..n {
            let i = i.into();
            loop {
                match r.find_and(&i, |rs| rs.len(), true) {
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

        let (r, mut w) = new(2, 0);
        w.add(vec![Record::Positive(a.clone())]);
        w.swap();
        w.add(vec![Record::Positive(b.clone())]);

        assert_eq!(r.find_and(&a[0], |rs| rs.len(), true).unwrap().0, Some(1));
        assert!(
            r.find_and(
                &a[0],
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

        let (r, mut w) = new(2, 0);
        w.add(vec![Record::Positive(a.clone())]);
        w.add(vec![Record::Positive(b.clone())]);
        w.swap();
        w.add(vec![Record::Positive(c.clone())]);

        assert_eq!(r.find_and(&a[0], |rs| rs.len(), true).unwrap().0, Some(2));
        assert!(
            r.find_and(
                &a[0],
                |rs| rs.iter().any(|r| r[0] == a[0] && r[1] == a[1]),
                true
            ).unwrap()
                .0
                .unwrap()
        );
        assert!(
            r.find_and(
                &a[0],
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

        let (r, mut w) = new(2, 0);
        w.add(vec![Record::Positive(a.clone())]);
        w.add(vec![Record::Positive(b.clone())]);
        w.add(vec![Record::Negative(a.clone())]);
        w.swap();

        assert_eq!(r.find_and(&a[0], |rs| rs.len(), true).unwrap().0, Some(1));
        assert!(
            r.find_and(
                &a[0],
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

        let (r, mut w) = new(2, 0);
        w.add(vec![Record::Positive(a.clone())]);
        w.add(vec![Record::Positive(b.clone())]);
        w.swap();
        w.add(vec![Record::Negative(a.clone())]);
        w.swap();

        assert_eq!(r.find_and(&a[0], |rs| rs.len(), true).unwrap().0, Some(1));
        assert!(
            r.find_and(
                &a[0],
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

        let (r, mut w) = new(2, 0);
        w.add(vec![
            Record::Positive(a.clone()),
            Record::Positive(b.clone()),
        ]);
        w.swap();

        assert_eq!(r.find_and(&a[0], |rs| rs.len(), true).unwrap().0, Some(2));
        assert!(
            r.find_and(
                &a[0],
                |rs| rs.iter().any(|r| r[0] == a[0] && r[1] == a[1]),
                true
            ).unwrap()
                .0
                .unwrap()
        );
        assert!(
            r.find_and(
                &a[0],
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

        assert_eq!(r.find_and(&a[0], |rs| rs.len(), true).unwrap().0, Some(1));
        assert!(
            r.find_and(
                &a[0],
                |rs| rs.iter().any(|r| r[0] == b[0] && r[1] == b[1]),
                true
            ).unwrap()
                .0
                .unwrap()
        );
    }
}
