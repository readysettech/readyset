use ops::Record;
use query::DataType;
use fnv::FnvBuildHasher;
use evmap;

use std::sync::Arc;

/// Allocate a new buffered `Store`.
pub fn new(cols: usize, key: usize) -> (ReadHandle, WriteHandle) {
    let (r, w) = evmap::Options::default()
        .with_meta(-1)
        .with_hasher(FnvBuildHasher::default())
        .construct();
    let r = ReadHandle {
        handle: r,
        key: key,
    };
    let w = WriteHandle {
        handle: w,
        key: key,
        cols: cols,
    };
    (r, w)
}

pub struct WriteHandle {
    handle: evmap::WriteHandle<DataType, Arc<Vec<DataType>>, i64, FnvBuildHasher>,
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
        where I: IntoIterator<Item = Record>
    {
        for r in rs {
            debug_assert_eq!(r.len(), self.cols);
            let key = r[self.key].clone();
            match r {
                Record::Positive(r) => {
                    self.handle.insert(key, r);
                }
                Record::Negative(r) => {
                    self.handle.remove(key, r);
                }

            }
        }
    }

    pub fn update_ts(&mut self, ts: i64) {
        self.handle.set_meta(ts);
    }
}

#[derive(Clone)]
pub struct ReadHandle {
    handle: evmap::ReadHandle<DataType, Arc<Vec<DataType>>, i64, FnvBuildHasher>,
    key: usize,
}

impl ReadHandle {
    /// Find all entries that matched the given conditions.
    ///
    /// Returned records are passed to `then` before being returned.
    ///
    /// Note that not all writes will be included with this read -- only those that have been
    /// swapped in by the writer.
    pub fn find_and<F, T>(&self, key: &DataType, then: F) -> Result<(T, i64), ()>
        where F: FnOnce(&[Arc<Vec<DataType>>]) -> T
    {
        self.handle.meta_get_and(key, then).ok_or(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn store_works() {
        let a = Arc::new(vec![1.into(), "a".into()]);

        let (r, mut w) = new(2, 0);

        // initially, store is uninitialized
        assert_eq!(r.find_and(&a[0], |rs| rs.len()), Err(()));

        w.swap();

        // after first swap, it is empty, but ready
        assert_eq!(r.find_and(&a[0], |rs| rs.len()), Ok((0, -1)));

        w.add(vec![Record::Positive(a.clone())]);

        // it is empty even after an add (we haven't swapped yet)
        assert_eq!(r.find_and(&a[0], |rs| rs.len()), Ok((0, -1)));

        w.swap();

        // but after the swap, the record is there!
        assert_eq!(r.find_and(&a[0], |rs| rs.len()).unwrap().0, 1);
        assert!(r.find_and(&a[0], |rs| rs.iter().any(|r| r[0] == a[0] && r[1] == a[1])).unwrap().0);
    }

    #[test]
    fn busybusybusy() {
        use std::thread;

        let n = 10000;
        let (r, mut w) = new(1, 0);
        thread::spawn(move || for i in 0..n {
            w.add(vec![Record::Positive(Arc::new(vec![i.into()]))]);
            w.swap();
        });

        for i in 0..n {
            let i = i.into();
            loop {
                match r.find_and(&i, |rs| rs.len()) {
                    Ok((0, _)) => continue,
                    Ok((1, _)) => break,
                    Ok((i, _)) => assert_ne!(i, 1),
                    Err(()) => continue,
                }
            }
        }
    }

    #[test]
    fn minimal_query() {
        let a = Arc::new(vec![1.into(), "a".into()]);
        let b = Arc::new(vec![1.into(), "b".into()]);

        let (r, mut w) = new(2, 0);
        w.add(vec![Record::Positive(a.clone())]);
        w.swap();
        w.add(vec![Record::Positive(b.clone())]);

        assert_eq!(r.find_and(&a[0], |rs| rs.len()).unwrap().0, 1);
        assert!(r.find_and(&a[0], |rs| rs.iter().any(|r| r[0] == a[0] && r[1] == a[1])).unwrap().0);
    }

    #[test]
    fn non_minimal_query() {
        let a = Arc::new(vec![1.into(), "a".into()]);
        let b = Arc::new(vec![1.into(), "b".into()]);
        let c = Arc::new(vec![1.into(), "c".into()]);

        let (r, mut w) = new(2, 0);
        w.add(vec![Record::Positive(a.clone())]);
        w.add(vec![Record::Positive(b.clone())]);
        w.swap();
        w.add(vec![Record::Positive(c.clone())]);

        assert_eq!(r.find_and(&a[0], |rs| rs.len()).unwrap().0, 2);
        assert!(r.find_and(&a[0], |rs| rs.iter().any(|r| r[0] == a[0] && r[1] == a[1])).unwrap().0);
        assert!(r.find_and(&a[0], |rs| rs.iter().any(|r| r[0] == b[0] && r[1] == b[1])).unwrap().0);
    }

    #[test]
    fn absorb_negative_immediate() {
        let a = Arc::new(vec![1.into(), "a".into()]);
        let b = Arc::new(vec![1.into(), "b".into()]);

        let (r, mut w) = new(2, 0);
        w.add(vec![Record::Positive(a.clone())]);
        w.add(vec![Record::Positive(b.clone())]);
        w.add(vec![Record::Negative(a.clone())]);
        w.swap();

        assert_eq!(r.find_and(&a[0], |rs| rs.len()).unwrap().0, 1);
        assert!(r.find_and(&a[0], |rs| rs.iter().any(|r| r[0] == b[0] && r[1] == b[1])).unwrap().0);
    }

    #[test]
    fn absorb_negative_later() {
        let a = Arc::new(vec![1.into(), "a".into()]);
        let b = Arc::new(vec![1.into(), "b".into()]);

        let (r, mut w) = new(2, 0);
        w.add(vec![Record::Positive(a.clone())]);
        w.add(vec![Record::Positive(b.clone())]);
        w.swap();
        w.add(vec![Record::Negative(a.clone())]);
        w.swap();

        assert_eq!(r.find_and(&a[0], |rs| rs.len()).unwrap().0, 1);
        assert!(r.find_and(&a[0], |rs| rs.iter().any(|r| r[0] == b[0] && r[1] == b[1])).unwrap().0);
    }

    #[test]
    fn absorb_multi() {
        let a = Arc::new(vec![1.into(), "a".into()]);
        let b = Arc::new(vec![1.into(), "b".into()]);
        let c = Arc::new(vec![1.into(), "c".into()]);

        let (r, mut w) = new(2, 0);
        w.add(vec![Record::Positive(a.clone()), Record::Positive(b.clone())]);
        w.swap();

        assert_eq!(r.find_and(&a[0], |rs| rs.len()).unwrap().0, 2);
        assert!(r.find_and(&a[0], |rs| rs.iter().any(|r| r[0] == a[0] && r[1] == a[1])).unwrap().0);
        assert!(r.find_and(&a[0], |rs| rs.iter().any(|r| r[0] == b[0] && r[1] == b[1])).unwrap().0);

        w.add(vec![Record::Negative(a.clone()),
                   Record::Positive(c.clone()),
                   Record::Negative(c.clone())]);
        w.swap();

        assert_eq!(r.find_and(&a[0], |rs| rs.len()).unwrap().0, 1);
        assert!(r.find_and(&a[0], |rs| rs.iter().any(|r| r[0] == b[0] && r[1] == b[1])).unwrap().0);
    }
}
