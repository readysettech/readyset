use ops;
use query;
use fnv::FnvHashMap;

use std::borrow::Cow;
use std::sync;
use std::sync::atomic;
use std::sync::atomic::AtomicPtr;

type S = (FnvHashMap<query::DataType, Vec<sync::Arc<Vec<query::DataType>>>>, i64, bool);
pub struct WriteHandle {
    w_store: Option<Box<sync::Arc<S>>>,
    w_log: Vec<ops::Record>,
    bs: BufferedStore,
    cols: usize,
    key: usize,
    first: bool,
}

#[derive(Clone)]
pub struct BufferedStore {
    store: sync::Arc<AtomicPtr<sync::Arc<S>>>,
    key: usize,
}

pub struct BufferedStoreBuilder {
    r_store: S,
    w_store: S,
    key: usize,
    cols: usize,
}

impl WriteHandle {
    pub fn swap(&mut self) {
        use std::thread;

        // at this point, we have exclusive access to w_store, and it is up-to-date with all writes
        // r_store is accessed by readers through a sync::Weak upgrade, and has old data
        // w_log contains all the changes that are in w_store, but not in r_store
        //
        // we're going to do the following:
        //
        //  - atomically swap in a weak pointer to the current w_store into the BufferedStore,
        //    letting readers see new and updated state
        //  - store r_store as our new w_store
        //  - wait until we have exclusive access to this new w_store
        //  - replay w_log onto w_store

        // prepare w_store
        let w_store = self.w_store.take().unwrap();
        let w_ts = w_store.1;
        let w_store_clone = if self.first {
            // this is the *first* swap, clone current w_store instead of insterting all the rows
            // one-by-one
            self.first = false;
            Some(w_store.0.clone())
        } else {
            None
        };
        let w_store: *mut sync::Arc<S> = Box::into_raw(w_store);

        // swap in our w_store, and get r_store in return
        let r_store = self.bs.store.swap(w_store, atomic::Ordering::SeqCst);
        self.w_store = Some(unsafe { Box::from_raw(r_store) });

        // let readers go so they will be done with the old read Arc
        thread::yield_now();

        // now, wait for all existing readers to go away
        loop {
            if let Some(w_store) = sync::Arc::get_mut(&mut *self.w_store.as_mut().unwrap()) {
                // they're all gone
                // OR ARE THEY?
                // some poor reader could have *read* the pointer right before we swapped it,
                // *but not yet cloned the Arc*. we then check that there's only one strong
                // reference, *which there is*. *then* that reader upgrades their Arc => Uh-oh.
                // *however*, because of the second pointer read in readers, we know that a reader
                // will detect this case, and simply refuse to use the Arc it cloned. therefore, it
                // is safe for us to start mutating here

                // put in all the updates the read store hasn't seen
                if let Some(old_w_store) = w_store_clone {
                    w_store.0 = old_w_store;
                } else {
                    for r in self.w_log.drain(..) {
                        Self::apply(w_store, self.key, Cow::Owned(r));
                    }
                }
                w_store.1 = w_ts;
                w_store.2 = true;

                // w_store (the old r_store) is now fully up to date!
                break;
            } else {
                thread::yield_now();
            }
        }
    }

    /// Add a new set of records to the backlog.
    ///
    /// These will be made visible to readers after the next call to `swap()`.
    pub fn add<I>(&mut self, rs: I)
        where I: IntoIterator<Item = ops::Record>
    {
        for r in rs {
            // apply to the current write set
            debug_assert_eq!(r.len(), self.cols);
            {
                let arc: &mut sync::Arc<_> = &mut *self.w_store.as_mut().unwrap();
                loop {
                    if let Some(s) = sync::Arc::get_mut(arc) {
                        Self::apply(s, self.key, Cow::Borrowed(&r));
                        break;
                    } else {
                        // writer should always be sole owner outside of swap
                        // *however*, there may have been a reader who read the arc pointer before
                        // the atomic pointer swap, and cloned *after* the Arc::get_mut call in
                        // swap(), so we could still end up here. we know that the reader will
                        // detect its mistake and drop that Arc (without using it), so we
                        // eventually end up with a unique Arc. In fact, because we know the reader
                        // will never use the Arc in that case, we *could* just start mutating
                        // straight away, but unfortunately Arc doesn't provide an API to force
                        // this.
                        use std::thread;
                        thread::yield_now();
                    }
                }
            }

            // and also log it to later apply to the reads
            if self.first {
                // except before first swap, when we'd rather just clone the entire map
            } else {
                self.w_log.push(r);
            }
        }
    }

    pub fn update_ts(&mut self, ts: i64) {
        // See Self::add()
        {
            let arc: &mut sync::Arc<_> = &mut *self.w_store.as_mut().unwrap();
            loop {
                if let Some(s) = sync::Arc::get_mut(arc) {
                    s.1 = ts;
                    break;
                } else {
                    use std::thread;
                    thread::yield_now();
                }
            }
        }
    }

    fn apply(store: &mut S, key: usize, r: Cow<ops::Record>) {
        debug_assert!(!r[key].is_none());
        if let ops::Record::Positive(..) = *r {
            let (r, _) = r.into_owned().extract();
            store.0.entry(r[key].clone()).or_insert_with(Vec::new).push(r);
            return;
        }

        match *r {
            ops::Record::Negative(ref r) => {
                let mut now_empty = false;
                if let Some(mut e) = store.0.get_mut(&r[key]) {
                    // find the first entry that matches all fields
                    if let Some(i) = e.iter().position(|er| er == r) {
                        e.swap_remove(i);
                        now_empty = e.is_empty();
                    }
                }
                if now_empty {
                    // no more entries for this key -- free up some space in the map
                    store.0.remove(&r[key]);
                }
            }
            _ => unreachable!(),
        }
    }
}

/// Allocate a new buffered `Store`.
pub fn new(cols: usize, key: usize) -> BufferedStoreBuilder {
    BufferedStoreBuilder {
        key: key,
        cols: cols,
        w_store: (FnvHashMap::default(), -1, true),
        r_store: (FnvHashMap::default(), -1, false),
    }
}

impl BufferedStoreBuilder {
    pub fn commit(self) -> (BufferedStore, WriteHandle) {
        let store = Box::into_raw(Box::new(sync::Arc::new(self.r_store)));
        let r = BufferedStore {
            store: sync::Arc::new(AtomicPtr::new(store)),
            key: self.key,
        };
        let w = WriteHandle {
            w_store: Some(Box::new(sync::Arc::new(self.w_store))),
            w_log: Vec::new(),
            bs: r.clone(),
            cols: self.cols,
            key: self.key,
            first: true,
        };
        (r, w)
    }
}

impl BufferedStore {
    /// Find all entries that matched the given conditions.
    ///
    /// Returned records are passed to `then` before being returned.
    ///
    /// Note that not all writes will be included with this read -- only those that have been
    /// swapped in by the writer.
    pub fn find_and<F, T>(&self, key: &query::DataType, then: F) -> Result<(T, i64), ()>
        where F: FnOnce(&[sync::Arc<Vec<query::DataType>>]) -> T
    {
        use std::mem;
        let r_store = unsafe { Box::from_raw(self.store.load(atomic::Ordering::SeqCst)) };
        let rs: sync::Arc<_> = (&*r_store).clone();

        let r_store_again = unsafe { Box::from_raw(self.store.load(atomic::Ordering::SeqCst)) };
        #[allow(collapsible_if)]
        let res = if !sync::Arc::ptr_eq(&*r_store, &*r_store_again) {
            // a swap happened under us.
            //
            // let's first figure out where the writer can possibly be at this point. since we *do*
            // have a clone of an Arc, and a pointer swap has happened, we must be in one of the
            // following cases:
            //
            //  (a) we read and cloned, *then* a writer swapped, checked for uniqueness, and blocks
            //  (b) we read, then a writer swapped, checked for uniqueness, and continued
            //
            // in (a), we know that the writer that did the pointer swap is waiting on our Arc (rs)
            // we also know that we *ought* to be using a clone of the *new* pointer to read from.
            // since the writer won't do anything until we release rs, we can just do a new clone,
            // and *then* release the old Arc.
            //
            // in (b), we know that there is a writer that thinks it owns rs, and so it is not safe
            // to use rs. how about Arc(r_store_again)? the only way that would be unsafe to use
            // would be if a writer has gone all the way through the pointer swap and Arc
            // uniqueness test *after* we read r_store_again. can that have happened? no. if a
            // second writer came along after we read r_store_again, and wanted to swap, it would
            // get r_store from its atomic pointer swap. since we're still holding an Arc(r_store),
            // it would fail the uniqueness test, and therefore block at that point. thus, we know
            // that no writer is currently modifying r_store_again (and won't be for as long as we
            // hold rs).
            let rs2: sync::Arc<_> = (&*r_store_again).clone();
            if !rs2.2 {
                Err(())
            } else {
                let res = then(rs2.0.get(key).map(|v| &**v).unwrap_or(&[]));
                let res = (res, rs2.1);
                drop(rs2);
                drop(rs);
                Ok(res)
            }
        } else {
            // are we actually safe in this case? could there not have been two swaps in a row,
            // making the value r_store -> r_store_again -> r_store? well, there could, but that
            // implies that r_store is safe to use. why? well, for this to have happened, we must
            // have read the pointer, then a writer went runs swap() (potentially multiple times).
            // then, at some point, we cloned(). then, we read r_store_again to be
            // equal to r_store.
            //
            // the moment we clone, we immediately prevent any writer from taking ownership of
            // r_store. however, what if the current writer thinks that it owns r_store? well, we
            // read r_store_again == r_store, which means that at some point *after the clone*, a
            // writer made r_store read owned. since no writer can take ownership of r_store after
            // we cloned it, we know that r_store must still be owned by readers.
            if !rs.2 {
                Err(())
            } else {
                let res = then(rs.0.get(key).map(|v| &**v).unwrap_or(&[]));
                let res = (res, rs.1);
                drop(rs);
                Ok(res)
            }
        };
        mem::forget(r_store); // don't free the Box!
        mem::forget(r_store_again); // don't free the Box!
        res
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ops;

    #[test]
    fn store_works() {
        let a = sync::Arc::new(vec![1.into(), "a".into()]);

        let (r, mut w) = new(2, 0).commit();

        // initially, store is uninitialized
        assert_eq!(r.find_and(&a[0], |rs| rs.len()), Err(()));

        w.swap();

        // after first swap, it is empty, but ready
        assert_eq!(r.find_and(&a[0], |rs| rs.len()), Ok((0, -1)));

        w.add(vec![ops::Record::Positive(a.clone())]);

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
        let (r, mut w) = new(1, 0).commit();
        thread::spawn(move || for i in 0..n {
            w.add(vec![ops::Record::Positive(sync::Arc::new(vec![i.into()]))]);
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
        let a = sync::Arc::new(vec![1.into(), "a".into()]);
        let b = sync::Arc::new(vec![1.into(), "b".into()]);

        let (r, mut w) = new(2, 0).commit();
        w.add(vec![ops::Record::Positive(a.clone())]);
        w.swap();
        w.add(vec![ops::Record::Positive(b.clone())]);

        assert_eq!(r.find_and(&a[0], |rs| rs.len()).unwrap().0, 1);
        assert!(r.find_and(&a[0], |rs| rs.iter().any(|r| r[0] == a[0] && r[1] == a[1])).unwrap().0);
    }

    #[test]
    fn non_minimal_query() {
        let a = sync::Arc::new(vec![1.into(), "a".into()]);
        let b = sync::Arc::new(vec![1.into(), "b".into()]);
        let c = sync::Arc::new(vec![1.into(), "c".into()]);

        let (r, mut w) = new(2, 0).commit();
        w.add(vec![ops::Record::Positive(a.clone())]);
        w.add(vec![ops::Record::Positive(b.clone())]);
        w.swap();
        w.add(vec![ops::Record::Positive(c.clone())]);

        assert_eq!(r.find_and(&a[0], |rs| rs.len()).unwrap().0, 2);
        assert!(r.find_and(&a[0], |rs| rs.iter().any(|r| r[0] == a[0] && r[1] == a[1])).unwrap().0);
        assert!(r.find_and(&a[0], |rs| rs.iter().any(|r| r[0] == b[0] && r[1] == b[1])).unwrap().0);
    }

    #[test]
    fn absorb_negative_immediate() {
        let a = sync::Arc::new(vec![1.into(), "a".into()]);
        let b = sync::Arc::new(vec![1.into(), "b".into()]);

        let (r, mut w) = new(2, 0).commit();
        w.add(vec![ops::Record::Positive(a.clone())]);
        w.add(vec![ops::Record::Positive(b.clone())]);
        w.add(vec![ops::Record::Negative(a.clone())]);
        w.swap();

        assert_eq!(r.find_and(&a[0], |rs| rs.len()).unwrap().0, 1);
        assert!(r.find_and(&a[0], |rs| rs.iter().any(|r| r[0] == b[0] && r[1] == b[1])).unwrap().0);
    }

    #[test]
    fn absorb_negative_later() {
        let a = sync::Arc::new(vec![1.into(), "a".into()]);
        let b = sync::Arc::new(vec![1.into(), "b".into()]);

        let (r, mut w) = new(2, 0).commit();
        w.add(vec![ops::Record::Positive(a.clone())]);
        w.add(vec![ops::Record::Positive(b.clone())]);
        w.swap();
        w.add(vec![ops::Record::Negative(a.clone())]);
        w.swap();

        assert_eq!(r.find_and(&a[0], |rs| rs.len()).unwrap().0, 1);
        assert!(r.find_and(&a[0], |rs| rs.iter().any(|r| r[0] == b[0] && r[1] == b[1])).unwrap().0);
    }

    #[test]
    fn absorb_multi() {
        let a = sync::Arc::new(vec![1.into(), "a".into()]);
        let b = sync::Arc::new(vec![1.into(), "b".into()]);
        let c = sync::Arc::new(vec![1.into(), "c".into()]);

        let (r, mut w) = new(2, 0).commit();
        w.add(vec![ops::Record::Positive(a.clone()), ops::Record::Positive(b.clone())]);
        w.swap();

        assert_eq!(r.find_and(&a[0], |rs| rs.len()).unwrap().0, 2);
        assert!(r.find_and(&a[0], |rs| rs.iter().any(|r| r[0] == a[0] && r[1] == a[1])).unwrap().0);
        assert!(r.find_and(&a[0], |rs| rs.iter().any(|r| r[0] == b[0] && r[1] == b[1])).unwrap().0);

        w.add(vec![ops::Record::Negative(a.clone()),
                   ops::Record::Positive(c.clone()),
                   ops::Record::Negative(c.clone())]);
        w.swap();

        assert_eq!(r.find_and(&a[0], |rs| rs.len()).unwrap().0, 1);
        assert!(r.find_and(&a[0], |rs| rs.iter().any(|r| r[0] == b[0] && r[1] == b[1])).unwrap().0);
    }
}
