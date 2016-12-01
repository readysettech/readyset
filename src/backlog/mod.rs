use ops;
use query;
use shortcut;

use std::borrow::Cow;
use std::sync;
use std::sync::atomic;
use std::sync::atomic::AtomicPtr;

type S = shortcut::Store<query::DataType, sync::Arc<Vec<query::DataType>>>;
pub struct WriteHandle {
    w_store: Option<Box<sync::Arc<S>>>,
    w_log: Vec<ops::Record>,
    bs: BufferedStore,
}

#[derive(Clone)]
pub struct BufferedStore(sync::Arc<AtomicPtr<sync::Arc<S>>>);

pub struct BufferedStoreBuilder {
    r_store: S,
    w_store: S,
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
        let w_store: *mut sync::Arc<S> = Box::into_raw(w_store);

        // swap in our w_store, and get r_store in return
        let r_store = self.bs.0.swap(w_store, atomic::Ordering::SeqCst);
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
                for r in self.w_log.drain(..) {
                    Self::apply(w_store, Cow::Owned(r));
                }

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
            {
                let arc: &mut sync::Arc<_> = &mut *self.w_store.as_mut().unwrap();
                loop {
                    if let Some(s) = sync::Arc::get_mut(arc) {
                        Self::apply(s, Cow::Borrowed(&r));
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
            self.w_log.push(r);
        }
    }

    fn apply(store: &mut S, r: Cow<ops::Record>) {
        if let ops::Record::Positive(..) = *r {
            let (r, _) = r.into_owned().extract();
            store.insert(r);
            return;
        }

        match *r {
            ops::Record::Negative(ref r) => {
                // we need a cond that will match this row.
                let conds = r.iter()
                    .enumerate()
                    .map(|(coli, v)| {
                        shortcut::Condition {
                            column: coli,
                            cmp: shortcut::Comparison::Equal(shortcut::Value::using(v)),
                        }
                    })
                    .collect::<Vec<_>>();

                // however, multiple rows may have the same values as this row for
                // every column. afaict, it is safe to delete any one of these rows. we
                // do this by returning true for the first invocation of the filter
                // function, and false for all subsequent invocations.
                let mut first = true;
                store.delete_filter(&conds[..], |_| if first {
                    first = false;
                    true
                } else {
                    false
                });
            }
            _ => unreachable!(),
        }
    }
}

/// Allocate a new buffered `Store`.
pub fn new(cols: usize) -> BufferedStoreBuilder {
    BufferedStoreBuilder {
        w_store: shortcut::Store::new(cols),
        r_store: shortcut::Store::new(cols),
    }
}

impl BufferedStoreBuilder {
    pub fn index<I>(&mut self, column: usize, indexer: I)
        where I: Clone + Into<shortcut::Index<query::DataType>>
    {
        let i1 = indexer.clone();
        let i2 = indexer;
        self.w_store.index(column, i1);
        self.r_store.index(column, i2);
    }

    pub fn commit(self) -> (BufferedStore, WriteHandle) {
        let r =
            BufferedStore(sync::Arc::new(AtomicPtr::new(Box::into_raw(Box::new(sync::Arc::new(self.r_store))))));
        let w = WriteHandle {
            w_store: Some(Box::new(sync::Arc::new(self.w_store))),
            w_log: Vec::new(),
            bs: r.clone(),
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
    pub fn find_and<F, T>(&self, q: &[shortcut::cmp::Condition<query::DataType>], then: F) -> T
        where F: FnOnce(Vec<&sync::Arc<Vec<query::DataType>>>) -> T
    {
        use std::mem;
        let r_store = unsafe { Box::from_raw(self.0.load(atomic::Ordering::SeqCst)) };
        let rs: sync::Arc<_> = (&*r_store).clone();

        let r_store_again = unsafe { Box::from_raw(self.0.load(atomic::Ordering::SeqCst)) };
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
            let res = then(rs2.find(q).collect());
            drop(rs2);
            drop(rs);
            res
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
            let res = then(rs.find(q).collect());
            drop(rs);
            res
        };
        mem::forget(r_store); // don't free the Box!
        mem::forget(r_store_again); // don't free the Box!
        res
    }
}

pub mod index;

#[cfg(test)]
mod tests {
    use super::*;
    use ops;

    #[test]
    fn store_works() {
        let a = sync::Arc::new(vec![1.into(), "a".into()]);

        let (r, mut w) = new(2).commit();

        // nothing there initially
        assert_eq!(r.find_and(&[], |rs| rs.len()), 0);

        w.add(vec![ops::Record::Positive(a.clone())]);

        // not even after an add (we haven't swapped yet)
        assert_eq!(r.find_and(&[], |rs| rs.len()), 0);

        w.swap();

        // but after the swap, the record is there!
        assert_eq!(r.find_and(&[], |rs| rs.len()), 1);
        assert!(r.find_and(&[], |rs| rs.iter().any(|r| r[0] == a[0] && r[1] == a[1])));
    }

    #[test]
    fn busybusybusy() {
        use shortcut;
        use std::thread;

        let mut db = new(1);
        db.index(0, shortcut::idx::HashIndex::new());

        let n = 10000;
        let (r, mut w) = db.commit();
        thread::spawn(move || for i in 0..n {
            w.add(vec![ops::Record::Positive(sync::Arc::new(vec![i.into()]))]);
            w.swap();
        });

        let mut cmp = vec![shortcut::Condition {
                               column: 0,
                               cmp: shortcut::Comparison::Equal(shortcut::Value::new(0)),
                           }];
        for i in 0..n {
            cmp[0].cmp = shortcut::Comparison::Equal(shortcut::Value::new(i));
            loop {
                let rows = r.find_and(&cmp[..], |rs| rs.len());
                match rows {
                    0 => continue,
                    1 => break,
                    i => assert_ne!(i, 1),
                }
            }
        }
    }

    #[test]
    fn minimal_query() {
        let a = sync::Arc::new(vec![1.into(), "a".into()]);
        let b = sync::Arc::new(vec![2.into(), "b".into()]);

        let (r, mut w) = new(2).commit();
        w.add(vec![ops::Record::Positive(a.clone())]);
        w.swap();
        w.add(vec![ops::Record::Positive(b.clone())]);

        assert_eq!(r.find_and(&[], |rs| rs.len()), 1);
        assert!(r.find_and(&[], |rs| rs.iter().any(|r| r[0] == a[0] && r[1] == a[1])));
    }

    #[test]
    fn non_minimal_query() {
        let a = sync::Arc::new(vec![1.into(), "a".into()]);
        let b = sync::Arc::new(vec![2.into(), "b".into()]);
        let c = sync::Arc::new(vec![3.into(), "c".into()]);

        let (r, mut w) = new(2).commit();
        w.add(vec![ops::Record::Positive(a.clone())]);
        w.add(vec![ops::Record::Positive(b.clone())]);
        w.swap();
        w.add(vec![ops::Record::Positive(c.clone())]);

        assert_eq!(r.find_and(&[], |rs| rs.len()), 2);
        assert!(r.find_and(&[], |rs| rs.iter().any(|r| r[0] == a[0] && r[1] == a[1])));
        assert!(r.find_and(&[], |rs| rs.iter().any(|r| r[0] == b[0] && r[1] == b[1])));
    }

    #[test]
    fn absorb_negative_immediate() {
        let a = sync::Arc::new(vec![1.into(), "a".into()]);
        let b = sync::Arc::new(vec![2.into(), "b".into()]);

        let (r, mut w) = new(2).commit();
        w.add(vec![ops::Record::Positive(a.clone())]);
        w.add(vec![ops::Record::Positive(b.clone())]);
        w.add(vec![ops::Record::Negative(a.clone())]);
        w.swap();

        assert_eq!(r.find_and(&[], |rs| rs.len()), 1);
        assert!(r.find_and(&[], |rs| rs.iter().any(|r| r[0] == b[0] && r[1] == b[1])));
    }

    #[test]
    fn absorb_negative_later() {
        let a = sync::Arc::new(vec![1.into(), "a".into()]);
        let b = sync::Arc::new(vec![2.into(), "b".into()]);

        let (r, mut w) = new(2).commit();
        w.add(vec![ops::Record::Positive(a.clone())]);
        w.add(vec![ops::Record::Positive(b.clone())]);
        w.swap();
        w.add(vec![ops::Record::Negative(a.clone())]);
        w.swap();

        assert_eq!(r.find_and(&[], |rs| rs.len()), 1);
        assert!(r.find_and(&[], |rs| rs.iter().any(|r| r[0] == b[0] && r[1] == b[1])));
    }

    #[test]
    fn absorb_multi() {
        let a = sync::Arc::new(vec![1.into(), "a".into()]);
        let b = sync::Arc::new(vec![2.into(), "b".into()]);
        let c = sync::Arc::new(vec![3.into(), "c".into()]);

        let (r, mut w) = new(2).commit();
        w.add(vec![ops::Record::Positive(a.clone()), ops::Record::Positive(b.clone())]);
        w.swap();

        assert_eq!(r.find_and(&[], |rs| rs.len()), 2);
        assert!(r.find_and(&[], |rs| rs.iter().any(|r| r[0] == a[0] && r[1] == a[1])));
        assert!(r.find_and(&[], |rs| rs.iter().any(|r| r[0] == b[0] && r[1] == b[1])));

        w.add(vec![ops::Record::Negative(a.clone()),
                   ops::Record::Positive(c.clone()),
                   ops::Record::Negative(c.clone())]);
        w.swap();

        assert_eq!(r.find_and(&[], |rs| rs.len()), 1);
        assert!(r.find_and(&[], |rs| rs.iter().any(|r| r[0] == b[0] && r[1] == b[1])));
    }
}
