use ops;
use query;
use shortcut;

use std::ptr;
use std::sync;
use std::sync::atomic;
use std::sync::atomic::AtomicPtr;

type S = (shortcut::Store<query::DataType>, LL);

/// This structure provides a storage mechanism that allows limited time-scoped queries. That is,
/// callers of `find()` may choose to *ignore* a suffix of the latest updates added with `add()`.
/// The results will be as if the `find()` was executed before those updates were received.
///
/// Only updates whose timestamp are higher than what was provided to the last call to `absorb()`
/// may be ignored. The backlog should periodically be absorbed back into the `Store` for
/// efficiency, as every find incurs a *linear scan* of all updates in the backlog.
pub struct BufferedStore {
    cols: usize,
    absorbed: atomic::AtomicIsize,
    store: sync::RwLock<S>,
}

#[derive(Debug)]
struct LL {
    // next is never mutatated, only overwritten or read
    next: AtomicPtr<LL>,
    entry: Option<(i64, Vec<ops::Record>)>,
}

impl LL {
    fn new(e: Option<(i64, Vec<ops::Record>)>) -> LL {
        use std::mem;
        LL {
            next: AtomicPtr::new(unsafe { mem::transmute::<*const LL, *mut LL>(ptr::null()) }),
            entry: e,
        }
    }

    fn adopt(&self, next: Box<LL>) {
        // TODO
        // avoid having to iterate to the last node all over again for every add.
        // one way to do this would be to keep a pointer to the last LL.
        // we probably don't want to keep this inside each LL though...
        self.last().next.store(Box::into_raw(next), atomic::Ordering::Release);
    }

    fn last(&self) -> &LL {
        if let Some(n) = self.after() {
            unsafe { &*n }.last()
        } else {
            self
        }
    }

    fn after(&self) -> Option<*mut LL> {
        let next = self.next.load(atomic::Ordering::Acquire);
        if (next as *const LL).is_null() {
            // there's no next
            return None;
        }

        Some(next)
    }

    fn take(&mut self) -> Option<(i64, Vec<ops::Record>)> {
        self.after().map(|next| {
            // steal the next and bypass
            let next = unsafe { Box::from_raw(next) };
            self.next.store(next.next.load(atomic::Ordering::Acquire),
                            atomic::Ordering::Release);
            next.entry.expect("only first LL should have None entry")
        })
    }

    fn iter(&self) -> LLIter {
        LLIter(self)
    }
}

struct LLIter<'a>(&'a LL);
impl<'a> Iterator for LLIter<'a> {
    type Item = &'a (i64, Vec<ops::Record>);
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // we assume that the current node has already been yielded
            // so, we first advance, and then check for a value
            let next = self.0.after();

            if next.is_none() {
                // no next, so nothing more to iterate over
                return None;
            }

            self.0 = unsafe { &*next.unwrap() };

            // if we moved to a node that has a value, yield it
            if let Some(ref e) = self.0.entry {
                return Some(e);
            }
            // otherwise move again
        }
    }
}

impl BufferedStore {
    /// Allocate a new buffered `Store`.
    pub fn new(cols: usize) -> BufferedStore {
        BufferedStore {
            cols: cols,
            absorbed: atomic::AtomicIsize::new(-1),
            store: sync::RwLock::new((shortcut::Store::new(cols + 1 /* ts */), LL::new(None))),
        }
    }

    /// Absorb all updates in the backlog with a timestamp less than or equal to the given
    /// timestamp into the underlying `Store`. Note that this precludes calling `find()` with an
    /// `including` argument that is less than the given value.
    ///
    /// This operation will take time proportional to the number of entries in the backlog whose
    /// timestamp is less than or equal to the given timestamp.
    pub fn absorb(&self, including: i64) {
        let including = including as isize;
        if including <= self.absorbed.load(atomic::Ordering::Acquire) {
            return;
        }

        let mut store = self.store.write().unwrap();
        self.absorbed.store(including, atomic::Ordering::Release);
        loop {
            match store.1.after() {
                Some(next) => {
                    // there's a next node to process
                    // check its timestamp
                    let n = unsafe { &*next };
                    assert!(n.entry.is_some());
                    if n.entry.as_ref().unwrap().0 as isize > including {
                        // it's too new, we're done
                        break;
                    }
                }
                None => break,
            }

            for r in store.1
                .take()
                .expect("no concurrent access, so if .after() is Some, so should .take()")
                .1 {
                match r {
                    ops::Record::Positive(mut r, ts) => {
                        r.push(query::DataType::Number(ts));
                        store.0.insert(r);
                    }
                    ops::Record::Negative(r, ts) => {
                        // we need a cond that will match this row.
                        let conds = r.into_iter()
                            .enumerate()
                            .chain(Some((self.cols, query::DataType::Number(ts))).into_iter())
                            .map(|(coli, v)| {
                                shortcut::Condition {
                                    column: coli,
                                    cmp: shortcut::Comparison::Equal(shortcut::Value::Const(v)),
                                }
                            })
                            .collect::<Vec<_>>();

                        // however, multiple rows may have the same values as this row for every
                        // column. afaict, it is safe to delete any one of these rows. we do this
                        // by returning true for the first invocation of the filter function, and
                        // false for all subsequent invocations.
                        let mut first = true;
                        store.0.delete_filter(&conds[..], |_| if first {
                            first = false;
                            true
                        } else {
                            false
                        });
                    }
                }
            }
        }
    }

    /// Add a new set of records to the backlog at the given timestamp.
    ///
    /// This method should never be called twice with the same timestamp, and the given timestamp
    /// must not yet have been absorbed.
    ///
    /// This method assumes that there are no other concurrent writers.
    pub unsafe fn add(&self, r: Vec<ops::Record>, ts: i64) {
        assert!(ts > self.absorbed.load(atomic::Ordering::Acquire) as i64);
        self.store.read().unwrap().1.adopt(Box::new(LL::new(Some((ts, r)))));
    }

    #[cfg(test)]
    /// Safe wrapper around `add` for when you have an exclusive reference
    pub fn safe_add(&mut self, r: Vec<ops::Record>, ts: i64) {
        unsafe { self.add(r, ts) };
    }

    /// Important and absorb a set of records at the given timestamp.
    pub fn batch_import(&self, rs: Vec<(Vec<query::DataType>, i64)>, ts: i64) {
        let mut lock = self.store.write().unwrap();
        assert!((lock.1.next.load(atomic::Ordering::Acquire) as *const LL).is_null());
        assert!(self.absorbed.load(atomic::Ordering::Acquire) < ts as isize);
        for (mut row, ts) in rs {
            row.push(query::DataType::Number(ts));
            lock.0.insert(row);
        }
        self.absorbed.store(ts as isize, atomic::Ordering::Release);
    }

    fn extract_ts<'a>(&self, r: &'a [query::DataType]) -> (&'a [query::DataType], i64) {
        if let query::DataType::Number(ts) = r[self.cols] {
            (&r[0..self.cols], ts)
        } else {
            unreachable!()
        }
    }

    /// Find all entries that matched the given conditions just after the given point in time.
    ///
    /// Equivalent to running `find_and(&q.having[..], including)`, but projecting through the
    /// given query before results are returned. If not query is given, the returned records are
    /// cloned.
    pub fn find(&self,
                q: Option<&query::Query>,
                including: Option<i64>)
                -> Vec<(Vec<query::DataType>, i64)> {
        self.find_and(q.map(|q| &q.having[..]).unwrap_or(&[]), including, |rs| {
            rs.into_iter()
                .map(|(r, ts)| {
                    let r = r.iter().cloned().collect();
                    if let Some(q) = q {
                        (q.project(r), ts)
                    } else {
                        (r, ts)
                    }
                })
                .collect()
        })
    }

    /// Find all entries that matched the given conditions just after the given point in time.
    ///
    /// Returned records are passed to `then` before being returned.
    ///
    /// This method will panic if the given timestamp falls before the last absorbed timestamp, as
    /// it cannot guarantee correct results in that case. Queries *at* the time of the last absorb
    /// are fine.
    ///
    /// Completes in `O(Store::find + b)` where `b` is the number of records in the backlog whose
    /// timestamp fall at or before the given timestamp.
    pub fn find_and<'a, F, T>(&self,
                              conds: &[shortcut::cmp::Condition<query::DataType>],
                              including: Option<i64>,
                              then: F)
                              -> T
        where T: 'a,
              F: 'a + FnOnce(Vec<(&[query::DataType], i64)>) -> T
    {
        let store = self.store.read().unwrap();

        // if we don't have a timestamp (i.e., end-user query), then we want to scan the entire
        // backlog. this may be somewhat slow, but will give the reader as up-to-date results as we
        // can, so that we minimize user-visible inconsistencies.
        // TODO: we may want to expose this trade-off to the user in the future.
        let including = including.unwrap_or_else(i64::max_value);

        // if we are querying at the absorption timestamp, we need only look at the store
        let absorbed = self.absorbed.load(atomic::Ordering::Acquire) as i64;
        if including == absorbed {
            return then(store.0.find(conds).map(|r| self.extract_ts(r)).collect());
        }

        // okay, so we want to:
        //
        //  a) get the base results
        //  b) add any backlogged positives
        //  c) remove any backlogged negatives
        //
        // (a) is trivial (self.store.find)
        // we'll do (b) and (c) in two steps:
        //
        //  1) chain in all the positives in the backlog onto the base result iterator
        //  2) for each resulting row, check all backlogged negatives, and eliminate that result +
        //     the backlogged entry if there's a match.
        assert!(including > absorbed);
        let mut relevant = store.1
            .iter()
            .take_while(|&&(ts, _)| ts <= including)
            .flat_map(|&(_, ref group)| group.iter())
            .filter(|r| conds.iter().all(|c| c.matches(&r.rec()[..])))
            .peekable();

        if relevant.peek().is_some() {
            let (positives, mut negatives): (_, Vec<_>) = relevant.partition(|r| r.is_positive());
            if negatives.is_empty() {
                then(store.0
                    .find(conds)
                    .map(|r| self.extract_ts(r))
                    .chain(positives.into_iter().map(|r| (r.rec(), r.ts())))
                    .collect())
            } else {
                then(store.0
                    .find(conds)
                    .map(|r| self.extract_ts(r))
                    .chain(positives.into_iter().map(|r| (r.rec(), r.ts())))
                    .filter_map(|(r, ts)| {
                        let revocation = negatives.iter()
                            .position(|neg| {
                                ts == neg.ts() &&
                                neg.rec().iter().enumerate().all(|(i, v)| &r[i] == v)
                            });

                        if let Some(revocation) = revocation {
                            // order of negatives doesn't matter, so O(1) swap_remove is fine
                            negatives.swap_remove(revocation);
                            None
                        } else {
                            Some((r, ts))
                        }
                    })
                    .collect())
            }
        } else {
            then(store.0.find(conds).map(|r| self.extract_ts(r)).collect())
        }
    }

    pub fn index<I: Into<shortcut::Index<query::DataType>>>(&self, column: usize, indexer: I) {
        self.store.write().unwrap().0.index(column, indexer);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::LL;
    use ops;

    #[test]
    fn ll() {
        let mut start = LL::new(None);
        assert_eq!(start.iter().count(), 0);

        start.adopt(Box::new(LL::new(Some((1, vec![])))));
        assert_eq!(start.iter().count(), 1);

        start.adopt(Box::new(LL::new(Some((2, vec![])))));
        {
            let mut it = start.iter();
            assert_eq!(it.next(), Some(&(1, vec![])));
            assert_eq!(it.next(), Some(&(2, vec![])));
            assert_eq!(it.next(), None);
        }

        start.adopt(Box::new(LL::new(Some((3, vec![])))));
        {
            let mut it = start.iter();
            assert_eq!(it.next(), Some(&(1, vec![])));
            assert_eq!(it.next(), Some(&(2, vec![])));
            assert_eq!(it.next(), Some(&(3, vec![])));
            assert_eq!(it.next(), None);
        }

        let x = start.take();
        assert_eq!(x, Some((1, vec![])));
        {
            let mut it = start.iter();
            assert_eq!(it.next(), Some(&(2, vec![])));
            assert_eq!(it.next(), Some(&(3, vec![])));
            assert_eq!(it.next(), None);
        }

        let x = start.take();
        assert_eq!(x, Some((2, vec![])));
        {
            let mut it = start.iter();
            assert_eq!(it.next(), Some(&(3, vec![])));
            assert_eq!(it.next(), None);
        }

        let x = start.take();
        assert_eq!(x, Some((3, vec![])));
        assert_eq!(start.iter().count(), 0);
    }

    #[test]
    fn store_only() {
        let a1 = vec![1.into(), "a".into()];

        let mut b = BufferedStore::new(2);
        b.safe_add(vec![ops::Record::Positive(a1.clone(), 0)], 0);
        b.absorb(0);
        assert_eq!(b.find_and(&[], Some(0), |rs| rs.len()), 1);
        assert!(b.find_and(&[], Some(0), |rs| {
            rs.iter().any(|&(r, ts)| ts == 0 && r[0] == 1.into() && r[1] == "a".into())
        }));
    }

    #[test]
    fn backlog_only() {
        let a1 = vec![1.into(), "a".into()];

        let mut b = BufferedStore::new(2);
        b.safe_add(vec![ops::Record::Positive(a1.clone(), 0)], 0);
        assert_eq!(b.find_and(&[], Some(0), |rs| rs.len()), 1);
        assert!(b.find_and(&[], Some(0), |rs| {
            rs.iter()
                .any(|&(r, ts)| ts == 0 && r[0] == 1.into() && r[1] == "a".into())
        }));
    }

    #[test]
    fn no_ts_scans_backlog() {
        let a1 = vec![1.into(), "a".into()];
        let b2 = vec![2.into(), "b".into()];

        let mut b = BufferedStore::new(2);
        b.safe_add(vec![ops::Record::Positive(a1.clone(), 0)], 0);
        b.safe_add(vec![ops::Record::Positive(b2.clone(), 1)], 1);
        b.absorb(0);
        assert_eq!(b.find_and(&[], None, |rs| rs.len()), 2);
        assert!(b.find_and(&[], None, |rs| {
            rs.iter()
                .any(|&(r, ts)| ts == 0 && r[0] == 1.into() && r[1] == "a".into()) &&
            rs.iter()
                .any(|&(r, ts)| ts == 1 && r[0] == 2.into() && r[1] == "b".into())
        }));
    }

    #[test]
    fn store_and_backlog() {
        let a1 = vec![1.into(), "a".into()];
        let b2 = vec![2.into(), "b".into()];

        let mut b = BufferedStore::new(2);
        b.safe_add(vec![ops::Record::Positive(a1.clone(), 0)], 0);
        b.safe_add(vec![ops::Record::Positive(b2.clone(), 1)], 1);
        b.absorb(0);
        assert_eq!(b.find_and(&[], Some(1), |rs| rs.len()), 2);
        assert!(b.find_and(&[], Some(1), |rs| {
            rs.iter()
                .any(|&(r, ts)| ts == 0 && r[0] == 1.into() && r[1] == "a".into())
        }));
        assert!(b.find_and(&[], Some(1), |rs| {
            rs.iter()
                .any(|&(r, ts)| ts == 1 && r[0] == 2.into() && r[1] == "b".into())
        }));
    }

    #[test]
    fn minimal_query() {
        let a1 = vec![1.into(), "a".into()];
        let b2 = vec![2.into(), "b".into()];

        let mut b = BufferedStore::new(2);
        b.safe_add(vec![ops::Record::Positive(a1.clone(), 0)], 0);
        b.safe_add(vec![ops::Record::Positive(b2.clone(), 1)], 1);
        b.absorb(0);
        assert_eq!(b.find_and(&[], Some(0), |rs| rs.len()), 1);
        assert!(b.find_and(&[], Some(0), |rs| {
            rs.iter()
                .any(|&(r, ts)| ts == 0 && r[0] == 1.into() && r[1] == "a".into())
        }));
    }

    #[test]
    fn non_minimal_query() {
        let a1 = vec![1.into(), "a".into()];
        let b2 = vec![2.into(), "b".into()];
        let c3 = vec![3.into(), "c".into()];

        let mut b = BufferedStore::new(2);
        b.safe_add(vec![ops::Record::Positive(a1.clone(), 0)], 0);
        b.safe_add(vec![ops::Record::Positive(b2.clone(), 1)], 1);
        b.safe_add(vec![ops::Record::Positive(c3.clone(), 2)], 2);
        b.absorb(0);
        assert_eq!(b.find_and(&[], Some(1), |rs| rs.len()), 2);
        assert!(b.find_and(&[], Some(1), |rs| {
            rs.iter()
                .any(|&(r, ts)| ts == 0 && r[0] == 1.into() && r[1] == "a".into())
        }));
        assert!(b.find_and(&[], Some(1), |rs| {
            rs.iter()
                .any(|&(r, ts)| ts == 1 && r[0] == 2.into() && r[1] == "b".into())
        }));
    }

    #[test]
    fn absorb_negative_immediate() {
        let a1 = vec![1.into(), "a".into()];
        let b2 = vec![2.into(), "b".into()];

        let mut b = BufferedStore::new(2);
        b.safe_add(vec![ops::Record::Positive(a1.clone(), 0)], 0);
        b.safe_add(vec![ops::Record::Positive(b2.clone(), 1)], 1);
        b.safe_add(vec![ops::Record::Negative(a1.clone(), 0)], 2);
        b.absorb(2);
        assert_eq!(b.find_and(&[], Some(2), |rs| rs.len()), 1);
        assert!(b.find_and(&[], Some(2), |rs| {
            rs.iter()
                .any(|&(r, ts)| ts == 1 && r[0] == 2.into() && r[1] == "b".into())
        }));
    }

    #[test]
    fn absorb_negative_later() {
        let a1 = vec![1.into(), "a".into()];
        let b2 = vec![2.into(), "b".into()];

        let mut b = BufferedStore::new(2);
        b.safe_add(vec![ops::Record::Positive(a1.clone(), 0)], 0);
        b.safe_add(vec![ops::Record::Positive(b2.clone(), 1)], 1);
        b.absorb(1);
        b.safe_add(vec![ops::Record::Negative(a1.clone(), 0)], 2);
        b.absorb(2);
        assert_eq!(b.find_and(&[], Some(2), |rs| rs.len()), 1);
        assert!(b.find_and(&[], Some(2), |rs| {
            rs.iter()
                .any(|&(r, ts)| ts == 1 && r[0] == 2.into() && r[1] == "b".into())
        }));
    }

    #[test]
    fn query_negative() {
        let a1 = vec![1.into(), "a".into()];
        let b2 = vec![2.into(), "b".into()];

        let mut b = BufferedStore::new(2);
        b.safe_add(vec![ops::Record::Positive(a1.clone(), 0)], 0);
        b.safe_add(vec![ops::Record::Positive(b2.clone(), 1)], 1);
        b.safe_add(vec![ops::Record::Negative(a1.clone(), 0)], 2);
        assert_eq!(b.find_and(&[], Some(2), |rs| rs.len()), 1);
        assert!(b.find_and(&[], Some(2), |rs| {
            rs.iter()
                .any(|&(r, ts)| ts == 1 && r[0] == 2.into() && r[1] == "b".into())
        }));
    }

    #[test]
    fn absorb_multi() {
        let a1 = vec![1.into(), "a".into()];
        let b2 = vec![2.into(), "b".into()];
        let c3 = vec![3.into(), "c".into()];

        let mut b = BufferedStore::new(2);

        b.safe_add(vec![ops::Record::Positive(a1.clone(), 0),
                        ops::Record::Positive(b2.clone(), 1)],
                   0);
        b.absorb(0);
        assert_eq!(b.find_and(&[], Some(0), |rs| rs.len()), 2);
        assert!(b.find_and(&[], Some(0), |rs| {
            rs.iter()
                .any(|&(r, ts)| ts == 0 && r[0] == 1.into() && r[1] == "a".into())
        }));
        assert!(b.find_and(&[], Some(0), |rs| {
            rs.iter()
                .any(|&(r, ts)| ts == 1 && r[0] == 2.into() && r[1] == "b".into())
        }));

        b.safe_add(vec![ops::Record::Negative(a1.clone(), 0),
                        ops::Record::Positive(c3.clone(), 2),
                        ops::Record::Negative(c3.clone(), 2)],
                   1);
        b.absorb(1);
        assert_eq!(b.find_and(&[], Some(1), |rs| rs.len()), 1);
        assert!(b.find_and(&[], Some(1), |rs| {
            rs.iter()
                .any(|&(r, ts)| ts == 1 && r[0] == 2.into() && r[1] == "b".into())
        }));
    }

    #[test]
    fn query_multi() {
        let a1 = vec![1.into(), "a".into()];
        let b2 = vec![2.into(), "b".into()];
        let c3 = vec![3.into(), "c".into()];

        let mut b = BufferedStore::new(2);

        b.safe_add(vec![ops::Record::Positive(a1.clone(), 0),
                        ops::Record::Positive(b2.clone(), 1)],
                   0);
        assert_eq!(b.find_and(&[], Some(0), |rs| rs.len()), 2);
        assert!(b.find_and(&[], Some(0), |rs| {
            rs.iter()
                .any(|&(r, ts)| ts == 0 && r[0] == 1.into() && r[1] == "a".into())
        }));
        assert!(b.find_and(&[], Some(0), |rs| {
            rs.iter()
                .any(|&(r, ts)| ts == 1 && r[0] == 2.into() && r[1] == "b".into())
        }));

        b.safe_add(vec![ops::Record::Negative(a1.clone(), 0),
                        ops::Record::Positive(c3.clone(), 2),
                        ops::Record::Negative(c3.clone(), 2)],
                   1);
        assert_eq!(b.find_and(&[], Some(1), |rs| rs.len()), 1);
        assert!(b.find_and(&[], Some(1), |rs| {
            rs.iter()
                .any(|&(r, ts)| ts == 1 && r[0] == 2.into() && r[1] == "b".into())
        }));
    }

    #[test]
    fn query_complex() {
        let a1 = vec![1.into(), "a".into()];
        let b2 = vec![2.into(), "b".into()];
        let c3 = vec![3.into(), "c".into()];

        let mut b = BufferedStore::new(2);

        b.safe_add(vec![ops::Record::Negative(a1.clone(), 0),
                        ops::Record::Positive(b2.clone(), 1)],
                   0);
        b.safe_add(vec![ops::Record::Negative(b2.clone(), 1),
                        ops::Record::Positive(c3.clone(), 2)],
                   1);
        b.find_and(&[], Some(2), |rs| assert_eq!(rs, vec![(&*c3, 2)]));
    }
}
