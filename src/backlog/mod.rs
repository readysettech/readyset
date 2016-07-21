use ops;
use query;
use shortcut;

use std::ops::Deref;
use std::ops::DerefMut;

use std::collections::VecDeque;

/// This structure provides a storage mechanism that allows limited time-scoped queries. That is,
/// callers of `find()` may choose to *ignore* a suffix of the latest updates added with `add()`.
/// The results will be as if the `find()` was executed before those updates were received.
///
/// Only updates whose timestamp are higher than what was provided to the last call to `absorb()`
/// may be ignored. The backlog should periodically be absorbed back into the `Store` for
/// efficiency, as every find incurs a *linear scan* of all updates in the backlog.
pub struct BufferedStore {
    absorbed: i64,
    store: shortcut::Store<query::DataType>,
    backlog: VecDeque<(i64, Vec<ops::Record>)>,
}

impl BufferedStore {
    /// Allocate a new buffered `Store`.
    pub fn new(cols: usize) -> BufferedStore {
        BufferedStore {
            absorbed: -1,
            store: shortcut::Store::new(cols),
            backlog: VecDeque::new(),
        }
    }

    /// Absorb all updates in the backlog with a timestamp less than or equal to the given
    /// timestamp into the underlying `Store`. Note that this precludes calling `find()` with an
    /// `including` argument that is less than the given value.
    ///
    /// This operation will take time proportional to the number of entries in the backlog whose
    /// timestamp is less than or equal to the given timestamp.
    pub fn absorb(&mut self, including: i64) {
        if including <= self.absorbed {
            return;
        }

        self.absorbed = including;
        loop {
            match self.backlog.front() {
                None => break,
                Some(&(ts, _)) if ts > including => {
                    break;
                }
                _ => (),
            }

            for r in self.backlog.pop_front().unwrap().1.into_iter() {
                match r {
                    ops::Record::Positive(r) => {
                        self.store.insert(r);
                    }
                    ops::Record::Negative(r) => {
                        // we need a cond that will match this row.
                        let conds = r.into_iter()
                            .enumerate()
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
                        self.store.delete_filter(&conds[..], |_| {
                            if first {
                                first = false;
                                true
                            } else {
                                false
                            }
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
    /// This operation completes in amortized constant-time.
    pub fn add(&mut self, r: Vec<ops::Record>, ts: i64) {
        assert!(ts > self.absorbed);
        self.backlog.push_back((ts, r));
    }

    /// Important and absorb a set of records at the given timestamp.
    pub fn batch_import(&mut self, rs: Vec<Vec<query::DataType>>, ts: i64) {
        assert!(self.backlog.is_empty());
        assert!(self.absorbed < ts);
        for row in rs.into_iter() {
            self.store.insert(row);
        }
        self.absorbed = ts;
    }

    /// Find all entries that matched the given conditions just after the given point in time.
    ///
    /// This method will panic if the given timestamp falls before the last absorbed timestamp, as
    /// it cannot guarantee correct results in that case. Queries *at* the time of the last absorb
    /// are fine.
    ///
    /// Completes in `O(Store::find + b)` where `b` is the number of records in the backlog whose
    /// timestamp fall at or before the given timestamp.
    pub fn find<'a>(&'a self,
                    conds: &[shortcut::cmp::Condition<query::DataType>],
                    including: Option<i64>)
                    -> Vec<&'a [query::DataType]> {
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

        if including.is_none() {
            return self.store.find(conds).collect();
        }

        let including = including.unwrap();
        if including == self.absorbed {
            return self.store.find(conds).collect();
        }

        assert!(including > self.absorbed);
        let mut relevant = self.backlog
            .iter()
            .take_while(|&&(ts, _)| ts <= including)
            .flat_map(|&(_, ref group)| group.iter())
            .filter(|r| conds.iter().all(|c| c.matches(&r.rec()[..])))
            .peekable();

        if relevant.peek().is_some() {
            let (positives, mut negatives): (_, Vec<_>) = relevant.partition(|r| r.is_positive());
            if negatives.is_empty() {
                self.store
                    .find(conds)
                    .chain(positives.into_iter().map(|r| r.rec()))
                    .collect()
            } else {
                self.store
                    .find(conds)
                    .chain(positives.into_iter().map(|r| r.rec()))
                    .filter_map(|r| {
                        let revocation = negatives.iter()
                            .position(|neg| neg.rec().iter().enumerate().all(|(i, v)| &r[i] == v));

                        if let Some(revocation) = revocation {
                            // order of negatives doesn't matter, so O(1) swap_remove is fine
                            negatives.swap_remove(revocation);
                            None
                        } else {
                            Some(r)
                        }
                    })
                    .collect()
            }
        } else {
            self.store.find(conds).collect()
        }
    }
}

impl Deref for BufferedStore {
    type Target = shortcut::Store<query::DataType>;
    fn deref(&self) -> &Self::Target {
        &self.store
    }
}

impl DerefMut for BufferedStore {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.store
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use ops;

    #[test]
    fn store_only() {
        let a1 = vec![1.into(), "a".into()];

        let mut b = BufferedStore::new(2);
        b.add(vec![ops::Record::Positive(a1.clone())], 0);
        b.absorb(0);
        assert_eq!(b.find(&[], Some(0)).len(), 1);
        assert!(b.find(&[], Some(0)).iter().any(|r| r[0] == 1.into() && r[1] == "a".into()));
    }

    #[test]
    fn backlog_only() {
        let a1 = vec![1.into(), "a".into()];

        let mut b = BufferedStore::new(2);
        b.add(vec![ops::Record::Positive(a1.clone())], 0);
        assert_eq!(b.find(&[], Some(0)).len(), 1);
        assert!(b.find(&[], Some(0)).iter().any(|r| r[0] == 1.into() && r[1] == "a".into()));
    }

    #[test]
    fn no_ts_ignores_backlog() {
        let a1 = vec![1.into(), "a".into()];
        let b2 = vec![2.into(), "b".into()];

        let mut b = BufferedStore::new(2);
        b.add(vec![ops::Record::Positive(a1.clone())], 0);
        b.add(vec![ops::Record::Positive(b2.clone())], 1);
        b.absorb(0);
        assert_eq!(b.find(&[], None).len(), 1);
        assert!(b.find(&[], None).iter().any(|r| r[0] == 1.into() && r[1] == "a".into()));
    }

    #[test]
    fn store_and_backlog() {
        let a1 = vec![1.into(), "a".into()];
        let b2 = vec![2.into(), "b".into()];

        let mut b = BufferedStore::new(2);
        b.add(vec![ops::Record::Positive(a1.clone())], 0);
        b.add(vec![ops::Record::Positive(b2.clone())], 1);
        b.absorb(0);
        assert_eq!(b.find(&[], Some(1)).len(), 2);
        assert!(b.find(&[], Some(1)).iter().any(|r| r[0] == 1.into() && r[1] == "a".into()));
        assert!(b.find(&[], Some(1)).iter().any(|r| r[0] == 2.into() && r[1] == "b".into()));
    }

    #[test]
    fn minimal_query() {
        let a1 = vec![1.into(), "a".into()];
        let b2 = vec![2.into(), "b".into()];

        let mut b = BufferedStore::new(2);
        b.add(vec![ops::Record::Positive(a1.clone())], 0);
        b.add(vec![ops::Record::Positive(b2.clone())], 1);
        b.absorb(0);
        assert_eq!(b.find(&[], Some(0)).len(), 1);
        assert!(b.find(&[], Some(0)).iter().any(|r| r[0] == 1.into() && r[1] == "a".into()));
    }

    #[test]
    fn non_minimal_query() {
        let a1 = vec![1.into(), "a".into()];
        let b2 = vec![2.into(), "b".into()];
        let c3 = vec![3.into(), "c".into()];

        let mut b = BufferedStore::new(2);
        b.add(vec![ops::Record::Positive(a1.clone())], 0);
        b.add(vec![ops::Record::Positive(b2.clone())], 1);
        b.add(vec![ops::Record::Positive(c3.clone())], 2);
        b.absorb(0);
        assert_eq!(b.find(&[], Some(1)).len(), 2);
        assert!(b.find(&[], Some(1)).iter().any(|r| r[0] == 1.into() && r[1] == "a".into()));
        assert!(b.find(&[], Some(1)).iter().any(|r| r[0] == 2.into() && r[1] == "b".into()));
    }

    #[test]
    fn absorb_negative_immediate() {
        let a1 = vec![1.into(), "a".into()];
        let b2 = vec![2.into(), "b".into()];

        let mut b = BufferedStore::new(2);
        b.add(vec![ops::Record::Positive(a1.clone())], 0);
        b.add(vec![ops::Record::Positive(b2.clone())], 1);
        b.add(vec![ops::Record::Negative(a1.clone())], 2);
        b.absorb(2);
        assert_eq!(b.find(&[], Some(2)).len(), 1);
        assert!(b.find(&[], Some(2)).iter().any(|r| r[0] == 2.into() && r[1] == "b".into()));
    }

    #[test]
    fn absorb_negative_later() {
        let a1 = vec![1.into(), "a".into()];
        let b2 = vec![2.into(), "b".into()];

        let mut b = BufferedStore::new(2);
        b.add(vec![ops::Record::Positive(a1.clone())], 0);
        b.add(vec![ops::Record::Positive(b2.clone())], 1);
        b.absorb(1);
        b.add(vec![ops::Record::Negative(a1.clone())], 2);
        b.absorb(2);
        assert_eq!(b.find(&[], Some(2)).len(), 1);
        assert!(b.find(&[], Some(2)).iter().any(|r| r[0] == 2.into() && r[1] == "b".into()));
    }

    #[test]
    fn query_negative() {
        let a1 = vec![1.into(), "a".into()];
        let b2 = vec![2.into(), "b".into()];

        let mut b = BufferedStore::new(2);
        b.add(vec![ops::Record::Positive(a1.clone())], 0);
        b.add(vec![ops::Record::Positive(b2.clone())], 1);
        b.add(vec![ops::Record::Negative(a1.clone())], 2);
        assert_eq!(b.find(&[], Some(2)).len(), 1);
        assert!(b.find(&[], Some(2)).iter().any(|r| r[0] == 2.into() && r[1] == "b".into()));
    }

    #[test]
    fn absorb_multi() {
        let a1 = vec![1.into(), "a".into()];
        let b2 = vec![2.into(), "b".into()];
        let c3 = vec![3.into(), "c".into()];

        let mut b = BufferedStore::new(2);

        b.add(vec![ops::Record::Positive(a1.clone()), ops::Record::Positive(b2.clone())],
              0);
        b.absorb(0);
        assert_eq!(b.find(&[], Some(0)).len(), 2);
        assert!(b.find(&[], Some(0)).iter().any(|r| r[0] == 1.into() && r[1] == "a".into()));
        assert!(b.find(&[], Some(0)).iter().any(|r| r[0] == 2.into() && r[1] == "b".into()));

        b.add(vec![ops::Record::Negative(a1.clone()),
                   ops::Record::Positive(c3.clone()),
                   ops::Record::Negative(c3.clone())],
              1);
        b.absorb(1);
        assert_eq!(b.find(&[], Some(1)).len(), 1);
        assert!(b.find(&[], Some(1)).iter().any(|r| r[0] == 2.into() && r[1] == "b".into()));
    }

    #[test]
    fn query_multi() {
        let a1 = vec![1.into(), "a".into()];
        let b2 = vec![2.into(), "b".into()];
        let c3 = vec![3.into(), "c".into()];

        let mut b = BufferedStore::new(2);

        b.add(vec![ops::Record::Positive(a1.clone()), ops::Record::Positive(b2.clone())],
              0);
        assert_eq!(b.find(&[], Some(0)).len(), 2);
        assert!(b.find(&[], Some(0)).iter().any(|r| r[0] == 1.into() && r[1] == "a".into()));
        assert!(b.find(&[], Some(0)).iter().any(|r| r[0] == 2.into() && r[1] == "b".into()));

        b.add(vec![ops::Record::Negative(a1.clone()),
                   ops::Record::Positive(c3.clone()),
                   ops::Record::Negative(c3.clone())],
              1);
        assert_eq!(b.find(&[], Some(1)).len(), 1);
        assert!(b.find(&[], Some(1)).iter().any(|r| r[0] == 2.into() && r[1] == "b".into()));
    }

    #[test]
    fn query_complex() {
        let a1 = vec![1.into(), "a".into()];
        let b2 = vec![2.into(), "b".into()];
        let c3 = vec![3.into(), "c".into()];

        let mut b = BufferedStore::new(2);

        b.add(vec![ops::Record::Negative(a1.clone()), ops::Record::Positive(b2.clone())],
              0);
        b.add(vec![ops::Record::Negative(b2.clone()), ops::Record::Positive(c3.clone())],
              1);
        assert_eq!(b.find(&[], Some(2)), vec![&*c3]);
    }
}
