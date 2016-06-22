use ops;
use query;
use shortcut;

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
                Some(&(ts, _)) if ts > including => break,
                _ => (),
            }

            for r in self.backlog.pop_front().unwrap().1.into_iter() {
                match r {
                    ops::Record::Positive(r) => {
                        self.store.insert(r);
                    }
                    ops::Record::Negative(_) => {
                        // TODO
                        // delete the corresponding row.
                        // we can use data.delete(), but need to come up with a condition
                        // that will match *only* that one row (not, for example, other
                        // rows with all the same values). do we need uniquifiers again?
                        unimplemented!();
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

    /// Find all entries that matched the given conditions just after the given point in time.
    ///
    /// This method will panic if the given timestamp falls before the last absorbed timestamp, as
    /// it cannot guarantee correct results in that case. Queries *at* the time of the last absorb
    /// are fine.
    ///
    /// Completes in `O(Store::find + b)` where `b` is the number of records in the backlog whose
    /// timestamp fall at or before the given timestamp.
    pub fn find<'a>(&'a self,
                    conds: &'a [shortcut::cmp::Condition<query::DataType>],
                    including: i64)
                    -> Box<Iterator<Item = &'a [query::DataType]> + 'a> {
        assert!(including >= self.absorbed);
        self.store.find(conds)
    }
}
