use launchpad::hash::hash;
use launchpad::Indices;
use maplit::hashmap;
use noria::{internal, invariant};
use std::borrow::Cow;
use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::{hash_map::DefaultHasher, HashMap};
use std::convert::TryInto;
use std::hash::{Hash, Hasher};
use std::num::NonZeroUsize;
use std::ops::Index;

use crate::prelude::*;

use crate::processing::ColumnSource;
use nom_sql::OrderType;
use noria::errors::{internal_err, ReadySetResult};

#[derive(Clone, Serialize, Deserialize)]
struct Order(Vec<(usize, OrderType)>);
impl Order {
    fn cmp(&self, a: &[DataType], b: &[DataType]) -> Ordering {
        for &(c, ref order_type) in &self.0 {
            let result = match *order_type {
                OrderType::OrderAscending => a[c].cmp(&b[c]),
                OrderType::OrderDescending => b[c].cmp(&a[c]),
            };
            if result != Ordering::Equal {
                return result;
            }
        }
        Ordering::Equal
    }
}

impl From<Vec<(usize, OrderType)>> for Order {
    fn from(other: Vec<(usize, OrderType)>) -> Self {
        Order(other)
    }
}

type GroupHash = u64;

/// TopK provides an operator that will produce the top k elements for each group.
///
/// Positives are generally fast to process, while negative records can trigger expensive backwards
/// queries. It is also worth noting that due the nature of Soup, the results of this operator are
/// unordered.
#[derive(Clone, Serialize, Deserialize)]
pub struct TopK {
    src: IndexPair,

    /// Cached records, keyed by the hash of the group and ordered by the ordering of this TopK,
    /// used when we are fully materialized and run out of records
    ///
    /// Note that key is equal to the *hash* of the group, to avoid the overhead of storing the
    /// entire group
    ///
    /// Invariant: If this node is partial, this will always be empty
    extra_records: RefCell<HashMap<GroupHash, Vec<Vec<DataType>>>>,

    // some cache state
    us: Option<IndexPair>,
    cols: usize,

    // precomputed datastructures
    group_by: Vec<usize>,

    order: Order,
    k: usize,
}

impl TopK {
    /// Construct a new TopK operator.
    ///
    /// # Arguments
    ///
    /// * `src` - this operator's ancestor
    /// * `order` - The list of columns to compute top k over
    /// * `group_by` - the columns that this operator is keyed on
    /// * `k` - the maximum number of results per group.
    pub fn new(
        src: NodeIndex,
        order: Vec<(usize, OrderType)>,
        group_by: Vec<usize>,
        k: usize,
    ) -> Self {
        let mut group_by = group_by;
        group_by.sort_unstable();

        TopK {
            src: src.into(),

            extra_records: RefCell::new(Default::default()),

            us: None,
            cols: 0,

            group_by,
            order: order.into(),
            k,
        }
    }

    /// Project the columns we are grouping by out of the given record
    fn project_group<'rec, R>(&self, rec: &'rec R) -> Vec<&'rec DataType>
    where
        R: Index<usize, Output = DataType> + ?Sized,
    {
        rec.indices(self.group_by.clone())
    }

    /// Calculate a hash for the columns we are grouping by out of the given record, for use in
    /// `self.extra_records`
    fn group_hash<R>(&self, rec: &R) -> GroupHash
    where
        R: Index<usize, Output = DataType> + ?Sized,
    {
        let mut hasher = DefaultHasher::new();
        self.project_group(rec).hash(&mut hasher);
        hasher.finish()
    }

    /// Compare two records based on the columns we're grouping by
    fn group_cmp(&self, rec_a: &Record, rec_b: &Record) -> Ordering {
        self.project_group(rec_a.rec())
            .cmp(&self.project_group(rec_b.rec()))
    }
}

impl Ingredient for TopK {
    fn take(&mut self) -> NodeOperator {
        self.clone().into()
    }

    fn ancestors(&self) -> Vec<NodeIndex> {
        vec![self.src.as_global()]
    }

    fn on_connected(&mut self, g: &Graph) {
        let srcn = &g[self.src.as_global()];
        self.cols = srcn.fields().len();
    }

    fn on_commit(&mut self, us: NodeIndex, remap: &HashMap<NodeIndex, IndexPair>) {
        // who's our parent really?
        self.src.remap(remap);

        // who are we?
        self.us = Some(remap[&us]);
    }

    #[allow(clippy::cognitive_complexity)]
    fn on_input(
        &mut self,
        _: &mut dyn Executor,
        from: LocalNodeIndex,
        rs: Records,
        replay_key_cols: Option<&[usize]>,
        _: &DomainNodes,
        state: &StateMap,
    ) -> ReadySetResult<ProcessingResult> {
        debug_assert_eq!(from, *self.src);

        if rs.is_empty() {
            return Ok(ProcessingResult {
                results: rs,
                ..Default::default()
            });
        }

        // First, we want to be smart about multiple added/removed rows with same group.
        // For example, if we get a -, then a +, for the same group, we don't want to
        // execute two queries. We'll do this by sorting the batch by our group by.
        let mut rs: Vec<_> = rs.into();
        rs.sort_by(|l, r| self.group_cmp(l, r));

        let us = self.us.unwrap();
        let db: &dyn State = state
            .get(*us)
            .ok_or_else(|| internal_err("topk operators must have their own state materialized"))?
            .as_ref();

        let mut out = Vec::new();
        let mut grp = Vec::new();
        let mut grpk = 0;
        let mut missed = false;
        // current holds (Cow<Row>, bool) where bool = is_new
        let mut current: Vec<(Cow<[DataType]>, bool)> = Vec::new();
        let mut misses = Vec::new();
        let mut lookups = Vec::new();
        let is_partial = db.is_partial();

        let post_group = |out: &mut Vec<Record>,
                          current: &mut Vec<(Cow<[DataType]>, bool)>,
                          grp: &mut Vec<DataType>,
                          grpk: usize|
         -> ReadySetResult<_> {
            current.sort_unstable_by(|a, b| self.order.cmp(&*a.0, &*b.0));

            let start = current.len().saturating_sub(self.k);

            if grpk == self.k {
                if let Some(diff) = grpk.checked_sub(current.len()).and_then(NonZeroUsize::new) {
                    // there used to be k things in the group, now there are fewer than k.
                    if is_partial {
                        // If we're partially materialized, we can't (currently) do anything
                        internal!("partially materialized TopK has fewer than k")
                    } else {
                        // If we're fully materialized, that means we've been keeping track of
                        // records *out of* our topk group in `self.current_records` - let's
                        // backfill up to k from that if we can.
                        if let Some(ref mut extra_records) =
                            self.extra_records.borrow_mut().get_mut(&hash(&grp))
                        {
                            current.extend(
                                extra_records
                                    .drain(0..diff.into())
                                    .map(|r| (r.into(), true)),
                            );
                        }
                    }
                }

                // FIXME: if all the elements with the smallest value in the new topk are new,
                // then it *could* be that there exists some value that is greater than all
                // those values, and <= the smallest old value. we would only discover that by
                // querying. unfortunately, the check below isn't *quite* right because it does
                // not consider old rows that were removed in this batch (which should still be
                // counted for this condition).
                if false {
                    let all_new_bottom = current[start..]
                        .iter()
                        .take_while(|(ref r, _)| {
                            self.order.cmp(r, &current[start].0) == Ordering::Equal
                        })
                        .all(|&(_, is_new)| is_new);
                    if all_new_bottom {
                        eprintln!("topk is guesstimating bottom row");
                    }
                }
            }

            // optimization: if we don't *have to* remove something, we don't
            for i in start..current.len() {
                if current[i].1 {
                    // we found an `is_new` in current
                    // can we replace it with a !is_new with the same order value?
                    let replace = current[0..start].iter().position(|&(ref r, is_new)| {
                        !is_new && self.order.cmp(r, &current[i].0) == Ordering::Equal
                    });
                    if let Some(ri) = replace {
                        current.swap(i, ri);
                    }
                }
            }

            for (r, is_new) in current.drain(start..) {
                if is_new {
                    out.push(Record::Positive(r.into_owned()));
                }
            }

            if !current.is_empty() {
                for (r, is_new) in current.drain(..) {
                    if !is_new {
                        // Was in k, now isn't
                        out.push(Record::Negative(r.clone().into()));
                    }

                    if !is_partial {
                        // If we're fully materialized, save records beyond k into `extra_records`
                        // so we can use them if we ever receive a negative.
                        let mut extra_records = self.extra_records.borrow_mut();
                        let entry = extra_records.entry(self.group_hash(&(*r))).or_default();
                        entry.push(r.into());
                        // TODO(grfn): Sorting here every step of the way is not optimal, we should
                        // make some sort of btree here wrapping a type with an (unsafe) reference
                        // to self.order for comparison
                        entry.sort_unstable_by(|a, b| self.order.cmp(b, a));
                    }
                }
            }
            Ok(())
        };

        // records are now chunked by group
        for r in &rs {
            if grp.iter().cmp(self.project_group(r.rec())) != Ordering::Equal {
                // new group!

                // first, tidy up the old one
                if !grp.is_empty() {
                    post_group(&mut out, &mut current, &mut grp, grpk)?;
                }
                invariant!(current.is_empty());

                // make ready for the new one
                // NOTE(grfn): Is this the most optimal way of doing this?
                grp.clear();
                grp.extend(self.project_group(r.rec()).into_iter().cloned());

                // check out current state
                match db.lookup(&self.group_by[..], &KeyType::from(&grp[..])) {
                    LookupResult::Some(local_records) => {
                        if replay_key_cols.is_some() {
                            lookups.push(Lookup {
                                on: *us,
                                cols: self.group_by.clone(),
                                key: grp.clone().try_into().expect("Empty group"),
                            });
                        }

                        missed = false;
                        grpk = local_records.len();
                        current.extend(local_records.into_iter().map(|r| (r.clone(), false)))
                    }
                    LookupResult::Missing => {
                        missed = true;
                    }
                }
            }

            if missed {
                misses.push(Miss {
                    on: *us,
                    lookup_idx: self.group_by.clone(),
                    lookup_cols: self.group_by.clone(),
                    replay_cols: replay_key_cols.map(Vec::from),
                    record: r.row().clone().try_into().expect("Empty record"),
                });
            } else {
                match r {
                    Record::Positive(r) => current.push((Cow::Owned(r.clone()), true)),
                    Record::Negative(r) => {
                        if let Some(p) = current.iter().position(|&(ref x, _)| *r == **x) {
                            let (_, was_new) = current.swap_remove(p);
                            // was_new = we received a positive and a negative
                            // for the same value in one batch
                            if !was_new {
                                out.push(Record::Negative(r.clone()));
                            }
                        }
                    }
                }
            }
        }

        if !grp.is_empty() {
            post_group(&mut out, &mut current, &mut grp, grpk)?;
        }

        Ok(ProcessingResult {
            results: out.into(),
            lookups,
            misses,
        })
    }

    fn suggest_indexes(&self, this: NodeIndex) -> HashMap<NodeIndex, noria::internal::Index> {
        hashmap! {
            this => noria::internal::Index::hash_map(self.group_by.clone())
        }
    }

    fn column_source(&self, cols: &[usize]) -> ReadySetResult<ColumnSource> {
        Ok(ColumnSource::exact_copy(
            self.src.as_global(),
            cols.try_into().unwrap(),
        ))
    }

    fn description(&self, detailed: bool) -> String {
        if !detailed {
            return String::from("TopK");
        }

        let group_cols = self
            .group_by
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        format!("TopK γ[{}]", group_cols)
    }

    fn is_selective(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::ops;

    fn setup(reversed: bool) -> (ops::test::MockGraph, IndexPair) {
        let cmp_rows = if reversed {
            vec![(2, OrderType::OrderDescending)]
        } else {
            vec![(2, OrderType::OrderAscending)]
        };

        let mut g = ops::test::MockGraph::new();
        let s = g.add_base("source", &["x", "y", "z"]);

        g.set_op(
            "topk",
            &["x", "y", "z"],
            TopK::new(s.as_global(), cmp_rows, vec![1], 3),
            true,
        );
        (g, s)
    }

    #[test]
    fn it_keeps_topk() {
        let (mut g, _) = setup(false);
        let ni = g.node().local_addr();

        let r12: Vec<DataType> = vec![1.into(), "z".into(), 12.into()];
        let r10: Vec<DataType> = vec![2.into(), "z".into(), 10.into()];
        let r11: Vec<DataType> = vec![3.into(), "z".into(), 11.into()];
        let r5: Vec<DataType> = vec![4.into(), "z".into(), 5.into()];
        let r15: Vec<DataType> = vec![5.into(), "z".into(), 15.into()];
        let r10b: Vec<DataType> = vec![6.into(), "z".into(), 10.into()];
        let r10c: Vec<DataType> = vec![7.into(), "z".into(), 10.into()];

        g.narrow_one_row(r12, true);
        g.narrow_one_row(r11, true);
        g.narrow_one_row(r5, true);
        g.narrow_one_row(r10b, true);
        g.narrow_one_row(r10c, true);
        assert_eq!(g.states[ni].rows(), 3);

        g.narrow_one_row(r15, true);
        g.narrow_one_row(r10, true);
        assert_eq!(g.states[ni].rows(), 3);
    }

    #[test]
    fn it_forwards() {
        let (mut g, _) = setup(false);

        let r12: Vec<DataType> = vec![1.into(), "z".into(), 12.into()];
        let r10: Vec<DataType> = vec![2.into(), "z".into(), 10.into()];
        let r11: Vec<DataType> = vec![3.into(), "z".into(), 11.into()];
        let r5: Vec<DataType> = vec![4.into(), "z".into(), 5.into()];
        let r15: Vec<DataType> = vec![5.into(), "z".into(), 15.into()];

        let a = g.narrow_one_row(r12.clone(), true);
        assert_eq!(a, vec![r12].into());

        let a = g.narrow_one_row(r10.clone(), true);
        assert_eq!(a, vec![r10.clone()].into());

        let a = g.narrow_one_row(r11.clone(), true);
        assert_eq!(a, vec![r11].into());

        let a = g.narrow_one_row(r5, true);
        assert_eq!(a.len(), 0);

        let a = g.narrow_one_row(r15.clone(), true);
        assert_eq!(a.len(), 2);
        assert!(a.iter().any(|r| r == &(r10.clone(), false).into()));
        assert!(a.iter().any(|r| r == &(r15.clone(), true).into()));
    }

    #[test]
    fn it_caches_when_full() {
        let (mut g, _) = setup(false);

        let r12: Vec<DataType> = vec![1.into(), "z".into(), 12.into()];
        let r10: Vec<DataType> = vec![2.into(), "z".into(), 10.into()];
        let r11: Vec<DataType> = vec![3.into(), "z".into(), 11.into()];
        let r5: Vec<DataType> = vec![4.into(), "z".into(), 5.into()];
        let r15: Vec<DataType> = vec![5.into(), "z".into(), 15.into()];

        // fill topk
        g.narrow_one_row(r12, true);
        g.narrow_one_row(r10.clone(), true);
        g.narrow_one_row(r11, true);
        g.narrow_one_row(r5, true);
        g.narrow_one_row(r15.clone(), true);

        // [5, z, 15]
        // [1, z, 12]
        // [3, z, 11]

        // check that removing 15 brings back 10
        let delta = g.narrow_one_row((r15.clone(), false), true);
        assert_eq!(delta.len(), 2); // one negative, one positive
        assert!(delta.iter().any(|r| r == &(r15.clone(), false).into()));
        assert!(
            delta.iter().any(|r| r == &(r10.clone(), true).into()),
            "a = {:?} does not contain ({:?}, true)",
            &delta,
            r10
        );
    }

    #[test]
    fn it_caches_when_full_reversed() {
        let (mut g, _) = setup(true);

        let r12: Vec<DataType> = vec![1.into(), "z".into(), 12.into()];
        let r10: Vec<DataType> = vec![2.into(), "z".into(), 10.into()];
        let r11: Vec<DataType> = vec![3.into(), "z".into(), 11.into()];
        let r5: Vec<DataType> = vec![4.into(), "z".into(), 5.into()];
        let r15: Vec<DataType> = vec![5.into(), "z".into(), 15.into()];

        // fill topk
        g.narrow_one_row(r12.clone(), true);
        g.narrow_one_row(r10, true);
        g.narrow_one_row(r11, true);
        g.narrow_one_row(r5.clone(), true);
        g.narrow_one_row(r15, true);

        // [4, z, 5]
        // [2, z, 10]
        // [3, z, 11]

        // check that removing 5 brings back 12
        let delta = g.narrow_one_row((r5.clone(), false), true);
        assert_eq!(delta.len(), 2); // one negative, one positive
        assert!(delta.iter().any(|r| r == &(r5.clone(), false).into()));
        assert!(
            delta.iter().any(|r| r == &(r12.clone(), true).into()),
            "a = {:?} does not contain ({:?}, true)",
            &delta,
            r12
        );
    }

    #[test]
    fn it_forwards_reversed() {
        use std::convert::TryFrom;

        let (mut g, _) = setup(true);

        let r12: Vec<DataType> = vec![1.into(), "z".into(), DataType::try_from(-12.123).unwrap()];
        let r10: Vec<DataType> = vec![2.into(), "z".into(), DataType::try_from(0.0431).unwrap()];
        let r11: Vec<DataType> = vec![3.into(), "z".into(), DataType::try_from(-0.082).unwrap()];
        let r5: Vec<DataType> = vec![4.into(), "z".into(), DataType::try_from(5.601).unwrap()];
        let r15: Vec<DataType> = vec![5.into(), "z".into(), DataType::try_from(-15.9).unwrap()];

        let a = g.narrow_one_row(r12.clone(), true);
        assert_eq!(a, vec![r12].into());

        let a = g.narrow_one_row(r10.clone(), true);
        assert_eq!(a, vec![r10.clone()].into());

        let a = g.narrow_one_row(r11.clone(), true);
        assert_eq!(a, vec![r11].into());

        let a = g.narrow_one_row(r5, true);
        assert_eq!(a.len(), 0);

        let a = g.narrow_one_row(r15.clone(), true);
        assert_eq!(a.len(), 2);
        assert!(a.iter().any(|r| r == &(r10.clone(), false).into()));
        assert!(a.iter().any(|r| r == &(r15.clone(), true).into()));
    }

    #[test]
    fn it_suggests_indices() {
        let (g, _) = setup(false);
        let me = 2.into();
        let idx = g.node().suggest_indexes(me);
        assert_eq!(idx.len(), 1);
        assert_eq!(
            *idx.iter().next().unwrap().1,
            noria::internal::Index::hash_map(vec![1])
        );
    }

    #[test]
    fn it_resolves() {
        let (g, _) = setup(false);
        assert_eq!(
            g.node().resolve(0).unwrap(),
            Some(vec![(g.narrow_base_id().as_global(), 0)])
        );
        assert_eq!(
            g.node().resolve(1).unwrap(),
            Some(vec![(g.narrow_base_id().as_global(), 1)])
        );
        assert_eq!(
            g.node().resolve(2).unwrap(),
            Some(vec![(g.narrow_base_id().as_global(), 2)])
        );
    }

    #[test]
    fn it_parent_columns() {
        let (g, _) = setup(false);
        assert_eq!(
            g.node().resolve(0).unwrap(),
            Some(vec![(g.narrow_base_id().as_global(), 0)])
        );
        assert_eq!(
            g.node().resolve(1).unwrap(),
            Some(vec![(g.narrow_base_id().as_global(), 1)])
        );
        assert_eq!(
            g.node().resolve(2).unwrap(),
            Some(vec![(g.narrow_base_id().as_global(), 2)])
        );
    }

    #[test]
    fn it_handles_updates() {
        let (mut g, _) = setup(false);
        let ni = g.node().local_addr();

        let r1: Vec<DataType> = vec![1.into(), "z".into(), 10.into()];
        let r2: Vec<DataType> = vec![2.into(), "z".into(), 10.into()];
        let r3: Vec<DataType> = vec![3.into(), "z".into(), 10.into()];
        let r4: Vec<DataType> = vec![4.into(), "z".into(), 5.into()];
        let r4a: Vec<DataType> = vec![4.into(), "z".into(), 10.into()];
        let r4b: Vec<DataType> = vec![4.into(), "z".into(), 11.into()];

        g.narrow_one_row(r1, true);
        g.narrow_one_row(r2, true);
        g.narrow_one_row(r3, true);

        // a positive for a row not in the Top-K should not change the Top-K and shouldn't emit
        // anything
        let emit = g.narrow_one_row(r4.clone(), true);
        assert_eq!(g.states[ni].rows(), 3);
        assert_eq!(emit, Vec::<Record>::new().into());

        // should now have 3 rows in Top-K
        // [1, z, 10]
        // [2, z, 10]
        // [3, z, 10]

        let emit = g.narrow_one(
            vec![Record::Negative(r4), Record::Positive(r4a.clone())],
            true,
        );
        // nothing should have been emitted, as [4, z, 10] doesn't enter Top-K
        assert_eq!(emit, Vec::<Record>::new().into());

        let emit = g.narrow_one(vec![Record::Negative(r4a), Record::Positive(r4b)], true);

        // now [4, z, 11] is in, BUT we still only keep 3 elements
        // and have to remove one of the existing ones
        assert_eq!(g.states[ni].rows(), 3);
        assert_eq!(emit.len(), 2); // 1 pos, 1 neg
        assert!(emit.iter().any(|r| !r.is_positive() && r[2] == 10.into()));
        assert!(emit.iter().any(|r| r.is_positive() && r[2] == 11.into()));
    }
}
