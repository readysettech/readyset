use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::HashMap;

use crate::prelude::*;

use nom_sql::OrderType;

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

/// TopK provides an operator that will produce the top k elements for each group.
///
/// Positives are generally fast to process, while negative records can trigger expensive backwards
/// queries. It is also worth noting that due the nature of Soup, the results of this operator are
/// unordered.
#[derive(Clone, Serialize, Deserialize)]
pub struct TopK {
    src: IndexPair,

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
    /// `src` is this operator's ancestor, `over` is the column to compute the top K over,
    /// `group_by` indicates the columns that this operator is keyed on, and k is the maximum number
    /// of results per group.
    pub fn new(
        src: NodeIndex,
        order: Vec<(usize, OrderType)>,
        group_by: Vec<usize>,
        k: usize,
    ) -> Self {
        let mut group_by = group_by;
        group_by.sort();

        TopK {
            src: src.into(),

            us: None,
            cols: 0,

            group_by,
            order: order.into(),
            k,
        }
    }
}

impl Ingredient for TopK {
    fn take(&mut self) -> NodeOperator {
        // Necessary because cmp_rows can't be cloned.
        Self {
            src: self.src,

            us: self.us,
            cols: self.cols,

            group_by: self.group_by.clone(),

            order: self.order.clone(),
            k: self.k,
        }
        .into()
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
    ) -> ProcessingResult {
        debug_assert_eq!(from, *self.src);

        if rs.is_empty() {
            return ProcessingResult {
                results: rs,
                ..Default::default()
            };
        }

        let group_by = &self.group_by;
        let group_cmp = |a: &Record, b: &Record| {
            group_by
                .iter()
                .map(|&col| &a[col])
                .cmp(group_by.iter().map(|&col| &b[col]))
        };

        // First, we want to be smart about multiple added/removed rows with same group.
        // For example, if we get a -, then a +, for the same group, we don't want to
        // execute two queries. We'll do this by sorting the batch by our group by.
        let mut rs: Vec<_> = rs.into();
        rs.sort_by(&group_cmp);

        let us = self.us.unwrap();
        let db = state
            .get(*us)
            .expect("topk operators must have their own state materialized");

        let mut out = Vec::new();
        let mut grp = Vec::new();
        let mut grpk = 0;
        let mut missed = false;
        // current holds (Cow<Row>, bool) where bool = is_new
        let mut current: Vec<(Cow<[DataType]>, bool)> = Vec::new();
        let mut misses = Vec::new();
        let mut lookups = Vec::new();

        macro_rules! post_group {
            ($out:ident, $current:ident, $grpk:expr, $k:expr, $order:expr) => {{
                $current.sort_unstable_by(|a, b| $order.cmp(&*a.0, &*b.0));

                let start = $current.len().saturating_sub($k);

                if $grpk == $k {
                    if $current.len() < $grpk {
                        // there used to be k things in the group
                        // now there are fewer than k
                        // we don't know if querying would bring us back to k
                        unimplemented!();
                    }

                    // FIXME: if all the elements with the smallest value in the new topk are new,
                    // then it *could* be that there exists some value that is greater than all
                    // those values, and <= the smallest old value. we would only discover that by
                    // querying. unfortunately, the check below isn't *quite* right because it does
                    // not consider old rows that were removed in this batch (which should still be
                    // counted for this condition).
                    if false {
                        let all_new_bottom = $current[start..]
                            .iter()
                            .take_while(|(ref r, _)| {
                                $order.cmp(r, &$current[start].0) == Ordering::Equal
                            })
                            .all(|&(_, is_new)| is_new);
                        if all_new_bottom {
                            eprintln!("topk is guesstimating bottom row");
                        }
                    }
                }

                // optimization: if we don't *have to* remove something, we don't
                for i in start..$current.len() {
                    if $current[i].1 {
                        // we found an `is_new` in current
                        // can we replace it with a !is_new with the same order value?
                        let replace = $current[0..start].iter().position(|&(ref r, is_new)| {
                            !is_new && $order.cmp(r, &$current[i].0) == Ordering::Equal
                        });
                        if let Some(ri) = replace {
                            $current.swap(i, ri);
                        }
                    }
                }

                for (r, is_new) in $current.drain(start..) {
                    if is_new {
                        $out.push(Record::Positive(r.into_owned()));
                    }
                }

                if !$current.is_empty() {
                    $out.extend($current.drain(..).filter_map(|(r, is_new)| {
                        if !is_new {
                            Some(Record::Negative(r.into_owned()))
                        } else {
                            None
                        }
                    }));
                }
            }};
        };

        for r in rs {
            if grp.iter().cmp(group_by.iter().map(|&col| &r[col])) != Ordering::Equal {
                // new group!

                // first, tidy up the old one
                if !grp.is_empty() {
                    post_group!(out, current, grpk, self.k, self.order);
                }

                // make ready for the new one
                grp.clear();
                grp.extend(group_by.iter().map(|&col| &r[col]).cloned());

                // check out current state
                match db.lookup(&group_by[..], &KeyType::from(&grp[..])) {
                    LookupResult::Some(rs) => {
                        if replay_key_cols.is_some() {
                            lookups.push(Lookup {
                                on: *us,
                                cols: group_by.clone(),
                                key: grp.clone(),
                            });
                        }

                        missed = false;
                        grpk = rs.len();
                        current.extend(rs.into_iter().map(|r| (r, false)))
                    }
                    LookupResult::Missing => {
                        missed = true;
                    }
                }
            }

            if missed {
                misses.push(Miss {
                    on: *us,
                    lookup_idx: group_by.clone(),
                    lookup_cols: group_by.clone(),
                    replay_cols: replay_key_cols.map(Vec::from),
                    record: r.extract().0,
                });
            } else {
                match r {
                    Record::Positive(r) => current.push((Cow::Owned(r), true)),
                    Record::Negative(r) => {
                        if let Some(p) = current.iter().position(|&(ref x, _)| *r == **x) {
                            let (_, was_new) = current.swap_remove(p);
                            if !was_new {
                                out.push(Record::Negative(r));
                            }
                        }
                    }
                }
            }
        }
        if !grp.is_empty() {
            post_group!(out, current, grpk, self.k, self.order);
        }

        ProcessingResult {
            results: out.into(),
            lookups,
            misses,
        }
    }

    fn on_eviction(
        &mut self,
        _: LocalNodeIndex,
        key_columns: &[usize],
        _: &mut Vec<Vec<DataType>>,
    ) {
        assert_eq!(key_columns, &self.group_by[..]);
    }

    fn suggest_indexes(&self, this: NodeIndex) -> HashMap<NodeIndex, Vec<usize>> {
        vec![(this, self.group_by.clone())].into_iter().collect()
    }

    fn resolve(&self, col: usize) -> Option<Vec<(NodeIndex, usize)>> {
        Some(vec![(self.src.as_global(), col)])
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

    fn parent_columns(&self, col: usize) -> Vec<(NodeIndex, Option<usize>)> {
        vec![(self.src.as_global(), Some(col))]
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

        g.narrow_one_row(r12.clone(), true);
        g.narrow_one_row(r11.clone(), true);
        g.narrow_one_row(r5.clone(), true);
        g.narrow_one_row(r10b.clone(), true);
        g.narrow_one_row(r10c.clone(), true);
        assert_eq!(g.states[ni].rows(), 3);

        g.narrow_one_row(r15.clone(), true);
        g.narrow_one_row(r10.clone(), true);
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
        assert_eq!(a, vec![r12.clone()].into());

        let a = g.narrow_one_row(r10.clone(), true);
        assert_eq!(a, vec![r10.clone()].into());

        let a = g.narrow_one_row(r11.clone(), true);
        assert_eq!(a, vec![r11.clone()].into());

        let a = g.narrow_one_row(r5.clone(), true);
        assert_eq!(a.len(), 0);

        let a = g.narrow_one_row(r15.clone(), true);
        assert_eq!(a.len(), 2);
        assert!(a.iter().any(|r| r == &(r10.clone(), false).into()));
        assert!(a.iter().any(|r| r == &(r15.clone(), true).into()));
    }

    #[test]
    #[ignore]
    fn it_must_query() {
        let (mut g, s) = setup(false);

        let r12: Vec<DataType> = vec![1.into(), "z".into(), 12.into()];
        let r10: Vec<DataType> = vec![2.into(), "z".into(), 10.into()];
        let r11: Vec<DataType> = vec![3.into(), "z".into(), 11.into()];
        let r5: Vec<DataType> = vec![4.into(), "z".into(), 5.into()];
        let r15: Vec<DataType> = vec![5.into(), "z".into(), 15.into()];
        let r10b: Vec<DataType> = vec![6.into(), "z".into(), 10.into()];
        let r10c: Vec<DataType> = vec![7.into(), "z".into(), 10.into()];

        // fill topk
        g.narrow_one_row(r12.clone(), true);
        g.narrow_one_row(r10.clone(), true);
        g.narrow_one_row(r11.clone(), true);
        g.narrow_one_row(r5.clone(), true);
        g.narrow_one_row(r15.clone(), true);

        // put stuff to query for in the bases
        g.seed(s, r12.clone());
        g.seed(s, r10.clone());
        g.seed(s, r11.clone());
        g.seed(s, r5.clone());

        // check that removing 15 brings back 10
        let a = g.narrow_one_row((r15.clone(), false), true);
        assert_eq!(a.len(), 2);
        assert!(a.iter().any(|r| r == &(r15.clone(), false).into()));
        assert!(a.iter().any(|r| r == &(r10.clone(), true).into()));
        g.unseed(s);

        let a = g.narrow_one_row(r10b.clone(), true);
        assert_eq!(a.len(), 0);

        let a = g.narrow_one_row(r10c.clone(), true);
        assert_eq!(a.len(), 0);

        g.seed(s, r12.clone());
        g.seed(s, r11.clone());
        g.seed(s, r5.clone());
        g.seed(s, r10b.clone());
        g.seed(s, r10c.clone());
        let a = g.narrow_one_row((r10.clone(), false), true);
        assert_eq!(a.len(), 2);
        assert_eq!(a[0], (r10.clone(), false).into());
        assert!(a[1] == (r10b.clone(), true).into() || a[1] == (r10c.clone(), true).into());
    }

    #[test]
    fn it_forwards_reversed() {
        let (mut g, _) = setup(true);

        let r12: Vec<DataType> = vec![1.into(), "z".into(), (-12.123).into()];
        let r10: Vec<DataType> = vec![2.into(), "z".into(), (0.0431).into()];
        let r11: Vec<DataType> = vec![3.into(), "z".into(), (-0.082).into()];
        let r5: Vec<DataType> = vec![4.into(), "z".into(), (5.601).into()];
        let r15: Vec<DataType> = vec![5.into(), "z".into(), (-15.9).into()];

        let a = g.narrow_one_row(r12.clone(), true);
        assert_eq!(a, vec![r12.clone()].into());

        let a = g.narrow_one_row(r10.clone(), true);
        assert_eq!(a, vec![r10.clone()].into());

        let a = g.narrow_one_row(r11.clone(), true);
        assert_eq!(a, vec![r11.clone()].into());

        let a = g.narrow_one_row(r5.clone(), true);
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
        assert_eq!(*idx.iter().next().unwrap().1, vec![1]);
    }

    #[test]
    fn it_resolves() {
        let (g, _) = setup(false);
        assert_eq!(
            g.node().resolve(0),
            Some(vec![(g.narrow_base_id().as_global(), 0)])
        );
        assert_eq!(
            g.node().resolve(1),
            Some(vec![(g.narrow_base_id().as_global(), 1)])
        );
        assert_eq!(
            g.node().resolve(2),
            Some(vec![(g.narrow_base_id().as_global(), 2)])
        );
    }

    #[test]
    fn it_parent_columns() {
        let (g, _) = setup(false);
        assert_eq!(
            g.node().resolve(0),
            Some(vec![(g.narrow_base_id().as_global(), 0)])
        );
        assert_eq!(
            g.node().resolve(1),
            Some(vec![(g.narrow_base_id().as_global(), 1)])
        );
        assert_eq!(
            g.node().resolve(2),
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

        g.narrow_one_row(r1.clone(), true);
        g.narrow_one_row(r2.clone(), true);
        g.narrow_one_row(r3.clone(), true);

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
            vec![Record::Negative(r4.clone()), Record::Positive(r4a.clone())],
            true,
        );
        // nothing should have been emitted, as [4, z, 10] doesn't enter Top-K
        assert_eq!(emit, Vec::<Record>::new().into());

        let emit = g.narrow_one(
            vec![Record::Negative(r4a.clone()), Record::Positive(r4b.clone())],
            true,
        );

        // now [4, z, 11] is in, BUT we still only keep 3 elements
        // and have to remove one of the existing ones
        assert_eq!(g.states[ni].rows(), 3);
        assert_eq!(emit.len(), 2); // 1 pos, 1 neg
        assert!(emit.iter().any(|r| !r.is_positive() && r[2] == 10.into()));
        assert!(emit.iter().any(|r| r.is_positive() && r[2] == 11.into()));
    }
}
