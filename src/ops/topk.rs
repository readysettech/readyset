use std::mem;
use std::collections::HashMap;
use std::sync::Arc;

use flow::prelude::*;
use std::cmp::Ordering;

pub type OrderedRecordComparator =
    Fn(&&Arc<Vec<DataType>>, &&Arc<Vec<DataType>>) -> Ordering + Send + 'static;

/// TopK provides an operator that will produce the top k elements for each group.
///
/// Positives are generally fast to process, while negative records can trigger expensive backwards
/// queries. It is also worth noting that due the nature of Soup, the results of this operator are
/// unordered.
pub struct TopK {
    src: NodeAddress,

    // some cache state
    us: Option<NodeAddress>,
    cols: usize,

    // precomputed datastructures
    group_by: Vec<usize>,

    cmp_rows: Box<OrderedRecordComparator>,
    k: usize,

    counts: HashMap<Vec<DataType>, usize>,
}

impl TopK {
    /// Construct a new TopK operator.
    ///
    /// `src` is this operator's ancestor, `over` is the column to compute the top K over,
    /// `group_by` indicates the columns that this operator is keyed on, and k is the maximum number
    /// of results per group.
    pub fn new(src: NodeAddress,
               cmp_rows: Box<OrderedRecordComparator>,
               group_by: Vec<usize>,
               k: usize)
               -> Self {
        let mut group_by = group_by;
        group_by.sort();

        TopK {
            src: src,

            us: None,
            cols: 0,

            group_by: group_by,

            cmp_rows: cmp_rows,
            k: k,

            counts: HashMap::new(),
        }
    }

    /// Returns the set of Record structs to be emitted by this node, for some group. In steady
    /// state operation this will typically include some number of positives (at most k), and the
    /// same number of negatives.
    ///
    /// Cannot result in partial misses because we received a record with this key, so our parent
    /// must have seen and emitted that key. If they missed, they would have replayed *before*
    /// relaying to us. thus, since we got this key, our parent must have it.
    fn apply(&self,
             current_topk: &[Arc<Vec<DataType>>],
             new: Records,
             state: &StateMap,
             group: &[DataType])
             -> Records {

        let mut delta: Vec<Record> = Vec::new();
        let mut current: Vec<&Arc<Vec<DataType>>> = current_topk.iter().collect();
        current.sort_by(self.cmp_rows.as_ref());
        for r in new.iter() {
            if let &Record::Negative(ref a) = r {
                let idx = current.binary_search_by(|row| (self.cmp_rows)(&row, &&a));
                if let Ok(idx) = idx {
                    current.remove(idx);
                    delta.push(r.clone())
                }
            }
        }

        let mut output_rows: Vec<(&Arc<Vec<DataType>>, bool)> = new.iter()
            .filter_map(|r| match r {
                            &Record::Positive(ref a) => Some((a, false)),
                            _ => None,
                        })
            .chain(current.into_iter().map(|a| (a, true)))
            .collect();
        output_rows.sort_by(|a, b| (self.cmp_rows)(&a.0, &b.0));

        let src_db =
            state.get(self.src.as_local()).expect("topk must have its parent's state materialized");
        if output_rows.len() < self.k {
            let rs = match src_db.lookup(&self.group_by[..], &KeyType::from(group)) {
                LookupResult::Some(rs) => rs,
                LookupResult::Missing => unreachable!(),
            };

            // Get the minimum element of output_rows.
            if let Some((min, _)) = output_rows.iter().cloned().next() {
                let is_min = |&&(ref r, _): &&(&Arc<Vec<DataType>>, bool)| {
                    (self.cmp_rows)(&&r, &&min) == Ordering::Equal
                };

                let mut current_mins: Vec<_> = output_rows.iter()
                    .filter(is_min)
                    .cloned()
                    .collect();

                output_rows = rs.iter()
                    .filter_map(|r| {
                        // Make sure that no duplicates are added to output_rows. This is simplified
                        // by the fact that it currently contains all rows greater than `min`, and
                        // none less than it. The only complication are rows which compare equal to
                        // `min`: they get added except if there is already an identical row.
                        match (self.cmp_rows)(&r, &&min) {
                            Ordering::Less => Some((r, false)),
                            Ordering::Equal => {
                                let e = current_mins.iter().position(|&(ref s, _)| *s == r);
                                match e {
                                    Some(i) => {
                                        current_mins.swap_remove(i);
                                        None
                                    }
                                    None => Some((r, false)),
                                }
                            }
                            Ordering::Greater => None,
                        }
                    })
                    .chain(output_rows.into_iter())
                    .collect();
            } else {
                output_rows = rs.iter().map(|rs| (rs, false)).collect();
            }
            output_rows.sort_by(|a, b| (self.cmp_rows)(&a.0, &b.0));
        }

        if output_rows.len() > self.k {
            // Remove the topk elements from `output_rows`, splitting them off into `rows`. Then
            // swap and rename so that `output_rows` contains the top K elements, and `bottom_rows`
            // contains the rest.
            let i = output_rows.len() - self.k;
            let mut rows = output_rows.split_off(i);
            mem::swap(&mut output_rows, &mut rows);
            let bottom_rows = rows;

            // Emit negatives for any elements in `bottom_rows` that were originally in
            // current_topk.
            delta.extend(bottom_rows.into_iter()
                .filter(|p| p.1)
                .map(|p| Record::Negative(p.0.clone())));
        }

        // Emit positives for any elements in `output_rows` that weren't originally in current_topk.
        delta.extend(output_rows.into_iter().filter(|p| !p.1).map(|p| {
                                                                      Record::Positive(p.0.clone())
                                                                  }));
        delta.into()
    }
}

impl Ingredient for TopK {
    fn take(&mut self) -> Box<Ingredient> {
        // Necessary because cmp_rows can't be cloned.
        Box::new(Self {
                     src: self.src,

                     us: self.us,
                     cols: self.cols,

                     group_by: self.group_by.clone(),

                     cmp_rows: mem::replace(&mut self.cmp_rows, Box::new(|_, _| Ordering::Equal)),
                     k: self.k,

                     counts: self.counts.clone(),
                 })
    }

    fn ancestors(&self) -> Vec<NodeAddress> {
        vec![self.src]
    }

    fn should_materialize(&self) -> bool {
        true
    }

    fn will_query(&self, _: bool) -> bool {
        true
    }

    fn on_connected(&mut self, g: &Graph) {
        let srcn = &g[*self.src.as_global()];
        self.cols = srcn.fields().len();
    }

    fn on_commit(&mut self, us: NodeAddress, remap: &HashMap<NodeAddress, NodeAddress>) {
        // who's our parent really?
        self.src = remap[&self.src];

        // who are we?
        self.us = Some(us);
    }

    fn on_input(&mut self,
                from: NodeAddress,
                rs: Records,
                _: &DomainNodes,
                state: &StateMap)
                -> ProcessingResult {
        debug_assert_eq!(from, self.src);

        if rs.is_empty() {
            return ProcessingResult {
                       results: rs,
                       misses: vec![],
                   };
        }

        // First, we want to be smart about multiple added/removed rows with same group.
        // For example, if we get a -, then a +, for the same group, we don't want to
        // execute two queries.
        let mut consolidate = HashMap::new();
        for rec in rs.iter() {
            let group = rec.iter()
                .enumerate()
                .filter_map(|(i, v)| if self.group_by.iter().any(|col| col == &i) {
                                Some(v)
                            } else {
                                None
                            })
                .cloned()
                .collect::<Vec<_>>();

            consolidate.entry(group).or_insert_with(Vec::new).push(rec.clone());
        }

        // find the current value for each group
        let db = state.get(self.us
                               .as_ref()
                               .unwrap()
                               .as_local())
            .expect("topk must have its own state materialized");

        let mut misses = Vec::new();
        let mut out = Vec::with_capacity(2 * self.k);
        {
            let us = *self.us.unwrap().as_local();
            let group_by = &self.group_by[..];
            let current = consolidate.into_iter().filter_map(|(group, diffs)| {
                match db.lookup(group_by, &KeyType::from(&group[..])) {
                    LookupResult::Some(rs) => Some((group, diffs, rs)),
                    LookupResult::Missing => {
                        misses.push(Miss {
                                        node: us,
                                        key: group.clone(),
                                    });
                        None
                    }
                }
            });

            for (group, mut diffs, old_rs) in current {
                // Retrieve then update the number of times in this group
                let count: i64 = *self.counts.get(&group).unwrap_or(&0) as i64;
                let count_diff: i64 = diffs.iter()
                    .map(|r| match r {
                             &Record::Positive(..) => 1,
                             &Record::Negative(..) => -1,
                             &Record::DeleteRequest(..) => unreachable!(),
                         })
                    .sum();

                if count + count_diff <= self.k as i64 {
                    out.append(&mut diffs);
                } else {
                    assert!(count as usize >= old_rs.len());

                    out.append(&mut self.apply(old_rs, diffs.into(), state, &group[..]).into());
                }
                self.counts.insert(group, (count + count_diff) as usize);
            }
        }

        ProcessingResult {
            results: out.into(),
            misses: misses,
        }
    }

    fn suggest_indexes(&self, this: NodeAddress) -> HashMap<NodeAddress, Vec<usize>> {
        vec![(this, self.group_by.clone()), (self.src, self.group_by.clone())].into_iter().collect()
    }

    fn resolve(&self, col: usize) -> Option<Vec<(NodeAddress, usize)>> {
        Some(vec![(self.src, col)])
    }

    fn description(&self) -> String {
        format!("TopK")
    }

    fn parent_columns(&self, col: usize) -> Vec<(NodeAddress, Option<usize>)> {
        vec![(self.src, Some(col))]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use ops;

    fn setup(reversed: bool) -> (ops::test::MockGraph, NodeAddress) {
        let cmp_rows = if !reversed {
            Box::new(|a: &&Arc<Vec<DataType>>, b: &&Arc<Vec<DataType>>| a[2].cmp(&b[2])) as
            Box<OrderedRecordComparator>
        } else {
            Box::new(|a: &&Arc<Vec<DataType>>, b: &&Arc<Vec<DataType>>| b[2].cmp(&a[2])) as
            Box<OrderedRecordComparator>
        };

        let mut g = ops::test::MockGraph::new();
        let s = g.add_base("source", &["x", "y", "z"]);
        g.set_op("topk",
                 &["x", "y", "z"],
                 TopK::new(s, cmp_rows, vec![1], 3),
                 true);
        (g, s)
    }

    #[test]
    fn it_forwards() {
        let (mut g, s) = setup(false);

        let r12: Vec<DataType> = vec![1.into(), "z".into(), 12.into()];
        let r10: Vec<DataType> = vec![2.into(), "z".into(), 10.into()];
        let r11: Vec<DataType> = vec![3.into(), "z".into(), 11.into()];
        let r5: Vec<DataType> = vec![4.into(), "z".into(), 5.into()];
        let r15: Vec<DataType> = vec![5.into(), "z".into(), 15.into()];
        let r10b: Vec<DataType> = vec![6.into(), "z".into(), 10.into()];
        let r10c: Vec<DataType> = vec![7.into(), "z".into(), 10.into()];

        let a = g.narrow_one_row(r12.clone(), true);
        assert_eq!(a, vec![r12.clone()].into());

        let a = g.narrow_one_row(r10.clone(), true);
        assert_eq!(a, vec![r10.clone()].into());

        let a = g.narrow_one_row(r11.clone(), true);
        assert_eq!(a, vec![r11.clone()].into());

        let a = g.narrow_one_row(r5.clone(), true);
        assert_eq!(a.len(), 0);

        let a = g.narrow_one_row(r15.clone(), true);
        assert_eq!(a, vec![(r10.clone(), false), (r15.clone(), true)].into());

        g.seed(s, r12.clone());
        g.seed(s, r10.clone());
        g.seed(s, r11.clone());
        g.seed(s, r5.clone());
        let a = g.narrow_one_row((r15.clone(), false), true);
        assert_eq!(a, vec![(r15.clone(), false), (r10.clone(), true)].into());
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
        let (mut g, s) = setup(true);

        let r12: Vec<DataType> = vec![1.into(), "z".into(), (-12.123).into()];
        let r10: Vec<DataType> = vec![2.into(), "z".into(), (0.0431).into()];
        let r11: Vec<DataType> = vec![3.into(), "z".into(), (-0.082).into()];
        let r5: Vec<DataType> = vec![4.into(), "z".into(), (5.601).into()];
        let r15: Vec<DataType> = vec![5.into(), "z".into(), (-15.9).into()];
        let r10b: Vec<DataType> = vec![6.into(), "z".into(), (0.0431).into()];
        let r10c: Vec<DataType> = vec![7.into(), "z".into(), (0.0431).into()];

        let a = g.narrow_one_row(r12.clone(), true);
        assert_eq!(a, vec![r12.clone()].into());

        let a = g.narrow_one_row(r10.clone(), true);
        assert_eq!(a, vec![r10.clone()].into());

        let a = g.narrow_one_row(r11.clone(), true);
        assert_eq!(a, vec![r11.clone()].into());

        let a = g.narrow_one_row(r5.clone(), true);
        assert_eq!(a.len(), 0);

        let a = g.narrow_one_row(r15.clone(), true);
        assert_eq!(a, vec![(r10.clone(), false), (r15.clone(), true)].into());

        g.seed(s, r12.clone());
        g.seed(s, r10.clone());
        g.seed(s, r11.clone());
        g.seed(s, r5.clone());
        let a = g.narrow_one_row((r15.clone(), false), true);
        assert_eq!(a, vec![(r15.clone(), false), (r10.clone(), true)].into());
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
    fn it_suggests_indices() {
        let (g, _) = setup(false);
        let me = NodeAddress::mock_global(1.into());
        let idx = g.node().suggest_indexes(me);
        assert_eq!(idx.len(), 2);
        assert_eq!(*idx.iter()
                        .next()
                        .unwrap()
                        .1,
                   vec![1]);
        assert_eq!(*idx.iter()
                        .skip(1)
                        .next()
                        .unwrap()
                        .1,
                   vec![1]);
    }

    #[test]
    fn it_resolves() {
        let (g, _) = setup(false);
        assert_eq!(g.node().resolve(0), Some(vec![(g.narrow_base_id(), 0)]));
        assert_eq!(g.node().resolve(1), Some(vec![(g.narrow_base_id(), 1)]));
        assert_eq!(g.node().resolve(2), Some(vec![(g.narrow_base_id(), 2)]));
    }

    #[test]
    fn it_parent_columns() {
        let (g, _) = setup(false);
        assert_eq!(g.node().resolve(0), Some(vec![(g.narrow_base_id(), 0)]));
        assert_eq!(g.node().resolve(1), Some(vec![(g.narrow_base_id(), 1)]));
        assert_eq!(g.node().resolve(2), Some(vec![(g.narrow_base_id(), 2)]));
    }
}
