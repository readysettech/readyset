
use flow::data::DataType;

use std::mem;
use std::collections::HashMap;
use std::sync::Arc;

use flow::prelude::*;

#[derive(Debug, Clone)]
pub struct TopK {
    src: NodeAddress,

    // some cache state
    us: Option<NodeAddress>,
    cols: usize,

    // precomputed datastructures
    group_by: Vec<usize>,

    // column to use for ordering
    over: usize,
    k: usize,

    counts: HashMap<Vec<DataType>, usize>,
}

impl TopK {
    pub fn new(src: NodeAddress, over: usize, group_by: Vec<usize>, k: usize) -> Self {
        let mut group_by = group_by;
        group_by.sort();

        TopK {
            src: src,

            us: None,
            cols: 0,

            group_by: group_by,

            over: over,
            k: k,

            counts: HashMap::new(),
        }
    }

    /// Returns the set of Record structs to be emitted by this node, for some group. In steady
    /// state operation this will typically include some number of positives (at most k), and the same number
    /// of negatives.
    fn apply(&self, new_count: usize, current_topk: &[Arc<Vec<DataType>>], new: Vec<Record>) -> Vec<Record> {
        if new_count <= self.k {
            return new
        }

        let cmp_rows = |a: &Arc<Vec<DataType>>, b: &Arc<Vec<DataType>>|{
            a[self.over].cmp(&b[self.over])
        };

        let mut delta = Vec::new();
        let mut current:Vec<_> = current_topk.iter().cloned().collect();
        current.sort_by(&cmp_rows);
        for r in new.iter() {
            if let &Record::Negative(ref a) = r {
                let idx = current.binary_search_by_key(&&a[self.over], |arc|&arc[self.over]);
                if let Ok(idx) = idx {
                    current.remove(idx);
                    delta.push(r.clone())
                }
            }
        }

        let mut output_rows:Vec<_> = current.into_iter().map(|a|(a,true)).collect();
        output_rows.extend(new.into_iter().filter_map(|r|{match r {
            Record::Positive(a) => Some((a,false)),
            _ => None,
        }}));
        output_rows.sort_by(|a,b| cmp_rows(&a.0, &b.0));

        if output_rows.len() > self.k {
            // Remove the topk elements from `output_rows`, splitting them off into `rows`. Then
            // swap and rename so that `output_rows` contains the top K elements, and `bottom_rows`
            // contains the rest.
            let i = output_rows.len() - self.k;
            let mut rows = output_rows.split_off(i);
            mem::swap(&mut output_rows, &mut rows);
            let bottom_rows = rows;

            // Emit negatives for any elements in `bottom_rows` that were originally in current_topk.
            delta.extend(bottom_rows.into_iter().filter_map(|p| {
                if p.1 {
                    Some(Record::Negative(p.0))
                } else {
                    None
                }
            }));
        } else if output_rows.len() < self.k && new_count >= self.k {
            // TODO: handle this case by querying into the parent.
            unimplemented!();
        }

        // Emit positives for any elements in `output_rows` that weren't originally in current_topk.
        delta.extend(output_rows.into_iter().filter_map(|p| {
            if !p.1 {
                Some(Record::Positive(p.0))
            } else {
                None
            }
        }));

        delta
    }
}

impl Ingredient for TopK {
    fn take(&mut self) -> Box<Ingredient> {
        Box::new(Clone::clone(self))
    }

    fn ancestors(&self) -> Vec<NodeAddress> {
        vec![self.src]
    }

    fn should_materialize(&self) -> bool {
        true
    }

    fn will_query(&self, materialized: bool) -> bool {
        !materialized
    }

    fn on_connected(&mut self, g: &Graph) {
        let srcn = &g[*self.src.as_global()];
        assert!(self.over < srcn.fields().len(),
                "cannot compute top K over non-existing column");

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
                -> Records {
        debug_assert_eq!(from, self.src);

        if rs.is_empty() {
            return rs;
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

        let mut out = Vec::new();
        for (group, diffs) in consolidate {
            // find the current value for this group
            let db = state.get(self.us.as_ref().unwrap().as_local())
                .expect("grouped operators must have their own state materialized");

            let old_rs = db.lookup(&self.group_by[..], &KeyType::from(&group[..]));

            // Retrieve then update the number of times in this group
            let count = *self.counts.get(&group).unwrap_or(&0);
            let count_diff:i64 = diffs.iter().map(|r|match r {
                &Record::Positive(..) => 1,
                &Record::Negative(..) => -1,
                &Record::DeleteRequest(..) => unreachable!(),
            }).sum();
            self.counts.insert(group, ((count as i64) + count_diff) as usize);
            assert!(count >= old_rs.len());

            out.append(&mut self.apply(count + count_diff as usize, old_rs, diffs));
        }

        out.into()
    }

    fn suggest_indexes(&self, this: NodeAddress) -> HashMap<NodeAddress, Vec<usize>> {
        Some((this, self.group_by.clone())).into_iter().collect()
    }

    fn resolve(&self, col: usize) -> Option<Vec<(NodeAddress, usize)>> {
        Some(vec![(self.src, col)])
    }

    fn description(&self) -> String {
        format!("TopK({})", self.over)
    }

    fn parent_columns(&self, col: usize) -> Vec<(NodeAddress, Option<usize>)> {
        vec![(self.src, Some(col))]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use ops;

    fn setup() -> ops::test::MockGraph {
        let mut g = ops::test::MockGraph::new();
        let s = g.add_base("source", &["x", "y", "z"]);
        g.set_op("topk", &["x", "y", "z"], TopK::new(s, 2, vec![1], 3), true);
        g
    }

    #[test]
    fn it_forwards() {
        let mut g = setup();

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

        let a = g.narrow_one_row(r5.clone(), true);
        assert_eq!(a.len(), 0);

        let a = g.narrow_one_row(r15.clone(), true);
        assert_eq!(a, vec![(r10, false), (r15, true)].into());
    }

    #[test]
    fn it_suggests_indices() {
        let g = setup();
        let me = NodeAddress::mock_global(1.into());
        let idx = g.node().suggest_indexes(me);
        assert_eq!(idx.len(), 1);
        assert_eq!(*idx.iter().next().unwrap().1, vec![1])
    }

    #[test]
    fn it_resolves() {
        let g = setup();
        assert_eq!(g.node().resolve(0), Some(vec![(g.narrow_base_id(), 0)]));
        assert_eq!(g.node().resolve(1), Some(vec![(g.narrow_base_id(), 1)]));
        assert_eq!(g.node().resolve(2), Some(vec![(g.narrow_base_id(), 2)]));
    }

    #[test]
    fn it_parent_columns() {
        let g = setup();
        assert_eq!(g.node().resolve(0), Some(vec![(g.narrow_base_id(), 0)]));
        assert_eq!(g.node().resolve(1), Some(vec![(g.narrow_base_id(), 1)]));
        assert_eq!(g.node().resolve(2), Some(vec![(g.narrow_base_id(), 2)]));
    }
}
