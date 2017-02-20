use ops;
use query;

use std::fmt;
use std::collections::HashSet;
use std::collections::HashMap;
use std::sync;
use std::sync::Arc;

use flow::prelude::*;

#[derive(Debug, Clone)]
pub struct TopK {
    src: NodeAddress,

    // some cache state
    us: Option<NodeAddress>,
    cols: usize,

    pkey_in: usize, // column in our input that is our primary key
    pkey_out: usize, // column in our output that is our primary key

    // precomputed datastructures
    group: HashSet<usize>,
    colfix: Vec<usize>,

    // column to use for ordering
    over: usize,
    k: usize,

    counts: HashMap<Vec<query::DataType>, usize>,
}

impl TopK {
    pub fn new(src: NodeAddress, over: usize, group: Vec<usize>, k: usize) -> Self {
        TopK {
            src: src,

            pkey_out: usize::max_value(),
            pkey_in: usize::max_value(),

            us: None,
            cols: 0,
            group: group.into_iter().collect(),
            colfix: Vec::new(),

            over: over,
            k: k,

            counts: HashMap::new(),
        }
    }

    /// Returns the set of Record structs to be emitted by this node, for some group. In steady
    /// state operation this will include some number of positives (at most k), and the same number
    /// of negatives.
    fn apply(&self, count: usize, current_topk: &[Arc<Vec<query::DataType>>], new: Vec<Record>) -> Vec<Record> {
        let mut extreme = current_topk.iter().map(|r|{(r, true)});

        // // Extreme values are those that are at least as extreme as the current min/max (if any).
        // // let mut is_extreme_value : Box<Fn(i64) -> bool> = Box::new(|_|true);
        // let mut extreme_values: Vec<i64> = vec![];
        // if let Some(&query::DataType::Number(n)) = current {
        //     extreme_values.push(n);
        // };

        // let is_extreme_value = |x: i64| if let Some(&query::DataType::Number(n)) = current {
        //     match self.op {
        //         Extremum::MAX => x >= n,
        //         Extremum::MIN => x <= n,
        //     }
        // } else {
        //     true
        // };

        // for d in diffs {
        //     match d {
        //         DiffType::Insert(v) if is_extreme_value(v) => extreme_values.push(v),
        //         DiffType::Remove(v) if is_extreme_value(v) => {
        //             if let Some(i) = extreme_values.iter().position(|x: &i64| *x == v) {
        //                 extreme_values.swap_remove(i);
        //             }
        //         }
        //         _ => {}
        //     };
        // }

        // let extreme = match self.op {
        //     Extremum::MIN => extreme_values.into_iter().min(),
        //     Extremum::MAX => extreme_values.into_iter().max(),
        // };

        // if let Some(extreme) = extreme {
        //     return extreme.into();
        // }

        // TODO: handle this case by querying into the parent.
        unimplemented!();
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
                "cannot aggregate over non-existing column");

        // group by all columns
        self.cols = srcn.fields().len();
        if self.group.len() != 1 {
            unimplemented!();
        }
        // primary key is the first (and only) group by key
        self.pkey_in = *self.group.iter().next().unwrap();
        // what output column does this correspond to?
        // well, the first one given that we currently only have one group by
        // and that group by comes first.
        self.pkey_out = 0;

        // build a translation mechanism for going from output columns to input columns
        let colfix: Vec<_> = (0..self.cols)
            .into_iter()
            .filter_map(|col| {
                if self.group.contains(&col) {
                    // since the generated value goes at the end,
                    // this is the n'th output value
                    Some(col)
                } else {
                    // this column does not appear in output
                    None
                }
            })
            .collect();
        self.colfix.extend(colfix.into_iter());
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
                .filter_map(|(i, v)| if self.group.contains(&i) {
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
            let old_rs = db.lookup(self.pkey_out, &group[self.pkey_in]);

            // Retrieve then update the number of times in this group
            let count = *self.counts.get(&group).unwrap_or(&0);
            let count_diff:i64 = diffs.iter().map(|r|match r {
                &Record::Positive(..) => 1,
                &Record::Negative(..) => -1,
                &Record::DeleteRequest(..) => unreachable!(),
            }).sum();
            self.counts.insert(group, ((count as i64) + count_diff) as usize);
            assert!(count >= old_rs.len());

            out.append(&mut self.apply(count, old_rs, diffs));
            // match current {
            //     None => {
            //         // emit positive, which is group + new.
            //         let rec: Vec<_> = group.into_iter()
            //             .filter_map(|v| v)
            //             .cloned()
            //             .chain(Some(new.into()).into_iter())
            //             .collect();
            //         out.push(ops::Record::Positive(sync::Arc::new(rec)));
            //     }
            //     Some(ref current) if new == **current => {
            //         // no change
            //     }
            //     Some(current) => {
            //         // construct prefix of output record used for both - and +
            //         let mut rec = Vec::with_capacity(group.len() + 1);
            //         rec.extend(group.into_iter().filter_map(|v| v).cloned());

            //         // revoke old value
            //         if old.is_none() {
            //             // we're generating a zero row
            //             // revoke old value
            //             rec.push(current.into_owned());
            //             out.push(ops::Record::Negative(sync::Arc::new(rec.clone())));

            //             // remove the old value from the end of the record
            //             rec.pop();
            //         } else {
            //             out.push(ops::Record::Negative(old.unwrap().clone()));
            //         }

            //         // emit new value
            //         rec.push(new.into());
            //         out.push(ops::Record::Positive(sync::Arc::new(rec)));
            //     }
            // }
        }

        out.into()
    }

    fn suggest_indexes(&self, this: NodeAddress) -> HashMap<NodeAddress, usize> {
        // index by our primary key
        Some((this, self.pkey_out)).into_iter().collect()
    }

    fn resolve(&self, col: usize) -> Option<Vec<(NodeAddress, usize)>> {
        if col == self.cols - 1 {
            return None;
        }
        Some(vec![(self.src, self.colfix[col])])
    }

    fn description(&self) -> String {
        format!("TopK({})", self.over)
    }

    fn parent_columns(&self, column: usize) -> Vec<(NodeAddress, Option<usize>)> {
        if column == self.cols - 1 {
            return vec![(self.src, None)];
        }
        vec![(self.src, Some(self.colfix[column]))]
    }
}
