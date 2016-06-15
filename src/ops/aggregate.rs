use ops;
use query;
use ops::base::NodeOp;

use std::collections::HashSet;
use std::collections::HashMap;

use shortcut;

pub enum Aggregation {
    COUNT,
    SUM,
}

impl Aggregation {
    pub fn zero(&self) -> i64 {
        match *self {
            Aggregation::COUNT => 0,
            Aggregation::SUM => 0,
        }
    }

    pub fn update(&self, old: i64, delta: i64, positive: bool) -> i64 {
        match *self {
            Aggregation::COUNT if positive => old + 1,
            Aggregation::COUNT => old - 1,
            Aggregation::SUM if positive => old + delta,
            Aggregation::SUM => old - delta,
        }
    }
}

pub struct Aggregator {
    group: HashSet<usize>,
    op: Aggregation,
    over: usize,
}


impl NodeOp for Aggregator {
    fn forward(&self,
               u: ops::Update,
               db: Option<&shortcut::Store<query::DataType>>,
               _: &ops::base::AQ)
               -> Option<ops::Update> {

        // Construct the query we'll need
        let mut q = self.group
            .iter()
            .map(|col| {
                shortcut::Condition {
                    column: *col,
                    cmp: shortcut::Comparison::Equal(shortcut::Value::Const(query::DataType::None)),
                }
            })
            .collect::<Vec<_>>();

        match u {
            ops::Update::Records(rs) => {
                let cols = rs.get(0).and_then(|c| Some(c.rec().len())).unwrap_or(0);

                // First, we want to be smart about multiple added/removed rows with same group.
                // For example, if we get a -, then a +, for the same group, we don't want to
                // execute two queries.
                let mut consolidate = HashMap::new();
                for rec in rs.into_iter() {
                    let (r, pos) = rec.extract();
                    let val = r[self.over].clone().into();
                    let group = r.into_iter()
                        .enumerate()
                        .filter(|&(i, _)| self.group.contains(&i))
                        .collect::<Vec<_>>();

                    consolidate.entry(group).or_insert_with(Vec::new).push((val, pos));
                }

                let mut out = Vec::with_capacity(2 * consolidate.len());
                for (group, diffs) in consolidate.into_iter() {
                    let mut group = group.into_iter().collect::<HashMap<_, _>>();

                    // build a query for this group
                    for s in q.iter_mut() {
                        s.cmp =
                          shortcut::Comparison::Equal(
                            shortcut::Value::Const(
                              group
                                .remove(&s.column)
                                .expect("group by column is beyond number of columns in record")
                            )
                          );
                    }

                    // find the current value for this group
                    let current = match db {
                        Some(db) => {
                            let mut matches = db.find(&q[..]);
                            let current = matches.next();
                            assert!(current.is_none() || matches.count() == 0,
                                    "aggregation had more than 1 result");
                            current.and_then(|r| Some(r[self.over].clone().into()))
                                .unwrap_or(self.op.zero())
                        }
                        None => {
                            // TODO
                            // query ancestor (self.query?) based on self.group columns
                            // aggregate using self.op
                            unimplemented!()
                        }
                    };

                    // get back values from query (to avoid cloning)
                    for s in q.iter_mut() {
                        if let shortcut::Comparison::Equal(shortcut::Value::Const(ref mut v)) =
                               s.cmp {
                            use std::mem;

                            let mut x = query::DataType::None;
                            mem::swap(&mut x, v);
                            group.insert(s.column, x);
                        }
                    }

                    // construct prefix of output record
                    let mut rec = Vec::with_capacity(group.len() + 1);
                    rec.extend((0..cols).into_iter().filter_map(|i| group.remove(&i)));

                    // revoke old value
                    rec.push(current.into());
                    out.push(ops::Record::Negative(rec.clone()));

                    // update value using self.op
                    let new = diffs.into_iter()
                        .fold(current,
                              |current, (diff, is_pos)| self.op.update(current, diff, is_pos));

                    // emit new value
                    rec.pop();
                    rec.push(new.into());
                    out.push(ops::Record::Positive(rec));
                }

                Some(ops::Update::Records(out))
            }
        }
    }

    fn query(&self, _: Option<query::Query>, _: &ops::base::AQ) -> ops::base::Datas {
        // TODO
        // we have to implement this in order to support the addition of later, materialized
        // aggregations, as well as to support non-materialized aggregations. For now though:
        unimplemented!();
    }
}
