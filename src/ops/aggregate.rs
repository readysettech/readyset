use ops;
use flow;
use query;
use ops::base::NodeOp;

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
    op: Aggregation,
    over: usize,
    cols: usize,
}

impl NodeOp for Aggregator {
    fn forward(&self,
               u: ops::Update,
               _: flow::NodeIndex,
               db: Option<&shortcut::Store<query::DataType>>,
               _: &ops::base::AQ)
               -> Option<ops::Update> {

        // Construct the query we'll need
        let mut q = (0..self.cols)
            .filter(|&i| i != self.over)
            .map(|col| {
                shortcut::Condition {
                    column: col,
                    cmp: shortcut::Comparison::Equal(shortcut::Value::Const(query::DataType::None)),
                }
            })
            .collect::<Vec<_>>();

        match u {
            ops::Update::Records(rs) => {
                assert_eq!(rs.get(0).and_then(|c| Some(c.rec().len())).unwrap_or(0),
                           self.cols);

                // First, we want to be smart about multiple added/removed rows with same group.
                // For example, if we get a -, then a +, for the same group, we don't want to
                // execute two queries.
                let mut consolidate = HashMap::new();
                for rec in rs.into_iter() {
                    let (r, pos) = rec.extract();
                    let val = r[self.over].clone().into();
                    let group = r.into_iter()
                        .enumerate()
                        .filter(|&(i, _)| i != self.over)
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
                    rec.extend((0..self.cols).into_iter().filter_map(|i| group.remove(&i)));

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

    fn query(&self, q: Option<query::Query>, aqfs: &ops::base::AQ) -> ops::base::Datas {
        use std::sync::mpsc;
        use std::iter;

        assert_eq!(aqfs.len(), 1);

        // we need to figure out what parameters to pass to our source to get only the rows
        // relevant to our query.
        let mut params: Vec<shortcut::Value<query::DataType>> =
            iter::repeat(shortcut::Value::Const(query::DataType::None)).take(self.cols).collect();

        // we find all conditions that filter over a field present in the input (so everything
        // except conditions on self.over), and use those as parameters.
        if let Some(q) = q {
            for c in q.having.into_iter() {
                // FIXME: we could technically support querying over the output of the aggregation,
                // but a) it would be inefficient, and b) we'd have to restructure this function a
                // fair bit so that we keep that part of the query around for after we've got the
                // results back. We'd then need to do another filtering pass over the results of
                // query.
                let col = c.column;
                assert!(col != self.over);
                match c.cmp {
                    shortcut::Comparison::Equal(v) => {
                        *params.get_mut(c.column).unwrap() = v;
                    }
                }
            }
        }
        params.remove(self.over);

        // now, query our ancestor, and aggregate into groups.
        let (tx, rx) = mpsc::channel();
        let ptx = (*aqfs.iter().next().unwrap().1)(tx);
        ptx.send(params).unwrap();
        drop(ptx);

        // FIXME: having an order by would be nice here, so that we didn't have to keep the entire
        // aggregated state in memory until we've seen all rows.
        let mut consolidate = HashMap::new();
        for rec in rx.into_iter() {
            let (group, mut over): (_, Vec<_>) =
                rec.into_iter().enumerate().partition(|&(fi, _)| fi != self.over);
            assert_eq!(over.len(), 1);
            let group = group.into_iter().map(|(_, v)| v).collect();
            let over = over.pop().unwrap().1.into();

            let cur = consolidate.entry(group).or_insert(self.op.zero());
            *cur = self.op.update(*cur, over, true);
        }

        Box::new(consolidate.into_iter().map(|(mut group, over): (Vec<query::DataType>, i64)| {
            group.push(over.into());
            // TODO: respect q.select
            group
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use ops;
    use flow;
    use query;
    use shortcut;

    use ops::base::NodeOp;
    use std::sync::mpsc;
    use std::collections::HashMap;

    #[test]
    fn it_forwards() {
        let c = Aggregator {
            cols: 2,
            over: 1,
            op: Aggregation::COUNT,
        };

        let mut s = shortcut::Store::new(2);
        let src = flow::NodeIndex::new(0);

        let u = ops::Update::Records(vec![ops::Record::Positive(vec![1.into(), 1.into()])]);

        // first row for a group should emit -0 and +1 for that group
        let out = c.forward(u, src, Some(&s), &HashMap::new());
        if let Some(ops::Update::Records(rs)) = out {
            assert_eq!(rs.len(), 2);
            let mut rs = rs.into_iter();

            match rs.next().unwrap() {
                ops::Record::Negative(r) => {
                    assert_eq!(r[0], 1.into());
                    assert_eq!(r[1], 0.into());
                }
                _ => unreachable!(),
            }
            match rs.next().unwrap() {
                ops::Record::Positive(r) => {
                    assert_eq!(r[0], 1.into());
                    assert_eq!(r[1], 1.into());
                    s.insert(r);
                }
                _ => unreachable!(),
            }
        } else {
            unreachable!();
        }

        let u = ops::Update::Records(vec![ops::Record::Positive(vec![2.into(), 2.into()])]);

        // first row for a second group should emit -0 and +1 for that new group
        let out = c.forward(u, src, Some(&s), &HashMap::new());
        if let Some(ops::Update::Records(rs)) = out {
            assert_eq!(rs.len(), 2);
            let mut rs = rs.into_iter();

            match rs.next().unwrap() {
                ops::Record::Negative(r) => {
                    assert_eq!(r[0], 2.into());
                    assert_eq!(r[1], 0.into());
                }
                _ => unreachable!(),
            }
            match rs.next().unwrap() {
                ops::Record::Positive(r) => {
                    assert_eq!(r[0], 2.into());
                    assert_eq!(r[1], 1.into());
                    s.insert(r);
                }
                _ => unreachable!(),
            }
        } else {
            unreachable!();
        }

        let u = ops::Update::Records(vec![ops::Record::Positive(vec![1.into(), 2.into()])]);

        // second row for a group should emit -1 and +2
        let out = c.forward(u, src, Some(&s), &HashMap::new());
        if let Some(ops::Update::Records(rs)) = out {
            assert_eq!(rs.len(), 2);
            let mut rs = rs.into_iter();

            match rs.next().unwrap() {
                ops::Record::Negative(r) => {
                    assert_eq!(r[0], 1.into());
                    assert_eq!(r[1], 1.into());
                }
                _ => unreachable!(),
            }
            match rs.next().unwrap() {
                ops::Record::Positive(r) => {
                    assert_eq!(r[0], 1.into());
                    assert_eq!(r[1], 2.into());
                }
                _ => unreachable!(),
            }
        } else {
            unreachable!();
        }

        let u = ops::Update::Records(vec![ops::Record::Negative(vec![1.into(), 1.into()])]);

        // negative row for a group should emit -1 and +0
        let out = c.forward(u, src, Some(&s), &HashMap::new());
        if let Some(ops::Update::Records(rs)) = out {
            assert_eq!(rs.len(), 2);
            let mut rs = rs.into_iter();

            match rs.next().unwrap() {
                ops::Record::Negative(r) => {
                    assert_eq!(r[0], 1.into());
                    assert_eq!(r[1], 1.into());
                }
                _ => unreachable!(),
            }
            match rs.next().unwrap() {
                ops::Record::Positive(r) => {
                    assert_eq!(r[0], 1.into());
                    assert_eq!(r[1], 0.into());
                }
                _ => unreachable!(),
            }
        } else {
            unreachable!();
        }

        let u = ops::Update::Records(vec![
             ops::Record::Negative(vec![1.into(), 1.into()]),
             ops::Record::Positive(vec![1.into(), 1.into()]),
             ops::Record::Positive(vec![1.into(), 2.into()]),
             ops::Record::Negative(vec![2.into(), 2.into()]),
             ops::Record::Positive(vec![2.into(), 2.into()]),
             ops::Record::Positive(vec![2.into(), 3.into()]),
             ops::Record::Positive(vec![2.into(), 1.into()]),
             ops::Record::Positive(vec![3.into(), 3.into()]),
        ]);

        // multiple positives and negatives should update aggregation value by appropriate amount
        let out = c.forward(u, src, Some(&s), &HashMap::new());
        if let Some(ops::Update::Records(rs)) = out {
            assert_eq!(rs.len(), 6); // one - and one + for each group
            // group 1 lost 1 and gained 2
            assert!(rs.iter().any(|r| {
                if let ops::Record::Negative(ref r) = *r {
                    r[0] == 1.into() && r[1] == 1.into()
                } else {
                    false
                }
            }));
            assert!(rs.iter().any(|r| {
                if let ops::Record::Positive(ref r) = *r {
                    r[0] == 1.into() && r[1] == 2.into()
                } else {
                    false
                }
            }));
            // group 2 lost 1 and gained 3
            assert!(rs.iter().any(|r| {
                if let ops::Record::Negative(ref r) = *r {
                    r[0] == 2.into() && r[1] == 1.into()
                } else {
                    false
                }
            }));
            assert!(rs.iter().any(|r| {
                if let ops::Record::Positive(ref r) = *r {
                    r[0] == 2.into() && r[1] == 3.into()
                } else {
                    false
                }
            }));
            // group 3 lost 1 (well, 0) and gained 1
            assert!(rs.iter().any(|r| {
                if let ops::Record::Negative(ref r) = *r {
                    r[0] == 3.into() && r[1] == 0.into()
                } else {
                    false
                }
            }));
            assert!(rs.iter().any(|r| {
                if let ops::Record::Positive(ref r) = *r {
                    r[0] == 3.into() && r[1] == 1.into()
                } else {
                    false
                }
            }));
        } else {
            unreachable!();
        }
    }

    // TODO: also test SUM

    fn source(tx: mpsc::Sender<Vec<query::DataType>>) -> mpsc::Sender<ops::base::Params> {
        use std::thread;

        let (ptx, prx): (_, mpsc::Receiver<Vec<_>>) = mpsc::channel();
        let data = vec![
                vec![1.into(), 1.into()],
                vec![2.into(), 1.into()],
                vec![2.into(), 2.into()],
            ];
        let mut q = query::Query {
            select: vec![true, true],
            having: vec![shortcut::Condition{
                    column: 0,
                    cmp: shortcut::Comparison::Equal(shortcut::Value::Const(query::DataType::None))
                }],
        };

        thread::spawn(move || {
            for p in prx {
                assert_eq!(p.len(), 1);
                let p = p.into_iter().last().unwrap();
                q.having.get_mut(0).unwrap().cmp = shortcut::Comparison::Equal(p);

                for r in data.iter() {
                    if let Some(r) = q.feed(r) {
                        tx.send(r).unwrap();
                    }
                }
            }
        });
        ptx
    }

    #[test]
    fn it_queries() {
        let c = Aggregator {
            cols: 2,
            over: 1,
            op: Aggregation::COUNT,
        };

        let mut aqfs = HashMap::new();
        aqfs.insert(0.into(), Box::new(source) as Box<_>);

        let hits = c.query(None, &aqfs).collect::<Vec<_>>();
        assert_eq!(hits.len(), 2);
        assert!(hits.iter().any(|r| r[0] == 1.into() && r[1] == 1.into()));
        assert!(hits.iter().any(|r| r[0] == 2.into() && r[1] == 2.into()));

        let q = query::Query {
            select: vec![true, true],
            having: vec![shortcut::Condition {
                             column: 0,
                             cmp: shortcut::Comparison::Equal(shortcut::Value::Const(2.into())),
                         }],
        };

        let hits = c.query(Some(q), &aqfs).collect::<Vec<_>>();
        assert_eq!(hits.len(), 1);
        assert!(hits.iter().any(|r| r[0] == 2.into() && r[1] == 2.into()));
    }
}
