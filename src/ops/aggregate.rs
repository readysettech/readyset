use ops;
use flow;
use query;
use backlog;
use ops::NodeOp;
use ops::NodeType;

use std::collections::HashMap;

use shortcut;

/// Supported aggregation operators.
#[derive(Debug)]
pub enum Aggregation {
    /// Count the number of records for each group. The value for the `over` column is ignored.
    COUNT,
    /// Sum the value of the `over` column for all records of each group.
    SUM,
}

impl Aggregation {
    /// Zero value for this aggregation.
    pub fn zero(&self) -> i64 {
        match *self {
            Aggregation::COUNT => 0,
            Aggregation::SUM => 0,
        }
    }

    /// Procedure for computing the new value for this aggregation given the current value and a
    /// positive or negative delta.
    pub fn update(&self, old: i64, delta: i64, positive: bool) -> i64 {
        match *self {
            Aggregation::COUNT if positive => old + 1,
            Aggregation::COUNT => old - 1,
            Aggregation::SUM if positive => old + delta,
            Aggregation::SUM => old - delta,
        }
    }

    /// Construct a new `Aggregator` that performs this operation.
    ///
    /// The aggregation will be aggregate the value in column number `over` from its inputs (i.e.,
    /// from the `src` node in the graph), and use all other received columns as the group
    /// identifier. `cols` should be set to the number of columns in this view (that is, the number
    /// of group identifier columns + 1).
    pub fn new(self, src: flow::NodeIndex, over: usize, cols: usize) -> Aggregator {
        Aggregator {
            op: self,
            src: src,
            over: over,
            cols: cols,
        }
    }
}

/// Aggregator implementas a Soup node that performans common aggregation operations such as counts
/// and sums.
///
/// `Aggregator` nodes are constructed through `Aggregation` variants using `Aggregation::new`.
///
/// Logically, the aggregated value for all groups start out as `self.op.zero()`. Thus, when the
/// first record is received for a group, `Aggregator` will output a negative for the *zero row*,
/// followed by a positive for the newly aggregated value.
///
/// When a new record arrives, the aggregator will first query the currently aggregated value for
/// the new record's group by doing a query into its own output. The aggregated column
/// (`self.over`) of the incoming record is then combined with the current aggregation value using
/// `self.op.update`. The output record is constructed by concatenating the columns identifying the
/// group, and appending the aggregated value. For example, for a sum with `self.over == 1`, a
/// previous sum of `3`, and an incoming record with `[a, 1, x]`, the output would be `[a, x, 4]`.
///
/// Note that the code below also tries to be somewhat clever when given multiple records. Rather
/// than doing one lookup for every record, it will find all *groups*, query once for each group,
/// apply all the per-group deltas, and then emit one record for every group (well, a negative and
/// a positive). This increases the complexity of the code, but also saves a lot of work when
/// downstream of a join that may produce many records with the same group.
#[derive(Debug)]
pub struct Aggregator {
    op: Aggregation,
    src: flow::NodeIndex,
    over: usize,
    cols: usize,
}

impl From<Aggregator> for NodeType {
    fn from(b: Aggregator) -> NodeType {
        NodeType::AggregateNode(b)
    }
}

impl NodeOp for Aggregator {
    fn forward(&self,
               u: ops::Update,
               src: flow::NodeIndex,
               _: i64,
               db: Option<&backlog::BufferedStore>,
               _: &ops::AQ)
               -> Option<ops::Update> {

        assert_eq!(src, self.src);

        // Construct the query we'll need to query into ourselves
        let mut q = (0..self.cols)
            .filter(|&i| i != self.cols - 1)
            .map(|col| {
                shortcut::Condition {
                    column: col,
                    cmp: shortcut::Comparison::Equal(shortcut::Value::Const(query::DataType::None)),
                }
            })
            .collect::<Vec<_>>();

        match u {
            ops::Update::Records(rs) => {
                if rs.is_empty() {
                    return None;
                }

                assert_eq!(rs.get(0).and_then(|c| Some(c.rec().len())).unwrap_or(0),
                           self.cols);

                // First, we want to be smart about multiple added/removed rows with same group.
                // For example, if we get a -, then a +, for the same group, we don't want to
                // execute two queries.
                let mut consolidate = HashMap::new();
                for rec in rs.into_iter() {
                    let (r, pos, ts) = rec.extract();
                    let val = r[self.over].clone().into();
                    let group = r.into_iter()
                        .enumerate()
                        .filter(|&(i, _)| i != self.over)
                        .collect::<Vec<_>>();

                    consolidate.entry(group).or_insert_with(Vec::new).push((val, pos, ts));
                }

                let mut out = Vec::with_capacity(2 * consolidate.len());
                for (group, diffs) in consolidate.into_iter() {
                    let mut group = group.into_iter().collect::<HashMap<_, _>>();

                    // build a query for this group
                    for s in q.iter_mut() {
                        // s.column is the *output* column
                        // the *input* column must be computed
                        let mut col = s.column;
                        if col >= self.over {
                            col += 1;
                        }
                        s.cmp =
                            shortcut::Comparison::Equal(shortcut::Value::Const(group.remove(&col)
                                .expect("group by column is beyond number of columns in record")));
                    }

                    // find the current value for this group
                    let (current, old_ts) = match db {
                        Some(db) => {
                            db.find_and(&q[..], Some(i64::max_value()), |rs| {
                                assert!(rs.len() <= 1, "aggregation had more than 1 result");
                                rs.into_iter()
                                    .next()
                                    .and_then(|(r, ts)| Some((r[r.len() - 1].clone().into(), ts)))
                                    .unwrap_or((self.op.zero(), 0))
                            })
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
                    out.push(ops::Record::Negative(rec.clone(), old_ts));

                    // update value using self.op
                    let new_ts = diffs.iter().map(|&(_, _, ts)| ts).max().unwrap();
                    let new = diffs.into_iter()
                        .fold(current,
                              |current, (diff, is_pos, _)| self.op.update(current, diff, is_pos));

                    // emit new value
                    rec.pop();
                    rec.push(new.into());
                    out.push(ops::Record::Positive(rec, new_ts));
                }

                Some(ops::Update::Records(out))
            }
        }
    }

    fn query(&self, q: Option<&query::Query>, ts: i64, aqfs: &ops::AQ) -> ops::Datas {
        use std::iter;

        assert_eq!(aqfs.len(), 1);

        // we need to figure out what parameters to pass to our source to get only the rows
        // relevant to our query.
        let mut params: Vec<shortcut::Value<query::DataType>> =
            iter::repeat(shortcut::Value::Const(query::DataType::None)).take(self.cols).collect();

        // we find all conditions that filter over a field present in the input (so everything
        // except conditions on self.over), and use those as parameters.
        if let Some(q) = q {
            for c in q.having.iter() {
                // FIXME: we could technically support querying over the output of the aggregation,
                // but a) it would be inefficient, and b) we'd have to restructure this function a
                // fair bit so that we keep that part of the query around for after we've got the
                // results back. We'd then need to do another filtering pass over the results of
                // query.
                let mut col = c.column;
                assert!(col != self.cols - 1,
                        "filtering on aggregation output is not supported");

                // the order of output columns is the same as the order of the input columns
                // *except* that self.over is removed, and the aggregation result is placed last.
                // so, to figure out which column this is filtering on in our ancestor, we have to
                // do a little bit of math.
                if col >= self.over {
                    col += 1;
                }

                match c.cmp {
                    shortcut::Comparison::Equal(ref v) => {
                        *params.get_mut(col).unwrap() = v.clone();
                    }
                }
            }
        }
        params.remove(self.over);

        // now, query our ancestor, and aggregate into groups.
        let rx = (*aqfs.iter().next().unwrap().1)(params, ts);

        // FIXME: having an order by would be nice here, so that we didn't have to keep the entire
        // aggregated state in memory until we've seen all rows.
        let mut consolidate = HashMap::new();
        for (rec, ts) in rx.into_iter() {
            use std::cmp;

            let (group, mut over): (_, Vec<_>) =
                rec.into_iter().enumerate().partition(|&(fi, _)| fi != self.over);
            assert_eq!(over.len(), 1);
            let group = group.into_iter().map(|(_, v)| v).collect();
            let over = over.pop().unwrap().1.into();

            let cur = consolidate.entry(group).or_insert((self.op.zero(), ts));
            cur.0 = self.op.update(cur.0, over, true);
            cur.1 = cmp::max(ts, cur.1);
        }

        if consolidate.is_empty() {
            if let Some(q) = q {
                let mut group: Vec<_> = iter::repeat(query::DataType::None)
                    .take(self.cols - 1)
                    .collect();

                for c in q.having.iter() {
                    if c.column == self.cols - 1 {
                        continue;
                    }

                    if let shortcut::Comparison::Equal(shortcut::Value::Const(ref v)) = c.cmp {
                        *group.get_mut(c.column).unwrap() = v.clone();
                    } else {
                        continue;
                    }
                }

                if group.iter().all(|g| !g.is_none()) {
                    // we didn't match any groups, but all the group-by parameters are given.
                    // we can add a zero row!
                    consolidate.insert(group, (self.op.zero(), 0));
                }
            }
        }

        consolidate.into_iter()
            .map(|(mut group, (over, ts)): (Vec<query::DataType>, (i64, i64))| {
                group.push(over.into());
                // TODO: respect q.select
                (group, ts)
            })
            .collect()
    }

    fn suggest_indexes(&self, this: flow::NodeIndex) -> HashMap<flow::NodeIndex, Vec<usize>> {
        // index all group by columns
        Some((this, (0..self.cols).into_iter().filter(|&i| i != self.cols - 1).collect()))
            .into_iter()
            .collect()
    }

    fn resolve(&self, mut col: usize) -> Vec<(flow::NodeIndex, usize)> {
        if col == self.cols - 1 {
            return vec![];
        }
        if col >= self.over {
            col += 1
        }
        vec![(self.src, col)]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use ops;
    use flow;
    use query;
    use backlog;
    use shortcut;

    use ops::NodeOp;
    use std::collections::HashMap;

    #[test]
    fn it_forwards() {
        let c = Aggregation::COUNT.new(0.into(), 1, 2);

        let mut s = backlog::BufferedStore::new(2);
        let src = flow::NodeIndex::new(0);

        let u = (vec![1.into(), 1.into()], 1).into();

        // first row for a group should emit -0 and +1 for that group
        let out = c.forward(u, src, 1, Some(&s), &HashMap::new());
        if let Some(ops::Update::Records(rs)) = out {
            assert_eq!(rs.len(), 2);
            let mut rs = rs.into_iter();

            match rs.next().unwrap() {
                ops::Record::Negative(r, ts) => {
                    assert_eq!(r[0], 1.into());
                    assert_eq!(r[1], 0.into());
                    assert_eq!(ts, 0);
                }
                _ => unreachable!(),
            }
            match rs.next().unwrap() {
                ops::Record::Positive(r, ts) => {
                    assert_eq!(r[0], 1.into());
                    assert_eq!(r[1], 1.into());
                    assert_eq!(ts, 1);
                    s.add(vec![ops::Record::Positive(r, ts)], 1);
                    s.absorb(1);
                }
                _ => unreachable!(),
            }
        } else {
            unreachable!();
        }

        let u = (vec![2.into(), 2.into()], 2).into();

        // first row for a second group should emit -0 and +1 for that new group
        let out = c.forward(u, src, 2, Some(&s), &HashMap::new());
        if let Some(ops::Update::Records(rs)) = out {
            assert_eq!(rs.len(), 2);
            let mut rs = rs.into_iter();

            match rs.next().unwrap() {
                ops::Record::Negative(r, ts) => {
                    assert_eq!(r[0], 2.into());
                    assert_eq!(r[1], 0.into());
                    assert_eq!(ts, 0);
                }
                _ => unreachable!(),
            }
            match rs.next().unwrap() {
                ops::Record::Positive(r, ts) => {
                    assert_eq!(r[0], 2.into());
                    assert_eq!(r[1], 1.into());
                    assert_eq!(ts, 2);
                    s.add(vec![ops::Record::Positive(r, ts)], 2);
                    s.absorb(2);
                }
                _ => unreachable!(),
            }
        } else {
            unreachable!();
        }

        let u = (vec![1.into(), 2.into()], 3).into();

        // second row for a group should emit -1 and +2
        let out = c.forward(u, src, 3, Some(&s), &HashMap::new());
        if let Some(ops::Update::Records(rs)) = out {
            assert_eq!(rs.len(), 2);
            let mut rs = rs.into_iter();

            match rs.next().unwrap() {
                ops::Record::Negative(r, ts) => {
                    assert_eq!(r[0], 1.into());
                    assert_eq!(r[1], 1.into());
                    assert_eq!(ts, 1);
                }
                _ => unreachable!(),
            }
            match rs.next().unwrap() {
                ops::Record::Positive(r, ts) => {
                    assert_eq!(r[0], 1.into());
                    assert_eq!(r[1], 2.into());
                    assert_eq!(ts, 3);
                }
                _ => unreachable!(),
            }
        } else {
            unreachable!();
        }

        let u = ops::Record::Negative(vec![1.into(), 1.into()], 4).into();

        // negative row for a group should emit -1 and +0
        let out = c.forward(u, src, 4, Some(&s), &HashMap::new());
        if let Some(ops::Update::Records(rs)) = out {
            assert_eq!(rs.len(), 2);
            let mut rs = rs.into_iter();

            match rs.next().unwrap() {
                ops::Record::Negative(r, ts) => {
                    assert_eq!(r[0], 1.into());
                    assert_eq!(r[1], 1.into());
                    // NOTE: this is 1 because we didn't absorb 3
                    assert_eq!(ts, 1);
                }
                _ => unreachable!(),
            }
            match rs.next().unwrap() {
                ops::Record::Positive(r, ts) => {
                    assert_eq!(r[0], 1.into());
                    assert_eq!(r[1], 0.into());
                    assert_eq!(ts, 4);
                }
                _ => unreachable!(),
            }
        } else {
            unreachable!();
        }

        let u = ops::Update::Records(vec![
             ops::Record::Negative(vec![1.into(), 1.into()], 1),
             ops::Record::Positive(vec![1.into(), 1.into()], 5),
             ops::Record::Positive(vec![1.into(), 2.into()], 3),
             ops::Record::Negative(vec![2.into(), 2.into()], 2),
             ops::Record::Positive(vec![2.into(), 2.into()], 5),
             ops::Record::Positive(vec![2.into(), 3.into()], 5),
             ops::Record::Positive(vec![2.into(), 1.into()], 5),
             ops::Record::Positive(vec![3.into(), 3.into()], 5),
        ]);

        // multiple positives and negatives should update aggregation value by appropriate amount
        // TODO: check for correct output ts'es
        let out = c.forward(u, src, 5, Some(&s), &HashMap::new());
        if let Some(ops::Update::Records(rs)) = out {
            assert_eq!(rs.len(), 6); // one - and one + for each group
            // group 1 lost 1 and gained 2
            assert!(rs.iter().any(|r| {
                if let ops::Record::Negative(ref r, ts) = *r {
                    // previous result for group 1 was at ts 1
                    // because we did not add 3 or 4
                    r[0] == 1.into() && r[1] == 1.into() && ts == 1
                } else {
                    false
                }
            }));
            assert!(rs.iter().any(|r| {
                if let ops::Record::Positive(ref r, ts) = *r {
                    r[0] == 1.into() && r[1] == 2.into() && ts == 5
                } else {
                    false
                }
            }));
            // group 2 lost 1 and gained 3
            assert!(rs.iter().any(|r| {
                if let ops::Record::Negative(ref r, ts) = *r {
                    r[0] == 2.into() && r[1] == 1.into() && ts == 2
                } else {
                    false
                }
            }));
            assert!(rs.iter().any(|r| {
                if let ops::Record::Positive(ref r, ts) = *r {
                    r[0] == 2.into() && r[1] == 3.into() && ts == 5
                } else {
                    false
                }
            }));
            // group 3 lost 1 (well, 0) and gained 1
            assert!(rs.iter().any(|r| {
                if let ops::Record::Negative(ref r, ts) = *r {
                    r[0] == 3.into() && r[1] == 0.into() && ts == 0
                } else {
                    false
                }
            }));
            assert!(rs.iter().any(|r| {
                if let ops::Record::Positive(ref r, ts) = *r {
                    r[0] == 3.into() && r[1] == 1.into() && ts == 5
                } else {
                    false
                }
            }));
        } else {
            unreachable!();
        }
    }

    // TODO: also test SUM

    fn source(p: ops::Params, _: i64) -> Vec<(Vec<query::DataType>, i64)> {
        let data = vec![
                (vec![1.into(), 1.into()], 0),
                (vec![2.into(), 1.into()], 1),
                (vec![2.into(), 2.into()], 2),
            ];

        assert_eq!(p.len(), 1);
        let p = p.into_iter().last().unwrap();
        let q = query::Query::new(&[true, true],
                                  vec![shortcut::Condition {
                                           column: 0,
                                           cmp: shortcut::Comparison::Equal(p),
                                       }]);

        data.into_iter().filter_map(move |(r, ts)| q.feed(&r[..]).map(|r| (r, ts))).collect()
    }

    #[test]
    fn it_queries() {
        use std::sync;

        let c = Aggregation::COUNT.new(0.into(), 1, 2);

        let mut aqfs = HashMap::new();
        aqfs.insert(0.into(), Box::new(source) as Box<_>);
        let aqfs = sync::Arc::new(aqfs);

        let hits = c.query(None, 0, &aqfs);
        assert_eq!(hits.len(), 2);
        assert!(hits.iter().any(|&(ref r, _)| r[0] == 1.into() && r[1] == 1.into()));
        assert!(hits.iter().any(|&(ref r, _)| r[0] == 2.into() && r[1] == 2.into()));

        let q = query::Query::new(&[true, true],
                                  vec![shortcut::Condition {
                             column: 0,
                             cmp: shortcut::Comparison::Equal(shortcut::Value::Const(2.into())),
                         }]);

        let hits = c.query(Some(&q), 0, &aqfs);
        assert_eq!(hits.len(), 1);
        assert!(hits.iter().any(|&(ref r, _)| r[0] == 2.into() && r[1] == 2.into()));
    }

    #[test]
    fn it_queries_zeros() {
        use std::sync;

        let c = Aggregation::COUNT.new(0.into(), 1, 2);

        let mut aqfs = HashMap::new();
        aqfs.insert(0.into(), Box::new(source) as Box<_>);
        let aqfs = sync::Arc::new(aqfs);

        let q = query::Query::new(&[true, true],
                                  vec![shortcut::Condition {
                             column: 0,
                             cmp: shortcut::Comparison::Equal(shortcut::Value::Const(100.into())),
                         }]);

        let hits = c.query(Some(&q), 0, &aqfs);
        assert_eq!(hits.len(), 1);
        assert!(hits.iter().any(|&(ref r, _)| r[0] == 100.into() && r[1] == 0.into()));
    }

    #[test]
    fn it_suggests_indices() {
        let c = Aggregation::COUNT.new(1.into(), 1, 3);
        let hm: HashMap<_, _> = Some((0.into(), vec![0, 1]))
            .into_iter()
            .collect();
        assert_eq!(hm, c.suggest_indexes(0.into()));
    }

    #[test]
    fn it_resolves() {
        let c = Aggregation::COUNT.new(1.into(), 1, 3);
        assert_eq!(c.resolve(0), vec![(1.into(), 0)]);
        assert_eq!(c.resolve(1), vec![(1.into(), 2)]);
    }
}
