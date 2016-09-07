use ops;
use flow;
use query;
use backlog;
use ops::NodeOp;
use ops::NodeType;

use std::collections::HashMap;
use std::collections::HashSet;

use shortcut;

#[derive(Debug)]
pub struct Latest {
    src: flow::NodeIndex,
    // MUST be in reverse sorted order!
    key: Vec<usize>,
    key_m: HashMap<usize, usize>,
}

impl Latest {
    pub fn new(src: flow::NodeIndex, mut keys: Vec<usize>) -> Latest {
        keys.sort();
        let key_m = keys.clone().into_iter().enumerate().map(|(idx, col)| (col, idx)).collect();
        keys.reverse();
        Latest {
            src: src,
            key: keys,
            key_m: key_m,
        }
    }
}

impl From<Latest> for NodeType {
    fn from(b: Latest) -> NodeType {
        NodeType::LatestNode(b)
    }
}

impl NodeOp for Latest {
    fn forward(&self,
               u: ops::Update,
               src: flow::NodeIndex,
               _: i64,
               db: Option<&backlog::BufferedStore>,
               _: &ops::AQ)
               -> Option<ops::Update> {

        assert_eq!(src, self.src);

        // Construct the query we'll need to query into ourselves to find current latest
        let mut q = self.key
            .iter()
            .map(|&col| {
                shortcut::Condition {
                    column: col,
                    cmp: shortcut::Comparison::Equal(shortcut::Value::Const(query::DataType::None)),
                }
            })
            .collect::<Vec<_>>();

        match u {
            ops::Update::Records(rs) => {
                // We don't allow standalone negatives as input to a latest. This is because it
                // would be very computationally expensive (and currently impossible) to find what
                // the *previous* latest was if the current latest was revoked. However, if a
                // record is negated, and a positive for the same key is given in the same group,
                // then we should just emit the new record as the new latest.
                //
                // We do this by processing in two steps. We first process all positives, emitting
                // all the -/+ pairs for each one, and keeping track of which keys we have handled.
                // Then, we assert that there are no negatives whose key does not appear in the
                // list of keys that have been handled.
                let (pos, neg): (Vec<_>, _) = rs.into_iter().partition(|r| r.is_positive());
                let mut handled = HashSet::new();

                // buffer emitted records
                let mut out = Vec::with_capacity(pos.len());
                for r in pos.into_iter() {
                    {
                        let r = r.rec();

                        // set up the query for the current record for this record's key
                        for s in q.iter_mut() {
                            s.cmp =
                                shortcut::Comparison::Equal(shortcut::Value::Const(r[s.column]
                                    .clone()));
                        }
                    }

                    // find the current value for this group
                    let current = match db {
                        Some(db) => {
                            let matches = db.find(&q[..], Some(i64::max_value()));
                            println!("{:?}: {:?}", q, matches);
                            assert!(matches.len() <= 1, "latest group has more than 1 record");
                            matches.into_iter().next()
                        }
                        None => {
                            // TODO: query ancestor (self.query?) based on self.key columns
                            unimplemented!()
                        }
                    };


                    // get back values from query (to avoid cloning again)
                    let mut group = Vec::with_capacity(self.key.len());
                    for s in q.iter_mut() {
                        if let shortcut::Comparison::Equal(shortcut::Value::Const(ref mut v)) =
                               s.cmp {
                            use std::mem;

                            let mut x = query::DataType::None;
                            mem::swap(&mut x, v);
                            group.push(x);
                        }
                    }
                    handled.insert(group);

                    // if there was a previous latest for this key, revoke old record
                    if let Some(current) = current {
                        out.push(ops::Record::Negative(current.0.iter().cloned().collect(),
                                                       current.1));
                    }
                    out.push(r);
                }

                // check that there aren't any standalone negatives
                // XXX: this check actually incurs a decent performance hit -- both tracking
                // handled groups above, and this loop here. maybe just kill it?
                for r in neg.into_iter() {
                    // we can swap_remove here because we know self.keys is in reverse sorted order
                    let (mut r, _, _) = r.extract();
                    let group: Vec<_> = self.key.iter().map(|&i| r.swap_remove(i)).collect();
                    assert!(handled.contains(&group));
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
            iter::repeat(shortcut::Value::Const(query::DataType::None))
                .take(self.key.len())
                .collect();

        // we find all conditions that filter over a field present in the input (so everything
        // except conditions on self.over), and use those as parameters.
        if let Some(q) = q {
            for c in q.having.iter() {
                // non-key conditionals need to be matched against per group after
                // to match the semantics you'd get if the query was run against
                // the materialized output directly.
                if let Some(col) = self.key_m.get(&c.column) {
                    match c.cmp {
                        shortcut::Comparison::Equal(ref v) => {
                            *params.get_mut(*col).unwrap() = v.clone();
                        }
                    }
                }
            }
        }

        // now, query our ancestor, and aggregate into groups.
        let rx = (*aqfs.iter().next().unwrap().1)(params, ts);

        // FIXME: having an order by would be nice here, so that we didn't have to keep the entire
        // aggregated state in memory until we've seen all rows.
        let mut consolidate = HashMap::<_, (Vec<_>, i64)>::new();
        for (rec, ts) in rx.into_iter() {
            use std::collections::hash_map::Entry;

            let (group, rest): (Vec<_>, _) =
                rec.into_iter().enumerate().partition(|&(ref fi, _)| self.key_m.contains_key(fi));
            assert_eq!(group.len(), self.key.len());

            let group = group.into_iter().map(|(_, v)| v).collect();
            match consolidate.entry(group) {
                Entry::Occupied(mut e) => {
                    let e = e.get_mut();
                    if e.1 < ts {
                        e.1 = ts;
                        e.0.clear();
                        e.0.extend(rest.into_iter().map(|(_, v)| v));
                    }
                }
                Entry::Vacant(v) => {
                    v.insert((rest.into_iter().map(|(_, v)| v).collect(), ts));
                }
            }
        }

        consolidate.into_iter()
            .map(|(group, (rest, ts)): (Vec<_>, (Vec<_>, i64))| {
                let all = group.len() + rest.len();
                let mut group = group.into_iter();
                let mut rest = rest.into_iter();
                let mut row = Vec::with_capacity(all);
                for i in 0..all {
                    if self.key_m.contains_key(&i) {
                        row.push(group.next().unwrap());
                    } else {
                        row.push(rest.next().unwrap());
                    }
                }
                (row, ts)
            })
            .collect()
    }

    fn suggest_indexes(&self, this: flow::NodeIndex) -> HashMap<flow::NodeIndex, Vec<usize>> {
        // index all key columns
        Some((this, self.key.clone())).into_iter().collect()
    }

    fn resolve(&self, col: usize) -> Vec<(flow::NodeIndex, usize)> {
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
        let mut s = backlog::BufferedStore::new(2);
        let src = flow::NodeIndex::new(0);

        let c = Latest::new(src, vec![0]);
        let u = (vec![1.into(), 1.into()], 1).into();

        // first record for a group should emit just a positive
        let out = c.forward(u, src, 1, Some(&s), &HashMap::new());
        if let Some(ops::Update::Records(rs)) = out {
            assert_eq!(rs.len(), 1);
            let mut rs = rs.into_iter();

            match rs.next().unwrap() {
                ops::Record::Positive(r, ts) => {
                    assert_eq!(r[0], 1.into());
                    assert_eq!(r[1], 1.into());
                    assert_eq!(ts, 1);
                    // add back to store
                    s.add(vec![ops::Record::Positive(r, ts)], 1);
                    s.absorb(1);
                }
                _ => unreachable!(),
            }
        } else {
            unreachable!();
        }

        let u = (vec![2.into(), 2.into()], 2).into();

        // first record for a second group should also emit just a positive
        let out = c.forward(u, src, 2, Some(&s), &HashMap::new());
        if let Some(ops::Update::Records(rs)) = out {
            assert_eq!(rs.len(), 1);
            let mut rs = rs.into_iter();

            match rs.next().unwrap() {
                ops::Record::Positive(r, ts) => {
                    assert_eq!(r[0], 2.into());
                    assert_eq!(r[1], 2.into());
                    assert_eq!(ts, 2);
                    // add back to store
                    s.add(vec![ops::Record::Positive(r, ts)], 2);
                    s.absorb(2);
                }
                _ => unreachable!(),
            }
        } else {
            unreachable!();
        }

        let u = (vec![1.into(), 2.into()], 3).into();

        // new record for existing group should revoke the old latest, and emit the new
        let out = c.forward(u, src, 3, Some(&s), &HashMap::new());
        if let Some(ops::Update::Records(rs)) = out {
            assert_eq!(rs.len(), 2);
            let mut rs = rs.into_iter();

            match rs.next().unwrap() {
                ops::Record::Negative(r, ts) => {
                    assert_eq!(r[0], 1.into());
                    assert_eq!(r[1], 1.into());
                    assert_eq!(ts, 1);
                    // remove from store
                    s.add(vec![ops::Record::Negative(r, ts)], 3);
                }
                _ => unreachable!(),
            }
            match rs.next().unwrap() {
                ops::Record::Positive(r, ts) => {
                    assert_eq!(r[0], 1.into());
                    assert_eq!(r[1], 2.into());
                    assert_eq!(ts, 3);
                    // add to store
                    s.add(vec![ops::Record::Positive(r, ts)], 3);
                    s.absorb(3);
                }
                _ => unreachable!(),
            }
        } else {
            unreachable!();
        }

        let u = ops::Update::Records(vec![
             ops::Record::Negative(vec![1.into(), 1.into()], 1),
             ops::Record::Negative(vec![1.into(), 2.into()], 3),
             ops::Record::Positive(vec![1.into(), 3.into()], 4),
             ops::Record::Negative(vec![2.into(), 2.into()], 2),
             ops::Record::Positive(vec![2.into(), 4.into()], 4),
        ]);

        // negatives and positives should still result in only one new current for each group
        let out = c.forward(u, src, 4, Some(&s), &HashMap::new());
        if let Some(ops::Update::Records(rs)) = out {
            assert_eq!(rs.len(), 4); // one - and one + for each group
            // group 1 lost 2 and gained 3
            assert!(rs.iter().any(|r| {
                if let ops::Record::Negative(ref r, ts) = *r {
                    r[0] == 1.into() && r[1] == 2.into() && ts == 3
                } else {
                    false
                }
            }));
            assert!(rs.iter().any(|r| {
                if let ops::Record::Positive(ref r, ts) = *r {
                    r[0] == 1.into() && r[1] == 3.into() && ts == 4
                } else {
                    false
                }
            }));
            // group 2 lost 2 and gained 4
            assert!(rs.iter().any(|r| {
                if let ops::Record::Negative(ref r, ts) = *r {
                    r[0] == 2.into() && r[1] == 2.into() && ts == 2
                } else {
                    false
                }
            }));
            assert!(rs.iter().any(|r| {
                if let ops::Record::Positive(ref r, ts) = *r {
                    r[0] == 2.into() && r[1] == 4.into() && ts == 4
                } else {
                    false
                }
            }));
        } else {
            unreachable!();
        }
    }

    #[test]
    fn it_forwards_mkey() {
        let mut s = backlog::BufferedStore::new(3);
        let src = flow::NodeIndex::new(0);

        let c = Latest::new(src, vec![0, 1]);

        let u = (vec![1.into(), 1.into(), 1.into()], 1).into();

        if let Some(ops::Update::Records(rs)) = c.forward(u, src, 1, Some(&s), &HashMap::new()) {
            s.add(rs, 1);
            s.absorb(1);
        }

        // first record for a second group should also emit just a positive
        let u = (vec![1.into(), 2.into(), 2.into()], 2).into();

        let out = c.forward(u, src, 2, Some(&s), &HashMap::new());
        if let Some(ops::Update::Records(rs)) = out {
            assert_eq!(rs.len(), 1);
            let mut rs = rs.into_iter();

            match rs.next().unwrap() {
                ops::Record::Positive(r, ts) => {
                    assert_eq!(r[0], 1.into());
                    assert_eq!(r[1], 2.into());
                    assert_eq!(r[2], 2.into());
                    assert_eq!(ts, 2);
                    // add back to store
                    s.add(vec![ops::Record::Positive(r, ts)], 2);
                    s.absorb(2);
                }
                _ => unreachable!(),
            }
        } else {
            unreachable!();
        }

        let u = (vec![1.into(), 1.into(), 2.into()], 3).into();

        // new record for existing group should revoke the old latest, and emit the new
        let out = c.forward(u, src, 3, Some(&s), &HashMap::new());
        if let Some(ops::Update::Records(rs)) = out {
            assert_eq!(rs.len(), 2);
            let mut rs = rs.into_iter();

            match rs.next().unwrap() {
                ops::Record::Negative(r, ts) => {
                    assert_eq!(r[0], 1.into());
                    assert_eq!(r[1], 1.into());
                    assert_eq!(r[2], 1.into());
                    assert_eq!(ts, 1);
                    // remove from store
                    s.add(vec![ops::Record::Negative(r, ts)], 3);
                }
                _ => unreachable!(),
            }
            match rs.next().unwrap() {
                ops::Record::Positive(r, ts) => {
                    assert_eq!(r[0], 1.into());
                    assert_eq!(r[1], 1.into());
                    assert_eq!(r[2], 2.into());
                    assert_eq!(ts, 3);
                    // add to store
                    s.add(vec![ops::Record::Positive(r, ts)], 3);
                    s.absorb(3);
                }
                _ => unreachable!(),
            }
        } else {
            unreachable!();
        }

        let u = ops::Update::Records(vec![
             ops::Record::Negative(vec![1.into(), 1.into(), 1.into()], 1),
             ops::Record::Negative(vec![1.into(), 1.into(), 2.into()], 3),
             ops::Record::Positive(vec![1.into(), 1.into(), 3.into()], 4),
             ops::Record::Negative(vec![1.into(), 2.into(), 2.into()], 2),
             ops::Record::Positive(vec![1.into(), 2.into(), 4.into()], 4),
        ]);

        // negatives and positives should still result in only one new current for each group
        let out = c.forward(u, src, 4, Some(&s), &HashMap::new());
        if let Some(ops::Update::Records(rs)) = out {
            assert_eq!(rs.len(), 4); // one - and one + for each group
            // group 1 lost 2 and gained 3
            assert!(rs.iter().any(|r| {
                if let ops::Record::Negative(ref r, ts) = *r {
                    r[1] == 1.into() && r[2] == 2.into() && ts == 3
                } else {
                    false
                }
            }));
            assert!(rs.iter().any(|r| {
                if let ops::Record::Positive(ref r, ts) = *r {
                    r[1] == 1.into() && r[2] == 3.into() && ts == 4
                } else {
                    false
                }
            }));
            // group 2 lost 2 and gained 4
            assert!(rs.iter().any(|r| {
                if let ops::Record::Negative(ref r, ts) = *r {
                    r[1] == 2.into() && r[2] == 2.into() && ts == 2
                } else {
                    false
                }
            }));
            assert!(rs.iter().any(|r| {
                if let ops::Record::Positive(ref r, ts) = *r {
                    r[1] == 2.into() && r[2] == 4.into() && ts == 4
                } else {
                    false
                }
            }));
        } else {
            unreachable!();
        }
    }

    fn source(p: ops::Params, _: i64) -> Vec<(Vec<query::DataType>, i64)> {
        let data = vec![
                (vec![1.into(), 1.into()], 0),
                (vec![2.into(), 2.into()], 2),
                (vec![2.into(), 1.into()], 1),
                (vec![1.into(), 2.into()], 3),
                (vec![3.into(), 3.into()], 4),
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

        let l = Latest::new(0.into(), vec![0]);

        let mut aqfs = HashMap::new();
        aqfs.insert(0.into(), Box::new(source) as Box<_>);
        let aqfs = sync::Arc::new(aqfs);

        let hits = l.query(None, 0, &aqfs);
        assert_eq!(hits.len(), 3);
        assert!(hits.iter().any(|&(ref r, ts)| ts == 3 && r[0] == 1.into() && r[1] == 2.into()));
        assert!(hits.iter().any(|&(ref r, ts)| ts == 2 && r[0] == 2.into() && r[1] == 2.into()));
        assert!(hits.iter().any(|&(ref r, ts)| ts == 4 && r[0] == 3.into() && r[1] == 3.into()));

        let q = query::Query::new(&[true, true],
                                  vec![shortcut::Condition {
                             column: 0,
                             cmp: shortcut::Comparison::Equal(shortcut::Value::Const(2.into())),
                         }]);

        let hits = l.query(Some(&q), 0, &aqfs);
        assert_eq!(hits.len(), 1);
        assert!(hits.iter().any(|&(ref r, ts)| ts == 2 && r[0] == 2.into() && r[1] == 2.into()));
    }

    #[test]
    fn it_suggests_indices() {
        let c = Latest::new(1.into(), vec![0, 1]);
        let mut idx = c.suggest_indexes(0.into());
        assert!(idx.contains_key(&0.into()));
        let idx = idx.remove(&0.into()).unwrap();
        assert!(idx.iter().any(|&i| i == 0));
        assert!(idx.iter().any(|&i| i == 1));
    }

    #[test]
    fn it_resolves() {
        let c = Latest::new(1.into(), vec![0, 1]);
        assert_eq!(c.resolve(0), vec![(1.into(), 0)]);
        assert_eq!(c.resolve(1), vec![(1.into(), 1)]);
    }
}
