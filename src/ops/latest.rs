use ops;
use flow;
use query;
use backlog;
use ops::NodeOp;
use ops::NodeType;

use std::collections::HashMap;
use std::collections::HashSet;

use shortcut;

/// Latest provides an operator that will maintain the last record for every group.
///
/// Whenever a new record arrives for a group, the latest operator will negative the previous
/// latest for that group.
#[derive(Debug)]
pub struct Latest {
    src: flow::NodeIndex,
    srcn: Option<ops::V>,
    // MUST be in reverse sorted order!
    key: Vec<usize>,
    key_m: HashMap<usize, usize>,
}

impl Latest {
    /// Construct a new latest operator.
    ///
    /// `src` should be the ancestor the operation is performed over, and `keys` should be a list
    /// of fields used to group records by. The latest record *within each group* will be
    /// maintained.
    pub fn new(src: flow::NodeIndex, mut keys: Vec<usize>) -> Latest {
        keys.sort();
        let key_m = keys.clone().into_iter().enumerate().map(|(idx, col)| (col, idx)).collect();
        keys.reverse();
        Latest {
            src: src,
            srcn: None,
            key: keys,
            key_m: key_m,
        }
    }
}

impl From<Latest> for NodeType {
    fn from(b: Latest) -> NodeType {
        NodeType::Latest(b)
    }
}

impl NodeOp for Latest {
    fn prime(&mut self, g: &ops::Graph) -> Vec<flow::NodeIndex> {
        self.srcn = g[self.src].as_ref().cloned();
        vec![self.src]
    }

    fn forward(&self,
               u: Option<ops::Update>,
               src: flow::NodeIndex,
               _: i64,
               last: bool,
               db: Option<&backlog::BufferedStore>)
               -> flow::ProcessingResult<ops::Update> {

        assert_eq!(src, self.src);

        if u.is_none() {
            // we only have one ancestor, so this must be last, and our ancestor sent nothing
            debug_assert!(last);
            return u.into();
        }
        let u = u.unwrap();

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
                for r in pos {
                    {
                        let r = r.rec();

                        // set up the query for the current record for this record's key
                        for s in &mut q {
                            s.cmp =
                                shortcut::Comparison::Equal(shortcut::Value::Const(r[s.column]
                                    .clone()));
                        }
                    }

                    // find the current value for this group
                    let current = match db {
                        Some(db) => {
                            db.find_and(&q[..], Some(i64::max_value()), |rs| {
                                println!("{:?}: {:?}", q, rs);
                                assert!(rs.len() <= 1, "latest group has more than 1 record");
                                rs.into_iter()
                                    .next()
                                    .map(|(r, ts)| (r.into_iter().cloned().collect(), ts))
                            })
                        }
                        None => {
                            // TODO: query ancestor (self.query?) based on self.key columns
                            unimplemented!()
                        }
                    };


                    // get back values from query (to avoid cloning again)
                    let mut group = Vec::with_capacity(self.key.len());
                    for s in &mut q {
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
                        out.push(ops::Record::Negative(current.0, current.1));
                    }
                    out.push(r);
                }

                // check that there aren't any standalone negatives
                // XXX: this check actually incurs a decent performance hit -- both tracking
                // handled groups above, and this loop here. maybe just kill it?
                for r in neg {
                    // we can swap_remove here because we know self.keys is in reverse sorted order
                    let (mut r, _, _) = r.extract();
                    let group: Vec<_> = self.key.iter().map(|&i| r.swap_remove(i)).collect();
                    assert!(handled.contains(&group));
                }

                flow::ProcessingResult::Done(ops::Update::Records(out))
            }
        }
    }

    fn query(&self, q: Option<&query::Query>, ts: i64) -> ops::Datas {
        use std::iter;

        // we're fetching everything from our parent
        let mut params = None;

        // however, if there are some conditions that filter over a field present in the input (so
        // everything except conditions on self.over), we should use those as parameters to speed
        // things up.
        if let Some(q) = q {
            params = Some(q.having
                .iter()
                .filter_map(|c| {
                    // non-key conditionals need to be matched against per group after to match the
                    // semantics you'd get if the query was run against the materialized output
                    // directly.
                    self.key_m.get(&c.column).map(|&col| {
                        shortcut::Condition {
                            column: col,
                            cmp: c.cmp.clone(),
                        }

                    })
                })
                .collect::<Vec<_>>());

            if params.as_ref().unwrap().is_empty() {
                params = None;
            }
        }

        let q = params.map(|ps| {
            query::Query::new(&iter::repeat(true)
                                  .take(self.srcn.as_ref().unwrap().args().len())
                                  .collect::<Vec<_>>(),
                              ps)
        });

        // now, query our ancestor, and aggregate into groups.
        let rx = self.srcn.as_ref().unwrap().find(q.as_ref(), Some(ts));

        // FIXME: having an order by would be nice here, so that we didn't have to keep the entire
        // aggregated state in memory until we've seen all rows.
        let mut consolidate = HashMap::<_, (Vec<_>, i64)>::new();
        for (rec, ts) in rx {
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

    fn resolve(&self, col: usize) -> Option<Vec<(flow::NodeIndex, usize)>> {
        Some(vec![(self.src, col)])
    }

    fn description(&self) -> String {
        let key_cols = self.key.iter().map(|k| k.to_string())
            .collect::<Vec<_>>().join(", ");
        format!("⧖ γ[{}]", key_cols)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use ops;
    use flow;
    use query;
    use petgraph;
    use shortcut;

    use flow::View;
    use ops::NodeOp;

    fn setup(key: Vec<usize>, mat: bool) -> ops::Node {
        use std::sync;

        let mut g = petgraph::Graph::new();

        let big = key.len() > 1;
        let mut s = if big {
            ops::new("source", &["x", "y", "z"], true, ops::base::Base {})
        } else {
            ops::new("source", &["x", "y"], true, ops::base::Base {})
        };

        s.prime(&g);
        let s = g.add_node(Some(sync::Arc::new(s)));
        if !big {
            g[s].as_ref().unwrap().process(Some((vec![1.into(), 1.into()], 0).into()), s, 0, true);
            g[s].as_ref().unwrap().process(Some((vec![2.into(), 2.into()], 2).into()), s, 2, true);
            // note that this isn't really allowed in graphs. flow ensures that updates are
            // delivered in order. however it's safe to do this here because there's no forwarding,
            // and base does no computation. we do it to test Latest's behavior when it receives a
            // non-latest after a latest.
            g[s].as_ref().unwrap().process(Some((vec![2.into(), 1.into()], 1).into()), s, 1, true);
            g[s].as_ref().unwrap().process(Some((vec![1.into(), 2.into()], 3).into()), s, 3, true);
            g[s].as_ref().unwrap().process(Some((vec![3.into(), 3.into()], 4).into()), s, 4, true);
        }

        let mut l = Latest::new(s, key);
        l.prime(&g);
        if big {
            ops::new("latest", &["x", "y", "z"], mat, l)
        } else {
            ops::new("latest", &["x", "y"], mat, l)
        }
    }

    #[test]
    fn it_describes() {
        let c = setup(vec![0, 2], false);
        assert_eq!(c.inner.description(), "⧖ γ[2, 0]");
    }

    #[test]
    fn it_forwards() {
        let src = flow::NodeIndex::new(0);
        let c = setup(vec![0], true);

        let u = (vec![1.into(), 1.into()], 1).into();

        // first record for a group should emit just a positive
        let out = c.process(Some(u), src, 1, true);
        if let flow::ProcessingResult::Done(ops::Update::Records(rs)) = out {
            assert_eq!(rs.len(), 1);
            let mut rs = rs.into_iter();

            match rs.next().unwrap() {
                ops::Record::Positive(r, ts) => {
                    assert_eq!(r[0], 1.into());
                    assert_eq!(r[1], 1.into());
                    assert_eq!(ts, 1);
                    c.safe(1);
                }
                _ => unreachable!(),
            }
        } else {
            unreachable!();
        }

        let u = (vec![2.into(), 2.into()], 2).into();

        // first record for a second group should also emit just a positive
        let out = c.process(Some(u), src, 2, true);
        if let flow::ProcessingResult::Done(ops::Update::Records(rs)) = out {
            assert_eq!(rs.len(), 1);
            let mut rs = rs.into_iter();

            match rs.next().unwrap() {
                ops::Record::Positive(r, ts) => {
                    assert_eq!(r[0], 2.into());
                    assert_eq!(r[1], 2.into());
                    assert_eq!(ts, 2);
                    c.safe(2);
                }
                _ => unreachable!(),
            }
        } else {
            unreachable!();
        }

        let u = (vec![1.into(), 2.into()], 3).into();

        // new record for existing group should revoke the old latest, and emit the new
        let out = c.process(Some(u), src, 3, true);
        if let flow::ProcessingResult::Done(ops::Update::Records(rs)) = out {
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
                    c.safe(3);
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
        let out = c.process(Some(u), src, 4, true);
        if let flow::ProcessingResult::Done(ops::Update::Records(rs)) = out {
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
        let src = flow::NodeIndex::new(0);
        let c = setup(vec![0, 1], true);

        let u = (vec![1.into(), 1.into(), 1.into()], 1).into();
        c.process(Some(u), src, 1, true);
        c.safe(1);

        // first record for a second group should also emit just a positive
        let u = (vec![1.into(), 2.into(), 2.into()], 2).into();

        let out = c.process(Some(u), src, 2, true);
        if let flow::ProcessingResult::Done(ops::Update::Records(rs)) = out {
            assert_eq!(rs.len(), 1);
            let mut rs = rs.into_iter();

            match rs.next().unwrap() {
                ops::Record::Positive(r, ts) => {
                    assert_eq!(r[0], 1.into());
                    assert_eq!(r[1], 2.into());
                    assert_eq!(r[2], 2.into());
                    assert_eq!(ts, 2);
                    c.safe(2);
                }
                _ => unreachable!(),
            }
        } else {
            unreachable!();
        }

        let u = (vec![1.into(), 1.into(), 2.into()], 3).into();

        // new record for existing group should revoke the old latest, and emit the new
        let out = c.process(Some(u), src, 3, true);
        if let flow::ProcessingResult::Done(ops::Update::Records(rs)) = out {
            assert_eq!(rs.len(), 2);
            let mut rs = rs.into_iter();

            match rs.next().unwrap() {
                ops::Record::Negative(r, ts) => {
                    assert_eq!(r[0], 1.into());
                    assert_eq!(r[1], 1.into());
                    assert_eq!(r[2], 1.into());
                    assert_eq!(ts, 1);
                }
                _ => unreachable!(),
            }
            match rs.next().unwrap() {
                ops::Record::Positive(r, ts) => {
                    assert_eq!(r[0], 1.into());
                    assert_eq!(r[1], 1.into());
                    assert_eq!(r[2], 2.into());
                    assert_eq!(ts, 3);
                    c.safe(3);
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
        let out = c.process(Some(u), src, 4, true);
        if let flow::ProcessingResult::Done(ops::Update::Records(rs)) = out {
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

    #[test]
    fn it_queries() {
        let l = setup(vec![0], false);

        let hits = l.find(None, None);
        assert_eq!(hits.len(), 3);
        assert!(hits.iter().any(|&(ref r, ts)| ts == 3 && r[0] == 1.into() && r[1] == 2.into()));
        assert!(hits.iter().any(|&(ref r, ts)| ts == 2 && r[0] == 2.into() && r[1] == 2.into()));
        assert!(hits.iter().any(|&(ref r, ts)| ts == 4 && r[0] == 3.into() && r[1] == 3.into()));

        let q = query::Query::new(&[true, true],
                                  vec![shortcut::Condition {
                             column: 0,
                             cmp: shortcut::Comparison::Equal(shortcut::Value::Const(2.into())),
                         }]);

        let hits = l.find(Some(&q), None);
        assert_eq!(hits.len(), 1);
        assert!(hits.iter().any(|&(ref r, ts)| ts == 2 && r[0] == 2.into() && r[1] == 2.into()));
    }

    #[test]
    fn it_suggests_indices() {
        let c = setup(vec![0, 1], false);
        let idx = c.suggest_indexes(1.into());

        // should only add index on own columns
        assert_eq!(idx.len(), 1);
        assert!(idx.contains_key(&1.into()));

        // should only index on group-by columns
        assert_eq!(idx[&1.into()].len(), 2);
        assert!(idx[&1.into()].iter().any(|&i| i == 0));
        assert!(idx[&1.into()].iter().any(|&i| i == 1));
    }

    #[test]
    fn it_resolves() {
        let c = setup(vec![0, 1], false);
        assert_eq!(c.resolve(0), Some(vec![(0.into(), 0)]));
        assert_eq!(c.resolve(1), Some(vec![(0.into(), 1)]));
        assert_eq!(c.resolve(2), Some(vec![(0.into(), 2)]));
    }
}
