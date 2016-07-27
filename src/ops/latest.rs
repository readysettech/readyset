use ops;
use flow;
use query;
use backlog;
use ops::NodeOp;

use std::collections::HashMap;
use std::collections::HashSet;

use shortcut;

pub struct Latest {
    src: flow::NodeIndex,
    // MUST be in reverse sorted order!
    key: Vec<usize>,
}

impl Latest {
    pub fn new(src: flow::NodeIndex, mut keys: Vec<usize>) -> Latest {
        keys.sort();
        keys.reverse();
        Latest {
            src: src,
            key: keys,
        }
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
        assert!(db.is_some(), "LATEST views must be materialized");
        let db = db.unwrap();

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
                    let matches = db.find(&q[..], Some(i64::max_value()));
                    println!("{:?}: {:?}", q, matches);
                    assert!(matches.len() <= 1, "latest group has more than 1 record");
                    let current = matches.into_iter().next();

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
                        out.push(ops::Record::Negative(current.iter().cloned().collect()));
                    }
                    out.push(r);
                }

                // check that there aren't any standalone negatives
                // XXX: this check actually incurs a decent performance hit -- both tracking
                // handled groups above, and this loop here. maybe just kill it?
                for r in neg.into_iter() {
                    // we can swap_remove here because we know self.keys is in reverse sorted order
                    let (mut r, _) = r.extract();
                    let group: Vec<_> = self.key.iter().map(|&i| r.swap_remove(i)).collect();
                    assert!(handled.contains(&group));
                }

                Some(ops::Update::Records(out))
            }
        }
    }

    fn query(&self, _: Option<&query::Query>, _: i64, _: &ops::AQ) -> ops::Datas {
        // how would we do this? downstream doesn't expose timestamps per record to us :/
        // this also means that you can't add a latest in a migration :(
        unimplemented!();
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
    use backlog;

    use ops::NodeOp;
    use std::collections::HashMap;

    #[test]
    fn it_forwards() {
        let mut s = backlog::BufferedStore::new(2);
        let src = flow::NodeIndex::new(0);

        let c = Latest::new(src, vec![0]);
        let u = vec![1.into(), 1.into()].into();

        // first record for a group should emit just a positive
        let out = c.forward(u, src, 0, Some(&s), &HashMap::new());
        if let Some(ops::Update::Records(rs)) = out {
            assert_eq!(rs.len(), 1);
            let mut rs = rs.into_iter();

            match rs.next().unwrap() {
                ops::Record::Positive(r) => {
                    assert_eq!(r[0], 1.into());
                    assert_eq!(r[1], 1.into());
                    // add back to store
                    s.add(vec![ops::Record::Positive(r)], 0);
                    s.absorb(0);
                }
                _ => unreachable!(),
            }
        } else {
            unreachable!();
        }

        let u = vec![2.into(), 2.into()].into();

        // first record for a second group should also emit just a positive
        let out = c.forward(u, src, 0, Some(&s), &HashMap::new());
        if let Some(ops::Update::Records(rs)) = out {
            assert_eq!(rs.len(), 1);
            let mut rs = rs.into_iter();

            match rs.next().unwrap() {
                ops::Record::Positive(r) => {
                    assert_eq!(r[0], 2.into());
                    assert_eq!(r[1], 2.into());
                    // add back to store
                    s.add(vec![ops::Record::Positive(r)], 1);
                    s.absorb(1);
                }
                _ => unreachable!(),
            }
        } else {
            unreachable!();
        }

        let u = vec![1.into(), 2.into()].into();

        // new record for existing group should revoke the old latest, and emit the new
        let out = c.forward(u, src, 0, Some(&s), &HashMap::new());
        if let Some(ops::Update::Records(rs)) = out {
            assert_eq!(rs.len(), 2);
            let mut rs = rs.into_iter();

            match rs.next().unwrap() {
                ops::Record::Negative(r) => {
                    assert_eq!(r[0], 1.into());
                    assert_eq!(r[1], 1.into());
                    // remove from store
                    s.add(vec![ops::Record::Negative(r)], 2);
                }
                _ => unreachable!(),
            }
            match rs.next().unwrap() {
                ops::Record::Positive(r) => {
                    assert_eq!(r[0], 1.into());
                    assert_eq!(r[1], 2.into());
                    // add to store
                    s.add(vec![ops::Record::Positive(r)], 3);
                    s.absorb(3);
                }
                _ => unreachable!(),
            }
        } else {
            unreachable!();
        }

        let u = ops::Update::Records(vec![
             ops::Record::Negative(vec![1.into(), 1.into()]),
             ops::Record::Negative(vec![1.into(), 2.into()]),
             ops::Record::Positive(vec![1.into(), 3.into()]),
             ops::Record::Negative(vec![2.into(), 2.into()]),
             ops::Record::Positive(vec![2.into(), 4.into()]),
        ]);

        // negatives and positives should still result in only one new current for each group
        let out = c.forward(u, src, 0, Some(&s), &HashMap::new());
        if let Some(ops::Update::Records(rs)) = out {
            assert_eq!(rs.len(), 4); // one - and one + for each group
            // group 1 lost 2 and gained 3
            assert!(rs.iter().any(|r| {
                if let ops::Record::Negative(ref r) = *r {
                    r[0] == 1.into() && r[1] == 2.into()
                } else {
                    false
                }
            }));
            assert!(rs.iter().any(|r| {
                if let ops::Record::Positive(ref r) = *r {
                    r[0] == 1.into() && r[1] == 3.into()
                } else {
                    false
                }
            }));
            // group 2 lost 2 and gained 4
            assert!(rs.iter().any(|r| {
                if let ops::Record::Negative(ref r) = *r {
                    r[0] == 2.into() && r[1] == 2.into()
                } else {
                    false
                }
            }));
            assert!(rs.iter().any(|r| {
                if let ops::Record::Positive(ref r) = *r {
                    r[0] == 2.into() && r[1] == 4.into()
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

        let u = vec![1.into(), 1.into(), 1.into()].into();
        if let Some(ops::Update::Records(rs)) = c.forward(u, src, 0, Some(&s), &HashMap::new()) {
            s.add(rs, 1);
            s.absorb(1);
        }

        // first record for a second group should also emit just a positive
        let u = vec![1.into(), 2.into(), 2.into()].into();
        let out = c.forward(u, src, 0, Some(&s), &HashMap::new());
        if let Some(ops::Update::Records(rs)) = out {
            assert_eq!(rs.len(), 1);
            let mut rs = rs.into_iter();

            match rs.next().unwrap() {
                ops::Record::Positive(r) => {
                    assert_eq!(r[0], 1.into());
                    assert_eq!(r[1], 2.into());
                    assert_eq!(r[2], 2.into());
                    // add back to store
                    s.add(vec![ops::Record::Positive(r)], 2);
                    s.absorb(2);
                }
                _ => unreachable!(),
            }
        } else {
            unreachable!();
        }

        let u = vec![1.into(), 1.into(), 2.into()].into();

        // new record for existing group should revoke the old latest, and emit the new
        let out = c.forward(u, src, 0, Some(&s), &HashMap::new());
        if let Some(ops::Update::Records(rs)) = out {
            assert_eq!(rs.len(), 2);
            let mut rs = rs.into_iter();

            match rs.next().unwrap() {
                ops::Record::Negative(r) => {
                    assert_eq!(r[0], 1.into());
                    assert_eq!(r[1], 1.into());
                    assert_eq!(r[2], 1.into());
                    // remove from store
                    s.add(vec![ops::Record::Negative(r)], 3);
                }
                _ => unreachable!(),
            }
            match rs.next().unwrap() {
                ops::Record::Positive(r) => {
                    assert_eq!(r[0], 1.into());
                    assert_eq!(r[1], 1.into());
                    assert_eq!(r[2], 2.into());
                    // add to store
                    s.add(vec![ops::Record::Positive(r)], 4);
                    s.absorb(4);
                }
                _ => unreachable!(),
            }
        } else {
            unreachable!();
        }

        let u = ops::Update::Records(vec![
             ops::Record::Negative(vec![1.into(), 1.into(), 1.into()]),
             ops::Record::Negative(vec![1.into(), 1.into(), 2.into()]),
             ops::Record::Positive(vec![1.into(), 1.into(), 3.into()]),
             ops::Record::Negative(vec![1.into(), 2.into(), 2.into()]),
             ops::Record::Positive(vec![1.into(), 2.into(), 4.into()]),
        ]);

        // negatives and positives should still result in only one new current for each group
        let out = c.forward(u, src, 0, Some(&s), &HashMap::new());
        if let Some(ops::Update::Records(rs)) = out {
            assert_eq!(rs.len(), 4); // one - and one + for each group
            // group 1 lost 2 and gained 3
            assert!(rs.iter().any(|r| {
                if let ops::Record::Negative(ref r) = *r {
                    r[1] == 1.into() && r[2] == 2.into()
                } else {
                    false
                }
            }));
            assert!(rs.iter().any(|r| {
                if let ops::Record::Positive(ref r) = *r {
                    r[1] == 1.into() && r[2] == 3.into()
                } else {
                    false
                }
            }));
            // group 2 lost 2 and gained 4
            assert!(rs.iter().any(|r| {
                if let ops::Record::Negative(ref r) = *r {
                    r[1] == 2.into() && r[2] == 2.into()
                } else {
                    false
                }
            }));
            assert!(rs.iter().any(|r| {
                if let ops::Record::Positive(ref r) = *r {
                    r[1] == 2.into() && r[2] == 4.into()
                } else {
                    false
                }
            }));
        } else {
            unreachable!();
        }
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
