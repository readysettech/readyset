use ops;
use query;
use shortcut;

use std::collections::HashMap;
use std::collections::HashSet;

use flow::prelude::*;

/// Latest provides an operator that will maintain the last record for every group.
///
/// Whenever a new record arrives for a group, the latest operator will negative the previous
/// latest for that group.
#[derive(Debug)]
pub struct Latest {
    us: NodeIndex,
    src: NodeIndex,
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
    pub fn new(src: NodeIndex, mut keys: Vec<usize>) -> Latest {
        keys.sort();
        let key_m = keys.clone().into_iter().enumerate().map(|(idx, col)| (col, idx)).collect();
        keys.reverse();
        Latest {
            us: 0.into(),
            src: src,
            key: keys,
            key_m: key_m,
        }
    }
}

impl Ingredient for Latest {
    fn ancestors(&self) -> Vec<NodeIndex> {
        vec![self.src]
    }

    fn should_materialize(&self) -> bool {
        true
    }

    fn will_query(&self, _: bool) -> bool {
        true // because the latest may be retracted
    }

    fn on_connected(&mut self, _: &Graph) {}

    fn on_commit(&mut self, us: NodeIndex, remap: &HashMap<NodeIndex, NodeIndex>) {
        self.us = us;
        self.src = remap[&self.src]
    }

    fn on_input(&mut self, input: Message, _: &DomainNodes, state: &StateMap) -> Option<Update> {
        debug_assert_eq!(input.from, self.src);

        match input.data {
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
                    let group: Vec<_> = self.key.iter().map(|&col| r[col].clone()).collect();
                    handled.insert(group);

                    let current = {
                        let r = r.rec();

                        // Construct the query we'll need to query into ourselves to find current latest
                        let q =
                            self.key
                                .iter()
                                .map(|&col| {
                                    shortcut::Condition {
                                column: col,
                                cmp: shortcut::Comparison::Equal(shortcut::Value::using(&r[col])),
                            }
                                })
                                .collect::<Vec<_>>();


                        // find the current value for this group
                        match state.get(&self.us) {
                            Some(db) => {
                                let mut rs = db.find(&q[..]);
                                let cur = rs.next()
                                    .map(|r| (r.into_iter().cloned().collect(), 0));
                                assert_eq!(rs.count(), 0, "latest group has more than 1 record");
                                cur

                            }
                            None => {
                                // TODO: query ancestor (self.query?) based on self.key columns
                                unimplemented!()
                            }
                        }
                    };

                    // if there was a previous latest for this key, revoke old record
                    if let Some(current) = current {
                        out.push(ops::Record::Negative(current.0));
                    }
                    out.push(r);
                }

                // check that there aren't any standalone negatives
                // XXX: this check actually incurs a decent performance hit, as it causes us to
                // have to clone above, plus do this loop. maybe just kill it?
                for r in neg {
                    // we can swap_remove here because we know self.keys is in reverse sorted order
                    let (mut r, _) = r.extract();
                    let group: Vec<_> = self.key.iter().map(|&i| r.swap_remove(i)).collect();
                    assert!(handled.contains(&group));
                }

                ops::Update::Records(out).into()
            }
        }
    }

    fn query(&self,
             q: Option<&query::Query>,
             domain: &DomainNodes,
             states: &StateMap)
             -> ops::Datas {
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
                                  .take(domain[&self.src].borrow().fields().len())
                                  .collect::<Vec<_>>(),
                              ps)
        });

        // now, query our ancestor, and aggregate into groups.
        let rx = if let Some(state) = states.get(&self.src) {
            // other node is materialized
            state.find(q.as_ref().map(|q| &q.having[..]).unwrap_or(&[]))
                .map(|r| r.iter().cloned().collect())
                .collect()
        } else {
            // other node is not materialized, query instead
            domain[&self.src].borrow().query(q.as_ref(), domain, states)
        };

        // FIXME: having an order by would be nice here, so that we didn't have to keep the entire
        // aggregated state in memory until we've seen all rows.
        let mut consolidate = HashMap::<_, Vec<_>>::new();
        for rec in rx {
            use std::collections::hash_map::Entry;

            let (group, rest): (Vec<_>, _) =
                rec.into_iter().enumerate().partition(|&(ref fi, _)| self.key_m.contains_key(fi));
            assert_eq!(group.len(), self.key.len());

            let group = group.into_iter().map(|(_, v)| v).collect();
            match consolidate.entry(group) {
                Entry::Occupied(mut e) => {
                    let e = e.get_mut();
                    // TODO: only if newer
                    e.clear();
                    e.extend(rest.into_iter().map(|(_, v)| v));
                }
                Entry::Vacant(v) => {
                    v.insert(rest.into_iter().map(|(_, v)| v).collect());
                }
            }
        }

        consolidate.into_iter()
            .map(|(group, rest): (Vec<_>, Vec<_>)| {
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
                row
            })
            .collect()
    }

    fn suggest_indexes(&self, this: NodeIndex) -> HashMap<NodeIndex, Vec<usize>> {
        // index all key columns
        Some((this, self.key.clone())).into_iter().collect()
    }

    fn resolve(&self, col: usize) -> Option<Vec<(NodeIndex, usize)>> {
        Some(vec![(self.src, col)])
    }

    fn description(&self) -> String {
        let key_cols = self.key
            .iter()
            .map(|k| k.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        format!("⧖ γ[{}]", key_cols)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use ops;
    use query;
    use shortcut;

    fn setup(key: Vec<usize>, mat: bool) -> ops::test::MockGraph {
        let mut g = ops::test::MockGraph::new();

        let big = key.len() > 1;
        let s = if big {
            g.add_base("source", &["x", "y", "z"])
        } else {
            g.add_base("source", &["x", "y"])
        };
        if big {
            g.seed(s, vec![1.into(), 1.into(), 1.into()]);
            g.seed(s, vec![2.into(), 1.into(), 1.into()]);
            g.seed(s, vec![2.into(), 2.into(), 1.into()]);
            g.seed(s, vec![1.into(), 2.into(), 1.into()]);
            g.seed(s, vec![3.into(), 3.into(), 1.into()]);
        } else {
            g.seed(s, vec![1.into(), 1.into()]);
            g.seed(s, vec![2.into(), 1.into()]);
            g.seed(s, vec![2.into(), 2.into()]);
            g.seed(s, vec![1.into(), 2.into()]);
            g.seed(s, vec![3.into(), 3.into()]);
        }
        // TODO: test when last *isn't* latest!

        if big {
            g.set_op("latest", &["x", "y", "z"], Latest::new(s, key));
        } else {
            g.set_op("latest", &["x", "y"], Latest::new(s, key));
        }
        if mat {
            g.set_materialized();
        }
        g
    }

    #[test]
    fn it_describes() {
        let c = setup(vec![0, 2], false);
        assert_eq!(c.node().description(), "⧖ γ[2, 0]");
    }

    #[test]
    fn it_forwards() {
        let mut c = setup(vec![0], true);

        let u = vec![1.into(), 1.into()];

        // first record for a group should emit just a positive
        let out = c.narrow_one_row(u, true);
        if let Some(ops::Update::Records(rs)) = out {
            assert_eq!(rs.len(), 1);
            let mut rs = rs.into_iter();

            match rs.next().unwrap() {
                ops::Record::Positive(r) => {
                    assert_eq!(r[0], 1.into());
                    assert_eq!(r[1], 1.into());
                }
                _ => unreachable!(),
            }
        } else {
            unreachable!();
        }

        let u = vec![2.into(), 2.into()];

        // first record for a second group should also emit just a positive
        let out = c.narrow_one_row(u, true);
        if let Some(ops::Update::Records(rs)) = out {
            assert_eq!(rs.len(), 1);
            let mut rs = rs.into_iter();

            match rs.next().unwrap() {
                ops::Record::Positive(r) => {
                    assert_eq!(r[0], 2.into());
                    assert_eq!(r[1], 2.into());
                }
                _ => unreachable!(),
            }
        } else {
            unreachable!();
        }

        let u = vec![1.into(), 2.into()];

        // new record for existing group should revoke the old latest, and emit the new
        let out = c.narrow_one_row(u, true);
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

        let u = ops::Update::Records(vec![ops::Record::Negative(vec![1.into(), 1.into()]),
                                          ops::Record::Negative(vec![1.into(), 2.into()]),
                                          ops::Record::Positive(vec![1.into(), 3.into()]),
                                          ops::Record::Negative(vec![2.into(), 2.into()]),
                                          ops::Record::Positive(vec![2.into(), 4.into()])]);

        // negatives and positives should still result in only one new current for each group
        let out = c.narrow_one(u, true);
        if let Some(ops::Update::Records(rs)) = out {
            assert_eq!(rs.len(), 4); // one - and one + for each group
            // group 1 lost 2 and gained 3
            assert!(rs.iter().any(|r| if let ops::Record::Negative(ref r) = *r {
                r[0] == 1.into() && r[1] == 2.into()
            } else {
                false
            }));
            assert!(rs.iter().any(|r| if let ops::Record::Positive(ref r) = *r {
                r[0] == 1.into() && r[1] == 3.into()
            } else {
                false
            }));
            // group 2 lost 2 and gained 4
            assert!(rs.iter().any(|r| if let ops::Record::Negative(ref r) = *r {
                r[0] == 2.into() && r[1] == 2.into()
            } else {
                false
            }));
            assert!(rs.iter().any(|r| if let ops::Record::Positive(ref r) = *r {
                r[0] == 2.into() && r[1] == 4.into()
            } else {
                false
            }));
        } else {
            unreachable!();
        }
    }

    #[test]
    fn it_forwards_mkey() {
        let mut c = setup(vec![0, 1], true);

        let u = vec![1.into(), 1.into(), 1.into()];
        c.narrow_one_row(u, true);

        // first record for a second group should also emit just a positive
        let u = vec![1.into(), 2.into(), 2.into()];

        let out = c.narrow_one_row(u, true);
        if let Some(ops::Update::Records(rs)) = out {
            assert_eq!(rs.len(), 1);
            let mut rs = rs.into_iter();

            match rs.next().unwrap() {
                ops::Record::Positive(r) => {
                    assert_eq!(r[0], 1.into());
                    assert_eq!(r[1], 2.into());
                    assert_eq!(r[2], 2.into());
                }
                _ => unreachable!(),
            }
        } else {
            unreachable!();
        }

        let u = vec![1.into(), 1.into(), 2.into()];

        // new record for existing group should revoke the old latest, and emit the new
        let out = c.narrow_one_row(u, true);
        if let Some(ops::Update::Records(rs)) = out {
            assert_eq!(rs.len(), 2);
            let mut rs = rs.into_iter();

            match rs.next().unwrap() {
                ops::Record::Negative(r) => {
                    assert_eq!(r[0], 1.into());
                    assert_eq!(r[1], 1.into());
                    assert_eq!(r[2], 1.into());
                }
                _ => unreachable!(),
            }
            match rs.next().unwrap() {
                ops::Record::Positive(r) => {
                    assert_eq!(r[0], 1.into());
                    assert_eq!(r[1], 1.into());
                    assert_eq!(r[2], 2.into());
                }
                _ => unreachable!(),
            }
        } else {
            unreachable!();
        }

        let u =
            ops::Update::Records(vec![ops::Record::Negative(vec![1.into(), 1.into(), 1.into()]),
                                      ops::Record::Negative(vec![1.into(), 1.into(), 2.into()]),
                                      ops::Record::Positive(vec![1.into(), 1.into(), 3.into()]),
                                      ops::Record::Negative(vec![1.into(), 2.into(), 2.into()]),
                                      ops::Record::Positive(vec![1.into(), 2.into(), 4.into()])]);

        // negatives and positives should still result in only one new current for each group
        let out = c.narrow_one(u, true);
        if let Some(ops::Update::Records(rs)) = out {
            assert_eq!(rs.len(), 4); // one - and one + for each group
            // group 1 lost 2 and gained 3
            assert!(rs.iter().any(|r| if let ops::Record::Negative(ref r) = *r {
                r[1] == 1.into() && r[2] == 2.into()
            } else {
                false
            }));
            assert!(rs.iter().any(|r| if let ops::Record::Positive(ref r) = *r {
                r[1] == 1.into() && r[2] == 3.into()
            } else {
                false
            }));
            // group 2 lost 2 and gained 4
            assert!(rs.iter().any(|r| if let ops::Record::Negative(ref r) = *r {
                r[1] == 2.into() && r[2] == 2.into()
            } else {
                false
            }));
            assert!(rs.iter().any(|r| if let ops::Record::Positive(ref r) = *r {
                r[1] == 2.into() && r[2] == 4.into()
            } else {
                false
            }));
        } else {
            unreachable!();
        }
    }

    #[test]
    fn it_queries() {
        let l = setup(vec![0], false);

        let hits = l.query(None);
        assert_eq!(hits.len(), 3);
        assert!(hits.iter().any(|r| r[0] == 1.into() && r[1] == 2.into()));
        assert!(hits.iter().any(|r| r[0] == 2.into() && r[1] == 2.into()));
        assert!(hits.iter().any(|r| r[0] == 3.into() && r[1] == 3.into()));

        let val = shortcut::Comparison::Equal(shortcut::Value::new(query::DataType::from(2)));
        let q = query::Query::new(&[true, true],
                                  vec![shortcut::Condition {
                                           column: 0,
                                           cmp: val,
                                       }]);

        let hits = l.query(Some(&q));
        assert_eq!(hits.len(), 1);
        assert!(hits.iter().any(|r| r[0] == 2.into() && r[1] == 2.into()));
    }

    #[test]
    fn it_suggests_indices() {
        let c = setup(vec![0, 1], false);
        let idx = c.node().suggest_indexes(1.into());

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
        assert_eq!(c.node().resolve(0), Some(vec![(c.narrow_base_id(), 0)]));
        assert_eq!(c.node().resolve(1), Some(vec![(c.narrow_base_id(), 1)]));
        assert_eq!(c.node().resolve(2), Some(vec![(c.narrow_base_id(), 2)]));
    }
}
