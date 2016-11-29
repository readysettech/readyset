use ops;

use std::collections::HashMap;
use std::collections::HashSet;

use flow::prelude::*;

/// Latest provides an operator that will maintain the last record for every group.
///
/// Whenever a new record arrives for a group, the latest operator will negative the previous
/// latest for that group.
#[derive(Debug)]
pub struct Latest {
    us: Option<NodeAddress>,
    src: NodeAddress,
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
    pub fn new(src: NodeAddress, mut keys: Vec<usize>) -> Latest {
        assert_eq!(keys.len(),
                   1,
                   "only latest over a single column is supported");
        keys.sort();
        let key_m = keys.clone().into_iter().enumerate().map(|(idx, col)| (col, idx)).collect();
        keys.reverse();
        Latest {
            us: None,
            src: src,
            key: keys,
            key_m: key_m,
        }
    }
}

impl Ingredient for Latest {
    fn ancestors(&self) -> Vec<NodeAddress> {
        vec![self.src]
    }

    fn should_materialize(&self) -> bool {
        true
    }

    fn will_query(&self, _: bool) -> bool {
        true // because the latest may be retracted
    }

    fn on_connected(&mut self, _: &Graph) {}

    fn on_commit(&mut self, us: NodeAddress, remap: &HashMap<NodeAddress, NodeAddress>) {
        self.us = Some(us);
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

                        // find the current value for this group
                        let db = state.get(&self.us.as_ref().unwrap().as_local())
                            .expect("latest must have its own state materialized");
                        let rs = db.lookup(self.key[0], &r[self.key[0]]);
                        debug_assert!(rs.len() <= 1, "a group had more than 1 result");
                        rs.get(0).map(|r| (r.into_iter().cloned().collect(), 0))
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

    fn suggest_indexes(&self, this: NodeAddress) -> HashMap<NodeAddress, usize> {
        // index all key columns
        Some((this, self.key[0])).into_iter().collect()
    }

    fn resolve(&self, col: usize) -> Option<Vec<(NodeAddress, usize)>> {
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

    fn setup(key: usize, mat: bool) -> ops::test::MockGraph {
        let mut g = ops::test::MockGraph::new();
        let s = g.add_base("source", &["x", "y"]);
        g.set_op("latest", &["x", "y"], Latest::new(s, vec![key]), mat);
        g
    }

    // TODO: test when last *isn't* latest!

    #[test]
    fn it_describes() {
        let c = setup(0, false);
        assert_eq!(c.node().description(), "⧖ γ[0]");
    }

    #[test]
    fn it_forwards() {
        let mut c = setup(0, true);

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
    fn it_suggests_indices() {
        let me = NodeAddress::mock_global(1.into());
        let c = setup(1, false);
        let idx = c.node().suggest_indexes(me);

        // should only add index on own columns
        assert_eq!(idx.len(), 1);
        assert!(idx.contains_key(&me));

        // should only index on the group-by column
        assert_eq!(idx[&me], 1);
    }


    #[test]
    fn it_resolves() {
        let c = setup(1, false);
        assert_eq!(c.node().resolve(0), Some(vec![(c.narrow_base_id(), 0)]));
        assert_eq!(c.node().resolve(1), Some(vec![(c.narrow_base_id(), 1)]));
        assert_eq!(c.node().resolve(2), Some(vec![(c.narrow_base_id(), 2)]));
    }
}
