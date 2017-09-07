use std::collections::HashMap;

use flow::prelude::*;

/// Latest provides an operator that will maintain the last record for every group.
///
/// Whenever a new record arrives for a group, the latest operator will negative the previous
/// latest for that group.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Latest {
    us: Option<IndexPair>,
    src: IndexPair,
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
        assert_eq!(
            keys.len(),
            1,
            "only latest over a single column is supported"
        );
        keys.sort();
        let key_m = keys.clone()
            .into_iter()
            .enumerate()
            .map(|(idx, col)| (col, idx))
            .collect();
        keys.reverse();
        Latest {
            us: None,
            src: src.into(),
            key: keys,
            key_m: key_m,
        }
    }
}

impl Ingredient for Latest {
    fn take(&mut self) -> NodeOperator {
        Clone::clone(self).into()
    }

    fn ancestors(&self) -> Vec<NodeIndex> {
        vec![self.src.as_global()]
    }

    fn on_connected(&mut self, _: &Graph) {}

    fn on_commit(&mut self, us: NodeIndex, remap: &HashMap<NodeIndex, IndexPair>) {
        self.src.remap(remap);
        self.us = Some(remap[&us]);
    }

    fn on_input(
        &mut self,
        from: LocalNodeIndex,
        rs: Records,
        _: &mut Tracer,
        _: &DomainNodes,
        state: &StateMap,
    ) -> ProcessingResult {
        debug_assert_eq!(from, *self.src);

        // find the current value for each group
        let us = self.us.unwrap();
        let db = state
            .get(&us)
            .expect("latest must have its own state materialized");

        let mut misses = Vec::new();
        let mut out = Vec::with_capacity(rs.len());
        {
            let currents = rs.into_iter().filter_map(|r| {
            // We don't allow standalone negatives as input to a latest. This is because it would
            // be very computationally expensive (and currently impossible) to find what the
            // *previous* latest was if the current latest was revoked.
            if !r.is_positive() {
                return None;
            }

            match db.lookup(&[self.key[0]], &KeyType::Single(&r[self.key[0]])) {
                LookupResult::Some(rs) => {
                    debug_assert!(rs.len() <= 1, "a group had more than 1 result");
                    Some((r, rs.get(0)))
                }
                LookupResult::Missing => {
                    // we don't actively materialize holes unless requested by a read. this can't
                    // be a read, because reads cause replay, which fill holes with an empty set
                    // before processing!
                    misses.push(Miss{
                        node: *us,
                        key: vec![r[self.key[0]].clone()],
                    });
                    None
                }
            }
        });

            // buffer emitted records
            for (r, current) in currents {
                if let Some(current) = current {
                    out.push(Record::Negative((**current).clone()));
                }

                // if there was a previous latest for this key, revoke old record
                out.push(r);
            }
        }

        // TODO: check that there aren't any standalone negatives

        ProcessingResult {
            results: out.into(),
            misses: misses,
        }
    }

    fn suggest_indexes(&self, this: NodeIndex) -> HashMap<NodeIndex, (Vec<usize>, bool)> {
        // index all key columns
        Some((this, (self.key.clone(), true))).into_iter().collect()
    }

    fn resolve(&self, col: usize) -> Option<Vec<(NodeIndex, usize)>> {
        Some(vec![(self.src.as_global(), col)])
    }

    fn description(&self) -> String {
        let key_cols = self.key
            .iter()
            .map(|k| k.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        format!("⧖ γ[{}]", key_cols)
    }

    fn parent_columns(&self, column: usize) -> Vec<(NodeIndex, Option<usize>)> {
        vec![(self.src.as_global(), Some(column))]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use ops;

    fn setup(key: usize, mat: bool) -> ops::test::MockGraph {
        let mut g = ops::test::MockGraph::new();
        let s = g.add_base("source", &["x", "y"]);
        g.set_op(
            "latest",
            &["x", "y"],
            Latest::new(s.as_global(), vec![key]),
            mat,
        );
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
        let rs = c.narrow_one_row(u, true);
        assert_eq!(rs.len(), 1);
        let mut rs = rs.into_iter();

        match rs.next().unwrap() {
            Record::Positive(r) => {
                assert_eq!(r[0], 1.into());
                assert_eq!(r[1], 1.into());
            }
            _ => unreachable!(),
        }

        let u = vec![2.into(), 2.into()];

        // first record for a second group should also emit just a positive
        let rs = c.narrow_one_row(u, true);
        assert_eq!(rs.len(), 1);
        let mut rs = rs.into_iter();

        match rs.next().unwrap() {
            Record::Positive(r) => {
                assert_eq!(r[0], 2.into());
                assert_eq!(r[1], 2.into());
            }
            _ => unreachable!(),
        }

        let u = vec![1.into(), 2.into()];

        // new record for existing group should revoke the old latest, and emit the new
        let rs = c.narrow_one_row(u, true);
        assert_eq!(rs.len(), 2);
        let mut rs = rs.into_iter();

        match rs.next().unwrap() {
            Record::Negative(r) => {
                assert_eq!(r[0], 1.into());
                assert_eq!(r[1], 1.into());
            }
            _ => unreachable!(),
        }
        match rs.next().unwrap() {
            Record::Positive(r) => {
                assert_eq!(r[0], 1.into());
                assert_eq!(r[1], 2.into());
            }
            _ => unreachable!(),
        }

        let u = vec![
            (vec![1.into(), 1.into()], false),
            (vec![1.into(), 2.into()], false),
            (vec![1.into(), 3.into()], true),
            (vec![2.into(), 2.into()], false),
            (vec![2.into(), 4.into()], true),
        ];

        // negatives and positives should still result in only one new current for each group
        let rs = c.narrow_one(u, true);
        assert_eq!(rs.len(), 4); // one - and one + for each group
                                 // group 1 lost 2 and gained 3
        assert!(rs.iter().any(|r| if let Record::Negative(ref r) = *r {
            r[0] == 1.into() && r[1] == 2.into()
        } else {
            false
        }));
        assert!(rs.iter().any(|r| if let Record::Positive(ref r) = *r {
            r[0] == 1.into() && r[1] == 3.into()
        } else {
            false
        }));
        // group 2 lost 2 and gained 4
        assert!(rs.iter().any(|r| if let Record::Negative(ref r) = *r {
            r[0] == 2.into() && r[1] == 2.into()
        } else {
            false
        }));
        assert!(rs.iter().any(|r| if let Record::Positive(ref r) = *r {
            r[0] == 2.into() && r[1] == 4.into()
        } else {
            false
        }));
    }

    #[test]
    fn it_suggests_indices() {
        let me = 1.into();
        let c = setup(1, false);
        let idx = c.node().suggest_indexes(me);

        // should only add index on own columns
        assert_eq!(idx.len(), 1);
        assert!(idx.contains_key(&me));

        // should only index on the group-by column
        assert_eq!(idx[&me], (vec![1], true));
    }


    #[test]
    fn it_resolves() {
        let c = setup(1, false);
        assert_eq!(
            c.node().resolve(0),
            Some(vec![(c.narrow_base_id().as_global(), 0)])
        );
        assert_eq!(
            c.node().resolve(1),
            Some(vec![(c.narrow_base_id().as_global(), 1)])
        );
        assert_eq!(
            c.node().resolve(2),
            Some(vec![(c.narrow_base_id().as_global(), 2)])
        );
    }
}
