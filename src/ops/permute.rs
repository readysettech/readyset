use ops;
use query;
use shortcut;

use std::collections::HashMap;
use std::iter;

use flow::prelude::*;

/// Permutes or omits columns from its source node.
#[derive(Debug)]
pub struct Permute {
    emit: Option<Vec<usize>>,
    src: NodeIndex,
    cols: usize,
    us: NodeIndex,
}

impl Permute {
    /// Construct a new permuter operator.
    pub fn new(src: NodeIndex, emit: &[usize]) -> Permute {
        Permute {
            emit: Some(emit.into()),
            src: src,
            cols: 0,
            us: 0.into(),
        }
    }

    fn resolve_col(&self, col: usize) -> usize {
        self.emit.as_ref().map_or(col, |emit| emit[col])
    }

    fn permute(&self, data: &mut Vec<query::DataType>) {
        if let Some(ref emit) = self.emit {
            use std::iter;
            // http://stackoverflow.com/a/1683662/472927
            // TODO: compute the swaps in advance instead
            let mut done: Vec<_> = iter::repeat(false).take(self.cols).collect();
            for mut i in 0..emit.len() {
                // all data[i' < i] are in the right place
                if done[i] {
                    // data[i] has been made correct in previou swap cycle
                    continue;
                }
                if emit[i] == i {
                    // data[i] does not need to be swapped
                    continue;
                }

                // if we reach here, this means that we're supposed to hold the data from some
                // position i' = self.emit[i] at data[i]. but, we first need to make sure that
                // no-one else is trying to use the current value at data[i] (since we'd be
                // overwriting it). we *could* do a scan through all i' > i, but that can get quite
                // expensive. instead, we're going to do the following:
                //
                //  - swap the value at our position (j = i) with the one we want (emit[j]).
                //    emit[j] will thus hold the original data[i].
                //  - if output j wanted the value at i, we're done
                //  - if not, swap again
                //
                // as we do this, we also mark off the swaps we do. for each j we pass in the loop
                // above, we've put the correct value into data[j], so we should't swap again when
                // we get around to i == j.
                //
                // there is one bit of trickery compared to the original algorithm, and that is
                // that not all columns may be emitted. if we reach the case where j is beyond the
                // boundaries of emit, then what do we do? we can't just stop iterating, because
                // now data[i] will be at data[j], but someone might still need data[i]!
                // in this case, the only thing we can really do is *search* for the i' such that
                // emit[i'] = i. if there is one, then we swap it data[j] directly there. if there
                // isn't, that's great! we can just leave data[j] (which is data[i]) right where it
                // is -- in some remote location that no-one will look at again.

                // the original pseudocode calls for
                //
                //   let t = data[i].clone();
                //
                // but we can instead just swap this element along as we traverse the cycles. this
                // works if we assume that each source index is only emitted once.
                let mut j = i;
                loop {
                    done[j] = true;
                    if j >= emit.len() {
                        // this is the tricky situation described above.
                        // we need to search for the thing that emits data[i] (if any)
                        let mut swapped = false;
                        for k in (i + 1)..emit.len() {
                            if emit[k] == i {
                                done[k] = true;
                                data.swap(k, j);
                                swapped = true;
                                // now we're in another tough spot. we've given data[i] to data[k],
                                // as desired, but data[k] is now in data[j]! what if someone
                                // wanted data[k]? well, we're in luck. we have effectively just
                                // finished with i -- it has been assigned to its final
                                // destination, and everyone along the cycle there has been given
                                // the right value. we can therefore act as though we just stepped
                                // to j with i = k instead!
                                i = k;
                                break;
                            }
                        }

                        // note that if we never emit data[i], we also don't need to do any swaps
                        if swapped {
                            // still more work to do
                        } else {
                            break;
                        }
                    } else if emit[j] != i {
                        // we are supposed to put another column here. put that other value here,
                        // and then figure out what goes there to replace what we just took.
                        // the original pseudocode calls for
                        //
                        //   data[j] = data[emit[j]].clone();
                        //
                        // but this shouldn't be necessary if we assume that each source index is
                        // only emitted once.
                        data.swap(j, emit[j]);
                        j = emit[j];
                    } else {
                        // the original value is supposed to go here, and since we've been swapping
                        // values as we went, it's already there! the cycle is complete!
                        // the original pseudocode calls for
                        //
                        //   data[j] = t;
                        break;
                    }
                }
            }
            data.truncate(emit.len());
        }
    }
}

impl Ingredient for Permute {
    fn ancestors(&self) -> Vec<NodeIndex> {
        vec![self.src]
    }

    fn should_materialize(&self) -> bool {
        false
    }

    fn will_query(&self, materialized: bool) -> bool {
        !materialized
    }

    fn on_connected(&mut self, g: &Graph) {
        self.cols = g[self.src].fields().len();
    }

    fn on_commit(&mut self, us: NodeIndex, remap: &HashMap<NodeIndex, NodeIndex>) {
        self.us = us;
        self.src = remap[&self.src];

        // Eliminate emit specifications which require no permutation of
        // the inputs, so we don't needlessly perform extra work on each
        // update.
        self.emit = self.emit.take().and_then(|emit| {
            let complete = emit.len() == self.cols;
            let sequential = emit.iter().enumerate().all(|(i, &j)| i == j);
            if complete && sequential {
                None
            } else {
                Some(emit)
            }
        });
    }

    fn on_input(&mut self, mut input: Message, _: &NodeList, _: &StateMap) -> Option<Update> {
        debug_assert_eq!(input.from, self.src);

        if self.emit.is_some() {
            match input.data {
                ops::Update::Records(ref mut rs) => {
                    for r in rs {
                        self.permute(r);
                    }
                }
            }
        }
        input.data.into()
    }

    fn query(&self, q: Option<&query::Query>, domain: &NodeList, states: &StateMap) -> ops::Datas {
        use shortcut::cmp::Comparison::Equal;
        use shortcut::cmp::Value::{Const, Column};

        // TODO: We don't need to select all fields if our permutation
        // drops some fields--`self.permute` will end up dropping them
        // anyway--but it's not worth the trouble.
        let select = iter::repeat(true)
            .take(domain.lookup(self.src).fields().len())
            .collect::<Vec<_>>();

        let q = q.map(|q| {
            let having = q.having.iter().map(|c| {
                shortcut::Condition {
                    column: self.resolve_col(c.column),
                    cmp: match c.cmp {
                        Equal(Const(_)) => c.cmp.clone(),
                        Equal(Column(idx)) => Equal(Column(self.resolve_col(idx))),
                    },
                }
            });
            query::Query::new(&select, having.collect())
        });

        let mut rx = if let Some(state) = states.get(&self.src) {
            // other node is materialized
            state.find(q.as_ref().map(|q| &q.having[..]).unwrap_or(&[]))
                .map(|r| r.iter().cloned().collect())
                .collect()
        } else {
            // other node is not materialized, query instead
            domain.lookup(self.src).query(q.as_ref(), domain, states)
        };

        if self.emit.is_some() {
            for r in rx.iter_mut() {
                self.permute(r);
            }
        }
        rx
    }

    fn suggest_indexes(&self, _: NodeIndex) -> HashMap<NodeIndex, Vec<usize>> {
        // TODO
        HashMap::new()
    }

    fn resolve(&self, col: usize) -> Option<Vec<(NodeIndex, usize)>> {
        Some(vec![(self.src, self.resolve_col(col))])
    }

    fn description(&self) -> String {
        let emit_cols = match self.emit.as_ref() {
            None => "*".into(),
            Some(emit) => {
                emit.iter()
                    .map(|e| e.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            }
        };
        format!("π[{}]", emit_cols)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use ops;
    use query;
    use shortcut;

    fn setup(materialized: bool, all: bool) -> ops::test::MockGraph {
        let mut g = ops::test::MockGraph::new();
        let s = g.add_base("source", &["x", "y", "z"]);
        g.seed(s, vec![1.into(), 0.into(), 1.into()]);
        g.seed(s, vec![2.into(), 0.into(), 1.into()]);
        g.seed(s, vec![2.into(), 0.into(), 2.into()]);

        let permutation = if all { vec![0, 1, 2] } else { vec![2, 0] };
        g.set_op("permute",
                 &["x", "y", "z"],
                 Permute::new(s, &permutation[..]));
        if materialized {
            g.set_materialized();
        }
        g
    }

    #[test]
    fn it_describes() {
        let p = setup(false, false);
        assert_eq!(p.node().description(), "π[2, 0]");
    }

    #[test]
    fn it_describes_all() {
        let p = setup(false, true);
        assert_eq!(p.node().description(), "π[*]");
    }

    #[test]
    fn it_forwards_some() {
        let mut p = setup(false, false);

        let rec = vec!["a".into(), "b".into(), "c".into()];
        match p.narrow_one_row(rec, false).unwrap() {
            ops::Update::Records(rs) => {
                assert_eq!(rs,
                           vec![ops::Record::Positive(vec!["c".into(), "a".into()])]);
            }
        }
    }

    #[test]
    fn it_forwards_all() {
        let mut p = setup(false, true);

        let rec = vec!["a".into(), "b".into(), "c".into()];
        match p.narrow_one_row(rec, false).unwrap() {
            ops::Update::Records(rs) => {
                assert_eq!(rs,
                           vec![ops::Record::Positive(vec!["a".into(), "b".into(), "c".into()])]);
            }
        }
    }

    #[test]
    fn it_queries() {
        let p = setup(false, false);

        let hits = p.query(None);
        assert_eq!(hits.len(), 3);
        assert!(hits.iter().any(|r| r[0] == 1.into() && r[1] == 1.into()));
        assert!(hits.iter().any(|r| r[0] == 1.into() && r[1] == 2.into()));
        assert!(hits.iter().any(|r| r[0] == 2.into() && r[1] == 2.into()));

        let q = query::Query::new(&[true, true],
                                  vec![shortcut::Condition {
                             column: 1,
                             cmp: shortcut::Comparison::Equal(shortcut::Value::Const(2.into())),
                         }]);

        let hits = p.query(Some(&q));
        assert_eq!(hits.len(), 2);
        assert!(hits.iter().any(|r| r[0] == 1.into() && r[1] == 2.into()));
        assert!(hits.iter().any(|r| r[0] == 2.into() && r[1] == 2.into()));

        let q = query::Query::new(&[true, true],
                                  vec![shortcut::Condition {
                             column: 0,
                             cmp: shortcut::Comparison::Equal(shortcut::Value::Column(1)),
                         }]);

        let hits = p.query(Some(&q));
        assert_eq!(hits.len(), 2);
        assert!(hits.iter().any(|r| r[0] == 1.into() && r[1] == 1.into()));
        assert!(hits.iter().any(|r| r[0] == 2.into() && r[1] == 2.into()));
    }

    #[test]
    fn it_queries_all() {
        let p = setup(false, true);

        let q = query::Query::new(&[true, true, true],
                                  vec![shortcut::Condition {
                             column: 0,
                             cmp: shortcut::Comparison::Equal(shortcut::Value::Const(2.into())),
                         }]);

        let hits = p.query(Some(&q));
        assert_eq!(hits.len(), 2);
        assert!(hits.iter()
            .any(|r| r[0] == 2.into() && r[1] == 0.into() && r[2] == 1.into()));
        assert!(hits.iter()
            .any(|r| r[0] == 2.into() && r[1] == 0.into() && r[2] == 2.into()));

        let q = query::Query::new(&[true, true, true],
                                  vec![shortcut::Condition {
                             column: 0,
                             cmp: shortcut::Comparison::Equal(shortcut::Value::Column(2)),
                         }]);

        let hits = p.query(Some(&q));
        assert_eq!(hits.len(), 2);
        assert!(hits.iter()
            .any(|r| r[0] == 1.into() && r[1] == 0.into() && r[2] == 1.into()));
        assert!(hits.iter()
            .any(|r| r[0] == 2.into() && r[1] == 0.into() && r[2] == 2.into()));
    }

    #[test]
    fn it_suggests_indices() {
        let p = setup(false, false);
        let idx = p.node().suggest_indexes(1.into());
        assert_eq!(idx.len(), 0);
    }

    #[test]
    fn it_resolves() {
        let p = setup(false, false);
        assert_eq!(p.node().resolve(0), Some(vec![(p.narrow_base_id(), 2)]));
        assert_eq!(p.node().resolve(1), Some(vec![(p.narrow_base_id(), 0)]));
    }

    #[test]
    fn it_resolves_all() {
        let p = setup(false, true);
        assert_eq!(p.node().resolve(0), Some(vec![(p.narrow_base_id(), 0)]));
        assert_eq!(p.node().resolve(1), Some(vec![(p.narrow_base_id(), 1)]));
        assert_eq!(p.node().resolve(2), Some(vec![(p.narrow_base_id(), 2)]));
    }
}
