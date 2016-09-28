use ops;
use flow;
use query;
use backlog;
use ops::NodeOp;
use ops::NodeType;

use std::iter;
use std::collections::HashMap;

use shortcut;

/// Joiner provides a 2-way join between two views.
///
/// It shouldn't be *too* hard to extend this to `n`-way joins, but it would require restructuring
/// `.join` such that it can express "query this view first, then use one of its columns to query
/// this other view".
#[derive(Debug)]
pub struct Joiner {
    emit: Vec<(flow::NodeIndex, usize)>,
    join: HashMap<flow::NodeIndex, HashMap<flow::NodeIndex, Vec<(usize, usize)>>>,
    nodes: HashMap<flow::NodeIndex, ops::V>,
}

impl Joiner {
    /// Construct a new join operator.
    ///
    /// Joins are currently somewhat verbose to construct, though the process isn't too complex.
    /// Let us look at a SQL join such as
    ///
    /// ```sql
    /// SELECT a.0, b.0
    /// FROM a JOIN b USING (a.0 == b.1)
    /// ```
    ///
    /// First, we construct the `emit` argument by indicating, for each output, which source and
    /// column should be used. `vec![(a, 0), (b, 1)]` in this case.
    ///
    /// Next, we have to tell the `Joiner` how to construct an output row given an input row of any
    /// type. For each input view, we construct a list of the same length as the number of columns
    /// of that view, where each element is either 0 or a *group number*. Columns which share a
    /// group number are used to join nodes by equality. Thus, each group number can appear at most
    /// once for each view. 
    ///
    /// In the above example, assuming `a` has two columns and `b` has three, the map would look
    /// like this:
    ///
    /// ```rust,ignore
    /// map.insert(a, vec![1, 0]);
    /// map.insert(b, vec![0, 1, 0]);
    /// ```
    pub fn new(emit: Vec<(flow::NodeIndex, usize)>,
               join: HashMap<flow::NodeIndex, Vec<usize>>)
               -> Joiner {

        if join.len() != 2 {
            // only two-way joins are currently supported
            unimplemented!();
        }

        // we technically want this assert, but we don't have self.nodes until .prime() has been
        // called. unfortunately, at that time, we don't have .join in the original format, and so
        // the debug doesn't makes sense. it's probably not worth carrying along the original join
        // map just to verify this, but maybe...
        // assert!(self.nodes.iter().all(|(ni, n)| self.join[ni].len() == n.args().len()));

        // the format of `join` is convenient for users, but not particulary convenient for lookups
        // the particular use-case we want to be efficient is:
        //
        //  - we are given a record from `src`
        //  - for each other parent `p`, we want to know which columns of `p` to constrain, and
        //    which values in the `src` record those correspond to
        // 
        // so, we construct a map of the form
        //
        //   src: NodeIndex => {
        //     p: NodeIndex => [(srci, pi), ...]
        //   }
        //
        let join = join.iter().map(|(&src, srcg)| {
            // which groups are bound to which columns?
            let g2c = srcg.iter().enumerate().filter_map(|(c, &g)| {
                if g == 0 {
                    None
                } else {
                    Some((g, c))
                }
            }).collect::<HashMap<_, _>>();

            // for every other view
            let other = join.iter().filter_map(|(&p, pg)| {
                // *other* view
                if p == src {
                    return None;
                }
                // look through the group assignments for that other view
                let pg: Vec<_> = pg.iter().enumerate().filter_map(|(pi, g)| {
                    // look for ones that share a group with us
                    g2c.get(g).map(|srci| {
                        // and emit that mapping
                        (*srci, pi)
                    })
                }).collect();

                // if there are no shared columns, don't join against this view
                if pg.is_empty() {
                    return None;
                }
                // but if there are, emit the mapping we found
                Some((p, pg))
            }).collect();

            (src, other)
        }).collect();

        Joiner {
            emit: emit,
            join: join,
            nodes: HashMap::new(),
        }
    }

    fn join<'a>(&'a self, left: (flow::NodeIndex, Vec<query::DataType>, i64), ts: i64)
                -> Box<Iterator<Item = (Vec<query::DataType>, i64)> + 'a> {

        // NOTE: this only works for two-way joins
        let on = *self.join.keys().find(|&other| other != &left.0).unwrap();

        // figure out the join values for this record
        let params = self.join[&on][&left.0]
            .iter()
            .map(|&(lefti, righti)| {
                shortcut::Condition{
                    column: righti,
                    cmp: shortcut::Comparison::Equal(shortcut::Value::Const(left.1[lefti].clone())),
                }
            })
            .collect();

        // TODO: technically, we only need the columns in .join and .emit
        let q = query::Query::new(&iter::repeat(true).take(self.nodes[&on].args().len()).collect::<Vec<_>>(), params);

        // send the parameters to start the query.
        let rx = self.nodes[&on].find(Some(q), Some(ts));

        Box::new(rx.into_iter().map(move |(right, rts)| {
            use std::cmp;

            // weave together r and j according to join rules
            let r = self.emit
                .iter()
                .map(|&(source, column)| {
                    if source == on {
                        // FIXME: this clone is unnecessary.
                        // it's tricky to remove though, because it means we'd need to
                        // be removing things from right. what if a later column also needs
                        // to select from right? we'd need to keep track of which things we
                        // have removed, and subtract that many from the index of the
                        // later column. ugh.
                        right[column].clone()
                    } else {
                        left.1[column].clone()
                    }
                })
                .collect();

            // we need to be careful here.
            // we want to emit a record with the *same* timestamp regardless of which side of the
            // join is left and right. this is particularly important when the left is a negative,
            // because we want the resulting negative records to have the same timestamp as the
            // original positive we sent. however, the original positive *could* have been produced
            // by a right, not a left. in that case, the positive has the timestamp of the right!
            // we solve this by making the output timestamp always be the max of the left and
            // right, as this must be the timestamp that resulted in the join output in the first
            // place.
            (r, cmp::max(left.2, rts))
        }))
    }
}

impl From<Joiner> for NodeType {
    fn from(b: Joiner) -> NodeType {
        NodeType::JoinNode(b)
    }
}

impl NodeOp for Joiner {
    fn prime(&mut self, g: &ops::Graph) -> Vec<flow::NodeIndex> {
        self.nodes.extend(self.join.keys().filter_map(|&ni| g[ni].as_ref().map(move |n| (ni, n.clone()))));
        self.join.keys().cloned().collect()
    }

    fn forward(&self,
               u: ops::Update,
               from: flow::NodeIndex,
               ts: i64,
               _: Option<&backlog::BufferedStore>)
               -> Option<ops::Update> {

        match u {
            ops::Update::Records(rs) => {
                // okay, so here's what's going on:
                // the record(s) we receive are all from one side of the join. we need to query the
                // other side(s) for records matching the incoming records on that side's join
                // fields.

                // TODO: we should be clever here, and only query once per *distinct join value*,
                // instead of once per received record.
                Some(ops::Update::Records(rs.into_iter()
                    .flat_map(|rec| {
                        let (r, pos, lts) = rec.extract();

                        self.join((from, r, lts), ts).map(move |(res, ts)| {
                            // return new row with appropriate sign
                            if pos {
                                ops::Record::Positive(res, ts)
                            } else {
                                ops::Record::Negative(res, ts)
                            }
                        })
                    })
                    .collect()))
            }
        }
    }

    fn query(&self, q: Option<&query::Query>, ts: i64) -> ops::Datas {
        use std::iter;

        // We're essentially doing nested for loops, where each loop yields rows from one "table".
        // For the case of a two-way join (which is all that's supported for now), we call the two
        // tables `left` and `right`. We're going to iterate over results from `left` in the outer
        // loop, and query `right` inside the loop for each `left`.

        // pick some view query order
        // TODO: figure out which join order is best
        let left = *self.join.keys().min().unwrap();

        // Set up parameters for querying all rows in left.
        //
        // We find the number of parameters by looking at how many parameters the other side of the
        // join would have used if it tried to query us.
        let mut lparams = None;

        // Avoid scanning rows that wouldn't match the query anyway. We do this by finding all
        // conditions that filter over a field present in left, and use those as parameters.
        if let Some(q) = q {
            lparams = Some(q.having.iter().filter_map(|c| {
                let (srci, coli) = self.emit[c.column];
                if srci != left {
                    return None;
                }

                Some(shortcut::Condition{
                    column: coli,
                    cmp: c.cmp.clone(),
                })
            }).collect::<Vec<_>>());

            if lparams.as_ref().unwrap().len() == 0 {
                lparams = None;
            }
        }

        // produce a left * right given a left (basically the same as forward())
        // TODO: we probably don't need to select all columns here
        self.nodes[&left].find(lparams.map(|ps| {
            query::Query::new(&iter::repeat(true).take(self.nodes[&left].args().len()).collect::<Vec<_>>(), ps)
        }), Some(ts))
            .into_iter()
            .flat_map(move |(lrec, lts)| {
                // TODO: also add constants from q to filter used to select from right
                // TODO: respect q.select
                self.join((left, lrec, lts), ts)
            })
            .filter_map(move |(r, ts)| {
                if let Some(ref q) = q {
                    q.feed(&r[..]).map(|r| (r, ts))
                } else {
                    Some((r, ts))
                }
            })
            .collect()
    }

    fn suggest_indexes(&self, this: flow::NodeIndex) -> HashMap<flow::NodeIndex, Vec<usize>> {
        // index all join fields
        self.join
            .iter()
            // for every left
            .flat_map(|(left, rs)| {
                // for every right
                rs.iter().flat_map(move |(right, rs)| {
                    // emit both the left binding
                    rs.iter().map(move |&(li, _)| (left, li))
                    // and the right binding
                    .chain(rs.iter().map(move |&(_, ri)| (right, ri)))
                })
            })
            // we now have (NodeIndex, usize) for every join column.
            .fold(HashMap::new(), |mut hm, (node, col)| {
                hm.entry(*node).or_insert_with(Vec::new).push(col);

                // if this join column is emitted, we also want an index on that output column, as
                // it's likely the user will do lookups on it.
                if let Some(outi) = self.emit.iter().position(|&(ref n, c)| n == node && c == col) {
                    hm.entry(this).or_insert_with(Vec::new).push(outi);
                }
                hm
            })
    }

    fn resolve(&self, col: usize) -> Vec<(flow::NodeIndex, usize)> {
        vec![self.emit[col].clone()]
    }
}

// yes, this is never satisfied
// tests disabled until we can do dependency injection
#[cfg(all(unix, windows))]
#[cfg(test)]
mod tests {
    use super::*;

    use ops;
    use query;
    use shortcut;

    use ops::NodeOp;
    use std::collections::HashMap;

    fn setup() -> (ops::AQ, Joiner) {
        // 0 = left, 1 = right
        let mut aqfs = HashMap::new();
        aqfs.insert(0.into(), Box::new(left) as Box<_>);
        aqfs.insert(1.into(), Box::new(right) as Box<_>);

        let mut join = HashMap::new();
        // if left joins against right, join on the first field
        join.insert(0.into(), vec![(0.into(), vec![0]), (1.into(), vec![0])]);
        // if right joins against left, also join on the first field (duh)
        join.insert(1.into(), vec![(0.into(), vec![0]), (1.into(), vec![0])]);

        // emit first and second field from left
        // third field from right
        let emit = vec![(0.into(), 0), (0.into(), 1), (1.into(), 1)];

        let j = Joiner::new(emit, join);
        (aqfs, j)
    }

    #[test]
    fn it_works() {
        let (aqfs, j) = setup();

        // these are the data items we have to work with
        // these are in left
        let l_a1 = vec![1.into(), "a".into()];
        let l_b2 = vec![2.into(), "b".into()];
        let l_c3 = vec![3.into(), "c".into()];
        // these are in right
        // let r_x1 = vec![1.into(), "x".into()];
        // let r_y1 = vec![1.into(), "y".into()];
        // let r_z2 = vec![2.into(), "z".into()];

        // forward c3 from left; should produce [] since no records in right are 3
        match j.forward(l_c3.clone().into(), 0.into(), 0, None, &aqfs).unwrap() {
            ops::Update::Records(rs) => {
                // right has no records with value 3
                assert_eq!(rs.len(), 0);
            }
        }

        // forward b2 from left; should produce [b2*z2]
        match j.forward(l_b2.clone().into(), 0.into(), 0, None, &aqfs).unwrap() {
            ops::Update::Records(rs) => {
                // we're expecting to only match z2
                assert_eq!(rs,
                           vec![ops::Record::Positive(vec![2.into(), "b".into(), "z".into()], 2)]);
            }
        }

        // forward a1 from left; should produce [a1*x1, a1*y1]
        match j.forward(l_a1.clone().into(), 0.into(), 0, None, &aqfs).unwrap() {
            ops::Update::Records(rs) => {
                // we're expecting two results: x1 and y1
                assert_eq!(rs.len(), 2);
                // they should all be positive since input was positive
                assert!(rs.iter().all(|r| r.is_positive()));
                // they should all have the correct values from the provided left
                assert!(rs.iter().all(|r| r.rec()[0] == 1.into() && r.rec()[1] == "a".into()));
                // and both join results should be present
                // with ts set to be the max of left and right
                assert!(rs.iter().any(|r| r.rec()[2] == "x".into() && r.ts() == 0));
                assert!(rs.iter().any(|r| r.rec()[2] == "y".into() && r.ts() == 1));
            }
        }

        // TODO: write tests that forward from right
    }

    #[test]
    fn it_queries() {
        use std::sync;

        let (aqfs, j) = setup();
        let aqfs = sync::Arc::new(aqfs);

        // do a full query, which should return product of left + right:
        // [ax, ay, bz]
        let hits = j.query(None, 0, &aqfs);
        assert_eq!(hits.len(), 3);
        assert!(hits.iter()
            .any(|&(ref r, ts)| {
                ts == 0 && r[0] == 1.into() && r[1] == "a".into() && r[2] == "x".into()
            }));
        assert!(hits.iter()
            .any(|&(ref r, ts)| {
                ts == 1 && r[0] == 1.into() && r[1] == "a".into() && r[2] == "y".into()
            }));
        assert!(hits.iter()
            .any(|&(ref r, ts)| {
                ts == 2 && r[0] == 2.into() && r[1] == "b".into() && r[2] == "z".into()
            }));

        // query using join field
        let q = query::Query::new(&[true, true, true],
                                  vec![shortcut::Condition {
                             column: 0,
                             cmp: shortcut::Comparison::Equal(shortcut::Value::Const(2.into())),
                         }]);

        let hits = j.query(Some(&q), 0, &aqfs);
        assert_eq!(hits.len(), 1);
        assert!(hits.iter()
            .any(|&(ref r, ts)| {
                ts == 2 && r[0] == 2.into() && r[1] == "b".into() && r[2] == "z".into()
            }));

        // query using field from left
        let q = query::Query::new(&[true, true, true],
                                  vec![shortcut::Condition {
                             column: 1,
                             cmp: shortcut::Comparison::Equal(shortcut::Value::Const("a".into())),
                         }]);

        let hits = j.query(Some(&q), 0, &aqfs);
        assert_eq!(hits.len(), 2);
        assert!(hits.iter()
            .any(|&(ref r, ts)| {
                ts == 0 && r[0] == 1.into() && r[1] == "a".into() && r[2] == "x".into()
            }));
        assert!(hits.iter()
            .any(|&(ref r, ts)| {
                ts == 1 && r[0] == 1.into() && r[1] == "a".into() && r[2] == "y".into()
            }));

        // query using field from right
        let q = query::Query::new(&[true, true, true],
                                  vec![shortcut::Condition {
                             column: 2,
                             cmp: shortcut::Comparison::Equal(shortcut::Value::Const("z".into())),
                         }]);

        let hits = j.query(Some(&q), 0, &aqfs);
        assert_eq!(hits.len(), 1);
        assert!(hits.iter()
            .any(|&(ref r, ts)| {
                ts == 2 && r[0] == 2.into() && r[1] == "b".into() && r[2] == "z".into()
            }));
    }

    fn left(p: ops::Params, _: i64) -> Vec<(Vec<query::DataType>, i64)> {
        let data = vec![
                (vec![1.into(), "a".into()], 0),
                (vec![2.into(), "b".into()], 1),
                (vec![3.into(), "c".into()], 2),
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

    fn right(p: ops::Params, _: i64) -> Vec<(Vec<query::DataType>, i64)> {
        let data = vec![
                (vec![1.into(), "x".into()], 0),
                (vec![1.into(), "y".into()], 1),
                (vec![2.into(), "z".into()], 2),
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
    fn it_suggests_indices() {
        let (_, j) = setup();
        let hm: HashMap<_, _> = vec![
            (0.into(), vec![0]), // join column for left
            (1.into(), vec![0]), // join column for right
            (2.into(), vec![0]), // output column that is used as join column
        ]
            .into_iter()
            .collect();
        assert_eq!(hm, j.suggest_indexes(2.into()));
    }

    #[test]
    fn it_resolves() {
        let (_, j) = setup();
        assert_eq!(j.resolve(0), vec![(0.into(), 0)]);
        assert_eq!(j.resolve(1), vec![(0.into(), 1)]);
        assert_eq!(j.resolve(2), vec![(1.into(), 1)]);
    }
}
