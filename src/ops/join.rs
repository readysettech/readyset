use ops;
use flow;
use query;
use ops::base::NodeOp;

use std::sync;
use std::collections::HashMap;

use shortcut;

pub struct Joiner {
    emit: Vec<(flow::NodeIndex, usize)>,
    /// For a given node index `n`, this gives the set of nodes that should be joined against in
    /// `.0`, and which fields of an `n` record should be used as parameters for that query. For
    /// every `n`, the set also contains an entry `(n, _)` which indicates which fields of `n` can
    /// be used as parameters to query `n` itself.
    join: HashMap<flow::NodeIndex, Vec<(flow::NodeIndex, Vec<usize>)>>,
}

impl Joiner {
    fn other<'a>(&'a self, this: &flow::NodeIndex) -> &'a (flow::NodeIndex, Vec<usize>) {
        self.join[this].iter().find(|&&(ref other, _)| other != this).unwrap()
    }

    fn this<'a>(&'a self, this: &flow::NodeIndex) -> &'a (flow::NodeIndex, Vec<usize>) {
        self.join[this].iter().find(|&&(ref other, _)| other == this).unwrap()
    }

    fn join<'a>(&'a self,
                left: Vec<query::DataType>,
                on: &'a (flow::NodeIndex, Vec<usize>),
                aqfs: &ops::base::AQ)
                -> Box<Iterator<Item = Vec<query::DataType>> + 'a> {
        // figure out the join values for this record
        let params = on.1
            .iter()
            .map(|col| shortcut::Value::Const(left[*col].clone()))
            .collect();

        // send the parameters to start the query.
        let rx = (*aqfs[&on.0])(params);

        Box::new(rx.into_iter().map(move |right| {
            // weave together r and j according to join rules
            self.emit
                .iter()
                .map(|&(source, column)| {
                    if source == on.0 {
                        // FIXME: this clone is unnecessary.
                        // it's tricky to remove though, because it means we'd need to
                        // be removing things from right. what if a later column also needs
                        // to select from right? we'd need to keep track of which things we
                        // have removed, and subtract that many from the index of the
                        // later column. ugh.
                        right[column].clone()
                    } else {
                        left[column].clone()
                    }
                })
                .collect()
        }))
    }
}

impl NodeOp for Joiner {
    fn forward(&self,
               u: ops::Update,
               from: flow::NodeIndex,
               _: Option<&shortcut::Store<query::DataType>>,
               aqfs: &ops::base::AQ)
               -> Option<ops::Update> {
        if aqfs.len() != 2 {
            unimplemented!(); // only two-way joins are supported at the moment
        }

        match u {
            ops::Update::Records(rs) => {
                // okay, so here's what's going on:
                // the record(s) we receive are all from one side of the join. we need to query the
                // other side(s) for records matching the incoming records on that side's join
                // fields.
                //
                // first, let's find out what we should be joining with
                let join = self.other(&from);

                // TODO: we should be clever here, and only query once per *distinct join value*,
                // instead of once per received record.
                Some(ops::Update::Records(rs.into_iter()
                    .flat_map(|rec| {
                        let (r, pos) = rec.extract();

                        self.join(r, join, aqfs).map(move |res| {
                            // return new row with appropriate sign
                            if pos {
                                ops::Record::Positive(res)
                            } else {
                                ops::Record::Negative(res)
                            }
                        })
                    })
                    .collect()))
            }
        }
    }

    fn query<'a>(&'a self,
                 q: Option<&query::Query>,
                 aqfs: sync::Arc<ops::base::AQ>)
                 -> ops::base::Datas<'a> {
        use std::iter;

        if aqfs.len() != 2 {
            unimplemented!(); // only two-way joins are supported at the moment
        }

        // We're essentially doing nested for loops, where each loop yields rows from one "table".
        // For the case of a two-way join (which is all that's supported for now), we call the two
        // tables `left` and `right`. We're going to iterate over results from `left` in the outer
        // loop, and query `right` inside the loop for each `left`.

        // Identify left and right
        // TODO: figure out which join order is best
        let left = self.join.keys().min().unwrap();
        let on = self.other(left);
        let left = self.this(left);

        // Set up parameters for querying all rows in left. We find the number of parameters by
        // looking at how many parameters the other side of the join would have used if it tried to
        // query us.
        let mut lparams: Vec<shortcut::Value<query::DataType>> =
            iter::repeat(shortcut::Value::Const(query::DataType::None))
                .take(left.1.len())
                .collect();

        // Avoid scanning rows that wouldn't match the query anyway. We do this by finding all
        // conditions that filter over a field present in left, and use those as parameters.
        if let Some(q) = q {
            for c in q.having.iter() {
                // TODO: note that we assume here that the query to the left node is implemented as
                // an equality constraint. This is probably not necessarily true.
                let (srci, coli) = self.emit[c.column];
                if srci == left.0 {
                    // this is contraint on one of our columns!
                    // let's see if we can push it down into the query:
                    // first, is it available as a query parameter upstream?
                    if let Some(parami) = left.1.iter().position(|&col| col == coli) {
                        // and second, is it an equality comparison?
                        match c.cmp {
                            shortcut::Comparison::Equal(ref v) => {
                                // yay!
                                *lparams.get_mut(parami).unwrap() = v.clone();
                            }
                        }
                    }
                }

            }
        }

        // we need an owned copy of the query
        let q = q.and_then(|q| Some(q.to_owned()));

        // produce a left * right given a left (basically the same as forward())
        let aqfs2 = aqfs.clone(); // XXX: figure out why this is needed?
        Box::new((aqfs[&left.0])(lparams)
            .flat_map(move |left| {
                // TODO: also add constants from q to filter used to select from right
                // TODO: respect q.select
                self.join(left, on, &*aqfs2)
            })
            .filter_map(move |r| {
                if let Some(ref q) = q {
                    q.feed(&r[..]) // XXX: unnecessary to do a copy here, no?
                } else {
                    Some(r)
                }
            }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use ops;
    use query;
    use shortcut;

    use ops::base::NodeOp;
    use std::collections::HashMap;

    fn setup() -> (ops::base::AQ, Joiner) {
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

        let j = Joiner {
            emit: emit,
            join: join,
        };

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

        // to shorten stuff a little:
        let t = |r| ops::Update::Records(vec![ops::Record::Positive(r)]);

        // forward c3 from left; should produce [] since no records in right are 3
        match j.forward(t(l_c3.clone()), 0.into(), None, &aqfs).unwrap() {
            ops::Update::Records(rs) => {
                // right has no records with value 3
                assert_eq!(rs.len(), 0);
            }
        }

        // forward b2 from left; should produce [b2*z2]
        match j.forward(t(l_b2.clone()), 0.into(), None, &aqfs).unwrap() {
            ops::Update::Records(rs) => {
                // we're expecting to only match z2
                assert_eq!(rs,
                           vec![ops::Record::Positive(vec![2.into(), "b".into(), "z".into()])]);
            }
        }

        // forward a1 from left; should produce [a1*x1, a1*y1]
        match j.forward(t(l_a1.clone()), 0.into(), None, &aqfs).unwrap() {
            ops::Update::Records(rs) => {
                // we're expecting two results: x1 and y1
                assert_eq!(rs.len(), 2);
                // they should all be positive since input was positive
                assert!(rs.iter().all(|r| r.is_positive()));
                // they should all have the correct values from the provided left
                assert!(rs.iter().all(|r| r.rec()[0] == 1.into() && r.rec()[1] == "a".into()));
                // and both join results should be present
                assert!(rs.iter().any(|r| r.rec()[2] == "x".into()));
                assert!(rs.iter().any(|r| r.rec()[2] == "y".into()));
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
        let hits = j.query(None, aqfs.clone()).collect::<Vec<_>>();
        assert_eq!(hits.len(), 3);
        assert!(hits.iter().any(|r| r[0] == 1.into() && r[1] == "a".into() && r[2] == "x".into()));
        assert!(hits.iter().any(|r| r[0] == 1.into() && r[1] == "a".into() && r[2] == "y".into()));
        assert!(hits.iter().any(|r| r[0] == 2.into() && r[1] == "b".into() && r[2] == "z".into()));

        // query using join field
        let q = query::Query {
            select: vec![true, true, true],
            having: vec![shortcut::Condition {
                             column: 0,
                             cmp: shortcut::Comparison::Equal(shortcut::Value::Const(2.into())),
                         }],
        };

        let hits = j.query(Some(&q), aqfs.clone()).collect::<Vec<_>>();
        assert_eq!(hits.len(), 1);
        assert!(hits.iter().any(|r| r[0] == 2.into() && r[1] == "b".into() && r[2] == "z".into()));

        // query using field from left
        let q = query::Query {
            select: vec![true, true, true],
            having: vec![shortcut::Condition {
                             column: 1,
                             cmp: shortcut::Comparison::Equal(shortcut::Value::Const("a".into())),
                         }],
        };

        let hits = j.query(Some(&q), aqfs.clone()).collect::<Vec<_>>();
        assert_eq!(hits.len(), 2);
        assert!(hits.iter().any(|r| r[0] == 1.into() && r[1] == "a".into() && r[2] == "x".into()));
        assert!(hits.iter().any(|r| r[0] == 1.into() && r[1] == "a".into() && r[2] == "y".into()));

        // query using field from right
        let q = query::Query {
            select: vec![true, true, true],
            having: vec![shortcut::Condition {
                             column: 2,
                             cmp: shortcut::Comparison::Equal(shortcut::Value::Const("z".into())),
                         }],
        };

        let hits = j.query(Some(&q), aqfs).collect::<Vec<_>>();
        assert_eq!(hits.len(), 1);
        assert!(hits.iter().any(|r| r[0] == 2.into() && r[1] == "b".into() && r[2] == "z".into()));
    }

    fn left(p: ops::base::Params) -> Box<Iterator<Item = Vec<query::DataType>>> {
        let data = vec![
                vec![1.into(), "a".into()],
                vec![2.into(), "b".into()],
                vec![3.into(), "c".into()],
            ];

        assert_eq!(p.len(), 1);
        let p = p.into_iter().last().unwrap();
        let q = query::Query {
            select: vec![true, true],
            having: vec![shortcut::Condition {
                             column: 0,
                             cmp: shortcut::Comparison::Equal(p),
                         }],
        };

        Box::new(data.into_iter().filter_map(move |r| q.feed(&r[..])))
    }

    fn right(p: ops::base::Params) -> Box<Iterator<Item = Vec<query::DataType>>> {
        let data = vec![
                vec![1.into(), "x".into()],
                vec![1.into(), "y".into()],
                vec![2.into(), "z".into()],
            ];

        assert_eq!(p.len(), 1);
        let p = p.into_iter().last().unwrap();
        let q = query::Query {
            select: vec![true, true],
            having: vec![shortcut::Condition {
                             column: 0,
                             cmp: shortcut::Comparison::Equal(p),
                         }],
        };

        Box::new(data.into_iter().filter_map(move |r| q.feed(&r[..])))
    }
}
