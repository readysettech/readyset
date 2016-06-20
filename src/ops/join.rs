use ops;
use flow;
use query;
use ops::base::NodeOp;

use std::collections::HashMap;

use shortcut;

pub struct Joiner {
    emit: Vec<(flow::NodeIndex, usize)>,
    join: HashMap<flow::NodeIndex, Vec<(flow::NodeIndex, Vec<usize>)>>,
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
                let join = &self.join[&from];

                // next, what fields are we joining with `other` on?
                assert_eq!(join.len(), 1);
                let join = &join[0];

                // TODO: we should be clever here, and only query once per *distinct join value*,
                // instead of once per received record.
                Some(ops::Update::Records(rs.into_iter()
                    .flat_map(|rec| {
                        let (r, pos) = rec.extract();

                        // figure out the join values for this record
                        let params = join.1
                            .iter()
                            .map(|col| shortcut::Value::Const(r[*col].clone()))
                            .collect();

                        // send the parameters to start the query.
                        let rx = (*aqfs[&join.0])(params);

                        rx.into_iter().map(move |j| {
                            // weave together r and j according to join rules
                            let res = self.emit
                            .iter()
                            .map(|&(source, column)| {
                                if source == from {
                                    r[column].clone()
                                } else {
                                    // FIXME: this clone is unnecessary.
                                    // it's tricky to remove though, because it means we'd need to
                                    // be removing things from j. what if a later column also needs
                                    // to select from j? we'd need to keep track of which things we
                                    // have removed, and subtract that many from the index of the
                                    // later column. ugh.
                                    j[column].clone()
                                }
                            })
                            .collect();

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

    fn query(&self, _: Option<&query::Query>, _: &ops::base::AQ) -> ops::base::Datas {
        unimplemented!();
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

    #[test]
    fn it_works() {
        // 0 = left, 1 = right
        let mut aqfs = HashMap::new();
        aqfs.insert(0.into(), Box::new(left) as Box<_>);
        aqfs.insert(1.into(), Box::new(right) as Box<_>);

        let mut join = HashMap::new();
        // if left joins against right, join on the first field
        join.insert(0.into(), vec![(1.into(), vec![0])]);
        // if right joins against left, also join on the first field (duh)
        join.insert(1.into(), vec![(0.into(), vec![0])]);

        // emit first and second field from left
        // third field from right
        let emit = vec![(0.into(), 0), (0.into(), 1), (1.into(), 1)];

        let j = Joiner {
            emit: emit,
            join: join,
        };

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
    }

    fn left(p: ops::base::Params) -> Box<Iterator<Item = Vec<query::DataType>>> {
        let data = vec![
                vec![1.into(), "a".into()],
                vec![2.into(), "b".into()],
                vec![2.into(), "c".into()],
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
