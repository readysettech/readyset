use ops;
use flow;
use query;
use backlog;
use ops::NodeOp;

use std::sync;
use std::collections::HashMap;

use shortcut;

pub struct Union {
    emit: HashMap<flow::NodeIndex, Vec<usize>>,
    cols: HashMap<flow::NodeIndex, usize>,
}

impl NodeOp for Union {
    fn forward(&self,
               u: ops::Update,
               from: flow::NodeIndex,
               _: i64,
               _: Option<&backlog::BufferedStore>,
               _: &ops::AQ)
               -> Option<ops::Update> {
        match u {
            ops::Update::Records(rs) => {
                Some(ops::Update::Records(rs.into_iter()
                    .map(|rec| {
                        let (r, pos) = rec.extract();

                        // yield selected columns for this source
                        let res = self.emit[&from].iter().map(|&col| r[col].clone()).collect();

                        // return new row with appropriate sign
                        if pos {
                            ops::Record::Positive(res)
                        } else {
                            ops::Record::Negative(res)
                        }
                    })
                    .collect()))
            }
        }
    }

    fn query<'a>(&'a self,
                 q: Option<&query::Query>,
                 ts: i64,
                 aqfs: sync::Arc<ops::AQ>)
                 -> ops::Datas<'a> {
        use std::iter;

        let mut params = HashMap::new();
        for src in aqfs.keys() {
            // Set up parameters for querying all rows in this src.
            let mut p: Vec<shortcut::Value<query::DataType>> =
                iter::repeat(shortcut::Value::Const(query::DataType::None))
                    .take(self.cols[src])
                    .collect();

            // Avoid scanning rows that wouldn't match the query anyway. We do this by finding all
            // conditions that filter over a field present in left, and use those as parameters.
            let emit = &self.emit[src];
            if let Some(q) = q {
                for c in q.having.iter() {
                    // TODO: note that we assume here that the query to the left node is
                    // implemented as an equality constraint. This is probably not necessarily
                    // true.
                    let coli = emit[c.column];

                    // we can only push it down if it's an equality comparison
                    match c.cmp {
                        shortcut::Comparison::Equal(ref v) => {
                            // yay!
                            *p.get_mut(coli).unwrap() = v.clone();
                        }
                    }
                }
            }

            params.insert(*src, p);
        }

        // we need an owned copy of the query
        let q = q.and_then(|q| Some(q.to_owned()));

        // we select from each source in turn
        Box::new(params.into_iter()
            .flat_map(move |(src, params)| {
                let emit = &self.emit[&src];
                (aqfs[&src])((params, ts))
                // XXX: the clone here is really sad
                .map(move |r| emit.iter().map(|ci| r[*ci].clone()).collect::<Vec<_>>())
            })
            .filter_map(move |r| if let Some(ref q) = q {
                q.feed(&r[..])
            } else {
                Some(r)
            }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use ops;
    use query;
    use shortcut;

    use ops::NodeOp;
    use std::collections::HashMap;

    fn setup() -> (ops::AQ, Union) {
        // 0 = left, 1 = right
        let mut aqfs = HashMap::new();
        aqfs.insert(0.into(), Box::new(left) as Box<_>);
        aqfs.insert(1.into(), Box::new(right) as Box<_>);

        let mut emits = HashMap::new();
        emits.insert(0.into(), vec![0, 1]);
        emits.insert(1.into(), vec![0, 2]);
        let mut cols = HashMap::new();
        cols.insert(0.into(), 2);
        cols.insert(1.into(), 3);

        let u = Union {
            emit: emits,
            cols: cols,
        };

        (aqfs, u)
    }

    #[test]
    fn it_works() {
        let (aqfs, u) = setup();

        // to shorten stuff a little:
        let t = |r| ops::Update::Records(vec![ops::Record::Positive(r)]);

        // forward from left should emit original record
        let left = vec![1.into(), "a".into()];
        match u.forward(t(left.clone()), 0.into(), 0, None, &aqfs).unwrap() {
            ops::Update::Records(rs) => {
                assert_eq!(rs, vec![ops::Record::Positive(left)]);
            }
        }

        // forward from right should emit subset record
        let right = vec![1.into(), "skipped".into(), "x".into()];
        match u.forward(t(right.clone()), 1.into(), 0, None, &aqfs).unwrap() {
            ops::Update::Records(rs) => {
                assert_eq!(rs, vec![ops::Record::Positive(vec![1.into(), "x".into()])]);
            }
        }
    }

    #[test]
    fn it_queries() {
        use std::sync;

        let (aqfs, u) = setup();
        let aqfs = sync::Arc::new(aqfs);

        // do a full query, which should return left + right:
        // [a, b, x]
        let hits = u.query(None, 0, aqfs.clone()).collect::<Vec<_>>();
        assert_eq!(hits.len(), 3);
        assert!(hits.iter().any(|r| r[0] == 1.into() && r[1] == "a".into()));
        assert!(hits.iter().any(|r| r[0] == 2.into() && r[1] == "b".into()));
        assert!(hits.iter().any(|r| r[0] == 1.into() && r[1] == "x".into()));

        // query with parameters matching on both sides
        let q = query::Query {
            select: vec![true, true],
            having: vec![shortcut::Condition {
                             column: 0,
                             cmp: shortcut::Comparison::Equal(shortcut::Value::Const(1.into())),
                         }],
        };

        let hits = u.query(Some(&q), 0, aqfs.clone()).collect::<Vec<_>>();
        assert_eq!(hits.len(), 2);
        assert!(hits.iter().any(|r| r[0] == 1.into() && r[1] == "a".into()));
        assert!(hits.iter().any(|r| r[0] == 1.into() && r[1] == "x".into()));

        // query with parameter matching only on left
        let q = query::Query {
            select: vec![true, true],
            having: vec![shortcut::Condition {
                             column: 0,
                             cmp: shortcut::Comparison::Equal(shortcut::Value::Const(2.into())),
                         }],
        };

        let hits = u.query(Some(&q), 0, aqfs.clone()).collect::<Vec<_>>();
        assert_eq!(hits.len(), 1);
        assert!(hits.iter().any(|r| r[0] == 2.into() && r[1] == "b".into()));

        // query with parameter matching only on right
        let q = query::Query {
            select: vec![true, true],
            having: vec![shortcut::Condition {
                             column: 1,
                             cmp: shortcut::Comparison::Equal(shortcut::Value::Const("x".into())),
                         }],
        };

        let hits = u.query(Some(&q), 0, aqfs.clone()).collect::<Vec<_>>();
        assert_eq!(hits.len(), 1);
        assert!(hits.iter().any(|r| r[0] == 1.into() && r[1] == "x".into()));

        // query with parameter with no matches
        let q = query::Query {
            select: vec![true, true],
            having: vec![shortcut::Condition {
                             column: 0,
                             cmp: shortcut::Comparison::Equal(shortcut::Value::Const(3.into())),
                         }],
        };

        let hits = u.query(Some(&q), 0, aqfs.clone()).collect::<Vec<_>>();
        assert_eq!(hits.len(), 0);
    }

    fn left(p: ops::Params) -> Box<Iterator<Item = Vec<query::DataType>>> {
        let data = vec![
                vec![1.into(), "a".into()],
                vec![2.into(), "b".into()],
            ];

        assert_eq!(p.0.len(), 2);
        let mut p = p.0.into_iter();
        let q = query::Query {
            select: vec![true, true],
            having: vec![
                shortcut::Condition {
                             column: 0,
                             cmp: shortcut::Comparison::Equal(p.next().unwrap()),
                         },
                shortcut::Condition {
                             column: 1,
                             cmp: shortcut::Comparison::Equal(p.next().unwrap()),
                         },
            ],
        };

        Box::new(data.into_iter().filter_map(move |r| q.feed(&r[..])))
    }

    fn right(p: ops::Params) -> Box<Iterator<Item = Vec<query::DataType>>> {
        let data = vec![
                vec![1.into(), "skipped".into(), "x".into()],
            ];

        assert_eq!(p.0.len(), 3);
        let mut p = p.0.into_iter();
        let q = query::Query {
            select: vec![true, true, true],
            having: vec![shortcut::Condition {
                             column: 0,
                             cmp: shortcut::Comparison::Equal(p.next().unwrap()),
                         },
                         shortcut::Condition {
                             column: 1,
                             cmp: shortcut::Comparison::Equal(p.next().unwrap()),
                         },
                         shortcut::Condition {
                             column: 2,
                             cmp: shortcut::Comparison::Equal(p.next().unwrap()),
                         }],
        };

        Box::new(data.into_iter().filter_map(move |r| q.feed(&r[..])))
    }
}
