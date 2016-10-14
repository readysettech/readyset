use ops;
use flow;
use query;
use backlog;
use ops::NodeOp;
use ops::NodeType;

use std::collections::HashMap;
use std::collections::HashSet;

use shortcut;

#[derive(Debug)]
pub enum TextComponent {
    Literal(&'static str),
    Column(usize),
}

enum Modify {
    Add(String),
    Remove(String),
}

#[derive(Debug)]
pub struct GroupConcat {
    components: Vec<TextComponent>,
    separator: &'static str,
    src: flow::NodeIndex,

    srcn: Option<ops::V>,
    group: HashSet<usize>,
    slen: usize,
    cond: Vec<shortcut::Condition<query::DataType>>,
    colfix: Vec<usize>,
}

impl From<GroupConcat> for NodeType {
    fn from(b: GroupConcat) -> NodeType {
        NodeType::GroupConcatNode(b)
    }
}

impl GroupConcat {
    pub fn new(src: flow::NodeIndex,
               components: Vec<TextComponent>,
               separator: &'static str)
               -> GroupConcat {
        assert!(!separator.is_empty(),
                "group concat separator cannot be empty");
        GroupConcat {
            components: components,
            separator: separator,
            src: src,

            srcn: None,
            group: HashSet::new(),
            slen: 0,
            cond: Vec::new(),
            colfix: Vec::new(),
        }
    }

    fn build(&self, r: &ops::Record) -> Modify {
        let mut s = String::with_capacity(self.slen);
        let rec = r.rec();
        for tc in self.components.iter() {
            match *tc {
                TextComponent::Literal(l) => {
                    s.push_str(l);
                }
                TextComponent::Column(i) => {
                    match rec[i] {
                        query::DataType::Text(ref val) => {
                            s.push_str(&*val);
                        }
                        query::DataType::Number(ref n) => s.push_str(&n.to_string()),
                        query::DataType::None => unreachable!(),
                    }
                }
            }
        }

        if r.is_positive() {
            Modify::Add(s)
        } else {
            Modify::Remove(s)
        }
    }
}

impl NodeOp for GroupConcat {
    fn prime(&mut self, g: &ops::Graph) -> Vec<flow::NodeIndex> {
        // who's our parent?
        self.srcn = g[self.src].as_ref().map(|n| n.clone());

        // group by all columns
        let cols = self.srcn.as_ref().unwrap().args().len();
        self.group.extend(0..cols);
        // except the ones that are used in output
        for tc in self.components.iter() {
            if let TextComponent::Column(col) = *tc {
                assert!(col < cols, "group concat emits fields parent doesn't have");
                self.group.remove(&col);
            }
        }

        // how long are we expecting strings to be?
        self.slen = 0;
        // well, the length of all literal components
        for tc in self.components.iter() {
            if let TextComponent::Literal(l) = *tc {
                self.slen += l.len();
            }
        }
        // plus some fixed size per value
        self.slen += 10 * (cols - self.group.len());

        // construct condition for querying into ourselves
        self.cond = (0..self.group.len())
            .into_iter()
            .map(|col| {
                shortcut::Condition {
                    column: col,
                    cmp: shortcut::Comparison::Equal(shortcut::Value::Const(query::DataType::None)),
                }
            })
            .collect::<Vec<_>>();

        // the query into our own output (above) uses *output* column indices
        // but when we try to fill it, we have *input* column indices
        // build a translation mechanism for going from the former to the latter
        let colfix: Vec<_> = (0..cols)
            .into_iter()
            .filter_map(|col| {
                if self.group.contains(&col) {
                    // the next output column is this column
                    Some(col)
                } else {
                    // this column does not appear in output
                    None
                }
            })
            .collect();
        self.colfix.extend(colfix.into_iter());

        vec![self.src]
    }

    fn forward(&self,
               u: ops::Update,
               src: flow::NodeIndex,
               _: i64,
               db: Option<&backlog::BufferedStore>)
               -> Option<ops::Update> {

        assert_eq!(src, self.src);

        // Construct the query we'll need to query into ourselves
        let mut q = self.cond.clone();

        match u {
            ops::Update::Records(rs) => {
                if rs.is_empty() {
                    return None;
                }

                // First, we want to be smart about multiple added/removed rows with same group.
                // For example, if we get a -, then a +, for the same group, we don't want to
                // execute two queries.
                let mut consolidate = HashMap::new();
                for rec in rs.into_iter() {
                    let val = Some(self.build(&rec));
                    let (r, _, ts) = rec.extract();
                    let group = r.into_iter()
                        .enumerate()
                        .map(|(i, v)| {
                            if self.group.contains(&i) {
                                Some(v)
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<_>>();

                    consolidate.entry(group).or_insert_with(Vec::new).push((val, ts));
                }

                let mut out = Vec::with_capacity(2 * consolidate.len());
                for (mut group, diffs) in consolidate.into_iter() {
                    // note that each value in group is an Option so that we can take/swap without
                    // having to .remove or .insert into the HashMap (which is much more expensive)
                    // it should only be None for self.over

                    // build a query for this group
                    for s in q.iter_mut() {
                        s.cmp = shortcut::Comparison::Equal(
                            shortcut::Value::Const(
                                group[self.colfix[s.column]]
                                .take()
                                .unwrap()
                            )
                        );
                    }

                    // find the current value for this group
                    let (current, old_ts) = match db {
                        Some(db) => {
                            db.find_and(&q[..], Some(i64::max_value()), |rs| {
                                assert!(rs.len() <= 1,
                                        "current group concat had more than 1 result");
                                // current value is in the last output column
                                // or "" if there is no current group
                                rs.into_iter()
                                    .next()
                                    .map(|(r, ts)| (r[r.len() - 1].clone().into(), ts))
                                    .unwrap_or((query::DataType::from(""), 0))
                            })
                        }
                        None => {
                            // TODO
                            // query ancestor (self.query?) based on self.group columns
                            unimplemented!()
                        }
                    };

                    // get back values from query (to avoid cloning)
                    for s in q.iter_mut() {
                        if let shortcut::Comparison::Equal(shortcut::Value::Const(ref mut v)) =
                               s.cmp {
                            use std::mem;

                            let mut x = query::DataType::None;
                            mem::swap(&mut x, v);
                            group[self.colfix[s.column]] = Some(x);
                        }
                    }

                    // construct prefix of output record
                    let mut rec = Vec::with_capacity(group.len() + 1);
                    rec.extend(group.into_iter().filter_map(|v| v));

                    // revoke old value
                    rec.push(current.clone().into());
                    out.push(ops::Record::Negative(rec.clone(), old_ts));

                    // new ts is the max change timestamp
                    let new_ts = diffs.iter().map(|&(_, ts)| ts).max().unwrap();

                    // updating the value is a bit tricky because we want to retain ordering of the
                    // elements. we therefore need to first split the value, add the new ones,
                    // remove revoked ones, sort, and then join again. ugh. we try to make it more
                    // efficient by splitting into a BTree, which maintains sorting while
                    // supporting efficient add/remove.
                    let new = {
                        use std::collections::BTreeSet;
                        use std::iter::FromIterator;

                        let current = if let query::DataType::Text(ref s) = current {
                            s
                        } else {
                            unreachable!();
                        };
                        let clen = current.len();

                        // TODO this is not particularly robust, and requires a non-empty separator
                        let mut current =
                            BTreeSet::from_iter(current.split_terminator(self.separator));
                        for &(ref diff, _) in diffs.iter() {
                            match *diff {
                                Some(Modify::Add(ref s)) => {
                                    current.insert(s);
                                }
                                Some(Modify::Remove(ref s)) => {
                                    current.remove(&**s);
                                }
                                None => unreachable!(),
                            }
                        }

                        // WHY doesn't rust have an iterator joiner?
                        let mut new = current.into_iter()
                            .fold(String::with_capacity(2 * clen), |mut acc, s| {
                                acc.push_str(&s);
                                acc.push_str(self.separator);
                                acc
                            });
                        // we pushed one separator too many above
                        let real_len = new.len() - self.separator.len();
                        new.truncate(real_len);
                        new
                    };

                    // remove the old value from the end of the record
                    rec.pop();

                    // emit new value
                    rec.push(new.into());
                    out.push(ops::Record::Positive(rec, new_ts));
                }

                Some(ops::Update::Records(out))
            }
        }
    }

    fn query(&self, _: Option<&query::Query>, _: i64) -> ops::Datas {
        unimplemented!();
    }

    fn suggest_indexes(&self, this: flow::NodeIndex) -> HashMap<flow::NodeIndex, Vec<usize>> {
        // index all group by columns
        Some((this, self.group.iter().cloned().collect()))
            .into_iter()
            .collect()
    }

    fn resolve(&self, col: usize) -> Option<Vec<(flow::NodeIndex, usize)>> {
        if col == self.srcn.as_ref().unwrap().args().len() - 1 {
            return None;
        }
        Some(vec![(self.src, self.colfix[col])])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use ops;
    use flow;
    use petgraph;

    use flow::View;
    use ops::NodeOp;

    fn setup(mat: bool, wide: bool) -> ops::Node {
        use std::sync;
        use flow::View;

        let mut g = petgraph::Graph::new();
        let mut s = if wide {
            ops::new("source", &["x", "y", "z"], true, ops::base::Base {})
        } else {
            ops::new("source", &["x", "y"], true, ops::base::Base {})
        };

        s.prime(&g);
        let s = g.add_node(Some(sync::Arc::new(s)));

        g[s].as_ref().unwrap().process((vec![1.into(), 1.into()], 0).into(), s, 0);
        g[s].as_ref().unwrap().process((vec![2.into(), 1.into()], 1).into(), s, 1);
        g[s].as_ref().unwrap().process((vec![2.into(), 2.into()], 2).into(), s, 2);

        let mut c = GroupConcat::new(s,
                                     vec![TextComponent::Literal("."),
                                          TextComponent::Column(1),
                                          TextComponent::Literal(";")],
                                     "#");
        c.prime(&g);
        if wide {
            ops::new("concat", &["x", "z", "ys"], mat, c)
        } else {
            ops::new("concat", &["x", "ys"], mat, c)
        }
    }

    #[test]
    fn it_forwards() {
        let src = flow::NodeIndex::new(0);
        let c = setup(true, false);

        let u = (vec![1.into(), 1.into()], 1).into();

        // first row for a group should emit -"" and +".1;" for that group
        let out = c.process(u, src, 1);
        if let Some(ops::Update::Records(rs)) = out {
            assert_eq!(rs.len(), 2);
            let mut rs = rs.into_iter();

            match rs.next().unwrap() {
                ops::Record::Negative(r, ts) => {
                    assert_eq!(r[0], 1.into());
                    assert_eq!(r[1], "".into());
                    assert_eq!(ts, 0);
                }
                _ => unreachable!(),
            }
            match rs.next().unwrap() {
                ops::Record::Positive(r, ts) => {
                    assert_eq!(r[0], 1.into());
                    assert_eq!(r[1], ".1;".into());
                    assert_eq!(ts, 1);
                    c.safe(1);
                }
                _ => unreachable!(),
            }
        } else {
            unreachable!();
        }

        let u = (vec![2.into(), 2.into()], 2).into();

        // first row for a second group should emit -"" and +".2;" for that new group
        let out = c.process(u, src, 2);
        if let Some(ops::Update::Records(rs)) = out {
            assert_eq!(rs.len(), 2);
            let mut rs = rs.into_iter();

            match rs.next().unwrap() {
                ops::Record::Negative(r, ts) => {
                    assert_eq!(r[0], 2.into());
                    assert_eq!(r[1], "".into());
                    assert_eq!(ts, 0);
                }
                _ => unreachable!(),
            }
            match rs.next().unwrap() {
                ops::Record::Positive(r, ts) => {
                    assert_eq!(r[0], 2.into());
                    assert_eq!(r[1], ".2;".into());
                    assert_eq!(ts, 2);
                    c.safe(2);
                }
                _ => unreachable!(),
            }
        } else {
            unreachable!();
        }

        let u = (vec![1.into(), 2.into()], 3).into();

        // second row for a group should emit -".1;" and +".1;#.2;"
        let out = c.process(u, src, 3);
        if let Some(ops::Update::Records(rs)) = out {
            assert_eq!(rs.len(), 2);
            let mut rs = rs.into_iter();

            match rs.next().unwrap() {
                ops::Record::Negative(r, ts) => {
                    assert_eq!(r[0], 1.into());
                    assert_eq!(r[1], ".1;".into());
                    assert_eq!(ts, 1);
                }
                _ => unreachable!(),
            }
            match rs.next().unwrap() {
                ops::Record::Positive(r, ts) => {
                    assert_eq!(r[0], 1.into());
                    assert_eq!(r[1], ".1;#.2;".into());
                    assert_eq!(ts, 3);
                    c.safe(3);
                }
                _ => unreachable!(),
            }
        } else {
            unreachable!();
        }

        let u = ops::Record::Negative(vec![1.into(), 1.into()], 4).into();

        // negative row for a group should emit -".1;#.2;" and +".2;"
        let out = c.process(u, src, 4);
        if let Some(ops::Update::Records(rs)) = out {
            assert_eq!(rs.len(), 2);
            let mut rs = rs.into_iter();

            match rs.next().unwrap() {
                ops::Record::Negative(r, ts) => {
                    assert_eq!(r[0], 1.into());
                    assert_eq!(r[1], ".1;#.2;".into());
                    assert_eq!(ts, 3);
                }
                _ => unreachable!(),
            }
            match rs.next().unwrap() {
                ops::Record::Positive(r, ts) => {
                    assert_eq!(r[0], 1.into());
                    assert_eq!(r[1], ".2;".into());
                    assert_eq!(ts, 4);
                    c.safe(4);
                }
                _ => unreachable!(),
            }
        } else {
            unreachable!();
        }

        let u = ops::Update::Records(vec![
             // remove non-existing
             ops::Record::Negative(vec![1.into(), 1.into()], 1),
             // add old
             ops::Record::Positive(vec![1.into(), 1.into()], 5),
             // add duplicate
             ops::Record::Positive(vec![1.into(), 2.into()], 3),
             ops::Record::Negative(vec![2.into(), 2.into()], 2),
             ops::Record::Positive(vec![2.into(), 3.into()], 5),
             ops::Record::Positive(vec![2.into(), 2.into()], 5),
             ops::Record::Positive(vec![2.into(), 1.into()], 5),
             ops::Record::Positive(vec![3.into(), 3.into()], 5),
        ]);

        // multiple positives and negatives should update aggregation value by appropriate amount
        let out = c.process(u, src, 5);
        if let Some(ops::Update::Records(rs)) = out {
            assert_eq!(rs.len(), 6); // one - and one + for each group
            // group 1 had [2], now has [1,2]
            assert!(rs.iter().any(|r| {
                if let ops::Record::Negative(ref r, ts) = *r {
                    if r[0] == 1.into() {
                        assert_eq!(r[1], ".2;".into());
                        assert_eq!(ts, 4);
                        true
                    } else {
                        false
                    }
                } else {
                    false
                }
            }));
            assert!(rs.iter().any(|r| {
                if let ops::Record::Positive(ref r, ts) = *r {
                    if r[0] == 1.into() {
                        assert_eq!(r[1], ".1;#.2;".into());
                        assert_eq!(ts, 5);
                        true
                    } else {
                        false
                    }
                } else {
                    false
                }
            }));
            // group 2 was [2], is now [1,2,3]
            assert!(rs.iter().any(|r| {
                if let ops::Record::Negative(ref r, ts) = *r {
                    if r[0] == 2.into() {
                        assert_eq!(r[1], ".2;".into());
                        assert_eq!(ts, 2);
                        true
                    } else {
                        false
                    }
                } else {
                    false
                }
            }));
            assert!(rs.iter().any(|r| {
                if let ops::Record::Positive(ref r, ts) = *r {
                    if r[0] == 2.into() {
                        assert_eq!(r[1], ".1;#.2;#.3;".into());
                        assert_eq!(ts, 5);
                        true
                    } else {
                        false
                    }
                } else {
                    false
                }
            }));
            // group 3 was [], is now [3]
            assert!(rs.iter().any(|r| {
                if let ops::Record::Negative(ref r, ts) = *r {
                    if r[0] == 3.into() {
                        assert_eq!(r[1], "".into());
                        assert_eq!(ts, 0);
                        true
                    } else {
                        false
                    }
                } else {
                    false
                }
            }));
            assert!(rs.iter().any(|r| {
                if let ops::Record::Positive(ref r, ts) = *r {
                    if r[0] == 3.into() {
                        assert_eq!(r[1], ".3;".into());
                        assert_eq!(ts, 5);
                        true
                    } else {
                        false
                    }
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
        let c = setup(false, true);
        let idx = c.suggest_indexes(1.into());

        // should only add index on own columns
        assert_eq!(idx.len(), 1);
        assert!(idx.contains_key(&1.into()));

        // should only index on group-by columns
        assert_eq!(idx[&1.into()].len(), 2);
        assert!(idx[&1.into()].iter().any(|&i| i == 0));
        assert!(idx[&1.into()].iter().any(|&i| i == 2));
    }

    #[test]
    fn it_resolves() {
        let c = setup(false, true);
        assert_eq!(c.resolve(0), Some(vec![(0.into(), 0)]));
        assert_eq!(c.resolve(1), Some(vec![(0.into(), 2)]));
        assert_eq!(c.resolve(2), None);
    }
}
