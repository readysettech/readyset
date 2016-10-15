use ops;
use flow;
use query;
use backlog;
use ops::NodeOp;
use ops::NodeType;

use std::collections::HashMap;
use std::collections::HashSet;

use shortcut;

/// Designator for what a given position in a group concat output should contain.
#[derive(Debug)]
pub enum TextComponent {
    /// Emit a literal string.
    Literal(&'static str),
    /// Emit the string representation of the given column in the current record.
    Column(usize),
}

enum Modify {
    Add(String),
    Remove(String),
}

/// GroupConcat joins multiple records into one using string concatenation.
///
/// It is conceptually similar to the group_concat function available in most SQL databases. The
/// records are first grouped by a set of fields. Within each group, a string representation is
/// then constructed, and the strings of all the records in a group are concatenated by joining
/// them with a literal separator.
///
/// The current implementation *requires* the separator to be non-empty, and relatively distinct,
/// as it is used as a sentinel for reconstructing the individual records' string representations.
/// This is necessary to incrementally maintain the group concatenation efficiently. This
/// requirement may be relaxed in the future. \u001E may be a good candidate.
///
/// If a group has only one record, the separator is not used.
///
/// For convenience, `GroupConcat` also orders the string representations of the records within a
/// group before joining them. This allows easy equality comparison of `GroupConcat` outputs. This
/// is the primary reason for the "separator as sentinel" behavior mentioned above, and may be made
/// optional in the future such that more efficient incremental updating and relaxed separator
/// semantics can be implemented.
#[derive(Debug)]
pub struct GroupConcat {
    components: Vec<TextComponent>,
    separator: &'static str,
    src: flow::NodeIndex,

    srcn: Option<ops::V>,
    cols: usize,
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
    /// Construct a new `GroupConcat` operator.
    ///
    /// All columns of the input to this node that are not mentioned in `components` will be used
    /// as group by parameters. For each record in a group, `components` dictates the construction
    /// of the record's string representation. `Literal`s are used, well, literally, and `Column`s
    /// are replaced with the string representation of corresponding value from the record under
    /// consideration. The string representations of all records within each group are joined using
    /// the given `separator`.
    ///
    /// Note that `separator` is *also* used as a sentinel in the resulting data to reconstruct
    /// the individual record strings from a group string. It should therefore not appear in the
    /// record data.
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
            cols: 0,
            group: HashSet::new(),
            slen: 0,
            cond: Vec::new(),
            colfix: Vec::new(),
        }
    }

    fn build(&self, rec: &[query::DataType]) -> String {
        let mut s = String::with_capacity(self.slen);
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

        s
    }
}

impl NodeOp for GroupConcat {
    fn prime(&mut self, g: &ops::Graph) -> Vec<flow::NodeIndex> {
        // who's our parent?
        self.srcn = g[self.src].as_ref().map(|n| n.clone());

        // group by all columns
        self.cols = self.srcn.as_ref().unwrap().args().len();
        self.group.extend(0..self.cols);
        // except the ones that are used in output
        for tc in self.components.iter() {
            if let TextComponent::Column(col) = *tc {
                assert!(col < self.cols,
                        "group concat emits fields parent doesn't have");
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
        self.slen += 10 * (self.cols - self.group.len());

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
        let colfix: Vec<_> = (0..self.cols)
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
                    let (r, pos, ts) = rec.extract();
                    let val = self.build(&r[..]);
                    let val = if pos {
                        Modify::Add(val)
                    } else {
                        Modify::Remove(val)
                    };
                    let val = Some(val);
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

    fn query(&self, q: Option<&query::Query>, ts: i64) -> ops::Datas {
        use std::iter;

        // we're fetching everything from our parent
        let mut params = None;

        // however, if there are some conditions that filters over one of our group-bys, we should
        // use those as parameters to speed things up.
        if let Some(q) = q {
            params = Some(q.having.iter().map(|c| {
                // FIXME: we could technically support querying over the output of the group by,
                // but we'd have to restructure this function a fair bit so that we keep that part
                // of the query around for after we've got the results back. We'd then need to do
                // another filtering pass over the results of query. Unclear if that's worth it.
                assert!(c.column < self.colfix.len(),
                        "filtering on group concatenation output is not supported");

                shortcut::Condition{
                    column: self.colfix[c.column],
                    cmp: c.cmp.clone(),
                }
            }).collect::<Vec<_>>());

            if params.as_ref().unwrap().len() == 0 {
                params = None;
            }
        }

        // now, query our ancestor, and aggregate into groups.
        let q = params.map(|ps| {
            query::Query::new(&iter::repeat(true)
                                  .take(self.cols)
                                  .collect::<Vec<_>>(),
                              ps)
        });

        let rx = self.srcn.as_ref().unwrap().find(q.as_ref(), Some(ts));

        // FIXME: having an order by would be nice here, so that we didn't have to keep the entire
        // aggregated state in memory until we've seen all rows.
        let mut consolidate = HashMap::new();
        for (rec, ts) in rx.into_iter() {
            use std::collections::BTreeSet;
            use std::cmp;

            let val = self.build(&rec[..]);
            let group = rec.into_iter()
                .enumerate()
                .filter_map(|(i, v)| {
                    if self.group.contains(&i) {
                        Some(v)
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();

            let mut cur = consolidate.entry(group).or_insert_with(|| (BTreeSet::new(), ts));
            cur.0.insert(val);
            cur.1 = cmp::max(ts, cur.1);
        }

        consolidate.into_iter()
            .map(|(mut group, (vals, ts))| {
                let mut val = vals.into_iter()
                    .fold(String::with_capacity(self.slen), |mut acc, s| {
                        acc.push_str(&s);
                        acc.push_str(self.separator);
                        acc
                    });
                // we pushed one separator too many above
                let real_len = val.len() - self.separator.len();
                val.truncate(real_len);
                group.push(val.into());
                // TODO: respect q.select
                (group, ts)
            })
            .collect()
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
    use query;
    use petgraph;
    use shortcut;

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
    fn it_queries() {
        let c = setup(false, false);

        let hits = c.find(None, None);
        assert_eq!(hits.len(), 2);
        assert!(hits.iter().any(|&(ref r, _)| r[0] == 1.into() && r[1] == ".1;".into()));
        assert!(hits.iter().any(|&(ref r, _)| r[0] == 2.into() && r[1] == ".1;#.2;".into()));

        let q = query::Query::new(&[true, true],
                                  vec![shortcut::Condition {
                             column: 0,
                             cmp: shortcut::Comparison::Equal(shortcut::Value::Const(2.into())),
                         }]);

        let hits = c.find(Some(&q), None);
        assert_eq!(hits.len(), 1);
        assert!(hits.iter().any(|&(ref r, _)| r[0] == 2.into() && r[1] == ".1;#.2;".into()));
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
