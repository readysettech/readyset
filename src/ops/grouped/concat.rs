use ops;
use query;
use flow::NodeIndex;

use ops::grouped::GroupedOperation;
use ops::grouped::GroupedOperator;

use std::collections::HashSet;

/// Designator for what a given position in a group concat output should contain.
#[derive(Debug)]
pub enum TextComponent {
    /// Emit a literal string.
    Literal(&'static str),
    /// Emit the string representation of the given column in the current record.
    Column(usize),
}

pub enum Modify {
    Add(String),
    Remove(String),
}

/// `GroupConcat` joins multiple records into one using string concatenation.
///
/// It is conceptually similar to the `group_concat` function available in most SQL databases. The
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
    group: Vec<usize>,
    slen: usize,
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
    pub fn new(src: NodeIndex,
               components: Vec<TextComponent>,
               separator: &'static str)
               -> GroupedOperator<GroupConcat> {
        assert!(!separator.is_empty(),
                "group concat separator cannot be empty");

        GroupedOperator::new(src,
                             GroupConcat {
                                 components: components,
                                 separator: separator,
                                 group: Vec::new(),
                                 slen: 0,
                             })
    }

    fn build(&self, rec: &[query::DataType]) -> String {
        let mut s = String::with_capacity(self.slen);
        for tc in &self.components {
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

impl GroupedOperation for GroupConcat {
    type Diff = Modify;

    fn setup(&mut self, parent: &ops::V) {
        // group by all columns
        let cols = parent.args().len();
        let mut group = HashSet::new();
        group.extend(0..cols);
        // except the ones that are used in output
        for tc in &self.components {
            if let TextComponent::Column(col) = *tc {
                assert!(col < cols, "group concat emits fields parent doesn't have");
                group.remove(&col);
            }
        }
        self.group = group.into_iter().collect();

        // how long are we expecting strings to be?
        self.slen = 0;
        // well, the length of all literal components
        for tc in &self.components {
            if let TextComponent::Literal(l) = *tc {
                self.slen += l.len();
            }
        }
        // plus some fixed size per value
        self.slen += 10 * (cols - self.group.len());
    }

    fn group_by(&self) -> &[usize] {
        &self.group[..]
    }

    fn zero(&self) -> query::DataType {
        query::DataType::from("")
    }

    fn to_diff(&self, r: &[query::DataType], pos: bool) -> Self::Diff {
        let v = self.build(r);
        if pos {
            Modify::Add(v)
        } else {
            Modify::Remove(v)
        }
    }

    fn apply(&self, current: &query::DataType, diffs: Vec<(Self::Diff, i64)>) -> query::DataType {
        use std::collections::BTreeSet;
        use std::iter::FromIterator;

        // updating the value is a bit tricky because we want to retain ordering of the
        // elements. we therefore need to first split the value, add the new ones,
        // remove revoked ones, sort, and then join again. ugh. we try to make it more
        // efficient by splitting into a BTree, which maintains sorting while
        // supporting efficient add/remove.

        let current = if let query::DataType::Text(ref s) = *current {
            s
        } else {
            unreachable!();
        };
        let clen = current.len();

        // TODO this is not particularly robust, and requires a non-empty separator
        let mut current = BTreeSet::from_iter(current.split_terminator(self.separator));
        for &(ref diff, _) in &diffs {
            match *diff {
                Modify::Add(ref s) => {
                    current.insert(s);
                }
                Modify::Remove(ref s) => {
                    current.remove(&**s);
                }
            }
        }

        // WHY doesn't rust have an iterator joiner?
        let mut new = current.into_iter()
            .fold(String::with_capacity(2 * clen), |mut acc, s| {
                acc.push_str(s);
                acc.push_str(self.separator);
                acc
            });
        // we pushed one separator too many above
        let real_len = new.len() - self.separator.len();
        new.truncate(real_len);
        new.into()
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

        g[s].as_ref().unwrap().process(Some((vec![1.into(), 1.into()], 0).into()), s, 0, true);
        g[s].as_ref().unwrap().process(Some((vec![2.into(), 1.into()], 1).into()), s, 1, true);
        g[s].as_ref().unwrap().process(Some((vec![2.into(), 2.into()], 2).into()), s, 2, true);

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
        let out = c.process(Some(u), src, 1, true);
        if let flow::ProcessingResult::Done(ops::Update::Records(rs)) = out {
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
        let out = c.process(Some(u), src, 2, true);
        if let flow::ProcessingResult::Done(ops::Update::Records(rs)) = out {
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
        let out = c.process(Some(u), src, 3, true);
        if let flow::ProcessingResult::Done(ops::Update::Records(rs)) = out {
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
        let out = c.process(Some(u), src, 4, true);
        if let flow::ProcessingResult::Done(ops::Update::Records(rs)) = out {
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
        let out = c.process(Some(u), src, 5, true);
        if let flow::ProcessingResult::Done(ops::Update::Records(rs)) = out {
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
