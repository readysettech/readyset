use ops::grouped::GroupedOperation;
use ops::grouped::GroupedOperator;

use std::collections::HashSet;

use flow::prelude::*;

/// Designator for what a given position in a group concat output should contain.
#[derive(Debug, Clone)]
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
#[derive(Debug, Clone)]
pub struct GroupConcat {
    components: Vec<TextComponent>,
    separator: String,
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
    pub fn new(src: NodeAddress,
               components: Vec<TextComponent>,
               separator: String)
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

    fn build(&self, rec: &[DataType]) -> String {
        let mut s = String::with_capacity(self.slen);
        for tc in &self.components {
            match *tc {
                TextComponent::Literal(l) => {
                    s.push_str(l);
                }
                TextComponent::Column(i) => {
                    match rec[i] {
                        DataType::Text(..) |
                        DataType::TinyText(..) => {
                            use std::borrow::Cow;
                            let text: Cow<str> = (&rec[i]).into();
                            s.push_str(&*text);
                        }
                        DataType::Int(ref n) => s.push_str(&n.to_string()),
                        DataType::BigInt(ref n) => s.push_str(&n.to_string()),
                        DataType::Real(..) => s.push_str(&rec[i].to_string()),
                        DataType::None => unreachable!(),
                    }
                }
            }
        }

        s
    }
}

impl GroupedOperation for GroupConcat {
    type Diff = Modify;

    fn setup(&mut self, parent: &Node) {
        // group by all columns
        let cols = parent.fields().len();
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

    fn to_diff(&self, r: &[DataType], pos: bool) -> Self::Diff {
        let v = self.build(r);
        if pos {
            Modify::Add(v)
        } else {
            Modify::Remove(v)
        }
    }

    fn apply(&self, current: Option<&DataType>, diffs: Vec<Self::Diff>) -> DataType {
        use std::collections::BTreeSet;
        use std::iter::FromIterator;

        // updating the value is a bit tricky because we want to retain ordering of the
        // elements. we therefore need to first split the value, add the new ones,
        // remove revoked ones, sort, and then join again. ugh. we try to make it more
        // efficient by splitting into a BTree, which maintains sorting while
        // supporting efficient add/remove.

        use std::borrow::Cow;
        let current: Cow<str> = match current {
            Some(dt @ &DataType::Text(..)) |
            Some(dt @ &DataType::TinyText(..)) => dt.into(),
            None => Cow::Borrowed(""),
            _ => unreachable!(),
        };
        let clen = current.len();

        // TODO this is not particularly robust, and requires a non-empty separator
        let mut current = BTreeSet::from_iter(current.split_terminator(&self.separator));
        for diff in &diffs {
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
        let mut new = current
            .into_iter()
            .fold(String::with_capacity(2 * clen), |mut acc, s| {
                acc.push_str(s);
                acc.push_str(&self.separator);
                acc
            });
        // we pushed one separator too many above
        let real_len = new.len() - self.separator.len();
        new.truncate(real_len);
        new.into()
    }

    fn description(&self) -> String {
        let fields = self.components
            .iter()
            .map(|c| match *c {
                     TextComponent::Literal(s) => format!("\"{}\"", s),
                     TextComponent::Column(i) => i.to_string(),
                 })
            .collect::<Vec<_>>()
            .join(", ");

        // Sort group by columns for consistent output.
        let mut group_cols = self.group.clone();
        group_cols.sort();
        let group_cols = group_cols
            .iter()
            .map(|g| g.to_string())
            .collect::<Vec<_>>()
            .join(", ");

        format!("||([{}], \"{}\") γ[{}]",
                fields,
                self.separator,
                group_cols)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use ops;

    fn setup(mat: bool) -> ops::test::MockGraph {
        let mut g = ops::test::MockGraph::new();
        let s = g.add_base("source", &["x", "y"]);

        let c = GroupConcat::new(s,
                                 vec![TextComponent::Literal("."),
                                      TextComponent::Column(1),
                                      TextComponent::Literal(";")],
                                 String::from("#"));
        g.set_op("concat", &["x", "ys"], c, mat);
        g
    }

    #[test]
    fn it_describes() {
        let c = setup(true);
        assert_eq!(c.node().description(), "||([\".\", 1, \";\"], \"#\") γ[0]");
    }

    #[test]
    fn it_forwards() {
        let mut c = setup(true);

        let u: Record = vec![1.into(), 1.into()].into();

        // first row for a group should emit +".1;" for that group
        let rs = c.narrow_one(u, true);
        assert_eq!(rs.len(), 1);
        let mut rs = rs.into_iter();

        match rs.next().unwrap() {
            Record::Positive(r) => {
                assert_eq!(r[0], 1.into());
                assert_eq!(r[1], ".1;".into());
            }
            _ => unreachable!(),
        }

        let u: Record = vec![2.into(), 2.into()].into();

        // first row for a second group should emit +".2;" for that new group
        let rs = c.narrow_one(u, true);
        assert_eq!(rs.len(), 1);
        let mut rs = rs.into_iter();

        match rs.next().unwrap() {
            Record::Positive(r) => {
                assert_eq!(r[0], 2.into());
                assert_eq!(r[1], ".2;".into());
            }
            _ => unreachable!(),
        }

        let u: Record = vec![1.into(), 2.into()].into();

        // second row for a group should emit -".1;" and +".1;#.2;"
        let rs = c.narrow_one(u, true);
        assert_eq!(rs.len(), 2);
        let mut rs = rs.into_iter();

        match rs.next().unwrap() {
            Record::Negative(r) => {
                assert_eq!(r[0], 1.into());
                assert_eq!(r[1], ".1;".into());
            }
            _ => unreachable!(),
        }
        match rs.next().unwrap() {
            Record::Positive(r) => {
                assert_eq!(r[0], 1.into());
                assert_eq!(r[1], ".1;#.2;".into());
            }
            _ => unreachable!(),
        }

        let u = (vec![1.into(), 1.into()], false);

        // negative row for a group should emit -".1;#.2;" and +".2;"
        let rs = c.narrow_one_row(u, true);
        assert_eq!(rs.len(), 2);
        let mut rs = rs.into_iter();

        match rs.next().unwrap() {
            Record::Negative(r) => {
                assert_eq!(r[0], 1.into());
                assert_eq!(r[1], ".1;#.2;".into());
            }
            _ => unreachable!(),
        }
        match rs.next().unwrap() {
            Record::Positive(r) => {
                assert_eq!(r[0], 1.into());
                assert_eq!(r[1], ".2;".into());
            }
            _ => unreachable!(),
        }

        let u = vec![// remove non-existing
                     (vec![1.into(), 1.into()], false),
                     // add old
                     (vec![1.into(), 1.into()], true),
                     // add duplicate
                     (vec![1.into(), 2.into()], true),
                     (vec![2.into(), 2.into()], false),
                     (vec![2.into(), 3.into()], true),
                     (vec![2.into(), 2.into()], true),
                     (vec![2.into(), 1.into()], true),
                     // new group
                     (vec![3.into(), 3.into()], true)];

        // multiple positives and negatives should update aggregation value by appropriate amount
        let rs = c.narrow_one(u, true);
        assert_eq!(rs.len(), 5); // one - and one + for each group, except last (new) group
        // group 1 had [2], now has [1,2]
        assert!(rs.iter()
                    .any(|r| if let Record::Negative(ref r) = *r {
                             if r[0] == 1.into() {
                                 assert_eq!(r[1], ".2;".into());
                                 true
                             } else {
                                 false
                             }
                         } else {
                             false
                         }));
        assert!(rs.iter()
                    .any(|r| if let Record::Positive(ref r) = *r {
                             if r[0] == 1.into() {
                                 assert_eq!(r[1], ".1;#.2;".into());
                                 true
                             } else {
                                 false
                             }
                         } else {
                             false
                         }));
        // group 2 was [2], is now [1,2,3]
        assert!(rs.iter()
                    .any(|r| if let Record::Negative(ref r) = *r {
                             if r[0] == 2.into() {
                                 assert_eq!(r[1], ".2;".into());
                                 true
                             } else {
                                 false
                             }
                         } else {
                             false
                         }));
        assert!(rs.iter()
                    .any(|r| if let Record::Positive(ref r) = *r {
                             if r[0] == 2.into() {
                                 assert_eq!(r[1], ".1;#.2;#.3;".into());
                                 true
                             } else {
                                 false
                             }
                         } else {
                             false
                         }));
        // group 3 was [], is now [3]
        assert!(rs.iter()
                    .any(|r| if let Record::Positive(ref r) = *r {
                             if r[0] == 3.into() {
                                 assert_eq!(r[1], ".3;".into());
                                 true
                             } else {
                                 false
                             }
                         } else {
                             false
                         }));
    }

    #[test]
    fn it_suggests_indices() {
        let me = NodeAddress::mock_global(1.into());
        let c = setup(false);
        let idx = c.node().suggest_indexes(me);

        // should only add index on own columns
        assert_eq!(idx.len(), 1);
        assert!(idx.contains_key(&me));

        // should only index on the group-by column
        assert_eq!(idx[&me], vec![0]);
    }

    #[test]
    fn it_resolves() {
        let c = setup(false);
        assert_eq!(c.node().resolve(0), Some(vec![(c.narrow_base_id(), 0)]));
        assert_eq!(c.node().resolve(1), None);
    }
}
