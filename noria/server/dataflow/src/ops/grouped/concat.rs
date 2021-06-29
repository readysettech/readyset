use crate::ops::grouped::GroupedOperation;
use crate::ops::grouped::GroupedOperator;

use std::collections::HashSet;

use crate::prelude::*;
use noria::{invariant, ReadySetResult};
use std::convert::TryFrom;

/// Designator for what a given position in a group concat output should contain.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TextComponent {
    /// Emit a literal string.
    Literal(String),
    /// Emit the string representation of the given column in the current record.
    Column(usize),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
    ///
    /// TODO: support case conditions
    /// CH: https://app.clubhouse.io/readysettech/story/198/add-filtering-to-all-grouped-operations
    pub fn new(
        src: NodeIndex,
        components: Vec<TextComponent>,
        separator: String,
    ) -> ReadySetResult<GroupedOperator<GroupConcat>> {
        invariant!(
            !separator.is_empty(),
            "group concat separator cannot be empty"
        );

        Ok(GroupedOperator::new(
            src,
            GroupConcat {
                components,
                separator,
                group: Vec::new(),
                slen: 0,
            },
        ))
    }

    fn build(&self, rec: &[DataType]) -> ReadySetResult<String> {
        let mut s = String::with_capacity(self.slen);
        for tc in &self.components {
            match *tc {
                TextComponent::Literal(ref l) => {
                    s.push_str(l);
                }
                TextComponent::Column(ref i) => match rec[*i] {
                    DataType::Text(..) | DataType::TinyText(..) => {
                        let text: &str = <&str>::try_from(&rec[*i])?;
                        s.push_str(text);
                    }
                    DataType::Int(ref n) => s.push_str(&n.to_string()),
                    DataType::UnsignedInt(ref n) => s.push_str(&n.to_string()),
                    DataType::BigInt(ref n) => s.push_str(&n.to_string()),
                    DataType::UnsignedBigInt(ref n) => s.push_str(&n.to_string()),
                    DataType::Real(..) => s.push_str(&rec[*i].to_string()),
                    DataType::Timestamp(ref ts) => s.push_str(&ts.format("%+").to_string()),
                    DataType::Time(ref t) => s.push_str(&t.to_string()),
                    DataType::None => unreachable!(),
                },
            }
        }

        Ok(s)
    }
}

impl GroupedOperation for GroupConcat {
    type Diff = Modify;

    fn setup(&mut self, parent: &Node) -> ReadySetResult<()> {
        // group by all columns
        let cols = parent.fields().len();
        let mut group = HashSet::new();
        group.extend(0..cols);
        // except the ones that are used in output
        for tc in &self.components {
            if let TextComponent::Column(col) = *tc {
                invariant!(col < cols, "group concat emits fields parent doesn't have");
                group.remove(&col);
            }
        }
        self.group = group.into_iter().collect();

        // how long are we expecting strings to be?
        self.slen = 0;
        // well, the length of all literal components
        for tc in &self.components {
            if let TextComponent::Literal(ref l) = *tc {
                self.slen += l.len();
            }
        }
        // plus some fixed size per value
        self.slen += 10 * (cols - self.group.len());
        Ok(())
    }

    fn group_by(&self) -> &[usize] {
        &self.group[..]
    }

    fn to_diff(&self, r: &[DataType], pos: bool) -> ReadySetResult<Self::Diff> {
        let v = self.build(r)?;
        if pos {
            Ok(Modify::Add(v))
        } else {
            Ok(Modify::Remove(v))
        }
    }

    fn apply(
        &self,
        current: Option<&DataType>,
        diffs: &mut dyn Iterator<Item = Self::Diff>,
    ) -> ReadySetResult<DataType> {
        use std::collections::BTreeSet;

        // updating the value is a bit tricky because we want to retain ordering of the
        // elements. we therefore need to first split the value, add the new ones,
        // remove revoked ones, sort, and then join again. ugh. we try to make it more
        // efficient by splitting into a BTree, which maintains sorting while
        // supporting efficient add/remove.

        use std::borrow::Cow;
        let current: &str = match current {
            Some(dt @ &DataType::Text(..)) | Some(dt @ &DataType::TinyText(..)) => {
                <&str>::try_from(dt)?
            }
            None => "",
            _ => unreachable!(),
        };
        let clen = current.len();

        // TODO this is not particularly robust, and requires a non-empty separator
        let mut current: BTreeSet<_> = current
            .split_terminator(&self.separator)
            .map(Cow::Borrowed)
            .collect();
        for diff in diffs {
            match diff {
                Modify::Add(s) => {
                    current.insert(Cow::Owned(s));
                }
                Modify::Remove(s) => {
                    current.remove(&*s);
                }
            }
        }

        // WHY doesn't rust have an iterator joiner?
        let mut new = current
            .into_iter()
            .fold(String::with_capacity(2 * clen), |mut acc, s| {
                acc.push_str(&*s);
                acc.push_str(&self.separator);
                acc
            });
        // we pushed one separator too many above
        let real_len = new.len() - self.separator.len();
        new.truncate(real_len);
        Ok(DataType::from(new))
    }

    fn description(&self, detailed: bool) -> String {
        if !detailed {
            return String::from("CONCAT");
        }

        let fields = self
            .components
            .iter()
            .map(|c| match *c {
                TextComponent::Literal(ref s) => format!("\"{}\"", s),
                TextComponent::Column(ref i) => i.to_string(),
            })
            .collect::<Vec<_>>()
            .join(", ");

        // Sort group by columns for consistent output.
        let mut group_cols = self.group.clone();
        group_cols.sort_unstable();
        let group_cols = group_cols
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ");

        format!("||([{}], \"{}\") γ[{}]", fields, self.separator, group_cols)
    }

    fn over_columns(&self) -> Vec<usize> {
        self.components
            .iter()
            .filter_map(|c| match *c {
                TextComponent::Column(c) => Some(c),
                _ => None,
            })
            .collect::<Vec<_>>()
    }

    fn output_col_type(&self) -> Option<nom_sql::SqlType> {
        Some(nom_sql::SqlType::Text)
    }

    fn empty_value(&self) -> Option<DataType> {
        Some("".into())
    }
}
