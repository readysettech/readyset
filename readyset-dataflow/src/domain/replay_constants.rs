//! Reading a base table by the constants of the filter node that follows it. A replay from the
//! table looks up each key with those constants in front, so it skips rows the filter would drop.

use std::collections::BTreeMap;
use std::fmt;

use dataflow_expression::{BinaryOperator, Expr};
use itertools::Itertools;
use readyset_client::{KeyComparison, ReplayKeys};
use readyset_data::DfType;
use readyset_util::ranges::Bound;
use serde::{Deserialize, Serialize};
use vec1::Vec1;

use crate::node::Column;
use crate::prelude::*;

/// The maximum number of index lookups for one replay key
const MAX_LOOKUPS: usize = 8;

/// The widest BTreeMap index the persistent store has a fixed-width comparator for
const MAX_BTREE_COLUMNS: usize = 6;

/// The constants a replay path puts in front of each key when it reads its source, a base table
/// followed by a filter node
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReplayConstants {
    /// The index to read, on some of the filter's constant columns followed by the key columns
    index: Index,
    /// The values to put in front of each key, one combination of the constants per lookup
    prefixes: Box<[Box<[DfValue]>]>,
}

impl ReplayConstants {
    /// The constants for a replay keyed on `key` that reads `base`. `filter` is the next node on
    /// the replay path, and the result is `None` unless it is a filter node with constants that
    /// make the lookup cheaper.
    pub fn new(base: &Node, filter: &Node, key: &Index) -> Option<Self> {
        let (Some(table), Some(NodeOperator::Filter(filter))) =
            (base.get_base(), filter.as_internal())
        else {
            return None;
        };
        // A point lookup on a unique key finds at most one row, so constants can't make it
        // cheaper. A range lookup can find many rows even on a unique key.
        let key_is_unique = table
            .all_unique_keys()
            .iter()
            .any(|unique_key| unique_key.iter().all(|column| key.columns.contains(column)));
        if key.index_type == IndexType::HashMap && key_is_unique {
            return None;
        }
        let mut constants_by_column = equalities(filter.expression(), base.columns(), MAX_LOOKUPS);
        // The key already gives the value of its own columns
        constants_by_column.retain(|column, _| !key.columns.contains(column));

        // The persistent store compares BTreeMap keys of at most `MAX_BTREE_COLUMNS` columns, and
        // the key's own columns count toward that
        let max_constant_columns = match key.index_type {
            IndexType::BTreeMap => MAX_BTREE_COLUMNS.saturating_sub(key.columns.len()),
            IndexType::HashMap => constants_by_column.len(),
        };
        // A key needs one lookup per combination of the picked columns' values, so the lookups
        // multiply. Picking columns with fewer values first fits more of them under `MAX_LOOKUPS`.
        let mut columns_by_value_count: Vec<_> = constants_by_column
            .iter()
            .map(|(column, values)| (values.len(), *column))
            .collect();
        columns_by_value_count.sort_unstable();
        let mut picked_columns = Vec::new();
        let mut lookups_per_key = 1;
        for (value_count, column) in columns_by_value_count {
            if picked_columns.len() < max_constant_columns
                && lookups_per_key * value_count <= MAX_LOOKUPS
            {
                lookups_per_key *= value_count;
                picked_columns.push(column);
            }
        }
        if picked_columns.is_empty() {
            return None;
        }
        // Replays that pick the same columns then use the same index, whatever their value counts
        picked_columns.sort_unstable();
        let prefixes = picked_columns
            .iter()
            .map(|column| constants_by_column[column].iter().cloned())
            .multi_cartesian_product()
            .map(Vec::into_boxed_slice)
            .collect();
        // The constants go in front of the key, so a range on the key is still a single range in a
        // BTreeMap index
        let index_columns = picked_columns
            .into_iter()
            .chain(key.columns.iter().copied())
            .collect();
        Some(Self {
            index: Index::new(key.index_type, index_columns),
            prefixes,
        })
    }

    /// The index the replay reads
    pub fn index(&self) -> &Index {
        &self.index
    }

    /// How many lookups each key turns into
    pub(super) fn lookups_per_key(&self) -> usize {
        self.prefixes.len()
    }

    /// Each key in `keys` once per prefix, with the prefix in front. A BTreeMap index only answers
    /// range lookups, so point keys become ranges over a single key there.
    pub(super) fn prefixed_keys(&self, keys: &ReplayKeys) -> ReplayKeys {
        let mut prefixed = ReplayKeys::new();
        for prefix in self.prefixes.iter() {
            let with_prefix = |key: &Vec1<DfValue>| {
                let mut k = Vec::with_capacity(prefix.len() + key.len());
                k.extend(prefix.iter().chain(&key[..key.len() - 1]).cloned());
                Vec1::from_vec_push(k, key.last().clone())
            };
            for key in keys.equals().map(with_prefix) {
                prefixed.insert(match self.index.index_type {
                    IndexType::HashMap => KeyComparison::Equal(key),
                    IndexType::BTreeMap => {
                        KeyComparison::Range((Bound::Included(key.clone()), Bound::Included(key)))
                    }
                });
            }
            for (lower, upper) in keys.ranges() {
                let range = (
                    lower.as_ref().map(with_prefix),
                    upper.as_ref().map(with_prefix),
                );
                prefixed.insert(KeyComparison::Range(range));
            }
        }
        prefixed
    }
}

impl fmt::Display for ReplayConstants {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let prefixes = self
            .prefixes
            .iter()
            .map(|p| format!("({})", p.iter().join(", ")));
        write!(f, "{} by {}", self.index, prefixes.format(", "))
    }
}

/// A map from column index to the constant values that `filter`'s top-level `col = c` and
/// `col IN (c, ..)` conditions allow in that column. Only columns where an index lookup of those
/// values finds exactly the rows the conditions keep are included. When several conditions
/// restrict a column, it gets the values they all allow. A condition with more than `max_values`
/// values is ignored.
fn equalities(
    filter: &Expr,
    columns: &[Column],
    max_values: usize,
) -> BTreeMap<usize, Vec<DfValue>> {
    let mut found = BTreeMap::new();
    for condition in chain(filter, BinaryOperator::And) {
        if let Some((col, values)) = in_list(condition, columns, max_values) {
            found
                .entry(col)
                .and_modify(|kept: &mut Vec<DfValue>| {
                    kept.retain(|v| values.iter().any(|w| same_key(v, w)))
                })
                .or_insert(values);
        }
    }
    found
}

/// The column and distinct values of `col = c1 OR col = c2 ..`. `None` if
/// - there are more than `max_values` values
/// - an index lookup of some value could return rows that the filter drops, or miss rows that it
///   keeps
fn in_list(expr: &Expr, columns: &[Column], max_values: usize) -> Option<(usize, Vec<DfValue>)> {
    let comparisons: Vec<(usize, DfValue)> = chain(expr, BinaryOperator::Or)
        .into_iter()
        .map(|operand| column_equals(operand, columns))
        .collect::<Option<_>>()?;
    // All the comparisons must be on the same column
    let col = comparisons
        .iter()
        .map(|(col, _)| *col)
        .all_equal_value()
        .ok()?;
    let mut values: Vec<DfValue> = Vec::new();
    for (_, value) in comparisons {
        // Values with the same index key are a single lookup
        if !values.iter().any(|v| same_key(v, &value)) {
            if values.len() == max_values {
                return None;
            }
            values.push(value);
        }
    }
    Some((col, values))
}

/// The column and value of `col = value`, if an index lookup of the value returns exactly the
/// rows where `col = value` is true
fn column_equals(expr: &Expr, columns: &[Column]) -> Option<(usize, DfValue)> {
    let Expr::Op {
        op: BinaryOperator::Equal,
        left,
        right,
        ..
    } = expr
    else {
        return None;
    };
    let ((col, cast), constant) = match (column_of(left), column_of(right)) {
        (Some(col), None) => (col, right),
        (None, Some(col)) => (col, left),
        _ => return None,
    };
    let ty = columns.get(col)?.ty();
    let value = constant_value(constant)?;
    // Without a cast, the comparison uses the column's own type
    can_push(ty, cast.unwrap_or(ty), &value).then_some((col, value))
}

/// The operands of a chain of `op`, such as `a AND b AND c`. It uses a stack instead of recursion
/// because an IN list becomes nested ORs, one level per value.
fn chain(expr: &Expr, op: BinaryOperator) -> Vec<&Expr> {
    let mut stack = vec![expr];
    let mut operands = Vec::new();
    while let Some(expr) = stack.pop() {
        match expr {
            Expr::Op {
                op: o, left, right, ..
            } if *o == op => stack.extend([right.as_ref(), left.as_ref()]),
            _ => operands.push(expr),
        }
    }
    operands
}

/// The column `expr` reads, directly or through a cast, and the cast's type
fn column_of(expr: &Expr) -> Option<(usize, Option<&DfType>)> {
    match expr {
        Expr::Column { index, .. } => Some((*index, None)),
        Expr::Cast { expr, ty, .. } => match expr.as_ref() {
            Expr::Column { index, .. } => Some((*index, Some(ty))),
            _ => None,
        },
        _ => None,
    }
}

/// The value of a literal, cast or not
fn constant_value(expr: &Expr) -> Option<DfValue> {
    let mut literal = expr;
    while let Expr::Cast { expr: inner, .. } = literal {
        literal = inner;
    }
    matches!(literal, Expr::Literal { .. })
        .then(|| expr.eval::<DfValue>(&[]).ok())
        .flatten()
}

/// Whether a lookup of `value` in an index on a `column` column finds exactly the rows that
/// `column = value` keeps, with the column compared as `compared_as`
fn can_push(column: &DfType, compared_as: &DfType, value: &DfValue) -> bool {
    // Integers compare by value under any integer cast. A cast that fails on an out-of-range
    // value makes the filter drop the row, and a lookup of `value` doesn't find it either.
    column.is_any_int() && compared_as.is_any_int() && value.is_integer()
}

/// Whether `a` and `b` have the same index key
fn same_key(a: &DfValue, b: &DfValue) -> bool {
    a.transform_for_serialized_key() == b.transform_for_serialized_key()
}

#[cfg(test)]
mod tests {
    use dataflow_expression::utils::{column_with_type, make_int_column, make_literal};
    use readyset_data::Collation;

    use super::*;
    use crate::node::special::Base;
    use crate::ops::filter::Filter;

    fn op(op: BinaryOperator, left: Expr, right: Expr) -> Expr {
        Expr::Op {
            left: Box::new(left),
            op,
            right: Box::new(right),
            ty: DfType::Bool,
        }
    }

    fn eq(left: Expr, right: Expr) -> Expr {
        op(BinaryOperator::Equal, left, right)
    }

    fn any_of(conditions: impl IntoIterator<Item = Expr>) -> Expr {
        let or = |l, r| op(BinaryOperator::Or, l, r);
        conditions.into_iter().reduce(or).unwrap()
    }

    fn all_of(conditions: impl IntoIterator<Item = Expr>) -> Expr {
        let and = |l, r| op(BinaryOperator::And, l, r);
        conditions.into_iter().reduce(and).unwrap()
    }

    fn cast(expr: Expr, ty: DfType) -> Expr {
        Expr::Cast {
            expr: Box::new(expr),
            ty,
            null_on_failure: false,
            dialect: dataflow_expression::Dialect::DEFAULT_MYSQL,
        }
    }

    fn columns(types: &[DfType]) -> Vec<Column> {
        let named = |(i, ty): (usize, &DfType)| Column::new(format!("c{i}").into(), ty.clone());
        types.iter().enumerate().map(named).collect()
    }

    fn equals(expr: Expr, types: &[DfType]) -> Vec<(usize, Vec<DfValue>)> {
        equalities(&expr, &columns(types), MAX_LOOKUPS)
            .into_iter()
            .collect()
    }

    #[test]
    fn equalities_of_integers() {
        let types = [
            DfType::Int,
            DfType::Int,
            DfType::Int,
            DfType::BigInt,
            DfType::Int,
        ];
        let col_eq = |col: usize, v: i32| eq(make_int_column(col), make_literal(v.into()));
        let conditions = [
            any_of([col_eq(0, 2), col_eq(0, 7)]),
            col_eq(0, 2),
            any_of([col_eq(1, 3), col_eq(1, 4), col_eq(1, 3)]),
            Expr::Not {
                expr: Box::new(col_eq(2, 1)),
                ty: DfType::Bool,
            },
            eq(make_literal(5.into()), column_with_type(3, DfType::BigInt)),
            // MySQL compares integers of different types as BIGINT
            eq(
                cast(make_int_column(4), DfType::BigInt),
                cast(make_literal(8.into()), DfType::BigInt),
            ),
        ];
        let found = vec![
            (0, vec![2.into()]),
            (1, vec![3.into(), 4.into()]),
            (3, vec![5.into()]),
            (4, vec![8.into()]),
        ];
        assert_eq!(equals(all_of(conditions), &types), found);
    }

    #[test]
    fn equalities_leave_out_what_a_lookup_could_miss() {
        let types = [DfType::Int, DfType::Int, DfType::Double];
        let col_eq = |col: usize, v: DfValue| eq(make_int_column(col), make_literal(v));
        let int_list = |n: i32| any_of((1..=n).map(|v| col_eq(0, v.into())));
        for expr in [
            any_of([col_eq(0, 1.into()), col_eq(1, 2.into())]),
            eq(make_int_column(0), make_int_column(1)),
            col_eq(0, DfValue::None),
            op(
                BinaryOperator::Greater,
                make_int_column(0),
                make_literal(1.into()),
            ),
            col_eq(0, DfValue::Double(1.0)),
            eq(
                cast(make_int_column(0), DfType::Double),
                make_literal(DfValue::Double(1.0)),
            ),
            eq(
                column_with_type(2, DfType::Double),
                make_literal(DfValue::Double(1.0)),
            ),
            int_list(MAX_LOOKUPS as i32 + 1),
        ] {
            let shown = expr.to_string();
            assert_eq!(equals(expr, &types), vec![], "{shown}");
        }
        let all = (1..=MAX_LOOKUPS as i32).map(DfValue::from).collect();
        assert_eq!(equals(int_list(MAX_LOOKUPS as i32), &types), vec![(0, all)]);
        // A list over the cap is ignored, and the column's other conditions still apply
        let narrower = all_of([int_list(MAX_LOOKUPS as i32 + 1), col_eq(0, 2.into())]);
        assert_eq!(equals(narrower, &types), vec![(0, vec![2.into()])]);
    }

    #[test]
    fn prefixed_keys_put_each_prefix_in_front() {
        let text = |s: &str| DfValue::from_str_and_collation(s, Collation::Utf8Shifted);
        let (a, b) = (text("a"), text("b"));
        let constants = |index| ReplayConstants {
            index,
            prefixes: Box::from([
                Box::from([2.into(), a.clone()]),
                Box::from([2.into(), b.clone()]),
            ]),
        };
        let keys = |keys: Vec<KeyComparison>| keys.into_iter().collect::<ReplayKeys>();

        let point = constants(Index::hash_map(vec![2, 3, 1]));
        assert_eq!(point.to_string(), "HashMap[2, 3, 1] by (2, a), (2, b)");
        assert_eq!(
            point.prefixed_keys(&keys(vec![KeyComparison::Equal(vec1![5.into()])])),
            keys(vec![
                KeyComparison::Equal(vec1![2.into(), a.clone(), 5.into()]),
                KeyComparison::Equal(vec1![2.into(), b.clone(), 5.into()]),
            ])
        );

        // On a BTreeMap index, point keys become single-key ranges and ranges keep both bounds
        let range = constants(Index::btree_map(vec![2, 3, 1, 4]));
        let (k, m, max) = (text("k"), text("m"), DfValue::Max);
        let single = |key: Vec1<DfValue>| {
            KeyComparison::Range((Bound::Included(key.clone()), Bound::Included(key)))
        };
        let after = |prefix: &[DfValue]| {
            let bound =
                |last: DfValue| Vec1::try_from_vec([prefix, &[5.into(), last]].concat()).unwrap();
            KeyComparison::Range((
                Bound::Excluded(bound(k.clone())),
                Bound::Included(bound(max.clone())),
            ))
        };
        assert_eq!(
            range.prefixed_keys(&keys(vec![
                KeyComparison::Equal(vec1![5.into(), m.clone()]),
                after(&[])
            ])),
            keys(vec![
                single(vec1![2.into(), a.clone(), 5.into(), m.clone()]),
                single(vec1![2.into(), b.clone(), 5.into(), m]),
                after(&[2.into(), a]),
                after(&[2.into(), b]),
            ])
        );
    }

    /// A base table of 8 integer columns with primary key column 0, followed by a filter node that
    /// keeps `c2 IN (kinds) AND c3 IN (grades) AND c4 = 1`
    fn base_and_filter(kinds: &[i32], grades: &[i32]) -> (Node, Node) {
        let col_eq = |col, v: i32| eq(make_int_column(col), make_literal(v.into()));
        let listed = |col, values: &[i32]| any_of(values.iter().map(|&v| col_eq(col, v)));
        let expr = all_of([listed(2, kinds), listed(3, grades), col_eq(4, 1)]);
        let types = vec![DfType::Int; 8];
        let base = Node::new("base", columns(&types), Base::new().with_primary_key([0]));
        let filter = Filter::new(NodeIndex::new(0), expr);
        (
            base,
            Node::new("filter", columns(&types), NodeOperator::Filter(filter)),
        )
    }

    fn read(nodes: &(Node, Node), key: Index) -> Option<String> {
        let constants = ReplayConstants::new(&nodes.0, &nodes.1, &key);
        constants.map(|constants| constants.to_string())
    }

    #[test]
    fn constants_from_a_filter_node_below_a_base() {
        let nodes = base_and_filter(&[2], &[7, 8]);
        let (base, filter) = &nodes;

        // The constant columns in column order, then the key columns
        let point = read(&nodes, Index::hash_map(vec![1]));
        assert_eq!(
            point.as_deref(),
            Some("HashMap[2, 3, 4, 1] by (2, 7, 1), (2, 8, 1)")
        );
        let range = read(&nodes, Index::btree_map(vec![1, 5]));
        assert_eq!(
            range.as_deref(),
            Some("BTreeMap[2, 3, 4, 1, 5] by (2, 7, 1), (2, 8, 1)")
        );

        // A constant column that is also a key column is left out
        let pinned = read(&nodes, Index::hash_map(vec![3]));
        assert_eq!(pinned.as_deref(), Some("HashMap[2, 4, 3] by (2, 1)"));

        // A point lookup covering the primary key reads one row at most, and a range over it can
        // read many
        assert_eq!(read(&nodes, Index::hash_map(vec![0])), None);
        let primary_range = read(&nodes, Index::btree_map(vec![0]));
        assert_eq!(
            primary_range.as_deref(),
            Some("BTreeMap[2, 3, 4, 0] by (2, 7, 1), (2, 8, 1)")
        );

        // Only a base table followed by a filter node gets constants
        for key in [Index::hash_map(vec![1]), Index::btree_map(vec![1])] {
            assert!(ReplayConstants::new(base, base, &key).is_none());
            assert!(ReplayConstants::new(filter, filter, &key).is_none());
        }
    }

    #[test]
    fn constants_stay_within_the_lookup_and_index_limits() {
        // Columns with fewer values are picked first, while the lookups per key stay within the cap
        let many_kinds = base_and_filter(&[1, 2, 3, 4, 5], &[7, 8]);
        let capped = read(&many_kinds, Index::hash_map(vec![1]));
        assert_eq!(
            capped.as_deref(),
            Some("HashMap[3, 4, 1] by (7, 1), (8, 1)")
        );

        // A BTreeMap index has at most `MAX_BTREE_COLUMNS` columns
        let nodes = base_and_filter(&[2], &[7, 8]);
        let wide = read(&nodes, Index::btree_map(vec![1, 5, 6, 7]));
        assert_eq!(
            wide.as_deref(),
            Some("BTreeMap[2, 4, 1, 5, 6, 7] by (2, 1)")
        );
    }
}
