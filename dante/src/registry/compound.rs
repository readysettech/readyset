//! Compound SELECT patterns: UNION ALL, etc.

use readyset_sql::ast::{CompoundSelectOperator, SqlType};

use crate::constraint::Constraint;
use crate::pattern::{Pattern, PatternBuilder};

/// SELECT t.c0 FROM t UNION ALL SELECT t.c0 FROM t
///
/// Both branches project the same column from the same table. Readyset's
/// Union MIR node requires all branches to have matching column names,
/// so the simplest correct pattern reuses the same table and column.
pub fn union_all_same_table() -> Pattern {
    let mut b = PatternBuilder::new("union_all_same_table");
    let t = b.table();
    let c = b.column(t);

    b.compound_select(
        CompoundSelectOperator::UnionAll,
        vec![
            vec![
                Constraint::From(t),
                Constraint::ProjectColumn { col: c, table: t },
            ],
            vec![
                Constraint::From(t),
                Constraint::ProjectColumn { col: c, table: t },
            ],
        ],
    );
    b.tags(&["compound", "union"]);
    b.build()
}

/// SELECT t1.c FROM t1 UNION ALL SELECT t2.c FROM t2 with heterogeneous
/// declared types unified by [`Constraint::TypeCompatible`].
///
/// `c1` is declared `Integer` (resolves to Int/BigInt/...) and `c2` is
/// pinned to exact `Double`. Pinning `c2` to a float type — rather than
/// the broader `Numeric` class which also covers Int/BigInt — guarantees
/// every seed exercises a true cross-type pair, not an Int/Int collapse.
/// The `TypeCompatible(c1, c2)` constraint anchors the resolver's
/// cross-class type-check on the MIR Union node and is a regression
/// anchor if `types_compatible` is ever narrowed.
pub fn union_all_cross_type() -> Pattern {
    let mut b = PatternBuilder::new("union_all_cross_type");
    let t1 = b.table();
    let t2 = b.table();
    let c1 = b.column(t1);
    let c2 = b.column(t2);
    b.not_eq(t1, t2);
    b.column_type_class(c1, crate::constraint::TypeClass::Integer);
    b.column_type_class(c2, crate::constraint::TypeClass::Exact(SqlType::Double));
    b.type_compatible(c1, c2);

    b.compound_select(
        CompoundSelectOperator::UnionAll,
        vec![
            vec![
                Constraint::From(t1),
                Constraint::ProjectColumn { col: c1, table: t1 },
            ],
            vec![
                Constraint::From(t2),
                Constraint::ProjectColumn { col: c2, table: t2 },
            ],
        ],
    );
    b.tags(&["compound", "union"]);
    b.build()
}

/// SELECT t1.c FROM t1 UNION ALL SELECT t2.c FROM t2
///
/// Branches FROM two distinct tables projecting type-compatible columns.
/// Exercises compound-select MIR with heterogeneous base relations and
/// distinct Base nodes feeding a single Union — a different code path
/// from the same-table variant where both branches share state.
pub fn union_all_two_tables(class: crate::constraint::TypeClass) -> Pattern {
    let name: &'static str = match &class {
        crate::constraint::TypeClass::Integer => "union_all_two_tables_int",
        crate::constraint::TypeClass::Numeric => "union_all_two_tables_numeric",
        crate::constraint::TypeClass::String => "union_all_two_tables_string",
        crate::constraint::TypeClass::DateTime => "union_all_two_tables_datetime",
        crate::constraint::TypeClass::Any | crate::constraint::TypeClass::Exact(_) => {
            "union_all_two_tables_any"
        }
    };
    let mut b = PatternBuilder::new(name);
    let t1 = b.table();
    let t2 = b.table();
    let c1 = b.column(t1);
    let c2 = b.column(t2);
    b.not_eq(t1, t2);
    // Pin both projection columns to the same class so the union's column
    // types match, but vary the class across registered variants — pinning
    // a single class (e.g., Integer) would hide cross-type bugs.
    b.column_type_class(c1, class.clone());
    b.column_type_class(c2, class);

    b.compound_select(
        CompoundSelectOperator::UnionAll,
        vec![
            vec![
                Constraint::From(t1),
                Constraint::ProjectColumn { col: c1, table: t1 },
            ],
            vec![
                Constraint::From(t2),
                Constraint::ProjectColumn { col: c2, table: t2 },
            ],
        ],
    );
    b.tags(&["compound", "union"]);
    b.build()
}

#[cfg(test)]
mod tests {
    use readyset_sql::Dialect;

    use super::*;
    use crate::test_util::resolve_pattern;

    #[test]
    fn union_all_same_table_builds() {
        let p = union_all_same_table();
        assert_eq!(p.name, "union_all_same_table");
        assert_eq!(p.tags, vec!["compound", "union"]);
        assert_eq!(p.min_depth, 0); // no subquery nesting
        assert_eq!(p.num_vars(), 2); // 1 table + 1 column
    }

    #[test]
    fn union_all_same_table_resolves() {
        let p = union_all_same_table();
        let sql = resolve_pattern(&p, Dialect::MySQL);
        assert!(sql.contains("UNION ALL"), "expected UNION ALL in: {sql}");
        assert!(sql.contains("SELECT"), "expected SELECT in: {sql}");
    }

    #[test]
    fn union_all_cross_type_resolves() {
        // Pattern declares c1: Integer, c2: Numeric, with TypeCompatible(c1, c2).
        // Resolver must accept the heterogeneous classes (Integer ⊂ Numeric)
        // and emit a UNION ALL across two distinct base tables.
        let p = union_all_cross_type();
        assert!(
            p.constraints
                .iter()
                .any(|c| matches!(c, crate::constraint::Constraint::TypeCompatible(_, _))),
            "cross-type pattern must carry a TypeCompatible constraint"
        );
        let sql = resolve_pattern(&p, Dialect::MySQL);
        assert!(sql.contains("UNION ALL"), "expected UNION ALL in: {sql}");
        assert!(
            sql.contains("FROM `t0`") && sql.contains("FROM `t1`"),
            "expected both `t0` and `t1` as base FROMs in: {sql}"
        );
    }

    #[test]
    fn union_all_two_tables_resolves() {
        use crate::constraint::TypeClass;
        for class in [TypeClass::Integer, TypeClass::String, TypeClass::DateTime] {
            let p = union_all_two_tables(class.clone());
            let sql = resolve_pattern(&p, Dialect::MySQL);
            assert!(
                sql.contains("UNION ALL"),
                "expected UNION ALL in {class:?}: {sql}"
            );
            // Distinct base tables in the two branches.
            assert!(
                sql.contains("FROM `t0`") && sql.contains("FROM `t1`"),
                "expected both `t0` and `t1` as base FROMs in {class:?}: {sql}"
            );
        }
    }
}
