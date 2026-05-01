//! Subquery patterns: exists_subquery, in_subquery, scalar_subquery, join_subquery.

use readyset_sql::ast::JoinOperator;

use crate::constraint::{Constraint, SubqueryExprKind, TypeClass};
use crate::pattern::{Pattern, PatternBuilder};

/// SELECT t0.c0 FROM t0 WHERE EXISTS (SELECT 1 FROM t1 WHERE t1.c1 = t0.c2)
pub fn exists_subquery() -> Pattern {
    let mut b = PatternBuilder::new("exists_subquery");
    let t_outer = b.table();
    let c_outer_proj = b.column(t_outer);
    let c_outer_ref = b.column(t_outer);

    let mut sq = b.subquery();
    let t_inner = sq.table();
    let c_inner = sq.column(t_inner);
    sq.from(t_inner);
    sq.project_column(c_inner, t_inner);
    // Correlated: reference outer column in inner WHERE
    sq.constraint(Constraint::WhereColumnCompare {
        left_col: c_inner,
        left_table: t_inner,
        op: readyset_sql::ast::BinaryOperator::Equal,
        right_col: c_outer_ref,
        right_table: t_outer,
    });
    sq.commit_as_where(SubqueryExprKind::ExistsCorrelated);

    b.from(t_outer);
    b.project_column(c_outer_proj, t_outer);

    b.tags(&["subquery", "correlated"]);
    b.build()
}

/// SELECT t0.c0 FROM t0 WHERE t0.c1 IN (SELECT t1.c2 FROM t1)
pub fn in_subquery() -> Pattern {
    let mut b = PatternBuilder::new("in_subquery");
    let t_outer = b.table();
    let c_outer_proj = b.column(t_outer);
    let c_outer_in = b.column(t_outer);

    let mut sq = b.subquery();
    let t_inner = sq.table();
    let c_inner = sq.column(t_inner);
    sq.from(t_inner);
    sq.project_column(c_inner, t_inner);
    // Reference outer column to create shared_var for IN LHS
    sq.constraint(Constraint::TypeCompatible(c_outer_in, c_inner));
    sq.commit_as_where(SubqueryExprKind::InSubquery);

    b.from(t_outer);
    b.project_column(c_outer_proj, t_outer);

    b.tags(&["subquery"]);
    b.build()
}

/// SELECT (SELECT t1.c0 FROM t1 LIMIT 1), t0.c0 FROM t0
pub fn scalar_subquery() -> Pattern {
    let mut b = PatternBuilder::new("scalar_subquery");
    let t_outer = b.table();
    let c_outer = b.column(t_outer);

    let mut sq = b.subquery();
    let t_inner = sq.table();
    let c_inner = sq.column(t_inner);
    sq.from(t_inner);
    sq.project_column(c_inner, t_inner);
    sq.constraint(Constraint::Limit {
        limit: 1,
        offset: None,
    });
    sq.commit_as_where(SubqueryExprKind::ScalarSubquery);

    b.from(t_outer);
    b.project_column(c_outer, t_outer);

    b.tags(&["subquery"]);
    b.build()
}

/// SELECT t0.c0 FROM t0 INNER JOIN (SELECT t1.c1 FROM t1) AS sq0 ON t0.c2 = sq0.c1
pub fn join_subquery() -> Pattern {
    let mut b = PatternBuilder::new("join_subquery");
    let t_outer = b.table();
    let c_outer_proj = b.column(t_outer);
    let c_outer_join = b.column(t_outer);
    b.column_type_class(c_outer_join, TypeClass::Integer);

    let mut sq = b.subquery();
    let t_inner = sq.table();
    let c_inner = sq.column(t_inner);
    sq.column_type_class(c_inner, TypeClass::Integer);
    sq.from(t_inner);
    sq.project_column(c_inner, t_inner);
    sq.commit_as_join(JoinOperator::InnerJoin, c_outer_join, c_inner);

    b.from(t_outer);
    b.project_column(c_outer_proj, t_outer);

    b.tags(&["subquery", "join"]);
    b.build()
}

#[cfg(test)]
mod tests {
    use readyset_sql::Dialect;

    use super::*;
    use crate::test_util::resolve_pattern;

    #[test]
    fn exists_subquery_builds() {
        let p = exists_subquery();
        assert_eq!(p.name, "exists_subquery");
        assert!(p.tags.contains(&"subquery"));
        assert!(p.tags.contains(&"correlated"));
        assert!(p.min_depth >= 1);

        // Should have a Subquery constraint
        assert!(
            p.constraints
                .iter()
                .any(|c| matches!(c, Constraint::SubqueryExpr { .. }))
        );
    }

    #[test]
    fn in_subquery_builds() {
        let p = in_subquery();
        assert_eq!(p.name, "in_subquery");
        assert!(p.tags.contains(&"subquery"));

        assert!(
            p.constraints
                .iter()
                .any(|c| matches!(c, Constraint::SubqueryExpr { .. }))
        );
    }

    #[test]
    fn scalar_subquery_builds() {
        let p = scalar_subquery();
        assert_eq!(p.name, "scalar_subquery");
        assert!(p.tags.contains(&"subquery"));
        assert!(p.min_depth >= 1);
    }

    #[test]
    fn join_subquery_builds() {
        let p = join_subquery();
        assert_eq!(p.name, "join_subquery");
        assert!(p.tags.contains(&"subquery"));
        assert!(p.tags.contains(&"join"));

        // Should have a SubqueryRelation { kind: JoinTarget } sibling and
        // a Join referencing its alias.
        assert!(p.constraints.iter().any(|c| matches!(
            c,
            Constraint::SubqueryRelation {
                kind: crate::constraint::SubqueryRelationKind::JoinTarget,
                ..
            }
        )));
        assert!(p.constraints.iter().any(|c| matches!(
            c,
            Constraint::Join {
                right: crate::constraint::JoinRight::Table(_),
                ..
            }
        )));
    }

    #[test]
    fn exists_subquery_has_shared_vars() {
        let p = exists_subquery();
        // The correlated subquery should have shared_vars (outer column referenced inside)
        let sub = p
            .constraints
            .iter()
            .find(|c| matches!(c, Constraint::SubqueryExpr { .. }));
        if let Some(Constraint::SubqueryExpr { shared_vars, .. }) = sub {
            assert!(
                !shared_vars.is_empty(),
                "correlated subquery should have shared vars"
            );
        } else {
            panic!("expected Subquery constraint");
        }
    }

    #[test]
    fn in_subquery_has_shared_vars() {
        let p = in_subquery();
        let sub = p
            .constraints
            .iter()
            .find(|c| matches!(c, Constraint::SubqueryExpr { .. }));
        if let Some(Constraint::SubqueryExpr { shared_vars, .. }) = sub {
            assert!(
                !shared_vars.is_empty(),
                "IN subquery should have shared vars for LHS column"
            );
        } else {
            panic!("expected Subquery constraint");
        }
    }

    #[test]
    fn exists_subquery_resolves() {
        let p = exists_subquery();
        let sql = resolve_pattern(&p, Dialect::MySQL);
        assert!(sql.contains("EXISTS"), "sql: {sql}");
        assert!(sql.contains("SELECT"), "sql: {sql}");
    }

    #[test]
    fn in_subquery_resolves() {
        let p = in_subquery();
        let sql = resolve_pattern(&p, Dialect::MySQL);
        assert!(sql.contains("IN"), "sql: {sql}");
        assert!(sql.contains("SELECT"), "sql: {sql}");
    }

    #[test]
    fn join_subquery_resolves() {
        let p = join_subquery();
        let sql = resolve_pattern(&p, Dialect::MySQL);
        assert!(sql.contains("JOIN"), "sql: {sql}");
        assert!(sql.contains("SELECT"), "sql: {sql}");
    }

    /// Subquery scope must not leak inner-only BaseTable / ColumnExists into
    /// the outer pattern, otherwise the resolver synthesizes DDL for a
    /// phantom outer table that the outer query never references.
    #[test]
    fn exists_subquery_does_not_leak_inner_basetable_to_outer() {
        let p = exists_subquery();

        // The Subquery constraint sits on the outer pattern. Its inner
        // constraints contain BaseTable for the inner table; the outer
        // (top-level) constraints must not.
        let sub = p
            .constraints
            .iter()
            .find_map(|c| match c {
                Constraint::SubqueryExpr {
                    constraints: inner, ..
                } => Some(inner.as_slice()),
                _ => None,
            })
            .expect("expected a Subquery constraint");

        let inner_tables: Vec<_> = sub
            .iter()
            .filter_map(|c| match c {
                Constraint::BaseTable(v) => Some(*v),
                _ => None,
            })
            .collect();
        assert_eq!(
            inner_tables.len(),
            1,
            "inner subquery scope must hold its own BaseTable"
        );

        for c in &p.constraints {
            if let Constraint::BaseTable(v) = c {
                assert!(
                    !inner_tables.contains(v),
                    "outer constraints must not contain BaseTable for the inner-only relation {v:?}",
                );
            }
        }
    }
}
