//! CTE patterns: simple_cte, cte_with_join.

use crate::pattern::{Pattern, PatternBuilder};

/// WITH cte0 AS (SELECT t.c FROM t) SELECT cte0.c FROM cte0
pub fn simple_cte() -> Pattern {
    let mut b = PatternBuilder::new("simple_cte");

    let mut sq = b.subquery();
    let t_inner = sq.table();
    let c = sq.column(t_inner);
    sq.from(t_inner);
    sq.project_column(c, t_inner);
    let cte_alias = sq.commit_as_cte();

    // Outer query references the CTE alias, not the underlying base table.
    // The column var `c` keeps its name from the inner scope, but is
    // qualified by the CTE alias name on the outer side.
    b.from(cte_alias);
    b.project_column(c, cte_alias);

    b.tags(&["cte"]);
    b.build()
}

/// WITH cte0 AS (SELECT t0.c0 FROM t0) SELECT cte0.c0, t1.c1 FROM cte0 INNER JOIN t1 ON ...
pub fn cte_with_join() -> Pattern {
    let mut b = PatternBuilder::new("cte_with_join");

    let mut sq = b.subquery();
    let t_inner = sq.table();
    let c_cte = sq.column(t_inner);
    sq.from(t_inner);
    sq.project_column(c_cte, t_inner);
    let cte_alias = sq.commit_as_cte();

    // Outer: join CTE with another table. The CTE body's inner table is
    // hidden behind the alias here, so we don't need a cross-scope NotEq
    // to keep them distinct — a self-join through `cte0` is semantically
    // fine even when the CTE happens to wrap the same base table.
    let t_join = b.table();
    let c_join = b.column(t_join);
    let c_join_key = b.column(t_join);

    b.from(cte_alias);
    b.project_column(c_cte, cte_alias);
    b.project_column(c_join, t_join);
    b.join_table(
        readyset_sql::ast::JoinOperator::InnerJoin,
        t_join,
        c_cte,
        c_join_key,
    );

    b.tags(&["cte", "join"]);
    b.build()
}

#[cfg(test)]
mod tests {
    use readyset_sql::Dialect;

    use super::*;
    use crate::constraint::Constraint;
    use crate::test_util::resolve_pattern;

    #[test]
    fn simple_cte_builds() {
        let p = simple_cte();
        assert_eq!(p.name, "simple_cte");
        assert!(p.tags.contains(&"cte"));
        assert!(p.min_depth >= 1);

        // Should have a Cte constraint
        assert!(
            p.constraints
                .iter()
                .any(|c| matches!(c, Constraint::SubqueryRelation { .. }))
        );
    }

    #[test]
    fn cte_with_join_builds() {
        let p = cte_with_join();
        assert_eq!(p.name, "cte_with_join");
        assert!(p.tags.contains(&"cte"));
        assert!(p.tags.contains(&"join"));

        // Should have both Cte and Join constraints
        assert!(
            p.constraints
                .iter()
                .any(|c| matches!(c, Constraint::SubqueryRelation { .. }))
        );
        assert!(
            p.constraints
                .iter()
                .any(|c| matches!(c, Constraint::Join { .. }))
        );
    }

    #[test]
    fn simple_cte_resolves() {
        let p = simple_cte();
        let sql = resolve_pattern(&p, Dialect::MySQL);
        assert!(sql.contains("WITH"), "sql: {sql}");
        assert!(sql.contains("SELECT"), "sql: {sql}");
        // The outer query must reference the CTE alias, not the underlying
        // base table.
        assert!(
            sql.contains("FROM `cte0`"),
            "outer FROM should use CTE alias: {sql}"
        );
    }

    #[test]
    fn cte_with_join_resolves() {
        let p = cte_with_join();
        let sql = resolve_pattern(&p, Dialect::MySQL);
        assert!(sql.contains("WITH"), "sql: {sql}");
        assert!(sql.contains("JOIN"), "sql: {sql}");
        // The outer FROM must reference the CTE alias `cte0`, not the
        // underlying inner table.
        assert!(
            sql.contains("FROM `cte0`"),
            "outer FROM should use CTE alias: {sql}"
        );
    }
}
