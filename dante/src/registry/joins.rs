//! Join patterns: inner, left, self-join, cross join.

use readyset_sql::ast::{BinaryOperator, JoinOperator};

use crate::constraint::{Constraint, DialectSupport, TypeClass};
use crate::pattern::{Pattern, PatternBuilder};

/// SELECT t0.c0, t1.c1 FROM t0 INNER JOIN t1 ON t0.c2 = t1.c3
pub fn inner_join() -> Pattern {
    let mut b = PatternBuilder::new("inner_join");
    let t1 = b.table();
    let t2 = b.table();
    b.not_eq(t1, t2);

    let c1 = b.column(t1);
    let c2 = b.column(t2);
    let jc1 = b.column(t1);
    let jc2 = b.column(t2);

    b.column_type_class(jc1, TypeClass::Integer);
    b.column_type_class(jc2, TypeClass::Integer);
    b.type_compatible(jc1, jc2);

    b.from(t1);
    b.join_table(JoinOperator::InnerJoin, t2, jc1, jc2);
    b.project_column(c1, t1);
    b.project_column(c2, t2);

    b.tags(&["join"]);
    b.build()
}

/// SELECT t0.c0, t1.c1 FROM t0 LEFT JOIN t1 ON t0.c2 = t1.c3
pub fn left_join() -> Pattern {
    let mut b = PatternBuilder::new("left_join");
    let t1 = b.table();
    let t2 = b.table();
    b.not_eq(t1, t2);

    let c1 = b.column(t1);
    let c2 = b.column(t2);
    let jc1 = b.column(t1);
    let jc2 = b.column(t2);

    b.column_type_class(jc1, TypeClass::Integer);
    b.column_type_class(jc2, TypeClass::Integer);
    b.type_compatible(jc1, jc2);

    b.from(t1);
    b.join_table(JoinOperator::LeftJoin, t2, jc1, jc2);
    b.project_column(c1, t1);
    b.project_column(c2, t2);

    b.tags(&["join"]);
    b.build()
}

/// SELECT t0.c0, t1.c1 FROM t0 LEFT JOIN t1 ON t0.c2 = t1.c3 WHERE t1.c4 = ?
///
/// LEFT JOIN with a WHERE filter on the RHS table. The filter null-rejects
/// the RHS, so `normalize_null_rejecting_outer_joins` in
/// `derived_tables_rewrite` can promote this to INNER JOIN.
pub fn left_join_with_rhs_filter() -> Pattern {
    let mut b = PatternBuilder::new("left_join_with_rhs_filter");
    let t1 = b.table();
    let t2 = b.table();
    b.not_eq(t1, t2);

    let c1 = b.column(t1);
    let c2 = b.column(t2);
    let jc1 = b.column(t1);
    let jc2 = b.column(t2);
    let c_filter = b.column(t2); // filter on RHS table

    b.column_type_class(jc1, TypeClass::Integer);
    b.column_type_class(jc2, TypeClass::Integer);
    b.type_compatible(jc1, jc2);

    b.from(t1);
    b.join_table(JoinOperator::LeftJoin, t2, jc1, jc2);
    b.project_column(c1, t1);
    b.project_column(c2, t2);
    b.where_param(c_filter, t2, BinaryOperator::Equal);

    b.tags(&["join", "loj_promotion"]);
    b.set_weight(3);
    b.build()
}

/// SELECT t0.c0, t1.c1 FROM t0 INNER JOIN t1 ON t0.c2 = t1.c3
///   WHERE t0.c4 = ? AND t1.c5 = ?
///
/// A straddled cross-domain join: an integer equijoin over two DISTINCT base
/// tables with a parameterized filter on a column in EACH table. The per-side
/// params make a key miss split into two cross-domain upqueries (the two-source
/// version skew that opens the stale-negative replay-frontier race), so this is
/// the shape that drives the STRADDLED_REPLAY_RESOLVE failpoint.
pub fn straddled_inner_join_with_both_filters() -> Pattern {
    let mut b = PatternBuilder::new("straddled_inner_join_with_both_filters");
    let t1 = b.table();
    let t2 = b.table();
    b.not_eq(t1, t2);

    let c1 = b.column(t1);
    let c2 = b.column(t2);
    let jc1 = b.column(t1);
    let jc2 = b.column(t2);
    let f1 = b.column(t1); // per-side filter on the left table
    let f2 = b.column(t2); // per-side filter on the right table

    b.column_type_class(jc1, TypeClass::Integer);
    b.column_type_class(jc2, TypeClass::Integer);
    b.type_compatible(jc1, jc2);

    b.from(t1);
    b.join_table(JoinOperator::InnerJoin, t2, jc1, jc2);
    b.project_column(c1, t1);
    b.project_column(c2, t2);
    b.where_param(f1, t1, BinaryOperator::Equal);
    b.where_param(f2, t2, BinaryOperator::Equal);

    b.tags(&["join", "straddled"]);
    b.build()
}

/// SELECT a0.c0, a1.c1 FROM t0 AS a0 INNER JOIN t0 AS a1 ON a0.c2 = a1.c3
/// (self-join: alias_of creates two references to the same physical table)
pub fn self_join() -> Pattern {
    let mut b = PatternBuilder::new("self_join");
    let t1 = b.table();
    let t2 = b.alias_of(t1);

    let c1 = b.column(t1);
    let c2 = b.column(t2);
    let jc1 = b.column(t1);
    let jc2 = b.column(t2);

    b.column_type_class(jc1, TypeClass::Integer);
    b.column_type_class(jc2, TypeClass::Integer);
    b.type_compatible(jc1, jc2);

    b.from(t1);
    b.join_table(JoinOperator::InnerJoin, t2, jc1, jc2);
    b.project_column(c1, t1);
    b.project_column(c2, t2);

    b.tags(&["join", "self_join"]);
    b.build()
}

/// SELECT a0.c0, a1.c1 FROM t0 AS a0 INNER JOIN t0 AS a1 ON a0.c2 = a1.c3
///   WHERE a0.c4 = ?
///
/// A self-join with a parameterized filter on the left alias. The param forces
/// the join into PARTIAL keyed materialization (an unbounded keyspace cannot be
/// fully materialized), which is the precondition for the self-join EVICTION
/// stale-negative race: only a partial key carries a per-key replay frontier, so
/// only a partial key can have a forwarded delta discarded against it. The
/// paramless `self_join` is fully materialized and so never reaches the discard
/// site. This is the shape that drives the SELF_JOIN_EVICT_STALE_DELTA failpoint.
pub fn self_join_with_filter() -> Pattern {
    let mut b = PatternBuilder::new("self_join_with_filter");
    let t1 = b.table();
    let t2 = b.alias_of(t1);

    let c1 = b.column(t1);
    let c2 = b.column(t2);
    let jc1 = b.column(t1);
    let jc2 = b.column(t2);
    let f1 = b.column(t1); // parameterized filter on the left alias

    b.column_type_class(jc1, TypeClass::Integer);
    b.column_type_class(jc2, TypeClass::Integer);
    b.type_compatible(jc1, jc2);

    b.from(t1);
    b.join_table(JoinOperator::InnerJoin, t2, jc1, jc2);
    b.project_column(c1, t1);
    b.project_column(c2, t2);
    b.where_param(f1, t1, BinaryOperator::Equal);

    b.tags(&["join", "self_join", "self_join_filter"]);
    b.build()
}

/// SELECT a0.c0, a1.c1 FROM t0 AS a0 INNER JOIN t0 AS a1 ON a0.c = a1.c
///
/// Same-column self-join: both join keys are the SAME physical column. This is
/// the REA-6138 shape that stresses the AltNeu R_old/R_new in-dispatch
/// selection, unlike the different-column `self_join` (`a0.c2 = a1.c3`).
pub fn self_join_same_column() -> Pattern {
    let mut b = PatternBuilder::new("self_join_same_column");
    let t1 = b.table();
    let t2 = b.alias_of(t1);

    let c1 = b.column(t1);
    let c2 = b.column(t2);
    let jc1 = b.column(t1);
    let jc2 = b.column(t2);

    b.column_type_class(jc1, TypeClass::Integer);
    b.column_type_class(jc2, TypeClass::Integer);
    b.type_compatible(jc1, jc2);
    // Force both join keys to the same physical column: a0.c = a1.c.
    b.same_column_eq(jc1, jc2);

    b.from(t1);
    b.join_table(JoinOperator::InnerJoin, t2, jc1, jc2);
    b.project_column(c1, t1);
    b.project_column(c2, t2);

    b.tags(&["join", "self_join", "same_column"]);
    b.build()
}

/// SELECT t0.c0, t1.c1 FROM t0 CROSS JOIN t1
pub fn cross_join() -> Pattern {
    let mut b = PatternBuilder::new("cross_join");
    let t1 = b.table();
    let t2 = b.table();
    b.not_eq(t1, t2);

    let c1 = b.column(t1);
    let c2 = b.column(t2);

    b.from(t1);
    b.join_table(JoinOperator::CrossJoin, t2, c1, c2);
    b.project_column(c1, t1);
    b.project_column(c2, t2);

    b.tags(&["join", "cross_join"]);
    b.build()
}

/// SELECT t0.c0 FROM t0 INNER JOIN t1 ON t0.c_fk = t1.c_pk
///
/// The RHS join key (`c_pk`) is UNIQUE NOT NULL, and no column from `t1` is
/// projected. This exercises the `drop_redundant_join` rewrite pass, which
/// removes the join entirely when these conditions hold.
pub fn redundant_join_on_unique_key() -> Pattern {
    let mut b = PatternBuilder::new("redundant_join_on_unique_key");
    let t0 = b.table();
    let t1 = b.table();
    b.not_eq(t0, t1);

    let c_proj = b.column(t0);
    let c_fk = b.column(t0);
    let c_pk = b.column(t1);

    b.column_type_class(c_fk, TypeClass::Integer);
    b.column_type_class(c_pk, TypeClass::Integer);
    b.type_compatible(c_fk, c_pk);
    b.column_unique_not_null(c_pk);

    b.from(t0);
    b.join_table(JoinOperator::InnerJoin, t1, c_fk, c_pk);
    // Project only from t0 -- no RHS column in the SELECT list is the key
    // precondition for the redundancy check.
    b.project_column(c_proj, t0);

    b.tags(&["join", "drop_redundant_join"]);
    b.build()
}

/// SELECT t0.c0, t1.c1 FROM t0 INNER JOIN t1 USING (c_shared)
///
/// The shared column exists with the same name in both tables. This exercises
/// the `expand_join_on_using` rewrite pass, which desugars USING to ON.
pub fn inner_join_using() -> Pattern {
    let mut b = PatternBuilder::new("inner_join_using");
    let t0 = b.table();
    let t1 = b.table();
    b.not_eq(t0, t1);

    let c_proj0 = b.column(t0);
    let c_proj1 = b.column(t1);
    // Shared column: same name must exist in both t0 and t1.
    let c_shared = b.column(t0);
    b.column_type_class(c_shared, TypeClass::Integer);

    b.from(t0);
    b.join_table_using(JoinOperator::InnerJoin, t1, c_shared);
    b.project_column(c_proj0, t0);
    b.project_column(c_proj1, t1);

    b.tags(&["join", "using"]);
    b.build()
}

/// SELECT t.c0, v.a FROM t JOIN (VALUES (1, 'x'), (2, 'y')) AS v(a, b) ON t.c0 = v.a
///
/// Base table joined to a literal VALUES relation. Exercises the
/// `MirNodeInner::Constant` dataflow node path in Readyset.
pub fn values_join() -> Pattern {
    // Row data: two columns (int join key, string payload), three rows.
    const ROWS: &[&[&str]] = &[&["1", "x"], &["2", "y"], &["3", "z"]];
    const COL_NAMES: &[&str] = &["a", "b"];

    let mut b = PatternBuilder::new("values_join");
    let t = b.table();
    let jc = b.column(t);
    b.column_type_class(jc, TypeClass::Integer);

    let c_project = b.column(t);

    b.from(t);
    b.values_join(JoinOperator::InnerJoin, jc, COL_NAMES, ROWS);

    b.project_column(c_project, t);

    b.tags(&["join", "values"]);
    b.build()
}

/// SELECT t_outer.c_outer_proj, sq.c_inner_proj
/// FROM t_outer
/// JOIN LATERAL (SELECT t_inner.c_inner_proj
///               FROM t_inner
///               WHERE t_inner.c_inner_fk = t_outer.c_outer_pk) AS sq ON TRUE
///
/// Exercises the `lateral_join` rewrite pass, which flattens LATERAL
/// subqueries into the flat join structure Readyset's MIR planner expects.
/// PostgreSQL-only: LATERAL is a PG standard construct.
pub fn lateral_correlated_subquery() -> Pattern {
    let mut b = PatternBuilder::new("lateral_correlated_subquery");
    let t_outer = b.table();
    let c_outer_proj = b.column(t_outer);
    let c_outer_pk = b.column(t_outer);
    b.column_type_class(c_outer_pk, TypeClass::Integer);

    let mut sq = b.subquery();
    let t_inner = sq.table();
    let c_inner_proj = sq.column(t_inner);
    let c_inner_fk = sq.column(t_inner);
    sq.column_type_class(c_inner_fk, TypeClass::Integer);
    sq.constraint(Constraint::TypeCompatible(c_inner_fk, c_outer_pk));
    sq.from(t_inner);
    sq.project_column(c_inner_proj, t_inner);
    // Correlated predicate: inner.c_fk = outer.c_pk references the outer scope.
    sq.constraint(Constraint::WhereColumnCompare {
        left_col: c_inner_fk,
        left_table: t_inner,
        op: BinaryOperator::Equal,
        right_col: c_outer_pk,
        right_table: t_outer,
    });
    let sq_alias = sq.commit_as_lateral(JoinOperator::InnerJoin);

    b.from(t_outer);
    b.project_column(c_outer_proj, t_outer);
    // Project the inner column through the lateral alias.
    b.project_column(c_inner_proj, sq_alias);

    b.tags(&["subquery", "lateral", "join"]);
    b.set_dialect_support(DialectSupport::PostgresOnly);
    b.build()
}

#[cfg(test)]
mod tests {
    use readyset_sql::Dialect;

    use super::*;
    use crate::constraint::Constraint;
    use crate::test_util::resolve_pattern;

    #[test]
    fn inner_join_builds() {
        let p = inner_join();
        assert_eq!(p.name, "inner_join");
        assert!(p.tags.contains(&"join"));
        assert!(p.min_depth == 0);

        // Should have NotEq between the two tables
        assert!(
            p.constraints
                .iter()
                .any(|c| matches!(c, Constraint::NotEq(_, _)))
        );
    }

    #[test]
    fn left_join_builds() {
        let p = left_join();
        assert_eq!(p.name, "left_join");
        assert!(p.tags.contains(&"join"));
    }

    #[test]
    fn self_join_builds() {
        let p = self_join();
        assert_eq!(p.name, "self_join");
        assert!(p.tags.contains(&"self_join"));

        // Should have AliasOf between the two relations
        assert!(
            p.constraints
                .iter()
                .any(|c| matches!(c, Constraint::AliasOf { .. }))
        );
    }

    #[test]
    fn cross_join_builds() {
        let p = cross_join();
        assert_eq!(p.name, "cross_join");
        assert!(p.tags.contains(&"cross_join"));
    }

    #[test]
    fn inner_join_resolves() {
        let p = inner_join();
        let sql = resolve_pattern(&p, Dialect::MySQL);
        assert!(sql.contains("JOIN"), "sql: {sql}");
        assert!(sql.contains("ON"), "sql: {sql}");
    }

    #[test]
    fn left_join_resolves() {
        let p = left_join();
        let sql = resolve_pattern(&p, Dialect::MySQL);
        assert!(sql.contains("LEFT JOIN"), "sql: {sql}");
        assert!(sql.contains("ON"), "sql: {sql}");
    }

    #[test]
    fn self_join_resolves_with_aliases() {
        let p = self_join();
        let sql = resolve_pattern(&p, Dialect::MySQL);
        // Self-join MUST use aliases since same table appears twice
        assert!(sql.contains(" AS "), "self-join should use aliases: {sql}");
        assert!(sql.contains("JOIN"), "sql: {sql}");
        assert!(sql.contains("ON"), "sql: {sql}");
    }

    #[test]
    fn self_join_with_filter_builds() {
        let p = self_join_with_filter();
        assert_eq!(p.name, "self_join_with_filter");
        assert!(p.tags.contains(&"self_join"));
        assert!(p.tags.contains(&"self_join_filter"));
        // AliasOf (it is a self-join) and exactly one parameterized filter (so the
        // join is partial-keyed, the eviction-variant precondition).
        assert!(
            p.constraints
                .iter()
                .any(|c| matches!(c, Constraint::AliasOf { .. }))
        );
        let where_params = p
            .constraints
            .iter()
            .filter(|c| matches!(c, Constraint::WhereParam { .. }))
            .count();
        assert_eq!(
            where_params, 1,
            "filtered self-join needs exactly one param"
        );
    }

    #[test]
    fn self_join_with_filter_resolves() {
        let p = self_join_with_filter();
        let sql = resolve_pattern(&p, Dialect::MySQL);
        assert!(sql.contains(" AS "), "self-join should use aliases: {sql}");
        assert!(sql.contains("JOIN"), "sql: {sql}");
        assert!(sql.contains("ON"), "sql: {sql}");
        assert!(sql.contains("WHERE"), "sql: {sql}");
        assert_eq!(
            sql.matches("= ?").count(),
            1,
            "expected one parameterized filter: {sql}"
        );
    }

    #[test]
    fn cross_join_resolves() {
        let p = cross_join();
        let sql = resolve_pattern(&p, Dialect::MySQL);
        assert!(sql.contains("CROSS JOIN"), "sql: {sql}");
    }

    /// CROSS JOIN must not carry an ON clause: PostgreSQL rejects
    /// `CROSS JOIN ... ON` with a syntax error (SQLSTATE 42601), and it is
    /// nonstandard everywhere. (qp:wwtzoakmfzdg)
    #[test]
    fn cross_join_resolves_without_on_clause() {
        let p = cross_join();
        for dialect in [Dialect::MySQL, Dialect::PostgreSQL] {
            let sql = resolve_pattern(&p, dialect);
            assert!(sql.contains("CROSS JOIN"), "sql: {sql}");
            assert!(!sql.contains(" ON "), "CROSS JOIN must have no ON: {sql}");
        }
    }

    #[test]
    fn self_join_same_column_builds() {
        let p = self_join_same_column();
        assert_eq!(p.name, "self_join_same_column");
        assert!(p.tags.contains(&"self_join"));
        assert!(p.tags.contains(&"same_column"));
        assert!(
            p.constraints
                .iter()
                .any(|c| matches!(c, Constraint::SameColumnEq(_, _))),
            "same-column self-join must carry a SameColumnEq constraint"
        );
    }

    #[test]
    fn self_join_same_column_resolves() {
        let p = self_join_same_column();
        let sql = resolve_pattern(&p, Dialect::MySQL);
        assert!(sql.contains(" AS "), "self-join uses aliases: {sql}");
        let after_on = sql
            .split(" ON ")
            .nth(1)
            .unwrap_or_else(|| panic!("self-join has an ON clause: {sql}"));
        let sides: Vec<&str> = after_on.split(" = ").collect();
        assert!(sides.len() >= 2, "ON clause has an equality: {sql}");
        // Strip the alias qualifier and any quoting, leaving the bare column.
        let col = |s: &str| -> String {
            s.split_whitespace()
                .next()
                .unwrap()
                .rsplit('.')
                .next()
                .unwrap()
                .replace(['`', '"', '(', ')'], "")
        };
        assert_eq!(
            col(sides[0]),
            col(sides[1]),
            "same-column self-join must reference the same column on both aliases: {sql}"
        );
    }

    #[test]
    fn left_join_with_rhs_filter_builds() {
        let p = left_join_with_rhs_filter();
        assert_eq!(p.name, "left_join_with_rhs_filter");
        assert!(p.tags.contains(&"loj_promotion"));
        assert!(p.tags.contains(&"join"));
    }

    #[test]
    fn left_join_with_rhs_filter_resolves() {
        let p = left_join_with_rhs_filter();
        let sql = resolve_pattern(&p, Dialect::MySQL);
        assert!(
            sql.contains("LEFT JOIN"),
            "expected LEFT JOIN in sql: {sql}"
        );
        assert!(sql.contains("WHERE"), "expected WHERE in sql: {sql}");
        assert!(sql.contains("= ?"), "expected = ? in sql: {sql}");
    }

    #[test]
    fn straddled_inner_join_with_both_filters_builds() {
        let p = straddled_inner_join_with_both_filters();
        assert_eq!(p.name, "straddled_inner_join_with_both_filters");
        assert!(p.tags.contains(&"join"));
        assert!(p.tags.contains(&"straddled"));
        // One parameterized filter per side so a key miss splits into two
        // cross-domain upqueries: exactly two WhereParam constraints.
        let where_params = p
            .constraints
            .iter()
            .filter(|c| matches!(c, Constraint::WhereParam { .. }))
            .count();
        assert_eq!(where_params, 2, "straddled join needs one param per side");
    }

    #[test]
    fn straddled_inner_join_with_both_filters_resolves() {
        let p = straddled_inner_join_with_both_filters();
        let sql = resolve_pattern(&p, Dialect::MySQL);
        assert!(sql.contains("JOIN"), "expected JOIN in sql: {sql}");
        assert!(sql.contains("ON"), "expected ON in sql: {sql}");
        assert!(sql.contains("WHERE"), "expected WHERE in sql: {sql}");
        // Distinct tables (no self-join alias) and a param on each side.
        assert!(
            !sql.contains(" AS "),
            "straddled join is over distinct tables: {sql}"
        );
        assert_eq!(
            sql.matches("= ?").count(),
            2,
            "expected one parameterized filter per side in sql: {sql}"
        );
    }

    #[test]
    fn values_join_builds() {
        let p = values_join();
        assert_eq!(p.name, "values_join");
        assert!(p.tags.contains(&"join"), "should carry join tag");
        assert!(p.tags.contains(&"values"), "should carry values tag");
    }

    #[test]
    fn values_join_resolves_pg() {
        let p = values_join();
        let sql = resolve_pattern(&p, Dialect::PostgreSQL);
        assert!(sql.contains("JOIN"), "expected JOIN in sql: {sql}");
        assert!(
            sql.contains("VALUES"),
            "expected VALUES keyword in sql: {sql}"
        );
        assert!(sql.contains("AS"), "expected AS alias in sql: {sql}");
    }

    #[test]
    fn values_join_resolves_mysql() {
        let p = values_join();
        let sql = resolve_pattern(&p, Dialect::MySQL);
        assert!(sql.contains("JOIN"), "expected JOIN in sql: {sql}");
        assert!(
            sql.contains("VALUES"),
            "expected VALUES keyword in sql: {sql}"
        );
        assert!(
            sql.contains("ROW"),
            "expected ROW keyword in MySQL sql: {sql}"
        );
    }

    #[test]
    fn inner_join_using_builds() {
        let p = inner_join_using();
        assert_eq!(p.name, "inner_join_using");
        assert!(p.tags.contains(&"join"), "should carry join tag");
        assert!(p.tags.contains(&"using"), "should carry using tag");
        assert!(
            p.constraints
                .iter()
                .any(|c| matches!(c, Constraint::NotEq(_, _))),
            "should have NotEq between the two tables"
        );
        assert!(
            p.constraints
                .iter()
                .any(|c| matches!(c, Constraint::JoinUsing { .. })),
            "should have JoinUsing constraint"
        );
    }

    #[test]
    fn inner_join_using_resolves() {
        let p = inner_join_using();
        let sql = resolve_pattern(&p, Dialect::MySQL);
        assert!(sql.contains("USING ("), "expected USING ( in sql: {sql}");
        assert!(sql.contains("JOIN"), "expected JOIN keyword in sql: {sql}");
    }

    #[test]
    fn inner_join_using_resolves_pg() {
        let p = inner_join_using();
        let sql = resolve_pattern(&p, Dialect::PostgreSQL);
        assert!(sql.contains("USING ("), "expected USING ( in sql: {sql}");
        assert!(sql.contains("JOIN"), "expected JOIN keyword in sql: {sql}");
    }

    #[test]
    fn redundant_join_on_unique_key_builds() {
        let p = redundant_join_on_unique_key();
        assert_eq!(p.name, "redundant_join_on_unique_key");
        assert!(p.tags.contains(&"join"), "should carry join tag");
        assert!(
            p.tags.contains(&"drop_redundant_join"),
            "should carry drop_redundant_join tag"
        );
        assert!(
            p.constraints
                .iter()
                .any(|c| matches!(c, Constraint::NotEq(_, _))),
            "should have NotEq between the two tables"
        );
        assert!(
            p.constraints
                .iter()
                .any(|c| matches!(c, Constraint::ColumnUniqueNotNull { .. })),
            "should have ColumnUniqueNotNull on the RHS join key"
        );
    }

    #[test]
    fn redundant_join_on_unique_key_resolves() {
        let p = redundant_join_on_unique_key();
        let sql = resolve_pattern(&p, Dialect::MySQL);
        assert!(
            sql.contains("INNER JOIN"),
            "expected INNER JOIN in sql: {sql}"
        );
        assert!(sql.contains("ON"), "expected ON in sql: {sql}");
        // Only LHS columns projected: no column from the RHS table in SELECT.
        // The SELECT list should not contain the RHS table alias after the
        // resolver assigns concrete names.
        assert!(sql.contains("SELECT"), "expected SELECT in sql: {sql}");
    }

    #[test]
    fn redundant_join_on_unique_key_resolves_pg() {
        let p = redundant_join_on_unique_key();
        let sql = resolve_pattern(&p, Dialect::PostgreSQL);
        assert!(
            sql.contains("INNER JOIN"),
            "expected INNER JOIN in sql: {sql}"
        );
        assert!(sql.contains("ON"), "expected ON in sql: {sql}");
    }

    #[test]
    fn lateral_correlated_subquery_builds() {
        let p = lateral_correlated_subquery();
        assert_eq!(p.name, "lateral_correlated_subquery");
        assert!(p.tags.contains(&"lateral"), "should carry lateral tag");
        assert!(p.tags.contains(&"subquery"), "should carry subquery tag");
        assert!(p.tags.contains(&"join"), "should carry join tag");
        assert!(
            p.min_depth >= 1,
            "lateral subquery pattern must have min_depth >= 1"
        );
        assert!(
            p.constraints
                .iter()
                .any(|c| matches!(c, Constraint::SubqueryRelation { .. })),
            "should have a SubqueryRelation constraint"
        );
    }

    #[test]
    fn lateral_correlated_subquery_resolves() {
        let p = lateral_correlated_subquery();
        let sql = resolve_pattern(&p, Dialect::PostgreSQL);
        assert!(
            sql.contains("LATERAL"),
            "expected LATERAL keyword in sql: {sql}"
        );
        assert!(sql.contains("JOIN"), "expected JOIN in sql: {sql}");
        assert!(
            sql.contains("ON TRUE"),
            "expected ON TRUE in lateral join sql: {sql}"
        );
    }

    #[test]
    fn values_alias_does_not_compose_with_base_table() {
        // The VALUES alias is DerivedRelation; compose must not unify it
        // with a partner pattern's base-table primary.
        use crate::var::VarKind;
        let p = values_join();
        let alias_var = p
            .constraints
            .iter()
            .find_map(|c| match c {
                crate::constraint::Constraint::ValuesRelation { alias, .. } => Some(*alias),
                _ => None,
            })
            .expect("values_join must carry a ValuesRelation constraint");
        assert!(
            matches!(p.vars[alias_var.0], VarKind::DerivedRelation),
            "VALUES alias must be DerivedRelation so compose refuses to unify it"
        );
    }
}
