//! Join patterns: inner, left, self-join, cross join.

use readyset_sql::ast::{BinaryOperator, JoinOperator};

use crate::constraint::TypeClass;
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

    b.tags(&["join", "two_table"]);
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

    b.tags(&["join", "two_table"]);
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

    b.tags(&["join", "two_table", "loj_promotion"]);
    b.set_weight(3);
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
        assert!(p.tags.contains(&"two_table"));
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
    fn cross_join_resolves() {
        let p = cross_join();
        let sql = resolve_pattern(&p, Dialect::MySQL);
        assert!(sql.contains("CROSS JOIN"), "sql: {sql}");
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
}
