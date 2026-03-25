//! Hoisting patterns: query shapes that exercise the HoistParametrizableFilters
//! pass. These produce queries where parametrizable filters are buried inside
//! non-inlinable subqueries (aggregated derived tables) or in HAVING on GROUP
//! BY keys. The hoisting pass lifts these to the outer WHERE, enabling
//! autoparameterization.

use readyset_sql::ast::{BinaryOperator, JoinOperator};

use crate::constraint::{AggregateFn, Constraint, TypeClass};
use crate::pattern::{Pattern, PatternBuilder};

/// SELECT t0.c0, sq.total
/// FROM t0
/// INNER JOIN (
///   SELECT t1.c_group, SUM(t1.c_agg) AS total
///   FROM t1
///   WHERE t1.c_filter = ?
///   GROUP BY t1.c_group
/// ) AS sq ON t0.c_join = sq.c_group
///
/// The inner subquery has GROUP BY + aggregate → non-inlinable. The equality
/// filter in inner WHERE is hoisted by HoistParametrizableFilters.
pub fn aggregated_join_subquery_eq_filter() -> Pattern {
    let mut b = PatternBuilder::new("aggregated_join_subquery_eq_filter");
    let t_outer = b.table();
    let c_outer_proj = b.column(t_outer);
    let c_outer_join = b.column(t_outer);
    b.column_type_class(c_outer_join, TypeClass::Integer);

    let mut sq = b.subquery();
    let t_inner = sq.table();
    let c_group = sq.column(t_inner);
    let c_agg = sq.column(t_inner);
    let c_filter = sq.column(t_inner);
    sq.column_type_class(c_group, TypeClass::Integer);
    sq.column_type_class(c_agg, TypeClass::Numeric);
    sq.from(t_inner);
    sq.project_column(c_group, t_inner);
    sq.project_aggregate(AggregateFn::Sum { distinct: false }, c_agg, t_inner);
    sq.group_by(c_group, t_inner);
    sq.constraint(Constraint::WhereParam {
        col: c_filter,
        table: t_inner,
        op: BinaryOperator::Equal,
    });
    sq.commit_as_join(JoinOperator::InnerJoin, c_outer_join, c_group);

    b.from(t_outer);
    b.project_column(c_outer_proj, t_outer);

    b.tags(&["hoisting", "subquery", "aggregate", "group_by"]);
    b.build()
}

/// SELECT t0.c0, sq.total
/// FROM t0
/// INNER JOIN (
///   SELECT t1.c_group, SUM(t1.c_agg) AS total
///   FROM t1
///   GROUP BY t1.c_group
///   HAVING t1.c_group = ?
/// ) AS sq ON t0.c_join = sq.c_group
///
/// Same structure but filter is in HAVING via HavingKeyFilter. Tests the
/// HAVING extraction path of the hoisting pass.
pub fn aggregated_join_subquery_having_filter() -> Pattern {
    let mut b = PatternBuilder::new("aggregated_join_subquery_having_filter");
    let t_outer = b.table();
    let c_outer_proj = b.column(t_outer);
    let c_outer_join = b.column(t_outer);
    b.column_type_class(c_outer_join, TypeClass::Integer);

    let mut sq = b.subquery();
    let t_inner = sq.table();
    let c_group = sq.column(t_inner);
    let c_agg = sq.column(t_inner);
    sq.column_type_class(c_group, TypeClass::Integer);
    sq.column_type_class(c_agg, TypeClass::Numeric);
    sq.from(t_inner);
    sq.project_column(c_group, t_inner);
    sq.project_aggregate(AggregateFn::Sum { distinct: false }, c_agg, t_inner);
    sq.group_by(c_group, t_inner);
    sq.having_key_filter(c_group, t_inner, BinaryOperator::Equal);
    sq.commit_as_join(JoinOperator::InnerJoin, c_outer_join, c_group);

    b.from(t_outer);
    b.project_column(c_outer_proj, t_outer);

    b.tags(&["hoisting", "subquery", "aggregate", "group_by", "having"]);
    b.build()
}

/// SELECT t.c_group, COUNT(t.c_agg)
/// FROM t
/// GROUP BY t.c_group
/// HAVING t.c_group = ?
///
/// Flat query (no subquery) with GROUP BY + HAVING group-key filter.
/// The hoisting pass moves the group-key filter from HAVING to WHERE.
pub fn having_to_where_promotion() -> Pattern {
    let mut b = PatternBuilder::new("having_to_where_promotion");
    let t = b.table();
    let c_group = b.column(t);
    let c_agg = b.column(t);
    b.from(t);
    b.project_column(c_group, t);
    b.project_aggregate(AggregateFn::Count { distinct: false }, c_agg, t);
    b.group_by(c_group, t);
    b.having_key_filter(c_group, t, BinaryOperator::Equal);
    b.tags(&["hoisting", "aggregate", "group_by", "having"]);
    b.build()
}

/// SELECT sq.c0, sq.c1
/// FROM (SELECT t.c0, t.c1 FROM t WHERE t.c2 = ?) AS sq
///
/// Non-aggregated derived table as the leftmost FROM item with a
/// parametrizable filter. The `inline_leading_derived_table` pass
/// will flatten this into `SELECT t.c0, t.c1 FROM t WHERE t.c2 = ?`.
pub fn from_subquery_filter() -> Pattern {
    let mut b = PatternBuilder::new("from_subquery_filter");

    let mut sq = b.subquery();
    let t_inner = sq.table();
    let c_proj = sq.column(t_inner);
    let c_filter = sq.column(t_inner);
    // Ensure projected column and filter column are distinct so the
    // derived table never has duplicate column names.
    sq.constraint(Constraint::NotEq(c_proj, c_filter));
    sq.from(t_inner);
    sq.project_column(c_proj, t_inner);
    sq.project_column(c_filter, t_inner);
    sq.constraint(Constraint::WhereParam {
        col: c_filter,
        table: t_inner,
        op: BinaryOperator::Equal,
    });
    let _derived_alias = sq.commit_as_from();

    b.tags(&["subquery", "inline_leading"]);
    b.set_weight(3);
    b.build()
}

#[cfg(test)]
mod tests {
    use readyset_sql::Dialect;

    use super::*;
    use crate::test_util::resolve_pattern;

    #[test]
    fn aggregated_join_subquery_eq_filter_builds() {
        let p = aggregated_join_subquery_eq_filter();
        assert_eq!(p.name, "aggregated_join_subquery_eq_filter");
        assert!(p.tags.contains(&"hoisting"));
        assert!(p.tags.contains(&"subquery"));
        assert!(p.tags.contains(&"aggregate"));
        assert!(p.min_depth >= 1);

        // Should have a SubqueryRelation { kind: JoinTarget } and a Join
        // referencing its alias.
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
    fn aggregated_join_subquery_eq_filter_resolves() {
        let p = aggregated_join_subquery_eq_filter();
        let sql = resolve_pattern(&p, Dialect::MySQL);
        assert!(sql.contains("GROUP BY"), "expected GROUP BY in sql: {sql}");
        assert!(
            sql.contains("SUM(") || sql.contains("sum("),
            "expected SUM in sql: {sql}"
        );
        assert!(sql.contains("JOIN"), "expected JOIN in sql: {sql}");
        assert!(sql.contains("= ?"), "expected = ? in sql: {sql}");
    }

    #[test]
    fn aggregated_join_subquery_having_filter_builds() {
        let p = aggregated_join_subquery_having_filter();
        assert_eq!(p.name, "aggregated_join_subquery_having_filter");
        assert!(p.tags.contains(&"hoisting"));
        assert!(p.tags.contains(&"having"));
        assert!(p.min_depth >= 1);
    }

    #[test]
    fn aggregated_join_subquery_having_filter_resolves() {
        let p = aggregated_join_subquery_having_filter();
        let sql = resolve_pattern(&p, Dialect::MySQL);
        assert!(sql.contains("GROUP BY"), "expected GROUP BY in sql: {sql}");
        assert!(
            sql.contains("SUM(") || sql.contains("sum("),
            "expected SUM in sql: {sql}"
        );
        assert!(sql.contains("HAVING"), "expected HAVING in sql: {sql}");
        assert!(sql.contains("JOIN"), "expected JOIN in sql: {sql}");
    }

    #[test]
    fn having_to_where_promotion_builds() {
        let p = having_to_where_promotion();
        assert_eq!(p.name, "having_to_where_promotion");
        assert!(p.tags.contains(&"hoisting"));
        assert!(p.tags.contains(&"having"));

        // Should have GroupBy and HavingKeyFilter constraints
        assert!(
            p.constraints
                .iter()
                .any(|c| matches!(c, Constraint::GroupBy { .. }))
        );
        assert!(
            p.constraints
                .iter()
                .any(|c| matches!(c, Constraint::HavingKeyFilter { .. }))
        );
    }

    #[test]
    fn having_to_where_promotion_resolves() {
        let p = having_to_where_promotion();
        let sql = resolve_pattern(&p, Dialect::MySQL);
        assert!(sql.contains("GROUP BY"), "expected GROUP BY in sql: {sql}");
        assert!(
            sql.contains("COUNT(") || sql.contains("count("),
            "expected COUNT in sql: {sql}"
        );
        assert!(sql.contains("HAVING"), "expected HAVING in sql: {sql}");
        assert!(sql.contains("= ?"), "expected = ? in sql: {sql}");
    }

    #[test]
    fn from_subquery_filter_builds() {
        let p = from_subquery_filter();
        assert_eq!(p.name, "from_subquery_filter");
        assert!(p.tags.contains(&"subquery"));
        assert!(p.tags.contains(&"inline_leading"));
        assert!(p.min_depth >= 1);
        // Primary is the FROM-subquery alias and must be DerivedRelation
        // so `compose` refuses to unify it with a partner pattern's
        // base-table primary (same invariant as CTE aliases).
        assert_eq!(
            p.vars[p.primary_table.0],
            crate::var::VarKind::DerivedRelation,
            "FROM-subquery alias must be DerivedRelation, got: {:?}",
            p.vars[p.primary_table.0]
        );
    }

    #[test]
    fn from_subquery_filter_resolves() {
        let p = from_subquery_filter();
        let sql = resolve_pattern(&p, Dialect::MySQL);
        // Should be a derived table in FROM, not a JOIN
        assert!(!sql.contains("JOIN"), "should not have JOIN in sql: {sql}");
        assert!(sql.contains("= ?"), "expected = ? in sql: {sql}");
        // The outer query should reference the subquery alias columns
        assert!(
            sql.contains("`sq"),
            "expected sq alias reference in sql: {sql}"
        );
    }
}
