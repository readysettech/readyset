//! Aggregate patterns: count, sum, avg, min/max, count_distinct, group_by,
//! and dialect-specific aggregates (group_concat, array_agg).

use readyset_sql::ast::{BinaryOperator, OrderType};

use crate::constraint::{AggregateFn, DialectSupport, TypeClass};
use crate::pattern::{Pattern, PatternBuilder};

/// SELECT COUNT(t.c) FROM t
pub fn count() -> Pattern {
    let mut b = PatternBuilder::new("count");
    let t = b.table();
    let c = b.column(t);
    b.from(t);
    b.project_aggregate(AggregateFn::Count { distinct: false }, c, t);
    b.tags(&["aggregate"]);
    b.build()
}

/// SELECT SUM(t.c) FROM t (c must be numeric)
pub fn sum() -> Pattern {
    let mut b = PatternBuilder::new("sum");
    let t = b.table();
    let c = b.column(t);
    b.column_type_class(c, TypeClass::Numeric);
    b.from(t);
    b.project_aggregate(AggregateFn::Sum { distinct: false }, c, t);
    b.tags(&["aggregate", "numeric"]);
    b.build()
}

/// SELECT AVG(t.c) FROM t (c must be numeric)
pub fn avg() -> Pattern {
    let mut b = PatternBuilder::new("avg");
    let t = b.table();
    let c = b.column(t);
    b.column_type_class(c, TypeClass::Numeric);
    b.from(t);
    b.project_aggregate(AggregateFn::Avg { distinct: false }, c, t);
    b.tags(&["aggregate", "numeric"]);
    b.build()
}

/// SELECT MIN(t.c) FROM t
pub fn min_max() -> Pattern {
    let mut b = PatternBuilder::new("min_max");
    let t = b.table();
    let c = b.column(t);
    b.from(t);
    b.project_aggregate(AggregateFn::Min, c, t);
    b.tags(&["aggregate"]);
    b.build()
}

/// SELECT COUNT(DISTINCT t.c) FROM t
pub fn count_distinct() -> Pattern {
    let mut b = PatternBuilder::new("count_distinct");
    let t = b.table();
    let c = b.column(t);
    b.from(t);
    b.project_aggregate(AggregateFn::Count { distinct: true }, c, t);
    b.tags(&["aggregate"]);
    b.build()
}

/// SELECT t.c1, COUNT(t.c2) FROM t GROUP BY t.c1
pub fn aggregate_with_group_by() -> Pattern {
    let mut b = PatternBuilder::new("aggregate_with_group_by");
    let t = b.table();
    let c_group = b.column(t);
    let c_agg = b.column(t);
    b.from(t);
    b.project_column(c_group, t);
    b.project_aggregate(AggregateFn::Count { distinct: false }, c_agg, t);
    b.group_by(c_group, t);
    b.tags(&["aggregate", "group_by"]);
    b.build()
}

/// SELECT t.c1, COUNT(t.c2) FROM t GROUP BY t.c1 HAVING COUNT(t.c2) > ?
pub fn having_clause() -> Pattern {
    let mut b = PatternBuilder::new("having_clause");
    let t = b.table();
    let c_group = b.column(t);
    let c_agg = b.column(t);
    b.from(t);
    b.project_column(c_group, t);
    b.project_aggregate(AggregateFn::Count { distinct: false }, c_agg, t);
    b.group_by(c_group, t);
    b.having(
        AggregateFn::Count { distinct: false },
        c_agg,
        t,
        BinaryOperator::Greater,
    );
    b.tags(&["aggregate", "group_by", "having"]);
    b.build()
}

/// SELECT GROUP_CONCAT(t.c) FROM t (MySQL only)
pub fn group_concat() -> Pattern {
    let mut b = PatternBuilder::new("group_concat");
    let t = b.table();
    let c = b.column(t);
    b.from(t);
    b.project_aggregate(AggregateFn::GroupConcat, c, t);
    b.set_dialect_support(DialectSupport::MySqlOnly);
    b.tags(&["aggregate", "mysql_only", "group_concat"]);
    b.build()
}

/// SELECT t.c0, GROUP_CONCAT(t.c1) FROM t GROUP BY t.c0 ORDER BY t.c0 ASC
/// (MySQL only)
///
/// Deterministic-output variant of [`group_concat`]: GROUP BY on the
/// auto-allocated PK (`c0` is `Unique`) means each group has exactly one
/// source row, so the order of values within each `GROUP_CONCAT` string is
/// trivially fixed. ORDER BY on the same PK then fixes the outer row
/// order. The underlying MIR planner still emits `MirNodeInner::Accumulator`
/// for the `GROUP_CONCAT`, so the `Create dataflow node::Accumulator`
/// Antithesis assertion fires on MySQL just like `array_agg` does on
/// PostgreSQL.
pub fn group_concat_grouped() -> Pattern {
    let mut b = PatternBuilder::new("group_concat_grouped");
    let t = b.table();
    let c_group = b.column(t);
    let c_agg = b.column(t);
    b.from(t);
    b.project_column(c_group, t);
    b.project_aggregate(AggregateFn::GroupConcat, c_agg, t);
    b.group_by(c_group, t);
    b.order_by(c_group, t, OrderType::OrderAscending, None);
    b.set_dialect_support(DialectSupport::MySqlOnly);
    b.tags(&["aggregate", "mysql_only", "group_concat", "accumulator"]);
    b.build()
}

/// SELECT t.c3, COUNT(t.c1), SUM(t.c2) FROM t GROUP BY t.c3
///
/// Multiple aggregates in one query trigger JoinAggregates dataflow nodes.
pub fn multi_aggregate() -> Pattern {
    let mut b = PatternBuilder::new("multi_aggregate");
    let t = b.table();
    let c_count = b.column(t);
    let c_sum = b.column(t);
    let c_group = b.column(t);
    b.column_type_class(c_sum, TypeClass::Numeric);
    b.from(t);
    b.project_column(c_group, t);
    b.project_aggregate(AggregateFn::Count { distinct: false }, c_count, t);
    b.project_aggregate(AggregateFn::Sum { distinct: false }, c_sum, t);
    b.group_by(c_group, t);
    b.tags(&["aggregate", "group_by", "multi_aggregate"]);
    b.build()
}

/// SELECT SUM(t.c1) FROM t WHERE t.c2 IN (?, ?, ?)
///
/// Combining an aggregate with WHERE IN triggers post-lookup aggregation
/// in Readyset, where partial results from multiple point lookups are
/// re-aggregated after the cache read.
pub fn in_list_aggregate() -> Pattern {
    let mut b = PatternBuilder::new("in_list_aggregate");
    let t = b.table();
    let c_agg = b.column(t);
    let c_filter = b.column(t);
    b.column_type_class(c_agg, TypeClass::Numeric);
    b.from(t);
    b.project_aggregate(AggregateFn::Sum { distinct: false }, c_agg, t);
    b.where_in_param(c_filter, t, 3);
    b.tags(&["aggregate", "filter", "post_lookup"]);
    b.build()
}

/// SELECT ARRAY_AGG(t.c) FROM t (PostgreSQL only)
pub fn array_agg() -> Pattern {
    let mut b = PatternBuilder::new("array_agg");
    let t = b.table();
    let c = b.column(t);
    b.from(t);
    b.project_aggregate(AggregateFn::ArrayAgg, c, t);
    b.set_dialect_support(DialectSupport::PostgresOnly);
    b.tags(&["aggregate", "postgres_only"]);
    b.build()
}

/// SELECT MAX(t.c) FROM t
///
/// `min_max` currently emits only `Min`; without a sibling for `Max` the
/// `Aggregation::Max` dataflow-node assertion in Readyset never fires, and
/// no path exists for `Post-lookup aggregate::Max` either. Pair this with
/// `max_in_list` to cover both the aggregation and post-lookup paths.
pub fn max() -> Pattern {
    let mut b = PatternBuilder::new("max");
    let t = b.table();
    let c = b.column(t);
    b.from(t);
    b.project_aggregate(AggregateFn::Max, c, t);
    b.tags(&["aggregate"]);
    b.build()
}

/// SELECT MAX(t.c1) FROM t WHERE t.c2 IN (?, ?, ?)
///
/// Combines `Max` with `WHERE IN` so the planner emits a post-lookup
/// `Max` aggregator after the multi-key cache read.
pub fn max_in_list() -> Pattern {
    let mut b = PatternBuilder::new("max_in_list");
    let t = b.table();
    let c_agg = b.column(t);
    let c_filter = b.column(t);
    b.from(t);
    b.project_aggregate(AggregateFn::Max, c_agg, t);
    b.where_in_param(c_filter, t, 3);
    b.tags(&["aggregate", "filter", "post_lookup"]);
    b.build()
}

/// SELECT MIN(t.c1) FROM t WHERE t.c2 IN (?, ?, ?)
///
/// Sibling of [`max_in_list`] for the `Min` post-lookup path. `min_max`
/// already drives the bare-aggregation `Min` node; this variant
/// specifically targets the post-lookup re-aggregation step.
pub fn min_in_list() -> Pattern {
    let mut b = PatternBuilder::new("min_in_list");
    let t = b.table();
    let c_agg = b.column(t);
    let c_filter = b.column(t);
    b.from(t);
    b.project_aggregate(AggregateFn::Min, c_agg, t);
    b.where_in_param(c_filter, t, 3);
    b.tags(&["aggregate", "filter", "post_lookup"]);
    b.build()
}

/// SELECT AVG(t.c1) FROM t WHERE t.c2 IN (?, ?, ?)
///
/// Post-lookup average. `c1` is numeric to match `AVG`'s domain; `c2` is
/// the IN-list filter.
pub fn avg_in_list() -> Pattern {
    let mut b = PatternBuilder::new("avg_in_list");
    let t = b.table();
    let c_agg = b.column(t);
    let c_filter = b.column(t);
    b.column_type_class(c_agg, TypeClass::Numeric);
    b.from(t);
    b.project_aggregate(AggregateFn::Avg { distinct: false }, c_agg, t);
    b.where_in_param(c_filter, t, 3);
    b.tags(&["aggregate", "filter", "post_lookup", "numeric"]);
    b.build()
}

/// SELECT COUNT(t.c1) FROM t WHERE t.c2 IN (?, ?, ?)
///
/// Mirror of [`avg_in_list`] for `Count`. We already drive
/// `Post-lookup aggregate::Count` indirectly through the `Sum` path of
/// `in_list_aggregate`, but a dedicated COUNT-only variant keeps the
/// path covered if other patterns shift.
pub fn count_in_list() -> Pattern {
    let mut b = PatternBuilder::new("count_in_list");
    let t = b.table();
    let c_agg = b.column(t);
    let c_filter = b.column(t);
    b.from(t);
    b.project_aggregate(AggregateFn::Count { distinct: false }, c_agg, t);
    b.where_in_param(c_filter, t, 3);
    b.tags(&["aggregate", "filter", "post_lookup"]);
    b.build()
}

#[cfg(test)]
mod tests {
    use readyset_sql::Dialect;

    use super::*;
    use crate::constraint::{Constraint, DialectSupport};
    use crate::test_util::resolve_pattern;

    #[test]
    fn count_builds() {
        let p = count();
        assert_eq!(p.name, "count");
        assert!(p.tags.contains(&"aggregate"));
        assert_eq!(p.num_vars(), 2); // table + column
    }

    #[test]
    fn sum_builds() {
        let p = sum();
        assert_eq!(p.name, "sum");
        assert!(p.tags.contains(&"numeric"));

        // Should have a ColumnTypeClass(Numeric) constraint
        assert!(p.constraints.iter().any(|c| matches!(
            c,
            Constraint::ColumnTypeClass {
                type_class: TypeClass::Numeric,
                ..
            }
        )));
    }

    #[test]
    fn avg_builds() {
        let p = avg();
        assert_eq!(p.name, "avg");
        assert!(p.tags.contains(&"numeric"));
    }

    #[test]
    fn min_max_builds() {
        let p = min_max();
        assert_eq!(p.name, "min_max");
        assert!(p.tags.contains(&"aggregate"));
    }

    #[test]
    fn count_distinct_builds() {
        let p = count_distinct();
        assert_eq!(p.name, "count_distinct");
    }

    #[test]
    fn aggregate_with_group_by_builds() {
        let p = aggregate_with_group_by();
        assert_eq!(p.name, "aggregate_with_group_by");
        assert!(p.tags.contains(&"group_by"));

        // Should have a GroupBy constraint
        assert!(
            p.constraints
                .iter()
                .any(|c| matches!(c, Constraint::GroupBy { .. }))
        );
    }

    #[test]
    fn having_clause_builds() {
        let p = having_clause();
        assert_eq!(p.name, "having_clause");
        assert!(p.tags.contains(&"having"));
    }

    #[test]
    fn count_resolves() {
        let p = count();
        let sql = resolve_pattern(&p, Dialect::MySQL);
        assert!(
            sql.contains("COUNT(") || sql.contains("count("),
            "sql: {sql}"
        );
    }

    #[test]
    fn sum_resolves() {
        let p = sum();
        let sql = resolve_pattern(&p, Dialect::MySQL);
        assert!(sql.contains("SUM(") || sql.contains("sum("), "sql: {sql}");
    }

    #[test]
    fn aggregate_with_group_by_resolves() {
        let p = aggregate_with_group_by();
        let sql = resolve_pattern(&p, Dialect::MySQL);
        assert!(sql.contains("GROUP BY"), "sql: {sql}");
        assert!(
            sql.contains("COUNT(") || sql.contains("count("),
            "sql: {sql}"
        );
    }

    #[test]
    fn group_concat_builds() {
        let p = group_concat();
        assert_eq!(p.name, "group_concat");
        assert!(p.tags.contains(&"mysql_only"));
        assert_eq!(p.dialect_support, DialectSupport::MySqlOnly);
    }

    #[test]
    fn group_concat_resolves() {
        let p = group_concat();
        let sql = resolve_pattern(&p, Dialect::MySQL);
        assert!(
            sql.contains("GROUP_CONCAT("),
            "expected GROUP_CONCAT in sql: {sql}"
        );
    }

    #[test]
    fn array_agg_builds() {
        let p = array_agg();
        assert_eq!(p.name, "array_agg");
        assert!(p.tags.contains(&"postgres_only"));
        assert_eq!(p.dialect_support, DialectSupport::PostgresOnly);
    }

    #[test]
    fn array_agg_resolves() {
        let p = array_agg();
        let sql = resolve_pattern(&p, Dialect::PostgreSQL);
        assert!(
            sql.contains("ARRAY_AGG("),
            "expected ARRAY_AGG in sql: {sql}"
        );
    }

    #[test]
    fn multi_aggregate_builds() {
        let p = multi_aggregate();
        assert_eq!(p.name, "multi_aggregate");
        assert!(p.tags.contains(&"aggregate"));
        assert!(p.tags.contains(&"group_by"));

        // Should have exactly 2 ProjectAggregate constraints
        let agg_count = p
            .constraints
            .iter()
            .filter(|c| matches!(c, Constraint::ProjectAggregate { .. }))
            .count();
        assert_eq!(agg_count, 2, "expected 2 ProjectAggregate constraints");

        // Should have a GroupBy constraint
        assert!(
            p.constraints
                .iter()
                .any(|c| matches!(c, Constraint::GroupBy { .. }))
        );
    }

    #[test]
    fn multi_aggregate_resolves() {
        let p = multi_aggregate();
        let sql = resolve_pattern(&p, Dialect::MySQL);
        assert!(
            sql.contains("COUNT(") || sql.contains("count("),
            "expected COUNT in sql: {sql}"
        );
        assert!(
            sql.contains("SUM(") || sql.contains("sum("),
            "expected SUM in sql: {sql}"
        );
        assert!(sql.contains("GROUP BY"), "expected GROUP BY in sql: {sql}");
    }

    #[test]
    fn in_list_aggregate_builds() {
        let p = in_list_aggregate();
        assert_eq!(p.name, "in_list_aggregate");
        assert!(p.tags.contains(&"aggregate"));
        assert!(p.tags.contains(&"filter"));

        // Should have a ProjectAggregate constraint
        assert!(
            p.constraints
                .iter()
                .any(|c| matches!(c, Constraint::ProjectAggregate { .. }))
        );

        // Should have a WhereInParam constraint
        assert!(
            p.constraints
                .iter()
                .any(|c| matches!(c, Constraint::WhereInParam { num_values: 3, .. }))
        );
    }

    #[test]
    fn in_list_aggregate_resolves() {
        let p = in_list_aggregate();
        let sql = resolve_pattern(&p, Dialect::MySQL);
        assert!(
            sql.contains("SUM(") || sql.contains("sum("),
            "expected SUM in sql: {sql}"
        );
        assert!(sql.contains("IN"), "expected IN in sql: {sql}");
    }
}
