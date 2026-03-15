//! Aggregate patterns: count, sum, avg, min/max, count_distinct, group_by.

use readyset_sql::ast::BinaryOperator;

use crate::constraint::{AggregateFn, TypeClass};
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

#[cfg(test)]
mod tests {
    use readyset_sql::Dialect;

    use super::*;
    use crate::constraint::Constraint;
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
}
