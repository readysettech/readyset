//! Advanced patterns: window_function, distinct, multi_join.

use readyset_sql::ast::{JoinOperator, OrderType};

use crate::constraint::{TypeClass, WindowFn};
use crate::pattern::{Pattern, PatternBuilder};

/// SELECT t.c0, ROW_NUMBER() OVER (PARTITION BY t.c1 ORDER BY t.c2 ASC) FROM t
pub fn window_function() -> Pattern {
    let mut b = PatternBuilder::new("window_function");
    let t = b.table();
    let c_proj = b.column(t);
    let c_partition = b.column(t);
    let c_order = b.column(t);
    b.from(t);
    b.project_column(c_proj, t);
    b.window_function(
        WindowFn::RowNumber,
        Some((c_partition, t)),
        Some((c_order, t)),
        Some(OrderType::OrderAscending),
    );
    b.tags(&["advanced", "window"]);
    b.build()
}

/// SELECT t.c0, RANK() OVER (PARTITION BY t.c1 ORDER BY t.c2 ASC) FROM t
pub fn window_rank() -> Pattern {
    let mut b = PatternBuilder::new("window_rank");
    let t = b.table();
    let c_proj = b.column(t);
    let c_partition = b.column(t);
    let c_order = b.column(t);
    b.from(t);
    b.project_column(c_proj, t);
    b.window_function(
        WindowFn::Rank,
        Some((c_partition, t)),
        Some((c_order, t)),
        Some(OrderType::OrderAscending),
    );
    b.tags(&["advanced", "window"]);
    b.build()
}

/// SELECT t.c0, DENSE_RANK() OVER (PARTITION BY t.c1 ORDER BY t.c2 ASC) FROM t
pub fn window_dense_rank() -> Pattern {
    let mut b = PatternBuilder::new("window_dense_rank");
    let t = b.table();
    let c_proj = b.column(t);
    let c_partition = b.column(t);
    let c_order = b.column(t);
    b.from(t);
    b.project_column(c_proj, t);
    b.window_function(
        WindowFn::DenseRank,
        Some((c_partition, t)),
        Some((c_order, t)),
        Some(OrderType::OrderAscending),
    );
    b.tags(&["advanced", "window"]);
    b.build()
}

/// SELECT t.c0, ROW_NUMBER() OVER (PARTITION BY t.c1 ORDER BY t.c2 ASC) FROM t
/// WHERE t.c3 IN (?, ?, ?) — drives the post-lookup `RowNumber` aggregator.
pub fn window_row_number_in_list() -> Pattern {
    let mut b = PatternBuilder::new("window_row_number_in_list");
    let t = b.table();
    let c_proj = b.column(t);
    let c_partition = b.column(t);
    let c_order = b.column(t);
    let c_filter = b.column(t);
    b.from(t);
    b.project_column(c_proj, t);
    b.window_function(
        WindowFn::RowNumber,
        Some((c_partition, t)),
        Some((c_order, t)),
        Some(OrderType::OrderAscending),
    );
    b.where_in_param(c_filter, t, 3);
    b.tags(&["advanced", "window", "post_lookup"]);
    b.build()
}

/// SELECT t.c0, RANK() OVER (PARTITION BY t.c1 ORDER BY t.c2 ASC) FROM t
/// WHERE t.c3 IN (?, ?, ?) — drives the post-lookup `Rank` aggregator.
pub fn window_rank_in_list() -> Pattern {
    let mut b = PatternBuilder::new("window_rank_in_list");
    let t = b.table();
    let c_proj = b.column(t);
    let c_partition = b.column(t);
    let c_order = b.column(t);
    let c_filter = b.column(t);
    b.from(t);
    b.project_column(c_proj, t);
    b.window_function(
        WindowFn::Rank,
        Some((c_partition, t)),
        Some((c_order, t)),
        Some(OrderType::OrderAscending),
    );
    b.where_in_param(c_filter, t, 3);
    b.tags(&["advanced", "window", "post_lookup"]);
    b.build()
}

/// SELECT t.c0, DENSE_RANK() OVER (PARTITION BY t.c1 ORDER BY t.c2 ASC) FROM t
/// WHERE t.c3 IN (?, ?, ?) — drives the post-lookup `DenseRank` aggregator.
pub fn window_dense_rank_in_list() -> Pattern {
    let mut b = PatternBuilder::new("window_dense_rank_in_list");
    let t = b.table();
    let c_proj = b.column(t);
    let c_partition = b.column(t);
    let c_order = b.column(t);
    let c_filter = b.column(t);
    b.from(t);
    b.project_column(c_proj, t);
    b.window_function(
        WindowFn::DenseRank,
        Some((c_partition, t)),
        Some((c_order, t)),
        Some(OrderType::OrderAscending),
    );
    b.where_in_param(c_filter, t, 3);
    b.tags(&["advanced", "window", "post_lookup"]);
    b.build()
}

/// SELECT DISTINCT t.c0 FROM t
pub fn distinct() -> Pattern {
    let mut b = PatternBuilder::new("distinct");
    let t = b.table();
    let c = b.column(t);
    b.from(t);
    b.project_column(c, t);
    b.distinct();
    b.tags(&["advanced", "distinct"]);
    b.build()
}

/// SELECT t0.c0, t1.c1, t2.c2 FROM t0 JOIN t1 ON t0.c3 = t1.c4 JOIN t2 ON t1.c5 = t2.c6
pub fn multi_join() -> Pattern {
    let mut b = PatternBuilder::new("multi_join");
    let t0 = b.table();
    let t1 = b.table();
    let t2 = b.table();
    b.not_eq(t0, t1);
    b.not_eq(t0, t2);
    b.not_eq(t1, t2);

    // Projection columns
    let c0 = b.column(t0);
    let c1 = b.column(t1);
    let c2 = b.column(t2);

    // Join keys: t0-t1
    let jk0 = b.column(t0);
    let jk1 = b.column(t1);
    b.column_type_class(jk0, TypeClass::Integer);
    b.column_type_class(jk1, TypeClass::Integer);
    b.type_compatible(jk0, jk1);

    // Join keys: t1-t2
    let jk2 = b.column(t1);
    let jk3 = b.column(t2);
    b.column_type_class(jk2, TypeClass::Integer);
    b.column_type_class(jk3, TypeClass::Integer);
    b.type_compatible(jk2, jk3);

    b.from(t0);
    b.join_table(JoinOperator::InnerJoin, t1, jk0, jk1);
    b.join_table(JoinOperator::InnerJoin, t2, jk2, jk3);
    b.project_column(c0, t0);
    b.project_column(c1, t1);
    b.project_column(c2, t2);

    b.tags(&["advanced", "join", "multi_join", "three_table"]);
    b.build()
}

#[cfg(test)]
mod tests {
    use readyset_sql::Dialect;

    use super::*;
    use crate::constraint::Constraint;
    use crate::test_util::resolve_pattern;

    #[test]
    fn window_function_builds() {
        let p = window_function();
        assert_eq!(p.name, "window_function");
        assert!(p.tags.contains(&"window"));

        assert!(
            p.constraints
                .iter()
                .any(|c| matches!(c, Constraint::WindowFunction { .. }))
        );
    }

    #[test]
    fn distinct_builds() {
        let p = distinct();
        assert_eq!(p.name, "distinct");
        assert!(p.tags.contains(&"distinct"));

        assert!(
            p.constraints
                .iter()
                .any(|c| matches!(c, Constraint::Distinct))
        );
    }

    #[test]
    fn multi_join_builds() {
        let p = multi_join();
        assert_eq!(p.name, "multi_join");
        assert!(p.tags.contains(&"multi_join"));
        assert!(p.tags.contains(&"three_table"));

        // Should have exactly 2 Join constraints
        let join_count = p
            .constraints
            .iter()
            .filter(|c| matches!(c, Constraint::Join { .. }))
            .count();
        assert_eq!(join_count, 2);

        // Should have 3 NotEq constraints
        let not_eq_count = p
            .constraints
            .iter()
            .filter(|c| matches!(c, Constraint::NotEq(_, _)))
            .count();
        assert_eq!(not_eq_count, 3);
    }

    #[test]
    fn distinct_resolves() {
        let p = distinct();
        let sql = resolve_pattern(&p, Dialect::MySQL);
        assert!(sql.contains("DISTINCT"), "sql: {sql}");
    }

    #[test]
    fn multi_join_resolves() {
        let p = multi_join();
        let sql = resolve_pattern(&p, Dialect::MySQL);
        assert!(sql.contains("JOIN"), "sql: {sql}");
        // Should have at least 2 JOINs
        let join_count = sql.matches("JOIN").count();
        assert!(
            join_count >= 2,
            "expected >= 2 JOINs, got {join_count} in: {sql}"
        );
    }

    #[test]
    fn window_function_resolves() {
        let p = window_function();
        let sql = resolve_pattern(&p, Dialect::MySQL);
        // Window function should produce ROW_NUMBER() OVER (...)
        assert!(sql.contains("OVER"), "sql: {sql}");
    }

    #[test]
    fn window_function_order_by_has_pk_tiebreaker() {
        // ROW_NUMBER/RANK/DENSE_RANK are non-deterministic unless the OVER ORDER BY establishes a
        // total order. The builder appends the table's primary key as a tiebreaker so generated
        // window queries match upstream. The seeded PK is c0; the window orders by a later fresh
        // column, so without the tiebreaker the OVER clause would not reference c0.
        let p = window_function();
        let sql = resolve_pattern(&p, Dialect::MySQL);
        let over = sql.split_once("OVER").expect("window fn has OVER").1;
        assert!(
            over.contains("c0"),
            "OVER clause should order by the PK (c0) as a tiebreaker: {sql}"
        );
    }
}
