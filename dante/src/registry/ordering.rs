//! Ordering and limit patterns: topk, order_by, paginate.

use readyset_sql::ast::OrderType;

use crate::pattern::{Pattern, PatternBuilder};

/// SELECT t.c1 FROM t ORDER BY t.c2 ASC LIMIT 10
pub fn topk() -> Pattern {
    let mut b = PatternBuilder::new("topk");
    let t = b.table();
    let c1 = b.column(t);
    let c2 = b.column(t);
    b.from(t);
    b.project_column(c1, t);
    b.order_by(c2, t, OrderType::OrderAscending, None);
    b.limit(10, None);
    b.tags(&["topk", "ordering", "limit"]);
    b.build()
}

/// SELECT t.c1 FROM t ORDER BY t.c2 DESC
pub fn order_by() -> Pattern {
    let mut b = PatternBuilder::new("order_by");
    let t = b.table();
    let c1 = b.column(t);
    let c2 = b.column(t);
    b.from(t);
    b.project_column(c1, t);
    b.order_by(c2, t, OrderType::OrderDescending, None);
    b.tags(&["ordering"]);
    b.build()
}

/// SELECT t.c1 FROM t ORDER BY t.c2 ASC LIMIT 10 OFFSET 5
///
/// ORDER BY + LIMIT + OFFSET triggers the Paginate dataflow node
/// (as opposed to TopK which requires no OFFSET).
pub fn paginate() -> Pattern {
    let mut b = PatternBuilder::new("paginate");
    let t = b.table();
    let c1 = b.column(t);
    let c2 = b.column(t);
    b.from(t);
    b.project_column(c1, t);
    b.order_by(c2, t, OrderType::OrderAscending, None);
    b.limit(10, Some(5));
    b.tags(&["ordering", "limit", "paginate"]);
    b.build()
}

/// SELECT t.c1 FROM t WHERE t.c0 = 1 ORDER BY t.c1 ASC LIMIT 1
///
/// The WHERE clause pins the unique key to a concrete literal (not a placeholder),
/// so the `order_limit_removal` rewrite pass can eliminate the ORDER BY and LIMIT
/// at query planning time (at most one row matches a unique key).
pub fn topk_unique_key_eq() -> Pattern {
    let mut b = PatternBuilder::new("topk_unique_key_eq");
    let t = b.table();
    let c_proj = b.column(t);
    let c_key = b.column(t);
    b.column_type_class(c_key, crate::constraint::TypeClass::Integer);
    b.column_unique_not_null(c_key);
    b.from(t);
    b.project_column(c_proj, t);
    b.where_literal_eq(c_key, t);
    b.order_by(c_proj, t, OrderType::OrderAscending, None);
    b.limit(1, None);
    b.tags(&["ordering", "limit", "order_limit_removal"]);
    b.build()
}

#[cfg(test)]
mod tests {
    use readyset_sql::Dialect;

    use super::*;
    use crate::constraint::Constraint;
    use crate::test_util::resolve_pattern;

    #[test]
    fn topk_builds() {
        let p = topk();
        assert_eq!(p.name, "topk");
        assert!(p.tags.contains(&"topk"));
        assert!(p.tags.contains(&"ordering"));
        assert!(p.tags.contains(&"limit"));

        // Should have both OrderBy and Limit constraints
        assert!(
            p.constraints
                .iter()
                .any(|c| matches!(c, Constraint::OrderBy { .. }))
        );
        assert!(
            p.constraints
                .iter()
                .any(|c| matches!(c, Constraint::Limit { .. }))
        );
    }

    #[test]
    fn order_by_builds() {
        let p = order_by();
        assert_eq!(p.name, "order_by");
        assert!(p.tags.contains(&"ordering"));
    }

    #[test]
    fn topk_resolves() {
        let p = topk();
        let sql = resolve_pattern(&p, Dialect::MySQL);
        assert!(sql.contains("ORDER BY"), "sql: {sql}");
        assert!(sql.contains("LIMIT"), "sql: {sql}");
    }

    #[test]
    fn order_by_resolves() {
        let p = order_by();
        let sql = resolve_pattern(&p, Dialect::MySQL);
        assert!(sql.contains("ORDER BY"), "sql: {sql}");
        assert!(!sql.contains("LIMIT"), "sql: {sql}");
    }

    #[test]
    fn order_by_has_pk_tiebreaker() {
        // ORDER BY on a non-unique column is non-deterministic — and under LIMIT the returned
        // row SET becomes non-deterministic — unless the ordering is total. The builder appends
        // the table's primary key as a tiebreaker. The seeded PK is c0 and the order column is a
        // later fresh column, so without the tiebreaker the ORDER BY would not reference c0.
        for p in [topk(), order_by(), paginate()] {
            let sql = resolve_pattern(&p, Dialect::MySQL);
            let order = sql.split_once("ORDER BY").expect("has ORDER BY").1;
            assert!(
                order.contains("c0"),
                "ORDER BY should append the PK (c0) as a tiebreaker: {sql}"
            );
        }
    }

    #[test]
    fn paginate_builds() {
        let p = paginate();
        assert_eq!(p.name, "paginate");
        assert!(p.tags.contains(&"ordering"));
        assert!(p.tags.contains(&"limit"));
        assert!(p.tags.contains(&"paginate"));

        // Should have both OrderBy and Limit (with offset) constraints
        assert!(
            p.constraints
                .iter()
                .any(|c| matches!(c, Constraint::OrderBy { .. }))
        );
        assert!(p.constraints.iter().any(|c| matches!(
            c,
            Constraint::Limit {
                offset: Some(_),
                ..
            }
        )));
    }

    #[test]
    fn paginate_resolves() {
        let p = paginate();
        let sql = resolve_pattern(&p, Dialect::MySQL);
        assert!(sql.contains("ORDER BY"), "sql: {sql}");
        assert!(sql.contains("LIMIT"), "sql: {sql}");
        assert!(sql.contains("OFFSET"), "sql: {sql}");
    }

    #[test]
    fn topk_unique_key_eq_builds() {
        let p = topk_unique_key_eq();
        assert_eq!(p.name, "topk_unique_key_eq");
        assert!(p.tags.contains(&"order_limit_removal"));
        assert!(
            p.constraints
                .iter()
                .any(|c| matches!(c, Constraint::WhereLiteralEq { .. }))
        );
        assert!(
            p.constraints
                .iter()
                .any(|c| matches!(c, Constraint::OrderBy { .. }))
        );
        assert!(
            p.constraints
                .iter()
                .any(|c| matches!(c, Constraint::Limit { .. }))
        );
        assert!(
            p.constraints
                .iter()
                .any(|c| matches!(c, Constraint::ColumnUniqueNotNull { .. }))
        );
    }

    #[test]
    fn topk_unique_key_eq_resolves() {
        let p = topk_unique_key_eq();
        let sql = resolve_pattern(&p, Dialect::MySQL);
        // Must have a concrete literal, not a parameter placeholder.
        assert!(
            sql.contains("= 1"),
            "sql should have concrete literal: {sql}"
        );
        assert!(!sql.contains("= ?"), "sql must not have placeholder: {sql}");
        assert!(
            !sql.contains("= $"),
            "sql must not have PG placeholder: {sql}"
        );
        assert!(sql.contains("ORDER BY"), "sql: {sql}");
        assert!(sql.contains("LIMIT"), "sql: {sql}");
    }
}
