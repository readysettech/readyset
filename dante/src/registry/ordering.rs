//! Ordering and limit patterns: topk, order_by, limit_offset.

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

/// SELECT t.c1 FROM t LIMIT 10 OFFSET 5
pub fn limit_offset() -> Pattern {
    let mut b = PatternBuilder::new("limit_offset");
    let t = b.table();
    let c = b.column(t);
    b.from(t);
    b.project_column(c, t);
    b.limit(10, Some(5));
    b.tags(&["limit"]);
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
    fn limit_offset_builds() {
        let p = limit_offset();
        assert_eq!(p.name, "limit_offset");
        assert!(p.tags.contains(&"limit"));

        // Should have Limit with offset
        assert!(p.constraints.iter().any(|c| matches!(
            c,
            Constraint::Limit {
                offset: Some(5),
                ..
            }
        )));
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
    fn limit_offset_resolves() {
        let p = limit_offset();
        let sql = resolve_pattern(&p, Dialect::MySQL);
        assert!(sql.contains("LIMIT"), "sql: {sql}");
        assert!(sql.contains("OFFSET"), "sql: {sql}");
    }
}
