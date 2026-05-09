//! Basic patterns: the simplest query shapes.

use readyset_sql::ast::BinaryOperator;

use crate::constraint::LiteralKind;
use crate::pattern::{Pattern, PatternBuilder};

/// SELECT t.c FROM t
pub fn single_table() -> Pattern {
    let mut b = PatternBuilder::new("single_table");
    let t = b.table();
    let c = b.column(t);
    b.from(t);
    b.project_column(c, t);
    b.tags(&["base"]);
    b.build()
}

/// SELECT t.c1 FROM t WHERE t.c2 = ?
pub fn single_parameter() -> Pattern {
    let mut b = PatternBuilder::new("single_parameter");
    let t = b.table();
    let c1 = b.column(t);
    let c2 = b.column(t);
    b.from(t);
    b.project_column(c1, t);
    b.where_param(c2, t, BinaryOperator::Equal);
    b.tags(&["filter", "parameter"]);
    b.build()
}

/// SELECT 1 FROM t
pub fn project_literal() -> Pattern {
    let mut b = PatternBuilder::new("project_literal");
    let t = b.table();
    b.from(t);
    b.project_literal(LiteralKind::Integer);
    b.tags(&["literal"]);
    b.build()
}

#[cfg(test)]
mod tests {
    use readyset_sql::Dialect;

    use super::*;
    use crate::test_util::resolve_pattern;

    #[test]
    fn single_table_builds() {
        let p = single_table();
        assert_eq!(p.name, "single_table");
        assert_eq!(p.tags, vec!["base"]);
        assert_eq!(p.min_depth, 0);
        assert_eq!(p.num_vars(), 2); // table + column
    }

    #[test]
    fn single_parameter_builds() {
        let p = single_parameter();
        assert_eq!(p.name, "single_parameter");
        assert_eq!(p.tags, vec!["filter", "parameter"]);
        assert_eq!(p.min_depth, 0);
        assert_eq!(p.num_vars(), 4); // table + 2 columns + 1 param
    }

    #[test]
    fn project_literal_builds() {
        let p = project_literal();
        assert_eq!(p.name, "project_literal");
        assert_eq!(p.tags, vec!["literal"]);
        assert_eq!(p.min_depth, 0);
        assert_eq!(p.num_vars(), 1); // table only
    }

    #[test]
    fn single_table_resolves() {
        let p = single_table();
        let sql = resolve_pattern(&p, Dialect::MySQL);
        assert!(sql.contains("SELECT"), "sql: {sql}");
        assert!(sql.contains("FROM"), "sql: {sql}");
        assert!(!sql.contains("WHERE"), "sql: {sql}");
    }

    #[test]
    fn single_parameter_resolves_mysql() {
        let p = single_parameter();
        let sql = resolve_pattern(&p, Dialect::MySQL);
        assert!(sql.contains("SELECT"), "sql: {sql}");
        assert!(sql.contains("FROM"), "sql: {sql}");
        assert!(sql.contains("WHERE"), "sql: {sql}");
        assert!(sql.contains("= ?"), "sql: {sql}");
    }

    #[test]
    fn single_parameter_resolves_postgres() {
        let p = single_parameter();
        let sql = resolve_pattern(&p, Dialect::PostgreSQL);
        assert!(sql.contains("$1"), "sql: {sql}");
    }

    #[test]
    fn project_literal_resolves() {
        let p = project_literal();
        let sql = resolve_pattern(&p, Dialect::MySQL);
        assert!(sql.contains("SELECT"), "sql: {sql}");
        assert!(sql.contains("FROM"), "sql: {sql}");
        assert!(sql.contains("1"), "sql: {sql}"); // literal integer 1
    }
}
