//! Filter patterns: between, in_list, like, is_null, compound_where.

use readyset_sql::ast::{BinaryOperator, SqlType};

use crate::constraint::{DialectSupport, ExampleCell, ExampleValue, TypeClass};
use crate::pattern::{Pattern, PatternBuilder};

/// SELECT t.c1 FROM t WHERE t.c2 BETWEEN ? AND ?
pub fn between() -> Pattern {
    let mut b = PatternBuilder::new("between");
    let t = b.table();
    let c1 = b.column(t);
    let c2 = b.column(t);
    b.from(t);
    b.project_column(c1, t);
    b.where_between_param(c2, t);
    b.tags(&["filter"]);
    b.build()
}

/// SELECT t.c1 FROM t WHERE t.c2 IN (?, ?, ?)
pub fn in_list() -> Pattern {
    let mut b = PatternBuilder::new("in_list");
    let t = b.table();
    let c1 = b.column(t);
    let c2 = b.column(t);
    b.from(t);
    b.project_column(c1, t);
    b.where_in_param(c2, t, 3);
    b.tags(&["filter"]);
    b.build()
}

/// Probe a three-character key with both the stored value and an overlong value that shares its
/// prefix. Only the exact lookup should match the seeded row.
pub fn pg_overlong_key(name: &'static str, key_type: SqlType) -> Pattern {
    let mut b = PatternBuilder::new(name);
    let table = b.table();
    let key = b.column(table);
    b.column_type_class(key, TypeClass::Exact(key_type));
    b.from(table);
    b.project_column(key, table);
    let param = b.where_param(key, table, BinaryOperator::Equal);
    let cell = |var, value| ExampleCell {
        var,
        value: ExampleValue::Literal(value),
    };
    b.example(
        "overlong lookup",
        DialectSupport::PostgresOnly,
        vec![cell(key, "abc"), cell(param, "abcd")],
    );
    b.example(
        "exact lookup",
        DialectSupport::PostgresOnly,
        vec![cell(param, "abc")],
    );
    b.set_dialect_support(DialectSupport::PostgresOnly);
    b.tags(&[
        "filter",
        "parameter",
        "string",
        "type_reproducer",
        "exclude-nightly",
        "no-compose",
    ]);
    b.build()
}

/// PostgreSQL drops the CHAR padding when comparing with TEXT, while preserving the TEXT value.
pub fn pg_char_text_compare() -> Pattern {
    let mut b = PatternBuilder::new("pg_char_text_compare");
    let table = b.table();
    let fixed = b.column(table);
    let text = b.column(table);
    b.column_type_class(fixed, TypeClass::Exact(SqlType::Char(Some(3))));
    b.column_type_class(text, TypeClass::Exact(SqlType::Text));
    b.from(table);
    b.project_column(text, table);
    b.where_column_compare(fixed, table, BinaryOperator::Equal, text, table);
    for value in ["ab", "ab "] {
        b.example(
            "CHAR compared with TEXT",
            DialectSupport::PostgresOnly,
            vec![
                ExampleCell {
                    var: fixed,
                    value: ExampleValue::Literal("ab"),
                },
                ExampleCell {
                    var: text,
                    value: ExampleValue::Literal(value),
                },
            ],
        );
    }
    b.set_dialect_support(DialectSupport::PostgresOnly);
    b.tags(&[
        "filter",
        "string",
        "type_reproducer",
        "exclude-nightly",
        "no-compose",
    ]);
    b.build()
}

/// SELECT t.c1 FROM t WHERE t.c2 LIKE ?
pub fn like() -> Pattern {
    let mut b = PatternBuilder::new("like");
    let t = b.table();
    let c1 = b.column(t);
    let c2 = b.column(t);
    b.column_type_class(c2, TypeClass::String);
    b.from(t);
    b.project_column(c1, t);
    b.where_like(c2, t, false);
    b.tags(&["filter", "string"]);
    b.build()
}

/// SELECT t.c1 FROM t WHERE t.c2 IS NULL
pub fn is_null() -> Pattern {
    let mut b = PatternBuilder::new("is_null");
    let t = b.table();
    let c1 = b.column(t);
    let c2 = b.column(t);
    b.from(t);
    b.project_column(c1, t);
    b.where_is_null(c2, t, false);
    b.tags(&["filter"]);
    b.build()
}

/// SELECT t.c1 FROM t WHERE t.c2 = ? AND t.c3 > ?
pub fn compound_where() -> Pattern {
    let mut b = PatternBuilder::new("compound_where");
    let t = b.table();
    let c1 = b.column(t);
    let c2 = b.column(t);
    let c3 = b.column(t);
    b.from(t);
    b.project_column(c1, t);
    b.where_param(c2, t, BinaryOperator::Equal);
    b.where_param(c3, t, BinaryOperator::Greater);
    b.tags(&["filter", "compound"]);
    b.build()
}

#[cfg(test)]
mod tests {
    use readyset_sql::Dialect;

    use super::*;
    use crate::constraint::Constraint;
    use crate::test_util::resolve_pattern;

    #[test]
    fn between_builds() {
        let p = between();
        assert_eq!(p.name, "between");
        assert!(p.tags.contains(&"filter"));
        assert_eq!(p.num_vars(), 5); // table + 2 columns + lo + hi params
    }

    #[test]
    fn in_list_builds() {
        let p = in_list();
        assert_eq!(p.name, "in_list");
        assert!(p.tags.contains(&"filter"));
    }

    #[test]
    fn like_builds() {
        let p = like();
        assert_eq!(p.name, "like");
        assert!(p.tags.contains(&"string"));

        // Should have a ColumnTypeClass(String) constraint
        assert!(p.constraints.iter().any(|c| matches!(
            c,
            Constraint::ColumnTypeClass {
                type_class: TypeClass::String,
                ..
            }
        )));
    }

    #[test]
    fn is_null_builds() {
        let p = is_null();
        assert_eq!(p.name, "is_null");
        assert!(p.tags.contains(&"filter"));
    }

    #[test]
    fn compound_where_builds() {
        let p = compound_where();
        assert_eq!(p.name, "compound_where");
        assert!(p.tags.contains(&"compound"));

        // Should have exactly 2 WhereParam constraints
        let where_count = p
            .constraints
            .iter()
            .filter(|c| matches!(c, Constraint::WhereParam { .. }))
            .count();
        assert_eq!(where_count, 2);
    }

    #[test]
    fn between_resolves() {
        let p = between();
        let sql = resolve_pattern(&p, Dialect::PostgreSQL);
        assert!(sql.contains("BETWEEN"), "sql: {sql}");
        assert!(sql.contains("$1"), "sql: {sql}");
        assert!(sql.contains("$2"), "sql: {sql}");
    }

    #[test]
    fn in_list_resolves() {
        let p = in_list();
        let sql = resolve_pattern(&p, Dialect::PostgreSQL);
        assert!(sql.contains("IN"), "sql: {sql}");
    }

    #[test]
    fn like_resolves() {
        let p = like();
        let sql = resolve_pattern(&p, Dialect::MySQL);
        assert!(sql.contains("LIKE"), "sql: {sql}");
    }

    #[test]
    fn is_null_resolves() {
        let p = is_null();
        let sql = resolve_pattern(&p, Dialect::MySQL);
        assert!(sql.contains("IS NULL"), "sql: {sql}");
    }

    #[test]
    fn compound_where_resolves() {
        let p = compound_where();
        let sql = resolve_pattern(&p, Dialect::PostgreSQL);
        assert!(sql.contains("$1"), "sql: {sql}");
        assert!(sql.contains("$2"), "sql: {sql}");
        assert!(sql.contains("AND"), "sql: {sql}");
    }
}
