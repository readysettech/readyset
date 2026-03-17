//! Detection of schema-qualified table references in sqlparser ASTs.
//!
//! This module provides a function to check whether a sqlparser [`Query`] AST contains any table
//! references that are qualified with a specific schema/database name. This is used to detect
//! queries like `SELECT * FROM readyset.test` that should be routed to the readyset-schema
//! (DataFusion) engine, even when the user hasn't issued a `USE readyset` statement.

use std::ops::ControlFlow;

use sqlparser::ast::{ObjectName, ObjectNamePart, Query, Visit, Visitor};

/// Returns `true` if any table reference in the query uses the given schema qualifier.
///
/// Walks the entire sqlparser AST looking for [`ObjectName`] nodes with two or more parts where
/// the first part matches `schema` (case-insensitive). This covers `FROM`, `JOIN`, subqueries,
/// `UNION`, and any other context where a table name appears.
///
/// # Examples
///
/// ```ignore
/// // "SELECT * FROM readyset.test" → true for schema "readyset"
/// // "SELECT * FROM test"          → false for schema "readyset"
/// ```
pub fn references_schema(query: &Query, schema: &str) -> bool {
    struct SchemaDetector<'a> {
        schema: &'a str,
    }

    impl<'a> Visitor for SchemaDetector<'a> {
        type Break = ();

        fn pre_visit_relation(&mut self, name: &ObjectName) -> ControlFlow<Self::Break> {
            if has_schema_prefix(name, self.schema) {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        }
    }

    let mut detector = SchemaDetector { schema };
    query.visit(&mut detector).is_break()
}

/// Checks if an [`ObjectName`] has a schema prefix matching the given name (case-insensitive).
///
/// An `ObjectName` like `readyset.test` is represented as two `ObjectNamePart::Identifier` entries.
/// We check that there are at least 2 parts and the first is an identifier matching `schema`.
fn has_schema_prefix(name: &ObjectName, schema: &str) -> bool {
    let parts = &name.0;
    if parts.len() < 2 {
        return false;
    }
    match &parts[0] {
        ObjectNamePart::Identifier(ident) => ident.value.eq_ignore_ascii_case(schema),
        ObjectNamePart::Function(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    use super::*;

    fn parse_query(sql: &str) -> Query {
        let stmts = Parser::parse_sql(&GenericDialect {}, sql).expect("failed to parse SQL");
        match stmts.into_iter().next().expect("no statements") {
            sqlparser::ast::Statement::Query(q) => *q,
            other => panic!("expected Query, got {other:?}"),
        }
    }

    #[test]
    fn qualified_table_reference() {
        let q = parse_query("SELECT * FROM readyset.test");
        assert!(references_schema(&q, "readyset"));
    }

    #[test]
    fn unqualified_table_reference() {
        let q = parse_query("SELECT * FROM test");
        assert!(!references_schema(&q, "readyset"));
    }

    #[test]
    fn different_schema() {
        let q = parse_query("SELECT * FROM other_schema.test");
        assert!(!references_schema(&q, "readyset"));
    }

    #[test]
    fn case_insensitive() {
        let q = parse_query("SELECT * FROM READYSET.test");
        assert!(references_schema(&q, "readyset"));
    }

    #[test]
    fn join_with_qualified_reference() {
        let q = parse_query(
            "SELECT * FROM plain_table JOIN readyset.other ON plain_table.id = readyset.other.id",
        );
        assert!(references_schema(&q, "readyset"));
    }

    #[test]
    fn subquery_with_qualified_reference() {
        let q = parse_query("SELECT * FROM (SELECT * FROM readyset.inner_table) sub");
        assert!(references_schema(&q, "readyset"));
    }

    #[test]
    fn union_with_qualified_reference() {
        let q = parse_query("SELECT * FROM plain UNION SELECT * FROM readyset.other");
        assert!(references_schema(&q, "readyset"));
    }

    #[test]
    fn no_false_positive_on_column_reference() {
        // Schema qualifier on column should not trigger (it's not a table reference)
        let q = parse_query("SELECT readyset.col FROM test");
        assert!(!references_schema(&q, "readyset"));
    }

    #[test]
    fn multiple_qualified_tables() {
        let q = parse_query(
            "SELECT * FROM readyset.a JOIN readyset.b ON readyset.a.id = readyset.b.id",
        );
        assert!(references_schema(&q, "readyset"));
    }
}
