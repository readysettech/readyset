//! Utilities for extracting table references from SQL queries
//!
//! This module provides functionality to extract all tables referenced by various SQL query types,
//! which is useful for cache invalidation, dependency tracking, and other analysis tasks.

use std::collections::HashSet;

use readyset_client::ViewCreateRequest;
use readyset_client::query::Query;
use readyset_sql::analysis::visit::{Visitor, walk_select_statement};
use readyset_sql::ast::{Relation, TableExpr};

/// Visitor to collect all table references in a query, including nested ones
/// Only collects actual table names from TableExpr, not aliases from column references
#[derive(Default)]
struct TableReferenceCollector {
    tables: HashSet<Relation>,
}

impl<'ast> Visitor<'ast> for TableReferenceCollector {
    type Error = std::convert::Infallible;

    fn visit_table_expr(&mut self, table_expr: &'ast TableExpr) -> Result<(), Self::Error> {
        match &table_expr.inner {
            readyset_sql::ast::TableExprInner::Table(table) => {
                // Only collect actual table references, not aliases
                self.tables.insert(table.clone());
            }
            readyset_sql::ast::TableExprInner::Subquery(_) => {
                // Let the visitor handle subqueries recursively
            }
        }

        // Continue walking the table expression for nested structures
        readyset_sql::analysis::visit::walk_table_expr(self, table_expr)
    }
}

/// Extracts all table references from a query using visitor pattern,
/// returning None if the query type doesn't reference tables or if extraction fails
pub fn extract_referenced_tables(query: &Query) -> Option<Vec<Relation>> {
    match query {
        Query::Parsed(view_create_request) => {
            let mut collector = TableReferenceCollector::default();

            if walk_select_statement(&mut collector, &view_create_request.statement).is_err() {
                return None;
            }

            if collector.tables.is_empty() {
                None
            } else {
                Some(collector.tables.into_iter().collect())
            }
        }
        // ParseFailed queries don't have structured data to extract tables from
        // ShallowParsed are handled separately.
        Query::ShallowParsed(_) | Query::ParseFailed(..) => None,
    }
}

/// Extracts table references from a ViewCreateRequest (SELECT statement)
pub fn extract_from_view_create_request(
    view_create_request: &ViewCreateRequest,
) -> Option<Vec<Relation>> {
    let mut collector = TableReferenceCollector::default();

    if walk_select_statement(&mut collector, &view_create_request.statement).is_err() {
        return None;
    }

    if collector.tables.is_empty() {
        None
    } else {
        Some(collector.tables.into_iter().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use readyset_sql::Dialect;
    use readyset_sql::ast::SelectStatement;
    use readyset_sql_parsing::parse_select;

    fn select_statement(q: &str) -> Option<SelectStatement> {
        parse_select(Dialect::MySQL, q).ok()
    }

    #[test]
    fn extract_single_table() {
        let vcr = ViewCreateRequest::new(select_statement("SELECT * FROM users").unwrap(), vec![]);

        let tables = extract_from_view_create_request(&vcr).unwrap();
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].name, "users");
    }

    #[test]
    fn extract_join_tables() {
        let vcr = ViewCreateRequest::new(
            select_statement("SELECT * FROM users u JOIN orders o ON u.id = o.user_id").unwrap(),
            vec![],
        );

        let tables = extract_from_view_create_request(&vcr).unwrap();
        assert_eq!(tables.len(), 2);
        assert!(tables.iter().any(|t| t.name == "users"));
        assert!(tables.iter().any(|t| t.name == "orders"));
    }

    #[test]
    fn extract_multiple_joins() {
        let vcr = ViewCreateRequest::new(
            select_statement("SELECT * FROM users u JOIN orders o ON u.id = o.user_id JOIN products p ON o.product_id = p.id").unwrap(),
            vec![]
        );

        let tables = extract_from_view_create_request(&vcr).unwrap();
        assert_eq!(tables.len(), 3);
        assert!(tables.iter().any(|t| t.name == "users"));
        assert!(tables.iter().any(|t| t.name == "orders"));
        assert!(tables.iter().any(|t| t.name == "products"));
    }

    #[test]
    fn extract_from_parsed_query() {
        let vcr =
            ViewCreateRequest::new(select_statement("SELECT * FROM customers").unwrap(), vec![]);
        let query = Query::Parsed(std::sync::Arc::new(vcr));

        let tables = extract_referenced_tables(&query).unwrap();
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].name, "customers");
    }

    #[test]
    fn extract_from_parse_failed_returns_none() {
        let query = Query::ParseFailed(
            std::sync::Arc::new("invalid sql".to_string()),
            "parse error".to_string(),
        );

        let tables = extract_referenced_tables(&query);
        assert!(tables.is_none());
    }
}
