//! Rewrite pass to resolve schemas for table references.
//!
//! See [`ResolveSchemas::resolve_schemas`] for more information.

use std::collections::{HashMap, HashSet};

use nom_sql::analysis::visit::{self, walk_select_statement, Visitor};
use nom_sql::{SelectStatement, SqlIdentifier, Table, TableExpr};

struct ResolveSchemaVisitor<'schema> {
    /// Map from schema name to the set of table names in that schema
    tables: HashMap<&'schema SqlIdentifier, HashSet<&'schema SqlIdentifier>>,

    /// List of schema names to use to resolve schemas for tables. Schemas earlier in this list
    /// will take precedence over schemas later in this list
    search_path: &'schema [SqlIdentifier],

    /// Stack of visible aliases for table expressions, which should not be resolved to tables in
    /// the database schema.
    ///
    /// Each element of this `Vec` is a level of subquery nesting, which can be `pop()`ed after
    /// walking through a query.
    alias_stack: Vec<HashSet<SqlIdentifier>>,
}

impl<'schema> ResolveSchemaVisitor<'schema> {
    fn insert_alias(&mut self, alias: SqlIdentifier) {
        self.alias_stack.last_mut().unwrap().insert(alias);
    }
}

impl<'ast, 'schema> Visitor<'ast> for ResolveSchemaVisitor<'schema> {
    type Error = !;

    fn visit_select_statement(
        &mut self,
        select_statement: &'ast mut SelectStatement,
    ) -> Result<(), Self::Error> {
        // We need to gather aliases from the table exprs here (not in `visit_table_expr`) since
        // normally they get walked after field exprs
        let table_expr_aliases = select_statement
            .tables
            .iter()
            .filter_map(|te| te.alias.clone())
            .collect();
        self.alias_stack.push(table_expr_aliases);

        walk_select_statement(self, select_statement)?;
        self.alias_stack.pop();
        Ok(())
    }

    fn visit_common_table_expr(
        &mut self,
        cte: &'ast mut nom_sql::CommonTableExpr,
    ) -> Result<(), Self::Error> {
        // Walk first, since the alias for the CTE is not visible inside the CTE itself (TODO:
        // except in the case of `WITH RECURSIVE`, which we don't even parse yet).
        visit::walk_common_table_expr(self, cte)?;
        self.insert_alias(cte.name.clone());
        Ok(())
    }

    fn visit_table_expr(&mut self, table_expr: &'ast mut TableExpr) -> Result<(), Self::Error> {
        if let Some(alias) = &table_expr.alias {
            self.insert_alias(alias.clone())
        }
        visit::walk_table_expr(self, table_expr)
    }

    fn visit_table(&mut self, table: &'ast mut Table) -> Result<(), Self::Error> {
        if table.schema.is_some() {
            return Ok(());
        }

        if self
            .alias_stack
            .iter()
            .any(|frame| frame.contains(&table.name))
        {
            // Reference to aliased table expression; remove
            return Ok(());
        }

        if let Some(schema) = self.search_path.iter().find(|schema| {
            self.tables
                .get(schema)
                .into_iter()
                .any(|ts| ts.contains(&table.name))
        }) {
            table.schema = Some(schema.clone());
        }

        Ok(())
    }
}

pub trait ResolveSchemas {
    /// Attempt to resolve schemas for all non-schema-qualified table references in `self` by
    /// looking up those tables in `tables`, using `search_path` for precedence.
    ///
    /// A couple of details worth noting:
    ///
    /// * Any schemas which do not appear in `search_path` will not be used to resolve tables, even
    ///   if they appear in `tables`
    /// * Any tables which *do not* resolve to any of the tables in `tables` will be left
    ///   unqualified (it is not the responsibility of this pass to make sure referenced tables
    ///   exist).
    /// * Any unqualified references to aliases for tables (including CTEs) will not be rewritten,
    ///   as they should take precedence over tables in the database
    fn resolve_schemas<'schema>(
        self,
        tables: HashMap<&'schema SqlIdentifier, HashSet<&'schema SqlIdentifier>>,
        search_path: &'schema [SqlIdentifier],
    ) -> Self;
}

impl ResolveSchemas for SelectStatement {
    fn resolve_schemas<'schema>(
        mut self,
        tables: HashMap<&'schema SqlIdentifier, HashSet<&'schema SqlIdentifier>>,
        search_path: &'schema [SqlIdentifier],
    ) -> Self {
        let Ok(()) = ResolveSchemaVisitor {
            tables,
            search_path,
            alias_stack: Default::default(),
        }
        .visit_select_statement(&mut self);

        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::parse_select_statement;

    #[track_caller]
    fn rewrites_to(input: &str, expected: &str) {
        let input = parse_select_statement(input);
        let expected = parse_select_statement(expected);
        let result = input.resolve_schemas(
            HashMap::from([
                (&"s1".into(), HashSet::from([&"t1".into(), &"t2".into()])),
                (
                    &"s2".into(),
                    HashSet::from([&"t1".into(), &"t2".into(), &"t3".into()]),
                ),
                (&"s3".into(), HashSet::from([&"t4".into()])),
            ]),
            &["s1".into(), "s2".into()],
        );
        assert_eq!(
            result, expected,
            "\nExpected: {expected}\n     Got: {result}"
        );
    }

    #[test]
    fn rewrites_in_star() {
        rewrites_to("select t1.* from t1", "select s1.t1.* from s1.t1")
    }

    #[test]
    fn resolve_table_in_top_of_search_path() {
        rewrites_to("select * from t1", "select * from s1.t1");
    }

    #[test]
    fn resolve_table_deeper_in_search_path() {
        rewrites_to("select * from t3", "select * from s2.t3");
    }

    #[test]
    fn table_not_in_search_path_is_untouched() {
        rewrites_to("select * from t1, t4", "select * from s1.t1, t4");
    }

    #[test]
    fn ignores_cte_alias_reference() {
        rewrites_to(
            "with t2 as (select * from t1) select * from t2",
            "with t2 as (select * from s1.t1) select * from t2",
        );
    }

    #[test]
    fn ignores_table_expr_alias_reference() {
        rewrites_to("select t2.* from t1 as t2", "select t2.* from s1.t1 as t2");
    }

    #[test]
    fn subqueries_dont_resolve_down() {
        rewrites_to(
            "select t1.* from t1 join (select t1.* from t2 as t1) sq",
            "select s1.t1.* from s1.t1 join (select t1.* from s1.t2 as t1) sq",
        )
    }
}
