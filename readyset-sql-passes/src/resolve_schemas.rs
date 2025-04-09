//! Rewrite pass to resolve schemas for table references.
//!
//! See [`ResolveSchemas::resolve_schemas`] for more information.

use std::collections::{HashMap, HashSet};

use readyset_errors::{ReadySetError, ReadySetResult};
use readyset_sql::analysis::visit_mut::{self, VisitorMut};
use readyset_sql::ast::{
    CreateTableStatement, JoinClause, JoinRightSide, Relation, SelectStatement, SqlIdentifier,
    SqlType,
};

use crate::CanQuery;

struct ResolveSchemaVisitor<'schema> {
    /// Map from schema name to the set of table names in that schema
    tables: HashMap<&'schema SqlIdentifier, HashMap<&'schema SqlIdentifier, CanQuery>>,

    /// Map from schema name to the set of custom types in that schema
    custom_types: &'schema HashMap<&'schema SqlIdentifier, HashSet<&'schema SqlIdentifier>>,

    /// List of schema names to use to resolve schemas for tables. Schemas earlier in this list
    /// will take precedence over schemas later in this list
    search_path: &'schema [SqlIdentifier],

    /// Stack of visible aliases for table expressions, which should not be resolved to tables in
    /// the database schema.
    ///
    /// Each element of this `Vec` is a level of subquery nesting, which can be `pop()`ed after
    /// walking through a query.
    alias_stack: Vec<HashSet<SqlIdentifier>>,

    /// List of tables which, if created, should invalidate this query.
    invalidating_tables: Option<&'schema mut Vec<Relation>>,
}

impl ResolveSchemaVisitor<'_> {
    fn table_is_aliased(&self, table: &Relation) -> bool {
        self.alias_stack
            .iter()
            .any(|frame| frame.contains(&table.name))
    }

    fn resolve_schema(&mut self, table: &mut Relation) -> Result<(), ReadySetError> {
        for schema in self.search_path {
            let found = self
                .tables
                .get(schema)
                .into_iter()
                .find_map(|ts| ts.get(&table.name).copied());
            match found {
                Some(CanQuery::Yes) => {
                    table.schema = Some(schema.clone());
                    return Ok(());
                }
                Some(CanQuery::No) => {
                    return Err(ReadySetError::TableNotReplicated {
                        name: table.name.clone().into(),
                        schema: Some(schema.into()),
                    });
                }
                None => {
                    if let Some(invalidating) = self.invalidating_tables.as_deref_mut() {
                        invalidating.push(Relation {
                            schema: Some(schema.clone()),
                            name: table.name.clone(),
                        });
                    }
                }
            };
        }
        Ok(())
    }

    /// Visit a join clause with an extra set of aliases which should be available for the `ON`
    /// condition but *not* for the table factor or subquery. We take ownership of it and return it
    /// at the end to avoid cloning it in the loop in `visit_select_statement`.
    fn visit_join_clause_with_extra_aliases(
        &mut self,
        join: &mut JoinClause,
        extra_aliases: HashSet<SqlIdentifier>,
    ) -> Result<HashSet<SqlIdentifier>, ReadySetError> {
        match &mut join.right {
            JoinRightSide::Table(table_expr) => self.visit_table_expr(table_expr)?,
            JoinRightSide::Tables(table_exprs) => {
                for table_expr in table_exprs {
                    self.visit_table_expr(table_expr)?;
                }
            }
        }

        self.alias_stack.push(extra_aliases);
        self.visit_join_constraint(&mut join.constraint)?;
        self.alias_stack
            .pop()
            .ok_or_else(|| ReadySetError::Internal("Alias stack underflow".to_string()))
    }
}

impl<'ast> VisitorMut<'ast> for ResolveSchemaVisitor<'_> {
    type Error = ReadySetError;

    fn visit_sql_type(&mut self, sql_type: &'ast mut SqlType) -> Result<(), Self::Error> {
        if let SqlType::Other(ty) = sql_type {
            if ty.schema.is_none() {
                if let Some(schema) = self.search_path.iter().find(|schema| {
                    self.custom_types
                        .get(schema)
                        .into_iter()
                        .any(|tys| tys.contains(&ty.name))
                }) {
                    ty.schema = Some(schema.clone());
                }
            }
        }

        visit_mut::walk_sql_type(self, sql_type)
    }

    fn visit_select_statement(
        &mut self,
        select_statement: &'ast mut SelectStatement,
    ) -> Result<(), Self::Error> {
        for cte in &mut select_statement.ctes {
            self.visit_common_table_expr(cte)?;
        }

        // CTE aliases should be available everywhere except the CTE definitions (unless we have
        // `WITH RECURSIVE`, which we don't support)
        self.alias_stack.push(
            select_statement
                .ctes
                .iter()
                .map(|cte| cte.name.clone())
                .collect(),
        );

        for table_expr in &mut select_statement.tables {
            self.visit_table_expr(table_expr)?;
        }

        // These aliases will be available in `JOIN ON` conditions, the field list, `WHERE`
        // conditions, and subqueries within those clauses. They will not be available in CTEs, the
        // table list, or table expressions in `JOIN` clauses (specifically, subqueries in the table
        // factors themselves, and CTE definitions, should not see these aliases).
        let mut pending_aliases: HashSet<_> = select_statement
            .tables
            .iter()
            .chain(
                select_statement
                    .join
                    .iter()
                    .flat_map(|j| j.right.table_exprs()),
            )
            .filter_map(|te| te.alias.clone())
            .collect();

        for join in &mut select_statement.join {
            pending_aliases = self.visit_join_clause_with_extra_aliases(join, pending_aliases)?;
        }

        // Now the pending aliases are available to subqueries
        self.alias_stack.last_mut().unwrap().extend(pending_aliases);

        for field in &mut select_statement.fields {
            self.visit_field_definition_expr(field)?;
        }
        if let Some(where_clause) = &mut select_statement.where_clause {
            self.visit_where_clause(where_clause)?;
        }
        if let Some(having_clause) = &mut select_statement.having {
            self.visit_having_clause(having_clause)?;
        }
        if let Some(group_by_clause) = &mut select_statement.group_by {
            self.visit_group_by_clause(group_by_clause)?;
        }
        if let Some(order_clause) = &mut select_statement.order {
            self.visit_order_clause(order_clause)?;
        }
        self.visit_limit_clause(&mut select_statement.limit_clause)?;

        self.alias_stack.pop();
        Ok(())
    }

    fn visit_create_table_statement(
        &mut self,
        create_table_statement: &'ast mut CreateTableStatement,
    ) -> Result<(), Self::Error> {
        if create_table_statement.table.schema.is_none() {
            // If the table name in the CREATE TABLE statement has no schema, use the first schema
            // in the search path (if it isn't empty)
            if let Some(first_schema) = self.search_path.first() {
                create_table_statement.table.schema = Some(first_schema.clone());
            }
        }
        visit_mut::walk_create_table_statement(self, create_table_statement)
    }

    fn visit_target_table_fk(&mut self, table: &'ast mut Relation) -> Result<(), Self::Error> {
        if table.schema.is_none() {
            // If the target table name in the CREATE TABLE statement has no schema,
            // use the first schema in the search path
            // (if it isn't empty)
            if let Some(first_schema) = self.search_path.first() {
                table.schema = Some(first_schema.clone());
            }
        }
        match self.visit_table(table) {
            Ok(()) => Ok(()),
            Err(ReadySetError::TableNotReplicated { name: _, schema: _ }) => {
                table.schema = Some(SqlIdentifier::from("public"));
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    fn visit_table(&mut self, table: &'ast mut Relation) -> Result<(), Self::Error> {
        if table.schema.is_some() {
            return Ok(());
        }

        if self.table_is_aliased(table) {
            return Ok(());
        }

        self.resolve_schema(table)
    }
}

pub trait ResolveSchemas: Sized {
    /// Attempt to resolve schemas for all non-schema-qualified table references in `self` by
    /// looking up those tables in `tables`, using `search_path` for precedence.
    ///
    /// During resolution, if any schema is "skipped over" when resolving a table, that table will
    /// be added to `invalidating_tables` if provided, to mark that if that table is later created
    /// then this query should be invalidated.
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
        tables: HashMap<&'schema SqlIdentifier, HashMap<&'schema SqlIdentifier, CanQuery>>,
        custom_types: &'schema HashMap<&'schema SqlIdentifier, HashSet<&'schema SqlIdentifier>>,
        search_path: &'schema [SqlIdentifier],
        invalidating_tables: Option<&'schema mut Vec<Relation>>,
    ) -> ReadySetResult<Self>;
}

impl ResolveSchemas for SelectStatement {
    fn resolve_schemas<'schema>(
        mut self,
        tables: HashMap<&'schema SqlIdentifier, HashMap<&'schema SqlIdentifier, CanQuery>>,
        custom_types: &'schema HashMap<&'schema SqlIdentifier, HashSet<&'schema SqlIdentifier>>,
        search_path: &'schema [SqlIdentifier],
        invalidating_tables: Option<&'schema mut Vec<Relation>>,
    ) -> ReadySetResult<Self> {
        ResolveSchemaVisitor {
            tables,
            custom_types,
            search_path,
            alias_stack: Default::default(),
            invalidating_tables,
        }
        .visit_select_statement(&mut self)?;

        Ok(self)
    }
}

impl ResolveSchemas for CreateTableStatement {
    fn resolve_schemas<'schema>(
        mut self,
        tables: HashMap<&'schema SqlIdentifier, HashMap<&'schema SqlIdentifier, CanQuery>>,
        custom_types: &'schema HashMap<&'schema SqlIdentifier, HashSet<&'schema SqlIdentifier>>,
        search_path: &'schema [SqlIdentifier],
        invalidating_tables: Option<&'schema mut Vec<Relation>>,
    ) -> ReadySetResult<Self> {
        ResolveSchemaVisitor {
            tables,
            custom_types,
            search_path,
            alias_stack: Default::default(),
            invalidating_tables,
        }
        .visit_create_table_statement(&mut self)?;

        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use std::fmt::Debug;

    use nom_sql::parse_create_table;
    use readyset_sql::{Dialect, DialectDisplay};

    use super::*;
    use crate::util::parse_select_statement;

    #[track_caller]
    fn rewrites_to<S>(
        input: &str,
        expected: &str,
        parser: impl Fn(&str) -> S,
        result_to_string: impl Fn(&S) -> String,
    ) where
        S: Debug + PartialEq + ResolveSchemas,
    {
        let input = parser(input);
        let expected = parser(expected);
        let result = input
            .resolve_schemas(
                HashMap::from([
                    (
                        &"s1".into(),
                        HashMap::from([
                            (&"t1".into(), CanQuery::Yes),
                            (&"t2".into(), CanQuery::Yes),
                            (&"t_ignored".into(), CanQuery::No),
                        ]),
                    ),
                    (
                        &"s2".into(),
                        HashMap::from([
                            (&"t1".into(), CanQuery::Yes),
                            (&"t2".into(), CanQuery::Yes),
                            (&"t3".into(), CanQuery::Yes),
                        ]),
                    ),
                    (&"s3".into(), HashMap::from([(&"t4".into(), CanQuery::Yes)])),
                ]),
                &HashMap::from([(&"s2".into(), HashSet::from([&"abc".into()]))]),
                &["s1".into(), "s2".into()],
                None,
            )
            .unwrap();

        assert_eq!(
            result,
            expected,
            "\nExpected: {expected}\n     Got: {result}",
            expected = result_to_string(&expected),
            result = result_to_string(&result),
        );
    }

    #[track_caller]
    fn select_rewrites_to(input: &str, expected: &str) {
        rewrites_to(input, expected, parse_select_statement, |result| {
            result.display(Dialect::MySQL).to_string()
        });
    }

    #[test]
    fn rewrites_in_star() {
        select_rewrites_to("select t1.* from t1", "select s1.t1.* from s1.t1")
    }

    #[test]
    fn resolve_table_in_top_of_search_path() {
        select_rewrites_to("select * from t1", "select * from s1.t1");
    }

    #[test]
    fn resolve_table_deeper_in_search_path() {
        select_rewrites_to("select * from t3", "select * from s2.t3");
    }

    #[test]
    fn table_not_in_search_path_is_untouched() {
        select_rewrites_to("select * from t1, t4", "select * from s1.t1, t4");
    }

    #[test]
    fn ignores_cte_alias_reference() {
        select_rewrites_to(
            "with t2 as (select * from t1) select * from t2",
            "with t2 as (select * from s1.t1) select * from t2",
        );
    }

    #[test]
    fn ignores_shadowing_cte_alias() {
        select_rewrites_to(
            "with t2 as (select * from t1) select t2.* from t2",
            "with t2 as (select * from s1.t1) select t2.* from t2",
        );
    }

    #[test]
    fn ignores_table_expr_alias_reference() {
        select_rewrites_to("select t2.* from t1 as t2", "select t2.* from s1.t1 as t2");
    }

    #[test]
    fn subqueries_dont_resolve_down() {
        select_rewrites_to(
            "select t1.* from t1 join (select t1.* from t2 as t1) sq",
            "select s1.t1.* from s1.t1 join (select t1.* from s1.t2 as t1) sq",
        )
    }

    #[test]
    fn select_with_cast_to_custom_type() {
        rewrites_to(
            "select cast(t1.x as abc) from t1",
            "select cast(s1.t1.x as s2.abc) from s1.t1",
            |s| readyset_sql_parsing::parse_select(Dialect::PostgreSQL, s).unwrap(),
            |result| result.display(Dialect::MySQL).to_string(),
        )
    }

    #[test]
    fn select_with_cast_to_custom_type_array() {
        rewrites_to(
            "select cast(t1.x as abc[][]) from t1",
            "select cast(s1.t1.x as s2.abc[][]) from s1.t1",
            |s| readyset_sql_parsing::parse_select(Dialect::PostgreSQL, s).unwrap(),
            |result| result.display(Dialect::MySQL).to_string(),
        )
    }

    #[track_caller]
    fn create_table_rewrites_to(input: &str, expected: &str) {
        rewrites_to(
            input,
            expected,
            |s| parse_create_table(Dialect::MySQL, s).unwrap(),
            |result| result.display(Dialect::MySQL).to_string(),
        );
    }

    #[test]
    fn create_table_unqualified() {
        create_table_rewrites_to(
            "create table new_table (id int primary key)",
            "create table s1.new_table (id int primary key)",
        );
    }

    #[test]
    fn create_table_qualified() {
        create_table_rewrites_to(
            "create table s2.new_table (id int primary key)",
            "create table s2.new_table (id int primary key)",
        );
    }

    #[test]
    fn create_table_foreign_key() {
        create_table_rewrites_to(
            "create table new_table (id int primary key, t2_id int, foreign key (t2_id) references t2 (id))",
            "create table s1.new_table (id int primary key, t2_id int, foreign key (t2_id) references s1.t2 (id))",
        );
    }

    #[test]
    fn create_table_with_custom_type() {
        rewrites_to(
            "create table t (x abc)",
            "create table s1.t (x s2.abc)",
            |s| parse_create_table(Dialect::PostgreSQL, s).unwrap(),
            |result| result.display(Dialect::MySQL).to_string(),
        );
    }

    #[test]
    fn create_table_with_array_of_custom_type() {
        rewrites_to(
            "create table t (x abc[][])",
            "create table s1.t (x s2.abc[][])",
            |s| parse_create_table(Dialect::PostgreSQL, s).unwrap(),
            |result| result.display(Dialect::MySQL).to_string(),
        );
    }

    #[test]
    fn writes_to_invalidating_tables() {
        let input = parse_select_statement("select * from t");
        let mut invalidating_tables = vec![];
        let _result = input
            .resolve_schemas(
                HashMap::from([(&"s2".into(), HashMap::from([(&"t".into(), CanQuery::Yes)]))]),
                &HashMap::new(),
                &["s1".into(), "s2".into()],
                Some(&mut invalidating_tables),
            )
            .unwrap();

        assert_eq!(
            invalidating_tables,
            vec![Relation {
                schema: Some("s1".into()),
                name: "t".into(),
            }]
        );
    }

    #[test]
    fn cant_query_returns_error() {
        let input = parse_select_statement("select * from t");
        let result = input.resolve_schemas(
            HashMap::from([
                (&"s1".into(), HashMap::from([(&"t".into(), CanQuery::No)])),
                (&"s2".into(), HashMap::from([(&"t".into(), CanQuery::Yes)])),
            ]),
            &HashMap::new(),
            &["s1".into(), "s2".into()],
            None,
        );
        let err = result.unwrap_err();
        assert_eq!(
            err,
            ReadySetError::TableNotReplicated {
                name: "t".into(),
                schema: Some("s1".into())
            }
        )
    }

    #[test]
    fn unresolved_cant_query_works() {
        let input = parse_select_statement("select * from t");
        let result = input
            .resolve_schemas(
                HashMap::from([
                    (&"s1".into(), HashMap::from([(&"t".into(), CanQuery::Yes)])),
                    (&"s2".into(), HashMap::from([(&"t".into(), CanQuery::No)])),
                ]),
                &HashMap::new(),
                &["s1".into(), "s2".into()],
                None,
            )
            .unwrap();
        assert_eq!(result, parse_select_statement("select * from s1.t"));
    }

    #[test]
    fn ignores_join_alias_shadowing_ignored_table() {
        select_rewrites_to(
            "SELECT foo.x, t_ignored.y FROM t1 AS foo JOIN t2 AS t_ignored ON foo.id = t_ignored.id",
            "SELECT foo.x, t_ignored.y FROM s1.t1 AS foo JOIN s1.t2 AS t_ignored ON foo.id = t_ignored.id",
        );
    }

    #[test]
    fn resolves_self_shadowing_alias() {
        select_rewrites_to("SELECT * FROM t1 AS t1", "SELECT * FROM s1.t1 AS t1");
    }

    #[test]
    fn resolves_other_shadowing_alias() {
        select_rewrites_to(
            "SELECT t1.id, t3.id FROM t1 AS t3 JOIN t3 AS t1 ON t1.id = t3.id",
            "SELECT t1.id, t3.id FROM s1.t1 AS t3 JOIN s2.t3 AS t1 ON t1.id = t3.id",
        );
    }

    #[test]
    fn resolves_subquery_referencing_outer_aliased_table() {
        select_rewrites_to(
            "SELECT * FROM t1 AS t2, (SELECT t2.id FROM t2) AS foo",
            "SELECT * FROM s1.t1 AS t2, (SELECT s1.t2.id FROM s1.t2) AS foo",
        );
    }
    #[test]

    fn resolves_join_subquery_referencing_outer_aliased_table() {
        select_rewrites_to(
            "SELECT * FROM t1 AS t2 JOIN (SELECT t2.id FROM t2) AS foo",
            "SELECT * FROM s1.t1 AS t2 JOIN (SELECT s1.t2.id FROM s1.t2) AS foo",
        );
    }
}
