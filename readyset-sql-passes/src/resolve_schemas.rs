//! Rewrite pass to resolve schemas for table references.
//!
//! See [`ResolveSchemas::resolve_schemas`] for more information.

use crate::CanQuery;
use itertools::Either;
use readyset_errors::{ReadySetError, ReadySetResult};
use readyset_sql::analysis::visit_mut::{self, VisitorMut};
use readyset_sql::ast::{
    CreateTableStatement, JoinRightSide, Relation, SelectStatement, SqlIdentifier, SqlType,
    TableExpr, TableExprInner,
};
use std::collections::{HashMap, HashSet};
use std::iter;

pub trait ResolveSchemasContext {
    /// Possibly add to a list of tables which, if created, should invalidate this query.
    ///
    /// This is (optionally) inserted into during rewriting of certain queries when the
    /// [`crate::resolve_schemas`] attempts to resolve a table within a schema but is unable to.
    ///
    /// Note that this takes `&self`, effectively requiring interior mutability, (for now) because
    /// its implementors are effectively partially borrowed (for now).
    fn add_invalidating_table(&self, table: Relation);

    /// Look up a table in a given schema, returning whether it can be queried.
    // tables: HashMap<&'schema SqlIdentifier, HashMap<&'schema SqlIdentifier, CanQuery>>,
    fn can_query_table(&self, schema: &SqlIdentifier, table: &SqlIdentifier) -> Option<CanQuery>;
}

impl<R: ResolveSchemasContext> ResolveSchemasContext for &R {
    fn add_invalidating_table(&self, table: Relation) {
        (*self).add_invalidating_table(table);
    }

    fn can_query_table(&self, schema: &SqlIdentifier, table: &SqlIdentifier) -> Option<CanQuery> {
        (*self).can_query_table(schema, table)
    }
}

struct ResolveSchemaVisitor<'schema, R: ResolveSchemasContext> {
    context: R,

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
}

impl<R> ResolveSchemaVisitor<'_, R>
where
    R: ResolveSchemasContext,
{
    fn table_is_aliased(&self, table: &Relation) -> bool {
        self.alias_stack
            .iter()
            .any(|frame| frame.contains(&table.name))
    }

    fn resolve_schema(&mut self, table: &mut Relation) -> Result<(), ReadySetError> {
        for schema in self.search_path {
            match self.context.can_query_table(schema, &table.name) {
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
                    self.context.add_invalidating_table(Relation {
                        schema: Some(schema.clone()),
                        name: table.name.clone(),
                    });
                }
            };
        }
        Ok(())
    }

    /// Visit FROM items defined via iterator, with the preceding `from_aliases` available only
    /// for `lateral` subqueries, accumulating `from_aliases` as we are iterating over the items.
    fn visit_from_items_with_from_aliases<'a>(
        &mut self,
        it: impl Iterator<Item = &'a mut TableExpr>,
        mut from_aliases: HashSet<SqlIdentifier>,
    ) -> Result<HashSet<SqlIdentifier>, ReadySetError> {
        for from_item in it {
            if matches!(from_item, TableExpr { inner : TableExprInner::Subquery(sq), .. } if sq.as_ref().lateral)
            {
                self.alias_stack.push(from_aliases);
                self.visit_table_expr(from_item)?;
                from_aliases = self
                    .alias_stack
                    .pop()
                    .ok_or_else(Self::stack_underflow_error)?;
            } else {
                self.visit_table_expr(from_item)?;
            }
            if let Some(alias) = &from_item.alias {
                from_aliases.insert(alias.clone());
            }
        }
        Ok(from_aliases)
    }

    fn stack_underflow_error() -> ReadySetError {
        ReadySetError::Internal("Stack underflow".to_string())
    }
}

impl<'ast, R: ResolveSchemasContext> VisitorMut<'ast> for ResolveSchemaVisitor<'_, R> {
    type Error = ReadySetError;

    fn visit_sql_type(&mut self, sql_type: &'ast mut SqlType) -> Result<(), Self::Error> {
        if let SqlType::Other(ty) = sql_type
            && ty.schema.is_none()
            && let Some(schema) = self.search_path.iter().find(|schema| {
                self.custom_types
                    .get(schema)
                    .into_iter()
                    .any(|tys| tys.contains(&ty.name))
            })
        {
            ty.schema = Some(schema.clone());
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

        // The `from_aliases` set will accumulate aliases on `FROM` items
        // (joined tables or subqueries).
        //
        // They should be available:
        // - In the field list
        // - In `WHERE` conditions
        // - In subqueries within those clauses
        // - Inside `LATERAL` joined subqueries (in left-to-right order)
        // - In all top-level `JOIN ON` conditions
        //
        // But they should NOT be available:
        // - Inside non-`LATERAL` joined subqueries
        // - In CTES (already handled above)
        //
        // To accomplish this, `visit_from_items_with_from_aliases` processes
        // all `FROM` items left-to-right, and conditionally adds this set of
        // aliases to the alias stack for `LATERAL` subqueries but not for other
        // types of `FROM` items. Each `FROM` item with an alias also adds that
        // alias to `from_aliases`. After processing all `FROM` items, these
        // aliases are subsequently available everywhere for the remainder of
        // this visitation.
        let mut from_aliases: HashSet<SqlIdentifier> = HashSet::new();

        from_aliases = self
            .visit_from_items_with_from_aliases(select_statement.tables.iter_mut(), from_aliases)?;

        for join in &mut select_statement.join {
            from_aliases = self.visit_from_items_with_from_aliases(
                match &mut join.right {
                    JoinRightSide::Table(table) => Either::Left(iter::once(table)),
                    JoinRightSide::Tables(tables) => Either::Right(tables.iter_mut()),
                },
                from_aliases,
            )?;
            self.alias_stack.push(from_aliases);
            self.visit_join_constraint(&mut join.constraint)?;
            from_aliases = self
                .alias_stack
                .pop()
                .ok_or_else(Self::stack_underflow_error)?;
        }

        // Now the lateral aliases are available to subqueries
        self.alias_stack.last_mut().unwrap().extend(from_aliases);

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
    fn resolve_schemas<'schema, R: ResolveSchemasContext>(
        &mut self,
        context: R,
        custom_types: &'schema HashMap<&'schema SqlIdentifier, HashSet<&'schema SqlIdentifier>>,
        search_path: &'schema [SqlIdentifier],
    ) -> ReadySetResult<&mut Self>;
}

impl ResolveSchemas for SelectStatement {
    fn resolve_schemas<'schema, R: ResolveSchemasContext>(
        &mut self,
        context: R,
        custom_types: &'schema HashMap<&'schema SqlIdentifier, HashSet<&'schema SqlIdentifier>>,
        search_path: &'schema [SqlIdentifier],
    ) -> ReadySetResult<&mut Self> {
        ResolveSchemaVisitor {
            context,
            custom_types,
            search_path,
            alias_stack: Default::default(),
        }
        .visit_select_statement(self)?;
        Ok(self)
    }
}

impl ResolveSchemas for CreateTableStatement {
    fn resolve_schemas<'schema, R: ResolveSchemasContext>(
        &mut self,
        context: R,
        custom_types: &'schema HashMap<&'schema SqlIdentifier, HashSet<&'schema SqlIdentifier>>,
        search_path: &'schema [SqlIdentifier],
    ) -> ReadySetResult<&mut Self> {
        ResolveSchemaVisitor {
            context,
            custom_types,
            search_path,
            alias_stack: Default::default(),
        }
        .visit_create_table_statement(self)?;

        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use std::{cell::RefCell, fmt::Debug};

    use readyset_sql::{Dialect, DialectDisplay};
    use readyset_sql_parsing::parse_create_table;

    use super::*;
    use crate::util::parse_select_statement;

    struct TestRewriteContext {
        tables: HashMap<SqlIdentifier, HashMap<SqlIdentifier, CanQuery>>,
        invalidating_tables: RefCell<Vec<Relation>>,
    }

    impl TestRewriteContext {
        fn new(tables: HashMap<SqlIdentifier, HashMap<SqlIdentifier, CanQuery>>) -> Self {
            Self {
                tables,
                invalidating_tables: RefCell::new(Vec::new()),
            }
        }
    }

    impl ResolveSchemasContext for TestRewriteContext {
        fn add_invalidating_table(&self, table: Relation) {
            self.invalidating_tables.borrow_mut().push(table);
        }

        fn can_query_table(
            &self,
            schema: &SqlIdentifier,
            table: &SqlIdentifier,
        ) -> Option<CanQuery> {
            self.tables
                .get(schema)
                .and_then(|tables| tables.get(table).copied())
        }
    }

    #[track_caller]
    fn rewrites_to<S>(
        input: &str,
        expected: &str,
        parser: impl Fn(&str) -> S,
        result_to_string: impl Fn(&S) -> String,
    ) where
        S: Debug + PartialEq + ResolveSchemas,
    {
        let mut q = parser(input);
        let expected = parser(expected);
        q.resolve_schemas(
            TestRewriteContext::new(HashMap::from([
                (
                    "s1".into(),
                    HashMap::from([
                        ("t1".into(), CanQuery::Yes),
                        ("t2".into(), CanQuery::Yes),
                        ("t_ignored".into(), CanQuery::No),
                    ]),
                ),
                (
                    "s2".into(),
                    HashMap::from([
                        ("t1".into(), CanQuery::Yes),
                        ("t2".into(), CanQuery::Yes),
                        ("t3".into(), CanQuery::Yes),
                    ]),
                ),
                ("s3".into(), HashMap::from([("t4".into(), CanQuery::Yes)])),
            ])),
            &HashMap::from([(&"s2".into(), HashSet::from([&"abc".into()]))]),
            &["s1".into(), "s2".into()],
        )
        .unwrap();

        assert_eq!(
            q,
            expected,
            "\nExpected: {expected}\n     Got: {result}",
            expected = result_to_string(&expected),
            result = result_to_string(&q),
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
        let mut q = parse_select_statement("select * from t");
        let context = TestRewriteContext::new(HashMap::from([(
            "s2".into(),
            HashMap::from([("t".into(), CanQuery::Yes)]),
        )]));
        q.resolve_schemas(&context, &HashMap::new(), &["s1".into(), "s2".into()])
            .unwrap();

        assert_eq!(
            context.invalidating_tables.into_inner(),
            vec![Relation {
                schema: Some("s1".into()),
                name: "t".into(),
            }]
        );
    }

    #[test]
    fn cant_query_returns_error() {
        let mut q = parse_select_statement("select * from t");
        let result = q.resolve_schemas(
            TestRewriteContext::new(HashMap::from([
                ("s1".into(), HashMap::from([("t".into(), CanQuery::No)])),
                ("s2".into(), HashMap::from([("t".into(), CanQuery::Yes)])),
            ])),
            &HashMap::new(),
            &["s1".into(), "s2".into()],
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
        let mut q = parse_select_statement("select * from t");
        q.resolve_schemas(
            TestRewriteContext::new(HashMap::from([
                ("s1".into(), HashMap::from([("t".into(), CanQuery::Yes)])),
                ("s2".into(), HashMap::from([("t".into(), CanQuery::No)])),
            ])),
            &HashMap::new(),
            &["s1".into(), "s2".into()],
        )
        .unwrap();
        assert_eq!(q, parse_select_statement("select * from s1.t"));
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

    #[test]
    fn resolves_lateral_subquery_referencing_preceding_lateral_subquery() {
        select_rewrites_to(
            "SELECT * from t1 AS foo,
                LATERAL (SELECT * FROM t1 WHERE foo.id = t1.id) AS t1,
                LATERAL (SELECT * FROM t1 WHERE t1.id = s2.t1.id) AS bar",
            "SELECT * from s1.t1 AS foo,
                LATERAL (SELECT * FROM s1.t1 WHERE foo.id = s1.t1.id) AS t1,
                LATERAL (SELECT * FROM t1 WHERE t1.id = s2.t1.id) AS bar",
        );
    }

    #[test]
    fn does_not_resolve_nonlateral_subquery_referencing_preceding_lateral_subquery() {
        select_rewrites_to(
            "SELECT * from t1 AS foo,
                LATERAL (SELECT * FROM t1 WHERE foo.id = t1.id) AS t1,
                (SELECT * FROM t1 WHERE t1.id = s2.t1.id) AS bar",
            "SELECT * from s1.t1 AS foo,
                LATERAL (SELECT * FROM s1.t1 WHERE foo.id = s1.t1.id) AS t1,
                (SELECT * FROM s1.t1 WHERE s1.t1.id = s2.t1.id) AS bar",
        );
    }

    #[test]
    fn does_not_resolve_join_condition_referencing_subsequent_join() {
        select_rewrites_to(
            "SELECT t1.id, t2.id, foo.id, bar.id FROM t1 AS foo
                JOIN t2 AS bar ON bar.id = t1.id AND bar.id = foo.id
                JOIN (SELECT * FROM t1 WHERE foo.id = t1.id) AS t1
                WHERE bar.id = t1.id AND t1.id = foo.id",
            "SELECT t1.id, s1.t2.id, foo.id, bar.id FROM s1.t1 AS foo
                JOIN s1.t2 AS bar ON bar.id = s1.t1.id AND bar.id = foo.id
                JOIN (SELECT * FROM s1.t1 WHERE foo.id = s1.t1.id) AS t1
                WHERE bar.id = t1.id AND t1.id = foo.id",
        );
    }
}
