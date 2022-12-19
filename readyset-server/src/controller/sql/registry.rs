use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};

use launchpad::hash::hash;
use launchpad::redacted::Sensitive;
use nom_sql::analysis::visit::{self, Visitor};
use nom_sql::{
    CreateTableBody, CreateTableStatement, CreateViewStatement, Relation, SelectSpecification,
    SelectStatement, SqlType,
};
use readyset_errors::{unsupported_err, ReadySetError, ReadySetResult};
use readyset_tracing::debug;
use serde::{Deserialize, Serialize};

use super::QueryID;

/// A single SQL expression stored in a Recipe.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum RecipeExpr {
    /// Expression that represents a `CREATE TABLE` statement.
    Table {
        name: Relation,
        body: CreateTableBody,
    },
    /// Expression that represents a `CREATE VIEW` statement.
    View {
        name: Relation,
        definition: SelectSpecification,
    },
    /// Expression that represents a `CREATE CACHE` statement.
    Cache {
        name: Relation,
        statement: SelectStatement,
        always: bool,
    },
}

impl RecipeExpr {
    /// Returns the name associated with the [`RecipeExpr`].
    pub(crate) fn name(&self) -> &Relation {
        match self {
            RecipeExpr::Table { name, .. }
            | RecipeExpr::View { name, .. }
            | RecipeExpr::Cache { name, .. } => name,
        }
    }

    /// Returns the set of table names being referenced by the [`RecipeExpr`] (for views and
    /// queries).
    /// If the [`RecipeExpr`] is a [`RecipeExpr::Table`], then the set will be empty.
    pub(super) fn table_references(&self) -> HashSet<Relation> {
        match self {
            RecipeExpr::Table { .. } => HashSet::new(),
            RecipeExpr::View { definition, .. } => {
                let mut references = HashSet::new();
                match definition {
                    SelectSpecification::Compound(compound_select) => {
                        references.extend(compound_select.selects.iter().flat_map(
                            |(_, select)| {
                                select
                                    .tables
                                    .iter()
                                    .filter_map(|te| te.inner.as_table().cloned())
                            },
                        ));
                    }
                    SelectSpecification::Simple(select) => {
                        references.extend(
                            select
                                .tables
                                .iter()
                                .filter_map(|te| te.inner.as_table().cloned()),
                        );
                    }
                }
                references
            }
            RecipeExpr::Cache { statement, .. } => {
                let mut references = HashSet::with_capacity(statement.tables.len());
                references.extend(
                    statement
                        .tables
                        .iter()
                        .filter_map(|te| te.inner.as_table().cloned()),
                );
                references
            }
        }
    }

    /// Returns a list of names of custom types referenced by this [`RecipeExpr`]
    pub(super) fn custom_type_references(&self) -> Vec<&Relation> {
        match self {
            RecipeExpr::Table { body, .. } => body
                .fields
                .iter()
                .filter_map(|field| match field.sql_type.innermost_array_type() {
                    SqlType::Other(ty) => Some(ty),
                    _ => None,
                })
                .collect(),
            RecipeExpr::View {
                definition: SelectSpecification::Simple(statement),
                ..
            }
            | RecipeExpr::Cache { statement, .. } => {
                #[derive(Default)]
                struct CollectCustomTypesVisitor<'a>(Vec<&'a Relation>);

                impl<'a> Visitor<'a> for CollectCustomTypesVisitor<'a> {
                    type Error = !;

                    fn visit_sql_type(&mut self, sql_type: &'a SqlType) -> Result<(), Self::Error> {
                        if let SqlType::Other(ty) = sql_type {
                            self.0.push(ty);
                        }

                        visit::walk_sql_type(self, sql_type)
                    }
                }

                let mut visitor = CollectCustomTypesVisitor::default();
                let Ok(()) = visitor.visit_select_statement(statement);
                visitor.0
            }
            _ => vec![], // TODO: compound select statements,
        }
    }

    /// Calculates a SHA-1 hash of the [`RecipeExpr`], to identify it based on its contents.
    pub(super) fn calculate_hash(&self) -> QueryID {
        // NOTE: this has to be the same as `<SelectStatement as RegistryExpr>::query_id`
        use sha1::{Digest, Sha1};
        let mut hasher = Sha1::new();
        match self {
            RecipeExpr::Table { name, body } => {
                hasher.update(hash(name).to_le_bytes());
                hasher.update(hash(body).to_le_bytes());
            }
            RecipeExpr::View { name, definition } => {
                hasher.update(hash(name).to_le_bytes());
                hasher.update(hash(definition).to_le_bytes());
            }
            RecipeExpr::Cache { statement, .. } => hasher.update(hash(statement).to_le_bytes()),
        };
        // Sha1 digest is 20 byte long, so it is safe to consume only 16 bytes
        u128::from_le_bytes(hasher.finalize()[..16].try_into().unwrap())
    }
}

/// The set of all [`RecipeExpr`]s installed in a ReadySet server cluster.
#[derive(Clone, Default, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub(super) struct ExprRegistry {
    /// A map from [`QueryID`] to the [`RecipeExpr`] associated with it.
    #[serde(with = "serde_with::rust::hashmap_as_tuple_list")]
    expressions: HashMap<QueryID, RecipeExpr>,

    /// The set of queries that depend on other queries.
    ///
    /// # Invariants
    ///
    /// - The keys here *must* be valid [`QueryID`]s (aka, present in `expressions`), and their
    ///   associated expression should be of [`RecipeExpr::Table`] variant: Tables don't depend on
    ///   anything, but queries depend on tables.
    /// - The values *must* be valid [`QueryID`]s (aka, present in `expressions`), and their
    ///   associated expression should be of [`RecipeExpr::Cache`] or [`RecipeExpr::View`] variant.
    dependencies: HashMap<QueryID, HashSet<QueryID>>,

    /// Map from names of custom types to the set of Query IDs for tables that have columns with
    /// those types.
    ///
    /// # Invariants
    ///
    /// - The keys *must* be valid custom types (aka, present in `self.inc.custom_types`),
    /// - The values *must* be valid [`QueryID`]s (aka, present in `expressions`), and their
    ///   associated expression should be of [`RecipeExpr::Table`]
    custom_type_dependencies: HashMap<Relation, HashSet<QueryID>>,

    /// Map from names of *nonexistent* tables, to a set of names for queries which should be
    /// invalidated if those tables are ever created
    table_to_invalidated_queries: HashMap<Relation, HashSet<Relation>>,

    /// Aliases assigned to each [`RecipeExpr`] stored.
    ///
    /// # Invariants
    /// - Each `QueryID` present here *must* exist as key in the `expressions` map.
    aliases: HashMap<Relation, QueryID>,
}

impl ExprRegistry {
    /// Adds a [`RecipeExpr`] to the registry.
    /// If the [`RecipeExpr`] was already present, returns `Ok(false)`; otherwise it returns
    /// `Ok(true)`.
    ///
    /// # Errors
    /// A [`ReadySetError::RecipeInvariantViolated`] error is returned if:
    /// - The [`RecipeExpr`] is a [`RecipeExpr::View`] or [`RecipeExpr::CachedQuery`], and
    ///   references a table that is not present in the registry.
    /// - The [`RecipeExpr`] has a name that is already being used by a different [`RecipeExpr`].
    pub(super) fn add_query(&mut self, expression: RecipeExpr) -> ReadySetResult<bool> {
        let query_id = expression.calculate_hash();
        debug!(?expression, %query_id, "Adding expression to the registry");
        // We always try adding the alias first, in case there's another table/query
        // with the same name already.
        self.assign_alias(expression.name().clone(), query_id)?;
        if self.expressions.contains_key(&query_id) {
            return Ok(false);
        }
        for table_reference in expression.table_references() {
            // Get the references from the expression.
            let Some(table_id) = self.aliases.get(&table_reference) else {
                // Queries can reference tables that don't exist in the schema, eg with CTEs or
                // aliased subqueries
                continue
            };
            // Add the dependency.
            self.dependencies
                .entry(*table_id)
                .or_insert_with(|| HashSet::new())
                .insert(query_id);
        }

        for ty in expression.custom_type_references() {
            if let Some(deps) = self.custom_type_dependencies.get_mut(ty) {
                deps.insert(query_id);
            }
        }

        self.expressions.insert(query_id, expression);
        Ok(true)
    }

    /// Retrieves the [`RecipeExpr`] associated with the given name or alias.
    /// If no query is found, returns `None`.
    pub(super) fn get(&self, alias: &Relation) -> Option<&RecipeExpr> {
        let query_id = self.aliases.get(alias)?;
        self.expressions.get(query_id)
    }

    /// Returns true if the given expression exists in `self`
    pub(super) fn contains<E>(&self, expression: &E) -> bool
    where
        E: RegistryExpr,
    {
        self.expressions.contains_key(&expression.query_id())
    }

    /// Retrieves the original name for the query with the given `alias` (which might already be the
    /// original name). Returns `None` is there no [`RecipeExpr`] associated with the
    /// given `alias`.
    pub(super) fn resolve_alias(&self, name_or_alias: &Relation) -> Option<&Relation> {
        self.aliases
            .get(name_or_alias)
            .map(|query_id| self.expressions[query_id].name())
    }

    /// Returns an iterator over all *original names* for all caches in the recipe (not including
    /// aliases)
    pub(super) fn cache_names(&self) -> impl Iterator<Item = &Relation> + '_ {
        self.expressions.values().filter_map(|expr| match expr {
            RecipeExpr::Cache { name, .. } => Some(name),
            _ => None,
        })
    }

    /// Removes the [`RecipeExpr`] associated with the given name (or alias), if
    /// it exists, and all the [`RecipeExpr`]s that depend on it.
    /// Returns the removed [`RecipeExpr`] if it was present, or `None` otherwise.
    pub(super) fn remove_expression(&mut self, name_or_alias: &Relation) -> Option<RecipeExpr> {
        let query_id = *self.aliases.get(name_or_alias)?;
        self.aliases.retain(|_, v| *v != query_id);
        let expression = self.expressions.remove(&query_id)?;
        if !matches!(expression, RecipeExpr::Table { .. }) {
            self.dependencies.iter_mut().for_each(|(_, deps)| {
                deps.remove(&query_id);
            });
        } else if let Some(deps) = self.dependencies.remove(&query_id) {
            for dependency_id in deps.iter() {
                self.aliases.retain(|_, v| *v != *dependency_id);
                self.expressions.remove(dependency_id);
            }
        }
        Some(expression)
    }

    /// Removes the custom type associated with the given name from the registry. Returns `true` if
    /// the type was present, `false` otherwise
    pub(super) fn remove_custom_type(&mut self, name: &Relation) -> bool {
        self.custom_type_dependencies.remove(name).is_some()
    }

    /// Returns the number of [`RecipeExpr`]s being stored in the registry.
    pub(super) fn len(&self) -> usize {
        self.expressions.len()
    }

    /// Returns the number of aliases being stored in the registry.
    pub(super) fn num_aliases(&self) -> usize {
        self.aliases.len()
    }

    /// Record a set of names for tables which, if ever created, should invalidate the query with
    /// the given name.
    ///
    /// Returns an error if a query named `query_name` does not exist
    pub(super) fn insert_invalidating_tables<I>(
        &mut self,
        query_name: Relation,
        invalidating_tables: I,
    ) -> ReadySetResult<()>
    where
        I: IntoIterator<Item = Relation>,
    {
        if !self.aliases.contains_key(&query_name) {
            return Err(ReadySetError::RecipeInvariantViolated(format!(
                "Query {} does not exist",
                query_name,
            )));
        }

        for table in invalidating_tables {
            self.table_to_invalidated_queries
                .entry(table)
                .or_default()
                .insert(query_name.clone());
        }

        Ok(())
    }

    /// Returns an iterator over a list of queries that should be invalidated as a result of a table
    /// with the given name being created.
    pub(super) fn queries_to_invalidate_for_table(
        &self,
        table_name: &Relation,
    ) -> impl Iterator<Item = &Relation> {
        self.table_to_invalidated_queries
            .get(table_name)
            .into_iter()
            .flatten()
    }

    /// Returns an iterator over a list of expressions that contain columns referencing the given
    /// custom type
    pub(super) fn expressions_referencing_custom_type(
        &self,
        custom_type_name: &Relation,
    ) -> impl Iterator<Item = &RecipeExpr> {
        self.custom_type_dependencies
            .get(custom_type_name)
            .into_iter()
            .flatten()
            .map(|dep| self.expressions.get(dep).expect("Documented invariant"))
    }

    fn assign_alias(&mut self, alias: Relation, query_id: QueryID) -> ReadySetResult<()> {
        match self.aliases.entry(alias.clone()) {
            Entry::Occupied(e) => {
                if *e.get() != query_id {
                    return Err(ReadySetError::RecipeInvariantViolated(format!(
                        "Query name exists but existing query is different: {alias}"
                    )));
                }
            }
            Entry::Vacant(e) => {
                debug!(alias = %e.key(), %query_id, "Aliasing existing query");
                e.insert(query_id);
            }
        }
        Ok(())
    }

    pub(crate) fn contains_custom_type(&self, name: &Relation) -> bool {
        self.custom_type_dependencies.contains_key(name)
    }

    pub(crate) fn add_custom_type(&mut self, name: Relation) {
        self.custom_type_dependencies.entry(name).or_default();
    }

    pub(crate) fn rename_custom_type(&mut self, old_name: &Relation, new_name: &Relation) {
        if let Some(prev) = self.custom_type_dependencies.remove(old_name) {
            self.custom_type_dependencies.insert(new_name.clone(), prev);
        }
    }
}

impl TryFrom<CreateTableStatement> for RecipeExpr {
    type Error = ReadySetError;

    fn try_from(stmt: CreateTableStatement) -> Result<Self, Self::Error> {
        Ok(RecipeExpr::Table {
            name: stmt.table,
            body: stmt.body.map_err(|unparsed| {
                unsupported_err!(
                    "CREATE TABLE statement failed to parse: {}",
                    Sensitive(&unparsed)
                )
            })?,
        })
    }
}

impl TryFrom<CreateViewStatement> for RecipeExpr {
    type Error = ReadySetError;

    fn try_from(stmt: CreateViewStatement) -> Result<Self, Self::Error> {
        Ok(RecipeExpr::View {
            name: stmt.name,
            definition: *stmt.definition.map_err(|unparsed| {
                unsupported_err!(
                    "CREATE VIEW statement failed to parse: {}",
                    Sensitive(&unparsed)
                )
            })?,
        })
    }
}

/// Trait to overload different types of expressions that might exist in the [`ExprRegistry`]
pub trait RegistryExpr {
    /// Return a unique ID for this expression
    fn query_id(&self) -> QueryID;
}

impl RegistryExpr for RecipeExpr {
    fn query_id(&self) -> QueryID {
        self.calculate_hash()
    }
}

impl RegistryExpr for SelectStatement {
    fn query_id(&self) -> QueryID {
        // NOTE: this has to be the same as `RecipeExpr::calculate_hash`
        use sha1::{Digest, Sha1};
        let mut hasher = Sha1::new();
        hasher.update(hash(self).to_le_bytes());
        // Sha1 digest is 20 byte long, so it is safe to consume only 16 bytes
        u128::from_le_bytes(hasher.finalize()[..16].try_into().unwrap())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod expression {
        use nom_sql::{parse_create_table, parse_select_statement, Dialect};

        use super::*;

        #[test]
        fn name() {
            let table_name: Relation = "test_table".into();
            let create_table = RecipeExpr::try_from(
                parse_create_table(Dialect::MySQL, "CREATE TABLE test_table (col1 INT);").unwrap(),
            )
            .unwrap();

            assert_eq!(create_table.name(), &table_name);

            let query_name: Relation = "test_query".into();
            let cached_query = RecipeExpr::Cache {
                name: query_name.clone(),
                statement: parse_select_statement(Dialect::MySQL, "SELECT * FROM test_table;")
                    .unwrap(),
                always: false,
            };

            assert_eq!(cached_query.name(), &query_name);

            let view_name: Relation = "test_view".into();
            let view = RecipeExpr::View {
                name: view_name.clone(),
                definition: SelectSpecification::Simple(
                    parse_select_statement(Dialect::MySQL, "SELECT * FROM test_table;").unwrap(),
                ),
            };

            assert_eq!(view.name(), &view_name);
        }

        #[test]
        fn table_references() {
            let table_name = "test_table".into();
            let create_table = RecipeExpr::try_from(
                parse_create_table(Dialect::MySQL, "CREATE TABLE test_table (col1 INT);").unwrap(),
            )
            .unwrap();

            assert!(create_table.table_references().is_empty());

            let cached_query = RecipeExpr::Cache {
                name: "test_query".into(),
                statement: parse_select_statement(Dialect::MySQL, "SELECT * FROM test_table;")
                    .unwrap(),
                always: false,
            };

            let cached_query_table_refs = cached_query.table_references();
            assert_eq!(cached_query_table_refs.len(), 1);
            assert_eq!(cached_query_table_refs.iter().next().unwrap(), &table_name);

            let view = RecipeExpr::View {
                name: "test_view".into(),
                definition: SelectSpecification::Simple(
                    parse_select_statement(Dialect::MySQL, "SELECT * FROM test_table;").unwrap(),
                ),
            };

            let view_table_refs = view.table_references();
            assert_eq!(view_table_refs.len(), 1);
            assert_eq!(view_table_refs.iter().next().unwrap(), &table_name);
        }
    }

    mod registry {
        use nom_sql::{parse_create_table, parse_select_statement, Dialect};

        use super::*;

        fn setup() -> ExprRegistry {
            let mut registry = ExprRegistry::default();

            registry
                .add_query(
                    RecipeExpr::try_from(
                        parse_create_table(Dialect::MySQL, "CREATE TABLE test_table (col1 INT);")
                            .unwrap(),
                    )
                    .unwrap(),
                )
                .unwrap();

            let statement =
                parse_select_statement(Dialect::MySQL, "SELECT * FROM test_table;").unwrap();
            registry
                .add_query(RecipeExpr::Cache {
                    name: "test_query".into(),
                    statement: statement.clone(),
                    always: false,
                })
                .unwrap();
            registry
                .add_query(RecipeExpr::Cache {
                    name: "test_query_alias".into(),
                    statement,
                    always: false,
                })
                .unwrap();

            let statement =
                parse_select_statement(Dialect::MySQL, "SELECT * FROM test_table").unwrap();
            registry
                .add_query(RecipeExpr::Cache {
                    name: "test_query".into(),
                    statement: statement.clone(),
                    always: false,
                })
                .unwrap();
            registry
                .add_query(RecipeExpr::Cache {
                    name: "test_query_alias".into(),
                    statement,
                    always: false,
                })
                .unwrap();

            let view = SelectSpecification::Simple(
                parse_select_statement(Dialect::MySQL, "SELECT DISTINCT * FROM test_table;")
                    .unwrap(),
            );
            registry
                .add_query(RecipeExpr::View {
                    name: "test_view".into(),
                    definition: view.clone(),
                })
                .unwrap();

            registry
                .add_query(RecipeExpr::View {
                    name: "test_view_alias".into(),
                    definition: view,
                })
                .unwrap();

            registry
        }

        #[test]
        fn add_cached_query() {
            let mut registry = setup();

            let expr = RecipeExpr::Cache {
                name: "test_query2".into(),
                statement: parse_select_statement(
                    Dialect::MySQL,
                    "SELECT DISTINCT * FROM test_table;",
                )
                .unwrap(),
                always: false,
            };

            assert!(registry.add_query(expr.clone()).unwrap());

            let result = registry.get(&"test_query2".into()).unwrap();
            assert_eq!(*result, expr);
        }

        #[test]
        fn add_existing_cached_query() {
            let mut registry = setup();

            let expr = RecipeExpr::Cache {
                name: "test_query2".into(),
                statement: parse_select_statement(Dialect::MySQL, "SELECT * FROM test_table;")
                    .unwrap(),
                always: false,
            };
            assert!(!registry.add_query(expr).unwrap());

            let result = registry.get(&"test_query2".into()).unwrap();
            assert_eq!(
                *result,
                RecipeExpr::Cache {
                    name: "test_query".into(),
                    statement: parse_select_statement(Dialect::MySQL, "SELECT * FROM test_table;")
                        .unwrap(),
                    always: false
                }
            );
        }

        #[test]
        fn add_view() {
            let mut registry = setup();
            let view_name: Relation = "test_view2".into();
            let view = RecipeExpr::View {
                name: view_name.clone(),
                definition: SelectSpecification::Simple(
                    parse_select_statement(Dialect::MySQL, "SELECT DISTINCT * FROM test_table;")
                        .unwrap(),
                ),
            };
            let num_expressions = registry.expressions.len();
            let num_dependencies = registry.dependencies.len();
            let num_aliases = registry.aliases.len();
            assert!(registry.add_query(view.clone()).unwrap());
            assert_eq!(registry.expressions.len(), num_expressions + 1);
            assert_eq!(registry.dependencies.len(), num_dependencies);
            assert_eq!(registry.aliases.len(), num_aliases + 1);
            let view_qid = registry.aliases.get(&view_name).unwrap();
            // And the ID stored should be the same as the one in expressions.
            assert_eq!(registry.expressions.get(view_qid).unwrap(), &view);

            let table_dependencies = registry.dependencies.values().next().unwrap();
            assert_eq!(table_dependencies.len(), 4);
            assert!(table_dependencies.contains(view_qid));
        }

        // TODO(fran): The desired behaviour would be that we don't re-add an existing view,
        //  but rather just alias it.
        //  The problem is that we hash the display representation of the internal AST objects
        // (awful),  and that contains the view name, so the hashes are going to be
        // different.  Besides an unnecessary memory overhead, this does not affect
        // functionality.
        #[test]
        #[ignore]
        fn add_existing_view() {
            let mut registry = setup();
            let select =
                parse_select_statement(Dialect::MySQL, "SELECT * FROM test_table;").unwrap();
            let view_name: Relation = "test_view2".into();
            let view = RecipeExpr::View {
                name: view_name.clone(),
                definition: SelectSpecification::Simple(select.clone()),
            };
            let num_expressions = registry.expressions.len();
            let num_dependencies = registry.dependencies.len();
            let num_aliases = registry.aliases.len();
            assert!(!registry.add_query(view).unwrap());
            assert_eq!(registry.expressions.len(), num_expressions);
            assert_eq!(registry.dependencies.len(), num_dependencies);
            assert_eq!(registry.aliases.len(), num_aliases + 1);
            let view_qid = registry.aliases.get(&view_name).unwrap();
            let stored_expression = registry.expressions.get(view_qid).unwrap();
            if let RecipeExpr::View { name, definition } = stored_expression {
                assert_ne!(name, &view_name);
                if let SelectSpecification::Simple(stored_select) = definition {
                    assert_eq!(stored_select.clone(), select);
                } else {
                    panic!("Expected SimpleSelect");
                }
            } else {
                panic!("Expected CachedQuery");
            }
            assert_eq!(
                registry.aliases.get(stored_expression.name()).unwrap(),
                view_qid
            );

            let table_dependencies = registry.dependencies.values().next().unwrap();
            assert_eq!(table_dependencies.len(), 2);
            assert!(table_dependencies.contains(view_qid));
        }

        #[test]
        fn add_table() {
            let mut registry = setup();

            let expr = RecipeExpr::try_from(
                parse_create_table(Dialect::MySQL, "CREATE TABLE test_table2 (col1 INT);").unwrap(),
            )
            .unwrap();
            assert!(registry.add_query(expr.clone()).unwrap());

            let result = registry.get(&"test_table2".into()).unwrap();
            assert_eq!(*result, expr);
        }

        #[test]
        fn add_existing_table() {
            let mut registry = setup();

            assert!(!registry
                .add_query(
                    RecipeExpr::try_from(
                        parse_create_table(Dialect::MySQL, "CREATE TABLE test_table (col1 INT);")
                            .unwrap(),
                    )
                    .unwrap()
                )
                .unwrap());
        }

        #[test]
        fn get_by_name() {
            let registry = setup();
            let query_name = "test_query".into();
            let query = registry.get(&query_name).unwrap();
            assert_eq!(query.name().clone(), query_name);
            let query_qid = registry.aliases.get(&query_name).unwrap();
            assert_eq!(registry.expressions.get(query_qid).unwrap(), query);
        }

        #[test]
        fn get_by_alias() {
            let registry = setup();
            let query_alias = "test_query_alias".into();
            let query = registry.get(&query_alias).unwrap();
            assert_ne!(query.name().clone(), query_alias);
            let query_qid = registry.aliases.get(&query_alias).unwrap();
            assert_eq!(registry.expressions.get(query_qid).unwrap(), query);
            assert_eq!(registry.aliases.get(query.name()).unwrap(), query_qid);
        }

        #[test]
        fn remove_query() {
            let mut registry = setup();

            let expr = registry.remove_expression(&"test_query".into()).unwrap();
            assert_eq!(
                expr,
                RecipeExpr::Cache {
                    name: "test_query".into(),
                    statement: parse_select_statement(Dialect::MySQL, "SELECT * FROM test_table")
                        .unwrap(),
                    always: false
                }
            );
            assert!(registry.get(&"test_query_alias".into()).is_none())
        }

        #[test]
        fn remove_view() {
            let mut registry = setup();
            registry.remove_expression(&"test_view".into()).unwrap();
            assert!(registry.get(&"test_view".into()).is_none());
        }

        #[test]
        fn remove_table() {
            let mut registry = setup();
            let name: Relation = "test_table".into();
            let expression = registry.get(&name).unwrap().clone();
            let removed_expression = registry.remove_expression(&name).unwrap();
            assert_eq!(removed_expression, expression);
            assert!(registry.expressions.is_empty());
            assert!(registry.dependencies.is_empty());
            assert!(registry.aliases.is_empty());
        }

        #[test]
        fn len() {
            let registry = setup();
            assert_eq!(registry.len(), registry.expressions.len());
        }

        #[test]
        fn aliases_count() {
            let registry = setup();
            assert_eq!(registry.num_aliases(), registry.aliases.len());
        }

        #[test]
        fn contains() {
            let mut registry = setup();
            let stmt = parse_select_statement(Dialect::MySQL, "SELECT * FROM test_table").unwrap();
            registry
                .add_query(RecipeExpr::Cache {
                    name: "test".into(),
                    statement: stmt.clone(),
                    always: false,
                })
                .unwrap();
            assert!(registry.contains(&stmt))
        }

        #[test]
        fn insert_invalidating_tables() {
            let mut registry = setup();
            registry
                .insert_invalidating_tables(
                    "test_query".into(),
                    [
                        Relation {
                            schema: Some("s1".into()),
                            name: "t1".into(),
                        },
                        Relation {
                            schema: Some("s2".into()),
                            name: "t2".into(),
                        },
                    ],
                )
                .unwrap();

            registry
                .add_query(RecipeExpr::Cache {
                    name: "test_query2".into(),
                    statement: parse_select_statement(Dialect::MySQL, "SELECT * FROM test_table")
                        .unwrap(),
                    always: false,
                })
                .unwrap();

            registry
                .insert_invalidating_tables(
                    "test_query2".into(),
                    [Relation {
                        schema: Some("s1".into()),
                        name: "t1".into(),
                    }],
                )
                .unwrap();

            let result = registry
                .queries_to_invalidate_for_table(&Relation {
                    schema: Some("s1".into()),
                    name: "t1".into(),
                })
                .collect::<HashSet<_>>();

            assert_eq!(
                result,
                [
                    &Relation::from("test_query"),
                    &Relation::from("test_query2")
                ]
                .into()
            );
        }

        #[test]
        fn insert_invalidating_tables_with_nonexistent_query() {
            let mut registry = setup();
            registry
                .insert_invalidating_tables("nonexistent_query".into(), [])
                .unwrap_err();
        }

        #[test]
        fn insert_custom_type_and_table() {
            let mut registry = setup();

            let ty = Relation {
                schema: Some("public".into()),
                name: "abc".into(),
            };
            registry.add_custom_type(ty.clone());

            let table = parse_create_table(
                Dialect::PostgreSQL,
                "CREATE TABLE public.t (x public.abc, y int)",
            )
            .unwrap();
            assert!(registry
                .add_query(RecipeExpr::try_from(table.clone()).unwrap())
                .unwrap());

            assert_eq!(
                registry
                    .expressions_referencing_custom_type(&ty)
                    .map(|expr| match expr {
                        RecipeExpr::Table { body, .. } => body,
                        _ => panic!(),
                    })
                    .collect::<Vec<_>>(),
                vec![table.body.as_ref().unwrap()]
            )
        }

        #[test]
        fn query_referencing_custom_type() {
            let mut registry = setup();

            let ty = Relation {
                schema: Some("public".into()),
                name: "abc".into(),
            };
            registry.add_custom_type(ty.clone());

            let query =
                parse_select_statement(Dialect::PostgreSQL, "SELECT CAST(x AS public.abc) FROM t")
                    .unwrap();
            assert!(registry
                .add_query(RecipeExpr::Cache {
                    name: "foo".into(),
                    statement: query.clone(),
                    always: false
                })
                .unwrap());

            assert_eq!(
                registry
                    .expressions_referencing_custom_type(&ty)
                    .map(|expr| match expr {
                        RecipeExpr::Cache {
                            name, statement, ..
                        } => (name, statement),
                        _ => panic!(),
                    })
                    .collect::<Vec<_>>(),
                vec![(&"foo".into(), &query)]
            );
        }
    }
}
