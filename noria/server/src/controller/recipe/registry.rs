use std::borrow::Borrow;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::convert::TryInto;

use nom_sql::{
    CreateTableStatement, CreateViewStatement, SelectSpecification, SelectStatement, SqlIdentifier,
};
use noria_errors::{ReadySetError, ReadySetResult};
use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::controller::recipe::QueryID;

/// A single SQL expression stored in a Recipe.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) enum RecipeExpression {
    /// Expression that represents a `CREATE TABLE` statement.
    Table(CreateTableStatement),
    /// Expression that represents a `CREATE VIEW` statement.
    View(CreateViewStatement),
    /// Expression that represents a `CREATE CACHE` statement.
    Cache {
        name: SqlIdentifier,
        statement: SelectStatement,
    },
}

impl RecipeExpression {
    /// Returns the name associated with the [`RecipeExpression`].
    pub(crate) fn name(&self) -> &SqlIdentifier {
        match self {
            RecipeExpression::Table(stmt) => &stmt.table.name,
            RecipeExpression::View(cvs) => &cvs.name,
            RecipeExpression::Cache { name, .. } => name,
        }
    }

    /// Returns the set of table names being referenced by the [`RecipeExpression`] (for views and
    /// queries).
    /// If the [`RecipeExpression`] is a [`RecipeExpression::Table`], then the set will be empty.
    pub(super) fn table_references(&self) -> HashSet<SqlIdentifier> {
        match self {
            RecipeExpression::Table(_) => HashSet::new(),
            RecipeExpression::View(cvs) => {
                let mut references = HashSet::new();
                let select = cvs.definition.borrow();
                match select {
                    SelectSpecification::Compound(compound_select) => {
                        references.extend(compound_select.selects.iter().flat_map(
                            |(_, select)| select.tables.iter().map(|table| table.name.clone()),
                        ));
                    }
                    SelectSpecification::Simple(select) => {
                        references.extend(select.tables.iter().map(|table| table.name.clone()));
                    }
                }
                references
            }
            RecipeExpression::Cache { statement, .. } => {
                let mut references = HashSet::with_capacity(statement.tables.len());
                references.extend(statement.tables.iter().map(|t| t.name.clone()));
                references
            }
        }
    }

    /// Calculates a SHA-1 hash of the [`RecipeExpression`], to identify it based on its contents.
    pub(super) fn calculate_hash(&self) -> QueryID {
        use sha1::{Digest, Sha1};
        let mut hasher = Sha1::new();
        let query_string = match self {
            RecipeExpression::Table(cts) => cts.to_string(),
            RecipeExpression::View(cvs) => cvs.to_string(),
            RecipeExpression::Cache { statement, .. } => statement.to_string(),
        };
        hasher.update(query_string.as_bytes());
        // Sha1 digest is 20 byte long, so it is safe to consume only 16 bytes
        u128::from_le_bytes(hasher.finalize()[..16].try_into().unwrap())
    }
}

/// The set of all [`RecipeExpression`]s installed in a Noria server cluster.
#[derive(Clone, Default, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub(super) struct ExpressionRegistry {
    /// A map from [`QueryID`] to the [`RecipeExpression`] associated with it.
    #[serde(with = "serde_with::rust::hashmap_as_tuple_list")]
    expressions: HashMap<QueryID, RecipeExpression>,
    /// The set of queries that depend on other queries.
    ///
    /// # Invariants
    /// - The keys here *must* be valid [`QueryID`]s (aka, present in `expressions`), and their
    ///   associated expression should be of [`RecipeExpression::Table`] variant: Tables don't
    ///   depend on anything, but queries depend on tables.
    /// - The values *must* be valid [`QueryID`]s (aka, present in `expressions`), and their
    ///   associated expression should be of [`RecipeExpression::Cache`] or
    ///   [`RecipeExpression::View`] variant.
    dependencies: HashMap<QueryID, HashSet<QueryID>>,
    /// Aliases assigned to each [`RecipeExpression`] stored.
    ///
    /// # Invariants
    /// - Each `QueryID` present here *must* exist as key in the `expressions` map.
    aliases: HashMap<SqlIdentifier, QueryID>,
}

impl ExpressionRegistry {
    /// Creates a new, empty [`ExpressionRegistry`].
    pub(super) fn new() -> Self {
        Self {
            expressions: HashMap::new(),
            dependencies: HashMap::new(),
            aliases: HashMap::new(),
        }
    }

    /// Adds a [`RecipeExpression`] to the registry.
    /// If the [`RecipeExpression`] was already present, returns `Ok(false)`; otherwise it returns
    /// `Ok(true)`.
    ///
    /// # Errors
    /// A [`ReadySetError::RecipeInvariantViolated`] error is returned if:
    /// - The [`RecipeExpression`] is a [`RecipeExpression::View`] or
    ///   [`RecipeExpression::CachedQuery`], and references a table that is not present in the
    ///   registry.
    /// - The [`RecipeExpression`] has a name that is already being used by a different
    ///   [`RecipeExpression`].
    pub(super) fn add_query(&mut self, expression: RecipeExpression) -> ReadySetResult<bool> {
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
            let table_id = match self.aliases.get(&table_reference) {
                None => {
                    return Err(ReadySetError::RecipeInvariantViolated(format!(
                        "Referenced table {} does not exist",
                        table_reference
                    )))
                }
                Some(tid) => tid,
            };
            // Add the dependency.
            self.dependencies
                .entry(*table_id)
                .or_insert_with(|| HashSet::new())
                .insert(query_id);
        }
        self.expressions.insert(query_id, expression);
        Ok(true)
    }

    /// Retrieves the [`RecipeExpression`] associated with the given name or alias.
    /// If no query is found, returns `None`.
    pub(super) fn get(&self, alias: &SqlIdentifier) -> Option<&RecipeExpression> {
        let query_id = self.aliases.get(alias)?;
        self.expressions.get(query_id)
    }

    /// Retrieves the original name for the query with the given `alias` (which might already be the
    /// original name). Returns `None` is there no [`RecipeExpression`] associated with the
    /// given `alias`.
    pub(super) fn resolve_alias(&self, name_or_alias: &SqlIdentifier) -> Option<&SqlIdentifier> {
        self.aliases
            .get(name_or_alias)
            .map(|query_id| self.expressions[query_id].name())
    }

    /// Removes the [`RecipeExpression`] associated with the given name (or alias), if
    /// it exists, and all the [`RecipeExpression`]s that depend on it.
    /// Returns the removed [`RecipeExpression`] if it was present, or `None` otherwise.
    pub(super) fn remove_expression(
        &mut self,
        name_or_alias: &SqlIdentifier,
    ) -> Option<RecipeExpression> {
        let query_id = *self.aliases.get(name_or_alias)?;
        self.aliases.retain(|_, v| *v != query_id);
        let expression = self.expressions.remove(&query_id)?;
        if !matches!(expression, RecipeExpression::Table(_)) {
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

    /// Returns the number of [`RecipeExpression`]s being stored in the registry.
    pub(super) fn len(&self) -> usize {
        self.expressions.len()
    }

    /// Returns the number of aliases being stored in the registry.
    pub(super) fn num_aliases(&self) -> usize {
        self.aliases.len()
    }

    fn assign_alias(&mut self, alias: SqlIdentifier, query_id: QueryID) -> ReadySetResult<()> {
        debug!(%alias, %query_id, "Aliasing existing query");
        match self.aliases.entry(alias.clone()) {
            Entry::Occupied(e) => {
                if *e.get() != query_id {
                    return Err(ReadySetError::RecipeInvariantViolated(format!(
                        "Query name exists but existing query is different: {alias}"
                    )));
                }
            }
            Entry::Vacant(e) => {
                e.insert(query_id);
            }
        }
        Ok(())
    }
}

impl From<CreateTableStatement> for RecipeExpression {
    fn from(cts: CreateTableStatement) -> Self {
        RecipeExpression::Table(cts)
    }
}

impl From<CreateViewStatement> for RecipeExpression {
    fn from(cvs: CreateViewStatement) -> Self {
        RecipeExpression::View(cvs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod expression {
        use nom_sql::Dialect;

        use super::*;

        #[test]
        fn name() {
            let table_name: SqlIdentifier = "test_table".into();
            let create_table = RecipeExpression::Table(
                nom_sql::parse_create_table(Dialect::MySQL, "CREATE TABLE test_table (col1 INT);")
                    .unwrap(),
            );

            assert_eq!(create_table.name(), &table_name);

            let query_name: SqlIdentifier = "test_query".into();
            let cached_query = RecipeExpression::Cache {
                name: query_name.clone(),
                statement: nom_sql::parse_select_statement(
                    Dialect::MySQL,
                    "SELECT * FROM test_table;",
                )
                .unwrap(),
            };

            assert_eq!(cached_query.name(), &query_name);

            let view_name: SqlIdentifier = "test_view".into();
            let view = RecipeExpression::View(CreateViewStatement {
                name: view_name.clone(),
                fields: vec![],
                definition: Box::new(SelectSpecification::Simple(
                    nom_sql::parse_select_statement(Dialect::MySQL, "SELECT * FROM test_table;")
                        .unwrap(),
                )),
            });

            assert_eq!(view.name(), &view_name);
        }

        #[test]
        fn table_references() {
            let table_name: SqlIdentifier = "test_table".into();
            let create_table = RecipeExpression::Table(
                nom_sql::parse_create_table(Dialect::MySQL, "CREATE TABLE test_table (col1 INT);")
                    .unwrap(),
            );

            assert!(create_table.table_references().is_empty());

            let cached_query = RecipeExpression::Cache {
                name: "test_query".into(),
                statement: nom_sql::parse_select_statement(
                    Dialect::MySQL,
                    "SELECT * FROM test_table;",
                )
                .unwrap(),
            };

            let cached_query_table_refs = cached_query.table_references();
            assert_eq!(cached_query_table_refs.len(), 1);
            assert_eq!(cached_query_table_refs.iter().next().unwrap(), &table_name);

            let view = RecipeExpression::View(CreateViewStatement {
                name: "test_view".into(),
                fields: vec![],
                definition: Box::new(SelectSpecification::Simple(
                    nom_sql::parse_select_statement(Dialect::MySQL, "SELECT * FROM test_table;")
                        .unwrap(),
                )),
            });

            let view_table_refs = view.table_references();
            assert_eq!(view_table_refs.len(), 1);
            assert_eq!(view_table_refs.iter().next().unwrap(), &table_name);
        }
    }

    mod registry {
        use nom_sql::Dialect;

        use super::*;

        fn create_registry() -> ExpressionRegistry {
            // We *manually* build an `ExpressionRegistry` with a valid
            // state, to avoid creating it using the methods that we intend
            // to test.
            let mut expressions = HashMap::new();
            let mut dependencies = HashMap::new();
            let mut aliases = HashMap::new();

            let create_table = RecipeExpression::Table(
                nom_sql::parse_create_table(Dialect::MySQL, "CREATE TABLE test_table (col1 INT);")
                    .unwrap(),
            );
            let table_qid = create_table.calculate_hash();
            expressions.insert(table_qid, create_table);
            aliases.insert("test_table".into(), table_qid);

            let query_name: SqlIdentifier = "test_query".into();
            let cached_query = RecipeExpression::Cache {
                name: query_name.clone(),
                statement: nom_sql::parse_select_statement(
                    Dialect::MySQL,
                    "SELECT * FROM test_table;",
                )
                .unwrap(),
            };
            let query_qid = cached_query.calculate_hash();
            expressions.insert(query_qid, cached_query);
            aliases.insert(query_name, query_qid);
            aliases.insert("test_query_alias".into(), query_qid);

            let view_name: SqlIdentifier = "test_view".into();
            let view = RecipeExpression::View(CreateViewStatement {
                name: view_name.clone(),
                fields: vec![],
                definition: Box::new(SelectSpecification::Simple(
                    nom_sql::parse_select_statement(
                        Dialect::MySQL,
                        "SELECT DISTINCT * FROM test_table;",
                    )
                    .unwrap(),
                )),
            });
            let view_qid = view.calculate_hash();
            expressions.insert(view_qid, view);
            aliases.insert(view_name, view_qid);
            aliases.insert("test_view_alias".into(), view_qid);

            let table_dependencies = dependencies
                .entry(table_qid)
                .or_insert_with(|| HashSet::new());
            table_dependencies.insert(query_qid);
            table_dependencies.insert(view_qid);
            ExpressionRegistry {
                expressions,
                dependencies,
                aliases,
            }
        }

        #[test]
        fn new() {
            let registry = ExpressionRegistry::new();
            assert!(registry.expressions.is_empty());
            assert!(registry.dependencies.is_empty());
            assert!(registry.aliases.is_empty());
        }

        #[test]
        fn add_cached_query() {
            let mut registry = create_registry();
            let query_name: SqlIdentifier = "test_query2".into();
            let cached_query = RecipeExpression::Cache {
                name: query_name.clone(),
                statement: nom_sql::parse_select_statement(
                    Dialect::MySQL,
                    "SELECT DISTINCT * FROM test_table;",
                )
                .unwrap(),
            };
            let num_expressions = registry.expressions.len();
            let num_dependencies = registry.dependencies.len();
            let num_aliases = registry.aliases.len();
            assert!(registry.add_query(cached_query.clone()).unwrap());
            assert_eq!(registry.expressions.len(), num_expressions + 1);
            assert_eq!(registry.dependencies.len(), num_dependencies);
            assert_eq!(registry.aliases.len(), num_aliases + 1);
            let query_qid = registry.aliases.get(&query_name).unwrap();
            // And the ID stored should be the same as the one in expressions.
            assert_eq!(registry.expressions.get(query_qid).unwrap(), &cached_query);

            let table_dependencies = registry.dependencies.values().next().unwrap();
            assert_eq!(table_dependencies.len(), 3);
            assert!(table_dependencies.contains(query_qid));
        }

        #[test]
        fn add_existing_cached_query() {
            let mut registry = create_registry();
            let query_name: SqlIdentifier = "test_query2".into();
            let select =
                nom_sql::parse_select_statement(Dialect::MySQL, "SELECT * FROM test_table;")
                    .unwrap();
            let cached_query = RecipeExpression::Cache {
                name: query_name.clone(),
                statement: select.clone(),
            };
            let num_expressions = registry.expressions.len();
            let num_dependencies = registry.dependencies.len();
            let num_aliases = registry.aliases.len();
            assert!(!registry.add_query(cached_query.clone()).unwrap());
            assert_eq!(registry.expressions.len(), num_expressions);
            assert_eq!(registry.dependencies.len(), num_dependencies);
            assert_eq!(registry.aliases.len(), num_aliases + 1);
            let query_qid = registry.aliases.get(&query_name).unwrap();
            let stored_expression = registry.expressions.get(query_qid).unwrap();
            if let RecipeExpression::Cache { name, statement } = stored_expression {
                assert_ne!(name.clone(), query_name);
                assert_eq!(statement.clone(), select);
            } else {
                panic!("Expected CachedQuery");
            }
            assert_eq!(
                registry.aliases.get(stored_expression.name()).unwrap(),
                query_qid
            );

            let table_dependencies = registry.dependencies.values().next().unwrap();
            assert_eq!(table_dependencies.len(), 2);
            assert!(table_dependencies.contains(query_qid));
        }

        #[test]
        fn add_view() {
            let mut registry = create_registry();
            let view_name: SqlIdentifier = "test_view2".into();
            let view = RecipeExpression::View(CreateViewStatement {
                name: view_name.clone(),
                fields: vec![],
                definition: Box::new(SelectSpecification::Simple(
                    nom_sql::parse_select_statement(
                        Dialect::MySQL,
                        "SELECT DISTINCT * FROM test_table;",
                    )
                    .unwrap(),
                )),
            });
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
            assert_eq!(table_dependencies.len(), 3);
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
            let mut registry = create_registry();
            let select =
                nom_sql::parse_select_statement(Dialect::MySQL, "SELECT * FROM test_table;")
                    .unwrap();
            let view_name: SqlIdentifier = "test_view2".into();
            let view = RecipeExpression::View(CreateViewStatement {
                name: view_name.clone(),
                fields: vec![],
                definition: Box::new(SelectSpecification::Simple(select.clone())),
            });
            let num_expressions = registry.expressions.len();
            let num_dependencies = registry.dependencies.len();
            let num_aliases = registry.aliases.len();
            assert!(!registry.add_query(view.clone()).unwrap());
            assert_eq!(registry.expressions.len(), num_expressions);
            assert_eq!(registry.dependencies.len(), num_dependencies);
            assert_eq!(registry.aliases.len(), num_aliases + 1);
            let view_qid = registry.aliases.get(&view_name).unwrap();
            let stored_expression = registry.expressions.get(view_qid).unwrap();
            if let RecipeExpression::View(cvs) = stored_expression {
                assert_ne!(cvs.name.clone(), view_name);
                let stored_select = cvs.definition.as_ref();
                if let SelectSpecification::Simple(stored_select) = stored_select {
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
            let mut registry = create_registry();
            let table_name: SqlIdentifier = "test_table2".into();
            let create_table = RecipeExpression::Table(
                nom_sql::parse_create_table(Dialect::MySQL, "CREATE TABLE test_table2 (col1 INT);")
                    .unwrap(),
            );
            let num_expressions = registry.expressions.len();
            let num_dependencies = registry.dependencies.len();
            let num_aliases = registry.aliases.len();
            assert!(registry.add_query(create_table.clone()).unwrap());
            assert_eq!(registry.expressions.len(), num_expressions + 1);
            assert_eq!(registry.dependencies.len(), num_dependencies);
            assert_eq!(registry.aliases.len(), num_aliases + 1);
            let table_qid = registry.aliases.get(&table_name).unwrap();
            // And the ID stored should be the same as the one in expressions.
            assert_eq!(registry.expressions.get(table_qid).unwrap(), &create_table);

            assert!(!registry.dependencies.contains_key(&table_qid));
            let table_dependencies = registry.dependencies.values().next().unwrap();
            assert_eq!(table_dependencies.len(), 2);
            assert!(!table_dependencies.contains(table_qid));
        }

        #[test]
        fn add_existing_table() {
            let mut registry = create_registry();
            let table_name: SqlIdentifier = "test_table".into();
            let create_table = RecipeExpression::Table(
                nom_sql::parse_create_table(Dialect::MySQL, "CREATE TABLE test_table (col1 INT);")
                    .unwrap(),
            );
            let num_expressions = registry.expressions.len();
            let num_dependencies = registry.dependencies.len();
            let num_aliases = registry.aliases.len();
            assert!(!registry.add_query(create_table.clone()).unwrap());
            assert_eq!(registry.expressions.len(), num_expressions);
            assert_eq!(registry.dependencies.len(), num_dependencies);
            assert_eq!(registry.aliases.len(), num_aliases);
            let table_qid = registry.aliases.get(&table_name).unwrap();
            assert_eq!(registry.expressions.get(table_qid).unwrap(), &create_table);

            let table_dependencies = registry.dependencies.values().next().unwrap();
            assert_eq!(table_dependencies.len(), 2);
            assert!(!table_dependencies.contains(table_qid));
        }

        #[test]
        fn get_by_name() {
            let registry = create_registry();
            let query_name: SqlIdentifier = "test_query".into();
            let query = registry.get(&query_name).unwrap();
            assert_eq!(query.name().clone(), query_name);
            let query_qid = registry.aliases.get(&query_name).unwrap();
            assert_eq!(registry.expressions.get(query_qid).unwrap(), query);
        }

        #[test]
        fn get_by_alias() {
            let registry = create_registry();
            let query_alias: SqlIdentifier = "test_query_alias".into();
            let query = registry.get(&query_alias).unwrap();
            assert_ne!(query.name().clone(), query_alias);
            let query_qid = registry.aliases.get(&query_alias).unwrap();
            assert_eq!(registry.expressions.get(query_qid).unwrap(), query);
            assert_eq!(registry.aliases.get(query.name()).unwrap(), query_qid);
        }

        #[test]
        fn remove_query() {
            let mut registry = create_registry();
            let name: SqlIdentifier = "test_query".into();
            let alias: SqlIdentifier = "test_query_alias".into();
            let expression = registry.get(&name).unwrap().clone();
            let expression_qid = *registry.aliases.get(&name).unwrap();
            let num_expressions = registry.expressions.len();
            let num_dependencies = registry.dependencies.len();
            let num_aliases = registry.aliases.len();
            let removed_expression = registry.remove_expression(&name).unwrap();
            assert_eq!(removed_expression, expression);
            assert_eq!(registry.expressions.len(), num_expressions - 1);
            assert_eq!(registry.dependencies.len(), num_dependencies);
            // expression had 2 aliases
            assert_eq!(registry.aliases.len(), num_aliases - 2);
            assert!(!registry.aliases.contains_key(&name));
            assert!(!registry.aliases.contains_key(&alias));

            let table_dependencies = registry.dependencies.values().next().unwrap();
            assert_eq!(table_dependencies.len(), 1);
            assert!(!table_dependencies.contains(&expression_qid));
        }

        #[test]
        fn remove_view() {
            let mut registry = create_registry();
            let name: SqlIdentifier = "test_view".into();
            let alias: SqlIdentifier = "test_view_alias".into();
            let expression = registry.get(&name).unwrap().clone();
            let expression_qid = *registry.aliases.get(&name).unwrap();
            let num_expressions = registry.expressions.len();
            let num_dependencies = registry.dependencies.len();
            let num_aliases = registry.aliases.len();
            let removed_expression = registry.remove_expression(&name).unwrap();
            assert_eq!(removed_expression, expression);
            assert_eq!(registry.expressions.len(), num_expressions - 1);
            assert_eq!(registry.dependencies.len(), num_dependencies);
            // expression had 2 aliases
            assert_eq!(registry.aliases.len(), num_aliases - 2);
            assert!(!registry.aliases.contains_key(&name));
            assert!(!registry.aliases.contains_key(&alias));

            let table_dependencies = registry.dependencies.values().next().unwrap();
            assert_eq!(table_dependencies.len(), 1);
            assert!(!table_dependencies.contains(&expression_qid));
        }

        #[test]
        fn remove_table() {
            let mut registry = create_registry();
            let name: SqlIdentifier = "test_table".into();
            let expression = registry.get(&name).unwrap().clone();
            let removed_expression = registry.remove_expression(&name).unwrap();
            assert_eq!(removed_expression, expression);
            assert!(registry.expressions.is_empty());
            assert!(registry.dependencies.is_empty());
            assert!(registry.aliases.is_empty());
        }

        #[test]
        fn len() {
            let registry = create_registry();
            assert_eq!(registry.len(), registry.expressions.len());
        }

        #[test]
        fn aliases_count() {
            let registry = create_registry();
            assert_eq!(registry.num_aliases(), registry.aliases.len());
        }
    }
}
