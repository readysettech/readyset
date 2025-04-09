use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};

use readyset_client::query::QueryId;
use readyset_client::recipe::changelist::PostgresTableMetadata;
use readyset_client::PlaceholderIdx;
use readyset_errors::{internal_err, unsupported_err, ReadySetError, ReadySetResult};
use readyset_sql::analysis::visit::{self, Visitor};
use readyset_sql::ast::{
    CreateTableBody, CreateTableStatement, CreateViewStatement, ItemPlaceholder, Literal, Relation,
    SelectSpecification, SelectStatement, SqlType,
};
use readyset_sql_passes::SelectStatementSkeleton;
use readyset_util::redacted::Sensitive;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use tracing::debug;
use vec1::Vec1;

use super::ExprId;

/// A single SQL expression stored in a Recipe.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum RecipeExpr {
    /// Expression that represents a `CREATE TABLE` statement.
    Table {
        name: Relation,
        body: CreateTableBody,
        pg_meta: Option<PostgresTableMetadata>,
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
        query_id: QueryId,
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
                    type Error = std::convert::Infallible;

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
}

/// A pair of (Cached Literal, Given Literal) used to build a MatchedCache
type LiteralPair<'a, 'b> = (&'a Literal, &'b Literal);

/// A struct to capture the relationship between a query and a cached query. This will be used when
/// we allow making a [`ViewRequest`] by query instead of by name.
///
/// This struct maps values in a query AST to values in a corresponding dataflow migration. We
/// cannot directly relate an AST and dataflow migration - however, we can use this mapping to adapt
/// the mapping given by [`Reader::placeholder_map`].
#[derive(PartialEq, Eq, Debug, Clone, Serialize, Deserialize)]
pub struct MatchedCache {
    /// The name of the matching cached query.
    name: Relation,
    /// A mapping from query placeholders to inlined values in the cached query.
    ///
    /// The values given to these placeholders on query execution must match the corresponding
    /// Literal.
    required_values: HashMap<PlaceholderIdx, Literal>,
    /// A mapping from placeholder values in the cached query to inlined values in the given query.
    ///
    /// The placeholder indices may map to fixed literal values or placeholders in the given query.
    ///
    /// # Invariants
    /// - Every placeholder in the cached query must appear in this map.
    key_mapping: HashMap<PlaceholderIdx, Literal>,
}

impl MatchedCache {
    /// Builds a MatchedCache from a set of query literals and migrated query literals. The
    /// query and cached must correspond to the same [`ExprSkeletons`] entry and must match.
    pub fn new<'a, 'b>(
        name: Relation,
        mut literals: impl Iterator<Item = LiteralPair<'a, 'b>>,
    ) -> ReadySetResult<Self> {
        let mut required_values = HashMap::new();
        let mut key_mapping = HashMap::new();
        literals.try_for_each(|(l1, l2)| {
            match (l1, l2) {
                (Literal::Placeholder(ItemPlaceholder::DollarNumber(idx)), lit) => {
                    key_mapping.insert(*idx as usize, lit.clone());
                    Ok(())
                }
                (lit, Literal::Placeholder(ItemPlaceholder::DollarNumber(idx))) => {
                    required_values.insert(*idx as usize, lit.clone());
                    Ok(())
                }
                (Literal::Placeholder(_), _) | (_, Literal::Placeholder(_)) => {
                    Err(internal_err!("Expected numbered placeholder"))
                }
                _ => {
                    Ok(()) /* Literals exactly match - do nothing */
                }
            }
        })?;
        Ok(Self {
            name,
            required_values,
            key_mapping,
        })
    }

    /// Get the name of the cached View.
    pub fn name(&self) -> &Relation {
        &self.name
    }

    /// Get a map of placeholders in the query to values required for those placeholders to borrow
    /// this View.
    pub fn required_values(&self) -> &HashMap<PlaceholderIdx, Literal> {
        &self.required_values
    }

    /// Get a map of placeholders in the cached query, to Literals in the query that is borrowing
    /// this View.
    pub fn key_mapping(&self) -> &HashMap<PlaceholderIdx, Literal> {
        &self.key_mapping
    }
}

/// A struct to maintain queries by their AST structure, without regard for literal values.
#[derive(Clone, Default, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub(super) struct ExprSkeletons {
    /// Map from the query with all literals removed (the "skeleton") to the name of the view and
    /// the set of literals that belong to that view.
    inner: HashMap<SelectStatementSkeleton, Vec<(Relation, Vec<Literal>)>>,
}

impl ExprSkeletons {
    /// Create an empty ExprSkeletons
    #[cfg(test)]
    pub fn new() -> Self {
        Self {
            inner: HashMap::new(),
        }
    }

    /// Insert a new expr (by name and literal values)
    pub fn insert(&mut self, query: SelectStatement, name: Relation) {
        let (skeleton, literals) = SelectStatementSkeleton::decompose_select(query);
        let entry = self.inner.entry(skeleton);
        match entry {
            Entry::Occupied(mut e) => {
                let exprs = e.get_mut();
                // No need to check literals, each relation should have a unique name
                if !exprs.iter().any(|(r, _)| *r == name) {
                    exprs.push((name, literals))
                }
            }
            Entry::Vacant(e) => {
                e.insert(vec![(name, literals)]);
            }
        }
    }

    /// Remove a set of expressions by name. [`ExprSkeletons`] will not resolve aliases.
    pub fn remove_expressions(&mut self, exprs: &HashSet<Relation>) {
        // For each entry, remove a set of literals if it is associated with one of the given
        // aliases
        self.inner.retain(|_, v| {
            v.retain(|(alias, _)| !exprs.contains(alias));
            // Remove the entire entry if we've removed all associated literals
            !v.is_empty()
        });
    }

    /// Find caches that match the given query.
    ///
    /// Two queries match if all their literals match. Two literals match if one or both are
    /// placeholder literals, or if they have equivalent literal values. If all literal values are
    /// equal, then we have two identical queries. Identical queries are ignored here because they
    /// are handled elsewhere.
    pub fn caches_for_query(&self, query: SelectStatement) -> ReadySetResult<Vec<MatchedCache>> {
        let (skeleton, literals) = SelectStatementSkeleton::decompose_select(query);
        // Iterate over every cached query for this entry. Compare the literals in the query AST for
        // that cached query with the literals in the given query AST. Ignore identical queries.
        // Return each result that could be identical for some set of placeholder
        // substitutions.
        match self.inner.get(&skeleton) {
            Some(exprs) => {
                exprs
                    .iter()
                    .try_fold(vec![], |mut acc, (rel, cached_literals)| {
                        // Test whether the literals are all equal, all matching or not matching
                        match cached_literals.iter().zip(literals.iter()).try_fold(
                            true,
                            |acc, (l1, l2)| {
                                // TODO: values might be equal after casting
                                if l1 == l2 {
                                    Some(acc) // equal
                                } else if l1.is_placeholder() || l2.is_placeholder() {
                                    Some(false) // matching
                                } else {
                                    None // not matching
                                }
                            },
                        ) {
                            // The cached query is identical to our query or the queries do not
                            // match. Ignore identical queries as those are handled elsewhere.
                            Some(true) | None => Ok(acc),
                            // This query matches the cached query (i.e., can use the same
                            // migration).
                            Some(false) => {
                                match MatchedCache::new(
                                    rel.clone(),
                                    cached_literals.iter().zip(literals.iter()),
                                ) {
                                    Ok(m) => {
                                        acc.push(m);
                                        Ok(acc)
                                    }
                                    Err(e) => Err(e),
                                }
                            }
                        }
                    })
            }
            None => Ok(vec![]),
        }
    }
}

/// The set of all [`RecipeExpr`]s installed in a ReadySet server cluster.
#[serde_as]
#[derive(Clone, Default, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub(super) struct ExprRegistry {
    /// A map from [`ExprId`] to the [`RecipeExpr`] associated with it.
    #[serde_as(as = "Vec<(_, _)>")]
    expressions: HashMap<ExprId, RecipeExpr>,

    /// A map from a hash of a stripped SelectStatement to all sets of stripped literals for each
    /// cached version of the stripped SelectStatement.
    ///
    /// A stripped expression has all literals replaced with question mark identifiers, and is used
    /// to match syntactically equivalent queries.
    skeletons: ExprSkeletons,

    /// The set of queries that depend on other queries.
    ///
    /// # Invariants
    ///
    /// - The keys here *must* be valid [`ExprId`]s (aka, present in `expressions`), and their
    ///   associated expression should be of [`RecipeExpr::Table`] variant: Tables don't depend on
    ///   anything, but queries depend on tables.
    /// - The values *must* be valid [`ExprId`]s (aka, present in `expressions`), and their
    ///   associated expression should be of [`RecipeExpr::Cache`] or [`RecipeExpr::View`] variant.
    dependencies: HashMap<ExprId, HashSet<ExprId>>,

    /// Map from names of custom types to the set of Query IDs for tables that have columns with
    /// those types.
    ///
    /// # Invariants
    ///
    /// - The keys *must* be valid custom types (aka, present in `self.inc.custom_types`),
    /// - The values *must* be valid [`ExprId`]s (aka, present in `expressions`), and their
    ///   associated expression should be of [`RecipeExpr::Table`]
    custom_type_dependencies: HashMap<Relation, HashSet<ExprId>>,

    /// Map from names of *nonexistent* tables, to a set of names for queries which should be
    /// invalidated if those tables are ever created
    table_to_invalidated_queries: HashMap<Relation, HashSet<Relation>>,

    /// Aliases assigned to each [`RecipeExpr`] stored.
    ///
    /// # Invariants
    /// - Each `ExprId` present here *must* exist as key in the `expressions` map.
    aliases: HashMap<Relation, ExprId>,

    /// Queries that can reuse the view for a different [`RecipeExpr::Cache`], specified by
    /// [`ExprId`]
    reused_caches: HashMap<Relation, Vec1<MatchedCache>>,
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
        let expr_id = (&expression).into();
        debug!(?expression, %expr_id, "Adding expression to the registry");
        // We always try adding the alias first, in case there's another table/query
        // with the same name already.
        self.assign_alias(expression.name().clone(), expr_id)?;

        if self.expressions.contains_key(&expr_id) {
            return Ok(false);
        }

        // If we're adding a cache, then add the statement to Self::skeletons
        if let RecipeExpr::Cache {
            ref name,
            ref statement,
            ..
        } = expression
        {
            self.skeletons.insert(statement.clone(), name.clone());
        }

        for table_reference in expression.table_references() {
            // Get the references from the expression.
            let Some(table_id) = self.aliases.get(&table_reference) else {
                // Queries can reference tables that don't exist in the schema, eg with CTEs or
                // aliased subqueries
                continue;
            };
            // Add the dependency.
            self.dependencies
                .entry(*table_id)
                .or_insert_with(|| HashSet::new())
                .insert(expr_id);
        }

        for ty in expression.custom_type_references() {
            if let Some(deps) = self.custom_type_dependencies.get_mut(ty) {
                deps.insert(expr_id);
            }
        }

        self.expressions.insert(expr_id, expression);
        Ok(true)
    }

    /// Retrieves the [`RecipeExpr`] associated with the given name or alias.
    /// If no query is found, returns `None`.
    pub(super) fn get(&self, alias: &Relation) -> Option<&RecipeExpr> {
        let query_id = self.aliases.get(alias)?;
        self.expressions.get(query_id)
    }

    /// Returns the name of the given expression if it is present in the recipe.
    pub(super) fn expression_name<E>(&self, expression: E) -> Option<Relation>
    where
        E: Into<ExprId>,
    {
        self.expressions
            .get(&expression.into())
            .map(|exp| match exp {
                RecipeExpr::View { name, .. }
                | RecipeExpr::Cache { name, .. }
                | RecipeExpr::Table { name, .. } => name.clone(),
            })
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

        // Remove all aliases for this query
        let expr_aliases = self
            .aliases
            .iter()
            .filter(|&(_, &v)| v == query_id)
            .map(|(k, _)| k.clone())
            .collect::<Vec<_>>();
        let expr_aliases = expr_aliases
            .iter()
            .map(|k| self.aliases.remove_entry(k).unwrap().0)
            .collect::<HashSet<_>>();

        // Remove any queries that reuse this query's cache, or the query itself if we are removing
        // a query that reuses a cache.
        //
        // Since Vec1::retain will not remove all entries, we filter out any that are left in the
        // DrainFilter so that we don't accidentally use them later.
        let mut removed_reused_cache = Vec::new();
        for (k, v) in self.reused_caches.iter_mut() {
            if expr_aliases.contains(k) || {
                // We are retaining, not removing elements here, so we retain if the element is
                // not in expr_aliases
                let res = v.retain(|cache| !expr_aliases.contains(&cache.name));
                // If we've removed all `MatchedCache`s, then remove the entire entry.
                // Vec1::retain gives an error if all the elements would have been removed.
                res.is_err()
            } {
                removed_reused_cache.push(k.clone());
            }
        }
        for k in &removed_reused_cache {
            self.reused_caches.remove(k);
        }

        // Remove the entry for this query's skeleton.
        self.skeletons.remove_expressions(&expr_aliases);
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
        for deps in self.custom_type_dependencies.values_mut() {
            deps.remove(&query_id);
        }

        // If we have only removed a reused cache, there is nothing else to clean up because the
        // expression does not exist in the graph.
        if removed_reused_cache
            .iter()
            .any(|name| expr_aliases.contains(name))
        {
            None
        } else {
            Some(expression)
        }
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
                query_name.display_unquoted(),
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

    /// Returns a list of caches that could be used to create a View for the given query.
    ///
    /// This function is used to match a query to a cached query when one or the other has inlined
    /// values where the other has placeholder variables.
    pub(super) fn caches_for_query(
        &self,
        query: SelectStatement,
    ) -> ReadySetResult<Vec<MatchedCache>> {
        self.skeletons.caches_for_query(query)
    }

    /// Add an entry to [`reused_caches`]. Used to indicate that a Relation will borrow the view
    /// for an existing cache.
    pub(super) fn add_reused_caches(&mut self, name: Relation, caches: Vec1<MatchedCache>) {
        // TODO: handle multiple caches for a single query
        #[allow(clippy::unwrap_used)] // Vec1 is guaranteed to have one element
        self.reused_caches.insert(name, caches);
    }

    /// Returns a MatchedCache for the query if one exists.
    pub fn reused_caches(&self, name: &Relation) -> Option<&Vec1<MatchedCache>> {
        self.reused_caches.get(name)
    }

    fn assign_alias(&mut self, alias: Relation, expr_id: ExprId) -> ReadySetResult<()> {
        match self.aliases.entry(alias.clone()) {
            Entry::Occupied(e) => {
                if *e.get() != expr_id {
                    return Err(ReadySetError::RecipeInvariantViolated(format!(
                        "Expression name exists but existing expression is different: {}",
                        alias.display_unquoted()
                    )));
                }
            }
            Entry::Vacant(e) => {
                debug!(alias = %e.key().display_unquoted(), %expr_id, "Aliasing existing expression");
                e.insert(expr_id);
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
            pg_meta: None,
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

#[cfg(test)]
mod tests {
    use readyset_sql::Dialect;

    use super::*;

    fn recipe_expr_cache(name: &'static str, query: &'static str) -> RecipeExpr {
        let statement = readyset_sql_parsing::parse_select(Dialect::MySQL, query).unwrap();

        RecipeExpr::Cache {
            name: name.into(),
            always: false,
            query_id: QueryId::from_select(&statement, &[]),
            statement,
        }
    }

    fn recipe_expr_view(name: &'static str, query: &'static str) -> RecipeExpr {
        RecipeExpr::View {
            name: name.into(),
            definition: SelectSpecification::Simple(
                readyset_sql_parsing::parse_select(Dialect::MySQL, query).unwrap(),
            ),
        }
    }

    mod expression {
        use nom_sql::parse_create_table;
        use readyset_sql::Dialect;
        use readyset_sql_parsing::parse_select;

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
            let cached_query = recipe_expr_cache("test_query", "SELECT * FROM test_table;");

            assert_eq!(cached_query.name(), &query_name);

            let view_name: Relation = "test_view".into();
            let view = RecipeExpr::View {
                name: view_name.clone(),
                definition: SelectSpecification::Simple(
                    parse_select(Dialect::MySQL, "SELECT * FROM test_table;").unwrap(),
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

            let cached_query = recipe_expr_cache("test_query", "SELECT * FROM test_table;");
            let cached_query_table_refs = cached_query.table_references();
            assert_eq!(cached_query_table_refs.len(), 1);
            assert_eq!(cached_query_table_refs.iter().next().unwrap(), &table_name);

            let view = RecipeExpr::View {
                name: "test_view".into(),
                definition: SelectSpecification::Simple(
                    parse_select(Dialect::MySQL, "SELECT * FROM test_table;").unwrap(),
                ),
            };

            let view_table_refs = view.table_references();
            assert_eq!(view_table_refs.len(), 1);
            assert_eq!(view_table_refs.iter().next().unwrap(), &table_name);
        }
    }

    mod registry {
        use nom_sql::parse_create_table;
        use readyset_sql::Dialect;
        use readyset_sql_parsing::parse_select;

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

            let cached_query = recipe_expr_cache("test_query", "SELECT * FROM test_table;");
            registry.add_query(cached_query).unwrap();

            let cached_query_alias =
                recipe_expr_cache("test_query_alias", "SELECT * FROM test_table;");
            registry.add_query(cached_query_alias).unwrap();

            let cached_query = recipe_expr_cache("test_query", "SELECT * FROM test_table;");
            registry.add_query(cached_query).unwrap();
            let cached_query_alias =
                recipe_expr_cache("test_query_alias", "SELECT * FROM test_table;");
            registry.add_query(cached_query_alias).unwrap();

            let view = recipe_expr_view("test_view", "SELECT DISTINCT * FROM test_table;");
            registry.add_query(view).unwrap();

            let view_alias =
                recipe_expr_view("test_view_alias", "SELECT DISTINCT * FROM test_table;");
            registry.add_query(view_alias).unwrap();

            registry
        }

        #[test]
        fn add_cached_query() {
            let mut registry = setup();

            let expr = recipe_expr_cache("test_query2", "SELECT DISTINCT * FROM test_table;");
            assert!(registry.add_query(expr.clone()).unwrap());

            let result = registry.get(&"test_query2".into()).unwrap();
            assert_eq!(*result, expr);
        }

        #[test]
        fn add_existing_cached_query() {
            let mut registry = setup();

            let expr = recipe_expr_cache("test_query2", "SELECT * FROM test_table;");
            assert!(!registry.add_query(expr.clone()).unwrap());

            let result = registry.get(&"test_query2".into()).unwrap();
            assert_eq!(
                *result,
                recipe_expr_cache("test_query", "SELECT * FROM test_table;"),
            );
        }

        #[test]
        fn add_view() {
            let mut registry = setup();
            let view_name: Relation = "test_view2".into();
            let view = RecipeExpr::View {
                name: view_name.clone(),
                definition: SelectSpecification::Simple(
                    parse_select(Dialect::MySQL, "SELECT DISTINCT * FROM test_table;").unwrap(),
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
            let select = parse_select(Dialect::MySQL, "SELECT * FROM test_table;").unwrap();
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
                recipe_expr_cache("test_query", "SELECT * FROM test_table"),
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
            let stmt = parse_select(Dialect::MySQL, "SELECT * FROM test_table").unwrap();
            let expr = recipe_expr_cache("test", "SELECT * FROM test_table");

            registry.add_query(expr).unwrap();

            assert!(registry.expression_name(&stmt).is_some())
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
                .add_query(recipe_expr_cache("test_query2", "SELECT * FROM test_table"))
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

            let statement =
                parse_select(Dialect::PostgreSQL, "SELECT CAST(x AS public.abc) FROM t").unwrap();
            assert!(registry
                .add_query(RecipeExpr::Cache {
                    name: "foo".into(),
                    query_id: QueryId::from_select(&statement, &[]),
                    statement: statement.clone(),
                    always: false,
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
                vec![(&"foo".into(), &statement)]
            );
        }
    }

    mod expr_skeleton {
        use std::collections::HashMap;

        use nom_sql::parse_create_table;
        use readyset_sql::ast::{ItemPlaceholder, Literal};
        use readyset_sql::Dialect;
        use readyset_sql_parsing::parse_select;

        use super::{recipe_expr_cache, ExprRegistry, ExprSkeletons, MatchedCache, RecipeExpr};

        #[test]
        fn equates_literals() {
            let mut skeleton = ExprSkeletons::new();
            skeleton.insert(
                parse_select(Dialect::MySQL, "SELECT NULL, true, 1 FROM t").unwrap(),
                "foo".into(),
            );
            // NULL is considered equivalent to itself in this context
            assert_eq!(
                skeleton
                    .caches_for_query(
                        parse_select(Dialect::MySQL, "SELECT NULL, true, $1 FROM t").unwrap(),
                    )
                    .unwrap(),
                vec![MatchedCache {
                    name: "foo".into(),
                    required_values: HashMap::from_iter([(1, Literal::Integer(1))]),
                    key_mapping: HashMap::new(),
                }]
            );
            // true != false
            assert_eq!(
                skeleton
                    .caches_for_query(
                        parse_select(Dialect::MySQL, "SELECT NULL, false, $1 FROM t").unwrap(),
                    )
                    .unwrap(),
                vec![]
            );
            // Float(1) != Integer(1)
            // TODO: We would like to consider equality after casting (e.g., "1" == 1 == 1.0)
            assert_eq!(
                skeleton
                    .caches_for_query(
                        parse_select(Dialect::MySQL, "SELECT NULL, $1, 1.0 from t").unwrap()
                    )
                    .unwrap(),
                vec![]
            );
        }

        #[test]
        fn finds_many() {
            let mut skeleton = ExprSkeletons::new();
            skeleton.insert(
                parse_select(Dialect::MySQL, "SELECT $1, NULL, 1 FROM t").unwrap(),
                "foo".into(),
            );
            skeleton.insert(
                parse_select(Dialect::MySQL, "SELECT $1, NULL, 2 FROM t").unwrap(),
                "bar".into(),
            );
            skeleton.insert(
                parse_select(Dialect::MySQL, "SELECT $1, $2, 2 FROM t").unwrap(),
                "baz".into(),
            );

            let res = skeleton
                .caches_for_query(
                    parse_select(Dialect::MySQL, "SELECT \"string\", NULL, $1 FROM t").unwrap(),
                )
                .unwrap();
            let truth = vec![
                MatchedCache {
                    name: "foo".into(),
                    required_values: HashMap::from_iter([(1, Literal::Integer(1))]),
                    key_mapping: HashMap::from_iter([(1, Literal::String("string".to_string()))]),
                },
                MatchedCache {
                    name: "bar".into(),
                    required_values: HashMap::from_iter([(1, Literal::Integer(2))]),
                    key_mapping: HashMap::from_iter([(1, Literal::String("string".to_string()))]),
                },
                MatchedCache {
                    name: "baz".into(),
                    required_values: HashMap::from_iter([(1, Literal::Integer(2))]),
                    key_mapping: HashMap::from_iter([
                        (1, Literal::String("string".to_string())),
                        (2, Literal::Null),
                    ]),
                },
            ];
            assert_eq!(res, truth);

            let res = skeleton
                .caches_for_query(parse_select(Dialect::MySQL, "SELECT 1, 2, 2 FROM t").unwrap())
                .unwrap();
            assert_eq!(
                res,
                vec![MatchedCache {
                    name: "baz".into(),
                    required_values: HashMap::new(),
                    key_mapping: HashMap::from_iter([
                        (1, Literal::Integer(1)),
                        (2, Literal::Integer(2))
                    ])
                }]
            );
        }

        #[test]
        fn adds_query() {
            let mut registry = ExprRegistry::default();

            registry
                .add_query(
                    RecipeExpr::try_from(
                        parse_create_table(Dialect::MySQL, "CREATE TABLE t (a INT);").unwrap(),
                    )
                    .unwrap(),
                )
                .unwrap();

            let query = "SELECT a FROM t WHERE a = 1 LIMIT ?;";

            registry
                .add_query(recipe_expr_cache("test_query", query))
                .unwrap();

            // Assert that entry was added
            assert_eq!(registry.skeletons.inner.len(), 1);

            // Assert that stored literals match query
            let entry = registry.skeletons.inner.iter().next().unwrap().1;
            assert_eq!(
                entry[0].1,
                vec![
                    Literal::Integer(1),
                    Literal::Placeholder(ItemPlaceholder::QuestionMark)
                ]
            );

            // Assert that aliases don't affect this cache
            registry
                .add_query(recipe_expr_cache("alias", query))
                .unwrap();

            assert_eq!(registry.skeletons.inner.len(), 1);
            let entry = registry.skeletons.inner.iter().next().unwrap().1;
            assert_eq!(entry.len(), 1);
        }

        #[test]
        fn removes_query() {
            let mut registry = ExprRegistry::default();

            registry
                .add_query(
                    RecipeExpr::try_from(
                        parse_create_table(Dialect::MySQL, "CREATE TABLE t (a INT);").unwrap(),
                    )
                    .unwrap(),
                )
                .unwrap();

            let query1 = "SELECT a FROM t WHERE a = 1 LIMIT ?;";
            let query2 = "SELECT a FROM t WHERE a = 2 LIMIT ?;";

            registry
                .add_query(recipe_expr_cache("query1", query1))
                .unwrap();

            registry
                .add_query(recipe_expr_cache("query1_alias", query1))
                .unwrap();

            registry
                .add_query(recipe_expr_cache("query2", query2))
                .unwrap();

            assert_eq!(registry.skeletons.inner.len(), 1);
            assert_eq!(registry.skeletons.inner.iter().next().unwrap().1.len(), 2);
            registry.remove_expression(&"query1_alias".into());
            assert_eq!(registry.skeletons.inner.iter().next().unwrap().1.len(), 1);
            registry.remove_expression(&"query2".into());
            assert_eq!(registry.skeletons.inner.len(), 0);
        }
    }
}
