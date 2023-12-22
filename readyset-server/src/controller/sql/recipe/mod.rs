use std::str;

use nom_sql::{
    CacheInner, CreateCacheStatement, CreateTableStatement, CreateViewStatement, Relation,
    SqlIdentifier, SqlQuery,
};
use petgraph::graph::NodeIndex;
use readyset_client::recipe::changelist::ChangeList;
use readyset_client::ViewCreateRequest;
use readyset_data::Dialect;
use readyset_errors::ReadySetResult;
use serde::{Deserialize, Serialize};
use tracing::warn;
use vec1::Vec1;

use super::registry::{MatchedCache, RecipeExpr};
use super::BaseSchema;
use crate::controller::sql::SqlIncorporator;
use crate::controller::Migration;

pub(crate) type QueryID = u128;

/// Represents a Soup recipe.
#[derive(Clone, Debug, Serialize, Deserialize)]
// crate viz for tests
pub(crate) struct Recipe {
    /// Maintains lower-level state, but not the graph itself. Lazily initialized.
    inc: SqlIncorporator,
}

impl PartialEq for Recipe {
    /// Equality for recipes is defined in terms of the expr registry
    fn eq(&self, other: &Recipe) -> bool {
        self.inc.registry == other.inc.registry
    }
}

#[derive(Debug)]
pub(crate) enum Schema<'a> {
    Table(&'a BaseSchema),
    View(&'a [SqlIdentifier]),
}

impl Recipe {
    /// Get the id associated with an alias
    pub(crate) fn expression_by_alias(&self, alias: &Relation) -> Option<SqlQuery> {
        let expr = self.inc.registry.get(alias).map(|e| match e {
            RecipeExpr::Table { name, body, .. } => SqlQuery::CreateTable(CreateTableStatement {
                if_not_exists: false,
                table: name.clone(),
                body: Ok(body.clone()),
                options: Ok(vec![]),
            }),
            RecipeExpr::View { name, definition } => SqlQuery::CreateView(CreateViewStatement {
                name: name.clone(),
                or_replace: false,
                fields: vec![],
                definition: Ok(Box::new(definition.clone())),
            }),
            RecipeExpr::Cache {
                name,
                statement,
                always,
            } => SqlQuery::CreateCache(CreateCacheStatement {
                name: Some(name.clone()),
                inner: Ok(CacheInner::Statement(Box::new(statement.clone()))),
                unparsed_create_cache_statement: None, // not relevant after migrating
                always: *always,
                concurrently: false, // concurrently not relevant after migrating
            }),
        });
        if expr.is_none() {
            warn!(alias = %alias.display_unquoted(), "Query not found in expression registry");
        }
        expr
    }

    /// Creates a blank recipe. This is useful for bootstrapping, e.g., in interactive
    /// settings, and for temporary recipes.
    pub(crate) fn blank() -> Recipe {
        Recipe {
            inc: SqlIncorporator::new(),
        }
    }

    /// Creates a new blank recipe with the given SQL configuration and MIR configuration
    pub(crate) fn with_config(
        sql_config: super::Config,
        mir_config: super::mir::Config,
        permissive_writes: bool,
    ) -> Self {
        let mut res = Recipe::blank();
        res.set_permissive_writes(permissive_writes);
        res.set_sql_config(sql_config);
        res.set_mir_config(mir_config);
        res
    }

    /// Set the MIR configuration for this recipe
    // crate viz for tests
    pub(crate) fn set_mir_config(&mut self, mir_config: super::mir::Config) {
        self.inc.set_mir_config(mir_config)
    }

    /// Get a shared reference to this recipe's MIR configuration
    pub(crate) fn mir_config(&self) -> &super::mir::Config {
        self.inc.mir_config()
    }

    /// Set the SQL configuration for this recipe
    pub(crate) fn set_sql_config(&mut self, sql_config: super::Config) {
        self.inc.config = sql_config;
    }

    /// Change the behavior of failed writes to base nodes
    /// If permissive writes is true, failed writes will be no-ops, else they will return errors
    pub(crate) fn set_permissive_writes(&mut self, permissive_writes: bool) {
        self.inc.set_permissive_writes(permissive_writes);
    }

    pub(in crate::controller) fn resolve_alias(&self, alias: &Relation) -> Option<&Relation> {
        self.inc.registry.resolve_alias(alias)
    }

    /// Returns a set of all *original names* for all caches in the recipe (not including aliases)
    pub(in crate::controller) fn cache_names(&self) -> impl Iterator<Item = &Relation> + '_ {
        self.inc.registry.cache_names()
    }

    /// Obtains the `NodeIndex` for the node corresponding to a named query or a write type.
    pub(in crate::controller) fn node_addr_for(
        &self,
        name: &Relation,
    ) -> Result<NodeIndex, String> {
        // `name` might be an alias for another identical query, so resolve if needed
        let query_name = self.inc.registry.resolve_alias(name).unwrap_or(name);
        match self.inc.get_query_address(query_name) {
            None => Err(format!(
                "No query endpoint for {} exists .",
                name.display_unquoted()
            )),
            Some(na) => Ok(na),
        }
    }

    /// Get schema for a base table or view in the recipe.
    pub(crate) fn schema_for<'a>(&'a self, name: &Relation) -> Option<Schema<'a>> {
        match self.inc.get_base_schema(name) {
            None => {
                let s = match self.resolve_alias(name) {
                    None => self.inc.get_view_schema(name),
                    Some(internal_qn) => self.inc.get_view_schema(internal_qn),
                };
                s.map(Schema::View)
            }
            Some(s) => Some(Schema::Table(s)),
        }
    }

    /// Iterates through the list of changes that have to be made, and updates the MIR
    /// state accordingly.
    /// All the MIR graph changes are added to the [`Migration`], but it's up to the caller
    /// to call [`Migration::commit`] to also update the dataflow state.
    // crate viz for tests
    pub(crate) fn activate(
        &mut self,
        mig: &mut Migration<'_>,
        changelist: ChangeList,
    ) -> ReadySetResult<()> {
        self.inc.apply_changelist(changelist, mig)
    }

    /// Helper method to reparent a recipe. This is needed for some of t
    pub(crate) fn sql_inc(&self) -> &SqlIncorporator {
        &self.inc
    }

    /// Returns `true` if, after rewriting according to `dialect`, `self` contains a query that is
    /// semantically equivalent to the given `query`.
    ///
    /// Returns an error if rewriting fails for any reason
    pub(crate) fn contains(
        &self,
        query: ViewCreateRequest,
        dialect: Dialect,
    ) -> ReadySetResult<bool> {
        let statement =
            self.inc
                .rewrite(query.statement, &query.schema_search_path, dialect, None)?;
        Ok(self.inc.registry.contains(&statement))
    }

    /// Returns the MatchedCaches for the query if they exists.
    pub fn reused_caches(&self, name: &Relation) -> Option<&Vec1<MatchedCache>> {
        self.inc.registry.reused_caches(name)
    }
}
