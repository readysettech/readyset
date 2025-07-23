use std::{fmt, str};

use petgraph::graph::NodeIndex;
use readyset_client::recipe::changelist::ChangeList;
use readyset_client::ViewCreateRequest;
use readyset_data::Dialect;
use readyset_errors::ReadySetResult;
use readyset_sql::ast::{Relation, SelectStatement};
use readyset_sql_passes::adapter_rewrites::AdapterRewriteParams;
use readyset_util::hash::hash;
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};
use tracing::warn;
use vec1::Vec1;

use super::registry::{MatchedCache, RecipeExpr};
use super::BaseSchema;
use crate::controller::sql::SqlIncorporator;
use crate::controller::Migration;

/// Uniquely identifies an expression in the expression registry.
#[derive(Clone, Copy, Default, Debug, Deserialize, Hash, Serialize, PartialEq, Eq)]
#[repr(transparent)]
pub(crate) struct ExprId(u128);

impl From<&RecipeExpr> for ExprId {
    /// Calculates a SHA-1 hash of the [`RecipeExpr`], to identify it based on its contents.
    fn from(value: &RecipeExpr) -> Self {
        let mut hasher = Sha1::new();
        match value {
            RecipeExpr::Table {
                name,
                body,
                pg_meta,
            } => {
                hasher.update(hash(name).to_le_bytes());
                hasher.update(hash(body).to_le_bytes());
                hasher.update(hash(pg_meta).to_le_bytes());
            }
            RecipeExpr::View { name, definition } => {
                hasher.update(hash(name).to_le_bytes());
                hasher.update(hash(definition).to_le_bytes());
            }
            RecipeExpr::Cache { statement, .. } => hasher.update(hash(statement).to_le_bytes()),
        };
        // Sha1 digest is 20 byte long, so it is safe to consume only 16 bytes
        Self(u128::from_le_bytes(
            hasher.finalize()[..16].try_into().unwrap(),
        ))
    }
}

impl From<&SelectStatement> for ExprId {
    fn from(value: &SelectStatement) -> ExprId {
        let mut hasher = Sha1::new();
        hasher.update(hash(value).to_le_bytes());
        // Sha1 digest is 20 byte long, so it is safe to consume only 16 bytes
        Self(u128::from_le_bytes(
            hasher.finalize()[..16].try_into().unwrap(),
        ))
    }
}

impl fmt::Display for ExprId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "q_{:x}", self.0)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
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
    View,
}

impl Recipe {
    /// Get the [`RecipeExpr`] associated with an alias
    pub(crate) fn expression_by_alias(&self, alias: &Relation) -> Option<RecipeExpr> {
        let expr = self.inc.registry.get(alias).cloned();
        if expr.is_none() {
            warn!(alias = %alias.display_unquoted(), "Query not found in expression registry");
        }
        expr
    }

    /// Creates a blank recipe. This is useful for bootstrapping, e.g., in interactive
    /// settings, and for temporary recipes.
    pub(crate) fn new(dialect: Dialect) -> Recipe {
        Recipe {
            inc: SqlIncorporator::new(dialect),
        }
    }

    /// Creates a new blank recipe with the given SQL configuration and MIR configuration
    pub(crate) fn with_config(
        dialect: Dialect,
        sql_config: super::Config,
        mir_config: super::mir::Config,
        permissive_writes: bool,
    ) -> Self {
        let mut res = Recipe::new(dialect);
        res.set_permissive_writes(permissive_writes);
        res.set_sql_config(sql_config);
        res.set_mir_config(mir_config);
        res
    }

    /// Set the MIR configuration for this recipe
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
                s.map(|_| Schema::View)
            }
            Some(s) => Some(Schema::Table(s)),
        }
    }

    /// Iterates through the list of changes that have to be made, and updates the MIR
    /// state accordingly.
    /// All the MIR graph changes are added to the [`Migration`], but it's up to the caller
    /// to call [`Migration::commit`] to also update the dataflow state.
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

    /// Returns the query name if, after rewriting according to `dialect`, `self` contains a query
    /// that is semantically equivalent to the given `query`. Returns `None` if `self` does not
    /// contain the query.
    ///
    /// Returns an error if rewriting fails for any reason
    pub(crate) fn expression_name_for_query(
        &self,
        query: ViewCreateRequest,
        dialect: Dialect,
    ) -> ReadySetResult<Option<Relation>> {
        let statement = self.inc.rewrite(
            query.statement,
            None,
            &query.schema_search_path,
            dialect,
            None,
            None,
        )?;
        Ok(self.inc.registry.expression_name(&statement))
    }

    /// Returns the MatchedCaches for the query if they exists.
    pub fn reused_caches(&self, name: &Relation) -> Option<&Vec1<MatchedCache>> {
        self.inc.registry.reused_caches(name)
    }

    /// Returns the adapter rewrite params stored in the config
    pub(crate) fn adapter_rewrite_params(&self) -> AdapterRewriteParams {
        AdapterRewriteParams {
            dialect: self.dialect().into(),
            server_supports_topk: self.mir_config().allow_topk,
            server_supports_mixed_comparisons: self.mir_config().allow_mixed_comparisons,
            server_supports_pagination: self.mir_config().allow_paginate
                && self.mir_config().allow_topk,
        }
    }

    /// The dialect used for converting SQL and making semantic decisions
    pub(crate) fn dialect(&self) -> Dialect {
        self.inc.dialect
    }
}
