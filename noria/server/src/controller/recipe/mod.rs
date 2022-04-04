use std::collections::{HashMap, HashSet};
use std::str;
use std::vec::Vec;

use nom_sql::{
    CacheInner, CreateCacheStatement, CreateTableStatement, Dialect, SqlIdentifier, SqlQuery,
};
use noria::recipe::changelist::{Change, ChangeList};
use noria::ActivationResult;
use noria_errors::{internal, ReadySetError};
use petgraph::graph::NodeIndex;
use serde::{Deserialize, Serialize};
use tracing::{debug, error, warn};

use super::sql;
use crate::controller::recipe::registry::{ExpressionRegistry, RecipeExpression};
use crate::controller::sql::SqlIncorporator;
use crate::controller::Migration;
use crate::ReuseConfigType;

pub(super) mod registry;

/// The canonical SQL dialect used for central Noria server recipes. All direct clients of
/// noria-server must use this dialect for their SQL recipes, and all adapters and client libraries
/// must translate into this dialect as part of handling requests from users
pub const CANONICAL_DIALECT: Dialect = Dialect::MySQL;

type QueryID = u128;

/// Represents a Soup recipe.
#[derive(Clone, Debug, Serialize, Deserialize)]
// crate viz for tests
pub(crate) struct Recipe {
    /// A structure to keep track of all the [`RecipeExpression`]s in the recipe.
    registry: ExpressionRegistry,

    /// Maintains lower-level state, but not the graph itself. Lazily initialized.
    inc: SqlIncorporator,
}

unsafe impl Send for Recipe {}

impl PartialEq for Recipe {
    /// Equality for recipes is defined in terms of all members apart from `inc`.
    fn eq(&self, other: &Recipe) -> bool {
        self.registry == other.registry
    }
}

#[derive(Debug)]
pub(super) enum Schema {
    Table(CreateTableStatement),
    View(Vec<String>),
}

#[allow(unused)]
impl Recipe {
    /// Get the id associated with an alias
    pub(crate) fn expression_by_alias(&self, alias: &str) -> Option<SqlQuery> {
        let expr = self.registry.get(&alias.into()).map(|e| match e {
            RecipeExpression::Table(cts) => SqlQuery::CreateTable(cts.clone()),
            RecipeExpression::View(cvs) => SqlQuery::CreateView(cvs.clone()),
            RecipeExpression::Cache { name, statement } => {
                SqlQuery::CreateCache(CreateCacheStatement {
                    name: Some(name.clone()),
                    inner: CacheInner::Statement(Box::new(statement.clone())),
                })
            }
        });
        if expr.is_none() {
            warn!(%alias, "Query not found in expression registry");
        }
        expr
    }

    /// Creates a blank recipe. This is useful for bootstrapping, e.g., in interactive
    /// settings, and for temporary recipes.
    pub(crate) fn blank() -> Recipe {
        Recipe {
            registry: ExpressionRegistry::new(),
            inc: SqlIncorporator::new(),
        }
    }

    /// Creates a new blank recipe with the given SQL configuration and MIR configuration
    // crate viz for tests
    pub(crate) fn with_config(sql_config: sql::Config, mir_config: sql::mir::Config) -> Self {
        let mut res = Recipe::blank();
        res.set_sql_config(sql_config);
        res.set_mir_config(mir_config);
        res
    }

    /// Set the MIR configuration for this recipe
    // crate viz for tests
    pub(crate) fn set_mir_config(&mut self, mir_config: sql::mir::Config) {
        self.inc.set_mir_config(mir_config)
    }

    /// Get a shared reference to this recipe's MIR configuration
    pub(crate) fn mir_config(&self) -> &sql::mir::Config {
        self.inc.mir_config()
    }

    /// Set the SQL configuration for this recipe
    pub(crate) fn set_sql_config(&mut self, sql_config: sql::Config) {
        self.inc.config = sql_config;
    }

    /// Get a shared reference to this recipe's SQL configuration
    pub(crate) fn sql_config(&self) -> &sql::Config {
        &self.inc.config
    }

    /// Disable node reuse.
    // crate viz for tests
    pub(crate) fn disable_reuse(&mut self) {
        self.inc.disable_reuse();
    }

    /// Enable node reuse.
    // crate viz for tests
    pub(crate) fn enable_reuse(&mut self, reuse_type: ReuseConfigType) {
        self.inc.enable_reuse(reuse_type)
    }

    pub(in crate::controller) fn resolve_alias(&self, alias: &str) -> Option<&SqlIdentifier> {
        self.registry.resolve_alias(&alias.into())
    }

    /// Obtains the `NodeIndex` for the node corresponding to a named query or a write type.
    pub(in crate::controller) fn node_addr_for(
        &self,
        name: &SqlIdentifier,
    ) -> Result<NodeIndex, String> {
        // `name` might be an alias for another identical query, so resolve if needed
        let query_name = self.registry.resolve_alias(name).unwrap_or(name);
        match self.inc.get_query_address(query_name) {
            None => Err(format!("No query endpoint for \"{}\" exists .", name)),
            Some(na) => Ok(na),
        }
    }

    /// Get schema for a base table or view in the recipe.
    pub(super) fn schema_for(&self, name: &str) -> Option<Schema> {
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
    ) -> Result<ActivationResult, ReadySetError> {
        debug!(
            num_queries = self.registry.len(),
            named_queries = self.registry.num_aliases(),
        );

        let mut added = HashMap::new();
        let mut removed = HashSet::new();
        for change in changelist.changes {
            match change {
                Change::CreateTable(cts) => {
                    let query = SqlQuery::CreateTable(cts.clone());
                    let name = cts.table.name.clone();
                    let qfp = self
                        .inc
                        .add_parsed_query(query, Some(name.clone()), false, mig)?;
                    self.registry.add_query(RecipeExpression::Table(cts))?;
                    added.insert(name, qfp.query_leaf);
                    removed.remove(&qfp.query_leaf);
                }
                Change::CreateView(cvs) => {
                    let query = SqlQuery::CreateView(cvs.clone());
                    let name = cvs.name.clone();
                    let expression = RecipeExpression::View(cvs);
                    if !self.registry.add_query(expression)? {
                        // The expression is already present, and we successfully added
                        // a new alias for it.
                        continue;
                    }

                    // add the query
                    let qfp = self
                        .inc
                        .add_parsed_query(query, Some(name.clone()), true, mig)?;
                    added.insert(name, qfp.query_leaf);
                    removed.remove(&qfp.query_leaf);
                }
                Change::CreateCache(ccqs) => {
                    let select = match &ccqs.inner {
                        CacheInner::Statement(box stmt) => stmt.clone(),
                        CacheInner::Id(id) => {
                            error!("attempted to issue CREATE CACHE with an id: {}", id);
                            internal!("CREATE CACHE should've had its ID resolved by the adapter");
                        }
                    };
                    let query = SqlQuery::Select(select.clone());
                    if let Some(name) = ccqs.name {
                        let expression = RecipeExpression::Cache {
                            name: name.clone(),
                            statement: select,
                        };
                        if !self.registry.add_query(expression)? {
                            // The expression is already present, and we successfully added
                            // a new alias for it.
                            continue;
                        }
                        // add the query
                        let qfp =
                            self.inc
                                .add_parsed_query(query, Some(name.clone()), true, mig)?;
                        added.insert(name, qfp.query_leaf);
                        removed.remove(&qfp.query_leaf);
                    } else {
                        // add the query
                        let qfp = self.inc.add_parsed_query(query, None, true, mig)?;
                        self.registry.add_query(RecipeExpression::Cache {
                            name: qfp.name.clone(),
                            statement: select,
                        })?;
                        added.insert(qfp.name, qfp.query_leaf);
                        removed.remove(&qfp.query_leaf);
                    };
                }
                Change::Drop { name, if_exists } => {
                    let ni = match self.registry.remove_expression(&name) {
                        Some(RecipeExpression::Table(cts)) => {
                            // a base may have many dependent queries, including ones that also lost
                            // nodes; the code handling `removed_leaves` therefore needs to take
                            // care not to remove bases while they still
                            // have children, or to try removing
                            // them twice.
                            self.inc.remove_base(&cts.table.name)?;
                            match self.node_addr_for(&cts.table.name) {
                                Ok(ni) => Some(ni),
                                Err(e) => {
                                    error!(
                                        err = %e,
                                        name = %cts.table.name,
                                        "failed to remove base whose address could not be resolved",
                                    );
                                    internal!(
                                    "failed to remove base {} whose address could not be resolved",
                                    cts.table.name
                                );
                                }
                            }
                        }
                        Some(expression) => self.inc.remove_query(expression.name())?,
                        None => {
                            if !if_exists {
                                error!("attempted to DROP {} but it does not exist", name);
                                internal!("DROP {} does not exist", name);
                            }
                            None
                        }
                    };
                    if let Some(ni) = ni {
                        added.retain(|_, v| *v != ni);
                        removed.insert(ni);
                    }
                }
            }
        }

        // We upgrade schema version *after* applying changes, so that the initial
        // queries get correctly tagged with version 0.
        self.inc.upgrade_version();

        // TODO(fran): This is redundant. I'll make sure to remove it in the future.
        let expressions_added = added.len();
        let expressions_removed = removed.len();
        Ok(ActivationResult {
            new_nodes: added,
            removed_leaves: removed,
            expressions_added,
            expressions_removed,
        })
    }

    /// Helper method to reparent a recipe. This is needed for some of t
    pub(super) fn sql_inc(&self) -> &SqlIncorporator {
        &self.inc
    }
}
