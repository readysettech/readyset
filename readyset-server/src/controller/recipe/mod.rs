use std::str;
use std::vec::Vec;

use nom_sql::{CacheInner, CreateCacheStatement, CreateTableStatement, SqlIdentifier, SqlQuery};
use petgraph::graph::NodeIndex;
use petgraph::visit::Bfs;
use readyset::recipe::changelist::{Change, ChangeList};
use readyset_errors::{
    internal, internal_err, invariant, invariant_eq, ReadySetError, ReadySetResult,
};
use serde::{Deserialize, Serialize};
use tracing::{debug, error, info, trace, warn};

use super::sql;
use crate::controller::recipe::alter_table::rewrite_table_definition;
use crate::controller::recipe::registry::{ExprRegistry, RecipeExpr};
use crate::controller::sql::SqlIncorporator;
use crate::controller::Migration;
use crate::ReuseConfigType;

mod alter_table;
pub(super) mod registry;

type QueryID = u128;

/// Represents a Soup recipe.
#[derive(Clone, Debug, Serialize, Deserialize)]
// crate viz for tests
pub(crate) struct Recipe {
    /// A structure to keep track of all the [`RecipeExpr`]s in the recipe.
    registry: ExprRegistry,

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
            RecipeExpr::Table(cts) => SqlQuery::CreateTable(cts.clone()),
            RecipeExpr::View(cvs) => SqlQuery::CreateView(cvs.clone()),
            RecipeExpr::Cache {
                name,
                statement,
                always,
            } => SqlQuery::CreateCache(CreateCacheStatement {
                name: Some(name.clone().into()), // TODO: schema
                inner: CacheInner::Statement(Box::new(statement.clone())),
                always: *always,
            }),
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
            registry: ExprRegistry::new(),
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

    /// Returns a set of all *original names* for all caches in the recipe (not including aliases)
    pub(in crate::controller) fn cache_names(&self) -> impl Iterator<Item = &SqlIdentifier> + '_ {
        self.registry.cache_names()
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
    ) -> ReadySetResult<()> {
        debug!(
            num_queries = self.registry.len(),
            named_queries = self.registry.num_aliases(),
        );

        for change in changelist {
            match change {
                Change::CreateTable(cts) => {
                    match self.registry.get(&cts.table.name) {
                        Some(RecipeExpr::Table(current_cts)) => {
                            // Table already exists, so check if it has been changed.
                            if current_cts != &cts {
                                // Table has changed. Drop and recreate.
                                trace!(
                                    name = %cts.table.name,
                                    "table exists and has changed. Dropping and recreating..."
                                );
                                self.drop_and_recreate(&cts.table.name.clone(), cts, mig);
                                continue;
                            }
                            trace!(
                                name = %cts.table.name,
                                "table exists, but hasn't changed. Ignoring..."
                            );
                        }
                        Some(RecipeExpr::View(_)) => {
                            return Err(ReadySetError::ViewAlreadyExists(
                                cts.table.name.clone().into(),
                            ))
                        }
                        _ => {
                            self.inc.add_table(cts.clone(), mig)?;
                            self.registry.add_query(RecipeExpr::Table(cts))?;
                        }
                    }
                }
                Change::CreateView(stmt) => {
                    let expression = RecipeExpr::View(stmt.clone());
                    if !self.registry.add_query(expression)? {
                        // The expression is already present, and we successfully added
                        // a new alias for it.
                        continue;
                    }

                    // add the query
                    self.inc.add_view(stmt, mig)?;
                }
                Change::CreateCache(ccqs) => {
                    let statement = match &ccqs.inner {
                        CacheInner::Statement(box stmt) => stmt.clone(),
                        CacheInner::Id(id) => {
                            error!("attempted to issue CREATE CACHE with an id: {}", id);
                            internal!("CREATE CACHE should've had its ID resolved by the adapter");
                        }
                    };
                    if let Some(name) = &ccqs.name {
                        let expression = RecipeExpr::Cache {
                            name: name.name.clone(), // TODO: schema
                            statement: statement.clone(),
                            always: ccqs.always,
                        };
                        if !self.registry.add_query(expression)? {
                            // The expression is already present, and we successfully added
                            // a new alias for it.
                            continue;
                        }
                    }

                    let name = self.inc.add_query(
                        ccqs.name.map(|t| t.name /* TODO: schema */),
                        statement.clone(),
                        mig,
                    )?;
                    self.registry.add_query(RecipeExpr::Cache {
                        name,
                        statement,
                        always: ccqs.always,
                    })?;
                }
                // We process ALTER TABLE statements in the following way:
                // 1. Create a copy of the table that is being altered. If it doesn't exist, then
                // return an error.
                // 2. Rewrite the table copy to reflect the changes specified by the ALTER TABLE
                // statement.
                // 3. Drop the original table.
                // 4. Install the new table.
                Change::AlterTable(ats) => {
                    let original_expression =
                        self.registry.get(&ats.table.name).ok_or_else(|| {
                            internal_err!(
                                "Tried to alter table {}, but table doesn't exist.",
                                ats.table.name
                            )
                        })?;
                    let original_table = match original_expression {
                        RecipeExpr::Table(ref table) => table,
                        _ => internal!(
                            "Tried to alter table {}, but that name belongs to a different expression.",
                            ats.table.name
                        ),
                    };
                    let new_table = rewrite_table_definition(&ats, original_table.clone())?;
                    let new_table_name = new_table.table.name.clone();
                    self.drop_and_recreate(&ats.table.name, new_table, mig);
                }
                Change::Drop { name, if_exists } => {
                    let removed_indices = self.remove_expression(&name, mig)?;
                    if removed_indices.is_none() && !if_exists {
                        error!(table = %name, "attempted to DROP TABLE, but table does not exist");
                        internal!("attempted to DROP TABLE, but table {} does not exist", name);
                    }
                }
            }
        }

        // We upgrade schema version *after* applying changes, so that the initial
        // queries get correctly tagged with version 0.
        self.inc.upgrade_version();

        Ok(())
    }

    /// Helper method to reparent a recipe. This is needed for some of t
    pub(super) fn sql_inc(&self) -> &SqlIncorporator {
        &self.inc
    }

    fn drop_and_recreate(
        &mut self,
        name: &SqlIdentifier,
        new_table: CreateTableStatement,
        mig: &mut Migration,
    ) -> ReadySetResult<()> {
        let removed_node_indices = self.remove_expression(name, mig)?;
        if removed_node_indices.is_none() {
            error!(table = %name,
                "attempted to issue ALTER TABLE, but table does not exist");
            return Err(ReadySetError::TableNotFound {
                name: name.clone().into(),
                schema: None, /* TODO */
            });
        };
        self.inc.add_table(new_table.clone(), mig)?;
        self.registry.add_query(RecipeExpr::Table(new_table))?;
        Ok(())
    }

    /// Remove the expression with the given name or alias, from the recipe.
    /// Returns the node indices that were removed due to the removal of the expression.
    /// Returns `Ok(None)` if the expression was not found.
    ///
    /// # Errors
    /// This method will return an error whenever there's an inconsistence between the
    /// [`ExprRegistry`] and the [`SqlIncorporator`], i.e, an expression exists in one but not
    /// in the other.
    // TODO(fran): As we keep improving the `Recipe`, we should refactor the `SqlIncorporator` and
    //  make sure there are no inconsistencies between it and the `ExprRegistry`.
    //  It might be worth looking into what the `SqlIncorporator` is storing, and how. I feel like
    //  these inconsistencies are only possible because of redundant information (the expressions
    //  belong  to both the `SqlIncorporator` and the `ExprRegistry`), and that feels
    //  unnecessary.
    //  Could we store the `RecipeExpr`s along with their associated `MirQuery`?
    //  Would we still need a `Recipe` structure if that's the case?
    pub(super) fn remove_expression(
        &mut self,
        name_or_alias: &SqlIdentifier,
        mig: &mut Migration<'_>,
    ) -> ReadySetResult<Option<Vec<NodeIndex>>> {
        let expression = match self.registry.remove_expression(name_or_alias) {
            Some(expression) => expression,
            None => {
                return Ok(None);
            }
        };
        let name = expression.name();
        let ni = match expression {
            RecipeExpr::Table(_) => {
                // a base may have many dependent queries, including ones that also lost
                // nodes; the code handling `removed_leaves` therefore needs to take care
                // not to remove bases while they still have children, or to try removing
                // them twice.
                match self.inc.remove_base(name) {
                    Ok(ni) => ni,
                    Err(e) => {
                        error!(
                            err = %e,
                            %name,
                            "failed to remove base whose address could not be resolved",
                        );
                        internal!(
                            "failed to remove base {} whose address could not be resolved",
                            name
                        );
                    }
                }
            }
            _ => self.inc.remove_query(name)?
                .ok_or_else(|| internal_err!("Inconsistent state in recipe: query exists in recipe but not in SqlIncorporator. Query name: {}", name))?
        };
        let is_base = mig
            .dataflow_state
            .ingredients
            .node_weight(ni)
            .map(|x| x.is_base())
            .unwrap_or(false);

        if !is_base {
            Ok(Some(self.remove_leaf(ni, mig)?))
        } else {
            let mut nodes_to_remove: Vec<NodeIndex> = Vec::new();
            let mut stack = vec![ni];
            while let Some(node) = stack.pop() {
                nodes_to_remove.push(node);
                mig.changes.drop_node(node);
                stack.extend(
                    mig.dataflow_state
                        .ingredients
                        .neighbors_directed(node, petgraph::EdgeDirection::Outgoing)
                        .filter(|ni| !mig.dataflow_state.ingredients[*ni].is_dropped()),
                );
            }

            self.remove_leaf_aliases(&nodes_to_remove);
            Ok(Some(nodes_to_remove))
        }
    }

    fn remove_leaf(
        &mut self,
        mut leaf: NodeIndex,
        mig: &mut Migration<'_>,
    ) -> ReadySetResult<Vec<NodeIndex>> {
        let mut removals = vec![];
        let start = leaf;
        if mig.dataflow_state.ingredients.node_weight(leaf).is_none() {
            return Err(ReadySetError::NodeNotFound {
                index: leaf.index(),
            });
        }
        #[allow(clippy::indexing_slicing)] // checked above
        {
            invariant!(!mig.dataflow_state.ingredients[leaf].is_source());
        }

        info!(
            node = %leaf.index(),
            "Computing removals for removing node",
        );

        let nchildren = mig
            .dataflow_state
            .ingredients
            .neighbors_directed(leaf, petgraph::EdgeDirection::Outgoing)
            .count();
        if nchildren > 0 {
            // This query leaf node has children -- typically, these are readers, but they can also
            // include egress nodes or other, dependent queries. We need to find the actual reader,
            // and remove that.
            if nchildren != 1 {
                internal!(
                    "cannot remove node {}, as it still has multiple children",
                    leaf.index()
                );
            }

            let mut readers = Vec::new();
            let mut bfs = Bfs::new(&mig.dataflow_state.ingredients, leaf);
            while let Some(child) = bfs.next(&mig.dataflow_state.ingredients) {
                #[allow(clippy::indexing_slicing)] // just came from self.ingredients
                let n = &mig.dataflow_state.ingredients[child];
                if n.is_reader_for(leaf) {
                    readers.push(child);
                }
            }

            // nodes can have only one reader attached
            invariant_eq!(readers.len(), 1);
            #[allow(clippy::indexing_slicing)]
            let reader = readers[0];
            #[allow(clippy::indexing_slicing)]
            {
                debug!(
                    node = %leaf.index(),
                    really = %reader.index(),
                    "Removing query leaf \"{}\"", mig.dataflow_state.ingredients[leaf].name()
                );
            }
            removals.push(reader);
            mig.changes.drop_node(reader);
            leaf = reader;
        }

        // `node` now does not have any children any more
        assert_eq!(
            mig.dataflow_state
                .ingredients
                .neighbors_directed(leaf, petgraph::EdgeDirection::Outgoing)
                .count(),
            0
        );

        let mut nodes = vec![leaf];
        while let Some(node) = nodes.pop() {
            let mut parents = mig
                .dataflow_state
                .ingredients
                .neighbors_directed(node, petgraph::EdgeDirection::Incoming)
                .detach();
            while let Some(parent) = parents.next_node(&mig.dataflow_state.ingredients) {
                #[allow(clippy::expect_used)]
                let edge = mig
                    .dataflow_state
                    .ingredients
                    .find_edge(parent, node)
                    .expect(
                    "unreachable: neighbors_directed returned something that wasn't a neighbour",
                );
                mig.dataflow_state.ingredients.remove_edge(edge);

                #[allow(clippy::indexing_slicing)]
                if !mig.dataflow_state.ingredients[parent].is_source()
                    && !mig.dataflow_state.ingredients[parent].is_base()
                    // ok to remove original start leaf
                    && (parent == start || !mig.dataflow_state.recipe.sql_inc().is_leaf_address(parent))
                    && mig.dataflow_state
                    .ingredients
                    .neighbors_directed(parent, petgraph::EdgeDirection::Outgoing)
                    .count() == 0
                {
                    nodes.push(parent);
                }
            }

            removals.push(node);
            mig.changes.drop_node(node);
        }
        Ok(removals)
    }

    pub(crate) fn remove_leaf_aliases(&mut self, nodes: &[NodeIndex]) {
        for node in nodes {
            if let Some(name) = self.inc.get_leaf_name(*node) {
                self.registry.remove_expression(name);
            }
        }
    }
}
