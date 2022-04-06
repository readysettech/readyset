use std::collections::HashMap;
use std::convert::TryInto;
use std::str;
use std::vec::Vec;

use nom_sql::{
    CacheInner, CreateTableStatement, CreateViewStatement, Dialect, SelectStatement, SqlIdentifier,
    SqlQuery,
};
use noria::recipe::changelist::{Change, ChangeList};
use noria::ActivationResult;
use noria_errors::{internal, ReadySetError};
use petgraph::graph::NodeIndex;
use serde::{Deserialize, Serialize};
use tracing::{debug, error, info};

use super::sql;
use crate::controller::sql::SqlIncorporator;
use crate::controller::Migration;
use crate::{ReadySetResult, ReuseConfigType};

/// The canonical SQL dialect used for central Noria server recipes. All direct clients of
/// noria-server must use this dialect for their SQL recipes, and all adapters and client libraries
/// must translate into this dialect as part of handling requests from users
pub const CANONICAL_DIALECT: Dialect = Dialect::MySQL;

type QueryID = u128;

/// A single SQL expression stored in a Recipe.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum RecipeExpression {
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
    pub(crate) fn name(&self) -> &SqlIdentifier {
        match self {
            RecipeExpression::Table(stmt) => &stmt.table.name,
            RecipeExpression::View(cvs) => &cvs.name,
            RecipeExpression::Cache { name, .. } => name,
        }
    }
}

/// Represents a Soup recipe.
#[derive(Clone, Debug, Serialize, Deserialize)]
// crate viz for tests
pub(crate) struct Recipe {
    /// SQL queries represented in the recipe..
    #[serde(with = "serde_with::rust::hashmap_as_tuple_list")]
    expressions: HashMap<QueryID, RecipeExpression>,
    /// Addition order for the recipe expressions
    expression_order: Vec<QueryID>,
    /// Aliases assigned to read query and CreateTable expressions. Each alias maps
    /// to a `QueryId` in the expressions of the recipe.
    aliases: HashMap<SqlIdentifier, QueryID>,

    /// Maintains lower-level state, but not the graph itself. Lazily initialized.
    inc: SqlIncorporator,
}

unsafe impl Send for Recipe {}

impl PartialEq for Recipe {
    /// Equality for recipes is defined in terms of all members apart from `inc`.
    fn eq(&self, other: &Recipe) -> bool {
        self.expressions == other.expressions
            && self.expression_order == other.expression_order
            && self.aliases == other.aliases
    }
}

#[derive(Debug)]
pub(super) enum Schema {
    Table(CreateTableStatement),
    View(Vec<String>),
}

fn hash_query(q: &SqlQuery) -> QueryID {
    use sha1::{Digest, Sha1};
    let mut hasher = Sha1::new();
    hasher.update(q.to_string().as_bytes());
    // Sha1 digest is 20 byte long, so it is safe to consume only 16 bytes
    u128::from_le_bytes(hasher.finalize()[..16].try_into().unwrap())
}

#[allow(unused)]
impl Recipe {
    /// Get the id associated with an alias
    pub(crate) fn id_from_alias(&self, alias: &str) -> Option<&QueryID> {
        self.aliases.get(alias)
    }

    /// Get the SqlQuery associated with a query ID
    pub(crate) fn expression(&self, id: &QueryID) -> Option<SqlQuery> {
        self.expressions.get(id).map(|expr| match expr {
            RecipeExpression::Table(cts) => SqlQuery::CreateTable(cts.clone()),
            RecipeExpression::View(cvs) => SqlQuery::CreateView(cvs.clone()),
            RecipeExpression::Cache { statement, .. } => SqlQuery::Select(statement.clone()),
        })
    }

    /// Creates a blank recipe. This is useful for bootstrapping, e.g., in interactive
    /// settings, and for temporary recipes.
    pub(crate) fn blank() -> Recipe {
        Recipe {
            expressions: HashMap::default(),
            expression_order: Vec::default(),
            aliases: HashMap::default(),
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
        self.aliases
            .get(alias)
            .map(|qid| self.expressions[qid].name())
    }

    /// Obtains the `NodeIndex` for the node corresponding to a named query or a write type.
    pub(in crate::controller) fn node_addr_for(
        &self,
        name: &SqlIdentifier,
    ) -> Result<NodeIndex, String> {
        // `name` might be an alias for another identical query, so resolve if needed
        let na = match self.resolve_alias(name) {
            None => self.inc.get_query_address(name),
            Some(internal_qn) => self.inc.get_query_address(internal_qn),
        };
        match na {
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
            num_queries = self.expressions.len(),
            named_queries = self.aliases.len(),
        );

        let mut result = ActivationResult {
            new_nodes: HashMap::default(),
            removed_leaves: Vec::default(),
            expressions_added: 0,
            expressions_removed: 0,
        };

        let mut to_remove = Vec::new();
        for change in changelist {
            match change {
                Change::CreateTable(cts) => {
                    let query = SqlQuery::CreateTable(cts.clone());
                    let name = cts.table.name.clone();
                    let qid = hash_query(&query);
                    let qfp =
                        self.inc
                            .add_parsed_query(query.clone(), Some(name.clone()), false, mig)?;
                    self.aliases.insert(name.clone(), qid);
                    if self
                        .expressions
                        .insert(qid, RecipeExpression::Table(cts))
                        .is_some()
                    {
                        // We are re-adding a table that already existed, so
                        // remove the query from the list of queries to be removed.
                        to_remove.retain(|remove_qid| *remove_qid != qid);
                    }
                    self.expression_order.push(qid);
                    result.new_nodes.insert(name, qfp.query_leaf);
                }
                Change::CreateView(cvs) => {
                    let query = SqlQuery::CreateView(cvs.clone());
                    let name = cvs.name.clone();
                    let qid = hash_query(&query);
                    if self.alias_if_exists(qid, Some(&name), &mut to_remove)? {
                        continue;
                    }

                    // add the query
                    let qfp = self
                        .inc
                        .add_parsed_query(query, Some(name.clone()), true, mig)?;
                    self.aliases.insert(name.clone(), qid);
                    if self
                        .expressions
                        .insert(qid, RecipeExpression::View(cvs))
                        .is_some()
                    {
                        // We are re-adding a view that already existed, so
                        // remove the query from the list of queries to be removed.
                        to_remove.retain(|remove_qid| *remove_qid != qid);
                    }
                    self.expression_order.push(qid);
                    result.new_nodes.insert(name, qfp.query_leaf);
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
                    let name = ccqs.name;
                    let qid = hash_query(&query);
                    if self.alias_if_exists(qid, name.as_ref(), &mut to_remove)? {
                        continue;
                    }

                    // add the query
                    let qfp = self.inc.add_parsed_query(query, name.clone(), true, mig)?;
                    // If the user provided us with a query name, use that.
                    // If not, use the name internally used by the QFP.
                    let query_name = name.as_ref().cloned().unwrap_or_else(|| qfp.name.clone());

                    if let Some(name) = name {
                        self.aliases.insert(name, qid);
                    }
                    if self
                        .expressions
                        .insert(
                            qid,
                            RecipeExpression::Cache {
                                name: query_name.clone(),
                                statement: select,
                            },
                        )
                        .is_some()
                    {
                        // We are re-adding a cached query that already existed, so
                        // remove the query from the list of queries to be removed.
                        to_remove.retain(|remove_qid| *remove_qid != qid);
                    }
                    self.expression_order.push(qid);
                    result.new_nodes.insert(query_name, qfp.query_leaf);
                }
                Change::Drop { name, if_exists } => {
                    info!(%name, %if_exists, "Dropping table/query/view");
                    if let Some(remove_qid) = self.aliases.get(name.as_str()) {
                        to_remove.push(*remove_qid);
                    } else if !if_exists {
                        error!("Tried to drop query {}, but query doesn't exist.", name);
                        internal!("Tried to drop query {}, but query doesn't exist.", name);
                    }
                }
            }
        }

        result.expressions_added = result.new_nodes.len();
        result.expressions_removed = to_remove.len();
        result.removed_leaves = to_remove
            .iter()
            .map(|qid| {
                self.expression_order.retain(|i| *i != *qid);
                self.aliases.retain(|_, i| *i != *qid);
                let expr = self.expressions.remove(qid).unwrap();
                Ok(match expr {
                    RecipeExpression::Table(ref cts) => {
                        // a base may have many dependent queries, including ones that also lost
                        // nodes; the code handling `removed_leaves` therefore needs to take care
                        // not to remove bases while they still have children, or to try removing
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
                    e => self.inc.remove_query(e.name())?,
                })
            })
            // FIXME(eta): error handling impl overhead
            .collect::<ReadySetResult<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect();

        // We upgrade schema version *after* applying changes, so that the initial
        // queries get correctly tagged with version 0.
        self.inc.upgrade_version();
        Ok(result)
    }

    /// Helper method that adds the given alias (`name`) to the [`RecipeExpression`] with
    /// the given [`QueryID`].
    /// If the [`RecipeExpression`] was present, we also remove the [`QueryID`] from the list
    /// of queries marked for removal.
    ///
    /// This function returns:
    /// - `Ok(true)` if the [`RecipeExpression`] was already present
    /// and the new alias was added successfully.
    /// - `Ok(false)` if the [`RecipeExpression`] was not present.
    /// - `Err(ReadySetError::RecipeInvariantViolated(_))` if the alias was already taken by
    /// another (different) [`RecipeExpression`].
    fn alias_if_exists(
        &mut self,
        qid: QueryID,
        name: Option<&SqlIdentifier>,
        to_remove: &mut Vec<QueryID>,
    ) -> ReadySetResult<bool> {
        if let Some(cur) = self.expressions.get(&qid) {
            // The recipe already contains this query/view, so just alias it if
            // possible, or ignore if not
            if let Some(name) = name {
                info!(%name, existing_query = %qid, "Aliasing query/view");
                if *self.aliases.entry(name.clone()).or_insert(qid) != qid {
                    return Err(ReadySetError::RecipeInvariantViolated(format!(
                        "Query name exists but existing query is different: {name}"
                    )));
                }
            }
            // We are re-adding a query/view that already existed, so
            // remove the query from the list of queries to be removed.
            to_remove.retain(|remove_qid| *remove_qid != qid);
            return Ok(true);
        }
        Ok(false)
    }

    pub(crate) fn remove_leaf_aliases(&mut self, nodes: &[NodeIndex]) {
        for node in nodes {
            if let Some(name) = self.inc.get_leaf_name(*node) {
                if let Some(qid) = self.aliases.remove(name) {
                    self.expressions.remove(&qid);
                    self.aliases.retain(|_, i| *i != qid);
                }
            }
        }
    }

    /// Helper method to reparent a recipe. This is needed for some of t
    pub(super) fn sql_inc(&self) -> &SqlIncorporator {
        &self.inc
    }

    /// Remove the expression with the given alias, `qname`, from the recipe.
    pub(super) fn remove_query(&mut self, qname: &str) -> Option<ChangeList> {
        if !self.aliases.contains_key(qname) {
            return None;
        }
        Some(ChangeList {
            changes: vec![Change::Drop {
                name: qname.into(),
                if_exists: false,
            }],
        })
    }
}
