use std::collections::HashMap;
use std::str;
use std::vec::Vec;

use changelist::ChangeList;
use nom_sql::{CreateTableStatement, Dialect, SqlIdentifier, SqlQuery};
use noria::ActivationResult;
use noria_errors::{internal, internal_err, ReadySetError};
use petgraph::graph::NodeIndex;
use serde::{Deserialize, Serialize};
use tracing::{debug, error, warn};

use super::sql;
use crate::controller::recipe::changelist::{Change, SqlExpression};
use crate::controller::sql::SqlIncorporator;
use crate::controller::Migration;
use crate::{ReadySetResult, ReuseConfigType};

pub(crate) mod changelist;

/// The canonical SQL dialect used for central Noria server recipes. All direct clients of
/// noria-server must use this dialect for their SQL recipes, and all adapters and client libraries
/// must translate into this dialect as part of handling requests from users
pub const CANONICAL_DIALECT: Dialect = Dialect::MySQL;

type QueryID = u64;

/// Represents a Soup recipe.
#[derive(Clone, Debug, Serialize, Deserialize)]
// crate viz for tests
pub(crate) struct Recipe {
    /// SQL queries represented in the recipe..
    #[serde(with = "serde_with::rust::hashmap_as_tuple_list")]
    expressions: HashMap<QueryID, SqlExpression>,
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
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut h = DefaultHasher::new();
    q.hash(&mut h);
    h.finish()
}

#[allow(unused)]
impl Recipe {
    /// Get the id associated with an alias
    pub(crate) fn id_from_alias(&self, alias: &str) -> Option<&QueryID> {
        self.aliases.get(alias)
    }
    /// Get the SqlQuery associated with a query ID
    pub(crate) fn expression(&self, id: &QueryID) -> Option<&SqlQuery> {
        self.expressions.get(id).map(|expr| &expr.query)
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
            .map(|qid| self.expressions[qid].name.as_ref().unwrap())
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
        for change in changelist.changes {
            match change {
                Change::Add(expr) => {
                    // add the query
                    let qfp = self.inc.add_parsed_query(
                        expr.query.clone(),
                        expr.name.clone(),
                        expr.is_leaf,
                        mig,
                    )?;

                    let qid = hash_query(&expr.query);

                    if let Some(ref name) = expr.name {
                        if qfp.reused_nodes.get(0) == Some(&qfp.query_leaf)
                            && qfp.reused_nodes.len() == 1
                        {
                            self.alias_query(&qfp.name, name.clone()).map_err(|_| {
                                internal_err(
                                    "SqlIncorporator told recipe about a query it doesn't know about!",
                                )
                            })?;
                        }
                        if self.aliases.contains_key(name) && self.aliases[name] != qid {
                            return Err(ReadySetError::RecipeInvariantViolated(format!(
                                "Query name exists but existing query is different: {}",
                                name
                            )));
                        }
                        self.aliases.insert(name.clone(), qid);
                    }

                    // If the user provided us with a query name, use that.
                    // If not, use the name internally used by the QFP.
                    let query_name = expr
                        .name
                        .as_ref()
                        .cloned()
                        .unwrap_or_else(|| qfp.name.clone());
                    if self.expressions.insert(qid, expr).is_some() {
                        // We are re-adding a query that was already there.
                        to_remove.retain(|remove_qid| *remove_qid != qid);
                    }
                    self.expression_order.push(qid);
                    result.new_nodes.insert(query_name, qfp.query_leaf);
                    result.expressions_added += 1;
                }
                Change::Remove(expr) => {
                    match expr.query {
                        SqlQuery::DropTable(dts) => {
                            for table in dts.tables {
                                // As far as I know, we can't name CREATE TABLE statements, which
                                // are being assigned the table name
                                // as query name (based on the MIR code I've seen).
                                match self.aliases.get(table.name.as_str()) {
                                    Some(remove_qid) => {
                                        to_remove.push(*remove_qid);
                                    }
                                    None => {
                                        if !dts.if_exists {
                                            error!("Tried to drop table {} without IF EXISTS, but table doesn't exist.", table.name);
                                            internal!(
                                                "Tried to drop table {} without IF EXISTS, but table doesn't exist.",
                                                table.name
                                            );
                                        }
                                    }
                                }
                            }
                        }
                        _ => to_remove.push(hash_query(&expr.query)),
                    }
                }
            }
        }

        result.expressions_removed = to_remove.len();
        result.removed_leaves = to_remove
            .iter()
            .map(|qid| {
                self.expression_order.retain(|i| *i != *qid);
                self.aliases.retain(|_, i| *i != *qid);
                let expr = self.expressions.remove(qid).unwrap();
                Ok(match expr.query {
                    SqlQuery::CreateTable(ref ctq) => {
                        // a base may have many dependent queries, including ones that also lost
                        // nodes; the code handling `removed_leaves` therefore needs to take care
                        // not to remove bases while they still have children, or to try removing
                        // them twice.
                        self.inc.remove_base(&ctq.table.name)?;
                        match self.node_addr_for(&ctq.table.name) {
                            Ok(ni) => Some(ni),
                            Err(e) => {
                                error!(
                                    err = %e,
                                    name = %ctq.table.name,
                                    "failed to remove base whose address could not be resolved",
                                );
                                internal!(
                                    "failed to remove base {} whose address could not be resolved",
                                    ctq.table.name
                                );
                            }
                        }
                    }
                    _ => self.inc.remove_query(expr.name.as_ref().unwrap())?,
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

    /// Returns the query expressions in the recipe.
    // crate viz for tests
    pub(crate) fn expressions(&self) -> Vec<(Option<&SqlIdentifier>, &SqlQuery)> {
        self.expressions
            .values()
            .map(|expr| (expr.name.as_ref(), &expr.query))
            .collect()
    }

    /// Creates a [`ChangeList`] specifying the removal
    /// of all queries in the recipe.
    pub(super) fn remove_all(&self) -> ChangeList {
        let mut changes = Vec::new();
        for expr in self.expressions.values() {
            changes.push(Change::Remove(expr.clone()));
        }
        ChangeList { changes }
    }

    /// Helper method to reparent a recipe. This is needed for some of t
    pub(super) fn sql_inc(&self) -> &SqlIncorporator {
        &self.inc
    }

    /// Remove the expression with the given alias, `qname`, from the recipe.
    pub(super) fn remove_query(&self, qname: &str) -> Option<ChangeList> {
        let res = self.aliases.get(qname).and_then(|n| {
            self.expressions.get(n).cloned().map(|expr| ChangeList {
                changes: vec![Change::Remove(expr)],
            })
        });

        if res.is_none() {
            warn!(%qname, "Query not found in expressions");
        }

        res
    }

    /// Alias `query` as `alias`. Subsequent calls to `node_addr_for(alias)` will return the node
    /// addr for `query`.
    ///
    /// Returns an Err if `query` is not found in the recipe
    pub(super) fn alias_query(&mut self, query: &str, alias: SqlIdentifier) -> Result<(), String> {
        // NOTE: this is (consciously) O(n) because we don't have a reverse index from query name to
        // QueryID and I don't feel like it's worth the time-space tradeoff given this is only
        // called on migration
        let qid = self
            .expressions
            .iter()
            .find(|(_, expr)| expr.name.as_deref() == Some(query))
            .ok_or_else(|| "Query not found".to_string())?
            .0;
        self.aliases.insert(alias, *qid);
        Ok(())
    }
}
