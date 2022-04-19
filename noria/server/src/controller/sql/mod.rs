use std::cmp::max;
use std::collections::{HashMap, HashSet};
use std::str;
use std::vec::Vec;

use ::mir::node::node_inner::MirNodeInner;
use ::mir::query::{MirQuery, QueryFlowParts};
use ::mir::reuse::merge_mir_for_queries;
use ::mir::visualize::GraphViz;
use ::mir::{reuse as mir_reuse, Column, MirNodeRef};
use ::serde::{Deserialize, Serialize};
use nom_sql::analysis::ReferredTables;
use nom_sql::{
    parser as sql_parser, BinaryOperator, CompoundSelectOperator, CompoundSelectStatement,
    CreateTableStatement, FieldDefinitionExpression, SelectStatement, SqlIdentifier, SqlQuery,
    Table,
};
use noria::internal::IndexType;
use noria_errors::{
    internal, internal_err, invalid_err, unsupported, ReadySetError, ReadySetResult,
};
use noria_sql_passes::alias_removal::TableAliasRewrite;
use noria_sql_passes::{
    contains_aggregate, AliasRemoval, CountStarRewrite, DetectProblematicSelfJoins,
    ImpliedTableExpansion, KeyDefinitionCoalescing, NegationRemoval, NormalizeTopKWithAggregate,
    OrderLimitRemoval, RewriteBetween, StarExpansion, StripPostFilters,
};
use petgraph::graph::NodeIndex;
use tracing::{debug, trace, warn};

use self::mir::SqlToMirConverter;
use self::query_graph::{to_query_graph, QueryGraph};
use self::query_signature::Signature;
use self::reuse::ReuseConfig;
use super::mir_to_flow::mir_query_to_flow_parts;
use super::recipe::CANONICAL_DIALECT;
use crate::controller::Migration;
use crate::ReuseConfigType;

pub(crate) mod mir;
mod query_graph;
mod query_signature;
mod reuse;
mod serde;

#[derive(Clone, Debug)]
enum QueryGraphReuse<'a> {
    ExactMatch(&'a MirQuery),
    ExtendExisting(Vec<u64>),
    /// (node, columns to re-project if necessary, parameters, index_type)
    ReaderOntoExisting(MirNodeRef, Option<Vec<Column>>, Vec<Column>, IndexType),
    None,
}

/// Configuration for converting SQL to dataflow
#[derive(Clone, Debug, Serialize, Deserialize)]
#[non_exhaustive]
pub(crate) struct Config {
    pub(crate) reuse_type: Option<ReuseConfigType>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            reuse_type: Some(ReuseConfigType::Finkelstein),
        }
    }
}

/// Long-lived struct that holds information about the SQL queries that have been incorporated into
/// the dataflow graph `graph`.
///
/// The incorporator shares the lifetime of the dataflow graph it is associated with.
#[derive(Clone, Debug, Default)]
// crate viz for tests
pub(crate) struct SqlIncorporator {
    mir_converter: SqlToMirConverter,
    leaf_addresses: HashMap<SqlIdentifier, NodeIndex>,

    /// Stores VIEWs and CACHE queries.
    named_queries: HashMap<SqlIdentifier, u64>,
    query_graphs: HashMap<u64, QueryGraph>,
    /// Stores CREATE TABLE statements.
    base_mir_queries: HashMap<SqlIdentifier, MirQuery>,
    mir_queries: HashMap<u64, MirQuery>,
    num_queries: usize,

    base_schemas: HashMap<SqlIdentifier, CreateTableStatement>,
    view_schemas: HashMap<SqlIdentifier, Vec<SqlIdentifier>>,

    pub(crate) config: Config,
}

impl SqlIncorporator {
    /// Creates a new `SqlIncorporator` for an empty flow graph.
    pub(super) fn new() -> Self {
        Default::default()
    }

    /// Set the MIR configuration for future migrations
    pub(crate) fn set_mir_config(&mut self, mir_config: mir::Config) {
        self.mir_converter.set_config(mir_config);
    }

    pub(crate) fn mir_config(&self) -> &mir::Config {
        self.mir_converter.config()
    }

    /// Disable node reuse for future migrations.
    #[allow(unused)]
    pub(crate) fn disable_reuse(&mut self) {
        self.config.reuse_type = None;
    }

    /// Disable node reuse for future migrations.
    #[allow(unused)]
    pub(crate) fn enable_reuse(&mut self, reuse_type: ReuseConfigType) {
        self.config.reuse_type = Some(reuse_type);
    }

    /// Incorporates a single query into via the flow graph migration in `mig`. The `query`
    /// argument is a string that holds a parameterized SQL query, and the `name` argument supplies
    /// an optional name for the query. If no `name` is specified, the table name is used in the
    /// case of CREATE TABLE queries, and a deterministic, unique name is generated and returned
    /// otherwise.
    ///
    /// The return value is a tuple containing the query name (specified or computing) and a `Vec`
    /// of `NodeIndex`es representing the nodes added to support the query.
    #[cfg(test)]
    pub(crate) fn add_query(
        &mut self,
        query: &str,
        name: Option<String>,
        mig: &mut Migration<'_>,
    ) -> Result<QueryFlowParts, ReadySetError> {
        query.to_flow_parts(self, name.map(SqlIdentifier::from), mig)
    }

    /// Incorporates a single query into via the flow graph migration in `mig`. The `query`
    /// argument is a `SqlQuery` structure, and the `name` argument supplies an optional name for
    /// the query. If no `name` is specified, the table name is used in the case of CREATE TABLE
    /// queries, and a deterministic, unique name is generated and returned otherwise.
    ///
    /// The return value is a tuple containing the query name (specified or computing) and a `Vec`
    /// of `NodeIndex`es representing the nodes added to support the query.
    pub(super) fn add_parsed_query(
        &mut self,
        query: SqlQuery,
        name: Option<SqlIdentifier>,
        is_leaf: bool,
        mig: &mut Migration<'_>,
    ) -> Result<QueryFlowParts, ReadySetError> {
        match name {
            None => self.nodes_for_query(query, is_leaf, mig),
            Some(n) => self.nodes_for_named_query(query, n, false, is_leaf, mig),
        }
    }

    pub(super) fn get_base_schema(&self, name: &str) -> Option<CreateTableStatement> {
        self.base_schemas.get(name).cloned()
    }

    pub(super) fn get_view_schema(&self, name: &str) -> Option<Vec<String>> {
        self.view_schemas
            .get(name)
            .map(|s| s.iter().map(SqlIdentifier::to_string).collect())
    }

    #[cfg(test)]
    fn get_flow_node_address(&self, name: &SqlIdentifier, v: usize) -> Option<NodeIndex> {
        self.mir_converter.get_flow_node_address(name, v)
    }

    /// Retrieves the flow node associated with a given query's leaf view.
    #[allow(unused)]
    pub(super) fn get_query_address(&self, name: &SqlIdentifier) -> Option<NodeIndex> {
        match self.leaf_addresses.get(name) {
            None => self.mir_converter.get_leaf(name),
            Some(na) => Some(*na),
        }
    }

    pub(super) fn is_leaf_address(&self, ni: NodeIndex) -> bool {
        self.leaf_addresses.values().any(|nn| *nn == ni)
    }

    pub(super) fn get_leaf_name(&self, ni: NodeIndex) -> Option<&SqlIdentifier> {
        self.leaf_addresses
            .iter()
            .find(|(_, idx)| **idx == ni)
            .map(|(name, _)| name)
    }

    fn consider_query_graph(
        &mut self,
        query_name: &str,
        is_name_required: bool,
        st: &SelectStatement,
        is_leaf: bool,
    ) -> ReadySetResult<(QueryGraph, QueryGraphReuse)> {
        debug!(%query_name, "Making query graph");
        trace!(%query_name, ?st);

        let mut qg = to_query_graph(st)?;

        trace!(%query_name, ?qg);

        let reuse_config = if let Some(reuse_type) = self.config.reuse_type {
            ReuseConfig::new(reuse_type)
        } else {
            // if reuse is disabled, we're done
            return Ok((qg, QueryGraphReuse::None));
        };

        // Do we already have this exact query or a subset of it
        // TODO(malte): make this an O(1) lookup by QG signature
        let qg_hash = qg.signature().hash;
        match self.mir_queries.get(&(qg_hash)) {
            None => (),
            Some(mir_query) => {
                let existing_qg = self
                    .query_graphs
                    .get(&qg_hash)
                    .ok_or_else(|| internal_err("query graph should be present"))?;

                // note that this also checks the *order* in which parameters are specified; a
                // different order means that we cannot simply reuse the existing reader.
                if existing_qg.signature() == qg.signature()
                    && existing_qg.parameters() == qg.parameters()
                    && existing_qg.exact_hash() == qg.exact_hash()
                    && (!is_name_required || mir_query.name == query_name)
                {
                    // we already have this exact query, down to the exact same reader key columns
                    // in exactly the same order
                    debug!(
                        %query_name,
                        "An exact match already exists, reusing it"
                    );

                    trace!(%mir_query.name, ?existing_qg);

                    return Ok((qg, QueryGraphReuse::ExactMatch(mir_query)));
                } else if existing_qg.signature() == qg.signature()
                    && existing_qg.parameters() != qg.parameters()
                {
                    use self::query_graph::OutputColumn;

                    // the signatures match, but this comparison has only given us an inexact
                    // result: we know that both queries mention the same
                    // columns, but not that they actually do the same
                    // comparisons or have the same literals. Hence, we need
                    // to scan the predicates here and ensure that for each predicate in the
                    // incoming QG, we have a matching predicate in the existing one.
                    // Since `qg.relations.predicates` only contains comparisons between columns
                    // and literals (col/col is a join predicate and associated with the join edge,
                    // col/param is stored in qg.params), we will not be inhibited by the fact that
                    // the queries have different parameters.
                    let mut predicates_match = true;
                    for (r, n) in qg.relations.iter() {
                        for p in n.predicates.iter() {
                            if !existing_qg.relations.contains_key(r)
                                || !existing_qg.relations[r].predicates.contains(p)
                            {
                                predicates_match = false;
                            }
                        }
                    }

                    // if any of our columns are grouped expressions, we can't reuse here, since
                    // the difference in parameters means that there is a difference in the implied
                    // GROUP BY clause
                    let no_grouped_columns = qg.columns.iter().all(|c| match *c {
                        OutputColumn::Literal(_) => true,
                        OutputColumn::Expression(ref ec) => contains_aggregate(&ec.expression),
                        OutputColumn::Data { .. } => true,
                    });

                    // TODO(grfn): When we want to bring back reuse, revisit the below comment - for
                    // one, ParamFilter no longer exists, for another, we now have parameter
                    // operators that *are* reusable like range queries
                    //
                    // But regardless, preserved (for now) for posterity:
                    // -------
                    // The reuse implementation below may only be performed when all parameters
                    // are equality operators. Query Graphs constructed for other operator types
                    // may contain nodes such as ParamFilter that contain materializations specific
                    // to their parameters and are not suitable for reuse with alternative
                    // parameters. (Note that simple range queries are expected to be implemented
                    // with range key lookups over equality parameter views. These views will be
                    // reused because of the equality parameters.)
                    let are_all_parameters_equalities = qg
                        .parameters()
                        .iter()
                        .all(|p| p.op == BinaryOperator::Equal)
                        && existing_qg
                            .parameters()
                            .iter()
                            .all(|p| p.op == BinaryOperator::Equal);

                    // Leaf queries with no parameters require that a "bogokey" dummy literal column
                    // be projected to support lookups. The ReaderOntoExisting optimization below
                    // does not support projecting a new bogokey if the existing query graph has
                    // parameters and therefore lacks a bogokey in its leaf projection.
                    let is_new_bogokey_needed = is_leaf
                        && qg.parameters().is_empty()
                        && !existing_qg.parameters().is_empty();

                    if predicates_match
                        && no_grouped_columns
                        && are_all_parameters_equalities
                        && !is_new_bogokey_needed
                    {
                        // QGs are identical, except for parameters (or their order)
                        debug!(
                            %query_name,
                            matching_query = %mir_query.name,
                            "Query has an exact match modulo parameters, so making a new reader",
                        );

                        let mut index_type = None;
                        let params = qg
                            .parameters()
                            .into_iter()
                            .map(|param| -> ReadySetResult<_> {
                                match IndexType::for_operator(param.op) {
                                    Some(it) if index_type.is_none() => index_type = Some(it),
                                    Some(it) if index_type == Some(it) => {}
                                    Some(_) => {
                                        unsupported!("Conflicting binary operators in query")
                                    }
                                    None => {
                                        unsupported!("Unsupported binary operator `{}`", param.op)
                                    }
                                }

                                Ok(Column::from(param.col.clone()))
                            })
                            .collect::<Result<Vec<_>, _>>()?;

                        let parent = mir_query
                            .leaf
                            .borrow()
                            .ancestors()
                            .iter()
                            .next()
                            .unwrap()
                            .upgrade()
                            .unwrap();

                        // If the existing leaf's parent contains all required parameter columns,
                        // reuse based on this parent.
                        if params.iter().all(|p| parent.borrow().columns().contains(p)) {
                            return Ok((
                                qg,
                                QueryGraphReuse::ReaderOntoExisting(
                                    parent,
                                    None,
                                    params,
                                    index_type.unwrap_or(IndexType::HashMap),
                                ),
                            ));
                        }

                        // We want to hang the new leaf off the last non-leaf node of the query that
                        // has the parameter columns we need, so backtrack until we find this place.
                        // Typically, this unwinds only two steps, above the final projection.
                        // However, there might be cases in which a parameter column needed is not
                        // present in the query graph (because a later migration added the column to
                        // a base schema after the query was added to the graph). In this case, we
                        // move on to other reuse options.

                        // If parent does not introduce any new columns absent in its ancestors,
                        // traverse its ancestor chain to find an ancestor with the necessary
                        // parameter columns.
                        let may_rewind = match parent.borrow().inner {
                            MirNodeInner::Identity => true,
                            MirNodeInner::Project {
                                expressions: ref e,
                                literals: ref l,
                                ..
                            } => e.is_empty() && l.is_empty(),
                            _ => false,
                        };
                        let ancestor = if may_rewind {
                            mir_reuse::rewind_until_columns_found(parent.clone(), &params)
                        } else {
                            None
                        };

                        // Reuse based on ancestor, which contains the required parameter columns.
                        if let Some(ancestor) = ancestor {
                            let project_columns = match ancestor.borrow().inner {
                                MirNodeInner::Project { .. } => {
                                    // FIXME Ensure ancestor includes all columns in qg, with the
                                    // proper names.
                                    None
                                }

                                _ => {
                                    // N.B.: we can't just add an identity here, since we might
                                    // have backtracked above a projection in order to get the
                                    // new parameter column(s). In this case, we need to add a
                                    // new projection that includes the same columns as the one
                                    // for the existing query, but also additional parameter
                                    // columns. The latter get added later; here we simply
                                    // extract the columns that need reprojecting and pass them
                                    // along with the reuse instruction.
                                    // FIXME Ensure ancestor includes all columns in parent, with
                                    // the proper names.
                                    Some(parent.borrow().columns().to_vec())
                                }
                            };

                            return Ok((
                                qg,
                                QueryGraphReuse::ReaderOntoExisting(
                                    ancestor,
                                    project_columns,
                                    params,
                                    index_type.unwrap_or(IndexType::HashMap),
                                ),
                            ));
                        }
                    }
                }
            }
        }

        // Find a promising set of query graphs
        let reuse_candidates = reuse_config.reuse_candidates(&mut qg, &self.query_graphs)?;

        if !reuse_candidates.is_empty() {
            debug!(
                num_candidates = reuse_candidates.len(),
                "Identified candidate QGs for reuse",
            );
            trace!(?qg, ?reuse_candidates);

            return Ok((
                qg,
                QueryGraphReuse::ExtendExisting(
                    reuse_candidates.iter().map(|(_, (sig, _))| *sig).collect(),
                ),
            ));
        } else {
            debug!("No reuse opportunity, adding fresh query");
        }

        Ok((qg, QueryGraphReuse::None))
    }

    fn add_base_via_mir(
        &mut self,
        query_name: &SqlIdentifier,
        stmt: CreateTableStatement,
        mig: &mut Migration<'_>,
    ) -> ReadySetResult<QueryFlowParts> {
        // first, compute the MIR representation of the SQL query
        let mut mir = self.mir_converter.named_base_to_mir(query_name, &stmt)?;

        trace!(base_node_mir = ?mir);

        // no optimization, because standalone base nodes can't be optimized

        // push it into the flow graph using the migration in `mig`, and obtain `QueryFlowParts`
        let qfp = mir_query_to_flow_parts(&mut mir, mig)?;

        // remember the schema in case we need it later
        // on base table schema change, we will overwrite the existing schema here.
        // TODO(malte): this means that requests for this will always return the *latest* schema
        // for a base.
        self.base_schemas.insert(query_name.clone(), stmt);

        self.register_query(query_name, None, &mir);

        Ok(qfp)
    }

    fn add_compound_query(
        &mut self,
        query_name: &SqlIdentifier,
        is_name_required: bool,
        query: &CompoundSelectStatement,
        is_leaf: bool,
        mig: &mut Migration<'_>,
    ) -> Result<QueryFlowParts, ReadySetError> {
        let subqueries: Result<Vec<_>, ReadySetError> = query
            .selects
            .iter()
            .enumerate()
            .map(|(i, sq)| {
                Ok(self
                    .add_select_query(
                        &format!("{}_csq_{}", query_name, i).into(),
                        is_name_required,
                        &sq.1,
                        false,
                        mig,
                    )?
                    .1)
            })
            .collect();

        let mut combined_mir_query = self.mir_converter.compound_query_to_mir(
            query_name,
            subqueries?.iter().collect(),
            CompoundSelectOperator::Union,
            &query.order,
            &query.limit,
            is_leaf,
        )?;

        let qfp = mir_query_to_flow_parts(&mut combined_mir_query, mig)?;

        self.register_query(query_name, None, &combined_mir_query);

        Ok(qfp)
    }

    fn select_query_to_mir(
        &mut self,
        query_name: &SqlIdentifier,
        is_name_required: bool,
        sq: &SelectStatement,
        is_leaf: bool,
    ) -> ReadySetResult<(QueryGraph, MirQuery)> {
        let (qg, reuse) = self.consider_query_graph(query_name, is_name_required, sq, is_leaf)?;

        let mir_query = match reuse {
            QueryGraphReuse::ExactMatch(mir_query) => mir_query.clone(),
            QueryGraphReuse::ExtendExisting(reuse_mirs) => {
                let mut new_query_mir = self
                    .mir_converter
                    .named_query_to_mir(query_name, sq, &qg, is_leaf)?;
                let mut num_reused_nodes = 0;
                for m in reuse_mirs {
                    if !self.mir_queries.contains_key(&m) {
                        continue;
                    }
                    let mq = &self.mir_queries[&m];
                    let (merged_mir, merged_nodes) = merge_mir_for_queries(&new_query_mir, mq);
                    new_query_mir = merged_mir;
                    num_reused_nodes = max(merged_nodes, num_reused_nodes);
                }
                new_query_mir
            }
            QueryGraphReuse::ReaderOntoExisting(
                final_query_node,
                project_columns,
                params,
                index_type,
            ) => self.mir_converter.add_leaf_below(
                final_query_node,
                query_name,
                &params,
                index_type,
                project_columns,
            ),
            QueryGraphReuse::None => self
                .mir_converter
                .named_query_to_mir(query_name, sq, &qg, is_leaf)?,
        };

        Ok((qg, mir_query))
    }

    /// Add a new SelectStatement to the given migration, returning information about the dataflow
    /// and MIR nodes that were added
    fn add_select_query(
        &mut self,
        query_name: &SqlIdentifier,
        is_name_required: bool,
        sq: &SelectStatement,
        is_leaf: bool,
        mig: &mut Migration<'_>,
    ) -> Result<(QueryFlowParts, MirQuery), ReadySetError> {
        let on_err = |e| ReadySetError::SelectQueryCreationFailed {
            qname: query_name.to_string(),
            source: Box::new(e),
        };

        let (qg, mir_query) = self
            .select_query_to_mir(query_name, is_name_required, sq, is_leaf)
            .map_err(on_err)?;

        trace!(pre_opt_mir = %mir_query.to_graphviz());
        let mut opt_mir = mir_query.optimize().map_err(on_err)?;
        trace!(post_opt_mir = %opt_mir.to_graphviz());

        let qfp = mir_query_to_flow_parts(&mut opt_mir, mig).map_err(on_err)?;
        self.register_query(query_name, Some(qg), &opt_mir);
        Ok((qfp, opt_mir))
    }

    pub(super) fn remove_query(
        &mut self,
        query_name: &SqlIdentifier,
    ) -> ReadySetResult<Option<NodeIndex>> {
        let nodeid = self
            .leaf_addresses
            .remove(query_name)
            .ok_or_else(|| internal_err("tried to remove unknown query"))?;

        let qg_hash = self.named_queries.remove(query_name).ok_or_else(|| {
            internal_err(format!(
                "missing query hash for named query \"{}\"",
                query_name
            ))
        })?;
        let mir = match self.mir_queries.get(&qg_hash) {
            None => return Ok(None),
            Some(mir) => mir,
        };

        // TODO(malte): implement this
        self.mir_converter.remove_query(query_name, mir)?;

        // clean up local state
        self.mir_queries.remove(&(qg_hash)).unwrap();
        self.query_graphs.remove(&qg_hash).unwrap();
        self.view_schemas.remove(query_name).unwrap();

        // traverse self.leaf__addresses
        if !self.leaf_addresses.values().any(|id| *id == nodeid) {
            // ok to remove

            // trigger reader node removal
            Ok(Some(nodeid))
        } else {
            // more than one query uses this leaf
            // don't remove node yet!
            Ok(None)
        }
    }

    /// Removes the base table with the given `name`, and all the MIR queries
    /// that depend on it.
    pub(super) fn remove_base(&mut self, name: &SqlIdentifier) -> ReadySetResult<NodeIndex> {
        debug!(%name, "Removing base from SqlIncorporator");
        if self.base_schemas.remove(name).is_none() {
            warn!(
                %name,
                "Attempted to remove non-existent base node from SqlIncorporator"
            );
        }

        let mir = self
            .base_mir_queries
            .remove(name)
            .ok_or_else(|| invalid_err(format!("tried to remove unknown base {}", name)))?;
        let roots = mir
            .roots
            .iter()
            .map(|r| r.borrow().name.clone())
            .collect::<HashSet<_>>();

        self.mir_queries.retain(|_, v| {
            !v.roots
                .iter()
                .any(|root| roots.contains(&root.borrow().name))
        });

        self.mir_converter.remove_base(name, &mir)?;

        self.leaf_addresses
            .remove(name)
            .ok_or_else(|| invalid_err(format!("tried to remove unknown base {}", name)))
    }

    fn register_query(
        &mut self,
        query_name: &SqlIdentifier,
        qg: Option<QueryGraph>,
        mir: &MirQuery,
    ) {
        // TODO(malte): we currently need to remember these for local state, but should figure out
        // a better plan (see below)
        let fields = mir
            .leaf
            .borrow()
            .columns()
            .iter()
            .map(|c| c.name.clone())
            .collect::<Vec<_>>();

        // TODO(malte): get rid of duplication and figure out where to track this state
        debug!(%query_name, "registering query");
        self.view_schemas.insert(query_name.clone(), fields);

        // We made a new query, so store the query graph and the corresponding leaf MIR node.
        // TODO(malte): we currently store nothing if there is no QG (e.g., for compound queries).
        // This means we cannot reuse these queries.
        match qg {
            Some(qg) => {
                let qg_hash = qg.signature().hash;
                self.query_graphs.insert(qg_hash, qg);
                self.mir_queries.insert(qg_hash, mir.clone());
                self.named_queries.insert(query_name.clone(), qg_hash);
            }
            None => {
                self.base_mir_queries
                    .insert(query_name.clone(), mir.clone());
            }
        }
    }

    fn nodes_for_query(
        &mut self,
        q: SqlQuery,
        is_leaf: bool,
        mig: &mut Migration<'_>,
    ) -> Result<QueryFlowParts, ReadySetError> {
        let name = match q {
            SqlQuery::CreateTable(ref ctq) => ctq.table.name.clone(),
            SqlQuery::CreateView(ref cvq) => cvq.name.clone(),
            SqlQuery::Select(_) | SqlQuery::CompoundSelect(_) => {
                format!("q_{}", self.num_queries).into()
            }
            _ => unsupported!("only CREATE TABLE and SELECT queries can be added to the graph!"),
        };
        self.nodes_for_named_query(q, name, false, is_leaf, mig)
    }

    /// Runs some standard rewrite passes on the query.
    fn rewrite_query(
        &mut self,
        q: SqlQuery,
        query_name: &SqlIdentifier,
        mig: &mut Migration<'_>,
    ) -> Result<SqlQuery, ReadySetError> {
        // TODO: make this not take &mut self

        // Check that all tables mentioned in the query exist.
        // This must happen before the rewrite passes are applied because some of them rely on
        // having the table schema available in `self.view_schemas`.
        match q {
            // if we're just about to create the table, we don't need to check if it exists. If it
            // does, we will amend or reuse it; if it does not, we create it.
            SqlQuery::CreateTable(_)
            | SqlQuery::CreateView(_)
            | SqlQuery::StartTransaction(_)
            | SqlQuery::Commit(_)
            | SqlQuery::Rollback(_)
            | SqlQuery::Use(_)
            | SqlQuery::Show(_)
            | SqlQuery::Explain(_)
            | SqlQuery::DropCache(_) => (),
            // other kinds of queries *do* require their referred tables to exist!
            ref q @ SqlQuery::CompoundSelect(_)
            | ref q @ SqlQuery::Select(_)
            | ref q @ SqlQuery::Set(_)
            | ref q @ SqlQuery::Update(_)
            | ref q @ SqlQuery::Delete(_)
            | ref q @ SqlQuery::DropTable(_)
            | ref q @ SqlQuery::AlterTable(_)
            | ref q @ SqlQuery::RenameTable(_)
            | ref q @ SqlQuery::Insert(_)
            | ref q @ SqlQuery::CreateCache(_) => {
                for t in &q.referred_tables() {
                    if !self.view_schemas.contains_key(&t.name) {
                        return Err(ReadySetError::TableNotFound(t.name.to_string()));
                    }
                }
            }
        }

        let mut q = q
            .rewrite_between()
            .remove_negation()?
            .strip_post_filters()
            .coalesce_key_definitions()
            .expand_stars(&self.view_schemas)?
            .expand_implied_tables(&self.view_schemas)?
            .normalize_topk_with_aggregate()?
            .rewrite_count_star(&self.view_schemas)?
            .detect_problematic_self_joins()?
            .order_limit_removal(&self.base_schemas)?;

        self.num_queries += 1;

        // Remove all table aliases from 'fq'. Create named views in cases where the alias must be
        // replaced with a view rather than the table itself in order to prevent ambiguity. (This
        // may occur when a single table is referenced using more than one alias).
        let table_alias_rewrites = q.rewrite_table_aliases(query_name);
        for r in table_alias_rewrites {
            match r {
                TableAliasRewrite::View {
                    to_view, for_table, ..
                } => {
                    let query = SqlQuery::Select(SelectStatement {
                        tables: vec![Table {
                            name: for_table,
                            ..Default::default()
                        }],
                        fields: vec![FieldDefinitionExpression::All],
                        ..Default::default()
                    });
                    let is_name_required = true;
                    self.nodes_for_named_query(query, to_view, is_name_required, false, mig)?;
                }
                TableAliasRewrite::Cte {
                    to_view,
                    for_statement,
                    ..
                } => {
                    let query = SqlQuery::Select(*for_statement);
                    self.nodes_for_named_query(query, to_view, true, false, mig)?;
                }
                TableAliasRewrite::Table { .. } => {}
            }
        }

        trace!(rewritten_query = %q);

        Ok(q)
    }

    fn nodes_for_named_query(
        &mut self,
        q: SqlQuery,
        query_name: SqlIdentifier,
        is_name_required: bool,
        is_leaf: bool,
        mig: &mut Migration<'_>,
    ) -> Result<QueryFlowParts, ReadySetError> {
        // short-circuit if we're dealing with a CreateView query; this avoids having to deal with
        // CreateView in all of our rewrite passes.
        if let SqlQuery::CreateView(cvq) = q {
            use nom_sql::SelectSpecification;
            let name = cvq.name.clone();
            match *cvq.definition {
                SelectSpecification::Compound(csq) => {
                    return self.nodes_for_named_query(
                        SqlQuery::CompoundSelect(csq),
                        name,
                        is_name_required,
                        is_leaf,
                        mig,
                    );
                }
                SelectSpecification::Simple(sq) => {
                    return self.nodes_for_named_query(
                        SqlQuery::Select(sq),
                        name,
                        is_name_required,
                        is_leaf,
                        mig,
                    );
                }
            }
        };

        trace!(query = %q, "pre-rewrite");
        let q = self.rewrite_query(q, &query_name, mig)?;
        trace!(query = %q, "post-rewrite");

        // if this is a selection, we compute its `QueryGraph` and consider the existing ones we
        // hold for reuse or extension
        let qfp = match q {
            SqlQuery::CompoundSelect(csq) => {
                // NOTE(malte): We can't currently reuse complete compound select queries, since
                // our reuse logic operates on `SqlQuery` structures. Their subqueries do get
                // reused, however.
                self.add_compound_query(&query_name, is_name_required, &csq, is_leaf, mig)?
            }
            SqlQuery::Select(sq) => {
                self.add_select_query(&query_name, is_name_required, &sq, is_leaf, mig)?
                    .0
            }
            SqlQuery::CreateTable(stmt) => self.add_base_via_mir(&query_name, stmt, mig)?,
            q => internal!("unhandled query type in recipe: {:?}", q),
        };

        // record info about query
        self.leaf_addresses
            .insert(query_name.clone(), qfp.query_leaf);

        Ok(qfp)
    }

    /// Upgrades the schema version for the
    /// internal [`SqlToMirConverter`].
    pub(super) fn upgrade_version(&mut self) {
        self.mir_converter.upgrade_version();
    }
}

/// Enables incorporation of a textual SQL query into a Soup graph.
trait ToFlowParts {
    /// Turn a SQL query into a set of nodes inserted into the Soup graph managed by
    /// the `SqlIncorporator` in the second argument. The query can optionally be named by the
    /// string in the `Option<SqlIdentifier>` in the third argument.
    fn to_flow_parts(
        &self,
        inc: &mut SqlIncorporator,
        name: Option<SqlIdentifier>,
        mig: &mut Migration<'_>,
    ) -> Result<QueryFlowParts, ReadySetError>;
}

impl<'a> ToFlowParts for &'a String {
    fn to_flow_parts(
        &self,
        inc: &mut SqlIncorporator,
        name: Option<SqlIdentifier>,
        mig: &mut Migration<'_>,
    ) -> Result<QueryFlowParts, ReadySetError> {
        self.as_str().to_flow_parts(inc, name, mig)
    }
}

impl<'a> ToFlowParts for &'a str {
    fn to_flow_parts(
        &self,
        inc: &mut SqlIncorporator,
        name: Option<SqlIdentifier>,
        mig: &mut Migration<'_>,
    ) -> Result<QueryFlowParts, ReadySetError> {
        // try parsing the incoming SQL
        let parsed_query = sql_parser::parse_query(CANONICAL_DIALECT, self);

        // if ok, manufacture a node for the query structure we got
        match parsed_query {
            Ok(q) => inc.add_parsed_query(q, name, true, mig),
            Err(_) => Err(ReadySetError::UnparseableQuery {
                query: String::from(*self),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use dataflow::prelude::*;
    use nom_sql::{Column, Dialect, SqlIdentifier};

    use super::{SqlIncorporator, ToFlowParts};
    use crate::controller::Migration;
    use crate::integration_utils;

    /// Helper to grab a reference to a named view.
    fn get_node<'a>(inc: &SqlIncorporator, mig: &'a Migration<'_>, name: &str) -> &'a Node {
        let na = inc
            .get_flow_node_address(&SqlIdentifier::from(name), 0)
            .unwrap_or_else(|| panic!("No node named \"{}\" exists", name));
        mig.graph().node_weight(na).unwrap()
    }

    fn get_reader<'a>(inc: &SqlIncorporator, mig: &'a Migration<'_>, name: &str) -> &'a Node {
        let na = inc
            .get_flow_node_address(&SqlIdentifier::from(name), 0)
            .unwrap_or_else(|| panic!("No node named \"{}\" exists", name));
        let children: Vec<_> = mig
            .graph()
            .neighbors_directed(na, petgraph::EdgeDirection::Outgoing)
            .collect();
        assert_eq!(children.len(), 1);
        mig.graph().node_weight(children[0]).unwrap()
    }

    /// Helper to compute a query ID hash via the same method as in `QueryGraph::signature()`.
    /// Note that the argument slices must be ordered in the same way as &str and &Column are
    /// ordered by `Ord`.
    fn query_id_hash(relations: &[&str], attrs: &[&Column], columns: &[&Column]) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        use crate::controller::sql::query_graph::OutputColumn;

        let mut hasher = DefaultHasher::new();
        let mut r_vec: Vec<&str> = relations.to_vec();
        r_vec.sort_unstable(); // QueryGraph.signature() sorts them, so we must to match
        for r in &r_vec {
            r.hash(&mut hasher);
        }
        let mut a_vec: Vec<&Column> = attrs.to_vec();
        a_vec.sort(); // QueryGraph.signature() sorts them, so we must to match
        for a in &a_vec {
            a.hash(&mut hasher);
        }
        for c in columns.iter() {
            OutputColumn::Data {
                alias: c.name.clone(),
                column: (*c).clone(),
            }
            .hash(&mut hasher);
        }
        hasher.finish()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn it_parses() {
        // set up graph
        let mut g = integration_utils::start_simple("it_parses").await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            // Must have a base node for type inference to work, so make one manually
            assert!("CREATE TABLE users (id int, name varchar(40));"
                .to_flow_parts(&mut inc, None, mig)
                .is_ok());

            // Should have two nodes: source and "users" base table
            let ncount = mig.graph().node_count();
            assert_eq!(ncount, 2);
            assert_eq!(get_node(&inc, mig, "users").name(), "users");

            assert!("SELECT users.id from users;"
                .to_flow_parts(&mut inc, None, mig)
                .is_ok());
            // Should now have source, "users", a leaf projection node for the new selection, and
            // a reader node
            assert_eq!(mig.graph().node_count(), ncount + 2);

            // Invalid query should fail parsing and add no nodes
            assert!("foo bar from whatever;"
                .to_flow_parts(&mut inc, None, mig)
                .is_err());
            // Should still only have source, "users" and the two nodes for the above selection
            assert_eq!(mig.graph().node_count(), ncount + 2);
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn it_parses_parameter_column() {
        // set up graph
        let mut g = integration_utils::start_simple("it_parses_parameter_column").await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            assert!(inc
                .add_query(
                    "CREATE TABLE users (id int, name varchar(40), age int);",
                    None,
                    mig
                )
                .is_ok());

            // Add a new query with a parameter
            let res = inc.add_query(
                "SELECT id, name FROM users WHERE users.name = ?;",
                None,
                mig,
            );
            assert!(res.is_ok());
            let qfp = res.unwrap();
            assert_eq!(qfp.new_nodes.len(), 1);
            let node = get_node(&inc, mig, &qfp.name);
            // fields should be projected correctly in query order
            assert_eq!(node.fields(), &["id", "name"]);
            assert_eq!(node.description(true), "π[0, 1]");
            // reader key column should be correct
            let n = get_reader(&inc, mig, &qfp.name);
            assert_eq!(n.as_reader().unwrap().key().unwrap(), &[1]);
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn it_parses_unprojected_parameter_column() {
        // set up graph
        let mut g = integration_utils::start_simple("it_parses_unprojected_parameter_column").await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            assert!(inc
                .add_query(
                    "CREATE TABLE users (id int, name varchar(40), age int);",
                    None,
                    mig
                )
                .is_ok());

            // Add a new query with a parameter
            let res = inc.add_query("SELECT id FROM users WHERE users.name = ?;", None, mig);
            assert!(res.is_ok());
            let qfp = res.unwrap();
            assert_eq!(qfp.new_nodes.len(), 1);
            let node = get_node(&inc, mig, &qfp.name);
            // fields should be projected correctly in query order, with the
            // absent parameter column included
            assert_eq!(node.fields(), &["id", "name"]);
            assert_eq!(node.description(true), "π[0, 1]");
            // reader key column should be correct
            let n = get_reader(&inc, mig, &qfp.name);
            assert_eq!(n.as_reader().unwrap().key().unwrap(), &[1])
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn it_parses_filter_and_parameter_column() {
        // set up graph
        let mut g = integration_utils::start_simple("it_parses_filter_and_parameter_column").await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            assert!(inc
                .add_query(
                    "CREATE TABLE users (id int, name varchar(40), age int);",
                    None,
                    mig
                )
                .is_ok());

            // Add a new query with a parameter
            let res = inc.add_query(
                "SELECT id, name FROM users WHERE users.age > 20 AND users.name = ?;",
                None,
                mig,
            );
            assert!(res.is_ok());
            let qfp = res.unwrap();
            assert_eq!(qfp.new_nodes.len(), 2);

            // Check filter node
            let qid = query_id_hash(
                &["users"],
                &[&Column::from("users.age")],
                &[&Column::from("users.id"), &Column::from("users.name")],
            );
            let filter = get_node(&inc, mig, &format!("q_{:x}_n0_p0_f0", qid));
            assert_eq!(filter.description(true), "σ[(2 > (lit: 20))]");

            // Check projection node
            let projection = get_node(&inc, mig, &qfp.name);
            // fields should be projected correctly in query order
            assert_eq!(projection.fields(), &["id", "name"]);
            assert_eq!(projection.description(true), "π[0, 1]");

            // TODO Check that the filter and projection nodes are ordered properly.
            // println!("graph: {:?}", mig.graph());

            // Check reader
            let n = get_reader(&inc, mig, &qfp.name);
            assert_eq!(n.as_reader().unwrap().key().unwrap(), &[1]);
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn it_incorporates_simple_join() {
        // set up graph
        let mut g = integration_utils::start_simple("it_incorporates_simple_join").await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            // Establish a base write type for "users"
            assert!(inc
                .add_query("CREATE TABLE users (id int, name varchar(40));", None, mig)
                .is_ok());
            // Should have source and "users" base table node
            assert_eq!(mig.graph().node_count(), 2);
            assert_eq!(get_node(&inc, mig, "users").name(), "users");
            assert_eq!(get_node(&inc, mig, "users").fields(), &["id", "name"]);
            assert!(get_node(&inc, mig, "users").is_base());

            // Establish a base write type for "articles"
            assert!(inc
                .add_query(
                    "CREATE TABLE articles (id int, author int, title varchar(255));",
                    None,
                    mig
                )
                .is_ok());
            // Should have source and "users" base table node
            assert_eq!(mig.graph().node_count(), 3);
            assert_eq!(get_node(&inc, mig, "articles").name(), "articles");
            assert_eq!(
                get_node(&inc, mig, "articles").fields(),
                &["id", "author", "title"]
            );
            assert!(get_node(&inc, mig, "articles").is_base());

            // Try a simple equi-JOIN query
            let q = "SELECT users.name, articles.title \
                     FROM articles, users \
                     WHERE users.id = articles.author;";
            let q = inc.add_query(q, None, mig);
            assert!(q.is_ok());
            let qid = query_id_hash(
                &["articles", "users"],
                &[&Column::from("articles.author"), &Column::from("users.id")],
                &[&Column::from("users.name"), &Column::from("articles.title")],
            );
            // join node
            let new_join_view = get_node(&inc, mig, &format!("q_{:x}_n0", qid));
            assert_eq!(new_join_view.fields(), &["id", "author", "title", "name"]);
            // leaf node
            let new_leaf_view = get_node(&inc, mig, &q.unwrap().name);
            assert_eq!(new_leaf_view.fields(), &["name", "title", "bogokey"]);
            assert_eq!(new_leaf_view.description(true), "π[3, 2, lit: 0]");
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn it_incorporates_simple_selection() {
        // set up graph
        let mut g = integration_utils::start_simple("it_incorporates_simple_selection").await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            // Establish a base write type
            assert!(inc
                .add_query("CREATE TABLE users (id int, name varchar(40));", None, mig)
                .is_ok());
            // Should have source and "users" base table node
            assert_eq!(mig.graph().node_count(), 2);
            assert_eq!(get_node(&inc, mig, "users").name(), "users");
            assert_eq!(get_node(&inc, mig, "users").fields(), &["id", "name"]);
            assert!(get_node(&inc, mig, "users").is_base());

            // Try a simple query
            let res = inc.add_query(
                "SELECT users.name FROM users WHERE users.id = 42;",
                None,
                mig,
            );
            assert!(res.is_ok(), "{}", res.err().unwrap());

            let qid = query_id_hash(
                &["users"],
                &[&Column::from("users.id")],
                &[&Column::from("users.name")],
            );
            // filter node
            let filter = get_node(&inc, mig, &format!("q_{:x}_n0_p0_f0", qid));
            assert_eq!(filter.fields(), &["id", "name"]);
            assert_eq!(filter.description(true), "σ[(0 = (lit: 42))]");
            // leaf view node
            let edge = get_node(&inc, mig, &res.unwrap().name);
            assert_eq!(edge.fields(), &["name", "bogokey"]);
            assert_eq!(edge.description(true), "π[1, lit: 0]");
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn it_incorporates_aggregation() {
        // set up graph
        let mut g = integration_utils::start_simple("it_incorporates_aggregation").await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            // Establish a base write types
            assert!(inc
                .add_query("CREATE TABLE votes (aid int, userid int);", None, mig)
                .is_ok());
            // Should have source and "users" base table node
            assert_eq!(mig.graph().node_count(), 2);
            assert_eq!(get_node(&inc, mig, "votes").name(), "votes");
            assert_eq!(get_node(&inc, mig, "votes").fields(), &["aid", "userid"]);
            assert!(get_node(&inc, mig, "votes").is_base());

            // Try a simple COUNT function
            let res = inc.add_query(
                "SELECT COUNT(votes.userid) AS votes \
                 FROM votes GROUP BY votes.aid;",
                None,
                mig,
            );
            assert!(res.is_ok());
            // added the aggregation and the edge view, and a reader
            assert_eq!(mig.graph().node_count(), 5);
            // check aggregation view
            let qid = query_id_hash(
                &["votes"],
                &[&Column::from("votes.aid")],
                &[&Column {
                    name: "votes".into(),
                    table: None,
                }],
            );
            let agg_view = get_node(&inc, mig, &format!("q_{:x}_n0", qid));
            assert_eq!(agg_view.fields(), &["aid", "votes"]);
            assert_eq!(agg_view.description(true), "|*| γ[0]");
            // check edge view
            let edge_view = get_node(&inc, mig, &res.unwrap().name);
            assert_eq!(edge_view.fields(), &["votes", "bogokey"]);
            assert_eq!(edge_view.description(true), "π[1, lit: 0]");
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn it_does_not_reuse_if_disabled() {
        // set up graph
        let mut g = integration_utils::start_simple("it_does_not_reuse_if_disabled").await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            inc.disable_reuse();
            assert!(inc
                .add_query("CREATE TABLE users (id int, name varchar(40));", None, mig)
                .is_ok());
            let res = inc.add_query("SELECT id, name FROM users WHERE users.id = 42;", None, mig);
            assert!(res.is_ok());
            let leaf = res.unwrap().query_leaf;

            // Add the same query again; this should NOT reuse here.
            let ncount = mig.graph().node_count();
            let res = inc.add_query("SELECT name, id FROM users WHERE users.id = 42;", None, mig);
            assert!(res.is_ok());
            // should have added nodes for this query, too
            let qfp = res.unwrap();
            assert_eq!(qfp.new_nodes.len(), 2);
            // expect three new nodes: filter, project, reader
            assert_eq!(mig.graph().node_count(), ncount + 3);
            // should have ended up with a different leaf node
            assert_ne!(qfp.query_leaf, leaf);
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn it_reuses_identical_query() {
        // set up graph
        let mut g = integration_utils::start_simple("it_reuses_identical_query").await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            inc.enable_reuse(crate::ReuseConfigType::Finkelstein);
            // Establish a base write type
            assert!(inc
                .add_query("CREATE TABLE users (id int, name varchar(40));", None, mig)
                .is_ok());
            // Should have source and "users" base table node
            assert_eq!(mig.graph().node_count(), 2);
            assert_eq!(get_node(&inc, mig, "users").name(), "users");
            assert_eq!(get_node(&inc, mig, "users").fields(), &["id", "name"]);
            assert!(get_node(&inc, mig, "users").is_base());

            // Add a new query
            let res = inc.add_query("SELECT id, name FROM users WHERE users.id = 42;", None, mig);
            let leaf = res.unwrap().query_leaf;

            // Add the same query again
            let ncount = mig.graph().node_count();
            let res = inc.add_query("SELECT id, name FROM users WHERE users.id = 42;", None, mig);
            // should have added no more nodes
            let qfp = res.unwrap();
            assert_eq!(qfp.new_nodes, vec![]);
            assert_eq!(mig.graph().node_count(), ncount);
            // should have ended up with the same leaf node
            assert_eq!(qfp.query_leaf, leaf);

            // Add the same query again, but project columns in a different order
            let ncount = mig.graph().node_count();
            let res = inc.add_query("SELECT name, id FROM users WHERE users.id = 42;", None, mig);
            assert!(res.is_ok());
            // should have added two more nodes (project and reader)
            let qfp = res.unwrap();
            assert_eq!(mig.graph().node_count(), ncount + 2);
            // should NOT have ended up with the same leaf node
            assert_ne!(qfp.query_leaf, leaf);
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn it_reuses_with_different_parameter() {
        // set up graph
        let mut g = integration_utils::start_simple("it_reuses_with_different_parameter").await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            // Establish a base write type
            assert!(inc
                .add_query(
                    "CREATE TABLE users (id int, name varchar(40), address varchar(40));",
                    None,
                    mig
                )
                .is_ok());
            // Should have source and "users" base table node
            assert_eq!(mig.graph().node_count(), 2);
            assert_eq!(get_node(&inc, mig, "users").name(), "users");
            assert_eq!(
                get_node(&inc, mig, "users").fields(),
                &["id", "name", "address"]
            );
            assert!(get_node(&inc, mig, "users").is_base());

            // Add a new query
            let res = inc.add_query("SELECT id, name FROM users WHERE users.id = ?;", None, mig);
            assert!(res.is_ok());

            // Add the same query again, but with a parameter on a different column.
            // Project the same columns, so we can reuse the projection that already exists and only
            // add an identity node.
            let ncount = mig.graph().node_count();
            let res = inc.add_query(
                "SELECT id, name FROM users WHERE users.name = ?;",
                None,
                mig,
            );
            assert!(res.is_ok());
            // should have added two more nodes: one identity node and one reader node
            let qfp = res.unwrap();
            assert_eq!(mig.graph().node_count(), ncount + 2);
            // only the identity node is returned in the vector of new nodes
            assert_eq!(qfp.new_nodes.len(), 1);
            assert_eq!(get_node(&inc, mig, &qfp.name).description(true), "≡");
            // we should be based off the identity as our leaf
            let id_node = qfp.new_nodes.get(0).unwrap();
            assert_eq!(qfp.query_leaf, *id_node);

            // Do it again with a parameter on yet a different column.
            // Project different columns, so we need to add a new projection (not an identity).
            let ncount = mig.graph().node_count();
            let res = inc.add_query(
                "SELECT id, name FROM users WHERE users.address = ?;",
                None,
                mig,
            );
            assert!(res.is_ok());
            // should have added two more nodes: one projection node and one reader node
            let qfp = res.unwrap();
            assert_eq!(mig.graph().node_count(), ncount + 2);
            // only the projection node is returned in the vector of new nodes
            assert_eq!(qfp.new_nodes.len(), 1);
            assert_eq!(
                get_node(&inc, mig, &qfp.name).description(true),
                "π[0, 1, 2]"
            );
            // we should be based off the new projection as our leaf
            let id_node = qfp.new_nodes.get(0).unwrap();
            assert_eq!(qfp.query_leaf, *id_node);
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn it_does_not_reuse_bogokey_projection_for_different_projection() {
        // set up graph
        let mut g = integration_utils::start_simple(
            "it_does_not_reuse_bogokey_projection_with_different_projection",
        )
        .await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            // Establish a base write type
            assert!(inc
                .add_query(
                    "CREATE TABLE users (id int, name varchar(40), address varchar(40));",
                    None,
                    mig
                )
                .is_ok());
            let base_address = inc.get_flow_node_address(&"users".into(), 0).unwrap();

            // Add a new "full table" query. The view is expected to contain projected columns plus
            // the special 'bogokey' literal column.
            let res = inc.add_query("SELECT id, name FROM users;", None, mig);
            assert!(res.is_ok());
            let qfp = res.unwrap();
            // Check projection
            let projection = get_node(&inc, mig, &qfp.name);
            assert_eq!(projection.fields(), &["id", "name", "bogokey"]);
            assert_eq!(projection.description(true), "π[0, 1, lit: 0]");
            let leaf = qfp.query_leaf;
            // Check reader column
            let n = get_reader(&inc, mig, &qfp.name);
            assert_eq!(n.as_reader().unwrap().key().unwrap(), &[2]);

            // Add the name query again, but with a parameter and project columns in a different
            // order.
            let ncount = mig.graph().node_count();
            let res = inc.add_query(
                "SELECT name, id FROM users WHERE users.name = ?;",
                None,
                mig,
            );
            assert!(res.is_ok());
            let qfp = res.unwrap();
            // Check projection
            let projection = get_node(&inc, mig, &qfp.name);
            assert_eq!(projection.fields(), &["name", "id"]);
            assert_eq!(projection.description(true), "π[1, 0]");
            // should have added two more nodes (project and reader)
            assert_eq!(mig.graph().node_count(), ncount + 2);
            // should NOT have ended up with the same leaf node
            assert_ne!(qfp.query_leaf, leaf);
            // Check reader column
            let n = get_reader(&inc, mig, &qfp.name);
            assert_eq!(n.as_reader().unwrap().key().unwrap(), &[0]);

            // Check that the parent of the projection is the base table, and NOT the earlier
            // bogokey projection.
            let top_node = mig.graph().node_weight(qfp.new_nodes[0]).unwrap();
            assert_eq!(top_node.ancestors().unwrap(), [base_address]);

            // Add a query with a parameter on a new field
            let ncount = mig.graph().node_count();
            let res = inc.add_query(
                "SELECT id, name FROM users WHERE users.address = ?;",
                None,
                mig,
            );
            assert!(res.is_ok());
            let qfp = res.unwrap();
            // Check projection
            let projection = get_node(&inc, mig, &qfp.name);
            assert_eq!(projection.fields(), &["id", "name", "address"]);
            assert_eq!(projection.description(true), "π[0, 1, 2]");
            // should have added two more nodes (project and reader)
            assert_eq!(mig.graph().node_count(), ncount + 2);
            // should NOT have ended up with the same leaf node
            assert_ne!(qfp.query_leaf, leaf);
            // Check reader column
            let n = get_reader(&inc, mig, &qfp.name);
            assert_eq!(n.as_reader().unwrap().key().unwrap(), &[2]);
            // Check that the parent of the projection is the base table, and NOT the earlier
            // bogokey projection.
            let top_node = mig.graph().node_weight(qfp.new_nodes[0]).unwrap();
            assert_eq!(top_node.ancestors().unwrap(), [base_address]);
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn it_does_not_reuse_parameter_projection_for_bogokey_projection() {
        // set up graph
        let mut g = integration_utils::start_simple(
            "it_does_not_reuse_parameter_projection_with_bogokey_projection",
        )
        .await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            // Establish a base write type
            assert!(inc
                .add_query(
                    "CREATE TABLE users (id int, name varchar(40), address varchar(40));",
                    None,
                    mig
                )
                .is_ok());
            let base_address = inc.get_flow_node_address(&"users".into(), 0).unwrap();

            // Add a new parameterized query.
            let res = inc.add_query("SELECT id, name FROM users WHERE users.id = ?;", None, mig);
            assert!(res.is_ok());
            let qfp = res.unwrap();
            // Check projection
            let projection = get_node(&inc, mig, &qfp.name);
            assert_eq!(projection.fields(), &["id", "name"]);
            assert_eq!(projection.description(true), "π[0, 1]");
            // Check reader column
            let n = get_reader(&inc, mig, &qfp.name);
            assert_eq!(n.as_reader().unwrap().key().unwrap(), &[0]);

            // Add a new "full table" query. The view is expected to contain projected columns plus
            // the special 'bogokey' literal column.
            let res = inc.add_query("SELECT id, name FROM users;", None, mig);
            assert!(res.is_ok());
            let qfp = res.unwrap();
            // Check projection
            let projection = get_node(&inc, mig, &qfp.name);
            assert_eq!(projection.fields(), &["id", "name", "bogokey"]);
            assert_eq!(projection.description(true), "π[0, 1, lit: 0]");
            // Check reader column
            let n = get_reader(&inc, mig, &qfp.name);
            assert_eq!(n.as_reader().unwrap().key().unwrap(), &[2]);

            // Check that the parent of the projection node is the base table, and NOT the earlier
            // parameterized projection.
            let top_node = mig.graph().node_weight(qfp.new_nodes[0]).unwrap();
            assert_eq!(top_node.ancestors().unwrap(), [base_address]);
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn it_reuses_projection_for_non_bogokey_table_query() {
        use super::sql_parser;
        // set up graph
        let mut g = integration_utils::start_simple(
            "it_does_not_reuse_bogokey_projection_with_different_projection",
        )
        .await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            // Establish a base write type
            assert!(inc
                .add_query(
                    "CREATE TABLE users (id int, name varchar(40), address varchar(40));",
                    None,
                    mig
                )
                .is_ok());

            // Add a new parameterized query.
            let res = inc.add_query("SELECT id, name FROM users WHERE users.id = ?;", None, mig);
            assert!(res.is_ok());
            let qfp = res.unwrap();
            let leaf = qfp.query_leaf;
            let param_address = inc.get_flow_node_address(&qfp.name, 0).unwrap();
            // Check projection
            let projection = get_node(&inc, mig, &qfp.name);
            assert_eq!(projection.fields(), &["id", "name"]);
            assert_eq!(projection.description(true), "π[0, 1]");
            // Check reader column
            let n = get_reader(&inc, mig, &qfp.name);
            assert_eq!(n.as_reader().unwrap().key().unwrap(), &[0]);

            // Add a new "full table" query as a non leaf query. The view does not contain a
            // 'bogokey' literal column because it is for a non leaf query.
            let res = inc.add_parsed_query(
                sql_parser::parse_query(Dialect::MySQL, "SELECT id, name FROM users;").unwrap(),
                Some("short_users".into()),
                false,
                mig,
            );
            assert!(res.is_ok());
            let qfp = res.unwrap();
            // Check projection
            let identity = get_node(&inc, mig, &qfp.name);
            assert_eq!(identity.fields(), &["id", "name"]);
            assert_eq!(identity.description(true), "≡");
            // should NOT have ended up with the same leaf node
            assert_ne!(qfp.query_leaf, leaf);
            // Check that the parent of the identity is the paramaterized query.
            let top_node = mig.graph().node_weight(qfp.new_nodes[0]).unwrap();
            assert_eq!(top_node.ancestors().unwrap(), [param_address]);
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    // note: [Ignored HAVING tests]
    // A previous version of this test (and `it_doesnt_merge_sum_and_filter_on_sum_result`) was
    // incorrectly using SQL - it used a WHERE clause to filter on the result of an aggregate, which
    // isn't allowed in sql (you have to use HAVING). These tests have been updated to use HAVING
    // after moving aggregates above filters, but we don't support HAVING yet! so they're ignored.
    // See https://app.clubhouse.io/readysettech/story/425
    #[ignore]
    async fn it_reuses_by_extending_existing_query() {
        use super::sql_parser;
        // set up graph
        let mut g = integration_utils::start_simple("it_reuses_by_extending_existing_query").await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            // Add base tables
            assert!(inc
                .add_query(
                    "CREATE TABLE articles (id int, title varchar(40));",
                    None,
                    mig
                )
                .is_ok());
            assert!(inc
                .add_query("CREATE TABLE votes (aid int, uid int);", None, mig)
                .is_ok());
            // Should have source, "articles" and "votes" base tables
            assert_eq!(mig.graph().node_count(), 3);

            // Add a new query
            let res = inc.add_parsed_query(
                sql_parser::parse_query(
                    Dialect::MySQL,
                    "SELECT COUNT(uid) AS vc FROM votes GROUP BY aid;",
                )
                .unwrap(),
                Some("votecount".into()),
                false,
                mig,
            );
            assert!(res.is_ok());

            // Add a query that can reuse votecount by extending it.
            let ncount = mig.graph().node_count();
            let res = inc.add_parsed_query(
                sql_parser::parse_query(
                    Dialect::MySQL,
                    "SELECT COUNT(uid) AS vc FROM votes GROUP BY aid HAVING vc > 5;",
                )
                .unwrap(),
                Some("highvotes".into()),
                true,
                mig,
            );
            assert!(res.is_ok());
            // should have added three more nodes: a join, a projection, and a reader
            let qfp = res.unwrap();
            assert_eq!(mig.graph().node_count(), ncount + 3);
            // only the join and projection nodes are returned in the vector of new nodes
            assert_eq!(qfp.new_nodes.len(), 2);
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn it_does_not_reuse_ancestor_lacking_parent_key() {
        // set up graph
        let mut g =
            integration_utils::start_simple("it_does_not_reuse_ancestor_lacking_parent_key").await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            // Establish a base write type
            assert!(inc
                .add_query(
                    "CREATE TABLE users (id int, name varchar(40), address varchar(40));",
                    None,
                    mig
                )
                .is_ok());
            let base_address = inc.get_flow_node_address(&"users".into(), 0).unwrap();

            // Add a query with a parameter and a literal projection.
            let res = inc.add_query(
                "SELECT id, name, 1 as one FROM users WHERE id = ?;",
                None,
                mig,
            );
            assert!(res.is_ok());
            let qfp = res.unwrap();
            // Check projection
            let projection = get_node(&inc, mig, &qfp.name);
            assert_eq!(projection.fields(), &["id", "name", "one"]);
            assert_eq!(projection.description(true), "π[0, 1, lit: 1]");
            let leaf = qfp.query_leaf;
            // Check reader column
            let n = get_reader(&inc, mig, &qfp.name);
            assert_eq!(n.as_reader().unwrap().key().unwrap(), &[0]);

            // Add a query with the same literal projection but a different parameter from the base
            // table.
            let ncount = mig.graph().node_count();
            let res = inc.add_query(
                "SELECT id, name, 1 as one FROM users WHERE address = ?;",
                None,
                mig,
            );
            assert!(res.is_ok());
            let qfp = res.unwrap();
            // Check projection
            let projection = get_node(&inc, mig, &qfp.name);
            assert_eq!(projection.fields(), &["id", "name", "address", "one"]);
            assert_eq!(projection.description(true), "π[0, 1, 2, lit: 1]");
            // should have added two more nodes (identity and reader)
            assert_eq!(mig.graph().node_count(), ncount + 2);
            // should NOT have ended up with the same leaf node
            assert_ne!(qfp.query_leaf, leaf);
            // Check reader column
            let n = get_reader(&inc, mig, &qfp.name);
            assert_eq!(n.as_reader().unwrap().key().unwrap(), &[2]);
            // Check that the parent of the projection is the original table, NOT the earlier
            // projection.
            let top_node = mig.graph().node_weight(qfp.new_nodes[0]).unwrap();
            assert_eq!(top_node.ancestors().unwrap(), [base_address]);
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn it_incorporates_aggregation_no_group_by() {
        // set up graph
        let mut g =
            integration_utils::start_simple("it_incorporates_aggregation_no_group_by").await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            // Establish a base write type
            assert!(inc
                .add_query("CREATE TABLE votes (aid int, userid int);", None, mig)
                .is_ok());
            // Should have source and "users" base table node
            assert_eq!(mig.graph().node_count(), 2);
            assert_eq!(get_node(&inc, mig, "votes").name(), "votes");
            assert_eq!(get_node(&inc, mig, "votes").fields(), &["aid", "userid"]);
            assert!(get_node(&inc, mig, "votes").is_base());
            // Try a simple COUNT function without a GROUP BY clause
            let res = inc.add_query("SELECT COUNT(votes.userid) AS count FROM votes;", None, mig);
            assert!(res.is_ok());
            // added the aggregation, a project helper, the edge view, and reader
            assert_eq!(mig.graph().node_count(), 6);
            // check project helper node
            let qid = query_id_hash(
                &["votes"],
                &[],
                &[&Column {
                    name: "count".into(),
                    table: None,
                }],
            );
            let proj_helper_view = get_node(&inc, mig, &format!("q_{:x}_n0_prj_hlpr", qid));
            assert_eq!(proj_helper_view.fields(), &["userid", "grp"]);
            assert_eq!(proj_helper_view.description(true), "π[1, lit: 0]");
            // check aggregation view
            let agg_view = get_node(&inc, mig, &format!("q_{:x}_n0", qid));
            assert_eq!(agg_view.fields(), &["grp", "count"]);
            assert_eq!(agg_view.description(true), "|*| γ[1]");
            // check edge view -- note that it's not actually currently possible to read from
            // this for a lack of key (the value would be the key). Hence, the view also has a
            // bogokey column.
            let edge_view = get_node(&inc, mig, &res.unwrap().name);
            assert_eq!(edge_view.fields(), &["count", "bogokey"]);
            assert_eq!(edge_view.description(true), "π[1, lit: 0]");
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn it_incorporates_aggregation_count_star() {
        // set up graph
        let mut g = integration_utils::start_simple("it_incorporates_aggregation_count_star").await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            // Establish a base write type
            assert!(inc
                .add_query("CREATE TABLE votes (userid int, aid int);", None, mig)
                .is_ok());
            // Should have source and "users" base table node
            assert_eq!(mig.graph().node_count(), 2);
            assert_eq!(get_node(&inc, mig, "votes").name(), "votes");
            assert_eq!(get_node(&inc, mig, "votes").fields(), &["userid", "aid"]);
            assert!(get_node(&inc, mig, "votes").is_base());
            // Try a simple COUNT function
            let res = inc.add_query(
                "SELECT COUNT(*) AS count FROM votes GROUP BY votes.userid;",
                None,
                mig,
            );
            assert!(res.is_ok());
            // added the aggregation, a project helper, the edge view, and reader
            assert_eq!(mig.graph().node_count(), 5);
            // check aggregation view
            let qid = query_id_hash(
                &["votes"],
                &[&Column::from("votes.userid")],
                &[&Column {
                    name: "count".into(),
                    table: None,
                }],
            );
            let agg_view = get_node(&inc, mig, &format!("q_{:x}_n0", qid));
            assert_eq!(agg_view.fields(), &["userid", "count"]);
            assert_eq!(agg_view.description(true), "|*| γ[0]");
            // check edge view -- note that it's not actually currently possible to read from
            // this for a lack of key (the value would be the key). Hence, the view also has a
            // bogokey column.
            let edge_view = get_node(&inc, mig, &res.unwrap().name);
            assert_eq!(edge_view.fields(), &["count", "bogokey"]);
            assert_eq!(edge_view.description(true), "π[1, lit: 0]");
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn it_incorporates_aggregation_filter_count() {
        // set up graph
        let mut g =
            integration_utils::start_simple("it_incorporates_aggregation_filter_count").await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            // Establish a base write type
            assert!(inc
                .add_query("CREATE TABLE votes (userid int, aid int);", None, mig)
                .is_ok());
            // Should have source and "users" base table node
            assert_eq!(mig.graph().node_count(), 2);
            assert_eq!(get_node(&inc, mig, "votes").name(), "votes");
            assert_eq!(get_node(&inc, mig, "votes").fields(), &["userid", "aid"]);
            assert!(get_node(&inc, mig, "votes").is_base());
            // Try a simple COUNT function
            let res = inc.add_query(
                "SELECT COUNT(CASE WHEN aid = 5 THEN aid END) AS count FROM votes GROUP BY votes.userid;",
                None,
                mig,
            );
            assert!(res.is_ok());
            // added a project for the case, the aggregation, a project helper, the edge view, and
            // reader
            assert_eq!(mig.graph().node_count(), 6);
            // check aggregation view
            let qid = query_id_hash(
                &["votes"],
                &[&Column::from("votes.userid")],
                &[&Column {
                    name: "count".into(),
                    table: None,
                }],
            );
            let agg_view = get_node(&inc, mig, &format!("q_{:x}_n1", qid));
            assert_eq!(agg_view.fields(), &["userid", "count"]);
            assert_eq!(agg_view.description(true), "|*| γ[0]");
            // check edge view -- note that it's not actually currently possible to read from
            // this for a lack of key (the value would be the key). Hence, the view also has a
            // bogokey column.
            let edge_view = get_node(&inc, mig, &res.unwrap().name);
            assert_eq!(edge_view.fields(), &["count", "bogokey"]);
            assert_eq!(edge_view.description(true), "π[1, lit: 0]");
        })
            .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn it_incorporates_aggregation_filter_sum() {
        // set up graph
        let mut g = integration_utils::start_simple("it_incorporates_aggregation_filter_sum").await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            // Establish a base write type
            assert!(inc
                .add_query("CREATE TABLE votes (userid int, aid int, sign int);", None, mig)
                .is_ok());
            // Should have source and "users" base table node
            assert_eq!(mig.graph().node_count(), 2);
            assert_eq!(get_node(&inc, mig, "votes").name(), "votes");
            assert_eq!(get_node(&inc, mig, "votes").fields(), &["userid", "aid", "sign"]);
            assert!(get_node(&inc, mig, "votes").is_base());
            // Try a simple COUNT function
            let res = inc.add_query(
                "SELECT SUM(CASE WHEN aid = 5 THEN sign END) AS sum FROM votes GROUP BY votes.userid;",
                None,
                mig,
            );
            assert!(res.is_ok());
            // added a project for the case, the aggregation, a project helper, the edge view, and
            // reader
            assert_eq!(mig.graph().node_count(), 6);
            // check aggregation view
            let qid = query_id_hash(
                &["votes"],
                &[&Column::from("votes.userid")],
                &[&Column {
                    name: "sum".into(),
                    table: None,
                }],
            );
            let agg_view = get_node(&inc, mig, &format!("q_{:x}_n1", qid));
            assert_eq!(agg_view.fields(), &["userid", "sum"]);
            assert_eq!(agg_view.description(true), "𝛴(3) γ[0]");
            // check edge view -- note that it's not actually currently possible to read from
            // this for a lack of key (the value would be the key). Hence, the view also has a
            // bogokey column.
            let edge_view = get_node(&inc, mig, &res.unwrap().name);
            assert_eq!(edge_view.fields(), &["sum", "bogokey"]);
            assert_eq!(edge_view.description(true), "π[1, lit: 0]");
        })
            .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn it_incorporates_aggregation_filter_sum_else() {
        // set up graph
        let mut g =
            integration_utils::start_simple("it_incorporates_aggregation_filter_sum_else").await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            // Establish a base write type
            assert!(inc
                .add_query("CREATE TABLE votes (userid int, aid int, sign int);", None, mig)
                .is_ok());
            // Should have source and "users" base table node
            assert_eq!(mig.graph().node_count(), 2);
            assert_eq!(get_node(&inc, mig, "votes").name(), "votes");
            assert_eq!(get_node(&inc, mig, "votes").fields(), &["userid", "aid", "sign"]);
            assert!(get_node(&inc, mig, "votes").is_base());
            // Try a simple COUNT function
            let res = inc.add_query(
                "SELECT SUM(CASE WHEN aid = 5 THEN sign ELSE 6 END) AS sum FROM votes GROUP BY votes.userid;",
                None,
                mig,
            );
            assert!(res.is_ok());
            // added a project for the case, the aggregation, a project helper, the edge view, and
            // reader
            assert_eq!(mig.graph().node_count(), 6);
            // check aggregation view
            let qid = query_id_hash(
                &["votes"],
                &[&Column::from("votes.userid")],
                &[&Column {
                    name: "sum".into(),
                    table: None,
                }],
            );
            let agg_view = get_node(&inc, mig, &format!("q_{:x}_n1", qid));
            assert_eq!(agg_view.fields(), &["userid", "sum"]);
            assert_eq!(agg_view.description(true), "𝛴(3) γ[0]");
            // check edge view -- note that it's not actually currently possible to read from
            // this for a lack of key (the value would be the key). Hence, the view also has a
            // bogokey column.
            let edge_view = get_node(&inc, mig, &res.unwrap().name);
            assert_eq!(edge_view.fields(), &["sum", "bogokey"]);
            assert_eq!(edge_view.description(true), "π[1, lit: 0]");
        })
            .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore] // find_and_merge_filter_aggregates currently disabled
    async fn it_merges_filter_and_sum() {
        // set up graph
        let mut g = integration_utils::start_simple("it_merges_filter_and_sum").await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            // Establish a base write type
            assert!(inc
                .add_query(
                    "CREATE TABLE votes (userid int, aid int, sign int);",
                    None,
                    mig
                )
                .is_ok());
            // Should have source and "users" base table node
            assert_eq!(mig.graph().node_count(), 2);
            assert_eq!(get_node(&inc, mig, "votes").name(), "votes");
            assert_eq!(
                get_node(&inc, mig, "votes").fields(),
                &["userid", "aid", "sign"]
            );
            assert!(get_node(&inc, mig, "votes").is_base());
            let res = inc.add_query(
                "SELECT SUM(sign) AS sum FROM votes WHERE aid=5 GROUP BY votes.userid;",
                None,
                mig,
            );
            assert!(res.is_ok());
            // note: the FunctionExpression isn't a sumfilter because it takes the hash before
            // merging
            let qid = query_id_hash(
                &["votes"],
                &[&Column::from("votes.userid"), &Column::from("votes.aid")],
                &[&Column {
                    name: "sum".into(),
                    table: None,
                }],
            );

            let agg_view = get_node(&inc, mig, &format!("q_{:x}_n1_p0_f0_filteragg", qid));
            assert_eq!(agg_view.fields(), &["userid", "aid", "sum"]);
            assert_eq!(agg_view.description(true), "𝛴(σ(2)) γ[0, 1]");
            // check edge view -- note that it's not actually currently possible to read from
            // this for a lack of key (the value would be the key). Hence, the view also has a
            // bogokey column.
            let edge_view = get_node(&inc, mig, &res.unwrap().name);
            assert_eq!(edge_view.fields(), &["sum", "bogokey"]);
            assert_eq!(edge_view.description(true), "π[2, lit: 0]");
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore] // find_and_merge_filter_aggregates currently disabled
    async fn it_merges_filter_and_sum_on_filter_column() {
        // set up graph
        let mut g =
            integration_utils::start_simple("it_merges_filter_and_sum_on_filter_column").await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            // Establish a base write type
            assert!(inc
                .add_query(
                    "CREATE TABLE votes (userid int, aid int, sign int);",
                    None,
                    mig
                )
                .is_ok());
            // Should have source and "users" base table node
            assert_eq!(mig.graph().node_count(), 2);
            assert_eq!(get_node(&inc, mig, "votes").name(), "votes");
            assert_eq!(
                get_node(&inc, mig, "votes").fields(),
                &["userid", "aid", "sign"]
            );
            assert!(get_node(&inc, mig, "votes").is_base());
            let res = inc.add_query(
                "SELECT SUM(sign) AS sum FROM votes WHERE sign > 0 GROUP BY votes.userid;",
                None,
                mig,
            );
            assert!(res.is_ok());
            assert_eq!(mig.graph().node_count(), 5);
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    // See note: [Ignored HAVING tests]
    #[ignore]
    async fn it_doesnt_merge_sum_and_filter_on_sum_result() {
        // set up graph
        let mut g =
            integration_utils::start_simple("it_doesnt_merge_sum_and_filter_on_sum_result").await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            // Establish a base write type
            assert!(inc
                .add_query(
                    "CREATE TABLE votes (userid int, aid int, sign int);",
                    None,
                    mig
                )
                .is_ok());
            // Should have source and "users" base table node
            assert_eq!(mig.graph().node_count(), 2);
            assert_eq!(get_node(&inc, mig, "votes").name(), "votes");
            assert_eq!(
                get_node(&inc, mig, "votes").fields(),
                &["userid", "aid", "sign"]
            );
            assert!(get_node(&inc, mig, "votes").is_base());
            let res = inc.add_query(
                "SELECT SUM(sign) AS sum FROM votes GROUP BY votes.userid HAVING sum>0 ;",
                None,
                mig,
            );
            assert!(res.is_ok());
            // added a project for the case, the aggregation, a project helper, the edge view, and
            // reader
            assert_eq!(mig.graph().node_count(), 6);
            // check aggregation view
            let qid = query_id_hash(
                &["votes"],
                &[&Column::from("votes.userid"), &Column::from("sum")],
                &[&Column {
                    name: "sum".into(),
                    table: None,
                }],
            );
            let agg_view = get_node(&inc, mig, &format!("q_{:x}_n0", qid));
            assert_eq!(agg_view.fields(), &["userid", "sum"]);
            assert_eq!(agg_view.description(true), "𝛴(2) γ[0]");
            // check edge view -- note that it's not actually currently possible to read from
            // this for a lack of key (the value would be the key). Hence, the view also has a
            // bogokey column.
            let edge_view = get_node(&inc, mig, &res.unwrap().name);
            assert_eq!(edge_view.fields(), &["sum", "bogokey"]);
            assert_eq!(edge_view.description(true), "π[1, lit: 0]");
        })
        .await;
    }

    // currently, this test will fail because logical operations are unimplemented
    // (in particular, any complex operation that might involve multiple filter conditions
    // is currently unimplemented for filter-aggregations (TODO (jamb)))

    #[tokio::test(flavor = "multi_thread")]
    async fn it_incorporates_aggregation_filter_logical_op() {
        // set up graph
        let mut g =
            integration_utils::start_simple("it_incorporates_aggregation_filter_sum_else").await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            // Establish a base write type
            assert!(inc
                .add_query("CREATE TABLE votes (story_id int, comment_id int, vote int);", None, mig)
                .is_ok());
            // Should have source and "users" base table node
            assert_eq!(mig.graph().node_count(), 2);
            assert_eq!(get_node(&inc, mig, "votes").name(), "votes");
            assert_eq!(get_node(&inc, mig, "votes").fields(), &["story_id", "comment_id", "vote"]);
            assert!(get_node(&inc, mig, "votes").is_base());
            // Try a simple COUNT function
            let res = inc.add_query(
                "SELECT
                COUNT(CASE WHEN votes.story_id IS NULL AND votes.vote = 0 THEN votes.vote END) as votes
                FROM votes
                GROUP BY votes.comment_id;",
                None,
                mig,
            );
            assert!(res.is_ok(), "!{:?}.is_ok()", res);
            // added a project for the case, the aggregation, a project helper, the edge view, and
            // reader
            assert_eq!(mig.graph().node_count(), 6);
            // check aggregation view
            let qid = query_id_hash(
                &["votes"],
                &[&Column::from("votes.comment_id")],
                &[&Column {
                    name: "votes".into(),
                    table: None,
                }],
            );
            let agg_view = get_node(&inc, mig, &format!("q_{:x}_n1", qid));
            assert_eq!(agg_view.fields(), &["comment_id", "votes"]);
            assert_eq!(agg_view.description(true), "|*| γ[1]");
            // check edge view -- note that it's not actually currently possible to read from
            // this for a lack of key (the value would be the key). Hence, the view also has a
            // bogokey column.
            let edge_view = get_node(&inc, mig, &res.unwrap().name);
            assert_eq!(edge_view.fields(), &["votes", "bogokey"]);
            assert_eq!(edge_view.description(true), "π[1, lit: 0]");
        })
            .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn it_incorporates_explicit_multi_join() {
        // set up graph
        let mut g = integration_utils::start_simple("it_incorporates_explicit_multi_join").await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            // Establish base write types for "users" and "articles" and "votes"
            assert!(inc
                .add_query("CREATE TABLE users (id int, name varchar(40));", None, mig)
                .is_ok());
            assert!(inc
                .add_query("CREATE TABLE votes (aid int, uid int);", None, mig)
                .is_ok());
            assert!(inc
                .add_query(
                    "CREATE TABLE articles (aid int, title varchar(255), author int);",
                    None,
                    mig
                )
                .is_ok());

            // Try an explicit multi-way-join
            let q = "SELECT users.name, articles.title, votes.uid \
                 FROM articles
                 JOIN users ON (users.id = articles.author) \
                 JOIN votes ON (votes.aid = articles.aid);";

            let q = inc.add_query(q, None, mig);
            assert!(q.is_ok());
            let _qid = query_id_hash(
                &["articles", "users", "votes"],
                &[
                    &Column::from("articles.aid"),
                    &Column::from("articles.author"),
                    &Column::from("users.id"),
                    &Column::from("votes.aid"),
                ],
                &[
                    &Column::from("users.name"),
                    &Column::from("articles.title"),
                    &Column::from("votes.uid"),
                ],
            );
            // XXX(malte): non-deterministic join ordering make it difficult to assert on the join
            // views
            // leaf view
            let leaf_view = get_node(&inc, mig, "q_3");
            assert_eq!(leaf_view.fields(), &["name", "title", "uid", "bogokey"]);
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn it_incorporates_implicit_multi_join() {
        // set up graph
        let mut g = integration_utils::start_simple("it_incorporates_implicit_multi_join").await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            // Establish base write types for "users" and "articles" and "votes"
            assert!(inc
                .add_query("CREATE TABLE users (id int, name varchar(40));", None, mig)
                .is_ok());
            assert!(inc
                .add_query("CREATE TABLE votes (aid int, uid int);", None, mig)
                .is_ok());
            assert!(inc
                .add_query(
                    "CREATE TABLE articles (aid int, title varchar(255), author int);",
                    None,
                    mig
                )
                .is_ok());

            // Try an implicit multi-way-join
            let q = "SELECT users.name, articles.title, votes.uid \
                 FROM articles, users, votes
                 WHERE users.id = articles.author \
                 AND votes.aid = articles.aid;";

            let q = inc.add_query(q, None, mig);
            assert!(q.is_ok());
            // XXX(malte): below over-projects into the final leaf, and is thus inconsistent
            // with the explicit JOIN case!
            let qid = query_id_hash(
                &["articles", "users", "votes"],
                &[
                    &Column::from("articles.aid"),
                    &Column::from("articles.author"),
                    &Column::from("users.id"),
                    &Column::from("votes.aid"),
                ],
                &[
                    &Column::from("users.name"),
                    &Column::from("articles.title"),
                    &Column::from("votes.uid"),
                ],
            );
            let join1_view = get_node(&inc, mig, &format!("q_{:x}_n0", qid));
            // articles join users
            assert_eq!(join1_view.fields(), &["aid", "title", "author", "name"]);
            let join2_view = get_node(&inc, mig, &format!("q_{:x}_n1", qid));
            // join1_view join vptes
            assert_eq!(
                join2_view.fields(),
                &["aid", "title", "author", "name", "uid"]
            );
            // leaf view
            let leaf_view = get_node(&inc, mig, "q_3");
            assert_eq!(leaf_view.fields(), &["name", "title", "uid", "bogokey"]);
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore]
    async fn it_incorporates_join_projecting_join_columns() {
        // set up graph
        let mut g =
            integration_utils::start_simple("it_incorporates_join_projecting_join_columns").await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            assert!(inc
                .add_query("CREATE TABLE users (id int, name varchar(40));", None, mig)
                .is_ok());
            assert!(inc
                .add_query(
                    "CREATE TABLE articles (id int, author int, title varchar(255));",
                    None,
                    mig
                )
                .is_ok());
            let q = "SELECT users.id, users.name, articles.author, articles.title \
                     FROM articles, users \
                     WHERE users.id = articles.author;";
            let q = inc.add_query(q, None, mig);
            assert!(q.is_ok());
            let qid = query_id_hash(
                &["articles", "users"],
                &[&Column::from("articles.author"), &Column::from("users.id")],
                &[
                    &Column::from("users.id"),
                    &Column::from("users.name"),
                    &Column::from("articles.author"),
                    &Column::from("articles.title"),
                ],
            );
            // join node
            let new_join_view = get_node(&inc, mig, &format!("q_{:x}_n0", qid));
            assert_eq!(new_join_view.fields(), &["id", "author", "title", "name"]);
            // leaf node
            let new_leaf_view = get_node(&inc, mig, &q.unwrap().name);
            assert_eq!(
                new_leaf_view.fields(),
                &["id", "name", "author", "title", "bogokey"]
            );
            assert_eq!(new_leaf_view.description(true), "π[1, 3, 1, 2, lit: 0]");
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn it_incorporates_self_join() {
        // set up graph
        let mut g = integration_utils::start_simple("it_incorporates_self_join").await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            assert!(inc
                .add_query("CREATE TABLE friends (id int, friend int);", None, mig)
                .is_ok());

            // Try a friends-of-friends type computation via self join
            let q = "SELECT f1.id, f2.friend AS fof \
                     FROM friends AS f1 \
                     JOIN (SELECT * FROM friends) AS f2 ON (f1.friend = f2.id)
                     WHERE f1.id = ?;";

            let res = inc.add_query(q, None, mig);
            assert!(res.is_ok(), "{}", res.as_ref().unwrap_err());
            let qfp = res.unwrap();

            eprintln!(
                "{:?}",
                qfp.new_nodes
                    .iter()
                    .map(|n| mig.graph().node_weight(*n).unwrap().description(true))
                    .collect::<Vec<_>>()
            );

            // Check join node
            let join = mig.graph().node_weight(qfp.new_nodes[1]).unwrap();
            assert_eq!(join.fields(), &["id", "friend", "friend"]);
            assert_eq!(join.description(true), "[1:0, 1:1, 2:1] 1:(1) ⋈ 2:(0)");

            // Check leaf projection node
            let leaf_view = get_node(&inc, mig, "q_1");
            assert_eq!(leaf_view.fields(), &["id", "fof"]);
            assert_eq!(leaf_view.description(true), "π[0, 2]");
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn it_incorporates_literal_projection() {
        // set up graph
        let mut g = integration_utils::start_simple("it_incorporates_literal_projection").await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            assert!(inc
                .add_query("CREATE TABLE users (id int, name varchar(40));", None, mig)
                .is_ok());

            let res = inc.add_query("SELECT users.name, 1 FROM users;", None, mig);
            assert!(res.is_ok());

            // leaf view node
            let edge = get_node(&inc, mig, &res.unwrap().name);
            assert_eq!(edge.fields(), &["name", "1", "bogokey"]);
            assert_eq!(edge.description(true), "π[1, lit: 1, lit: 0]");
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn it_incorporates_arithmetic_projection() {
        // set up graph
        let mut g = integration_utils::start_simple("it_incorporates_arithmetic_projection").await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            assert!(inc
                .add_query("CREATE TABLE users (id int, age int);", None, mig)
                .is_ok());

            let res = inc.add_query(
                "SELECT 2 * users.age, 2 * 10 as twenty FROM users;",
                None,
                mig,
            );
            assert!(res.is_ok());

            // leaf view node
            let edge = get_node(&inc, mig, &res.unwrap().name);
            assert_eq!(edge.fields(), &["(2 * `users`.`age`)", "twenty", "bogokey"]);
            assert_eq!(
                edge.description(true),
                "π[((lit: 2) * 1), ((lit: 2) * (lit: 10)), lit: 0]"
            );
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn it_incorporates_arithmetic_projection_with_parameter_column() {
        // set up graph
        let mut g = integration_utils::start_simple(
            "it_incorporates_arithmetic_projection_with_parameter_column",
        )
        .await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            assert!(inc
                .add_query(
                    "CREATE TABLE users (id int, age int, name varchar(40));",
                    None,
                    mig
                )
                .is_ok());

            let res = inc.add_query(
                "SELECT 2 * users.age, 2 * 10 AS twenty FROM users WHERE users.name = ?;",
                None,
                mig,
            );
            assert!(res.is_ok());
            let qfp = res.unwrap();
            assert_eq!(qfp.new_nodes.len(), 1);

            // Check projection node
            let node = get_node(&inc, mig, &qfp.name);
            assert_eq!(node.fields(), &["name", "(2 * `users`.`age`)", "twenty"]);
            assert_eq!(
                node.description(true),
                "π[2, ((lit: 2) * 1), ((lit: 2) * (lit: 10))]"
            );

            // Check reader
            let n = get_reader(&inc, mig, &qfp.name);
            assert_eq!(n.as_reader().unwrap().key().unwrap(), &[0]);
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn it_incorporates_join_with_nested_query() {
        readyset_tracing::init_test_logging();
        let mut g = integration_utils::start_simple("it_incorporates_join_with_nested_query").await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            assert!(inc
                .add_query("CREATE TABLE users (id int, name varchar(40));", None, mig)
                .is_ok());
            assert!(inc
                .add_query(
                    "CREATE TABLE articles (id int, author int, title varchar(255));",
                    None,
                    mig
                )
                .is_ok());

            let q = "SELECT nested_users.name, articles.title \
                     FROM articles \
                     JOIN (SELECT * FROM users) AS nested_users \
                     ON (nested_users.id = articles.author);";
            let q = inc.add_query(q, None, mig).unwrap();
            let qid = query_id_hash(
                &["articles", "nested_users"],
                &[
                    &Column::from("articles.author"),
                    &Column::from("nested_users.id"),
                ],
                &[
                    &Column::from("nested_users.name"),
                    &Column::from("articles.title"),
                ],
            );
            // join node
            let new_join_view = get_node(&inc, mig, &format!("q_{:x}_n0", qid));
            assert_eq!(new_join_view.fields(), &["id", "author", "title", "name"]);
            // leaf node
            let new_leaf_view = get_node(&inc, mig, &q.name);
            assert_eq!(new_leaf_view.fields(), &["name", "title", "bogokey"]);
            assert_eq!(new_leaf_view.description(true), "π[3, 2, lit: 0]");
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn it_incorporates_join_with_reused_nested_query() {
        let mut g =
            integration_utils::start_simple("it_incorporates_join_with_reused_nested_query").await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            assert!(inc
                .add_query("CREATE TABLE users (id int, name varchar(40));", None, mig)
                .is_ok());
            assert!(inc
                .add_query(
                    "CREATE TABLE articles (id int, author int, title varchar(255));",
                    None,
                    mig
                )
                .is_ok());

            // Add a simple query on users, which will be duplicated in the subquery below.
            assert!(inc.add_query("SELECT * FROM users;", None, mig).is_ok());

            // Ensure that the JOIN with nested_users still works as expected, even though an
            // an identical query already exists with a different name.
            let q = "SELECT nested_users.name, articles.title \
                     FROM articles \
                     JOIN (SELECT * FROM users) AS nested_users \
                     ON (nested_users.id = articles.author);";
            let q = inc.add_query(q, None, mig);
            assert!(q.is_ok());
            let qid = query_id_hash(
                &["articles", "nested_users"],
                &[
                    &Column::from("articles.author"),
                    &Column::from("nested_users.id"),
                ],
                &[
                    &Column::from("nested_users.name"),
                    &Column::from("articles.title"),
                ],
            );
            // join node
            let new_join_view = get_node(&inc, mig, &format!("q_{:x}_n0", qid));
            assert_eq!(new_join_view.fields(), &["id", "author", "title", "name"]);
            // leaf node
            let new_leaf_view = get_node(&inc, mig, &q.unwrap().name);
            assert_eq!(new_leaf_view.fields(), &["name", "title", "bogokey"]);
            assert_eq!(new_leaf_view.description(true), "π[3, 2, lit: 0]");
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn it_incorporates_compound_selection() {
        // set up graph
        let mut g = integration_utils::start_simple("it_incorporates_compound_selection").await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            assert!(inc
                .add_query("CREATE TABLE users (id int, name varchar(40));", None, mig)
                .is_ok());

            let res = inc.add_query(
                "SELECT users.id, users.name FROM users \
                 WHERE users.id = 32 \
                 UNION \
                 SELECT users.id, users.name FROM users \
                 WHERE users.id = 42 AND users.name = 'bob';",
                None,
                mig,
            );
            assert!(res.is_ok());

            // the leaf of this query (node above the reader) is a union
            let union_view = get_node(&inc, mig, &res.unwrap().name);
            assert_eq!(union_view.fields(), &["id", "name"]);
            assert_eq!(union_view.description(true), "3:[0, 1] ⋃ 6:[0, 1]");
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn it_distinguishes_predicates() {
        // set up graph
        let mut g = integration_utils::start_simple("it_distinguishes_predicates").await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            // Establish a base write type
            assert!(inc
                .add_query("CREATE TABLE users (id int, name varchar(40));", None, mig)
                .is_ok());
            // Should have source and "users" base table node
            assert_eq!(mig.graph().node_count(), 2);
            assert_eq!(get_node(&inc, mig, "users").name(), "users");
            assert_eq!(get_node(&inc, mig, "users").fields(), &["id", "name"]);
            assert!(get_node(&inc, mig, "users").is_base());

            // Add a new query
            let res = inc.add_query("SELECT id, name FROM users WHERE users.id = 42;", None, mig);
            assert!(res.is_ok());
            let leaf = res.unwrap().query_leaf;

            // Add query with a different predicate
            let ncount = mig.graph().node_count();
            let res = inc.add_query("SELECT id, name FROM users WHERE users.id = 50;", None, mig);
            assert!(res.is_ok());
            let qfp = res.unwrap();
            // should NOT have ended up with the same leaf node
            assert_ne!(qfp.query_leaf, leaf);
            // should have added three more nodes (filter, project and reader)
            assert_eq!(mig.graph().node_count(), ncount + 3);
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn it_adds_topk() {
        let mut g = integration_utils::start_simple("it_adds_topk").await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            inc.set_mir_config(super::mir::Config {
                allow_topk: true,
                ..Default::default()
            });
            "CREATE TABLE things (id int primary key);"
                .to_flow_parts(&mut inc, None, mig)
                .unwrap();
            // source -> things
            assert_eq!(mig.graph().node_count(), 2);

            let query = inc
                .add_query(
                    "SELECT * FROM things ORDER BY id LIMIT 3",
                    Some("things_by_id_limit_3".into()),
                    mig,
                )
                .unwrap();

            // source -> things -> project bogokey -> topk -> project_columns -> leaf
            assert_eq!(mig.graph().node_count(), 6);
            assert_eq!(
                query
                    .new_nodes
                    .iter()
                    .filter(|ni| mig.graph()[**ni].description(true) == "π[0, lit: 0]")
                    .count(),
                1
            );
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore]
    async fn it_queries_over_aliased_view() {
        let mut g = integration_utils::start_simple("it_queries_over_aliased_view").await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            assert!(inc
                .add_query("CREATE TABLE users (id int, name varchar(40));", None, mig)
                .is_ok());
            // Add first copy of new query, called "tq1"
            let res = inc.add_query(
                "SELECT id, name FROM users WHERE users.id = 42;",
                Some("tq1".into()),
                mig,
            );
            assert!(res.is_ok());
            let leaf = res.unwrap().query_leaf;

            // Add the same query again, this time as "tq2"
            let ncount = mig.graph().node_count();
            let res = inc.add_query(
                "SELECT id, name FROM users WHERE users.id = 42;",
                Some("tq2".into()),
                mig,
            );
            assert!(res.is_ok());
            // should have added no more nodes
            let qfp = res.unwrap();
            assert_eq!(qfp.new_nodes, vec![]);
            assert_eq!(mig.graph().node_count(), ncount);
            // should have ended up with the same leaf node
            assert_eq!(qfp.query_leaf, leaf);

            // Add a query over tq2, which really is tq1
            let _res = inc.add_query("SELECT tq2.id FROM tq2;", Some("over_tq2".into()), mig);
            // should have added a projection and a reader
            assert_eq!(mig.graph().node_count(), ncount + 2);
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn count_star_nonexistent_table() {
        let mut g = integration_utils::start_simple("count_star_nonexistent_table").await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            let res = inc.add_query("SELECT count(*) FROM foo;", None, mig);
            assert!(res.is_err());
        })
        .await;
    }
}
