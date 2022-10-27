use std::cmp::max;
use std::collections::{hash_map, HashMap, HashSet};
use std::str;
use std::vec::Vec;

use ::mir::node::node_inner::MirNodeInner;
use ::mir::query::{MirQuery, QueryFlowParts};
use ::mir::reuse::merge_mir_for_queries;
use ::mir::visualize::GraphViz;
use ::mir::{reuse as mir_reuse, Column, MirNodeRef};
use ::serde::{Deserialize, Serialize};
use nom_sql::{
    BinaryOperator, CompoundSelectOperator, CompoundSelectStatement, CreateTableStatement,
    CreateViewStatement, FieldDefinitionExpr, Relation, SelectSpecification, SelectStatement,
    SqlIdentifier, TableExpr,
};
use petgraph::graph::NodeIndex;
use readyset::internal::IndexType;
use readyset_data::{DfType, Dialect};
use readyset_errors::{internal_err, invalid_err, unsupported, ReadySetError, ReadySetResult};
use readyset_sql_passes::alias_removal::TableAliasRewrite;
use readyset_sql_passes::{contains_aggregate, AliasRemoval, Rewrite, RewriteContext};
use tracing::{debug, trace, warn};

use self::mir::SqlToMirConverter;
use self::query_graph::{to_query_graph, QueryGraph};
use self::query_signature::Signature;
use self::reuse::ReuseConfig;
use super::mir_to_flow::mir_query_to_flow_parts;
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
///
/// The entrypoints for adding queries to the `SqlIncorporator` are:
///
/// * [`add_table`][Self::add_table], to add a new `TABLE`
/// * [`add_view`][Self::add_view], to add a new `VIEW`
/// * [`add_query`][Self::add_query], to add a new cached query
#[derive(Clone, Debug, Default)]
// crate viz for tests
pub(crate) struct SqlIncorporator {
    mir_converter: SqlToMirConverter,
    leaf_addresses: HashMap<Relation, NodeIndex>,

    /// Stores VIEWs and CACHE queries.
    named_queries: HashMap<Relation, u64>,
    query_graphs: HashMap<u64, QueryGraph>,
    /// Stores CREATE TABLE statements.
    base_mir_queries: HashMap<Relation, MirQuery>,
    mir_queries: HashMap<u64, MirQuery>,
    num_queries: usize,

    base_schemas: HashMap<Relation, CreateTableStatement>,
    view_schemas: HashMap<Relation, Vec<SqlIdentifier>>,

    /// User-defined custom types, indexed by (schema-qualified) name.
    ///
    /// Internally, we just represent custom types as named aliases for a [`DfType`].
    custom_types: HashMap<Relation, DfType>,

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

    /// Rewrite the given SQL statement to normalize, validate, and desugar it, based on the stored
    /// relations in `self`.
    ///
    /// Can optionally provide a mutable reference to a list of names of non-existent tables which,
    /// if created, should invalidate the query
    // TODO(grfn): This should really be happening as part of the `add_<whatever>` methods (it was,
    // before this was made pub(crate)) but since the recipe expression registry stores expressions
    // we need it happening earlier. We should move it back to its rightful place once we can get
    // rid of that.
    pub(crate) fn rewrite<S>(
        &self,
        stmt: S,
        search_path: &[SqlIdentifier],
        dialect: Dialect,
        invalidating_tables: Option<&mut Vec<Relation>>,
    ) -> ReadySetResult<S>
    where
        S: Rewrite,
    {
        stmt.rewrite(&mut RewriteContext {
            view_schemas: &self.view_schemas,
            base_schemas: &self.base_schemas,
            custom_types: &self
                .custom_types
                .keys()
                .filter_map(|t| Some((t.schema.as_ref()?, &t.name)))
                .fold(HashMap::new(), |mut acc, (schema, name)| {
                    acc.entry(schema).or_default().insert(name);
                    acc
                }),
            search_path,
            dialect,
            invalidating_tables,
        })
    }

    /// Add a new table, specified by the given `CREATE TABLE` statement, to the graph, using the
    /// given `mig` to track changes.
    pub(crate) fn add_table(
        &mut self,
        statement: CreateTableStatement,
        mig: &mut Migration<'_>,
    ) -> ReadySetResult<()> {
        let qfp = self.add_base_via_mir(statement, mig)?;
        self.leaf_addresses.insert(qfp.name, qfp.query_leaf);
        Ok(())
    }

    /// Add a new SQL VIEW, specified by the given `CREATE VIEW` statement, to the graph, using the
    /// given `mig` to track changes.
    pub(crate) fn add_view(
        &mut self,
        statement: CreateViewStatement,
        mig: &mut Migration<'_>,
    ) -> ReadySetResult<()> {
        let name = statement.name;
        let qfp = match *statement.definition {
            SelectSpecification::Compound(query) => {
                self.add_compound_query(
                    name.clone(),
                    query,
                    /* is_name_required = */ true,
                    /* is_leaf = */ true,
                    mig,
                )?
            }
            SelectSpecification::Simple(query) => {
                self.add_select_query(
                    name.clone(),
                    query,
                    /* is_name_required = */ true,
                    /* is_leaf = */ true,
                    mig,
                )?
                .0
            }
        };
        self.leaf_addresses.insert(name, qfp.query_leaf);
        Ok(())
    }

    /// Add a new query to the graph, using the given `mig` to track changes.
    ///
    /// If `name` is provided, will use that as the name for the query to add, otherwise a unique
    /// name will be generated from the query. In either case, returns the name of the added query.
    pub(crate) fn add_query(
        &mut self,
        name: Option<Relation>,
        stmt: SelectStatement,
        mig: &mut Migration<'_>,
    ) -> ReadySetResult<Relation> {
        let name = name.unwrap_or_else(|| format!("q_{}", self.num_queries).into());
        let (qfp, _) = self.add_select_query(
            name.clone(),
            stmt,
            /* is_name_required = */ false,
            /* is_leaf = */ true,
            mig,
        )?;

        self.leaf_addresses.insert(name.clone(), qfp.query_leaf);

        Ok(name)
    }

    /// Add a new user-defined custom type (represented internally as a named alias for a
    /// [`DfType`]). Will return an error if a type already exists with the same name
    pub(crate) fn add_custom_type(&mut self, name: Relation, ty: DfType) -> ReadySetResult<()> {
        match self.custom_types.entry(name.clone()) {
            hash_map::Entry::Occupied(_) => {
                Err(invalid_err!("Custom type named {name} already exists"))
            }
            hash_map::Entry::Vacant(e) => {
                e.insert(ty);
                Ok(())
            }
        }
    }

    pub(super) fn get_base_schema(&self, table: &Relation) -> Option<CreateTableStatement> {
        self.base_schemas.get(table).cloned()
    }

    pub(super) fn get_view_schema(&self, name: &Relation) -> Option<Vec<String>> {
        self.view_schemas
            .get(name)
            .map(|s| s.iter().map(SqlIdentifier::to_string).collect())
    }

    #[cfg(test)]
    fn get_flow_node_address(&self, name: &Relation, v: usize) -> Option<NodeIndex> {
        self.mir_converter.get_flow_node_address(name, v)
    }

    /// Retrieves the flow node associated with a given query's leaf view.
    #[allow(unused)]
    pub(super) fn get_query_address(&self, name: &Relation) -> Option<NodeIndex> {
        match self.leaf_addresses.get(name) {
            None => self.mir_converter.get_leaf(name),
            Some(na) => Some(*na),
        }
    }

    pub(super) fn is_leaf_address(&self, ni: NodeIndex) -> bool {
        self.leaf_addresses.values().any(|nn| *nn == ni)
    }

    pub(super) fn get_leaf_name(&self, ni: NodeIndex) -> Option<&Relation> {
        self.leaf_addresses
            .iter()
            .find(|(_, idx)| **idx == ni)
            .map(|(name, _)| name)
    }

    fn consider_query_graph(
        &mut self,
        query_name: &Relation,
        is_name_required: bool,
        st: &SelectStatement,
        is_leaf: bool,
    ) -> ReadySetResult<(QueryGraph, QueryGraphReuse)> {
        debug!(%query_name, "Making query graph");
        trace!(%query_name, %st);

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
                    .ok_or_else(|| internal_err!("query graph should be present"))?;

                // note that this also checks the *order* in which parameters are specified; a
                // different order means that we cannot simply reuse the existing reader.
                if existing_qg.signature() == qg.signature()
                    && existing_qg.parameters() == qg.parameters()
                    && existing_qg.exact_hash() == qg.exact_hash()
                    && (!is_name_required || mir_query.name == *query_name)
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
                        OutputColumn::Expr(ref ec) => contains_aggregate(&ec.expression),
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

                        let mut parent = mir_query
                            .leaf
                            .borrow()
                            .ancestors()
                            .iter()
                            .next()
                            .unwrap()
                            .upgrade()
                            .unwrap();

                        if matches!(parent.borrow().inner, MirNodeInner::Project { .. }) {
                            // This might be just a reordering projection on top of a different
                            // projection, in that case we actually want to start with the
                            // granparent instead.
                            let grand_parent = parent
                                .borrow()
                                .ancestors()
                                .iter()
                                .next()
                                .unwrap()
                                .upgrade()
                                .unwrap();

                            if matches!(grand_parent.borrow().inner, MirNodeInner::Project { .. }) {
                                parent = grand_parent;
                            }
                        }

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
                        fn may_rewind(node: MirNodeRef) -> bool {
                            match node.borrow().inner {
                                MirNodeInner::Identity => true,
                                MirNodeInner::AliasTable { .. } => {
                                    may_rewind(node.borrow().parent().unwrap())
                                }
                                MirNodeInner::Project {
                                    expressions: ref e,
                                    literals: ref l,
                                    ..
                                } => e.is_empty() && l.is_empty(),
                                _ => false,
                            }
                        }

                        let ancestor = if may_rewind(parent.clone()) {
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
                                    // FIXME Ensure ancestor includes all columns in parent,
                                    // with the proper
                                    // names.
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
        stmt: CreateTableStatement,
        mig: &mut Migration<'_>,
    ) -> ReadySetResult<QueryFlowParts> {
        // first, compute the MIR representation of the SQL query
        let mut mir = self.mir_converter.named_base_to_mir(&stmt)?;

        trace!(base_node_mir = ?mir);

        // no optimization, because standalone base nodes can't be optimized

        // push it into the flow graph using the migration in `mig`, and obtain `QueryFlowParts`
        let qfp = mir_query_to_flow_parts(&mut mir, &self.custom_types, mig)?;

        // remember the schema in case we need it later
        // on base table schema change, we will overwrite the existing schema here.
        // TODO(malte): this means that requests for this will always return the *latest* schema
        // for a base.
        let table = stmt.table.clone();
        self.base_schemas.insert(table.clone(), stmt);

        self.register_query(table, None, &mir);

        Ok(qfp)
    }

    fn add_compound_query(
        &mut self,
        query_name: Relation,
        query: CompoundSelectStatement,
        is_name_required: bool,
        is_leaf: bool,
        mig: &mut Migration<'_>,
    ) -> Result<QueryFlowParts, ReadySetError> {
        let subqueries = query
            .selects
            .into_iter()
            .enumerate()
            .map(|(i, (_op, stmt))| {
                Ok(self
                    .add_select_query(
                        format!("{}_csq_{}", query_name, i).into(),
                        stmt,
                        is_name_required,
                        false,
                        mig,
                    )?
                    .1)
            })
            .collect::<ReadySetResult<Vec<_>>>()?;

        let mut combined_mir_query = self.mir_converter.compound_query_to_mir(
            &query_name,
            subqueries,
            CompoundSelectOperator::Union,
            &query.order,
            &query.limit,
            &query.offset,
            is_leaf,
        )?;

        let qfp = mir_query_to_flow_parts(&mut combined_mir_query, &self.custom_types, mig)?;

        self.register_query(query_name.clone(), None, &combined_mir_query);

        Ok(qfp)
    }

    fn select_query_to_mir(
        &mut self,
        query_name: Relation,
        is_name_required: bool,
        sq: SelectStatement,
        is_leaf: bool,
    ) -> ReadySetResult<(QueryGraph, MirQuery)> {
        let (qg, reuse) = self.consider_query_graph(&query_name, is_name_required, &sq, is_leaf)?;

        let mir_query = match reuse {
            QueryGraphReuse::ExactMatch(mir_query) => mir_query.clone(),
            QueryGraphReuse::ExtendExisting(reuse_mirs) => {
                let mut new_query_mir =
                    self.mir_converter
                        .named_query_to_mir(&query_name, sq, &qg, is_leaf)?;
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
                &query_name,
                &params,
                index_type,
                project_columns,
            ),
            QueryGraphReuse::None => {
                self.mir_converter
                    .named_query_to_mir(&query_name, sq, &qg, is_leaf)?
            }
        };

        Ok((qg, mir_query))
    }

    /// Add a new SelectStatement to the given migration, returning information about the dataflow
    /// and MIR nodes that were added
    fn add_select_query(
        &mut self,
        query_name: Relation,
        mut stmt: SelectStatement,
        is_name_required: bool,
        is_leaf: bool,
        mig: &mut Migration<'_>,
    ) -> Result<(QueryFlowParts, MirQuery), ReadySetError> {
        let on_err = |e| ReadySetError::SelectQueryCreationFailed {
            qname: query_name.to_string(),
            source: Box::new(e),
        };

        self.num_queries += 1;

        // Remove all table aliases from the query. Create named views in cases where the alias must
        // be replaced with a view rather than the table itself in order to prevent ambiguity. (This
        // may occur when a single table is referenced using more than one alias).
        let table_alias_rewrites = stmt.rewrite_table_aliases(&query_name.name);
        for r in table_alias_rewrites {
            match r {
                TableAliasRewrite::View {
                    to_view, for_table, ..
                } => {
                    let query = SelectStatement {
                        tables: vec![TableExpr::from(for_table)],
                        fields: vec![FieldDefinitionExpr::All],
                        ..Default::default()
                    };
                    self.add_select_query(
                        to_view,
                        self.rewrite(
                            query,
                            &[], /* Don't need a schema search path since we're only resolving
                                  * one (already qualified) table */
                            mig.dialect,
                            None,
                        )?,
                        true,
                        false,
                        mig,
                    )?;
                }
                TableAliasRewrite::Cte {
                    to_view,
                    for_statement,
                    ..
                } => {
                    self.add_select_query(to_view, *for_statement, true, false, mig)?;
                }
                TableAliasRewrite::Table { .. } => {}
            }
        }

        trace!(rewritten_query = %stmt);

        let (qg, mir_query) = self
            .select_query_to_mir(query_name.clone(), is_name_required, stmt, is_leaf)
            .map_err(on_err)?;

        trace!(pre_opt_mir = %mir_query.to_graphviz());
        let mut opt_mir = mir_query.optimize().map_err(on_err)?;
        trace!(post_opt_mir = %opt_mir.to_graphviz());

        let qfp = mir_query_to_flow_parts(&mut opt_mir, &self.custom_types, mig).map_err(on_err)?;
        self.register_query(query_name.clone(), Some(qg), &opt_mir);
        Ok((qfp, opt_mir))
    }

    pub(super) fn remove_query(
        &mut self,
        query_name: &Relation,
    ) -> ReadySetResult<Option<NodeIndex>> {
        let nodeid = self
            .leaf_addresses
            .remove(query_name)
            .ok_or_else(|| internal_err!("tried to remove unknown query"))?;

        let qg_hash = self.named_queries.remove(query_name).ok_or_else(|| {
            internal_err!("missing query hash for named query \"{}\"", query_name)
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
    pub(super) fn remove_base(&mut self, name: &Relation) -> ReadySetResult<NodeIndex> {
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
            .ok_or_else(|| invalid_err!("tried to remove unknown base {}", name))?;
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
            .ok_or_else(|| invalid_err!("tried to remove unknown base {}", name))
    }

    fn register_query(&mut self, query_name: Relation, qg: Option<QueryGraph>, mir: &MirQuery) {
        debug!(%query_name, "registering query");
        self.view_schemas
            .insert(query_name.clone(), mir.fields.clone());

        // We made a new query, so store the query graph and the corresponding leaf MIR node.
        // TODO(malte): we currently store nothing if there is no QG (e.g., for compound queries).
        // This means we cannot reuse these queries.
        match qg {
            Some(qg) => {
                let qg_hash = qg.signature().hash;
                self.query_graphs.insert(qg_hash, qg);
                self.mir_queries.insert(qg_hash, mir.clone());
                self.named_queries.insert(query_name, qg_hash);
            }
            None => {
                self.base_mir_queries.insert(query_name, mir.clone());
            }
        }
    }

    /// Upgrades the schema version for the
    /// internal [`SqlToMirConverter`].
    pub(super) fn upgrade_version(&mut self) {
        self.mir_converter.upgrade_version();
    }
}

#[cfg(test)]
mod tests {
    use dataflow::prelude::*;
    use nom_sql::{parse_create_table, parse_select_statement, Column, Dialect, Relation};
    use readyset_data::{Collation, DfType, Dialect as DataDialect};

    use super::SqlIncorporator;
    use crate::controller::Migration;
    use crate::integration_utils;

    /// Helper to grab a reference to a named view.
    fn get_node<'a>(inc: &SqlIncorporator, mig: &'a Migration<'_>, name: &Relation) -> &'a Node {
        let na = inc
            .get_flow_node_address(name, 0)
            .unwrap_or_else(|| panic!("No node named {name} exists"));
        mig.graph().node_weight(na).unwrap()
    }

    /// Helper to grab the immediate parent of  a named view.
    fn get_parent_node<'a>(
        inc: &SqlIncorporator,
        mig: &'a Migration<'_>,
        name: &Relation,
    ) -> &'a Node {
        let na = inc
            .get_flow_node_address(name, 0)
            .unwrap_or_else(|| panic!("No node named {name} exists"));
        let ni = mig.graph().node_weight(na).unwrap().ancestors().unwrap()[0];
        &mig.graph()[ni]
    }

    fn get_reader<'a>(inc: &SqlIncorporator, mig: &'a Migration<'_>, name: &Relation) -> &'a Node {
        let na = inc
            .get_flow_node_address(name, 0)
            .unwrap_or_else(|| panic!("No node named {name} exists"));
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
        let mut g = integration_utils::start_simple_unsharded("it_parses").await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            // Must have a base node for type inference to work, so make one manually
            inc.add_table(
                inc.rewrite(
                    parse_create_table(
                        Dialect::MySQL,
                        "CREATE TABLE users (id int, name varchar(40));",
                    )
                    .unwrap(),
                    &[],
                    DataDialect::DEFAULT_MYSQL,
                    None,
                )
                .unwrap(),
                mig,
            )
            .unwrap();

            // Should have two nodes: source and "users" base table
            let ncount = mig.graph().node_count();
            assert_eq!(ncount, 2);
            assert_eq!(
                get_node(&inc, mig, &"users".into()).name(),
                &Relation::from("users")
            );

            assert!(inc
                .add_query(
                    None,
                    inc.rewrite(
                        parse_select_statement(Dialect::MySQL, "SELECT users.id from users;")
                            .unwrap(),
                        &[],
                        DataDialect::DEFAULT_MYSQL,
                        None,
                    )
                    .unwrap(),
                    mig
                )
                .is_ok());
            // Should now have source, "users", a leaf projection node for the new selection, a
            // reorder projection and a reader node
            assert_eq!(mig.graph().node_count(), ncount + 3);
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn it_parses_parameter_column() {
        // set up graph
        let mut g = integration_utils::start_simple_unsharded("it_parses_parameter_column").await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            assert!(inc
                .add_table(
                    inc.rewrite(
                        parse_create_table(
                            Dialect::MySQL,
                            "CREATE TABLE users (id int, name varchar(40), age int);"
                        )
                        .unwrap(),
                        &[],
                        DataDialect::DEFAULT_MYSQL,
                        None,
                    )
                    .unwrap(),
                    mig
                )
                .is_ok());

            // Add a new query with a parameter
            let res = inc.add_query(
                None,
                inc.rewrite(
                    parse_select_statement(
                        Dialect::MySQL,
                        "SELECT id, name FROM users WHERE users.name = ?;",
                    )
                    .unwrap(),
                    &[],
                    DataDialect::DEFAULT_MYSQL,
                    None,
                )
                .unwrap(),
                mig,
            );
            assert!(res.is_ok());
            let name = res.unwrap();
            let node = get_node(&inc, mig, &name);
            // fields should be projected correctly in query order
            assert_eq!(
                node.columns().iter().map(|c| c.name()).collect::<Vec<_>>(),
                &["id", "name"]
            );
            assert_eq!(node.description(true), "π[0, 1]");
            // reader key column should be correct
            let n = get_reader(&inc, mig, &name);
            assert_eq!(n.as_reader().unwrap().key().unwrap(), &[1]);
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn it_parses_unprojected_parameter_column() {
        // set up graph
        let mut g =
            integration_utils::start_simple_unsharded("it_parses_unprojected_parameter_column")
                .await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            assert!(inc
                .add_table(
                    inc.rewrite(
                        parse_create_table(
                            Dialect::MySQL,
                            "CREATE TABLE users (id int, name varchar(40), age int);"
                        )
                        .unwrap(),
                        &[],
                        DataDialect::DEFAULT_MYSQL,
                        None,
                    )
                    .unwrap(),
                    mig,
                )
                .is_ok());

            // Add a new query with a parameter
            let res = inc.add_query(
                None,
                inc.rewrite(
                    parse_select_statement(
                        Dialect::MySQL,
                        "SELECT id FROM users WHERE users.name = ?;",
                    )
                    .unwrap(),
                    &[],
                    DataDialect::DEFAULT_MYSQL,
                    None,
                )
                .unwrap(),
                mig,
            );
            assert!(res.is_ok());
            let name = res.unwrap();
            let node = get_node(&inc, mig, &name);
            // fields should be projected correctly in query order, with the
            // absent parameter column included
            assert_eq!(
                node.columns().iter().map(|c| c.name()).collect::<Vec<_>>(),
                &["id", "name"]
            );
            assert_eq!(node.description(true), "π[0, 1]");
            // reader key column should be correct
            let n = get_reader(&inc, mig, &name);
            assert_eq!(n.as_reader().unwrap().key().unwrap(), &[1])
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn it_parses_filter_and_parameter_column() {
        // set up graph
        let mut g =
            integration_utils::start_simple_unsharded("it_parses_filter_and_parameter_column")
                .await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            assert!(inc
                .add_table(
                    inc.rewrite(
                        parse_create_table(
                            Dialect::MySQL,
                            "CREATE TABLE users (id int, name varchar(40), age int);"
                        )
                        .unwrap(),
                        &[],
                        DataDialect::DEFAULT_MYSQL,
                        None,
                    )
                    .unwrap(),
                    mig,
                )
                .is_ok());

            // Add a new query with a parameter
            let res = inc.add_query(
                None,
                inc.rewrite(
                    parse_select_statement(
                        Dialect::MySQL,
                        "SELECT id, name FROM users WHERE users.age > 20 AND users.name = ?;",
                    )
                    .unwrap(),
                    &[],
                    DataDialect::DEFAULT_MYSQL,
                    None,
                )
                .unwrap(),
                mig,
            );
            assert!(res.is_ok());
            let name = res.unwrap();

            // Check projection node
            let projection = get_node(&inc, mig, &name);
            // fields should be projected correctly in query order
            assert_eq!(
                projection
                    .columns()
                    .iter()
                    .map(|c| c.name())
                    .collect::<Vec<_>>(),
                &["id", "name"]
            );
            assert_eq!(projection.description(true), "π[0, 1]");

            // TODO Check that the filter and projection nodes are ordered properly.
            // println!("graph: {:?}", mig.graph());

            // Check reader
            let n = get_reader(&inc, mig, &name);
            assert_eq!(n.as_reader().unwrap().key().unwrap(), &[1]);
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn it_incorporates_simple_join() {
        // set up graph
        let mut g = integration_utils::start_simple_unsharded("it_incorporates_simple_join").await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            // Establish a base write type for "users"
            assert!(inc
                .add_table(
                    inc.rewrite(
                        parse_create_table(
                            Dialect::MySQL,
                            "CREATE TABLE users (id int, name varchar(40));"
                        )
                        .unwrap(),
                        &[],
                        DataDialect::DEFAULT_MYSQL,
                        None,
                    )
                    .unwrap(),
                    mig,
                )
                .is_ok());
            // Should have source and "users" base table node
            assert_eq!(mig.graph().node_count(), 2);
            assert_eq!(
                get_node(&inc, mig, &"users".into()).name(),
                &Relation::from("users")
            );
            assert_eq!(
                get_node(&inc, mig, &"users".into())
                    .columns()
                    .iter()
                    .map(|c| c.name())
                    .collect::<Vec<_>>(),
                &["id", "name"]
            );
            assert!(get_node(&inc, mig, &"users".into()).is_base());

            // Establish a base write type for "articles"
            assert!(inc
                .add_table(
                    inc.rewrite(
                        parse_create_table(
                            Dialect::MySQL,
                            "CREATE TABLE articles (id int, author int, title varchar(255));"
                        )
                        .unwrap(),
                        &[],
                        DataDialect::DEFAULT_MYSQL,
                        None,
                    )
                    .unwrap(),
                    mig,
                )
                .is_ok());
            // Should have source and "users" base table node
            assert_eq!(mig.graph().node_count(), 3);
            assert_eq!(
                get_node(&inc, mig, &"articles".into()).name(),
                &Relation::from("articles")
            );
            assert_eq!(
                get_node(&inc, mig, &"articles".into())
                    .columns()
                    .iter()
                    .map(|c| c.name())
                    .collect::<Vec<_>>(),
                &["id", "author", "title"]
            );
            assert!(get_node(&inc, mig, &"articles".into()).is_base());

            // Try a simple equi-JOIN query
            let q = "SELECT users.name, articles.title \
                     FROM articles, users \
                     WHERE users.id = articles.author;";
            let q = inc.add_query(
                None,
                inc.rewrite(
                    parse_select_statement(Dialect::MySQL, q).unwrap(),
                    &[],
                    DataDialect::DEFAULT_MYSQL,
                    None,
                )
                .unwrap(),
                mig,
            );
            assert!(q.is_ok());
            // leaf node
            let new_leaf_view = get_parent_node(&inc, mig, &q.unwrap());
            assert_eq!(
                new_leaf_view
                    .columns()
                    .iter()
                    .map(|c| c.name())
                    .collect::<Vec<_>>(),
                &["name", "title", "bogokey"]
            );
            assert_eq!(new_leaf_view.description(true), "π[3, 2, lit: 0]");
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn it_incorporates_simple_selection() {
        // set up graph
        let mut g =
            integration_utils::start_simple_unsharded("it_incorporates_simple_selection").await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            // Establish a base write type
            assert!(inc
                .add_table(
                    inc.rewrite(
                        parse_create_table(
                            Dialect::MySQL,
                            "CREATE TABLE users (id int, name varchar(40));"
                        )
                        .unwrap(),
                        &[],
                        DataDialect::DEFAULT_MYSQL,
                        None,
                    )
                    .unwrap(),
                    mig,
                )
                .is_ok());
            // Should have source and "users" base table node
            assert_eq!(mig.graph().node_count(), 2);
            assert_eq!(
                get_node(&inc, mig, &"users".into()).name(),
                &Relation::from("users")
            );
            assert_eq!(
                get_node(&inc, mig, &"users".into())
                    .columns()
                    .iter()
                    .map(|c| c.name())
                    .collect::<Vec<_>>(),
                &["id", "name"]
            );
            assert!(get_node(&inc, mig, &"users".into()).is_base());

            // Try a simple query
            let res = inc.add_query(
                None,
                inc.rewrite(
                    parse_select_statement(
                        Dialect::MySQL,
                        "SELECT users.name FROM users WHERE users.id = 42;",
                    )
                    .unwrap(),
                    &[],
                    DataDialect::DEFAULT_MYSQL,
                    None,
                )
                .unwrap(),
                mig,
            );
            assert!(res.is_ok(), "{}", res.err().unwrap());

            // leaf view node
            let edge = get_parent_node(&inc, mig, &res.unwrap());
            assert_eq!(
                edge.columns().iter().map(|c| c.name()).collect::<Vec<_>>(),
                &["name", "bogokey"]
            );
            assert_eq!(edge.description(true), "π[1, lit: 0]");
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn it_incorporates_aggregation() {
        // set up graph
        let mut g = integration_utils::start_simple_unsharded("it_incorporates_aggregation").await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            // Establish a base write types
            assert!(inc
                .add_table(
                    inc.rewrite(
                        parse_create_table(
                            Dialect::MySQL,
                            "CREATE TABLE votes (aid int, userid int);"
                        )
                        .unwrap(),
                        &[],
                        DataDialect::DEFAULT_MYSQL,
                        None,
                    )
                    .unwrap(),
                    mig
                )
                .is_ok());
            // Should have source and "users" base table node
            assert_eq!(mig.graph().node_count(), 2);
            assert_eq!(
                get_node(&inc, mig, &"votes".into()).name(),
                &Relation::from("votes")
            );
            assert_eq!(
                get_node(&inc, mig, &"votes".into())
                    .columns()
                    .iter()
                    .map(|c| c.name())
                    .collect::<Vec<_>>(),
                &["aid", "userid"]
            );
            assert!(get_node(&inc, mig, &"votes".into()).is_base());

            // Try a simple COUNT function
            let res = inc.add_query(
                None,
                parse_select_statement(
                    Dialect::MySQL,
                    "SELECT COUNT(votes.userid) AS votes \
                    FROM votes GROUP BY votes.aid;",
                )
                .unwrap(),
                mig,
            );
            assert!(res.is_ok());
            // added the aggregation and the edge view, reorder project and a reader
            assert_eq!(mig.graph().node_count(), 6);
            // check edge view
            let edge_view = get_parent_node(&inc, mig, &res.unwrap());
            assert_eq!(
                edge_view
                    .columns()
                    .iter()
                    .map(|c| c.name())
                    .collect::<Vec<_>>(),
                &["votes", "bogokey"]
            );
            assert_eq!(edge_view.description(true), "π[1, lit: 0]");
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn it_does_not_reuse_if_disabled() {
        // set up graph
        let mut g =
            integration_utils::start_simple_unsharded("it_does_not_reuse_if_disabled").await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            inc.disable_reuse();
            assert!(inc
                .add_table(
                    inc.rewrite(
                        parse_create_table(
                            Dialect::MySQL,
                            "CREATE TABLE users (id int, name varchar(40));"
                        )
                        .unwrap(),
                        &[],
                        DataDialect::DEFAULT_MYSQL,
                        None,
                    )
                    .unwrap(),
                    mig,
                )
                .is_ok());
            let res = inc.add_query(
                None,
                inc.rewrite(
                    parse_select_statement(
                        Dialect::MySQL,
                        "SELECT id, name FROM users WHERE users.id = 42;",
                    )
                    .unwrap(),
                    &[],
                    DataDialect::DEFAULT_MYSQL,
                    None,
                )
                .unwrap(),
                mig,
            );
            assert!(res.is_ok());

            // Add the same query again; this should NOT reuse here.
            let ncount = mig.graph().node_count();
            let res = inc.add_query(
                None,
                inc.rewrite(
                    parse_select_statement(
                        Dialect::MySQL,
                        "SELECT name, id FROM users WHERE users.id = 42;",
                    )
                    .unwrap(),
                    &[],
                    DataDialect::DEFAULT_MYSQL,
                    None,
                )
                .unwrap(),
                mig,
            );
            assert!(res.is_ok());
            // expect three new nodes: filter, project, project (for reorder), reader
            assert_eq!(mig.graph().node_count(), ncount + 4);
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn it_reuses_identical_query() {
        // set up graph
        let mut g = integration_utils::start_simple_unsharded("it_reuses_identical_query").await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            inc.enable_reuse(crate::ReuseConfigType::Finkelstein);
            // Establish a base write type
            assert!(inc
                .add_table(
                    inc.rewrite(
                        parse_create_table(
                            Dialect::MySQL,
                            "CREATE TABLE users (id int, name varchar(40));"
                        )
                        .unwrap(),
                        &[],
                        DataDialect::DEFAULT_MYSQL,
                        None,
                    )
                    .unwrap(),
                    mig,
                )
                .is_ok());
            // Should have source and "users" base table node
            assert_eq!(mig.graph().node_count(), 2);
            assert_eq!(
                get_node(&inc, mig, &"users".into()).name(),
                &Relation::from("users")
            );
            assert_eq!(
                get_node(&inc, mig, &"users".into())
                    .columns()
                    .iter()
                    .map(|c| c.name())
                    .collect::<Vec<_>>(),
                &["id", "name"]
            );
            assert!(get_node(&inc, mig, &"users".into()).is_base());

            // Add a new query
            inc.add_query(
                None,
                inc.rewrite(
                    parse_select_statement(
                        Dialect::MySQL,
                        "SELECT id, name FROM users WHERE users.id = 42;",
                    )
                    .unwrap(),
                    &[],
                    DataDialect::DEFAULT_MYSQL,
                    None,
                )
                .unwrap(),
                mig,
            )
            .unwrap();
            // Add the same query again
            let ncount = mig.graph().node_count();
            inc.add_query(
                None,
                inc.rewrite(
                    parse_select_statement(
                        Dialect::MySQL,
                        "SELECT id, name FROM users WHERE users.id = 42;",
                    )
                    .unwrap(),
                    &[],
                    DataDialect::DEFAULT_MYSQL,
                    None,
                )
                .unwrap(),
                mig,
            )
            .unwrap();
            assert_eq!(mig.graph().node_count(), ncount);

            // Add the same query again, but project columns in a different order
            let ncount = mig.graph().node_count();
            let res = inc.add_query(
                None,
                inc.rewrite(
                    parse_select_statement(
                        Dialect::MySQL,
                        "SELECT name, id FROM users WHERE users.id = 42;",
                    )
                    .unwrap(),
                    &[],
                    DataDialect::DEFAULT_MYSQL,
                    None,
                )
                .unwrap(),
                mig,
            );
            assert!(res.is_ok());
            // should have added three more nodes (project, reorder project and reader)
            assert_eq!(mig.graph().node_count(), ncount + 3);
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn it_reuses_with_different_parameter() {
        // set up graph
        let mut g =
            integration_utils::start_simple_unsharded("it_reuses_with_different_parameter").await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            // Establish a base write type
            assert!(inc
                .add_table(
                    inc.rewrite(
                        parse_create_table(
                            Dialect::MySQL,
                            "CREATE TABLE users (id int, name varchar(40), address varchar(40));"
                        )
                        .unwrap(),
                        &[],
                        DataDialect::DEFAULT_MYSQL,
                        None,
                    )
                    .unwrap(),
                    mig,
                )
                .is_ok());
            // Should have source and "users" base table node
            assert_eq!(mig.graph().node_count(), 2);
            assert_eq!(
                get_node(&inc, mig, &"users".into()).name(),
                &Relation::from("users")
            );
            assert_eq!(
                get_node(&inc, mig, &"users".into())
                    .columns()
                    .iter()
                    .map(|c| c.name())
                    .collect::<Vec<_>>(),
                &["id", "name", "address"]
            );
            assert!(get_node(&inc, mig, &"users".into()).is_base());

            // Add a new query
            let res = inc.add_query(
                None,
                inc.rewrite(
                    parse_select_statement(
                        Dialect::MySQL,
                        "SELECT id, name FROM users WHERE users.id = ?;",
                    )
                    .unwrap(),
                    &[],
                    DataDialect::DEFAULT_MYSQL,
                    None,
                )
                .unwrap(),
                mig,
            );
            assert!(res.is_ok());

            // Add the same query again, but with a parameter on a different column.
            // Project the same columns, so we can reuse the projection that already exists and only
            // add an identity node.
            let ncount = mig.graph().node_count();
            let res = inc.add_query(
                None,
                inc.rewrite(
                    parse_select_statement(
                        Dialect::MySQL,
                        "SELECT id, name FROM users WHERE users.name = ?;",
                    )
                    .unwrap(),
                    &[],
                    DataDialect::DEFAULT_MYSQL,
                    None,
                )
                .unwrap(),
                mig,
            );
            assert!(res.is_ok(), "{}", res.err().unwrap());
            // should have added two more nodes: one identity node and one reader node
            let name = res.unwrap();
            assert_eq!(mig.graph().node_count(), ncount + 2);
            // only the identity node is returned in the vector of new nodes
            assert_eq!(get_node(&inc, mig, &name).description(true), "≡");

            // Do it again with a parameter on yet a different column.
            // Project different columns, so we need to add a new projection (not an identity).
            let ncount = mig.graph().node_count();
            let res = inc.add_query(
                None,
                inc.rewrite(
                    parse_select_statement(
                        Dialect::MySQL,
                        "SELECT id, name FROM users WHERE users.address = ?;",
                    )
                    .unwrap(),
                    &[],
                    DataDialect::DEFAULT_MYSQL,
                    None,
                )
                .unwrap(),
                mig,
            );
            assert!(res.is_ok());
            // should have added three more nodes: a projection, a reorder projection and one reader
            let name = res.unwrap();
            assert_eq!(mig.graph().node_count(), ncount + 3);
            assert_eq!(get_node(&inc, mig, &name).description(true), "π[0, 1, 2]");
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn it_does_not_reuse_bogokey_projection_for_different_projection() {
        // set up graph
        let mut g = integration_utils::start_simple_unsharded(
            "it_does_not_reuse_bogokey_projection_with_different_projection",
        )
        .await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            // Establish a base write type
            assert!(inc
                .add_table(
                    inc.rewrite(
                        parse_create_table(
                            Dialect::MySQL,
                            "CREATE TABLE users (id int, name varchar(40), address varchar(40));"
                        )
                        .unwrap(),
                        &[],
                        DataDialect::DEFAULT_MYSQL,
                        None,
                    )
                    .unwrap(),
                    mig,
                )
                .is_ok());

            // Add a new "full table" query. The view is expected to contain projected columns plus
            // the special 'bogokey' literal column.
            let res = inc.add_query(
                None,
                inc.rewrite(
                    parse_select_statement(Dialect::MySQL, "SELECT id, name FROM users;").unwrap(),
                    &[],
                    DataDialect::DEFAULT_MYSQL,
                    None,
                )
                .unwrap(),
                mig,
            );
            assert!(res.is_ok());
            let name = res.unwrap();
            // Check projection
            let projection = get_node(&inc, mig, &name);
            assert_eq!(
                projection
                    .columns()
                    .iter()
                    .map(|c| c.name())
                    .collect::<Vec<_>>(),
                &["id", "name", "bogokey"]
            );
            assert_eq!(projection.description(true), "π[0, 1, 2]");
            // Check reader column
            let n = get_reader(&inc, mig, &name);
            assert_eq!(n.as_reader().unwrap().key().unwrap(), &[2]);

            // Add the name query again, but with a parameter and project columns in a different
            // order.
            let ncount = mig.graph().node_count();
            let res = inc.add_query(
                None,
                inc.rewrite(
                    parse_select_statement(
                        Dialect::MySQL,
                        "SELECT name, id FROM users WHERE users.name = ?;",
                    )
                    .unwrap(),
                    &[],
                    DataDialect::DEFAULT_MYSQL,
                    None,
                )
                .unwrap(),
                mig,
            );
            assert!(res.is_ok());
            let name = res.unwrap();
            // Check projection
            let projection = get_node(&inc, mig, &name);
            assert_eq!(
                projection
                    .columns()
                    .iter()
                    .map(|c| c.name())
                    .collect::<Vec<_>>(),
                &["name", "id"]
            );
            assert_eq!(projection.description(true), "π[0, 1]");
            // should have added three more nodes (project, a reorder project and reader)
            assert_eq!(mig.graph().node_count(), ncount + 3);
            // Check reader column
            let n = get_reader(&inc, mig, &name);
            assert_eq!(n.as_reader().unwrap().key().unwrap(), &[0]);

            // Add a query with a parameter on a new field
            let ncount = mig.graph().node_count();
            let res = inc.add_query(
                None,
                inc.rewrite(
                    parse_select_statement(
                        Dialect::MySQL,
                        "SELECT id, name FROM users WHERE users.address = ?;",
                    )
                    .unwrap(),
                    &[],
                    DataDialect::DEFAULT_MYSQL,
                    None,
                )
                .unwrap(),
                mig,
            );
            assert!(res.is_ok());
            let name = res.unwrap();
            // Check projection
            let projection = get_node(&inc, mig, &name);
            assert_eq!(
                projection
                    .columns()
                    .iter()
                    .map(|c| c.name())
                    .collect::<Vec<_>>(),
                &["id", "name", "address"]
            );
            assert_eq!(projection.description(true), "π[0, 1, 2]");
            // should have added two more nodes (project, project reorder and reader)
            assert_eq!(mig.graph().node_count(), ncount + 3);
            // Check reader column
            let n = get_reader(&inc, mig, &name);
            assert_eq!(n.as_reader().unwrap().key().unwrap(), &[2]);
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn it_does_not_reuse_parameter_projection_for_bogokey_projection() {
        // set up graph
        let mut g = integration_utils::start_simple_unsharded(
            "it_does_not_reuse_parameter_projection_with_bogokey_projection",
        )
        .await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            // Establish a base write type
            assert!(inc
                .add_table(
                    inc.rewrite(
                        parse_create_table(
                            Dialect::MySQL,
                            "CREATE TABLE users (id int, name varchar(40), address varchar(40));"
                        )
                        .unwrap(),
                        &[],
                        DataDialect::DEFAULT_MYSQL,
                        None,
                    )
                    .unwrap(),
                    mig,
                )
                .is_ok());

            // Add a new parameterized query.
            let res = inc.add_query(
                None,
                inc.rewrite(
                    parse_select_statement(
                        Dialect::MySQL,
                        "SELECT id, name FROM users WHERE users.id = ?;",
                    )
                    .unwrap(),
                    &[],
                    DataDialect::DEFAULT_MYSQL,
                    None,
                )
                .unwrap(),
                mig,
            );
            assert!(res.is_ok());
            let name = res.unwrap();
            // Check projection
            let projection = get_node(&inc, mig, &name);
            assert_eq!(
                projection
                    .columns()
                    .iter()
                    .map(|c| c.name())
                    .collect::<Vec<_>>(),
                &["id", "name"]
            );
            assert_eq!(projection.description(true), "π[0, 1]");
            // Check reader column
            let n = get_reader(&inc, mig, &name);
            assert_eq!(n.as_reader().unwrap().key().unwrap(), &[0]);

            // Add a new "full table" query. The view is expected to contain projected columns plus
            // the special 'bogokey' literal column.
            let res = inc.add_query(
                None,
                inc.rewrite(
                    parse_select_statement(Dialect::MySQL, "SELECT id, name FROM users;").unwrap(),
                    &[],
                    DataDialect::DEFAULT_MYSQL,
                    None,
                )
                .unwrap(),
                mig,
            );
            assert!(res.is_ok());
            let name = res.unwrap();
            // Check projection
            let projection = get_node(&inc, mig, &name);
            assert_eq!(
                projection
                    .columns()
                    .iter()
                    .map(|c| c.name())
                    .collect::<Vec<_>>(),
                &["id", "name", "bogokey"]
            );
            assert_eq!(projection.description(true), "π[0, 1, 2]");
            // Check reader column
            let n = get_reader(&inc, mig, &name);
            assert_eq!(n.as_reader().unwrap().key().unwrap(), &[2]);
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn it_does_not_reuse_ancestor_lacking_parent_key() {
        // set up graph
        let mut g = integration_utils::start_simple_unsharded(
            "it_does_not_reuse_ancestor_lacking_parent_key",
        )
        .await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            // Establish a base write type
            assert!(inc
                .add_table(
                    inc.rewrite(
                        parse_create_table(
                            Dialect::MySQL,
                            "CREATE TABLE users (id int, name varchar(40), address varchar(40));"
                        )
                        .unwrap(),
                        &[],
                        DataDialect::DEFAULT_MYSQL,
                        None,
                    )
                    .unwrap(),
                    mig,
                )
                .is_ok());

            // Add a query with a parameter and a literal projection.
            let res = inc.add_query(
                None,
                inc.rewrite(
                    parse_select_statement(
                        Dialect::MySQL,
                        "SELECT id, name, 1 as one FROM users WHERE id = ?;",
                    )
                    .unwrap(),
                    &[],
                    DataDialect::DEFAULT_MYSQL,
                    None,
                )
                .unwrap(),
                mig,
            );
            assert!(res.is_ok());
            let name = res.unwrap();
            // Check projection
            let projection = get_node(&inc, mig, &name);
            assert_eq!(
                projection
                    .columns()
                    .iter()
                    .map(|c| c.name())
                    .collect::<Vec<_>>(),
                &["id", "name", "one"]
            );
            assert_eq!(projection.description(true), "π[0, 1, 2]");
            // Check reader column
            let n = get_reader(&inc, mig, &name);
            assert_eq!(n.as_reader().unwrap().key().unwrap(), &[0]);

            // Add a query with the same literal projection but a different parameter from the base
            // table.
            let ncount = mig.graph().node_count();
            let res = inc.add_query(
                None,
                inc.rewrite(
                    parse_select_statement(
                        Dialect::MySQL,
                        "SELECT id, name, 1 as one FROM users WHERE address = ?;",
                    )
                    .unwrap(),
                    &[],
                    DataDialect::DEFAULT_MYSQL,
                    None,
                )
                .unwrap(),
                mig,
            );
            assert!(res.is_ok(), "{}", res.err().unwrap());
            let name = res.unwrap();
            // Check projection
            let projection = get_node(&inc, mig, &name);
            assert_eq!(
                projection
                    .columns()
                    .iter()
                    .map(|c| c.name())
                    .collect::<Vec<_>>(),
                &["id", "name", "one", "address"]
            );
            assert_eq!(projection.description(true), "π[0, 1, 3, 2]");
            // should have added two more nodes (identity, project reorder and reader)
            assert_eq!(mig.graph().node_count(), ncount + 3);
            // Check reader column
            let n = get_reader(&inc, mig, &name);
            assert_eq!(n.as_reader().unwrap().key().unwrap(), &[3]);
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn it_incorporates_aggregation_no_group_by() {
        // set up graph
        let mut g =
            integration_utils::start_simple_unsharded("it_incorporates_aggregation_no_group_by")
                .await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            // Establish a base write type
            assert!(inc
                .add_table(
                    inc.rewrite(
                        parse_create_table(
                            Dialect::MySQL,
                            "CREATE TABLE votes (aid int, userid int);"
                        )
                        .unwrap(),
                        &[],
                        DataDialect::DEFAULT_MYSQL,
                        None,
                    )
                    .unwrap(),
                    mig
                )
                .is_ok());
            // Should have source and "users" base table node
            assert_eq!(mig.graph().node_count(), 2);
            assert_eq!(
                get_node(&inc, mig, &"votes".into()).name(),
                &Relation::from("votes")
            );
            assert_eq!(
                get_node(&inc, mig, &"votes".into())
                    .columns()
                    .iter()
                    .map(|c| c.name())
                    .collect::<Vec<_>>(),
                &["aid", "userid"]
            );
            assert!(get_node(&inc, mig, &"votes".into()).is_base());
            // Try a simple COUNT function without a GROUP BY clause
            let res = inc.add_query(
                None,
                inc.rewrite(
                    parse_select_statement(
                        Dialect::MySQL,
                        "SELECT COUNT(votes.userid) AS count FROM votes;",
                    )
                    .unwrap(),
                    &[],
                    DataDialect::DEFAULT_MYSQL,
                    None,
                )
                .unwrap(),
                mig,
            );
            assert!(res.is_ok());
            // added the aggregation, a project helper, the edge view, a reorder projection, and
            // reader
            assert_eq!(mig.graph().node_count(), 7);
            // check edge view -- note that it's not actually currently possible to read from
            // this for a lack of key (the value would be the key). Hence, the view also has a
            // bogokey column.
            let edge_view = get_parent_node(&inc, mig, &res.unwrap());
            assert_eq!(
                edge_view
                    .columns()
                    .iter()
                    .map(|c| c.name())
                    .collect::<Vec<_>>(),
                &["count", "bogokey"]
            );
            assert_eq!(edge_view.description(true), "π[1, lit: 0]");
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn it_incorporates_aggregation_count_star() {
        // set up graph
        let mut g =
            integration_utils::start_simple_unsharded("it_incorporates_aggregation_count_star")
                .await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            // Establish a base write type
            assert!(inc
                .add_table(
                    inc.rewrite(
                        parse_create_table(
                            Dialect::MySQL,
                            "CREATE TABLE votes (userid int, aid int);"
                        )
                        .unwrap(),
                        &[],
                        DataDialect::DEFAULT_MYSQL,
                        None,
                    )
                    .unwrap(),
                    mig
                )
                .is_ok());
            // Should have source and "users" base table node
            assert_eq!(mig.graph().node_count(), 2);
            assert_eq!(
                get_node(&inc, mig, &"votes".into()).name(),
                &Relation::from("votes")
            );
            assert_eq!(
                get_node(&inc, mig, &"votes".into())
                    .columns()
                    .iter()
                    .map(|c| c.name())
                    .collect::<Vec<_>>(),
                &["userid", "aid"]
            );
            assert!(get_node(&inc, mig, &"votes".into()).is_base());
            // Try a simple COUNT function
            let res = inc.add_query(
                None,
                inc.rewrite(
                    parse_select_statement(
                        Dialect::MySQL,
                        "SELECT COUNT(*) AS count FROM votes GROUP BY votes.userid;",
                    )
                    .unwrap(),
                    &[],
                    DataDialect::DEFAULT_MYSQL,
                    None,
                )
                .unwrap(),
                mig,
            );
            assert!(res.is_ok());
            // added a project for the coalesce (in the rewrite), the aggregation, a project helper,
            // the edge view, the reorder project and reader
            assert_eq!(mig.graph().node_count(), 7);
            // check edge view -- note that it's not actually currently possible to read from
            // this for a lack of key (the value would be the key). Hence, the view also has a
            // bogokey column.
            let edge_view = get_parent_node(&inc, mig, &res.unwrap());
            assert_eq!(
                edge_view
                    .columns()
                    .iter()
                    .map(|c| c.name())
                    .collect::<Vec<_>>(),
                &["count", "bogokey"]
            );
            assert_eq!(edge_view.description(true), "π[1, lit: 0]");
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn it_incorporates_aggregation_filter_count() {
        // set up graph
        let mut g =
            integration_utils::start_simple_unsharded("it_incorporates_aggregation_filter_count")
                .await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            // Establish a base write type
            assert!(inc
                .add_table(
                    inc.rewrite(
                        parse_create_table(
                            Dialect::MySQL,
                            "CREATE TABLE votes (userid int, aid int);"
                        )
                        .unwrap(),
                        &[],
                        DataDialect::DEFAULT_MYSQL,
                        None,
                    )
                    .unwrap(),
                    mig
                )
                .is_ok());
            // Should have source and "users" base table node
            assert_eq!(mig.graph().node_count(), 2);
            assert_eq!(
                get_node(&inc, mig, &"votes".into()).name(),
                &Relation::from("votes")
            );
            assert_eq!(
                get_node(&inc, mig, &"votes".into())
                    .columns()
                    .iter()
                    .map(|c| c.name())
                    .collect::<Vec<_>>(),
                &["userid", "aid"]
            );
            assert!(get_node(&inc, mig, &"votes".into()).is_base());
            // Try a simple COUNT function
            let res = inc.add_query(
                None,
                inc.rewrite(
                    parse_select_statement(
                        Dialect::MySQL,
                        "SELECT COUNT(CASE WHEN aid = 5 THEN aid END) AS count FROM votes \
                     GROUP BY votes.userid;",
                    )
                    .unwrap(),
                    &[],
                    DataDialect::DEFAULT_MYSQL,
                    None,
                )
                .unwrap(),
                mig,
            );
            assert!(res.is_ok());
            // added a project for the case, the aggregation, a project helper, the edge view, the
            // reorder project and reader
            assert_eq!(mig.graph().node_count(), 7);

            // check edge view -- note that it's not actually currently possible to read from
            // this for a lack of key (the value would be the key). Hence, the view also has a
            // bogokey column.
            let edge_view = get_parent_node(&inc, mig, &res.unwrap());
            assert_eq!(
                edge_view
                    .columns()
                    .iter()
                    .map(|c| c.name())
                    .collect::<Vec<_>>(),
                &["count", "bogokey"]
            );
            assert_eq!(edge_view.description(true), "π[1, lit: 0]");
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn it_incorporates_aggregation_filter_sum() {
        // set up graph
        let mut g =
            integration_utils::start_simple_unsharded("it_incorporates_aggregation_filter_sum")
                .await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            // Establish a base write type
            assert!(inc
                .add_table(
                    inc.rewrite(
                        parse_create_table(
                            Dialect::MySQL,
                            "CREATE TABLE votes (userid int, aid int, sign int);"
                        )
                        .unwrap(),
                        &[],
                        DataDialect::DEFAULT_MYSQL,
                        None,
                    )
                    .unwrap(),
                    mig,
                )
                .is_ok());
            // Should have source and "users" base table node
            assert_eq!(mig.graph().node_count(), 2);
            assert_eq!(
                get_node(&inc, mig, &"votes".into()).name(),
                &Relation::from("votes")
            );
            assert_eq!(
                get_node(&inc, mig, &"votes".into())
                    .columns()
                    .iter()
                    .map(|c| c.name())
                    .collect::<Vec<_>>(),
                &["userid", "aid", "sign"]
            );
            assert!(get_node(&inc, mig, &"votes".into()).is_base());
            // Try a simple COUNT function
            let res = inc.add_query(
                None,
                inc.rewrite(
                    parse_select_statement(
                        Dialect::MySQL,
                        "SELECT SUM(CASE WHEN aid = 5 THEN sign END) AS sum FROM votes \
                     GROUP BY votes.userid;",
                    )
                    .unwrap(),
                    &[],
                    DataDialect::DEFAULT_MYSQL,
                    None,
                )
                .unwrap(),
                mig,
            );
            assert!(res.is_ok());
            // added a project for the case, the aggregation, a project helper, the edge view, a
            // reorder projection, and reader
            assert_eq!(mig.graph().node_count(), 7);
            // check edge view -- note that it's not actually currently possible to read from
            // this for a lack of key (the value would be the key). Hence, the view also has a
            // bogokey column.
            let edge_view = get_parent_node(&inc, mig, &res.unwrap());
            assert_eq!(
                edge_view
                    .columns()
                    .iter()
                    .map(|c| c.name())
                    .collect::<Vec<_>>(),
                &["sum", "bogokey"]
            );
            assert_eq!(edge_view.description(true), "π[1, lit: 0]");
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn it_incorporates_aggregation_filter_sum_else() {
        // set up graph
        let mut g = integration_utils::start_simple_unsharded(
            "it_incorporates_aggregation_filter_sum_else",
        )
        .await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            // Establish a base write type
            assert!(inc
                .add_table(
                    inc.rewrite(
                        parse_create_table(
                            Dialect::MySQL,
                            "CREATE TABLE votes (userid int, aid int, sign int);"
                        )
                        .unwrap(),
                        &[],
                        DataDialect::DEFAULT_MYSQL,
                        None,
                    )
                    .unwrap(),
                    mig,
                )
                .is_ok());
            // Should have source and "users" base table node
            assert_eq!(mig.graph().node_count(), 2);
            assert_eq!(
                get_node(&inc, mig, &"votes".into()).name(),
                &Relation::from("votes")
            );
            assert_eq!(
                get_node(&inc, mig, &"votes".into())
                    .columns()
                    .iter()
                    .map(|c| c.name())
                    .collect::<Vec<_>>(),
                &["userid", "aid", "sign"]
            );
            assert!(get_node(&inc, mig, &"votes".into()).is_base());
            // Try a simple COUNT function
            let res = inc.add_query(
                None,
                inc.rewrite(
                    parse_select_statement(
                        Dialect::MySQL,
                        "SELECT SUM(CASE WHEN aid = 5 THEN sign ELSE 6 END) AS sum FROM votes \
                     GROUP BY votes.userid;",
                    )
                    .unwrap(),
                    &[],
                    DataDialect::DEFAULT_MYSQL,
                    None,
                )
                .unwrap(),
                mig,
            );
            assert!(res.is_ok());
            // added a project for the case, the aggregation, a project helper, the edge view, a
            // reorder projection, and reader
            assert_eq!(mig.graph().node_count(), 7);
            // check edge view -- note that it's not actually currently possible to read from
            // this for a lack of key (the value would be the key). Hence, the view also has a
            // bogokey column.
            let edge_view = get_parent_node(&inc, mig, &res.unwrap());
            assert_eq!(
                edge_view
                    .columns()
                    .iter()
                    .map(|c| c.name())
                    .collect::<Vec<_>>(),
                &["sum", "bogokey"]
            );
            assert_eq!(edge_view.description(true), "π[1, lit: 0]");
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore] // find_and_merge_filter_aggregates currently disabled
    async fn it_merges_filter_and_sum() {
        // set up graph
        let mut g = integration_utils::start_simple_unsharded("it_merges_filter_and_sum").await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            // Establish a base write type
            assert!(inc
                .add_table(
                    inc.rewrite(
                        parse_create_table(
                            Dialect::MySQL,
                            "CREATE TABLE votes (userid int, aid int, sign int);"
                        )
                        .unwrap(),
                        &[],
                        DataDialect::DEFAULT_MYSQL,
                        None,
                    )
                    .unwrap(),
                    mig,
                )
                .is_ok());
            // Should have source and "users" base table node
            assert_eq!(mig.graph().node_count(), 2);
            assert_eq!(
                get_node(&inc, mig, &"votes".into()).name(),
                &Relation::from("votes")
            );
            assert_eq!(
                get_node(&inc, mig, &"votes".into())
                    .columns()
                    .iter()
                    .map(|c| c.name())
                    .collect::<Vec<_>>(),
                &["userid", "aid", "sign"]
            );
            assert!(get_node(&inc, mig, &"votes".into()).is_base());
            let res = inc.add_query(
                None,
                inc.rewrite(
                    parse_select_statement(
                        Dialect::MySQL,
                        "SELECT SUM(sign) AS sum FROM votes WHERE aid=5 GROUP BY votes.userid;",
                    )
                    .unwrap(),
                    &[],
                    DataDialect::DEFAULT_MYSQL,
                    None,
                )
                .unwrap(),
                mig,
            );
            assert!(res.is_ok());
            // note: the FunctionExpr isn't a sumfilter because it takes the hash before
            // merging
            let qid = query_id_hash(
                &["votes"],
                &[&Column::from("votes.userid"), &Column::from("votes.aid")],
                &[&Column {
                    name: "sum".into(),
                    table: None,
                }],
            );

            let agg_view = get_node(&inc, mig, &format!("q_{:x}_n1_p0_f0_filteragg", qid).into());
            assert_eq!(
                agg_view
                    .columns()
                    .iter()
                    .map(|c| c.name())
                    .collect::<Vec<_>>(),
                &["userid", "aid", "sum"]
            );
            assert_eq!(agg_view.description(true), "𝛴(σ(2)) γ[0, 1]");
            // check edge view -- note that it's not actually currently possible to read from
            // this for a lack of key (the value would be the key). Hence, the view also has a
            // bogokey column.
            let edge_view = get_node(&inc, mig, &res.unwrap());
            assert_eq!(
                edge_view
                    .columns()
                    .iter()
                    .map(|c| c.name())
                    .collect::<Vec<_>>(),
                &["sum", "bogokey"]
            );
            assert_eq!(edge_view.description(true), "π[2, lit: 0]");
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore] // find_and_merge_filter_aggregates currently disabled
    async fn it_merges_filter_and_sum_on_filter_column() {
        // set up graph
        let mut g =
            integration_utils::start_simple_unsharded("it_merges_filter_and_sum_on_filter_column")
                .await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            // Establish a base write type
            assert!(inc
                .add_table(
                    inc.rewrite(
                        parse_create_table(
                            Dialect::MySQL,
                            "CREATE TABLE votes (userid int, aid int, sign int);"
                        )
                        .unwrap(),
                        &[],
                        DataDialect::DEFAULT_MYSQL,
                        None,
                    )
                    .unwrap(),
                    mig,
                )
                .is_ok());
            // Should have source and "users" base table node
            assert_eq!(mig.graph().node_count(), 2);
            assert_eq!(
                get_node(&inc, mig, &"votes".into()).name(),
                &Relation::from("votes")
            );
            assert_eq!(
                get_node(&inc, mig, &"votes".into())
                    .columns()
                    .iter()
                    .map(|c| c.name())
                    .collect::<Vec<_>>(),
                &["userid", "aid", "sign"]
            );
            assert!(get_node(&inc, mig, &"votes".into()).is_base());
            let res = inc.add_query(
                None,
                inc.rewrite(
                    parse_select_statement(
                        Dialect::MySQL,
                        "SELECT SUM(sign) AS sum FROM votes WHERE sign > 0 GROUP BY votes.userid;",
                    )
                    .unwrap(),
                    &[],
                    DataDialect::DEFAULT_MYSQL,
                    None,
                )
                .unwrap(),
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
        let mut g = integration_utils::start_simple_unsharded(
            "it_doesnt_merge_sum_and_filter_on_sum_result",
        )
        .await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            // Establish a base write type
            assert!(inc
                .add_table(
                    inc.rewrite(
                        parse_create_table(
                            Dialect::MySQL,
                            "CREATE TABLE votes (userid int, aid int, sign int);"
                        )
                        .unwrap(),
                        &[],
                        DataDialect::DEFAULT_MYSQL,
                        None,
                    )
                    .unwrap(),
                    mig,
                )
                .is_ok());
            // Should have source and "users" base table node
            assert_eq!(mig.graph().node_count(), 2);
            assert_eq!(
                get_node(&inc, mig, &"votes".into()).name(),
                &Relation::from("votes")
            );
            assert_eq!(
                get_node(&inc, mig, &"votes".into())
                    .columns()
                    .iter()
                    .map(|c| c.name())
                    .collect::<Vec<_>>(),
                &["userid", "aid", "sign"]
            );
            assert!(get_node(&inc, mig, &"votes".into()).is_base());
            let res = inc.add_query(
                None,
                inc.rewrite(
                    parse_select_statement(
                        Dialect::MySQL,
                        "SELECT SUM(sign) AS sum FROM votes GROUP BY votes.userid HAVING sum>0 ;",
                    )
                    .unwrap(),
                    &[],
                    DataDialect::DEFAULT_MYSQL,
                    None,
                )
                .unwrap(),
                mig,
            );
            assert!(res.is_ok());
            // added a project for the case, the aggregation, a project helper, the edge view, and
            // reader
            assert_eq!(mig.graph().node_count(), 6);
            // check edge view -- note that it's not actually currently possible to read from
            // this for a lack of key (the value would be the key). Hence, the view also has a
            // bogokey column.
            let edge_view = get_node(&inc, mig, &res.unwrap());
            assert_eq!(
                edge_view
                    .columns()
                    .iter()
                    .map(|c| c.name())
                    .collect::<Vec<_>>(),
                &["sum", "bogokey"]
            );
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
        let mut g = integration_utils::start_simple_unsharded(
            "it_incorporates_aggregation_filter_sum_else",
        )
        .await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            // Establish a base write type
            assert!(inc
                .add_table(
                    inc.rewrite(
                        parse_create_table(
                            Dialect::MySQL,
                            "CREATE TABLE votes (story_id int, comment_id int, vote int);"
                        )
                        .unwrap(),
                        &[],
                        DataDialect::DEFAULT_MYSQL,
                        None,
                    )
                    .unwrap(),
                    mig,
                )
                .is_ok());
            // Should have source and "users" base table node
            assert_eq!(mig.graph().node_count(), 2);
            assert_eq!(
                get_node(&inc, mig, &"votes".into()).name(),
                &Relation::from("votes")
            );
            assert_eq!(
                get_node(&inc, mig, &"votes".into())
                    .columns()
                    .iter()
                    .map(|c| c.name())
                    .collect::<Vec<_>>(),
                &["story_id", "comment_id", "vote"]
            );
            assert!(get_node(&inc, mig, &"votes".into()).is_base());
            // Try a simple COUNT function
            let res = inc.add_query(
                None,
                parse_select_statement(
                    Dialect::MySQL,
                    "SELECT
                    COUNT(CASE WHEN votes.story_id IS NULL AND votes.vote = 0 THEN votes.vote END) \
                    as votes
                    FROM votes
                    GROUP BY votes.comment_id;",
                )
                .unwrap(),
                mig,
            );
            assert!(res.is_ok(), "!{:?}.is_ok()", res);
            // added a project for the case, the aggregation, a project helper, the edge view, a
            // reorder projection, and reader
            assert_eq!(mig.graph().node_count(), 7);

            // check edge view -- note that it's not actually currently possible to read from
            // this for a lack of key (the value would be the key). Hence, the view also has a
            // bogokey column.
            let edge_view = get_parent_node(&inc, mig, &res.unwrap());
            assert_eq!(
                edge_view
                    .columns()
                    .iter()
                    .map(|c| c.name())
                    .collect::<Vec<_>>(),
                &["votes", "bogokey"]
            );
            assert_eq!(edge_view.description(true), "π[1, lit: 0]");
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn it_incorporates_explicit_multi_join() {
        // set up graph
        let mut g =
            integration_utils::start_simple_unsharded("it_incorporates_explicit_multi_join").await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            // Establish base write types for "users" and "articles" and "votes"
            assert!(inc
                .add_table(
                    inc.rewrite(
                        parse_create_table(
                            Dialect::MySQL,
                            "CREATE TABLE users (id int, name varchar(40));"
                        )
                        .unwrap(),
                        &[],
                        DataDialect::DEFAULT_MYSQL,
                        None,
                    )
                    .unwrap(),
                    mig,
                )
                .is_ok());
            assert!(inc
                .add_table(
                    inc.rewrite(
                        parse_create_table(
                            Dialect::MySQL,
                            "CREATE TABLE votes (aid int, uid int);"
                        )
                        .unwrap(),
                        &[],
                        DataDialect::DEFAULT_MYSQL,
                        None,
                    )
                    .unwrap(),
                    mig
                )
                .is_ok());
            assert!(inc
                .add_table(
                    inc.rewrite(
                        parse_create_table(
                            Dialect::MySQL,
                            "CREATE TABLE articles (aid int, title varchar(255), author int);"
                        )
                        .unwrap(),
                        &[],
                        DataDialect::DEFAULT_MYSQL,
                        None,
                    )
                    .unwrap(),
                    mig,
                )
                .is_ok());

            // Try an explicit multi-way-join
            let q = "SELECT users.name, articles.title, votes.uid \
                 FROM articles
                 JOIN users ON (users.id = articles.author) \
                 JOIN votes ON (votes.aid = articles.aid);";

            let res = inc.add_query(
                None,
                inc.rewrite(
                    parse_select_statement(Dialect::MySQL, q).unwrap(),
                    &[],
                    DataDialect::DEFAULT_MYSQL,
                    None,
                )
                .unwrap(),
                mig,
            );
            assert!(res.is_ok());
            let name = res.unwrap();
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
            let leaf_view = get_node(&inc, mig, &name);
            assert_eq!(
                leaf_view
                    .columns()
                    .iter()
                    .map(|c| c.name())
                    .collect::<Vec<_>>(),
                &["name", "title", "uid", "bogokey"]
            );
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn it_incorporates_implicit_multi_join() {
        // set up graph
        let mut g =
            integration_utils::start_simple_unsharded("it_incorporates_implicit_multi_join").await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            // Establish base write types for "users" and "articles" and "votes"
            assert!(inc
                .add_table(
                    inc.rewrite(
                        parse_create_table(
                            Dialect::MySQL,
                            "CREATE TABLE users (id int, name varchar(40));"
                        )
                        .unwrap(),
                        &[],
                        DataDialect::DEFAULT_MYSQL,
                        None,
                    )
                    .unwrap(),
                    mig,
                )
                .is_ok());
            assert!(inc
                .add_table(
                    inc.rewrite(
                        parse_create_table(
                            Dialect::MySQL,
                            "CREATE TABLE votes (aid int, uid int);"
                        )
                        .unwrap(),
                        &[],
                        DataDialect::DEFAULT_MYSQL,
                        None,
                    )
                    .unwrap(),
                    mig
                )
                .is_ok());
            assert!(inc
                .add_table(
                    inc.rewrite(
                        parse_create_table(
                            Dialect::MySQL,
                            "CREATE TABLE articles (aid int, title varchar(255), author int);"
                        )
                        .unwrap(),
                        &[],
                        DataDialect::DEFAULT_MYSQL,
                        None,
                    )
                    .unwrap(),
                    mig,
                )
                .is_ok());

            // Try an implicit multi-way-join
            let q = "SELECT users.name, articles.title, votes.uid \
                 FROM articles, users, votes
                 WHERE users.id = articles.author \
                 AND votes.aid = articles.aid;";

            let res = inc.add_query(
                None,
                inc.rewrite(
                    parse_select_statement(Dialect::MySQL, q).unwrap(),
                    &[],
                    DataDialect::DEFAULT_MYSQL,
                    None,
                )
                .unwrap(),
                mig,
            );
            assert!(res.is_ok());
            let name = res.unwrap();
            // XXX(malte): below over-projects into the final leaf, and is thus inconsistent
            // with the explicit JOIN case!

            // leaf view
            let leaf_view = get_node(&inc, mig, &name);
            assert_eq!(
                leaf_view
                    .columns()
                    .iter()
                    .map(|c| c.name())
                    .collect::<Vec<_>>(),
                &["name", "title", "uid", "bogokey"]
            );
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore]
    async fn it_incorporates_join_projecting_join_columns() {
        // set up graph
        let mut g = integration_utils::start_simple_unsharded(
            "it_incorporates_join_projecting_join_columns",
        )
        .await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            assert!(inc
                .add_table(
                    inc.rewrite(
                        parse_create_table(
                            Dialect::MySQL,
                            "CREATE TABLE users (id int, name varchar(40));"
                        )
                        .unwrap(),
                        &[],
                        DataDialect::DEFAULT_MYSQL,
                        None,
                    )
                    .unwrap(),
                    mig,
                )
                .is_ok());
            assert!(inc
                .add_table(
                    inc.rewrite(
                        parse_create_table(
                            Dialect::MySQL,
                            "CREATE TABLE articles (id int, author int, title varchar(255));"
                        )
                        .unwrap(),
                        &[],
                        DataDialect::DEFAULT_MYSQL,
                        None,
                    )
                    .unwrap(),
                    mig,
                )
                .is_ok());
            let q = "SELECT users.id, users.name, articles.author, articles.title \
                     FROM articles, users \
                     WHERE users.id = articles.author;";
            let q = inc.add_query(
                None,
                inc.rewrite(
                    parse_select_statement(Dialect::MySQL, q).unwrap(),
                    &[],
                    DataDialect::DEFAULT_MYSQL,
                    None,
                )
                .unwrap(),
                mig,
            );
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
            let new_join_view = get_node(&inc, mig, &format!("q_{:x}_n0", qid).into());
            assert_eq!(
                new_join_view
                    .columns()
                    .iter()
                    .map(|c| c.name())
                    .collect::<Vec<_>>(),
                &["id", "author", "title", "name"]
            );
            // leaf node
            let new_leaf_view = get_node(&inc, mig, &q.unwrap().into());
            assert_eq!(
                new_leaf_view
                    .columns()
                    .iter()
                    .map(|c| c.name())
                    .collect::<Vec<_>>(),
                &["id", "name", "author", "title", "bogokey"]
            );
            assert_eq!(new_leaf_view.description(true), "π[1, 3, 1, 2, lit: 0]");
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn it_incorporates_self_join() {
        // set up graph
        let mut g = integration_utils::start_simple_unsharded("it_incorporates_self_join").await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            assert!(inc
                .add_table(
                    inc.rewrite(
                        parse_create_table(
                            Dialect::MySQL,
                            "CREATE TABLE friends (id int, friend int);"
                        )
                        .unwrap(),
                        &[],
                        DataDialect::DEFAULT_MYSQL,
                        None,
                    )
                    .unwrap(),
                    mig
                )
                .is_ok());

            // Try a friends-of-friends type computation via self join
            let q = "SELECT f1.id, f2.friend AS fof \
                     FROM friends AS f1 \
                     JOIN (SELECT * FROM friends) AS f2 ON (f1.friend = f2.id)
                     WHERE f1.id = ?;";

            let res = inc.add_query(
                None,
                inc.rewrite(
                    parse_select_statement(Dialect::MySQL, q).unwrap(),
                    &[],
                    DataDialect::DEFAULT_MYSQL,
                    None,
                )
                .unwrap(),
                mig,
            );
            assert!(res.is_ok(), "{}", res.as_ref().unwrap_err());
            let name = res.unwrap();

            // Check leaf projection node
            let leaf_view = get_parent_node(&inc, mig, &name);
            assert_eq!(
                leaf_view
                    .columns()
                    .iter()
                    .map(|c| c.name())
                    .collect::<Vec<_>>(),
                &["id", "fof"]
            );
            assert_eq!(leaf_view.description(true), "π[0, 2]");
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn it_incorporates_literal_projection() {
        // set up graph
        let mut g =
            integration_utils::start_simple_unsharded("it_incorporates_literal_projection").await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            assert!(inc
                .add_table(
                    inc.rewrite(
                        parse_create_table(
                            Dialect::MySQL,
                            "CREATE TABLE users (id int, name varchar(40));"
                        )
                        .unwrap(),
                        &[],
                        DataDialect::DEFAULT_MYSQL,
                        None,
                    )
                    .unwrap(),
                    mig,
                )
                .is_ok());

            let res = inc.add_query(
                None,
                inc.rewrite(
                    parse_select_statement(Dialect::MySQL, "SELECT users.name, 1 FROM users;")
                        .unwrap(),
                    &[],
                    DataDialect::DEFAULT_MYSQL,
                    None,
                )
                .unwrap(),
                mig,
            );
            assert!(res.is_ok());

            // leaf view node
            let edge = get_parent_node(&inc, mig, &res.unwrap());
            assert_eq!(
                edge.columns().iter().map(|c| c.name()).collect::<Vec<_>>(),
                &["name", "1", "bogokey"]
            );
            assert_eq!(edge.description(true), "π[1, lit: 1, lit: 0]");
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn it_incorporates_arithmetic_projection() {
        // set up graph
        let mut g =
            integration_utils::start_simple_unsharded("it_incorporates_arithmetic_projection")
                .await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            assert!(inc
                .add_table(
                    inc.rewrite(
                        parse_create_table(Dialect::MySQL, "CREATE TABLE users (id int, age int);")
                            .unwrap(),
                        &[],
                        DataDialect::DEFAULT_MYSQL,
                        None,
                    )
                    .unwrap(),
                    mig
                )
                .is_ok());

            let res = inc.add_query(
                None,
                inc.rewrite(
                    parse_select_statement(
                        Dialect::MySQL,
                        "SELECT 2 * users.age, 2 * 10 as twenty FROM users;",
                    )
                    .unwrap(),
                    &[],
                    DataDialect::DEFAULT_MYSQL,
                    None,
                )
                .unwrap(),
                mig,
            );
            assert!(res.is_ok());

            // leaf view node
            let edge = get_parent_node(&inc, mig, &res.unwrap());

            assert_eq!(
                edge.columns().iter().map(|c| c.name()).collect::<Vec<_>>(),
                &["(2 * `users`.`age`)", "twenty", "bogokey"]
            );
            assert_eq!(edge.description(true), "π[((lit: 2) * 1), lit: 20, lit: 0]");
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn it_incorporates_arithmetic_projection_with_parameter_column() {
        // set up graph
        let mut g = integration_utils::start_simple_unsharded(
            "it_incorporates_arithmetic_projection_with_parameter_column",
        )
        .await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            assert!(inc
                .add_table(
                    inc.rewrite(
                        parse_create_table(
                            Dialect::MySQL,
                            "CREATE TABLE users (id int, age int, name varchar(40));"
                        )
                        .unwrap(),
                        &[],
                        DataDialect::DEFAULT_MYSQL,
                        None,
                    )
                    .unwrap(),
                    mig,
                )
                .is_ok());

            let res = inc.add_query(
                None,
                inc.rewrite(
                    parse_select_statement(
                        Dialect::MySQL,
                        "SELECT 2 * users.age, 2 * 10 AS twenty FROM users WHERE users.name = ?;",
                    )
                    .unwrap(),
                    &[],
                    DataDialect::DEFAULT_MYSQL,
                    None,
                )
                .unwrap(),
                mig,
            );
            assert!(res.is_ok());
            let name = res.unwrap();

            // Check projection node
            let node = get_parent_node(&inc, mig, &name);
            assert_eq!(
                node.columns().iter().map(|c| c.name()).collect::<Vec<_>>(),
                &["name", "(2 * `users`.`age`)", "twenty"]
            );
            assert_eq!(node.description(true), "π[2, ((lit: 2) * 1), lit: 20]");

            // Check reader
            let n = get_reader(&inc, mig, &name);
            assert_eq!(n.as_reader().unwrap().key().unwrap(), &[2]);
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn it_incorporates_join_with_nested_query() {
        readyset_tracing::init_test_logging();
        let mut g =
            integration_utils::start_simple_unsharded("it_incorporates_join_with_nested_query")
                .await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            assert!(inc
                .add_table(
                    inc.rewrite(
                        parse_create_table(
                            Dialect::MySQL,
                            "CREATE TABLE users (id int, name varchar(40));"
                        )
                        .unwrap(),
                        &[],
                        DataDialect::DEFAULT_MYSQL,
                        None,
                    )
                    .unwrap(),
                    mig,
                )
                .is_ok());
            assert!(inc
                .add_table(
                    inc.rewrite(
                        parse_create_table(
                            Dialect::MySQL,
                            "CREATE TABLE articles (id int, author int, title varchar(255));"
                        )
                        .unwrap(),
                        &[],
                        DataDialect::DEFAULT_MYSQL,
                        None,
                    )
                    .unwrap(),
                    mig,
                )
                .is_ok());

            let q = "SELECT nested_users.name, articles.title \
                     FROM articles \
                     JOIN (SELECT * FROM users) AS nested_users \
                     ON (nested_users.id = articles.author);";
            let name = inc
                .add_query(
                    None,
                    inc.rewrite(
                        parse_select_statement(Dialect::MySQL, q).unwrap(),
                        &[],
                        DataDialect::DEFAULT_MYSQL,
                        None,
                    )
                    .unwrap(),
                    mig,
                )
                .unwrap();

            // leaf node
            let new_leaf_view = get_parent_node(&inc, mig, &name);
            assert_eq!(
                new_leaf_view
                    .columns()
                    .iter()
                    .map(|c| c.name())
                    .collect::<Vec<_>>(),
                &["name", "title", "bogokey"]
            );
            assert_eq!(new_leaf_view.description(true), "π[3, 2, lit: 0]");
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn it_incorporates_join_with_reused_nested_query() {
        let mut g = integration_utils::start_simple_unsharded(
            "it_incorporates_join_with_reused_nested_query",
        )
        .await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            assert!(inc
                .add_table(
                    inc.rewrite(
                        parse_create_table(
                            Dialect::MySQL,
                            "CREATE TABLE users (id int, name varchar(40));"
                        )
                        .unwrap(),
                        &[],
                        DataDialect::DEFAULT_MYSQL,
                        None,
                    )
                    .unwrap(),
                    mig,
                )
                .is_ok());
            assert!(inc
                .add_table(
                    inc.rewrite(
                        parse_create_table(
                            Dialect::MySQL,
                            "CREATE TABLE articles (id int, author int, title varchar(255));"
                        )
                        .unwrap(),
                        &[],
                        DataDialect::DEFAULT_MYSQL,
                        None,
                    )
                    .unwrap(),
                    mig,
                )
                .is_ok());

            // Add a simple query on users, which will be duplicated in the subquery below.
            assert!(inc
                .add_query(
                    None,
                    inc.rewrite(
                        parse_select_statement(Dialect::MySQL, "SELECT * FROM users;").unwrap(),
                        &[],
                        DataDialect::DEFAULT_MYSQL,
                        None,
                    )
                    .unwrap(),
                    mig
                )
                .is_ok());

            // Ensure that the JOIN with nested_users still works as expected, even though an
            // an identical query already exists with a different name.
            let q = "SELECT nested_users.name, articles.title \
                     FROM articles \
                     JOIN (SELECT * FROM users) AS nested_users \
                     ON (nested_users.id = articles.author);";
            let q = inc.add_query(
                None,
                inc.rewrite(
                    parse_select_statement(Dialect::MySQL, q).unwrap(),
                    &[],
                    DataDialect::DEFAULT_MYSQL,
                    None,
                )
                .unwrap(),
                mig,
            );
            assert!(q.is_ok());

            // leaf node
            let new_leaf_view = get_parent_node(&inc, mig, &q.unwrap());
            assert_eq!(
                new_leaf_view
                    .columns()
                    .iter()
                    .map(|c| c.name())
                    .collect::<Vec<_>>(),
                &["name", "title", "bogokey"]
            );
            assert_eq!(new_leaf_view.description(true), "π[3, 2, lit: 0]");
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "Unions not currently supported (ENG-1605)"]
    async fn it_incorporates_compound_selection() {
        // set up graph
        let mut g =
            integration_utils::start_simple_unsharded("it_incorporates_compound_selection").await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            assert!(inc
                .add_table(
                    inc.rewrite(
                        parse_create_table(
                            Dialect::MySQL,
                            "CREATE TABLE users (id int, name varchar(40));"
                        )
                        .unwrap(),
                        &[],
                        DataDialect::DEFAULT_MYSQL,
                        None,
                    )
                    .unwrap(),
                    mig,
                )
                .is_ok());

            let res = inc.add_query(
                None,
                parse_select_statement(
                    Dialect::MySQL,
                    "SELECT users.id, users.name FROM users \
                 WHERE users.id = 32 \
                 UNION \
                 SELECT users.id, users.name FROM users \
                 WHERE users.id = 42 AND users.name = 'bob';",
                )
                .unwrap(),
                mig,
            );
            assert!(res.is_ok());

            // the leaf of this query (node above the reader) is a union
            let union_view = get_node(&inc, mig, &res.unwrap());
            assert_eq!(
                union_view
                    .columns()
                    .iter()
                    .map(|c| c.name())
                    .collect::<Vec<_>>(),
                &["id", "name"]
            );
            assert_eq!(union_view.description(true), "3:[0, 1] ⋃ 6:[0, 1]");
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn it_distinguishes_predicates() {
        // set up graph
        let mut g = integration_utils::start_simple_unsharded("it_distinguishes_predicates").await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            // Establish a base write type
            assert!(inc
                .add_table(
                    inc.rewrite(
                        parse_create_table(
                            Dialect::MySQL,
                            "CREATE TABLE users (id int, name varchar(40));"
                        )
                        .unwrap(),
                        &[],
                        DataDialect::DEFAULT_MYSQL,
                        None,
                    )
                    .unwrap(),
                    mig,
                )
                .is_ok());
            // Should have source and "users" base table node
            assert_eq!(mig.graph().node_count(), 2);
            assert_eq!(
                get_node(&inc, mig, &"users".into()).name(),
                &Relation::from("users")
            );
            assert_eq!(
                get_node(&inc, mig, &"users".into())
                    .columns()
                    .iter()
                    .map(|c| c.name())
                    .collect::<Vec<_>>(),
                &["id", "name"]
            );
            assert!(get_node(&inc, mig, &"users".into()).is_base());

            // Add a new query
            let res = inc.add_query(
                None,
                inc.rewrite(
                    parse_select_statement(
                        Dialect::MySQL,
                        "SELECT id, name FROM users WHERE users.id = 42;",
                    )
                    .unwrap(),
                    &[],
                    DataDialect::DEFAULT_MYSQL,
                    None,
                )
                .unwrap(),
                mig,
            );
            assert!(res.is_ok());

            // Add query with a different predicate
            let ncount = mig.graph().node_count();
            let res = inc.add_query(
                None,
                inc.rewrite(
                    parse_select_statement(
                        Dialect::MySQL,
                        "SELECT id, name FROM users WHERE users.id = 50;",
                    )
                    .unwrap(),
                    &[],
                    DataDialect::DEFAULT_MYSQL,
                    None,
                )
                .unwrap(),
                mig,
            );
            assert!(res.is_ok());
            // should have added three more nodes (filter, project, reorder project and reader)
            assert_eq!(mig.graph().node_count(), ncount + 4);
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn it_adds_topk() {
        let mut g = integration_utils::start_simple_unsharded("it_adds_topk").await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            inc.set_mir_config(super::mir::Config {
                allow_topk: true,
                ..Default::default()
            });
            inc.add_table(
                inc.rewrite(
                    parse_create_table(Dialect::MySQL, "CREATE TABLE things (id int primary key);")
                        .unwrap(),
                    &[],
                    DataDialect::DEFAULT_MYSQL,
                    None,
                )
                .unwrap(),
                mig,
            )
            .unwrap();
            // source -> things
            assert_eq!(mig.graph().node_count(), 2);

            inc.add_query(
                Some("things_by_id_limit_3".into()),
                inc.rewrite(
                    parse_select_statement(
                        Dialect::MySQL,
                        "SELECT * FROM things ORDER BY id LIMIT 3",
                    )
                    .unwrap(),
                    &[],
                    DataDialect::DEFAULT_MYSQL,
                    None,
                )
                .unwrap(),
                mig,
            )
            .unwrap();

            // source -> things -> project bogokey -> topk -> project_columns -> project reorder ->
            // leaf
            assert_eq!(mig.graph().node_count(), 7);
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore]
    async fn it_queries_over_aliased_view() {
        let mut g = integration_utils::start_simple_unsharded("it_queries_over_aliased_view").await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            assert!(inc
                .add_table(
                    inc.rewrite(
                        parse_create_table(
                            Dialect::MySQL,
                            "CREATE TABLE users (id int, name varchar(40));"
                        )
                        .unwrap(),
                        &[],
                        DataDialect::DEFAULT_MYSQL,
                        None,
                    )
                    .unwrap(),
                    mig,
                )
                .is_ok());
            // Add first copy of new query, called "tq1"
            let res = inc.add_query(
                Some("tq1".into()),
                inc.rewrite(
                    parse_select_statement(
                        Dialect::MySQL,
                        "SELECT id, name FROM users WHERE users.id = 42;",
                    )
                    .unwrap(),
                    &[],
                    DataDialect::DEFAULT_MYSQL,
                    None,
                )
                .unwrap(),
                mig,
            );
            assert!(res.is_ok());

            // Add the same query again, this time as "tq2"
            let ncount = mig.graph().node_count();
            let res = inc.add_query(
                Some("tq2".into()),
                inc.rewrite(
                    parse_select_statement(
                        Dialect::MySQL,
                        "SELECT id, name FROM users WHERE users.id = 42;",
                    )
                    .unwrap(),
                    &[],
                    DataDialect::DEFAULT_MYSQL,
                    None,
                )
                .unwrap(),
                mig,
            );
            assert!(res.is_ok());
            assert_eq!(mig.graph().node_count(), ncount);

            // Add a query over tq2, which really is tq1
            let _res = inc.add_query(
                Some("over_tq2".into()),
                inc.rewrite(
                    parse_select_statement(Dialect::MySQL, "SELECT tq2.id FROM tq2;").unwrap(),
                    &[],
                    DataDialect::DEFAULT_MYSQL,
                    None,
                )
                .unwrap(),
                mig,
            );
            // should have added a projection and a reader
            assert_eq!(mig.graph().node_count(), ncount + 2);
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn infers_type_for_topk() {
        // set up graph
        let mut g = integration_utils::start_simple_unsharded("infers_type_for_topk").await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            inc.set_mir_config(super::mir::Config {
                allow_topk: true,
                ..Default::default()
            });
            // Must have a base node for type inference to work, so make one manually
            assert!(inc
                .add_table(
                    inc.rewrite(
                        parse_create_table(
                            Dialect::MySQL,
                            "CREATE TABLE t1 (a int, b float, c Text);"
                        )
                        .unwrap(),
                        &[],
                        DataDialect::DEFAULT_MYSQL,
                        None,
                    )
                    .unwrap(),
                    mig
                )
                .is_ok());

            let _ = inc
                .add_query(
                    None,
                    inc.rewrite(
                        parse_select_statement(Dialect::MySQL, "SELECT t1.a from t1 LIMIT 3")
                            .unwrap(),
                        &[],
                        DataDialect::DEFAULT_MYSQL,
                        None,
                    )
                    .unwrap(),
                    mig,
                )
                .unwrap();

            let g = &mig.dataflow_state.ingredients;
            g.node_indices().for_each(|idx| {
                if matches!(g[idx].as_internal(), Some(NodeOperator::TopK(_))) {
                    let text = DfType::DEFAULT_TEXT;
                    let truth = vec![
                        &DfType::Int,    // a
                        &DfType::Float,  // b
                        &text,           // c
                        &DfType::BigInt, // bogokey projection
                    ];
                    let types = g[idx].columns().iter().map(|c| c.ty()).collect::<Vec<_>>();
                    assert_eq!(truth, types);
                }
            });
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn infers_type_for_filter() {
        // set up graph
        let mut g = integration_utils::start_simple_unsharded("infers_type_for_filter").await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            inc.set_mir_config(super::mir::Config {
                allow_topk: true,
                ..Default::default()
            });
            // Must have a base node for type inference to work, so make one manually
            assert!(inc
                .add_table(
                    inc.rewrite(
                        parse_create_table(
                            Dialect::MySQL,
                            "CREATE TABLE t1 (a int, b float, c Text);"
                        )
                        .unwrap(),
                        &[],
                        DataDialect::DEFAULT_MYSQL,
                        None,
                    )
                    .unwrap(),
                    mig
                )
                .is_ok());

            let _ = inc
                .add_query(
                    None,
                    inc.rewrite(
                        parse_select_statement(
                            Dialect::MySQL,
                            "SELECT t1.a from t1 where t1.a = t1.b",
                        )
                        .unwrap(),
                        &[],
                        DataDialect::DEFAULT_MYSQL,
                        None,
                    )
                    .unwrap(),
                    mig,
                )
                .unwrap();

            let g = &mig.dataflow_state.ingredients;
            g.node_indices().for_each(|idx| {
                if matches!(g[idx].as_internal(), Some(NodeOperator::Filter(_))) {
                    let text = DfType::DEFAULT_TEXT;
                    let truth = vec![
                        &DfType::Int,   // a
                        &DfType::Float, // b
                        &text,          // c
                    ];
                    let types = g[idx].columns().iter().map(|c| c.ty()).collect::<Vec<_>>();
                    assert_eq!(truth, types);
                }
            });
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn infers_type_for_grouped() {
        // set up graph
        let mut g = integration_utils::start_simple_unsharded("infers_type_for_grouped").await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            inc.set_mir_config(super::mir::Config {
                allow_topk: true,
                ..Default::default()
            });
            // Must have a base node for type inference to work, so make one manually
            assert!(inc
                .add_table(
                    inc.rewrite(
                        parse_create_table(
                            Dialect::MySQL,
                            "CREATE TABLE t1 (a int, b float, c Text, d Text);"
                        )
                        .unwrap(),
                        &[],
                        DataDialect::DEFAULT_MYSQL,
                        None,
                    )
                    .unwrap(),
                    mig,
                )
                .is_ok());

            let _ = inc
                .add_query(
                    None,
                    inc.rewrite(
                        parse_select_statement(
                            Dialect::MySQL,
                            "SELECT sum(t1.a), max(t1.b), group_concat(c separator ' ') from t1",
                        )
                        .unwrap(),
                        &[],
                        DataDialect::DEFAULT_MYSQL,
                        None,
                    )
                    .unwrap(),
                    mig,
                )
                .unwrap();

            let g = &mig.dataflow_state.ingredients;
            g.node_indices().for_each(|idx| {
                if matches!(g[idx].as_internal(), Some(NodeOperator::Aggregation(_))) {
                    let truth = vec![
                        &DfType::BigInt,          // bogokey
                        &DfType::DEFAULT_NUMERIC, // sum(t1.a)
                    ];
                    let types = g[idx].columns().iter().map(|c| c.ty()).collect::<Vec<_>>();
                    assert_eq!(truth, types);
                } else if matches!(g[idx].as_internal(), Some(NodeOperator::Extremum(_))) {
                    let truth = vec![
                        &DfType::BigInt, // bogokey
                        &DfType::Float,  // max(t1.b)
                    ];
                    let types = g[idx].columns().iter().map(|c| c.ty()).collect::<Vec<_>>();
                    assert_eq!(truth, types);
                } else if matches!(g[idx].as_internal(), Some(NodeOperator::Concat(_))) {
                    let text = DfType::DEFAULT_TEXT;
                    let truth = vec![
                        &DfType::BigInt, // bogokey
                        &text,           // group_concat()
                    ];
                    let types = g[idx].columns().iter().map(|c| c.ty()).collect::<Vec<_>>();
                    assert_eq!(truth, types);
                }
            });
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn infers_type_for_join() {
        // set up graph
        let mut g = integration_utils::start_simple_unsharded("infers_type_for_join").await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            inc.set_mir_config(super::mir::Config {
                allow_topk: true,
                ..Default::default()
            });
            // Must have a base node for type inference to work, so make one manually
            assert!(inc
                .add_table(
                    inc.rewrite(
                        parse_create_table(
                            Dialect::MySQL,
                            "CREATE TABLE t1 (a int, b float, c Text);"
                        )
                        .unwrap(),
                        &[],
                        DataDialect::DEFAULT_MYSQL,
                        None,
                    )
                    .unwrap(),
                    mig
                )
                .is_ok());
            assert!(inc
                .add_table(
                    inc.rewrite(
                        parse_create_table(
                            Dialect::MySQL,
                            "CREATE TABLE t2 (a int, b float, c Text);"
                        )
                        .unwrap(),
                        &[],
                        DataDialect::DEFAULT_MYSQL,
                        None,
                    )
                    .unwrap(),
                    mig
                )
                .is_ok());

            let _ = inc
                .add_query(
                    None,
                    inc.rewrite(
                        parse_select_statement(
                            Dialect::MySQL,
                            "SELECT t1.a, t2.a FROM t1 JOIN t2 on t1.c = t2.c where t2.b = ?",
                        )
                        .unwrap(),
                        &[],
                        DataDialect::DEFAULT_MYSQL,
                        None,
                    )
                    .unwrap(),
                    mig,
                )
                .unwrap();

            let g = &mig.dataflow_state.ingredients;
            g.node_indices().for_each(|idx| {
                if matches!(g[idx].as_internal(), Some(NodeOperator::Join(_))) {
                    let text = DfType::DEFAULT_TEXT;
                    let truth = vec![
                        &DfType::Int,   // t1.a
                        &DfType::Float, // t1.b
                        &text,          // t1.c
                        &DfType::Int,   // t2.a
                        &DfType::Float, // t2.b The rhs of the ON clause is omitted!
                    ];
                    let types = g[idx].columns().iter().map(|c| c.ty()).collect::<Vec<_>>();
                    assert_eq!(truth, types);
                }
            });
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "Unions not currently supported for CREATE CACHE (ENG-1605)"]
    async fn infers_type_for_union() {
        // set up graph
        let mut g = integration_utils::start_simple_unsharded("infers_type_for_union").await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            inc.set_mir_config(super::mir::Config {
                allow_topk: true,
                ..Default::default()
            });
            // Must have a base node for type inference to work, so make one manually
            assert!(inc
                .add_table(
                    inc.rewrite(
                        parse_create_table(
                            Dialect::MySQL,
                            "CREATE TABLE t1 (a int, b float, c Text);"
                        )
                        .unwrap(),
                        &[],
                        DataDialect::DEFAULT_MYSQL,
                        None,
                    )
                    .unwrap(),
                    mig
                )
                .is_ok());
            assert!(inc
                .add_table(
                    inc.rewrite(
                        parse_create_table(
                            Dialect::MySQL,
                            "CREATE TABLE t2 (a int, b float, c Text);"
                        )
                        .unwrap(),
                        &[],
                        DataDialect::DEFAULT_MYSQL,
                        None,
                    )
                    .unwrap(),
                    mig
                )
                .is_ok());

            let _ = inc
                .add_query(
                    None,
                    inc.rewrite(
                        parse_select_statement(
                            Dialect::MySQL,
                            "SELECT t1.a FROM t1 union select t2.a from t2",
                        )
                        .unwrap(),
                        &[],
                        DataDialect::DEFAULT_MYSQL,
                        None,
                    )
                    .unwrap(),
                    mig,
                )
                .unwrap();

            let g = &mig.dataflow_state.ingredients;
            g.node_indices().for_each(|idx| {
                if matches!(g[idx].as_internal(), Some(NodeOperator::Union(_))) {
                    let truth = vec![
                        &DfType::Int, // t1.a + t2.a
                    ];
                    let types = g[idx].columns().iter().map(|c| c.ty()).collect::<Vec<_>>();
                    assert_eq!(truth, types);
                }
            });
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn infers_type_for_project() {
        // set up graph
        let mut g = integration_utils::start_simple_unsharded("infers_type_for_project").await;
        g.migrate(|mig| {
            let mut inc = SqlIncorporator::default();
            inc.set_mir_config(super::mir::Config {
                allow_topk: true,
                ..Default::default()
            });
            // Must have a base node for type inference to work, so make one manually
            assert!(inc
                .add_table(
                    inc.rewrite(
                        parse_create_table(
                            Dialect::MySQL,
                            "CREATE TABLE t1 (a int, b float, c Text);"
                        )
                        .unwrap(),
                        &[],
                        DataDialect::DEFAULT_MYSQL,
                        None,
                    )
                    .unwrap(),
                    mig
                )
                .is_ok());

            let _ = inc
                .add_query(
                    None,
                    inc.rewrite(
                        parse_select_statement(
                            Dialect::MySQL,
                            "SELECT cast(t1.b as char), t1.a, t1.a + 1 from t1",
                        )
                        .unwrap(),
                        &[],
                        DataDialect::DEFAULT_MYSQL,
                        None,
                    )
                    .unwrap(),
                    mig,
                )
                .unwrap();

            let g = &mig.dataflow_state.ingredients;

            let mut indices = g.node_indices();
            indices.next_back(); // Skip reader node
            let project_reorder_node = indices.next_back().unwrap();
            let project_leaf_node = indices.next_back().unwrap();

            assert!(matches!(
                g[project_reorder_node].as_internal(),
                Some(NodeOperator::Project(_))
            ));

            assert!(matches!(
                g[project_leaf_node].as_internal(),
                Some(NodeOperator::Project(_))
            ));

            let truth = vec![
                &DfType::Int, // t1.a
                &DfType::Char(1, Collation::Utf8, DataDialect::DEFAULT_MYSQL), /* cast(t1.b as
                               * char) */
                &DfType::Int,    // t1.a + 1
                &DfType::BigInt, // bogokey
            ];
            let types = g[project_leaf_node]
                .columns()
                .iter()
                .map(|c| c.ty())
                .collect::<Vec<_>>();
            assert_eq!(truth, types);

            let truth = vec![
                &DfType::Char(1, Collation::Utf8, DataDialect::DEFAULT_MYSQL), /* cast(t1.b as
                                                                                * char) */
                &DfType::Int,    // t1.a
                &DfType::Int,    // t1.a + 1
                &DfType::BigInt, // bogokey
            ];
            let types = g[project_reorder_node]
                .columns()
                .iter()
                .map(|c| c.ty())
                .collect::<Vec<_>>();
            assert_eq!(truth, types);
        })
        .await;
    }
}
