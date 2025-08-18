use std::collections::{HashMap, HashSet};
use std::default::Default;
use std::fmt::Debug;
use std::iter;
use std::vec::Vec;

use ::serde::{Deserialize, Serialize};
use catalog_tables::is_catalog_table;
use common::IndexType;
use dataflow::ops::grouped::aggregate::Aggregation;
use dataflow::ops::union;
use dataflow::ops::window::WindowOperationKind;
use lazy_static::lazy_static;
use mir::graph::MirGraph;
use mir::node::node_inner::MirNodeInner;
use mir::node::{GroupedNodeType, MirNode, ProjectExpr, ViewKeyColumn};
use mir::query::{MirBase, MirQuery};
use mir::DfNodeIndex;
pub use mir::{Column, NodeIndex};
use petgraph::visit::Reversed;
use petgraph::Direction;
use readyset_client::ViewPlaceholder;
use readyset_data::Dialect;
use readyset_errors::{
    internal, internal_err, invalid_query, invalid_query_err, invariant, invariant_eq, unsupported,
    unsupported_err, ReadySetError, ReadySetResult,
};
use readyset_sql::analysis::visit::{walk_expr, Visitor};
use readyset_sql::analysis::{self, is_aggregate, ReferredColumns};
use readyset_sql::ast::{
    self, BinaryOperator, CaseWhenBranch, ColumnSpecification, CompoundSelectOperator,
    CreateTableBody, Expr, FieldDefinitionExpr, FieldReference, FunctionExpr, GroupByClause,
    InValue, LimitClause, Literal, NonReplicatedRelation, NullOrder, OrderBy, OrderClause,
    OrderType, Relation, SelectStatement, SqlIdentifier, TableExprInner, TableKey, UnaryOperator,
};
use readyset_sql::DialectDisplay;
use readyset_sql_passes::{is_correlated, outermost_table_exprs};
use readyset_util::redacted::Sensitive;
use tracing::{debug, trace};
use Expr::NestedSelect;

use super::query_graph::{extract_limit_offset, JoinPredicate};
use crate::controller::sql::mir::grouped::{
    make_expressions_above_grouped, make_grouped, make_predicates_above_grouped,
    post_lookup_aggregates,
};
use crate::controller::sql::mir::join::{make_cross_joins, make_joins};
use crate::controller::sql::query_graph::{
    to_query_graph, ExprColumn, OutputColumn, Pagination, QueryGraph,
};
use crate::controller::sql::query_signature::Signature;
use crate::sql::query_graph::WindowFunction;

mod grouped;
mod join;

lazy_static! {
    pub static ref PAGE_NUMBER_COL: SqlIdentifier = "__page_number".into();
}

fn value_columns_needed_for_predicates(
    value_columns: &[OutputColumn],
    predicates: &[Expr],
) -> Vec<(Column, OutputColumn)> {
    let pred_columns: Vec<Column> = predicates
        .iter()
        .flat_map(|p| p.referred_columns())
        .map(|col| col.clone().into())
        .collect();

    value_columns
        .iter()
        .filter_map(|oc| match *oc {
            OutputColumn::Expr(ref ec) => Some((
                Column {
                    name: ec.name.clone(),
                    table: ec.table.clone(),
                    aliases: vec![],
                },
                oc.clone(),
            )),
            OutputColumn::Literal(ref lc) => Some((
                Column {
                    name: lc.name.clone(),
                    table: lc.table.clone(),
                    aliases: vec![],
                },
                oc.clone(),
            )),
            OutputColumn::Data { .. } => None,
        })
        .filter(|(c, _)| pred_columns.contains(c))
        .collect()
}

/// The result of removing a relation from MIR.
#[derive(Default)]
pub struct MirRemovalResult {
    /// The dataflow nodes corresponding to the MIR nodes that
    /// need to be removed.
    pub dataflow_nodes_to_remove: HashSet<DfNodeIndex>,
    /// The relations (tables, queries and views) that were removed
    /// in the process.
    pub relations_removed: HashSet<Relation>,
}

/// Kinds of joins in MIR
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    /// Inner joins - see [`MirNodeInner::InnerJoin`]
    Inner,
    /// Left joins - see [`MirNodeInner::LeftJoin`]
    Left,
    /// Dependent inner joins - see [`MirNodeInner::DependentJoin`]
    DependentInner,
    /// Dependent left joins - see [`MirNodeInner::DependentLeftJoin`]
    DependentLeft,
}

/// Specification for how to treat the leaf node of a query when converting it to MIR
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeafBehavior {
    /// This is an anonymous query - no leaf should be made for it, and the query should not be
    /// registered in the map of relations
    Anonymous,

    /// This query should not have a leaf node, but should be registered in the map of relations.
    /// This is currently the case for SQL VIEWs
    NamedWithoutLeaf,

    /// This query should have a leaf node and should be registered in the map of relations. This
    /// is currently the case for CACHE statements
    Leaf,
}

impl LeafBehavior {
    /// Should we make a Leaf MIR node for this query?
    fn should_make_leaf(self) -> bool {
        self == Self::Leaf
    }

    /// Should we register this query in the map of relations?
    fn should_register(self) -> bool {
        matches!(self, Self::NamedWithoutLeaf | Self::Leaf)
    }
}

/// Configuration for how SQL is converted to MIR
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize, Default)]
pub(crate) struct Config {
    /// If set to `true`, a SQL `ORDER BY` with `LIMIT` will emit a [`TopK`][] node. If set to
    /// `false`, the SQL conversion process returns a [`ReadySetError::Unsupported`], causing the
    /// adapter to send the query to fallback. Defaults to `false`.
    ///
    /// [`TopK`]: MirNodeInner::TopK
    pub(crate) allow_topk: bool,

    /// If set to 'true', a SQL 'ORDER BY' with 'LIMIT' and 'OFFSET' will emit a ['Paginate'][]
    /// node. If set to 'false', the SQL conversion process returns a
    /// ['ReadySetError::Unsupported'], causing the adapter to send the query to fallback. Defaults
    /// to 'false'.
    ///
    /// ['Paginate']: MirNodeInner::Paginate
    pub(crate) allow_paginate: bool,

    /// Enable support for mixing equality and range comparisons in a query. Support for mixed
    /// comparisons is currently unfinished, so these queries may return incorrect results.
    pub(crate) allow_mixed_comparisons: bool,

    /// Enable support for post-lookup (queries which do extra work after the lookup into the
    /// reader)
    #[serde(default)]
    pub(crate) allow_post_lookup: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct SqlToMirConverter {
    pub(in crate::controller::sql) config: Config,
    pub(in crate::controller::sql) base_schemas:
        HashMap<Relation, Vec<(usize, Vec<ColumnSpecification>)>>,
    /// The graph containing all of the MIR base tables, views and cached queries.
    /// Each of them are a subgraph in the MIR Graph.
    pub(in crate::controller::sql) mir_graph: MirGraph,
    /// A map to the nodes corresponding to either base table nodes or leaf nodes (in case
    /// of views or cached queries) in the MIR Supergraph.
    pub(in crate::controller::sql) relations: HashMap<Relation, NodeIndex>,

    /// Set of relations (tables or views) that exist in the upstream database, but are not being
    /// replicated (either due to lack of support, or because the user explicitly opted out from
    /// them being replicated)
    pub(in crate::controller::sql) non_replicated_relations: HashSet<NonReplicatedRelation>,

    /// SQL dialect for expression display
    pub(in crate::controller::sql) dialect: Dialect,
}

enum SubqueryContext {
    In,
    NotIn,
    Scalar,
}

impl SqlToMirConverter {
    /// Creates a new `SqlToMirConverter` with the specified dialect
    pub(crate) fn new(dialect: Dialect) -> Self {
        Self {
            config: Default::default(),
            base_schemas: Default::default(),
            mir_graph: Default::default(),
            relations: Default::default(),
            non_replicated_relations: Default::default(),
            dialect,
        }
    }

    pub(crate) fn config(&self) -> &Config {
        &self.config
    }

    /// Set the [`Config`]
    pub(crate) fn set_config(&mut self, config: Config) {
        self.config = config;
    }

    /// Returns the index of the node that represents the given relation.
    /// If the relation is a base table, then the base table node index is returned.
    /// If the relation is a query (cached query or view), then the leaf node index is returned.
    fn get_relation(&self, relation: &Relation) -> Option<NodeIndex> {
        self.relations.get(relation).copied()
    }

    /// Generates a label based on the number of nodes in the MIR graph.
    /// Useful to generate label for new nodes.
    ///
    /// WARNING: If no new node is added between two calls to this function (with the same prefix),
    /// then the two names will be identical.
    /// New nodes must be added between consecutive calls for the names to be unique.
    // TODO(fran): Remove this once we get rid of node names.
    pub(crate) fn generate_label(&self, label_prefix: &Relation) -> Relation {
        format!(
            "{}_n{}",
            label_prefix.display_unquoted(),
            self.mir_graph.node_count()
        )
        .into()
    }

    /// Return the correct error for a table not being found during a migration.
    ///
    /// This is either [`ReadySetError::TableNotReplicated`] if the table is known to exist in the
    /// upstream database but is not being replicated, or [`ReadySetError::TableNotFound`] if the
    /// table is completely unknown
    pub(super) fn table_not_found_err(&self, name: &Relation) -> ReadySetError {
        let relation = NonReplicatedRelation::new(name.clone());
        if self.non_replicated_relations.contains(&relation) || is_catalog_table(name) {
            ReadySetError::TableNotReplicated {
                name: (&name.name).into(),
                schema: name.schema.as_ref().map(Into::into),
            }
        } else {
            ReadySetError::TableNotFound {
                name: (&name.name).into(),
                schema: name.schema.as_ref().map(Into::into),
            }
        }
    }

    pub(super) fn compound_query_to_mir(
        &mut self,
        query_name: &Relation,
        subquery_leaves: Vec<NodeIndex>,
        op: CompoundSelectOperator,
        order: &Option<OrderClause>,
        limit_clause: &LimitClause,
        leaf_behavior: LeafBehavior,
    ) -> ReadySetResult<NodeIndex> {
        let has_limit = matches!(
            limit_clause,
            LimitClause::OffsetCommaLimit { .. } | LimitClause::LimitOffset { limit: Some(_), .. }
        );
        let name = if !leaf_behavior.should_register() && !has_limit {
            query_name.clone()
        } else {
            format!("{}_union", query_name.display_unquoted()).into()
        };
        let mut final_node = match op {
            CompoundSelectOperator::UnionAll => self.make_union_node(
                query_name,
                name,
                subquery_leaves.as_slice(),
                union::DuplicateMode::UnionAll,
            )?,
            op => unsupported!("Unsupported compound select operator {op}"),
        };

        if let Some((limit, offset)) = extract_limit_offset(limit_clause)? {
            let make_topk = offset.is_none();
            let paginate_name = if leaf_behavior.should_register() {
                if make_topk {
                    format!("{}_topk", query_name.display_unquoted())
                } else {
                    format!("{}_paginate", query_name.display_unquoted())
                }
                .into()
            } else {
                query_name.clone()
            };

            // Either a topk or paginate node
            let group_by = self.mir_graph.columns(final_node);
            let paginate_node = *self
                .make_paginate_node(
                    query_name,
                    paginate_name.display_unquoted().to_string().into(),
                    final_node,
                    group_by,
                    &order
                        .as_ref()
                        .map(|o| {
                            o.order_by
                                .iter()
                                .map(
                                    |OrderBy {
                                         field,
                                         order_type,
                                         null_order,
                                     }| {
                                        let order_type =
                                            order_type.unwrap_or(OrderType::OrderAscending);

                                        Ok((
                                            match field {
                                                FieldReference::Numeric(_) => internal!(
                                                 "Numeric field references should have been removed"
                                             ),
                                                FieldReference::Expr(e) => e.clone(),
                                            },
                                            order_type,
                                            *null_order,
                                        ))
                                    },
                                )
                                .collect::<ReadySetResult<_>>()
                        })
                        .transpose()?,
                    limit,
                    make_topk,
                    // TODO: we should have access to the output cols even if this is a compound query
                    None,
                )?
                .last()
                .unwrap();
            final_node = paginate_node;
        }

        let mut alias_table_node = MirNode::new(
            if leaf_behavior.should_register() {
                format!("{}_alias_table", query_name.display_unquoted()).into()
            } else {
                query_name.clone()
            },
            MirNodeInner::AliasTable {
                table: query_name.clone(),
            },
        );
        alias_table_node.add_owner(query_name.clone());
        let alias_table = self.mir_graph.add_node(alias_table_node);
        self.mir_graph.add_edge(final_node, alias_table, 0);

        // TODO: Initialize leaf with ordering?
        let leaf_node = if leaf_behavior.should_make_leaf() {
            self.add_query_node(
                query_name.clone(),
                MirNode::new(
                    query_name.clone(),
                    MirNodeInner::leaf(
                        vec![],
                        // TODO: is this right?
                        IndexType::HashMap,
                    ),
                ),
                &[alias_table],
            )
        } else {
            alias_table
        };
        if leaf_behavior.should_register() {
            self.relations.insert(query_name.clone(), leaf_node);
        }

        Ok(leaf_node)
    }

    pub(super) fn get_flow_node_address(&self, name: &Relation) -> Option<DfNodeIndex> {
        self.relations
            .get(name)
            .and_then(|node| self.mir_graph.resolve_dataflow_node(*node))
    }

    pub(super) fn named_base_to_mir(
        &mut self,
        name: Relation,
        body: &CreateTableBody,
    ) -> ReadySetResult<MirBase<'_>> {
        let n = self.make_base_node(&name, &body.fields, body.keys.as_ref())?;
        Ok(MirBase {
            name,
            mir_node: n,
            fields: body
                .fields
                .iter()
                .map(|cs| cs.column.name.clone())
                .collect(),
            graph: &mut self.mir_graph,
        })
    }

    /// Removes a cached query/view from MIR, along with all views/cached queries that depend on
    /// it.
    pub(super) fn remove_query(&mut self, name: &Relation) -> ReadySetResult<MirRemovalResult> {
        let leaf_mn =
            self.relations
                .remove(name)
                .ok_or_else(|| ReadySetError::RelationNotFound {
                    relation: name.display_unquoted().to_string(),
                })?;

        self.remove_dependent_nodes(leaf_mn)
    }

    /// Removes a base table, along with all the views/cached queries associated with it.
    pub(super) fn remove_base(&mut self, name: &Relation) -> ReadySetResult<MirRemovalResult> {
        debug!(name = %name.display_unquoted(), "Removing base node");
        let root = self
            .relations
            .remove(name)
            .ok_or_else(|| ReadySetError::RelationNotFound {
                relation: name.display_unquoted().to_string(),
            })?;

        let mut mir_removal_result = self.remove_dependent_nodes(root)?;
        // All dependent MIR nodes have been removed, except for the base table node.
        // Add the base table node's dataflow node to the list of nodes marked for removal.
        mir_removal_result.dataflow_nodes_to_remove.insert(
            self.mir_graph.resolve_dataflow_node(root).ok_or_else(|| {
                ReadySetError::MirNodeMustHaveDfNodeAssigned {
                    mir_node_index: root.index(),
                }
            })?,
        );
        mir_removal_result.relations_removed.insert(name.clone());
        // Finally, remove the MIR node for the base table.
        self.mir_graph
            .remove_node(root)
            .ok_or_else(|| internal_err!("Table node should exist in MIR!"))?;
        Ok(mir_removal_result)
    }

    /// Removes the views/cached queries associated with the given base table (without removing the
    /// base table itself).
    pub(super) fn remove_dependent_queries(
        &mut self,
        table_name: &Relation,
    ) -> ReadySetResult<MirRemovalResult> {
        debug!(table_name = %table_name.display_unquoted(), "Removing dependent queries");
        let root =
            self.relations
                .get(table_name)
                .ok_or_else(|| ReadySetError::RelationNotFound {
                    relation: table_name.display_unquoted().to_string(),
                })?;
        self.mir_graph
            .node_weight(*root)
            .ok_or_else(|| ReadySetError::MirNodeNotFound {
                index: root.index(),
            })
            .and_then(|node| {
                if !node.is_base() {
                    internal!("Node should be a base node!");
                }
                Ok(node)
            })?;
        self.remove_dependent_nodes(*root)
    }

    pub(super) fn make_mir_query(
        &mut self,
        query_name: Relation,
        mir_leaf: NodeIndex,
    ) -> MirQuery<'_> {
        MirQuery::new(query_name, mir_leaf, &mut self.mir_graph)
    }

    /// Computes the list of columns in the output of this node.
    pub(super) fn columns(&self, node: NodeIndex) -> Vec<Column> {
        self.mir_graph.columns(node)
    }

    pub(super) fn get_node(&self, node: NodeIndex) -> Option<&MirNode> {
        self.mir_graph.node_weight(node)
    }

    fn add_query_node(
        &mut self,
        query_name: Relation,
        mut node: MirNode,
        parents: &[NodeIndex],
    ) -> NodeIndex {
        node.add_owner(query_name);
        let node_idx = self.mir_graph.add_node(node);
        for (i, &parent) in parents.iter().enumerate() {
            self.mir_graph.add_edge(parent, node_idx, i);
        }
        node_idx
    }

    /// Removes all the nodes that depend on the one provided, and the provided node itself (except
    /// if it's a base table node).
    fn remove_dependent_nodes(&mut self, node: NodeIndex) -> ReadySetResult<MirRemovalResult> {
        let mut removed = Vec::new();

        // Track down the leaves that depend on the given node, along with their owners.
        let mut owners_to_remove = HashSet::new();
        let mut leaves = Vec::new();
        let mut bfs = petgraph::visit::Bfs::new(&*self.mir_graph, node);
        while let Some(n) = bfs.next(&*self.mir_graph) {
            owners_to_remove.extend(self.mir_graph[n].owners().iter().cloned());
            if self
                .mir_graph
                .neighbors_directed(n, Direction::Outgoing)
                .next()
                .is_none()
            {
                leaves.push(n);
            }
        }

        // Traverse the graph backwards (starting from the leaves) and gather all the nodes that
        // need to be removed, in order. We need to traverse this from leaves to roots
        // because attempting to remove only downstream nodes is incorrect. The best example for
        // this are JOINs, where removing downstream nodes would only prune one part of the query,
        // leaving MIR in an inconsistent state.
        for leaf in leaves {
            let mut bfs = petgraph::visit::Bfs::new(Reversed(&*self.mir_graph), leaf);
            while let Some(node_idx) = bfs.next(Reversed(&*self.mir_graph)) {
                self.mir_graph[node_idx].retain_owners(|n| !owners_to_remove.contains(n));
                if self.mir_graph[node_idx].owners().is_empty()
                    && !self.mir_graph[node_idx].is_base()
                {
                    removed.push(node_idx);
                }
            }
        }

        // Remove the MIR nodes and keep track of their Dataflow node counterparts.
        let mut df_to_remove = HashSet::new();
        for node_idx in removed {
            if let Some(mir_node) = self.mir_graph.remove_node(node_idx) {
                if let Some(df_node) = mir_node.df_node_index() {
                    df_to_remove.insert(df_node);
                }
            }
        }

        // Finally, remove the affected queries from the state.
        self.relations
            .retain(|name, _| !owners_to_remove.contains(name));
        Ok(MirRemovalResult {
            dataflow_nodes_to_remove: df_to_remove,
            relations_removed: owners_to_remove,
        })
    }

    fn make_base_node(
        &mut self,
        table_name: &Relation,
        cols: &[ColumnSpecification],
        keys: Option<&Vec<TableKey>>,
    ) -> ReadySetResult<NodeIndex> {
        if let Some(ni) = self.get_relation(table_name) {
            match &self.mir_graph[ni].inner {
                MirNodeInner::Base { column_specs, .. } => {
                    if column_specs.as_slice() == cols {
                        debug!(
                            table_name = %table_name.display_unquoted(),
                            "base table already exists with identical schema; reusing it.",
                        );
                        return Ok(ni);
                    } else {
                        // TODO(fran): When we remove the Recipe, this here will trigger the
                        // drop-and-recreate  logic, since we interpret a
                        // new CREATE TABLE statement for an existing table
                        //  to be a replace request.
                        //  Furthermore, we could check if the depending queries can still work
                        // under  the new table schema and avoid dropping
                        // queries.
                        invalid_query_err!("a base table already exists with a different schema");
                    }
                }
                _ => internal!(
                    "a MIR node already exists with the same name: {}",
                    table_name.display_unquoted()
                ),
            }
        }
        // all columns on a base must have the base as their table
        invariant!(cols.iter().all(|c| c
            .column
            .table
            .as_ref()
            .map(|t| t == table_name)
            .unwrap_or(false)));

        // primary keys can either be specified directly (at the end of CREATE TABLE), or inline
        // with the definition of a field (i.e., as a ColumnConstraint).
        // We assume here that an earlier rewrite pass has coalesced all primary key definitions in
        // the TableKey structure.

        // For our unique keys, we want the very first key to be the primary key if present, as it
        // will be used as the primary index.
        // TODO: failing that we want to index on a unique integer key, failing that on whatever.

        let (primary_key, unique_keys) = match keys {
            None => (None, vec![].into()),
            Some(keys) => {
                let primary_key = keys.iter().find_map(|k| match k {
                    TableKey::PrimaryKey { columns, .. } => {
                        Some(columns.iter().map(Column::from).collect::<Box<[Column]>>())
                    }
                    _ => None,
                });

                let unique_keys = keys.iter().filter_map(|k| match k {
                    TableKey::UniqueKey { columns, .. } => {
                        Some(columns.iter().map(Column::from).collect::<Box<[Column]>>())
                    }
                    _ => None,
                });

                (primary_key, unique_keys.collect::<Box<[_]>>())
            }
        };

        // remember the schema for this version
        let node = MirNode::new(
            table_name.clone(),
            MirNodeInner::Base {
                column_specs: cols.to_vec(),
                primary_key,
                unique_keys,
            },
        );
        let ni = self.mir_graph.add_node(node);
        self.relations.insert(table_name.clone(), ni);

        Ok(ni)
    }

    fn make_union_node(
        &mut self,
        query_name: &Relation,
        name: Relation,
        ancestors: &[NodeIndex],
        duplicate_mode: union::DuplicateMode,
    ) -> ReadySetResult<NodeIndex> {
        let mut emit: Vec<Vec<Column>> = Vec::new();
        invariant!(ancestors.len() > 1, "union must have more than 1 ancestors");

        #[allow(clippy::unwrap_used)] // checked above
        let ucols: Vec<Column> = self.mir_graph.columns(*ancestors.first().unwrap()).to_vec();
        let num_ucols = ucols.len();

        // Find columns present in all ancestors
        // XXX(malte): this currently matches columns by **name** rather than by table and name,
        // which can go wrong if there are multiple columns of the same name in the inputs to the
        // union. Unfortunately, we have to do it by name here because the nested queries in
        // compound SELECT rewrite the table name on their output columns.
        let mut selected_cols = HashSet::new();
        for c in ucols {
            if ancestors
                .iter()
                .all(|&a| self.mir_graph.columns(a).iter().any(|ac| c.name == ac.name))
            {
                selected_cols.insert(c.name.clone());
            } else {
                internal!(
                    "column with name '{}' not found all union ancestors: all ancestors' \
                     output columns must have the same names",
                    c.name
                );
            }
        }
        invariant_eq!(
            num_ucols,
            selected_cols.len(),
            "union drops ancestor columns"
        );

        for ancestor in ancestors.iter() {
            let mut acols: Vec<Column> = Vec::new();
            for ac in self.mir_graph.columns(*ancestor) {
                if selected_cols.contains(&ac.name) && !acols.iter().any(|c| ac.name == c.name) {
                    acols.push(Column::named(ac.name));
                }
            }
            emit.push(acols);
        }

        invariant!(
            emit.iter().all(|e| e.len() == selected_cols.len()),
            "all ancestors columns must have the same size, but got emit: {:?}, selected: {:?}",
            emit,
            selected_cols
        );

        invariant!(!emit.is_empty());

        Ok(self.add_query_node(
            query_name.clone(),
            MirNode::new(
                name,
                MirNodeInner::Union {
                    emit,
                    duplicate_mode,
                },
            ),
            ancestors,
        ))
    }

    fn make_union_from_same_base(
        &mut self,
        query_name: &Relation,
        name: Relation,
        ancestors: Vec<NodeIndex>,
        columns: Vec<Column>,
        duplicate_mode: union::DuplicateMode,
    ) -> ReadySetResult<NodeIndex> {
        invariant!(ancestors.len() > 1, "union must have more than 1 ancestors");
        trace!(name = %name.display_unquoted(), ?columns, "Added union node");
        let emit = ancestors.iter().map(|_| columns.clone()).collect();
        Ok(self.add_query_node(
            query_name.clone(),
            MirNode::new(
                name,
                MirNodeInner::Union {
                    emit,
                    duplicate_mode,
                },
            ),
            ancestors.as_slice(),
        ))
    }

    fn make_filter_node(
        &mut self,
        query_name: &Relation,
        name: Relation,
        parent: NodeIndex,
        conditions: Expr,
    ) -> NodeIndex {
        trace!(
            name = %name.display_unquoted(),
            conditions = %conditions.display(self.dialect.into()),
            "Added filter node"
        );
        self.add_query_node(
            query_name.clone(),
            MirNode::new(name, MirNodeInner::Filter { conditions }),
            &[parent],
        )
    }

    fn make_aggregate_node(
        &mut self,
        query_name: &Relation,
        name: Relation,
        func_col: Column,
        function: FunctionExpr,
        group_cols: Vec<Column>,
        parent: NodeIndex,
        projected_exprs: &HashMap<Expr, SqlIdentifier>,
    ) -> ReadySetResult<Vec<NodeIndex>> {
        use dataflow::ops::grouped::extremum::Extremum;
        use readyset_sql::ast::FunctionExpr::*;

        macro_rules! mk_error {
            ($expression:expr) => {
                internal_err!(
                    "projected_exprs does not contain {:?}",
                    Sensitive($expression)
                )
            };
        }

        // COUNT(*) is special (see comments below), so we handle it specially before all other
        // aggregates
        if function == CountStar {
            // 1. Pick a column to aggregate over
            let parent_cols = self.mir_graph.columns(parent);
            let over_col = parent_cols
                .first()
                .ok_or_else(|| internal_err!("MIR node has no columns"))?;

            // 2. Aggregation::Count discards all null values in its input, but in SQL `COUNT(*)` is
            //    always expected to count all rows, regardless of if any of the columns (eg the
            //    column we picked above, for example) is null. So before the aggregate node we
            //    project out a `coalesce(col, 0)` expr to make sure that value is never `NULL`
            let coalesce_alias = SqlIdentifier::from(format!("{}_over", name.display_unquoted()));
            let project_coalesce = self.make_project_node(
                query_name,
                format!("{}_coalesce_over_col", name.display_unquoted()).into(),
                parent,
                vec![ProjectExpr::Expr {
                    expr: Expr::Call(Call {
                        name: "coalesce".into(),
                        arguments: Some(vec![
                            Expr::Column(ast::Column {
                                table: over_col.table.clone(),
                                name: over_col.name.clone(),
                            }),
                            Expr::Literal(0.into()),
                        ]),
                    }),
                    alias: coalesce_alias.clone(),
                }],
            );

            // 3. Actually add the aggregate node
            let grouped_node = self.make_grouped_node(
                query_name,
                name,
                func_col,
                (project_coalesce, Column::named(coalesce_alias)),
                group_cols,
                GroupedNodeType::Aggregation(Aggregation::Count),
            );

            return Ok(vec![project_coalesce, grouped_node]);
        }

        let mut out_nodes = Vec::new();

        let mknode = |over: Column, t: GroupedNodeType, distinct: bool| {
            if distinct {
                let new_name = format!("{}_d{}", name.display_unquoted(), out_nodes.len()).into();
                let mut dist_col = vec![over.clone()];
                dist_col.extend(group_cols.clone());
                let node = self.make_distinct_node(query_name, new_name, parent, dist_col.clone());
                out_nodes.push(node);
                out_nodes.push(self.make_grouped_node(
                    query_name,
                    name,
                    func_col,
                    (node, over),
                    group_cols,
                    t,
                ));
            } else {
                out_nodes.push(self.make_grouped_node(
                    query_name,
                    name,
                    func_col,
                    (parent, over),
                    group_cols,
                    t,
                ));
            }
            out_nodes
        };

        fn is_column(expr: &Expr) -> bool {
            matches!(expr, Expr::Column(_))
        }

        fn get_column(expr: &Expr) -> &ast::Column {
            match expr {
                Expr::Column(ref col) => col,
                _ => unreachable!(),
            }
        }

        Ok(match function {
            Sum { ref expr, distinct } if is_column(expr) => mknode(
                Column::from(get_column(expr)),
                GroupedNodeType::Aggregation(Aggregation::Sum),
                distinct,
            ),
            Sum { expr, distinct } => mknode(
                Column::named(
                    projected_exprs
                        .get(&expr)
                        .cloned()
                        .ok_or_else(|| mk_error!(&*expr))?,
                ),
                GroupedNodeType::Aggregation(Aggregation::Sum),
                distinct,
            ),
            CountStar => internal!("Handled earlier"),
            Count { ref expr, distinct } if is_column(expr) => mknode(
                Column::from(get_column(expr)),
                GroupedNodeType::Aggregation(Aggregation::Count),
                distinct,
            ),
            Count { ref expr, distinct } => mknode(
                Column::named(
                    projected_exprs
                        .get(expr)
                        .cloned()
                        .ok_or_else(|| mk_error!(expr))?,
                ),
                GroupedNodeType::Aggregation(Aggregation::Count),
                distinct,
            ),
            Avg { ref expr, distinct } if is_column(expr) => mknode(
                Column::from(get_column(expr)),
                GroupedNodeType::Aggregation(Aggregation::Avg),
                distinct,
            ),
            Avg { ref expr, distinct } => mknode(
                Column::named(
                    projected_exprs
                        .get(expr)
                        .cloned()
                        .ok_or_else(|| mk_error!(expr))?,
                ),
                GroupedNodeType::Aggregation(Aggregation::Avg),
                distinct,
            ),
            Max(ref expr) if is_column(expr) => mknode(
                Column::from(get_column(expr)),
                GroupedNodeType::Extremum(Extremum::Max),
                false,
            ),
            Max(ref expr) => mknode(
                Column::named(
                    projected_exprs
                        .get(expr)
                        .cloned()
                        .ok_or_else(|| mk_error!(expr))?,
                ),
                GroupedNodeType::Extremum(Extremum::Max),
                false,
            ),
            Min(ref expr) if is_column(expr) => mknode(
                Column::from(get_column(expr)),
                GroupedNodeType::Extremum(Extremum::Min),
                false,
            ),
            Min(ref expr) => mknode(
                Column::named(
                    projected_exprs
                        .get(expr)
                        .cloned()
                        .ok_or_else(|| mk_error!(expr))?,
                ),
                GroupedNodeType::Extremum(Extremum::Min),
                false,
            ),
            GroupConcat {
                ref expr,
                separator,
            } if is_column(expr) => mknode(
                Column::from(get_column(expr)),
                GroupedNodeType::Aggregation(Aggregation::GroupConcat {
                    separator: separator.unwrap_or_else(|| ",".to_owned()),
                }),
                false,
            ),
            JsonObjectAgg {
                allow_duplicate_keys,
                ..
            } => mknode(
                Column::named("__json_objects__"),
                GroupedNodeType::Aggregation(Aggregation::JsonObjectAgg {
                    allow_duplicate_keys,
                }),
                false,
            ),
            _ => {
                internal!("not an aggregate: {:?}", Sensitive(&function));
            }
        })
    }

    fn make_grouped_node(
        &mut self,
        query_name: &Relation,
        name: Relation,
        output_column: Column,
        (parent_node, on): (NodeIndex, Column),
        group_by: Vec<Column>,
        node_type: GroupedNodeType,
    ) -> NodeIndex {
        self.add_query_node(
            query_name.clone(),
            match node_type {
                GroupedNodeType::Aggregation(kind) => MirNode::new(
                    name,
                    MirNodeInner::Aggregation {
                        on,
                        group_by,
                        output_column,
                        kind,
                    },
                ),
                GroupedNodeType::Extremum(kind) => MirNode::new(
                    name,
                    MirNodeInner::Extremum {
                        on,
                        group_by,
                        output_column,
                        kind,
                    },
                ),
            },
            &[parent_node],
        )
    }

    fn make_join_node(
        &mut self,
        query_name: &Relation,
        name: Relation,
        join_predicates: &[JoinPredicate],
        left_node: NodeIndex,
        right_node: NodeIndex,
        kind: JoinKind,
    ) -> ReadySetResult<NodeIndex> {
        // TODO(malte): this is where we overproject join columns in order to increase reuse
        // opportunities. Technically, we need to only project those columns here that the query
        // actually needs; at a minimum, we could start with just the join columns, relying on the
        // automatic column pull-down to retrieve the remaining columns required.
        let projected_cols_left = self.mir_graph.columns(left_node);
        let projected_cols_right = self.mir_graph.columns(right_node);
        let mut project = projected_cols_left
            .into_iter()
            .chain(projected_cols_right)
            .collect::<Vec<Column>>();

        // join columns need us to generate join group configs for the operator
        let mut on = Vec::new();

        for jp in join_predicates {
            let mut l_col = Column::from(jp.left.clone());
            let r_col = Column::from(jp.right.clone());

            if kind == JoinKind::Inner {
                // for inner joins, don't duplicate the join column in the output, but instead add
                // aliases to the columns that represent it going forward (viz., the left-side join
                // column)
                l_col.add_alias(&r_col);
                // add the alias to all instances of `l_col` in `fields` (there might be more than
                // one if `l_col` is explicitly projected multiple times)
                project = project
                    .into_iter()
                    .filter_map(|mut f| {
                        if f == r_col {
                            // drop instances of right-side column
                            None
                        } else if f == l_col {
                            // add alias for right-side column to any left-side column
                            // N.B.: since `l_col` is already aliased, need to check this *after*
                            // checking for equivalence with `r_col` (by now, `l_col` == `r_col` via
                            // alias), so `f == l_col` also triggers if `f` is in `l_col.aliases`.
                            f.add_alias(&r_col);
                            Some(f)
                        } else {
                            // keep unaffected columns
                            Some(f)
                        }
                    })
                    .collect();
            }

            on.push((l_col, r_col));
        }

        let inner = match kind {
            JoinKind::Inner => MirNodeInner::Join { on, project },
            JoinKind::Left => MirNodeInner::LeftJoin { on, project },
            JoinKind::DependentInner => MirNodeInner::DependentJoin { on, project },
            JoinKind::DependentLeft => MirNodeInner::DependentLeftJoin { on, project },
        };
        trace!(?inner, "Added join node");
        Ok(self.add_query_node(
            query_name.clone(),
            MirNode::new(name, inner),
            &[left_node, right_node],
        ))
    }

    /// Make a series of nodes coresponding to an "antijoin" - emitting all rows in the left node
    /// that do *not* join to any of the rows in the right based on the given join predicate
    fn make_antijoin(
        &mut self,
        query_name: &Relation,
        name: Relation,
        join_predicates: &[JoinPredicate],
        left_node: NodeIndex,
        right_node: NodeIndex,
        dependent: bool,
    ) -> ReadySetResult<NodeIndex> {
        let mark_col = SqlIdentifier::from("__mark");
        let right_mark = self.make_project_node(
            query_name,
            self.generate_label(&format!("{}_antijoin_mark", name.display_unquoted()).into()),
            right_node,
            self.columns(right_node)
                .into_iter()
                .map(ProjectExpr::Column)
                .chain(iter::once(ProjectExpr::Expr {
                    expr: Expr::Literal(Literal::Integer(0)),
                    alias: mark_col.clone(),
                }))
                .collect(),
        );

        let join = self.make_join_node(
            query_name,
            name.clone(),
            join_predicates,
            left_node,
            right_mark,
            if dependent {
                JoinKind::DependentLeft
            } else {
                JoinKind::Left
            },
        )?;

        Ok(self.make_filter_node(
            query_name,
            self.generate_label(&format!("{}_antijoin_filter", name.display_unquoted()).into()),
            join,
            Expr::BinaryOp {
                lhs: Box::new(Expr::Column(mark_col.into())),
                op: BinaryOperator::Is,
                rhs: Box::new(Expr::Literal(Literal::Null)),
            },
        ))
    }

    fn make_join_aggregates_node(
        &mut self,
        query_name: &Relation,
        name: Relation,
        left_parent: NodeIndex,
        right_parent: NodeIndex,
    ) -> ReadySetResult<NodeIndex> {
        trace!("Added join aggregates node");
        Ok(self.add_query_node(
            query_name.clone(),
            MirNode::new(name, MirNodeInner::JoinAggregates),
            &[left_parent, right_parent],
        ))
    }

    /// Project out a column representing an `IN (subquery)` expr.
    ///
    /// Internally, these are compiled via a LEFT JOIN with a DISTINCT and a marker column on the
    /// right-hand side, followed by an `IS NULL` expr projected out for the result
    fn project_in_subquery(
        &mut self,
        query_name: &Relation,
        parent: NodeIndex,
        name: &str,
        lhs: &Expr,
        subquery: SelectStatement,
        negated: bool,
    ) -> ReadySetResult<NodeIndex> {
        let (lhs, parent) = match lhs {
            Expr::Column(col) => (col.clone(), parent),
            expr => {
                // The lhs is a non-column expr, so we need to project it first
                let label = lhs.display(self.dialect.into()).to_string();
                let prj = self.make_project_node(
                    query_name,
                    self.generate_label(&"in_lhs_project".into()),
                    parent,
                    vec![ProjectExpr::Expr {
                        alias: label.clone().into(),
                        expr: expr.clone(),
                    }],
                );
                (
                    ast::Column {
                        name: label.into(),
                        table: None,
                    },
                    prj,
                )
            }
        };

        let is_correlated = is_correlated(&subquery);
        let query_graph = to_query_graph(subquery, self.dialect)?;
        let subquery_leaf = self.named_query_to_mir(
            query_name,
            &query_graph,
            &HashMap::new(),
            LeafBehavior::Anonymous,
        )?;

        let cols = self.columns(subquery_leaf);
        if cols.len() != 1 {
            invalid_query!("Subquery on right-hand side of IN must have exactly one column");
        }
        let col = cols.into_iter().next().expect("Just checked");
        let distinct = self.make_distinct_node(
            query_name,
            self.generate_label(&"in_subquery_distinct".into()),
            subquery_leaf,
            vec![col.clone()],
        );

        let mark_col = SqlIdentifier::from("__mark");
        let right_mark = self.make_project_node(
            query_name,
            self.generate_label(&format!("{name}_in_mark").into()),
            distinct,
            self.columns(distinct)
                .into_iter()
                .map(ProjectExpr::Column)
                .chain(iter::once(ProjectExpr::Expr {
                    expr: Expr::Literal(Literal::Integer(0)),
                    alias: mark_col.clone(),
                }))
                .collect(),
        );

        let join = self.make_join_node(
            query_name,
            self.generate_label(&format!("{name}_join").into()),
            &[JoinPredicate {
                left: lhs.clone(),
                right: ast::Column {
                    name: col.name,
                    table: col.table,
                },
            }],
            parent,
            right_mark,
            if is_correlated {
                JoinKind::DependentLeft
            } else {
                JoinKind::Left
            },
        )?;

        Ok(self.make_project_node(
            query_name,
            self.generate_label(&format!("project_{name}").into()),
            join,
            self.columns(join)
                .into_iter()
                .map(ProjectExpr::Column)
                .chain(iter::once(ProjectExpr::Expr {
                    // CASE WHEN lhs IS NULL THEN NULL ELSE mark_col <IS|IS NOT> NULL END
                    expr: Expr::CaseWhen {
                        branches: vec![CaseWhenBranch {
                            condition: Expr::BinaryOp {
                                lhs: Box::new(Expr::Column(lhs)),
                                op: BinaryOperator::Is,
                                rhs: Box::new(Expr::Literal(Literal::Null)),
                            },
                            body: Expr::Literal(Literal::Null),
                        }],
                        else_expr: Some(Box::new(Expr::BinaryOp {
                            lhs: Box::new(Expr::Column(mark_col.into())),
                            op: if negated {
                                BinaryOperator::Is
                            } else {
                                BinaryOperator::IsNot
                            },
                            rhs: Box::new(Expr::Literal(Literal::Null)),
                        })),
                    },
                    alias: name.into(),
                }))
                .collect(),
        ))
    }

    fn make_project_node(
        &mut self,
        query_name: &Relation,
        name: Relation,
        parent_node: NodeIndex,
        emit: Vec<ProjectExpr>,
    ) -> NodeIndex {
        self.add_query_node(
            query_name.clone(),
            MirNode::new(name, MirNodeInner::Project { emit }),
            &[parent_node],
        )
    }

    fn make_distinct_node(
        &mut self,
        query_name: &Relation,
        name: Relation,
        parent: NodeIndex,
        group_by: Vec<Column>,
    ) -> NodeIndex {
        self.add_query_node(
            query_name.clone(),
            MirNode::new(name, MirNodeInner::Distinct { group_by }),
            &[parent],
        )
    }

    fn make_paginate_node(
        &mut self,
        query_name: &Relation,
        name: SqlIdentifier,
        mut parent: NodeIndex,
        group_by: Vec<Column>,
        order: &Option<Vec<(Expr, OrderType, NullOrder)>>,
        limit: usize,
        is_topk: bool,
        outputs: Option<&Vec<OutputColumn>>,
    ) -> ReadySetResult<Vec<NodeIndex>> {
        if !self.config.allow_topk && is_topk {
            unsupported!("TopK is not supported");
        } else if !self.config.allow_paginate && !is_topk {
            unsupported!("Paginate is not supported");
        }

        // Gather a list of expressions we need to evaluate before the paginate node
        let mut exprs_to_project = vec![];

        let order = order
            .clone()
            .unwrap_or_default()
            .iter()
            .map(|(expr, ot, no)| {
                let col = match expr {
                    Expr::Column(col) => {
                        let col = Column::from(col);
                        match self.mir_graph.column_id_for_column(parent, &col) {
                            Ok(_) => Ok(col),
                            Err(ReadySetError::NonExistentColumn { .. }) => {
                                // we couldn't find this column in the parent, this means
                                // that either:
                                // 1. the col is referencing an alias
                                // 2. the col is referencing a col that doesn't even exist
                                // We pass the list of output cols to check their aliases,
                                // if we couldn't find the outputs then mark the column
                                // as non-existent
                                let outputs =
                                    outputs.ok_or_else(|| ReadySetError::NonExistentColumn {
                                        column: col.name.to_string(),
                                        node: self.mir_graph[parent]
                                            .name()
                                            .display_unquoted()
                                            .to_string(),
                                    })?;
                                let resolved_expr = outputs
                                    .iter()
                                    .find_map(|o| {
                                        if *o.name() == *col.name {
                                            Some(o.clone().into_expr())
                                        } else {
                                            None
                                        }
                                    })
                                    .ok_or_else(|| ReadySetError::NonExistentColumn {
                                        column: col.name.to_string(),
                                        node: self.mir_graph[parent]
                                            .name()
                                            .display_unquoted()
                                            .to_string(),
                                    })?;

                                exprs_to_project.push((resolved_expr, Some(col.name.clone())));
                                Ok(col)
                            }
                            Err(e) => Err(e),
                        }
                    }
                    expr => {
                        let col = Column::named(expr.display(self.dialect.into()).to_string());
                        if self
                            .mir_graph
                            .column_id_for_column(parent, &col)
                            .err()
                            .iter()
                            .any(|err| matches!(err, ReadySetError::NonExistentColumn { .. }))
                        {
                            // Only project the expression if we haven't already
                            exprs_to_project.push((expr.clone(), None));
                        }
                        Ok(col)
                    }
                }?;

                Ok((col, *ot, *no))
            })
            .collect::<ReadySetResult<Vec<_>>>()?;

        let mut nodes = vec![];

        // If we're ordering on non-column expressions, add an extra node to project those first
        if !exprs_to_project.is_empty() {
            let parent_columns = self.mir_graph.columns(parent);
            let project_node = self.make_project_node(
                query_name,
                format!("{name}_proj").into(),
                parent,
                parent_columns
                    .into_iter()
                    .map(ProjectExpr::Column)
                    .chain(exprs_to_project.into_iter().map(|(expr, name)| {
                        let alias = name.unwrap_or_else(|| {
                            expr.display(self.dialect.into()).to_string().into()
                        });
                        ProjectExpr::Expr { alias, expr }
                    }))
                    .collect(),
            );
            nodes.push(project_node);
            parent = project_node;
        }

        // make the new operator and record its metadata
        let paginate_node = self.add_query_node(
            query_name.clone(),
            if is_topk {
                MirNode::new(
                    name.into(),
                    MirNodeInner::TopK {
                        order,
                        group_by,
                        limit,
                    },
                )
            } else {
                MirNode::new(
                    name.into(),
                    MirNodeInner::Paginate {
                        order,
                        group_by,
                        limit,
                    },
                )
            },
            &[parent],
        );
        nodes.push(paginate_node);

        Ok(nodes)
    }

    fn handle_exists(
        &mut self,
        ce: &Expr,
        query_name: &Relation,
        name: &Relation,
        parent: NodeIndex,
        subquery: &SelectStatement,
    ) -> ReadySetResult<NodeIndex> {
        let negated = matches!(
            ce,
            Expr::UnaryOp {
                op: UnaryOperator::Not,
                ..
            }
        );

        let query_graph = to_query_graph((*subquery).clone(), self.dialect)?;
        let subquery_leaf = self.named_query_to_mir(
            query_name,
            &query_graph,
            &HashMap::new(),
            LeafBehavior::Anonymous,
        )?;

        // -> π[lit: 0, lit: 0]
        let group_proj = self.make_project_node(
            query_name,
            format!("{}_prj_hlpr", name.display_unquoted()).into(),
            subquery_leaf,
            vec![
                ProjectExpr::Expr {
                    alias: "__count_val".into(),
                    expr: Expr::Literal(0u32.into()),
                },
                ProjectExpr::Expr {
                    alias: "__count_grp".into(),
                    expr: Expr::Literal(0u32.into()),
                },
            ],
        );
        // -> [0, 0] for each row

        // -> |0| γ[1]
        let exists_count_col = Column::named("__exists_count");
        let exists_count_node = self.make_grouped_node(
            query_name,
            format!("{}_count", name.display_unquoted()).into(),
            exists_count_col,
            (group_proj, Column::named("__count_val")),
            vec![Column::named("__count_grp")],
            GroupedNodeType::Aggregation(Aggregation::Count),
        );
        // -> [0, <count>] for each row

        // -> σ[c1 > 0]
        let gt_0_filter = self.make_filter_node(
            query_name,
            format!("{}_count_gt_0", name.display_unquoted()).into(),
            exists_count_node,
            Expr::BinaryOp {
                lhs: Box::new(Expr::Column("__exists_count".into())),
                op: BinaryOperator::Greater,
                rhs: Box::new(Expr::Literal(Literal::Integer(0))),
            },
        );

        // left -> π[...left, lit: 0]
        let parent_columns = self.mir_graph.columns(parent);
        let left_literal_join_key_proj = self.make_project_node(
            query_name,
            format!("{}_join_key", name.display_unquoted()).into(),
            parent,
            parent_columns
                .into_iter()
                .map(ProjectExpr::Column)
                .chain(iter::once(ProjectExpr::Expr {
                    alias: "__exists_join_key".into(),
                    expr: Expr::Literal(0u32.into()),
                }))
                .collect(),
        );

        let join_preds = [JoinPredicate {
            left: "__exists_join_key".into(),
            right: "__count_grp".into(),
        }];

        if negated {
            self.make_antijoin(
                query_name,
                format!("{}_antijoin", name.display_unquoted()).into(),
                &join_preds,
                left_literal_join_key_proj,
                gt_0_filter,
                /* dependent = */ is_correlated(subquery),
            )
        } else {
            // -> ⋈ on: l.__exists_join_key ≡ r.__count_grp
            self.make_join_node(
                query_name,
                format!("{}_join", name.display_unquoted()).into(),
                &join_preds,
                left_literal_join_key_proj,
                gt_0_filter,
                if is_correlated(subquery) {
                    JoinKind::DependentInner
                } else {
                    JoinKind::Inner
                },
            )
        }
    }

    fn extract_from_lhs(
        &mut self,
        query_name: &Relation,
        lhs: &Expr,
        parent: NodeIndex,
        text_context: &str,
    ) -> (ast::Column, NodeIndex) {
        match lhs {
            Expr::Column(col) => (col.clone(), parent),
            expr => {
                // The lhs is a non-column expr, so we need to project it first
                let label = lhs.display(self.dialect.into()).to_string();
                let prj = self.make_project_node(
                    query_name,
                    self.generate_label(&format!("{text_context}_lhs_project").into()),
                    parent,
                    self.mir_graph
                        .columns(parent)
                        .into_iter()
                        .map(ProjectExpr::Column)
                        .chain(iter::once(ProjectExpr::Expr {
                            alias: label.clone().into(),
                            expr: expr.clone(),
                        }))
                        .collect(),
                );
                (
                    ast::Column {
                        name: label.into(),
                        table: None,
                    },
                    prj,
                )
            }
        }
    }

    fn get_single_aggregated_select_item(subquery: &SelectStatement) -> Option<&Expr> {
        if subquery.fields.len() == 1 {
            let field = subquery.fields.first().unwrap();
            if let FieldDefinitionExpr::Expr { expr, .. } = field {
                if analysis::contains_aggregate(expr) {
                    return Some(expr);
                }
            }
        }
        None
    }

    /// Visits the comparison operators, between the AND nodes only.
    /// `local` columns - from tables referenced at in the subquery's FROM;
    /// `outer` - from tables referenced at in the outer subqueries FROM;
    /// Collects local columns referenced in EQ operators against
    /// either outer columns or a constant.
    /// Returns Ok if:
    ///    - there are no OR nodes;
    ///    - all comparisons are:
    ///      - local columns = outer column;
    ///      - local column = constant;
    ///    - if there is at least one comparison of the above format, then
    ///      the rest of the WHERE expression can be anything.
    ///
    fn collect_local_columns(subquery: &SelectStatement) -> ReadySetResult<HashSet<ast::Column>> {
        struct TheVisitor {
            local_tables: HashSet<Relation>,
            local_columns: HashSet<ast::Column>,
            unsupported_operations_depth: i32,
            result: ReadySetResult<()>,
        }

        impl<'ast> Visitor<'ast> for TheVisitor {
            type Error = std::convert::Infallible;

            fn visit_expr(&mut self, expr: &'ast Expr) -> Result<(), Self::Error> {
                macro_rules! as_column {
                    ($exp:expr) => {
                        match $exp {
                            Expr::Column(col) => col,
                            _ => unreachable!(),
                        }
                    };
                }

                macro_rules! is_local_column {
                    ($exp:expr) => {
                        matches!($exp, Expr::Column(ast::Column { name: _,table: Some(tab) }) if self.local_tables.contains(tab))
                    };
                }

                macro_rules! error_if_inside_unsupported_construct {
                    () => {
                        if self.unsupported_operations_depth != 0 {
                            self.result = Err(unsupported_err!(
                                "Correlated constraint inside unsupported construct"
                            ));
                        }
                    };
                }

                fn insert_into(set: &mut HashSet<ast::Column>, col: &ast::Column) {
                    if !set.contains(col) {
                        set.insert(col.clone());
                    }
                }

                if self.result.is_ok() {
                    match expr {
                        Expr::BinaryOp {
                            lhs: _,
                            op: BinaryOperator::And,
                            rhs: _,
                        } => {
                            walk_expr(self, expr)?;
                        }
                        Expr::BinaryOp {
                            lhs,
                            op: BinaryOperator::Equal,
                            rhs,
                        } if matches!(&**lhs, Expr::Column(_) | Expr::Literal(_))
                            && matches!(&**rhs, Expr::Column(_) | Expr::Literal(_)) =>
                        {
                            match (is_local_column!(&**lhs), is_local_column!(&**rhs)) {
                                (true, false) => {
                                    if matches!(&**rhs, Expr::Column(_)) {
                                        error_if_inside_unsupported_construct!();
                                    }
                                    insert_into(&mut self.local_columns, as_column!(lhs.as_ref()));
                                }
                                (false, true) => {
                                    if matches!(&**lhs, Expr::Column(_)) {
                                        error_if_inside_unsupported_construct!();
                                    }
                                    insert_into(&mut self.local_columns, as_column!(rhs.as_ref()));
                                }
                                (false, false) => {
                                    if matches!(&**lhs, Expr::Column(_))
                                        || matches!(&**rhs, Expr::Column(_))
                                    {
                                        error_if_inside_unsupported_construct!();
                                    }
                                    self.result = Err(unsupported_err!(
                                        "Both sides of comparison are from the outer scope"
                                    ));
                                }
                                (true, true) => {}
                            }
                        }
                        Expr::Column(_) if !is_local_column!(expr) => {
                            error_if_inside_unsupported_construct!();
                        }
                        _ => {
                            self.unsupported_operations_depth += 1;
                            walk_expr(self, expr)?;
                            self.unsupported_operations_depth -= 1;
                        }
                    }
                }
                Ok(())
            }
        }

        let mut visitor = TheVisitor {
            local_tables: outermost_table_exprs(subquery)
                .filter_map(|tab_expr| match tab_expr.inner {
                    TableExprInner::Table(ref relation) => Some(relation.clone()),
                    _ => None,
                })
                .collect(),
            local_columns: HashSet::new(),
            unsupported_operations_depth: 0,
            result: Ok(()),
        };

        if let Some(ref where_clause) = subquery.where_clause {
            visitor.visit_expr(where_clause).expect("Just checked");
        }

        if visitor.result.is_ok() {
            Ok(visitor.local_columns)
        } else {
            Err(visitor.result.unwrap_err())
        }
    }

    fn get_groupby_columns(group_by: &GroupByClause) -> ReadySetResult<HashSet<ast::Column>> {
        let mut columns = HashSet::with_capacity(group_by.fields.len());
        if group_by.fields.iter().all(|f| match f {
            FieldReference::Expr(Expr::Column(col)) => {
                columns.insert(col.clone());
                true
            }
            _ => false,
        }) {
            Ok(columns)
        } else {
            invalid_query!("Unsupported GROUP BY item")
        }
    }

    /// Scalar subquery should return a single value, in order to be a valid construct.
    /// In case of a scalar subquery, it should return a single value for each
    /// invocation. Upstream DB(s), MySQL/PostgreSQL implement run-time verification and
    /// error, in case the subquery return more than 1 row for each invocation (practically,
    /// it verifies the replacement join has only single match for each left side table).
    ///
    /// Since ReadySet does not have such verification during run-time, currently we
    /// only support EQ correlated constraints in the WHERE clause.
    ///
    /// Also, currently we only allow single use case for scalar subqueries, which
    /// is EQ comparison in the WHERE clause. The EQ comparison requirement is coming
    /// from the current subquery rewrite design (we might need to change in the future).
    /// We need to investigate if subqueries can be used in the select list.
    ///
    /// So that, as the 1st phase, the verification passes if the subquery:
    /// - has a single aggregated select item;
    /// - either does not have GROUP BY clause, or the GROUP BY keys are all
    ///   those local fields which are on either side of the correlated EQ constraints.
    ///
    /// For ex., these are valid use cases of correlated scalar subqueries:
    /// select name, val from foo where val = (select sum(b) from foo1 where a = foo.id group by a);
    /// select name, val from foo where val = (select sum(b) from foo1 where a = foo.id);
    ///
    /// These are NOT valid use cases of correlated scalar subqueries:
    /// select name, val from foo where val = (select sum(b) from foo1 where a = foo.id group by b);
    /// select name, val from foo where val = (select sum(b) from foo1 where a > foo.id group by a);
    fn is_scalar_subquery_qualified(subquery: &SelectStatement) -> ReadySetResult<()> {
        if Self::get_single_aggregated_select_item(subquery).is_some() {
            let local_columns = Self::collect_local_columns(subquery)?;
            if local_columns.is_empty()
                || matches!(&subquery.group_by,
                        Some(group_by) if !Self::get_groupby_columns(group_by)?.eq(&local_columns))
            {
                invalid_query!("Subquery returns more than 1 row")
            }
        } else {
            invalid_query!("Scalar subquery should be aggregated")
        }
        Ok(())
    }

    fn handle_in_and_scalar(
        &mut self,
        query_name: &Relation,
        parent: NodeIndex,
        expr: &Expr,
        subquery: &SelectStatement,
        ctx: SubqueryContext,
    ) -> ReadySetResult<NodeIndex> {
        if let SubqueryContext::Scalar = ctx {
            Self::is_scalar_subquery_qualified(subquery)?
        }

        let negated = matches!(ctx, SubqueryContext::NotIn);

        let text_context = if matches!(ctx, SubqueryContext::In | SubqueryContext::NotIn) {
            "in"
        } else {
            "scalar"
        };

        let (lhs, mut parent) = self.extract_from_lhs(query_name, expr, parent, text_context);

        // Remove rows where the lhs expr is NULL, since those would make the overall IN
        // expr NULL in regular SQL.
        //
        // Note that we only need to do this for `NOT IN` since NULLs would never match in
        // the rhs anyway
        if negated {
            parent = self.make_filter_node(
                query_name,
                self.generate_label(&"join_in_where_not_null".into()),
                parent,
                Expr::BinaryOp {
                    lhs: Box::new(Expr::Column(lhs.clone())),
                    op: BinaryOperator::IsNot,
                    rhs: Box::new(Expr::Literal(Literal::Null)),
                },
            );
        }

        let query_graph = to_query_graph(subquery.clone(), self.dialect)?;
        let subquery_leaf = self.named_query_to_mir(
            query_name,
            &query_graph,
            &HashMap::new(),
            LeafBehavior::Anonymous,
        )?;

        let cols = self.columns(subquery_leaf);
        if cols.len() != 1 {
            invalid_query!(
                "Subquery on right-hand side of \"{}\" expression must have exactly one column",
                text_context
            );
        }

        let col = cols.into_iter().next().expect("Just checked");
        let distinct = self.make_distinct_node(
            query_name,
            self.generate_label(&format!("{text_context}_subquery_distinct").into()),
            subquery_leaf,
            vec![col.clone()],
        );

        let join_preds = &[JoinPredicate {
            left: lhs,
            right: ast::Column {
                name: col.name,
                table: col.table,
            },
        }];

        let is_correlated = is_correlated(subquery);

        let node_index = if negated {
            self.make_antijoin(
                query_name,
                self.generate_label(&format!("join_not_{text_context}_subquery").into()),
                join_preds,
                parent,
                distinct,
                /* dependent = */ is_correlated,
            )?
        } else {
            self.make_join_node(
                query_name,
                self.generate_label(&format!("join_{text_context}_subquery").into()),
                join_preds,
                parent,
                distinct,
                if is_correlated {
                    JoinKind::DependentInner
                } else {
                    JoinKind::Inner
                },
            )?
        };

        Ok(node_index)
    }

    fn make_predicate_nodes(
        &mut self,
        query_name: &Relation,
        name: Relation,
        parent: NodeIndex,
        ce: &Expr,
    ) -> ReadySetResult<NodeIndex> {
        let output_cols = self.mir_graph.columns(parent);
        let leaf = match ce {
            Expr::BinaryOp {
                lhs,
                op: BinaryOperator::And,
                rhs,
            } => {
                let left_subquery_leaf =
                    self.make_predicate_nodes(query_name, name.clone(), parent, lhs)?;

                self.make_predicate_nodes(query_name, name, left_subquery_leaf, rhs)?
            }
            Expr::BinaryOp {
                lhs,
                op: BinaryOperator::Or,
                rhs,
            } => {
                let left_subquery_leaf =
                    self.make_predicate_nodes(query_name, name.clone(), parent, lhs)?;
                let right_subquery_leaf =
                    self.make_predicate_nodes(query_name, name.clone(), parent, rhs)?;

                debug!("Creating union node for `or` predicate");

                self.make_union_from_same_base(
                    query_name,
                    format!(
                        "{}_un{}",
                        name.display_unquoted(),
                        self.mir_graph.node_count()
                    )
                    .into(),
                    vec![left_subquery_leaf, right_subquery_leaf],
                    output_cols,
                    // the filters might overlap, so we need to set BagUnion mode which
                    // removes rows in one side that exist in the other
                    union::DuplicateMode::BagUnion,
                )?
            }
            Expr::Literal(_) | Expr::Column(_) => self.make_filter_node(
                query_name,
                format!(
                    "{}_f{}",
                    name.display_unquoted(),
                    self.mir_graph.node_count()
                )
                .into(),
                parent,
                Expr::BinaryOp {
                    lhs: Box::new(ce.clone()),
                    op: BinaryOperator::NotEqual,
                    rhs: Box::new(Expr::Literal(Literal::Integer(0))),
                },
            ),
            Expr::Between { .. } => internal!("BETWEEN should have been removed earlier"),
            Expr::Exists(subquery) => {
                self.handle_exists(ce, query_name, &name, parent, subquery)?
            }
            Expr::UnaryOp {
                op: UnaryOperator::Not,
                rhs,
            } if matches!(rhs.as_ref(), Expr::Exists(_)) => {
                let subquery = match rhs.as_ref() {
                    Expr::Exists(subquery) => subquery,
                    _ => unreachable!(),
                };

                self.handle_exists(ce, query_name, &name, parent, subquery)?
            }
            Expr::Call(_) => {
                internal!("Function calls should have been handled by projection earlier")
            }
            Expr::In {
                lhs,
                rhs: InValue::Subquery(subquery),
                negated,
            } => {
                //     σ[lhs IN π[x]R₂](R₁)
                //
                // is compiled like
                //
                //     R₁ ⋈[lhs ≡ rhs] π[DISTINCT x AS rhs](R₂)
                //
                // and
                //
                //     σ[lhs NOT IN π[x]R₂](R₁)
                //
                // is compiled like
                //
                //     σ[mark IS NULL](R₁ ⟕[lhs ≡ rhs] π[DISTINCT x AS rhs, 0 AS mark](R₂))
                self.handle_in_and_scalar(
                    query_name,
                    parent,
                    lhs,
                    subquery,
                    if *negated {
                        SubqueryContext::NotIn
                    } else {
                        SubqueryContext::In
                    },
                )?
            }
            Expr::BinaryOp {
                lhs,
                op: BinaryOperator::Equal,
                rhs,
            } if matches!(&**rhs, NestedSelect { .. }) => {
                //     σ[lhs = π[x]R₂](R₁)
                //
                // is compiled like
                //
                //     R₁ ⋈[lhs = rhs] π[DISTINCT x AS rhs](R₂)
                let subquery = match &**rhs {
                    NestedSelect(subquery) => subquery,
                    _ => unreachable!("Just checked"),
                };
                self.handle_in_and_scalar(
                    query_name,
                    parent,
                    lhs,
                    subquery,
                    SubqueryContext::Scalar,
                )?
            }
            Expr::BinaryOp {
                lhs,
                op: BinaryOperator::Equal,
                rhs,
            } if matches!(&**lhs, NestedSelect { .. }) => {
                //     σ[π[x]R₂ = rhs](R₁)
                //
                // is compiled like
                //
                //     R₁ ⋈[lhs = rhs] π[DISTINCT x AS lhs](R₂)
                let subquery = match &**lhs {
                    NestedSelect(subquery) => subquery,
                    _ => unreachable!("Just checked"),
                };
                self.handle_in_and_scalar(
                    query_name,
                    parent,
                    rhs,
                    subquery,
                    SubqueryContext::Scalar,
                )?
            }
            NestedSelect(_) => unsupported!("Nested selects not supported in filters"),
            _ => self.make_filter_node(
                query_name,
                format!(
                    "{}_f{}",
                    name.display_unquoted(),
                    self.mir_graph.node_count()
                )
                .into(),
                parent,
                ce.clone(),
            ),
        };

        Ok(leaf)
    }

    fn make_window_node(
        &mut self,
        query_name: &Relation,
        name: Relation,
        mut parent: NodeIndex,
        funcs: &[WindowFunction],
        group_by: Vec<Column>,
    ) -> ReadySetResult<NodeIndex> {
        if funcs.is_empty() {
            return Ok(parent);
        } else if funcs.len() != 1 {
            // TOOD(mohamed): On paper, it should be possible by simply connecting the
            // window functions in series (fn1 -> fn2 -> fn3 -> ... -> reader)
            // However, the performance will be so bad that we rather just not support
            // it for now. Making the execution "smarter" with how it handles
            // its inputs and outputs will make this possible, but should be left
            // as a future improvement
            unsupported!("Using multiple window functions is not yet supported");
        }

        let dialect: readyset_sql::Dialect = self.dialect.into();
        let WindowFunction {
            function,
            partition_by,
            order_by,
            alias,
        } = funcs.first().unwrap().clone();

        let arguments = function.arguments().cloned().collect::<Vec<_>>();

        let function = WindowOperationKind::from_fn(function)?;

        let order_cols = order_by
            .iter()
            .map(|(e, _, _)| e)
            .cloned()
            .collect::<Vec<_>>();

        let has_agg = |e: &Expr| matches!(e, Expr::Call(f) if is_aggregate(f));
        if partition_by.iter().any(has_agg)
            || arguments.iter().any(has_agg)
            || order_cols.iter().any(has_agg)
        {
            unsupported!("Aggregates in window functions not yet supported");
        }

        // if the partition, ordering cols, or args require projection,
        // create a projection node and use that as a parent
        // TODO: do we need to make this strict? i.e. only project certain
        // Expr variants?
        let p = |e: &Expr| !matches!(e, Expr::Column(_));
        let needs_proj_node =
            partition_by.iter().any(p) || arguments.iter().any(p) || order_cols.iter().any(p);

        if needs_proj_node {
            let node_name = format!(
                "{}_window_project_n{}",
                name.display_unquoted(),
                self.mir_graph.node_count()
            );

            let node = self.make_project_node(
                query_name,
                node_name.into(),
                parent,
                partition_by
                    .iter()
                    .chain(arguments.iter())
                    .chain(order_cols.iter())
                    .cloned()
                    .map(|e| -> ReadySetResult<_> {
                        Ok(ProjectExpr::Expr {
                            alias: e
                                .alias(dialect)
                                // returns None if e is a placeholder or a variable
                                .ok_or_else(|| {
                                    unsupported_err!("Placeholders not allowed in this context")
                                })?,
                            expr: e,
                        })
                    })
                    .collect::<ReadySetResult<Vec<_>>>()?,
            );
            parent = node;
        }

        let output_column = Column::named(alias);

        let partition_by = partition_by
            .iter()
            .map(|e| e.alias(dialect).unwrap())
            .map(|e| Column::named(e))
            .collect();

        let order_by = order_by
            .into_iter()
            .map(|(e, order, no)| (Column::named(e.alias(dialect).unwrap()), order, no))
            .collect();

        let args = arguments
            .iter()
            .map(|e| e.alias(dialect).unwrap())
            .map(|e| Column::named(e))
            .collect();

        let node_name = format!(
            "{}_window_n{}",
            name.display_unquoted(),
            self.mir_graph.node_count()
        );

        Ok(self.add_query_node(
            query_name.clone(),
            MirNode::new(
                node_name.into(),
                MirNodeInner::Window {
                    group_by,
                    partition_by,
                    order_by,
                    output_column,
                    function,
                    args,
                },
            ),
            &[parent],
        ))
    }

    fn predicates_above_group_by<'a>(
        &mut self,
        query_name: &Relation,
        name: Relation,
        column_to_predicates: &HashMap<ast::Column, Vec<&'a Expr>>,
        over_col: &ast::Column,
        parent: NodeIndex,
        created_predicates: &mut Vec<&'a Expr>,
    ) -> ReadySetResult<NodeIndex> {
        let mut leaf = parent;

        let ces = column_to_predicates.get(over_col).unwrap();
        for ce in ces {
            // If we have two aggregates over the same column, we will skip this step for the
            // second aggregate
            if !created_predicates.contains(ce) {
                let subquery_leaf = self.make_predicate_nodes(
                    query_name,
                    format!(
                        "{}_mp{}",
                        name.display_unquoted(),
                        self.mir_graph.node_count()
                    )
                    .into(),
                    leaf,
                    ce,
                )?;
                leaf = subquery_leaf;
                created_predicates.push(ce);
            }
        }

        Ok(leaf)
    }

    fn make_value_project_node(
        &mut self,
        query_name: &Relation,
        qg: &QueryGraph,
        prev_node: NodeIndex,
    ) -> ReadySetResult<Option<NodeIndex>> {
        let arith_and_lit_columns_needed =
            value_columns_needed_for_predicates(&qg.columns, &qg.global_predicates);

        if !arith_and_lit_columns_needed.is_empty() {
            let passthru_cols = self.mir_graph.columns(prev_node);
            let mut emit = passthru_cols
                .into_iter()
                .map(ProjectExpr::Column)
                .collect::<Vec<_>>();
            for (_, oc) in arith_and_lit_columns_needed {
                match oc {
                    OutputColumn::Data { .. } => {}
                    OutputColumn::Literal(lc) => emit.push(ProjectExpr::Expr {
                        expr: Expr::Literal(lc.value),
                        alias: lc.name,
                    }),
                    OutputColumn::Expr(ec) => emit.push(ProjectExpr::Expr {
                        expr: ec.expression,
                        alias: ec.name,
                    }),
                }
            }

            let projected = self.make_project_node(
                query_name,
                format!(
                    "q_{:x}_n{}",
                    qg.signature().hash,
                    self.mir_graph.node_count()
                )
                .into(),
                prev_node,
                emit,
            );

            Ok(Some(projected))
        } else {
            Ok(None)
        }
    }

    /// Adds all the MIR nodes corresponding to the given query,
    /// and returns the index of its leaf node.
    #[allow(clippy::cognitive_complexity)]
    pub(super) fn named_query_to_mir(
        &mut self,
        query_name: &Relation,
        query_graph: &QueryGraph,
        anon_queries: &HashMap<Relation, NodeIndex>,
        leaf_behavior: LeafBehavior,
    ) -> Result<NodeIndex, ReadySetError> {
        // TODO(fran): We are not modifying the execution of this method with the implementation
        //  of petgraph, which causes us to create nodes that could now easily be reused:
        //  Reuse should just require that we add the query name to the "owners" hashset in the
        //  reused nodes if the node properties are identical.

        // Canonical operator order: B-J-F-G-P-R
        // (Base, Join, Filter, GroupBy, Project, Reader)
        let leaf = {
            let mut node_for_rel: HashMap<&Relation, NodeIndex> = HashMap::default();
            let mut correlated_relations: HashSet<NodeIndex> = Default::default();

            // Convert the query parameters to an ordered list of columns that will comprise the
            // lookup key if a leaf node is attached.
            let view_key = query_graph.view_key(self.config())?;

            // 0. Base nodes (always reused)
            let mut base_nodes: Vec<NodeIndex> = Vec::new();
            let mut sorted_rels: Vec<&Relation> = query_graph.relations.keys().collect();
            sorted_rels.sort_unstable();
            for rel in &sorted_rels {
                let base_for_rel = if let Some(subquery) = &query_graph.relations[*rel].subgraph {
                    let correlated = subquery.is_correlated;
                    let subquery_leaf = self.named_query_to_mir(
                        query_name,
                        subquery,
                        &HashMap::new(),
                        LeafBehavior::Anonymous,
                    )?;
                    if correlated {
                        correlated_relations.insert(subquery_leaf);
                    }
                    subquery_leaf
                } else {
                    match self.get_relation(rel) {
                        Some(node_idx) => node_idx,
                        None => anon_queries
                            .get(rel)
                            .copied()
                            .ok_or_else(|| self.table_not_found_err(rel))?,
                    }
                };

                self.mir_graph[base_for_rel].add_owner(query_name.clone());

                let alias_table_node_name = format!(
                    "q_{:x}_{}_alias_table_{}",
                    query_graph.signature().hash,
                    self.mir_graph[base_for_rel].name().display_unquoted(),
                    rel.name
                )
                .into();

                let alias_table_node = self.add_query_node(
                    query_name.clone(),
                    MirNode::new(
                        alias_table_node_name,
                        MirNodeInner::AliasTable {
                            table: (*rel).clone(),
                        },
                    ),
                    &[base_for_rel],
                );

                base_nodes.push(alias_table_node);
                node_for_rel.insert(*rel, alias_table_node);
            }

            let join_nodes = make_joins(
                self,
                query_name,
                format!("q_{:x}", query_graph.signature().hash).into(),
                query_graph,
                &node_for_rel,
                &correlated_relations,
            )?;

            let mut prev_node = match join_nodes.last() {
                Some(&n) => n,
                None => {
                    invariant!(!base_nodes.is_empty());
                    if base_nodes.len() > 1 {
                        // If we have more than one base node, that means we have a list of tables
                        // that don't have (obvious) join clauses we can pull out of the conditions.
                        // So we need to make no-condition (cross) joins for those tables.
                        //
                        // Later, the optimizer might decide to add conditions to the joins anyway.
                        make_cross_joins(
                            self,
                            query_name,
                            &format!("q_{:x}", query_graph.signature().hash),
                            base_nodes.clone(),
                            &correlated_relations,
                        )?
                        .last()
                        .copied()
                        .unwrap()
                    } else {
                        #[allow(clippy::unwrap_used)] // checked above
                        *base_nodes.last().unwrap()
                    }
                }
            };

            // 2. If we're aggregating on expressions rather than directly on columns, project out
            // those expressions before the aggregate itself
            let expressions_above_grouped = make_expressions_above_grouped(
                self,
                query_name,
                &format!("q_{:x}", query_graph.signature().hash),
                query_graph,
                &mut prev_node,
            );

            // 3. Get columns used by each predicate. This will be used to check
            // if we need to reorder predicates before group_by nodes.
            let mut column_to_predicates: HashMap<ast::Column, Vec<&Expr>> = HashMap::new();

            for rel in &sorted_rels {
                let qgn = query_graph
                    .relations
                    .get(*rel)
                    .ok_or_else(|| internal_err!("couldn't find {:?} in qg relations", rel))?;
                for pred in qgn.predicates.iter().chain(&query_graph.global_predicates) {
                    for col in pred.referred_columns() {
                        column_to_predicates
                            .entry(col.clone())
                            .or_default()
                            .push(pred);
                    }
                }
            }

            // 3a. Reorder some predicates before group by nodes
            // FIXME(malte): This doesn't currently work correctly with arithmetic and literal
            // projections that form input to these filters -- these need to be lifted above them
            // (and above the aggregations).
            let created_predicates = make_predicates_above_grouped(
                self,
                query_name,
                format!("q_{:x}", query_graph.signature().hash).into(),
                query_graph,
                &column_to_predicates,
                &mut prev_node,
            )?;

            // 5. Generate the necessary filter nodes for local predicates associated with each
            // relation node in the query graph.
            //
            // Need to iterate over relations in a deterministic order, as otherwise nodes will be
            // added in a different order every time, which will yield different node identifiers
            // and make it difficult for applications to check what's going on.
            for rel in &sorted_rels {
                let qgn = query_graph
                    .relations
                    .get(*rel)
                    .ok_or_else(|| internal_err!("qg relations did not contain {:?}", rel))?;
                // the following conditional is required to avoid "empty" nodes (without any
                // projected columns) that are required as inputs to joins
                if !qgn.predicates.is_empty() {
                    // add a predicate chain for each query graph node's predicates
                    for (i, ref p) in qgn.predicates.iter().enumerate() {
                        if created_predicates.contains(p) {
                            continue;
                        }

                        let subquery_leaf = self.make_predicate_nodes(
                            query_name,
                            format!(
                                "q_{:x}_n{}_p{}",
                                query_graph.signature().hash,
                                self.mir_graph.node_count(),
                                i
                            )
                            .into(),
                            prev_node,
                            p,
                        )?;

                        prev_node = subquery_leaf;
                    }
                }
            }

            // 6. Determine literals and expressions that global predicates depend on and add them
            //    here; remembering that we've already added them-
            if let Some(projected) =
                self.make_value_project_node(query_name, query_graph, prev_node)?
            {
                prev_node = projected;
            }

            // 7. Global predicates
            for (i, ref p) in query_graph.global_predicates.iter().enumerate() {
                if created_predicates.contains(p) {
                    continue;
                }

                let subquery_leaf = self.make_predicate_nodes(
                    query_name,
                    format!(
                        "q_{:x}_n{}_{}",
                        query_graph.signature().hash,
                        self.mir_graph.node_count(),
                        i
                    )
                    .into(),
                    prev_node,
                    p,
                )?;

                prev_node = subquery_leaf;
            }

            // 8. Add window functions or grouped nodes (mutually exclusive for now)
            if !query_graph.aggregates.is_empty() && !query_graph.window_functions.is_empty() {
                unsupported!("Mixing window functions and aggregates is not yet supported")
            };

            let group_by: Vec<_> = view_key.columns.iter().map(|(c, _)| c.clone()).collect();

            prev_node = self.make_window_node(
                query_name,
                format!("q_{:x}", query_graph.signature().hash).into(),
                prev_node,
                &query_graph.window_functions,
                group_by,
            )?;

            let mut func_nodes: Vec<NodeIndex> = make_grouped(
                self,
                query_name,
                format!("q_{:x}", query_graph.signature().hash).into(),
                query_graph,
                &node_for_rel,
                &mut prev_node,
                &expressions_above_grouped,
            )?;

            // 9. Add predicate nodes for HAVING after GROUP BY nodes
            for (i, p) in query_graph.having_predicates.iter().enumerate() {
                let hp_name = format!(
                    "q_{:x}_h{}_{}",
                    query_graph.signature().hash,
                    self.mir_graph.node_count(),
                    i
                )
                .into();
                let subquery_leaf = self.make_predicate_nodes(query_name, hp_name, prev_node, p)?;

                prev_node = subquery_leaf;
            }

            // 10. Get the final node
            let mut final_node = prev_node;

            if let Some(Pagination {
                order,
                limit,
                offset,
            }) = query_graph.pagination.as_ref()
            {
                let make_topk = offset.is_none();
                // view key will have the offset parameter if it exists. We must filter it out
                // of the group by, because the column originates at this node
                let group_by: Vec<Column> = view_key
                    .columns
                    .iter()
                    .filter_map(|(col, _)| {
                        if col.name != *PAGE_NUMBER_COL {
                            Some(col.clone())
                        } else {
                            None
                        }
                    })
                    .collect();

                // Order by expression projections and either a topk or paginate node
                let paginate_nodes = self.make_paginate_node(
                    query_name,
                    format!(
                        "q_{:x}_n{}",
                        query_graph.signature().hash,
                        self.mir_graph.node_count()
                    )
                    .into(),
                    final_node,
                    group_by,
                    order,
                    *limit,
                    make_topk,
                    Some(&query_graph.columns),
                )?;
                func_nodes.extend(paginate_nodes.clone());
                final_node = *paginate_nodes.last().unwrap();
            }

            // 10. Generate leaf views that expose the query result

            // We may already have added some of the expression and literal columns
            let (_, mut already_computed): (Vec<_>, Vec<_>) = value_columns_needed_for_predicates(
                &query_graph.columns,
                &query_graph.global_predicates,
            )
            .into_iter()
            .unzip();

            // Project out any columns that need special handling
            for oc in &query_graph.columns {
                if expressions_above_grouped
                    .values()
                    .any(|alias| alias == oc.name())
                {
                    // Already projected, nothing to do
                    already_computed.push(oc.clone());
                    continue;
                }

                if let OutputColumn::Expr(ExprColumn {
                    name,
                    table: None,
                    expression:
                        Expr::In {
                            lhs,
                            rhs: InValue::Subquery(subquery),
                            negated,
                        },
                }) = oc
                {
                    final_node = self.project_in_subquery(
                        query_name,
                        final_node,
                        name,
                        lhs,
                        (**subquery).clone(),
                        *negated,
                    )?;
                    already_computed.push(oc.clone());
                }
            }

            let mut emit = query_graph
                .columns
                .iter()
                .map(|oc| match oc {
                    OutputColumn::Expr(ac) => {
                        if !already_computed.contains(oc) {
                            ProjectExpr::Expr {
                                alias: ac.name.clone(),
                                expr: ac.expression.clone(),
                            }
                        } else {
                            ProjectExpr::Column(Column::named(&ac.name))
                        }
                    }
                    OutputColumn::Literal(lc) => {
                        if !already_computed.contains(oc) {
                            ProjectExpr::Expr {
                                alias: lc.name.clone(),
                                expr: Expr::Literal(lc.value.clone()),
                            }
                        } else {
                            ProjectExpr::Column(Column::named(&lc.name))
                        }
                    }
                    OutputColumn::Data { column, alias } => {
                        ProjectExpr::Column(Column::from(column).aliased_as(alias.clone()))
                    }
                })
                .collect::<Vec<_>>();

            if leaf_behavior.should_make_leaf() {
                for (column, _) in &view_key.columns {
                    if !emit
                        .iter()
                        .any(|expr| matches!(expr, ProjectExpr::Column(c) if c == column))
                    {
                        emit.push(ProjectExpr::Column(column.clone()))
                    }
                }
            }

            final_node = self.make_project_node(
                query_name,
                if leaf_behavior.should_make_leaf() {
                    format!("q_{:x}_project", query_graph.signature().hash).into()
                } else {
                    query_name.clone()
                },
                final_node,
                emit.clone(),
            );

            if query_graph.distinct {
                let name = if leaf_behavior.should_make_leaf() {
                    format!(
                        "q_{:x}_n{}",
                        query_graph.signature().hash,
                        self.mir_graph.node_count()
                    )
                    .into()
                } else {
                    format!(
                        "{}_d{}",
                        query_name.display_unquoted(),
                        self.mir_graph.node_count()
                    )
                    .into()
                };
                // This needs to go *after* the leaf project node, so that we get distinct values
                // for the results of expressions (which might not be injective) rather than for the
                // *inputs* to those expressions
                final_node =
                    self.make_distinct_node(query_name, name, final_node, self.columns(final_node));
            }

            if leaf_behavior.should_make_leaf() {
                // We are supposed to add a `Leaf` node keyed on the query parameters. For purely
                // internal views (e.g., subqueries), this is not set.
                let returned_cols = query_graph
                    .fields
                    .iter()
                    .map(|expression| -> ReadySetResult<_> {
                        match expression {
                            FieldDefinitionExpr::All | FieldDefinitionExpr::AllInTable(_) => {
                                internal!("All expression should have been desugared at this point")
                            }
                            FieldDefinitionExpr::Expr {
                                alias: Some(alias), ..
                            } => Ok(Column::named(alias.clone())),
                            FieldDefinitionExpr::Expr {
                                expr: Expr::Column(c),
                                ..
                            } => Ok(Column::from(c)),
                            FieldDefinitionExpr::Expr { expr, .. } => {
                                Ok(Column::named(expr.display(self.dialect.into()).to_string()))
                            }
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                // After we have all of our returned columns figured out, find out how they are
                // projected by this projection, so we can then add another projection that returns
                // the columns in the correct order
                let mut project_order = Vec::with_capacity(returned_cols.len());
                let parent_columns = self.mir_graph.columns(final_node);
                for col in returned_cols.iter() {
                    if let Some(c) = parent_columns.iter().find(|c| col.cmp(c).is_eq()) {
                        project_order.push(ProjectExpr::Column(c.clone()));
                    } else {
                        internal!("Returned column {col} not in projected schema");
                    }
                }
                for col in parent_columns {
                    if !emit
                        .iter()
                        .any(|expr| matches!(expr, ProjectExpr::Column(c) if *c == col) ||
                            matches!(expr, ProjectExpr::Expr { alias: name, .. } if *name == col.name))
                    {
                        project_order.push(ProjectExpr::Column(col));
                    }
                }

                // Add another project node that will return the required columns in the right order
                let leaf_project_reorder_node = self.make_project_node(
                    query_name,
                    if leaf_behavior.should_make_leaf() {
                        format!("q_{:x}_project_reorder", query_graph.signature().hash).into()
                    } else {
                        query_name.clone()
                    },
                    final_node,
                    project_order,
                );

                let are_repeat_reads_required =
                    query_graph.collapsed_where_in || view_key.index_type == IndexType::BTreeMap;

                if are_repeat_reads_required && !query_graph.window_functions.is_empty() {
                    unsupported!("Post-lookups are not supported for window functions");
                }

                let post_lookup_aggregates = if are_repeat_reads_required {
                    // When a query contains WHERE col IN (?, ?, ...), it gets rewritten
                    // (or collapsed) to WHERE col = ? during SQL parsing, with the
                    // collapsed_where_in flag set to indicate this transformation.
                    //
                    // This creates a correctness issue for aggregates: the original multi-value IN clause
                    // should aggregate across all matching rows, but the rewritten single-value equality
                    // will only see one row at a time. To fix this, we need post-lookup aggregation that
                    // combines results from multiple point lookups.
                    //
                    // Example:
                    //   Original: SELECT sum(amount) FROM orders WHERE id IN (1, 2, 3)
                    //   Rewritten: SELECT sum(amount) FROM orders WHERE id = ? (executed 3 times)
                    //   Solution: Sum the results from each execution via post-lookup aggregation
                    //
                    // And obviously if the query is a range query, we need post-lookup aggregation
                    // since we can't precompute aggregations over different ranges.
                    //
                    // Note: Post-lookup operations have performance overhead, so they're gated behind
                    // the allow_post_lookup config flag.
                    match post_lookup_aggregates(query_graph, query_name, self.dialect)? {
                        Some(agg) if self.config.allow_post_lookup => Some(agg),
                        Some(_) => {
                            unsupported!(
                                "Queries which perform operations post-lookup are not supported"
                            );
                        }
                        // repeat reads with no aggregates, no problem
                        None => None,
                    }
                } else {
                    None
                };

                let order_by = query_graph.order.as_ref().map(|order| {
                    order
                        .iter()
                        .map(|(c, ot, no)| (Column::from(c), *ot, *no))
                        .collect()
                });
                let mut limit = query_graph.pagination.as_ref().map(|p| p.limit);
                let offset = query_graph.pagination.as_ref().and_then(|p| p.offset);
                let is_topk_query = order_by.is_some() && limit.is_some() && offset.is_none();

                // If the query is a topk query, and the user has opted in to using
                // topk feature, remove the limit from the post-lookup as TopK node
                // will handle it UNLESS the original query requires repeated reads.
                // In that case, limit is required for correctness.
                if self.config.allow_topk && is_topk_query && !are_repeat_reads_required {
                    limit = None;
                    // TODO: even though we are doing topk, we still need the reader
                    // to order stuff. Becuase TopK communictes the diff,
                    // and the reader keeps the values in ASC order if ORDER BY
                    // is not specified. Please refer to [reader_map::Values] struct.
                    // order_by = None;
                }

                self.add_query_node(
                    query_name.clone(),
                    MirNode::new(
                        query_name.clone(),
                        MirNodeInner::Leaf {
                            keys: view_key.columns.into_iter().collect(),
                            index_type: view_key.index_type,
                            lowered_to_df: false,
                            order_by,
                            limit,
                            returned_cols: Some(returned_cols),
                            default_row: query_graph.default_row.clone(),
                            aggregates: post_lookup_aggregates,
                        },
                    ),
                    &[leaf_project_reorder_node],
                )
            } else {
                trace!("Making view keys for queries instead of leaf node");

                let mut keys = Vec::with_capacity(view_key.columns.len());
                let mut unsupported_placeholders = vec![];
                for (column, vp) in view_key.columns {
                    match vp {
                        ViewPlaceholder::OneToOne(placeholder_idx, op) => {
                            keys.push(ViewKeyColumn {
                                column,
                                op,
                                placeholder_idx,
                            })
                        }
                        ViewPlaceholder::Generated => {}
                        ViewPlaceholder::Between(lower, upper) => {
                            unsupported_placeholders.extend([lower as u32, upper as u32])
                        }
                        ViewPlaceholder::PageNumber {
                            offset_placeholder, ..
                        } => unsupported_placeholders.push(offset_placeholder as u32),
                    }
                }

                if let Ok(key) = keys.try_into() {
                    self.add_query_node(
                        query_name.clone(),
                        MirNode::new(
                            format!("{}_view_key", query_name.display_unquoted()).into(),
                            MirNodeInner::ViewKey { key },
                        ),
                        &[final_node],
                    )
                } else if let Ok(placeholders) = unsupported_placeholders.try_into() {
                    return Err(ReadySetError::UnsupportedPlaceholders { placeholders });
                } else {
                    final_node
                }
            }
        };

        if leaf_behavior.should_register() {
            self.relations.insert(query_name.clone(), leaf);
        }

        debug!(query_name = %query_name.display_unquoted(), "Added final MIR node for query");

        // finally, we output all the nodes we generated
        Ok(leaf)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::controller::sql::mir::SqlToMirConverter;
    use crate::sql::mir::{Config, LeafBehavior};
    use mir::node::MirNodeInner;
    use mir::NodeIndex;
    use readyset_errors::ReadySetResult;
    use readyset_sql::ast::{
        Column, ColumnSpecification, Relation, SelectMetadata, SqlType, TableKey,
    };

    use crate::controller::sql::query_graph::to_query_graph;
    use readyset_data::Dialect;
    use readyset_sql_parsing::parse_select;

    fn sql_to_mir_test(
        name: &str,
        qg: crate::sql::query_graph::QueryGraph,
        table_name: &str,
        columns: &[ColumnSpecification],
        keys: Option<&Vec<TableKey>>,
    ) -> ReadySetResult<(SqlToMirConverter, NodeIndex)> {
        let mut converter = SqlToMirConverter::new(Dialect::DEFAULT_MYSQL);
        converter.set_config(Config {
            allow_topk: true,
            allow_post_lookup: true,
            ..Default::default()
        });

        let _ = converter.make_base_node(&Relation::from(table_name), columns, keys)?;

        let node = converter.named_query_to_mir(
            &Relation::from(name),
            &qg,
            &HashMap::new(),
            LeafBehavior::Leaf,
        )?;

        Ok((converter, node))
    }

    macro_rules! test_topk_scenario {
        (
        name: $test_name:ident,
        query: $query_str:literal,
        query_name: $query_name:literal,
        collapsed_where_in: $collapsed:expr,
        expect_leaf: {
            aggregates: $expect_agg:expr,
            order_by: $expect_order:expr,
            limit: $expect_limit:expr
        },
        expect_topk_node: $expect_topk:expr
    ) => {
            #[test]
            fn $test_name() -> ReadySetResult<()> {
                let mut query =
                    parse_select(readyset_sql::Dialect::PostgreSQL, $query_str).unwrap();

                if $collapsed {
                    query.metadata.push(SelectMetadata::CollapsedWhereIn);
                }

                let table_name = "topk_test";

                let columns = &[
                    ColumnSpecification {
                        column: Column::from("topk_test.a"),
                        sql_type: SqlType::Int(None),
                        generated: None,
                        constraints: vec![],
                        comment: None,
                    },
                    ColumnSpecification {
                        column: Column::from("topk_test.b"),
                        sql_type: SqlType::Int(None),
                        generated: None,
                        constraints: vec![],
                        comment: None,
                    },
                    ColumnSpecification {
                        column: Column::from("topk_test.c"),
                        sql_type: SqlType::Int(None),
                        generated: None,
                        constraints: vec![],
                        comment: None,
                    },
                ];

                let qg = to_query_graph(query, Dialect::DEFAULT_MYSQL).unwrap();
                let (mut converter, node) =
                    sql_to_mir_test($query_name, qg, table_name, columns, None)?;
                let query = converter.make_mir_query($query_name.into(), node);

                // Check leaf node properties
                if let MirNodeInner::Leaf {
                    aggregates,
                    order_by,
                    limit,
                    ..
                } = &query.get_node(node).unwrap().inner
                {
                    assert_eq!(aggregates.is_some(), $expect_agg, "aggregates mismatch");
                    assert_eq!(order_by.is_some(), $expect_order, "order_by mismatch");
                    assert_eq!(limit.is_some(), $expect_limit, "limit mismatch");
                } else {
                    panic!("Expected leaf node");
                }

                // Check for TopK node existence
                let mut has_topk = false;
                for node in query.topo_nodes() {
                    if let MirNodeInner::TopK { .. } = &query.get_node(node).unwrap().inner {
                        has_topk = true;
                        break;
                    }
                }

                if $expect_topk {
                    assert!(has_topk, "topk node not found");
                } else {
                    assert!(!has_topk, "unexpected topk node found");
                }

                Ok(())
            }
        };
    }

    test_topk_scenario! {
        name: topk_node_exists,
        query: "SELECT a FROM topk_test ORDER BY b LIMIT 3",
        query_name: "q1",
        collapsed_where_in: false,
        expect_leaf: {
            aggregates: false,
            order_by: true,
            limit: false
        },
        expect_topk_node: true
    }

    test_topk_scenario! {
        name: topk_node_exists_with_where_in,
        query: "SELECT a FROM topk_test WHERE b = 1 ORDER BY c LIMIT 3",
        query_name: "q1",
        collapsed_where_in: true,
        expect_leaf: {
            aggregates: false,
            order_by: true,
            limit: true
        },
        expect_topk_node: true
    }

    test_topk_scenario! {
        name: aggregate_with_where_in,
        query: "SELECT sum(topk_test.a) FROM topk_test WHERE b = 5 GROUP BY c ORDER BY b",
        query_name: "q2",
        collapsed_where_in: true,
        expect_leaf: {
            aggregates: true,
            order_by: true,
            limit: false
        },
        expect_topk_node: false
    }

    test_topk_scenario! {
        name: topk_without_where_in,
        query: "SELECT avg(topk_test.a) FROM topk_test WHERE topk_test.b = 5 GROUP BY topk_test.c ORDER BY topk_test.b LIMIT 10",
        query_name: "q2",
        collapsed_where_in: false,
        expect_leaf: {
            aggregates: false,
            order_by: true,
            limit: false
        },
        expect_topk_node: true
    }

    test_topk_scenario! {
        name: avg_with_no_limit,
        query: "SELECT avg(topk_test.a) FROM topk_test WHERE topk_test.b = 5 GROUP BY topk_test.c ORDER BY topk_test.b",
        query_name: "q3",
        collapsed_where_in: false,
        expect_leaf: {
            // aggregate should be handled by the agg node, not the leaf
            // in post-lookups
            aggregates: false,
            order_by: true,
            limit: false
        },
        expect_topk_node: false
    }

    test_topk_scenario! {
        name: use_agg_alias_in_order_by,
        query: "SELECT avg(topk_test.a) as avg_a FROM topk_test WHERE topk_test.b = 5 GROUP BY topk_test.c ORDER BY avg_a",
        query_name: "q3",
        collapsed_where_in: false,
        expect_leaf: {
            aggregates: false,
            order_by: true,
            limit: false
        },
        expect_topk_node: false
    }

    test_topk_scenario! {
        name: use_agg_alias_in_topk,
        query: "SELECT avg(topk_test.a) as avg_a FROM topk_test WHERE topk_test.b = 5 GROUP BY topk_test.c ORDER BY avg_a LIMIT 10",
        query_name: "q2",
        collapsed_where_in: false,
        expect_leaf: {
            aggregates: false,
            order_by: true,
            limit: false
        },
        expect_topk_node: true
    }

    test_topk_scenario! {
        name: topk_with_where_in,
        query: "SELECT a FROM topk_test WHERE b = 5 ORDER BY a LIMIT 10",
        query_name: "q1",
        collapsed_where_in: true,
        expect_leaf: {
            aggregates: false,
            order_by: true,
            limit: true
        },
        expect_topk_node: true
    }

    macro_rules! test_mir_with_config {
        (
            name: $test_name:ident,
            query: $query_str:literal,
            collapsed_where_in: $collapsed:expr,
            config: $config:expr,
            expect_success: $expect_success:expr
        ) => {
            #[test]
            fn $test_name() {
                let mut query =
                    parse_select(readyset_sql::Dialect::PostgreSQL, $query_str).unwrap();

                if $collapsed {
                    query.metadata.push(SelectMetadata::CollapsedWhereIn);
                }

                let qg = to_query_graph(query, Dialect::DEFAULT_MYSQL).unwrap();

                let table_name = "test_table";
                let columns = &[
                    ColumnSpecification {
                        column: Column::from("test_table.a"),
                        sql_type: SqlType::Int(None),
                        generated: None,
                        constraints: vec![],
                        comment: None,
                    },
                    ColumnSpecification {
                        column: Column::from("test_table.b"),
                        sql_type: SqlType::Int(None),
                        generated: None,
                        constraints: vec![],
                        comment: None,
                    },
                    ColumnSpecification {
                        column: Column::from("test_table.c"),
                        sql_type: SqlType::Int(None),
                        generated: None,
                        constraints: vec![],
                        comment: None,
                    },
                ];

                let mut converter = SqlToMirConverter::new(Dialect::DEFAULT_MYSQL);
                converter.set_config($config);
                let result = converter
                    .make_base_node(&table_name.into(), columns, None)
                    .and_then(|_| {
                        converter.named_query_to_mir(
                            &"q1".into(),
                            &qg,
                            &HashMap::new(),
                            LeafBehavior::Leaf,
                        )
                    });
                if $expect_success {
                    assert!(
                        result.is_ok(),
                        "Expected MIR lowering to succeed, but it failed: {:?}",
                        result.err()
                    );
                } else {
                    assert!(
                        result.is_err(),
                        "Expected MIR lowering to fail, but it succeeded"
                    );
                }
            }
        };
    }

    test_mir_with_config! {
        name: where_in_without_post_lookup,
        query: "SELECT a FROM test_table WHERE b = 5",
        collapsed_where_in: true,
        config: Config::default(),
        expect_success: true
    }

    test_mir_with_config! {
        name: range_queries_without_post_lookup,
        query: "SELECT a FROM test_table WHERE test_table.b > $1",
        collapsed_where_in: false,
        config: Config::default(),
        expect_success: true
    }

    test_mir_with_config! {
        name: agg_with_range_queries,
        query: "SELECT a FROM test_table WHERE test_table.b >= $1 ORDER BY a LIMIT 10",
        collapsed_where_in: false,
        config: Config {
            allow_topk: true,
            ..Default::default()
        },
        expect_success: true
    }

    test_mir_with_config! {
        name: range_queries_with_agg,
        query: "SELECT sum(test_table.a) FROM test_table WHERE test_table.b <= $1",
        collapsed_where_in: false,
        config: Config {
            allow_post_lookup: true,
            ..Default::default()
        },
        expect_success: true
    }

    test_mir_with_config! {
        name: order_by_without_post_lookup,
        query: "SELECT a FROM test_table WHERE test_table.b < $1 ORDER BY a",
        collapsed_where_in: false,
        config: Config::default(),
        expect_success: true
    }

    test_mir_with_config! {
        name: topk_with_repeated_reads,
        query: "SELECT a FROM test_table WHERE b = 5 ORDER BY a LIMIT 10",
        collapsed_where_in: true,
        config: Config {
            allow_topk: true,
            ..Default::default()
        },
        expect_success: true
    }

    test_mir_with_config! {
        name: aggs_with_topk,
        query: "SELECT sum(a) FROM test_table WHERE b = 5 ORDER BY a LIMIT 10",
        collapsed_where_in: true,
        config: Config {
            allow_topk: true,
            ..Default::default()
        },
        // post-lookups not enabled
        expect_success: false
    }
}
