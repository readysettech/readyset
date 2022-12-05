use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::fmt::{Debug, Display};
use std::vec::Vec;

use ::serde::{Deserialize, Serialize};
use common::{DfValue, IndexType};
use dataflow::ops::grouped::aggregate::Aggregation;
use dataflow::ops::union;
use launchpad::redacted::Sensitive;
use lazy_static::lazy_static;
use mir::graph::MirGraph;
use mir::node::node_inner::MirNodeInner;
use mir::node::{GroupedNodeType, MirNode};
use mir::query::{MirBase, MirQuery};
pub use mir::Column;
use nom_sql::analysis::ReferredColumns;
use nom_sql::{
    BinaryOperator, ColumnSpecification, CompoundSelectOperator, CreateTableBody, Expr,
    FieldDefinitionExpr, FieldReference, FunctionExpr, Literal, OrderClause, OrderType, Relation,
    SelectStatement, SqlIdentifier, TableKey, UnaryOperator,
};
use petgraph::graph::NodeIndex;
use petgraph::Direction;
use readyset_errors::{
    internal, internal_err, invalid_err, invariant, invariant_eq, unsupported, ReadySetError,
};
use readyset_sql_passes::is_correlated;
use readyset_tracing::{debug, trace};

use super::query_graph::{extract_limit_offset, JoinPredicate};
use crate::controller::sql::mir::grouped::{
    make_expressions_above_grouped, make_grouped, make_predicates_above_grouped,
    post_lookup_aggregates,
};
use crate::controller::sql::mir::join::{make_cross_joins, make_joins};
use crate::controller::sql::query_graph::{to_query_graph, OutputColumn, Pagination, QueryGraph};
use crate::controller::sql::query_signature::Signature;
use crate::ReadySetResult;

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

fn default_row_for_select(st: &SelectStatement) -> Option<Vec<DfValue>> {
    // If this is an aggregated query AND it does not contain a GROUP BY clause,
    // set default values based on the aggregation (or lack thereof) on each
    // individual field
    if !st.contains_aggregate_select() || st.group_by.is_some() {
        return None;
    }
    Some(
        st.fields
            .iter()
            .map(|f| match f {
                FieldDefinitionExpr::Expr {
                    expr: Expr::Call(func),
                    ..
                } => match func {
                    FunctionExpr::Avg { .. } => DfValue::None,
                    FunctionExpr::Count { .. } => DfValue::Int(0),
                    FunctionExpr::CountStar => DfValue::Int(0),
                    FunctionExpr::Sum { .. } => DfValue::None,
                    FunctionExpr::Max(..) => DfValue::None,
                    FunctionExpr::Min(..) => DfValue::None,
                    FunctionExpr::GroupConcat { .. } => DfValue::None,
                    FunctionExpr::Call { .. } | FunctionExpr::Substring { .. } => DfValue::None,
                },
                _ => DfValue::None,
            })
            .collect(),
    )
}

/// Kinds of joins in MIR
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    /// Inner joins - see [`MirNodeInner::InnerJoin`]
    Inner,
    /// Left joins - see [`MirNodeInner::LeftJoin`]
    Left,
    /// Dependent joins - see [`MirNodeInner::DependentJoin`]
    Dependent,
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
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
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
    pub(in crate::controller::sql) schema_version: usize,
}

impl SqlToMirConverter {
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
    pub(crate) fn generate_label<N>(&self, label_prefix: &N) -> Relation
    where
        N: Display,
    {
        format!("{}_n{}", label_prefix, self.mir_graph.node_count()).into()
    }

    pub(super) fn compound_query_to_mir(
        &mut self,
        query_name: &Relation,
        subquery_leaves: Vec<NodeIndex>,
        op: CompoundSelectOperator,
        order: &Option<OrderClause>,
        limit: &Option<Literal>,
        offset: &Option<Literal>,
        has_leaf: bool,
    ) -> ReadySetResult<NodeIndex> {
        let name = if !has_leaf && limit.is_none() {
            query_name.clone()
        } else {
            format!("{}_union", query_name).into()
        };
        let mut final_node = match op {
            CompoundSelectOperator::Union => self.make_union_node(
                query_name,
                name,
                subquery_leaves.as_slice(),
                union::DuplicateMode::UnionAll,
            )?,
            _ => internal!(),
        };

        if let Some((limit, offset)) = extract_limit_offset(limit, offset)? {
            let make_topk = offset.is_none();
            let paginate_name = if has_leaf {
                if make_topk {
                    format!("{}_topk", query_name)
                } else {
                    format!("{}_paginate", query_name)
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
                    paginate_name.to_string().into(),
                    final_node,
                    group_by,
                    &order
                        .as_ref()
                        .map(|o| {
                            o.order_by
                                .iter()
                                .map(|(e, ot)| {
                                    Ok((
                                        match e {
                                            FieldReference::Numeric(_) => internal!(
                                                "Numeric field references should have been removed"
                                            ),
                                            FieldReference::Expr(e) => e.clone(),
                                        },
                                        ot.unwrap_or(OrderType::OrderAscending),
                                    ))
                                })
                                .collect::<ReadySetResult<_>>()
                        })
                        .transpose()?,
                    limit,
                    make_topk,
                )?
                .last()
                .unwrap();
            final_node = paginate_node;
        }

        let mut alias_table_node = MirNode::new(
            if has_leaf {
                format!("{}_alias_table", query_name).into()
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
        let leaf_node = if has_leaf {
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
        self.relations.insert(query_name.clone(), leaf_node);

        Ok(leaf_node)
    }

    // pub(super) viz for tests
    pub(super) fn get_flow_node_address(&self, name: &Relation) -> Option<NodeIndex> {
        match self.relations.get(name) {
            None => None,
            Some(node) => self.mir_graph[*node]
                .flow_node
                .as_ref()
                .map(|flow_node| flow_node.address()),
        }
    }

    pub(super) fn get_leaf(&self, name: &Relation) -> Option<NodeIndex> {
        self.get_flow_node_address(name)
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

    pub(super) fn remove_query(&mut self, name: &Relation) -> ReadySetResult<NodeIndex> {
        let leaf_mn =
            self.relations
                .remove(name)
                .ok_or_else(|| ReadySetError::RelationNotFound {
                    relation: name.to_string(),
                })?;

        // The only moment when MIR nodes might not have a flow node present,
        // is during query creation. By now, it should have one
        #[allow(clippy::unwrap_used)]
        let dataflow_node = self.mir_graph[leaf_mn].flow_node.unwrap().address();

        let roots: Vec<NodeIndex> = self
            .mir_graph
            .node_indices()
            .filter(|&n| {
                self.mir_graph
                    .neighbors_directed(n, Direction::Incoming)
                    .next()
                    .is_none()
            })
            .filter(|&n| self.mir_graph[n].is_owned_by(name))
            .collect();

        self.remove_owners_below(roots.as_slice(), self.mir_graph[leaf_mn].owners().clone())?;
        Ok(dataflow_node)
    }

    /// Removes a base table, along with all the views/cached queries associated with it.
    pub(super) fn remove_base(&mut self, name: &Relation) -> ReadySetResult<NodeIndex> {
        debug!(%name, "Removing base node");
        let root = self
            .relations
            .remove(name)
            .ok_or_else(|| ReadySetError::RelationNotFound {
                relation: name.to_string(),
            })?;

        // The only moment when MIR nodes might not have a flow node present,
        // is during query creation. By now, it should have one
        #[allow(clippy::unwrap_used)]
        let dataflow_node = self.mir_graph[root].flow_node.unwrap().address();
        self.remove_owners_below(&[root], self.mir_graph[root].owners().clone())?;
        Ok(dataflow_node)
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

    /// Removes the given owners from all nodes below the ones provided.
    fn remove_owners_below(
        &mut self,
        roots: &[NodeIndex],
        owners_to_remove: HashSet<Relation>,
    ) -> ReadySetResult<()> {
        for &node in roots {
            let mut dfs = petgraph::visit::DfsPostOrder::new(&*self.mir_graph, node);
            while let Some(node_idx) = dfs.next(&*self.mir_graph) {
                self.mir_graph[node_idx].retain_owners(|n| !owners_to_remove.contains(n));
                if self.mir_graph[node_idx].owners().is_empty()
                    && !self.mir_graph[node_idx].is_base()
                {
                    let num_children = self
                        .mir_graph
                        .neighbors_directed(node_idx, Direction::Outgoing)
                        .count();
                    invariant_eq!(
                        num_children,
                        0,
                        "tried to remove MIR node '{:?}' that still has {} children",
                        node_idx,
                        num_children
                    );
                    self.mir_graph.remove_node(node_idx);
                }
            }
        }
        Ok(())
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
                            %table_name,
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
                        invalid_err!("a base table already exists with a different schema");
                    }
                }
                _ => internal!(
                    "a MIR node already exists with the same name: {}",
                    table_name
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
        trace!(%name, ?columns, "Added union node");
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
        trace!(%name, %conditions, "Added filter node");
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
        use nom_sql::FunctionExpr::*;

        macro_rules! mk_error {
            ($expression:expr) => {
                internal_err!(
                    "projected_exprs does not contain {:?}",
                    Sensitive($expression)
                )
            };
        }

        let mut out_nodes = Vec::new();

        let mknode = |over: Column, t: GroupedNodeType, distinct: bool| {
            if distinct {
                let new_name = format!("{}_d{}", name, out_nodes.len()).into();
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

        Ok(match function {
            Sum {
                expr: box Expr::Column(col),
                distinct,
            } => mknode(
                Column::from(col),
                GroupedNodeType::Aggregation(Aggregation::Sum),
                distinct,
            ),
            Sum { expr, distinct } => mknode(
                // TODO(celine): replace with ParentRef
                Column::named(
                    projected_exprs
                        .get(&expr)
                        .cloned()
                        .ok_or_else(|| mk_error!(&*expr))?,
                ),
                GroupedNodeType::Aggregation(Aggregation::Sum),
                distinct,
            ),
            CountStar => {
                internal!("COUNT(*) should have been rewritten earlier!")
            }
            Count {
                expr: box Expr::Column(col),
                distinct,
            } => mknode(
                Column::from(col),
                GroupedNodeType::Aggregation(Aggregation::Count),
                distinct,
            ),
            Count { ref expr, distinct } => mknode(
                // TODO(celine): replace with ParentRef
                Column::named(
                    projected_exprs
                        .get(expr)
                        .cloned()
                        .ok_or_else(|| mk_error!(expr))?,
                ),
                GroupedNodeType::Aggregation(Aggregation::Count),
                distinct,
            ),
            Avg {
                expr: box Expr::Column(col),
                distinct,
            } => mknode(
                Column::from(col),
                GroupedNodeType::Aggregation(Aggregation::Avg),
                distinct,
            ),
            Avg { ref expr, distinct } => mknode(
                // TODO(celine): replace with ParentRef
                Column::named(
                    projected_exprs
                        .get(expr)
                        .cloned()
                        .ok_or_else(|| mk_error!(expr))?,
                ),
                GroupedNodeType::Aggregation(Aggregation::Avg),
                distinct,
            ),
            // TODO(atsakiris): Support Filters for Extremum/GroupConcat
            // CH: https://app.clubhouse.io/readysettech/story/198
            Max(box Expr::Column(col)) => mknode(
                Column::from(col),
                GroupedNodeType::Extremum(Extremum::Max),
                false,
            ),
            Max(ref expr) => mknode(
                // TODO(celine): replace with ParentRef
                Column::named(
                    projected_exprs
                        .get(expr)
                        .cloned()
                        .ok_or_else(|| mk_error!(expr))?,
                ),
                GroupedNodeType::Extremum(Extremum::Max),
                false,
            ),
            Min(box Expr::Column(col)) => mknode(
                Column::from(col),
                GroupedNodeType::Extremum(Extremum::Min),
                false,
            ),
            Min(ref expr) => mknode(
                // TODO(celine): replace with ParentRef
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
                expr: box Expr::Column(col),
                separator,
            } => mknode(
                Column::from(col),
                GroupedNodeType::Aggregation(Aggregation::GroupConcat { separator }),
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
        // actually needs; at a minimum, we could start with just the join colums, relying on the
        // automatic column pull-down to retrieve the remaining columns required.
        let projected_cols_left = self.mir_graph.columns(left_node);
        let projected_cols_right = self.mir_graph.columns(right_node);
        let mut project = projected_cols_left
            .into_iter()
            .chain(projected_cols_right.into_iter())
            .collect::<Vec<Column>>();

        // join columns need us to generate join group configs for the operator
        let mut on = Vec::new();

        for jp in join_predicates {
            let mut l_col = match jp.left {
                Expr::Column(ref f) => Column::from(f),
                _ => unsupported!("no multi-level joins yet"),
            };
            let r_col = match jp.right {
                Expr::Column(ref f) => Column::from(f),
                _ => unsupported!("no multi-level joins yet"),
            };

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
            JoinKind::Dependent => MirNodeInner::DependentJoin { on, project },
        };
        trace!(?inner, "Added join node");
        Ok(self.add_query_node(
            query_name.clone(),
            MirNode::new(name, inner),
            &[left_node, right_node],
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

    fn make_projection_helper(
        &mut self,
        query_name: &Relation,
        name: Relation,
        parent: NodeIndex,
        fn_cols: Vec<Column>,
    ) -> NodeIndex {
        self.make_project_node(
            query_name,
            name,
            parent,
            fn_cols,
            vec![],
            vec![("grp".into(), DfValue::from(0i32))],
        )
    }

    fn make_project_node(
        &mut self,
        query_name: &Relation,
        name: Relation,
        parent_node: NodeIndex,
        emit: Vec<Column>,
        expressions: Vec<(SqlIdentifier, Expr)>,
        literals: Vec<(SqlIdentifier, DfValue)>,
    ) -> NodeIndex {
        self.add_query_node(
            query_name.clone(),
            MirNode::new(
                name,
                MirNodeInner::Project {
                    emit,
                    literals,
                    expressions,
                },
            ),
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
        order: &Option<Vec<(Expr, OrderType)>>,
        limit: usize,
        is_topk: bool,
    ) -> ReadySetResult<Vec<NodeIndex>> {
        if !self.config.allow_topk && is_topk {
            unsupported!("TopK is not supported");
        } else if !self.config.allow_paginate && !is_topk {
            unsupported!("Paginate is not supported");
        }

        // Gather a list of expressions we need to evaluate before the paginate node
        let mut exprs_to_project = vec![];
        let order = order.as_ref().map(|oc| {
            oc.iter()
                .map(|(expr, ot)| {
                    (
                        match expr {
                            Expr::Column(col) => Column::from(col),
                            expr => {
                                let col = Column::named(expr.to_string());
                                if self
                                    .mir_graph
                                    .column_id_for_column(parent, &col)
                                    .err()
                                    .iter()
                                    .any(|err| {
                                        matches!(err, ReadySetError::NonExistentColumn { .. })
                                    })
                                {
                                    // Only project the expression if we haven't already
                                    exprs_to_project.push(expr.clone());
                                }
                                col
                            }
                        },
                        *ot,
                    )
                })
                .collect()
        });

        let mut nodes = vec![];

        // If we're ordering on non-column expressions, add an extra node to project those first
        if !exprs_to_project.is_empty() {
            let parent_columns = self.mir_graph.columns(parent);
            let project_node = self.make_project_node(
                query_name,
                format!("{}_proj", name).into(),
                parent,
                parent_columns,
                exprs_to_project
                    .into_iter()
                    .map(|expr| (expr.to_string().into(), expr))
                    .collect(),
                vec![],
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
                    format!("{}_un{}", name, self.mir_graph.node_count()).into(),
                    vec![left_subquery_leaf, right_subquery_leaf],
                    output_cols,
                    // the filters might overlap, so we need to set BagUnion mode which
                    // removes rows in one side that exist in the other
                    union::DuplicateMode::BagUnion,
                )?
            }
            Expr::UnaryOp {
                op: UnaryOperator::Not | UnaryOperator::Neg,
                ..
            } => internal!("negation should have been removed earlier"),
            Expr::Literal(_) | Expr::Column(_) => self.make_filter_node(
                query_name,
                format!("{}_f{}", name, self.mir_graph.node_count()).into(),
                parent,
                Expr::BinaryOp {
                    lhs: Box::new(ce.clone()),
                    op: BinaryOperator::NotEqual,
                    rhs: Box::new(Expr::Literal(Literal::Integer(0))),
                },
            ),
            Expr::Between { .. } => internal!("BETWEEN should have been removed earlier"),
            Expr::Exists(subquery) => {
                let qg = to_query_graph(subquery)?;
                let subquery_leaf = self.named_query_to_mir(
                    query_name,
                    (**subquery).clone(),
                    &qg,
                    HashMap::new(),
                    false,
                )?;

                // -> π[lit: 0, lit: 0]
                let group_proj = self.make_project_node(
                    query_name,
                    format!("{}_prj_hlpr", name).into(),
                    subquery_leaf,
                    vec![],
                    vec![],
                    vec![
                        ("__count_val".into(), DfValue::from(0u32)),
                        ("__count_grp".into(), DfValue::from(0u32)),
                    ],
                );
                // -> [0, 0] for each row

                // -> |0| γ[1]
                let exists_count_col = Column::named("__exists_count");
                let exists_count_node = self.make_grouped_node(
                    query_name,
                    format!("{}_count", name).into(),
                    exists_count_col,
                    (group_proj, Column::named("__count_val")),
                    vec![Column::named("__count_grp")],
                    GroupedNodeType::Aggregation(Aggregation::Count),
                );
                // -> [0, <count>] for each row

                // -> σ[c1 > 0]
                let gt_0_filter = self.make_filter_node(
                    query_name,
                    format!("{}_count_gt_0", name).into(),
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
                    format!("{}_join_key", name).into(),
                    parent,
                    parent_columns,
                    vec![],
                    vec![("__exists_join_key".into(), DfValue::from(0u32))],
                );

                // -> ⋈ on: l.__exists_join_key ≡ r.__count_grp
                self.make_join_node(
                    query_name,
                    format!("{}_join", name).into(),
                    &[JoinPredicate {
                        left: Expr::Column("__exists_join_key".into()),
                        right: Expr::Column("__count_grp".into()),
                    }],
                    left_literal_join_key_proj,
                    gt_0_filter,
                    if is_correlated(subquery) {
                        JoinKind::Dependent
                    } else {
                        JoinKind::Inner
                    },
                )?
            }
            Expr::Call(_) => {
                internal!("Function calls should have been handled by projection earlier")
            }
            Expr::NestedSelect(_) => unsupported!("Nested selects not supported in filters"),
            _ => self.make_filter_node(
                query_name,
                format!("{}_f{}", name, self.mir_graph.node_count()).into(),
                parent,
                ce.clone(),
            ),
        };

        Ok(leaf)
    }

    fn predicates_above_group_by<'a>(
        &mut self,
        query_name: &Relation,
        name: Relation,
        column_to_predicates: &HashMap<nom_sql::Column, Vec<&'a Expr>>,
        over_col: &nom_sql::Column,
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
                    format!("{}_mp{}", name, self.mir_graph.node_count()).into(),
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

        Ok(if !arith_and_lit_columns_needed.is_empty() {
            let projected_expressions: Vec<(SqlIdentifier, Expr)> = arith_and_lit_columns_needed
                .iter()
                .filter_map(|&(_, ref oc)| match oc {
                    OutputColumn::Expr(ref ac) => Some((ac.name.clone(), ac.expression.clone())),
                    OutputColumn::Data { .. } => None,
                    OutputColumn::Literal(_) => None,
                })
                .collect();
            let projected_literals: Vec<(SqlIdentifier, DfValue)> = arith_and_lit_columns_needed
                .iter()
                .map(|&(_, ref oc)| -> ReadySetResult<_> {
                    match oc {
                        OutputColumn::Expr(_) => Ok(None),
                        OutputColumn::Data { .. } => Ok(None),
                        OutputColumn::Literal(ref lc) => {
                            Ok(Some((lc.name.clone(), DfValue::try_from(&lc.value)?)))
                        }
                    }
                })
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .flatten()
                .collect();

            let passthru_cols: Vec<_> = self.mir_graph.columns(prev_node);
            let projected = self.make_project_node(
                query_name,
                format!(
                    "q_{:x}_n{}",
                    qg.signature().hash,
                    self.mir_graph.node_count()
                )
                .into(),
                prev_node,
                passthru_cols,
                projected_expressions,
                projected_literals,
            );

            Some(projected)
        } else {
            None
        })
    }

    /// Adds all the MIR nodes corresponding to the given query,
    /// and returns the index of its leaf node.
    #[allow(clippy::cognitive_complexity)]
    pub(super) fn named_query_to_mir(
        &mut self,
        query_name: &Relation,
        st: SelectStatement,
        qg: &QueryGraph,
        anon_queries: HashMap<Relation, NodeIndex>,
        has_leaf: bool,
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
            let view_key = qg.view_key(self.config())?;

            // 0. Base nodes (always reused)
            let mut base_nodes: Vec<NodeIndex> = Vec::new();
            let mut sorted_rels: Vec<&Relation> = qg.relations.keys().collect();
            sorted_rels.sort_unstable();
            for rel in &sorted_rels {
                let base_for_rel = if let Some((subgraph, subquery)) = &qg.relations[*rel].subgraph
                {
                    let subquery_leaf = self.named_query_to_mir(
                        query_name,
                        subquery.clone(),
                        subgraph,
                        HashMap::new(),
                        false,
                    )?;
                    if is_correlated(subquery) {
                        correlated_relations.insert(subquery_leaf);
                    }
                    subquery_leaf
                } else {
                    match self.get_relation(rel) {
                        Some(node_idx) => node_idx,
                        None => anon_queries.get(rel).copied().ok_or_else(|| {
                            ReadySetError::TableNotFound {
                                name: rel.to_string(),
                                schema: None,
                            }
                        })?,
                    }
                };

                self.mir_graph[base_for_rel].add_owner(query_name.clone());

                let alias_table_node_name = format!(
                    "q_{:x}_{}_alias_table_{}",
                    qg.signature().hash,
                    self.mir_graph[base_for_rel].name(),
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
                format!("q_{:x}", qg.signature().hash).into(),
                qg,
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
                            &format!("q_{:x}", qg.signature().hash),
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
                &format!("q_{:x}", qg.signature().hash),
                qg,
                &mut prev_node,
            );

            // 3. Get columns used by each predicate. This will be used to check
            // if we need to reorder predicates before group_by nodes.
            let mut column_to_predicates: HashMap<nom_sql::Column, Vec<&Expr>> = HashMap::new();

            for rel in &sorted_rels {
                let qgn = qg
                    .relations
                    .get(*rel)
                    .ok_or_else(|| internal_err!("couldn't find {:?} in qg relations", rel))?;
                for pred in qgn.predicates.iter().chain(&qg.global_predicates) {
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
                format!("q_{:x}", qg.signature().hash).into(),
                qg,
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
                let qgn = qg
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
                                qg.signature().hash,
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

            // 6. Determine literals and expressions that global predicates depend
            //    on and add them here; remembering that we've already added them-
            if let Some(projected) = self.make_value_project_node(query_name, qg, prev_node)? {
                prev_node = projected;
            }

            // 7. Global predicates
            for (i, ref p) in qg.global_predicates.iter().enumerate() {
                if created_predicates.contains(p) {
                    continue;
                }

                let subquery_leaf = self.make_predicate_nodes(
                    query_name,
                    format!(
                        "q_{:x}_n{}_{}",
                        qg.signature().hash,
                        self.mir_graph.node_count(),
                        i
                    )
                    .into(),
                    prev_node,
                    p,
                )?;

                prev_node = subquery_leaf;
            }

            // 8. Add function and grouped nodes
            let mut func_nodes: Vec<NodeIndex> = make_grouped(
                self,
                query_name,
                format!("q_{:x}", qg.signature().hash).into(),
                qg,
                &node_for_rel,
                &mut prev_node,
                &expressions_above_grouped,
            )?;

            // 9. Add predicate nodes for HAVING after GROUP BY nodes
            for (i, p) in qg.having_predicates.iter().enumerate() {
                let hp_name = format!(
                    "q_{:x}_h{}_{}",
                    qg.signature().hash,
                    self.mir_graph.node_count(),
                    i
                )
                .into();
                let subquery_leaf = self.make_predicate_nodes(query_name, hp_name, prev_node, p)?;

                prev_node = subquery_leaf;
            }

            // 10. Get the final node
            let mut final_node = prev_node;

            // Indicates whether the final project should include a bogokey. This is required when
            // we have a TopK node that groups on a bogokey. However, it is not required for
            // Paginate nodes that group on a bogokey, as they will project a page number field
            let mut bogo_in_final_projection = false;
            let mut create_paginate = false;
            if let Some(Pagination {
                order,
                limit,
                offset,
            }) = qg.pagination.as_ref()
            {
                let make_topk = offset.is_none();
                let group_by = if qg.parameters().is_empty() {
                    // need to add another projection to introduce a bogokey to group by if there
                    // are no query parameters
                    let cols: Vec<_> = self.mir_graph.columns(final_node);
                    let name = format!(
                        "q_{:x}_n{}",
                        qg.signature().hash,
                        self.mir_graph.node_count()
                    )
                    .into();
                    let bogo_project = self.make_project_node(
                        query_name,
                        name,
                        final_node,
                        cols,
                        vec![],
                        vec![("bogokey".into(), DfValue::from(0i32))],
                    );
                    final_node = bogo_project;
                    // Indicates whether we need a bogokey at the leaf node. This is the case for
                    // topk nodes that group by a bogokey. However, this is not the case for
                    // paginate nodes as they will project a page number
                    bogo_in_final_projection = make_topk;
                    create_paginate = !make_topk;
                    vec![Column::named("bogokey")]
                } else {
                    // view key will have the offset parameter if it exists. We must filter it out
                    // of the group by, because the column originates at this node
                    view_key
                        .columns
                        .iter()
                        .filter_map(|(col, _)| {
                            if col.name != *PAGE_NUMBER_COL {
                                Some(col.clone())
                            } else {
                                None
                            }
                        })
                        .collect()
                };

                // Order by expression projections and either a topk or paginate node
                let paginate_nodes = self.make_paginate_node(
                    query_name,
                    format!(
                        "q_{:x}_n{}",
                        qg.signature().hash,
                        self.mir_graph.node_count()
                    )
                    .into(),
                    final_node,
                    group_by,
                    order,
                    *limit,
                    make_topk,
                )?;
                func_nodes.extend(paginate_nodes.clone());
                final_node = *paginate_nodes.last().unwrap();
            }

            // 10. Generate leaf views that expose the query result
            let mut projected_columns: Vec<Column> = qg
                .columns
                .iter()
                .filter_map(|oc| match *oc {
                    OutputColumn::Expr(_) => None,
                    OutputColumn::Data {
                        ref column,
                        alias: ref name,
                    } => Some(Column::from(column).aliased_as(name.clone())),
                    OutputColumn::Literal(_) => None,
                })
                .collect();

            // We may already have added some of the expression and literal columns
            let (_, already_computed): (Vec<_>, Vec<_>) =
                value_columns_needed_for_predicates(&qg.columns, &qg.global_predicates)
                    .into_iter()
                    .unzip();
            let projected_expressions: Vec<(SqlIdentifier, Expr)> = qg
                .columns
                .iter()
                .filter_map(|oc| match *oc {
                    OutputColumn::Expr(ref ac) => {
                        if !already_computed.contains(oc) {
                            Some((ac.name.clone(), ac.expression.clone()))
                        } else {
                            projected_columns.push(Column::named(&ac.name));
                            None
                        }
                    }
                    OutputColumn::Data { .. } => None,
                    OutputColumn::Literal(_) => None,
                })
                .collect();
            let mut projected_literals: Vec<(SqlIdentifier, DfValue)> = qg
                .columns
                .iter()
                .map(|oc| -> ReadySetResult<_> {
                    match *oc {
                        OutputColumn::Expr(_) => Ok(None),
                        OutputColumn::Data { .. } => Ok(None),
                        OutputColumn::Literal(ref lc) => {
                            if !already_computed.contains(oc) {
                                Ok(Some((lc.name.clone(), DfValue::try_from(&lc.value)?)))
                            } else {
                                projected_columns.push(Column::named(&lc.name));
                                Ok(None)
                            }
                        }
                    }
                })
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .flatten()
                .collect();

            // Bogokey will not be added to post-paginate project nodes
            if bogo_in_final_projection {
                projected_columns.push(Column::named("bogokey"));
            }

            if has_leaf {
                if qg.parameters().is_empty()
                    && !create_paginate
                    && !projected_columns.contains(&Column::named("bogokey"))
                {
                    projected_literals.push(("bogokey".into(), DfValue::from(0i32)));
                } else {
                    for (column, _) in &view_key.columns {
                        if !projected_columns.contains(column) {
                            projected_columns.push(column.clone())
                        }
                    }
                }
            }

            if st.distinct {
                let name = if has_leaf {
                    format!(
                        "q_{:x}_n{}",
                        qg.signature().hash,
                        self.mir_graph.node_count()
                    )
                    .into()
                } else {
                    format!("{}_d{}", query_name, self.mir_graph.node_count()).into()
                };
                let distinct_node = self.make_distinct_node(
                    query_name,
                    name,
                    final_node,
                    projected_columns.clone(),
                );
                final_node = distinct_node;
            }

            let leaf_project_node = self.make_project_node(
                query_name,
                if has_leaf {
                    format!("q_{:x}_project", qg.signature().hash).into()
                } else {
                    query_name.clone()
                },
                final_node,
                projected_columns,
                projected_expressions,
                projected_literals,
            );

            if has_leaf {
                // We are supposed to add a `Leaf` node keyed on the query parameters. For purely
                // internal views (e.g., subqueries), this is not set.
                let mut returned_cols = st
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
                                Ok(Column::named(expr.to_string()))
                            }
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                returned_cols.retain(|e| e.name != "bogokey");

                // After we have all of our returned columns figured out, find out how they are
                // projected by this projection, so we can then add another projection that returns
                // the columns in the correct order
                let mut project_order = Vec::with_capacity(returned_cols.len());
                let parent_columns = self.mir_graph.columns(leaf_project_node);
                for col in returned_cols.iter() {
                    if let Some(c) = parent_columns.iter().find(|c| col.cmp(c).is_eq()) {
                        project_order.push(c.clone());
                    } else {
                        internal!("Returned column not in projected schema");
                    }
                }
                for col in parent_columns {
                    if !project_order.contains(&col) {
                        project_order.push(col);
                    }
                }

                // Add another project node that will return the required columns in the right order
                let leaf_project_reorder_node = self.make_project_node(
                    query_name,
                    if has_leaf {
                        format!("q_{:x}_project_reorder", qg.signature().hash).into()
                    } else {
                        query_name.clone()
                    },
                    leaf_project_node,
                    project_order,
                    vec![],
                    vec![],
                );

                let aggregates = if view_key.index_type != IndexType::HashMap {
                    post_lookup_aggregates(qg, &st, query_name)?
                } else {
                    None
                };

                let leaf_node = self.add_query_node(query_name.clone(), MirNode::new(
                    query_name.clone(),
                    MirNodeInner::Leaf {
                        keys: view_key
                            .columns
                            .into_iter()
                            .map(|(col, placeholder)| (col, placeholder))
                            .collect(),
                        index_type: view_key.index_type,
                        order_by: st
                            .order
                            .as_ref()
                            .map(|order| {
                                order
                                    .order_by
                                    .iter()
                                    .cloned()
                                    .map(|(expr, ot)| {
                                        Ok((
                                            match expr {
                                                FieldReference::Expr(Expr::Column(
                                                                         col,
                                                                     )) => Column::from(col),
                                                FieldReference::Expr(expr) => {
                                                    Column::named(expr.to_string())
                                                }
                                                FieldReference::Numeric(_) => internal!(
                                                    "Numeric field references should have been removed"
                                                ),
                                            },
                                            ot.unwrap_or(OrderType::OrderAscending),
                                        ))
                                    })
                                    .collect::<ReadySetResult<_>>()
                            })
                            .transpose()?,
                        limit: qg.pagination.as_ref().map(|p| p.limit),
                        returned_cols: Some(returned_cols),
                        default_row: default_row_for_select(&st),
                        aggregates,
                    },
                ), &[leaf_project_reorder_node]);

                self.relations.insert(query_name.clone(), leaf_node);

                leaf_node
            } else {
                leaf_project_node
            }
        };
        debug!(%query_name, "Added final MIR node for query");

        // finally, we output all the nodes we generated
        Ok(leaf)
    }

    /// Upgrades the schema version of the MIR nodes.
    pub(super) fn upgrade_version(&mut self) {
        self.schema_version += 1;
    }
}
