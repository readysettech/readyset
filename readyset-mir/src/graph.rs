//! The MIR Graph module
//!
//! This module provides the structures to store the MIR graph state, primarily through the
//! [`MirGraph`] struct.
//! The graph representation allow us to centralize all the different MIR query graphs into
//! a single graph, with each query now being a subgraph of it.
use std::iter;
use std::ops::{Deref, DerefMut, Index, IndexMut};

use itertools::Itertools;
use nom_sql::analysis::ReferredColumns;
use petgraph::stable_graph::StableGraph;
use petgraph::visit::{Bfs, EdgeRef, Reversed};
use petgraph::{Directed, Direction};
use readyset_errors::{internal_err, ReadySetError, ReadySetResult};
use serde::{Deserialize, Serialize};

use crate::node::node_inner::ProjectExpr;
use crate::node::{MirNode, MirNodeInner};
use crate::{Column as MirColumn, DfNodeIndex, Ix, NodeIndex, PAGE_NUMBER_COL};

type Graph = StableGraph<MirNode, usize, Directed, Ix>;

/// The graph to store all the MIR query graphs.
/// The nodes in this graph are [`MirNode`]s, and the edges are represented as a
/// `usize` to keep an index (starting with 0 as default) on the ancestors of a given node,
/// which is mostly used in cases where a node has multiple ancestors, as is the case for JOINs
/// and UNIONs.
///
/// note [edge-ordering]
/// When traversing the graph to look for a node's ancestors, it's important that those nodes are
/// sorted according to the weight of the edges connecting them.
/// This is necessary since we resolve the columns from the parents
/// according to the order of the join (left to right), when we convert the
/// nodes from MIR to Dataflow later on.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MirGraph {
    /// The graph that stores the MIR queries.
    graph: Graph,
}

impl MirGraph {
    /// Creates a new, empty graph.
    pub fn new() -> Self {
        MirGraph {
            graph: StableGraph::default(),
        }
    }

    fn ensure_node_exists(&self, node: NodeIndex) -> ReadySetResult<()> {
        if !self.graph.contains_node(node) {
            Err(ReadySetError::MirNodeNotFound {
                index: node.index(),
            })
        } else {
            Ok(())
        }
    }

    /// Swaps a parent node with its child node.
    /// The parent node must have only one child and only one ancestor.
    pub fn swap_with_child(&mut self, parent: NodeIndex) -> ReadySetResult<()> {
        self.ensure_node_exists(parent)?;

        // If the invariants are held, we have this situation:
        // grandparent -n-> parent -m-> child -> [ -0-> grandchild_1, ..., -n-1-> grandchild_n]
        //
        // And we want this:
        // grandparent -n-> child -m-> parent -> [ -0-> grandchild_1, ..., -n-1-> grandchild_n]
        let child = self
            .graph
            .neighbors_directed(parent, Direction::Outgoing)
            .exactly_one()
            .map_err(|_| {
                internal_err!("can't call swap with a parent that has more than one child.")
            })?;
        // Remove the edge between parent and child
        let parent_child_edge = self
            .graph
            .find_edge(parent, child)
            .ok_or_else(|| internal_err!("There is no edge between parent and child"))?;
        let parent_child_weight = *self.graph.edge_weight(parent_child_edge).unwrap();
        self.graph.remove_edge(parent_child_edge);
        // grandparent -0-> parent, child -> [ -0-> grandchild_1, ..., -n-1-> grandchild_n]
        let grandparent_parent_edge = self
            .graph
            .edges_directed(parent, Direction::Incoming)
            .exactly_one()
            .map_err(|_| {
                internal_err!("can't call swap with a parent that has more than one ancestor.")
            })?;
        let grandparent_parent_weight = *grandparent_parent_edge.weight();
        let grandparent = grandparent_parent_edge.source();
        self.graph.remove_edge(grandparent_parent_edge.id());
        // grandparent, parent, child -> [ -0-> grandchild_1, ..., -n-1-> grandchild_n]
        self.graph.add_edge(grandparent, child, parent_child_weight);
        // grandparent -n-> child -> [ -0-> grandchild_1, ..., -n-1-> grandchild_n], parent
        for (grandchild, grandchild_child_edge, grandchild_child_edge_val) in self
            .graph
            .edges_directed(child, Direction::Outgoing)
            .map(|e| (e.target(), e.id(), *e.weight()))
            .collect::<Vec<_>>()
            .into_iter()
        {
            self.graph
                .add_edge(parent, grandchild, grandchild_child_edge_val);
            // In the i-th iteration:
            // grandparent -n-> child -> [ -i-1-> grandchild_i, ..., -n-1-> grandchild_n]
            // parent child -> [ -0-> grandchild_i, ..., -i-2-> grandchild_i-1]
            self.graph.remove_edge(grandchild_child_edge);
            // grandparent -n-> child -> [ -i-> grandchild_i+1, ..., -n-1-> grandchild_n]
            // parent -> [ -0-> grandchild_1, ..., -i-1-> grandchild_i]
        }
        self.graph
            .add_edge(child, parent, grandparent_parent_weight);
        // grandparent -n-> child -m-> parent -> [ -0-> grandchild_1, ..., -n-1-> grandchild_n]
        Ok(())
    }

    /// Insert a new node above the given child node index.
    ///
    /// The given node must have only one ancestor.
    pub fn insert_above(&mut self, child: NodeIndex, node: MirNode) -> ReadySetResult<NodeIndex> {
        self.ensure_node_exists(child)?;

        let parent = self
            .graph
            .neighbors_directed(child, Direction::Incoming)
            .exactly_one()
            .map_err(|_| {
                internal_err!("can't call insert_above with a child that has more than one parent")
            })?;
        let parent_child_edge = self
            .graph
            .find_edge(parent, child)
            .ok_or_else(|| internal_err!("There is no edge between parent and child"))?;
        self.graph.remove_edge(parent_child_edge);

        let node_idx = self.graph.add_node(node);
        self.graph.add_edge(parent, node_idx, 0);
        self.graph.add_edge(node_idx, child, 0);

        Ok(node_idx)
    }

    /// Computes the list of columns *referenced* by this node, ie the columns this node requires
    /// from its parent.
    pub fn referenced_columns(&self, node: NodeIndex) -> Vec<MirColumn> {
        match &self.graph[node].inner {
            MirNodeInner::Aggregation { on, group_by, .. }
            | MirNodeInner::Extremum { on, group_by, .. } => {
                // Aggregates need the group_by columns and the "over" column
                let mut columns = group_by.clone();
                if !columns.contains(on) {
                    columns.push(on.clone());
                }
                columns
            }
            MirNodeInner::Distinct { group_by } => group_by.clone(),
            MirNodeInner::Project { emit } => {
                let mut columns = vec![];
                for expr in emit {
                    match expr {
                        ProjectExpr::Column(c) => {
                            if !columns.contains(c) {
                                columns.push(c.clone());
                            }
                        }
                        ProjectExpr::Expr { expr, .. } => {
                            for c in expr.referred_columns() {
                                if !columns.iter().any(|col| col == c) {
                                    columns.push(c.clone().into());
                                }
                            }
                        }
                    }
                }
                columns
            }
            MirNodeInner::Leaf {
                keys,
                order_by,
                returned_cols,
                aggregates,
                ..
            } => {
                let mut columns = self.columns(node);
                columns.extend(
                    keys.iter()
                        .map(|(c, _)| c.clone())
                        .chain(order_by.iter().flatten().map(|(c, _)| c.clone()))
                        .chain(returned_cols.iter().flatten().cloned())
                        .chain(aggregates.iter().flat_map(|aggs| {
                            aggs.group_by
                                .clone()
                                .into_iter()
                                .chain(aggs.aggregates.iter().map(|agg| agg.column.clone()))
                        })),
                );
                columns
            }
            MirNodeInner::Filter { conditions } => {
                let mut columns = self.columns(node);
                for c in conditions.referred_columns() {
                    if !columns.iter().any(|col| col == c) {
                        columns.push(c.clone().into())
                    }
                }
                columns
            }
            _ => self.columns(node),
        }
    }

    /// Computes the list of columns in the output of this node.
    pub fn columns(&self, node: NodeIndex) -> Vec<MirColumn> {
        let parent_columns = || {
            // see note [edge-ordering]
            let parent = self.sorted_ancestors(node).next().unwrap();
            self.columns(parent)
        };

        match &self.graph[node].inner {
            MirNodeInner::Base { column_specs, .. } => column_specs
                .iter()
                .map(|spec| spec.column.clone().into())
                .collect(),
            MirNodeInner::Filter { .. }
            | MirNodeInner::ViewKey { .. }
            | MirNodeInner::Leaf { .. }
            | MirNodeInner::Identity
            | MirNodeInner::TopK { .. } => parent_columns(),
            MirNodeInner::AliasTable { table } => parent_columns()
                .iter()
                .map(|c| MirColumn {
                    table: Some(table.clone()),
                    name: c.name.clone(),
                    aliases: vec![],
                })
                .collect(),
            MirNodeInner::Aggregation {
                group_by,
                output_column,
                ..
            }
            | MirNodeInner::Extremum {
                group_by,
                output_column,
                ..
            } => group_by
                .iter()
                .cloned()
                .chain(iter::once(output_column.clone()))
                .collect(),
            MirNodeInner::Join { project, .. }
            | MirNodeInner::LeftJoin { project, .. }
            | MirNodeInner::DependentJoin { project, .. } => project.clone(),
            MirNodeInner::JoinAggregates => {
                let cols = self
                    // see note [edge-ordering]
                    .sorted_ancestors(node)
                    .flat_map(|n| self.columns(n))
                    // Crappy quadratic column deduplication, because we don't have Hash or Ord for
                    // Column due to aliases affecting equality
                    .fold(vec![], |mut cols, c| {
                        if !cols.contains(&c) {
                            cols.push(c);
                        }
                        cols
                    });
                cols
            }
            MirNodeInner::Project { emit } => emit
                .iter()
                .map(|expr| match expr {
                    ProjectExpr::Column(col) => col.clone(),
                    ProjectExpr::Expr { alias, .. } => MirColumn::named(alias),
                })
                .collect(),
            MirNodeInner::Union { emit, .. } => emit
                .first()
                .cloned()
                .expect("Union must have at least one set of emit columns"),
            MirNodeInner::Paginate { .. } => parent_columns()
                .into_iter()
                .chain(iter::once(MirColumn::named(&*PAGE_NUMBER_COL)))
                .collect(),
            MirNodeInner::Distinct { group_by } => group_by
                .iter()
                .cloned()
                // Distinct gets lowered to COUNT, so it emits one extra column at the end - but
                // nobody cares about that column, so just give it a throwaway name here
                .chain(iter::once(MirColumn::named("__distinct_count")))
                .collect(),
        }
    }

    /// Returns true if this node can provide the given column, meaning either the node
    /// has the column, or one of its ancestors does, and the column can be added to those ancestors
    /// to be projected by this node
    pub(crate) fn provides_column(&self, node: NodeIndex, column: &MirColumn) -> bool {
        if let MirNodeInner::AliasTable { table } = &self.graph[node].inner {
            // Can't project a column through an alias_table node unless the column has the aliased
            // table in one of its aliases
            if !column.has_table(table) {
                return false;
            }
        }

        self.columns(node).contains(column)
            || self
                .graph
                .neighbors_directed(node, Direction::Incoming)
                .any(|a| self.provides_column(a, column))
    }

    /// Returns the index of the column in the column list for the given node.
    pub fn column_id_for_column(&self, node: NodeIndex, c: &MirColumn) -> ReadySetResult<usize> {
        let err = Err(ReadySetError::NonExistentColumn {
            column: c.to_string(),
            node: self.graph[node].name().display_unquoted().to_string(),
        });
        #[allow(clippy::cmp_owned)]
        match self.graph[node].inner {
            // if we're a base, translate to absolute column ID (taking into account deleted
            // columns). We use the column specifications here, which track a tuple of (column
            // spec, absolute column ID).
            // Note that `rposition` is required because multiple columns of the same name might
            // exist if a column has been removed and re-added. We always use the latest column,
            // and assume that only one column of the same name ever exists at the same time.
            MirNodeInner::Base {
                ref column_specs, ..
            } => match column_specs
                .iter()
                .rposition(|cs| MirColumn::from(&cs.column) == *c)
            {
                None => err,
                Some(idx) => Ok(idx),
            },
            // otherwise, just look up in the column set
            // Compare by name if there is no table
            _ => match {
                if c.table.is_none() {
                    self.columns(node).iter().position(|cc| cc.name == c.name)
                } else {
                    self.columns(node).iter().position(|cc| cc == c)
                }
            } {
                Some(id) => Ok(id),
                None => err,
            },
        }
    }

    /// Finds the source of a child column within the node.
    /// This is currently used for locating the source of a projected column.
    pub fn find_source_for_child_column(
        &self,
        node: NodeIndex,
        child: &MirColumn,
    ) -> Option<usize> {
        // we give the alias preference here because in a query like
        // SELECT table1.column1 AS my_alias
        // my_alias will be the column name and "table1.column1" will be the alias.
        // This is slightly backwards from what intuition suggests when you first look at the
        // column struct but means its the "alias" that will exist in the parent node,
        // not the column name.
        if child.aliases.is_empty() {
            self.columns(node).iter().position(|c| child == c)
        } else {
            let columns = self.columns(node);
            columns
                .iter()
                .position(|c| child.aliases.contains(c))
                .or_else(|| columns.iter().position(|c| child == c))
        }
    }

    /// Add a new column to the set of emitted columns for this node
    pub fn add_column(&mut self, node: NodeIndex, c: MirColumn) -> ReadySetResult<()> {
        if !self.graph[node].inner.add_column(c.clone())? {
            let ancestors = self
                // see note [edge-ordering]
                .sorted_ancestors(node)
                .collect::<Vec<_>>();
            let parent = ancestors.get(0).copied().ok_or_else(|| {
                internal_err!(
                    "MIR node {:?} has the wrong number of parents ({})",
                    self.graph[node].inner,
                    ancestors.len()
                )
            })?;
            self.add_column(parent, c)?;
        }

        Ok(())
    }

    /// Returns the Dataflow node address for the given node.
    /// Returns [`None`] if the query was not converted to Dataflow yet or if the given node does
    /// not belong to the query.
    pub fn resolve_dataflow_node(&self, node_idx: NodeIndex) -> Option<DfNodeIndex> {
        // This check is needed, since `Bfs` panics with a node that does not belong to the graph.
        if !self.graph.contains_node(node_idx) {
            return None;
        }
        let mut bfs = Bfs::new(Reversed(&self.graph), node_idx);
        while let Some(ancestor) = bfs.next(Reversed(&self.graph)) {
            let df_node_address_opt = self.graph[ancestor].df_node_index();
            if df_node_address_opt.is_some() {
                return df_node_address_opt.map(|df| DfNodeIndex(df.address()));
            }
        }
        None
    }

    fn sorted_ancestors(&self, node: NodeIndex) -> impl Iterator<Item = NodeIndex> + '_ {
        self.graph
            .edges_directed(node, Direction::Incoming)
            .sorted_by_key(|e| e.weight())
            .map(|e| e.source())
    }
}

impl Deref for MirGraph {
    type Target = Graph;

    fn deref(&self) -> &Self::Target {
        &self.graph
    }
}

impl DerefMut for MirGraph {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.graph
    }
}

impl Index<NodeIndex> for MirGraph {
    type Output = MirNode;

    fn index(&self, index: NodeIndex) -> &MirNode {
        self.graph.node_weight(index).unwrap()
    }
}

impl IndexMut<NodeIndex> for MirGraph {
    fn index_mut(&mut self, index: NodeIndex) -> &mut MirNode {
        self.graph.node_weight_mut(index).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn swap_with_child_preserves_edge_weights() {
        let mut graph = MirGraph::new();
        let t1 = graph.add_node(MirNode::new(
            "t1".into(),
            MirNodeInner::Base {
                column_specs: vec![],
                primary_key: None,
                unique_keys: Default::default(),
            },
        ));
        let t2 = graph.add_node(MirNode::new(
            "t2".into(),
            MirNodeInner::Base {
                column_specs: vec![],
                primary_key: None,
                unique_keys: Default::default(),
            },
        ));
        let t2_prj = graph.add_node(MirNode::new(
            "t2".into(),
            MirNodeInner::Project { emit: vec![] },
        ));
        graph.add_edge(t2, t2_prj, 0);

        let join = graph.add_node(MirNode::new(
            "join".into(),
            MirNodeInner::Join {
                on: vec![],
                project: vec![],
            },
        ));
        graph.add_edge(t1, join, 0);
        graph.add_edge(t2_prj, join, 1);

        // setup done

        graph.swap_with_child(t2_prj).unwrap();

        let join_t2_prj_edge = graph.find_edge(join, t2_prj).unwrap();
        assert_eq!(*graph.edge_weight(join_t2_prj_edge).unwrap(), 0);

        let t2_join_edge = graph.find_edge(t2, join).unwrap();
        assert_eq!(*graph.edge_weight(t2_join_edge).unwrap(), 1);
    }
}
