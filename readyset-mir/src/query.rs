use std::marker::PhantomData;

use itertools::Itertools;
use nom_sql::{Relation, SqlIdentifier};
use petgraph::graph::NodeIndex;
use petgraph::visit::{Bfs, EdgeRef, Reversed};
use petgraph::Direction;
use readyset_errors::{internal_err, ReadySetError, ReadySetResult};

use crate::graph::MirGraph;
use crate::node::{MirNode, MirNodeInner};
use crate::rewrite::decorrelate::eliminate_dependent_joins;
use crate::rewrite::pull_columns::pull_all_required_columns;

/// MIR representation of a base table
#[derive(Debug)]
pub struct MirBase<'a> {
    pub name: Relation,
    pub mir_node: NodeIndex,
    /// The names of the fields that this query returns, in the original user-specified order.
    ///
    /// These fields should be used when querying from a [`MirQuery`] as a SQL `VIEW`.
    pub fields: Vec<SqlIdentifier>,
    pub graph: &'a mut MirGraph,
}

impl<'a> MirBase<'a> {
    pub fn get_node_mut(&mut self) -> &mut MirNode {
        self.graph.node_weight_mut(self.mir_node).unwrap()
    }
}

/// MIR representation of a cached query or view.
#[derive(Debug)]
pub struct MirQuery<'a> {
    /// The name of the view/cached query
    name: Relation,
    /// The leaf node for the query.
    /// This node will hold the output of the abstract computations performed
    /// on the rows by all its ancestors.
    leaf: NodeIndex,
    /// The MIR graph that holds all of this query nodes and their edges.
    pub graph: &'a mut MirGraph,
}

impl MirQuery<'_> {
    /// Creates a new [`MirQuery`]
    pub fn new(name: Relation, leaf: NodeIndex, graph: &mut MirGraph) -> MirQuery {
        MirQuery { name, leaf, graph }
    }

    pub fn name(&self) -> &Relation {
        &self.name
    }

    pub fn leaf(&self) -> NodeIndex {
        self.leaf
    }

    /// The names of the fields that this query returns, in the original user-specified order.
    /// These fields should be used when querying from a [`MirQuery`] as a SQL `VIEW`.
    pub fn fields(&self) -> Vec<SqlIdentifier> {
        self.graph
            .columns(self.leaf)
            .into_iter()
            .map(|c| c.name)
            .collect()
    }

    pub fn is_root(&self, node: NodeIndex) -> bool {
        self.graph[node].is_owned_by(&self.name)
            && self
                .graph
                .neighbors_directed(node, Direction::Incoming)
                .next()
                .is_none()
    }

    /// Returns a list of all the node indices belonging to this query,
    /// in topographical order.
    pub fn topo_nodes(&self) -> Vec<NodeIndex> {
        let mut topo = petgraph::visit::Topo::new(&**self.graph);
        let mut nodes = Vec::new();
        while let Some(node) = topo.next(&**self.graph) {
            if self.graph[node].is_owned_by(&self.name) {
                nodes.push(node);
            }
        }
        nodes
    }

    /// Returns the list of immediate ancestors for the given node.
    /// If the node does not belong to the query (or doesn't exist), an empty
    /// vector is returned.
    pub(crate) fn ancestors(&self, node_idx: NodeIndex) -> ReadySetResult<Vec<NodeIndex>> {
        if !self.graph.contains_node(node_idx) {
            return Err(ReadySetError::MirNodeNotFound {
                index: node_idx.index(),
            });
        }
        Ok(self
            .graph
            .edges_directed(node_idx, Direction::Incoming)
            .sorted_by_key(|e| e.weight())
            .map(|e| e.source())
            .filter(|&n| self.graph[n].is_owned_by(&self.name))
            .collect())
    }

    /// Returns the list of immediate descendants for the given node.
    /// If the node does not belong to the query (or doesn't exist), an empty
    /// vector is returned.
    pub fn descendants(&self, node_idx: NodeIndex) -> ReadySetResult<Vec<NodeIndex>> {
        if !self.graph.contains_node(node_idx) {
            return Err(ReadySetError::MirNodeNotFound {
                index: node_idx.index(),
            });
        }
        Ok(self
            .graph
            .neighbors_directed(node_idx, Direction::Outgoing)
            .filter(|&n| self.graph[n].is_owned_by(&self.name))
            .collect())
    }

    /// Returns an iterator of the query, starting from the given node, going in the direction
    /// of the ancestors (towards the base table nodes), in topological order.
    pub fn topo_ancestors(
        &self,
        node_idx: NodeIndex,
    ) -> ReadySetResult<impl Iterator<Item = NodeIndex> + '_> {
        Topo::<Ancestors>::new(node_idx, self.graph)
            .map(|iter| iter.filter(|&n| self.graph[n].is_owned_by(&self.name)))
    }

    /// Returns an iterator of the query, starting from the given node, going in the direction
    /// of the descendants (towards the leaf node), in topological order.
    pub fn topo_descendants(
        &self,
        node_idx: NodeIndex,
    ) -> ReadySetResult<impl Iterator<Item = NodeIndex> + '_> {
        Topo::<Descendants>::new(node_idx, self.graph)
            .map(|iter| iter.filter(|&n| self.graph[n].is_owned_by(&self.name)))
    }

    /// Returns the Dataflow node address of the query.
    /// Returns [`None`] if the query was not converted to Dataflow yet.
    pub fn dataflow_node(&self) -> Option<NodeIndex> {
        self.graph[self.leaf].flow_node.map(|n| n.address())
    }

    /// Returns a reference of the [`MirNode`] identified by the given node index.
    /// Returns [`None`] if the node does not belong to the query or doesn't exist.
    pub fn get_node(&self, node_idx: NodeIndex) -> Option<&MirNode> {
        self.graph
            .node_weight(node_idx)
            .filter(|node| node.is_owned_by(&self.name))
    }

    /// Returns a mutable reference of the [`MirNode`] identified by the given node index.
    /// Returns [`None`] if the node does not belong to the query or doesn't exist.
    pub fn get_node_mut(&mut self, node_idx: NodeIndex) -> Option<&mut MirNode> {
        self.graph
            .node_weight_mut(node_idx)
            .filter(|node| node.is_owned_by(&self.name))
    }

    /// Removes the given node from the graph.
    /// The ancestor of the node will be connected to its descendant.
    ///
    /// # Invariants
    /// - The node must have only one ancestor.
    /// - The node must have only one descendant.
    pub fn remove_node(&mut self, node_idx: NodeIndex) -> ReadySetResult<Option<MirNode>> {
        if !self.graph.contains_node(node_idx) {
            return Err(ReadySetError::MirNodeNotFound {
                index: node_idx.index(),
            });
        }
        let ancestor = self
            .graph
            .neighbors_directed(node_idx, Direction::Incoming)
            .exactly_one()
            .map_err(|_| {
                internal_err!(
                    "tried to remove node {} that doesn't have exactly one ancestor",
                    node_idx.index()
                )
            })?;
        let descendant_edge = self
            .graph
            .edges_directed(node_idx, Direction::Outgoing)
            .exactly_one()
            .map_err(|_| {
                internal_err!(
                    "tried to remove node {} that doesn't have exactly one descendant",
                    node_idx.index()
                )
            })?;
        let descendant = descendant_edge.target();
        let edge_weight = *descendant_edge.weight();
        self.graph.add_edge(ancestor, descendant, edge_weight);
        Ok(self.graph.remove_node(node_idx))
    }

    /// Swaps the given node with its children.
    ///
    /// # Invariants
    /// - The node must have only one ancestor.
    /// - The node must have only one descendant.
    ///
    /// # Example
    /// Suppose we have the following graph (where --x--> represents a directed edge with a weight
    /// value of x):
    /// ```ignore
    /// grandparent --0--> parent --0--> child --0--> grandchild_1
    ///                                        ...
    ///                                        --n--> grandchild_n
    /// ```
    /// Calling this function on `parent` will modify the graph to become like this:
    /// ```ignore
    /// grandparent --0--> child --0--> parent --0--> grandchild_1
    ///                                        ...
    ///                                        --n--> grandchild_n
    /// ```
    pub fn swap_with_child(&mut self, node: NodeIndex) -> ReadySetResult<()> {
        self.graph.swap_with_child(node)
    }

    /// Runs the given function on the [`MirNodeInner`] belonging to the given node,
    /// and returns the result of said function.
    /// Returns [`None`] if the node does not belong to the query or doesn't exist.
    pub fn process_inner<F, O>(&self, node: NodeIndex, func: F) -> Option<O>
    where
        F: FnOnce(&MirNodeInner) -> O,
    {
        if let Some(n) = self
            .graph
            .node_weight(node)
            .filter(|n| n.is_owned_by(self.name()))
        {
            return Some(func(&n.inner));
        }
        None
    }

    /// Run a set of rewrite and optimization passes on this [`MirQuery`],
    /// and returns the modified query.
    pub fn rewrite(mut self) -> ReadySetResult<Self> {
        eliminate_dependent_joins(&mut self)?;
        pull_all_required_columns(&mut self)?;
        Ok(self)
    }
}

/// A [`Relationship`] that goes towards the descendants of a node
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Descendants;

/// A [`Relationship`] that goes towards the ancestors of a node
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Ancestors;

mod sealed {
    pub trait Sealed {}
    impl Sealed for super::Descendants {}
    impl Sealed for super::Ancestors {}
}

/// A trait providing an abstract definition of the direction of an edge in a MIR.
///
/// This trait is [sealed][], so cannot be implemented outside of this module
///
/// [sealed]: https://rust-lang.github.io/api-guidelines/future-proofing.html#sealed-traits-protect-against-downstream-implementations-c-sealed
pub trait Relationship: sealed::Sealed {
    /// The relationship going in the other direction
    type Other: Relationship;
    /// A vector of node references in the direction of this relationship
    fn neighbors(node: NodeIndex, graph: &MirGraph) -> Bfs<NodeIndex, fixedbitset::FixedBitSet>;
}

impl Relationship for Descendants {
    type Other = Ancestors;
    fn neighbors(node: NodeIndex, graph: &MirGraph) -> Bfs<NodeIndex, fixedbitset::FixedBitSet> {
        Bfs::new(&**graph, node)
    }
}

impl Relationship for Ancestors {
    type Other = Descendants;
    fn neighbors(node: NodeIndex, graph: &MirGraph) -> Bfs<NodeIndex, fixedbitset::FixedBitSet> {
        Bfs::new(Reversed(&**graph), node)
    }
}

/// An iterator over the transitive relationships of a node in a MIR graph (either children or
/// ancestors). Constructed via the [`MirQuery::topo_ancestors`] and [`MirQuery::topo_descendants`]
/// function
pub struct Topo<'a, R: Relationship> {
    visitor: Bfs<NodeIndex, fixedbitset::FixedBitSet>,
    graph: &'a MirGraph,
    _phantom: PhantomData<R>,
}

impl<'a> Topo<'a, Descendants> {
    fn new(node: NodeIndex, graph: &'a MirGraph) -> ReadySetResult<Self> {
        if !graph.contains_node(node) {
            return Err(ReadySetError::MirNodeNotFound {
                index: node.index(),
            });
        }
        Ok(Topo {
            visitor: Descendants::neighbors(node, graph),
            graph,
            _phantom: PhantomData,
        })
    }
}

impl<'a> Topo<'a, Ancestors> {
    fn new(node: NodeIndex, graph: &'a MirGraph) -> ReadySetResult<Self> {
        if !graph.contains_node(node) {
            return Err(ReadySetError::MirNodeNotFound {
                index: node.index(),
            });
        }
        Ok(Topo {
            visitor: Ancestors::neighbors(node, graph),
            graph,
            _phantom: PhantomData,
        })
    }
}

impl<'a> Iterator for Topo<'a, Ancestors> {
    type Item = NodeIndex;

    fn next(&mut self) -> Option<Self::Item> {
        self.visitor.next(Reversed(&**self.graph))
    }
}

impl<'a> Iterator for Topo<'a, Descendants> {
    type Item = NodeIndex;

    fn next(&mut self) -> Option<Self::Item> {
        self.visitor.next(&**self.graph)
    }
}
