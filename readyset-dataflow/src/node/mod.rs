use std::collections::{HashMap, HashSet};

use readyset_client::consistency::Timestamp;
use readyset_data::{Collation, DfType, Dialect};
use readyset_sql::ast::{ColumnSpecification, Relation, SqlIdentifier};
use serde::{Deserialize, Serialize};

use crate::ops::grouped::aggregate::AggregatorState;
use crate::ops::grouped::concat::GroupConcatState;
use crate::ops::{self};
use crate::prelude::*;
use crate::processing::LookupIndex;

mod process;
#[cfg(test)]
pub(crate) use self::process::materialize;
pub(crate) use self::process::{NodeProcessingResult, ProcessEnv};

pub mod special;

mod ntype;
pub use self::ntype::NodeType;
use crate::processing::{ColumnMiss, ColumnRef, ColumnSource};

mod debug;

#[cfg(feature = "bench")]
pub use process::bench;

// NOTE(jfrg): the migration code should probably move into the dataflow crate...
// it is the reason why so much stuff here is pub

/// Dataflow column representation
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Column {
    /// Column name
    name: SqlIdentifier,
    /// Column type
    ty: DfType,
    /// The node or table where this column originates.
    ///
    /// TODO: Use this information to lookup the column specification required for returning a
    /// resultset to the client.
    source: Option<Relation>,
}

impl Column {
    pub fn new(name: SqlIdentifier, ty: DfType, source: Option<Relation>) -> Self {
        Self { name, ty, source }
    }

    /// Creates a dataflow column from the [`ColumnSpecification`].
    #[inline]
    pub fn from_spec<F>(
        spec: ColumnSpecification,
        dialect: Dialect,
        resolve_type: F,
    ) -> ReadySetResult<Self>
    where
        F: Fn(Relation) -> Option<DfType>,
    {
        Ok(Self::new(
            spec.column.name.clone(),
            DfType::from_sql_type(
                &spec.sql_type,
                dialect,
                resolve_type,
                Collation::from_mysql_collation(spec.get_collation().unwrap_or("")),
            )?,
            spec.column.table,
        ))
    }

    /// Column name
    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Type of the column
    pub fn ty(&self) -> &DfType {
        &self.ty
    }

    /// Originating column
    pub fn source(&self) -> Option<&Relation> {
        self.source.as_ref()
    }

    /// Sets this column's name
    pub fn set_name(&mut self, name: SqlIdentifier) {
        self.name = name;
    }
}

#[must_use]
#[derive(Clone, Serialize, Deserialize)]
pub struct Node {
    name: Relation,
    index: Option<IndexPair>,
    domain: Option<DomainIndex>,
    columns: Vec<Column>,
    parents: Vec<LocalNodeIndex>,
    children: Vec<LocalNodeIndex>,
    inner: NodeType,
    // We are skipping the serialization of `taken` so we get `false` for
    // this field upon deserialization.
    // We need that since we deserialize when we are recovering (and thus all nodes should
    // NOT be taken).
    #[serde(skip)]
    taken: bool,

    pub purge: bool,

    sharded_by: Sharding,

    // Tracks each up stream nodes timestamp.
    // Used to maintain read-your-write consistency when reading data
    // in the data flow graph.
    // Wrapped in a RefCell as this map will be mutated while using
    // immutable references to fields in Node.
    // We skip serde since we don't want the state of the node, just the configuration.
    #[serde(skip)]
    timestamps: HashMap<LocalNodeIndex, Timestamp>,
}

// constructors
impl Node {
    pub fn new<N, S2, CS, NT>(name: N, columns: CS, inner: NT) -> Node
    where
        N: Into<Relation>,
        S2: Into<Column>,
        CS: IntoIterator<Item = S2>,
        NT: Into<NodeType>,
    {
        Node {
            name: name.into(),
            index: None,
            domain: None,
            columns: columns.into_iter().map(|c| c.into()).collect(),
            parents: Vec::new(),
            children: Vec::new(),
            inner: inner.into(),
            taken: false,

            purge: false,

            sharded_by: Sharding::None,
            timestamps: HashMap::new(),
        }
    }

    pub fn mirror<NT: Into<NodeType>>(&self, n: NT) -> Node {
        Self::new(self.name.clone(), self.columns.clone(), n)
    }

    pub fn named_mirror<NT: Into<NodeType>>(&self, n: NT, name: Relation) -> Node {
        Self::new(name, self.columns.clone(), n)
    }

    /// Duplicates the existing node, clearing the index, taken flag, and timestamps
    /// Used to create fully materialized duplicates of partially materialized nodes
    pub fn duplicate(&self) -> Node {
        Self {
            index: None,
            taken: false,
            timestamps: HashMap::new(),
            ..self.clone()
        }
    }
}

#[must_use]
pub struct DanglingDomainNode(Node);

impl DanglingDomainNode {
    pub fn finalize(self, graph: &Graph) -> Node {
        let mut n = self.0;
        let ni = n.global_addr();
        let dm = n.domain();
        n.children = graph
            .neighbors_directed(ni, petgraph::EdgeDirection::Outgoing)
            .filter(|&c| graph[c].domain() == dm)
            .map(|ni| graph[ni].local_addr())
            .collect();
        n.parents = graph
            .neighbors_directed(ni, petgraph::EdgeDirection::Incoming)
            .filter(|&c| !graph[c].is_source() && graph[c].domain() == dm)
            .map(|ni| graph[ni].local_addr())
            .collect();
        n
    }
}

/// The state of an internal node in the graph
// Naming convention is NodeOperator(name of type internal to NodeOperator)
pub enum AuxiliaryNodeState {
    Aggregation(AggregatorState),
    Concat(GroupConcatState),
}

// external parts of Ingredient
impl Node {
    /// Called when a node is first connected to the graph.
    ///
    /// All its ancestors are present, but this node and its children may not have been connected
    /// yet.
    pub fn on_connected(&mut self, graph: &Graph) {
        if let Some(n) = self.as_mut_internal() {
            Ingredient::on_connected(n, graph)
        }
    }

    pub fn replace_sibling(&mut self, from_idx: NodeIndex, to_idx: NodeIndex) {
        assert!(!self.taken);
        if let Some(n) = self.as_mut_internal() {
            Ingredient::replace_sibling(n, from_idx, to_idx);
        }
    }

    pub fn on_commit(&mut self, remap: &HashMap<NodeIndex, IndexPair>) {
        // this is *only* overwritten for these asserts.
        assert!(!self.taken);
        if let NodeType::Internal(ref mut i) = self.inner {
            i.on_commit(self.index.unwrap().as_global(), remap)
        }
    }

    /// May return a set of nodes such that *one* of the given ancestors *must* be the one to be
    /// replayed if this node's state is to be initialized.
    pub fn must_replay_among(&self) -> Option<HashSet<NodeIndex>> {
        self.as_internal().and_then(Ingredient::must_replay_among)
    }

    /// Provide information about where the `cols` come from for the materialization planner
    /// (among other things) to make use of.
    ///
    /// See [`Ingredient::column_source`] for full documentation.
    ///
    /// # Invariants
    ///
    /// * self must have a self.inner of NodeType::Internal or this will panic We can't know the
    ///   column_source if we aren't an internal node.
    pub fn column_source(&self, cols: &[usize]) -> ColumnSource {
        #[allow(clippy::unwrap_used)] // Documented invariant.
        let ret = Ingredient::column_source(self.as_internal().unwrap(), cols);
        // in debug builds, double-check API invariants are maintained
        match ret {
            ColumnSource::ExactCopy(ColumnRef { ref columns, .. }) => {
                debug_assert_eq!(cols.len(), columns.len());
            }
            ColumnSource::Union(ref colrefs) => {
                if cfg!(debug_assertions) {
                    for ColumnRef { ref columns, .. } in colrefs {
                        debug_assert_eq!(cols.len(), columns.len());
                    }
                }
            }
            _ => {}
        }
        ret
    }

    /// Handle a miss on some columns that were marked as generated by the node's `column_source`
    /// implementation.
    ///
    /// See [`Ingredient::handle_upquery`] for full documentation.
    pub fn handle_upquery(&mut self, miss: ColumnMiss) -> ReadySetResult<Vec<ColumnMiss>> {
        if self.taken {
            return Err(ReadySetError::NodeAlreadyTaken);
        };
        Ingredient::handle_upquery(
            self.as_mut_internal()
                .ok_or(ReadySetError::NonInternalNode)?,
            miss,
        )
    }

    /// Translate a column in this ingredient into the corresponding column(s) in
    /// parent ingredients. None for the column means that the parent doesn't
    /// have an associated column. Similar to resolve, but does not depend on
    /// materialization, and returns results even for computed columns.
    ///
    /// This is implemented in terms of [`Ingredient::column_source`]; consider using that API
    /// instead in new code. It'll always return all ancestor nodes reported by that API, but will
    /// only report [`Some`] for the column if the source is [`ColumnSource::ExactCopy`] or
    /// [`ColumnSource::Union`].
    pub fn parent_columns(&self, column: usize) -> Vec<(NodeIndex, Option<usize>)> {
        match self.column_source(&[column]) {
            ColumnSource::ExactCopy(ColumnRef { node, columns }) => {
                vec![(node, columns.into_iter().next())]
            }
            ColumnSource::GeneratedFromColumns(refs) => {
                refs.into_iter().map(|x| (x.node, None)).collect()
            }
            ColumnSource::RequiresFullReplay(nodes) => {
                nodes.into_iter().map(|x| (x, None)).collect()
            }
            ColumnSource::Union(refs) => refs
                .into_iter()
                .map(|x| (x.node, x.columns.into_iter().next()))
                .collect(),
        }
    }

    /// Resolve where the given field originates from. If the view is materialized, or the value is
    /// otherwise created by this view, None should be returned.
    ///
    /// This is implemented in terms of [`Ingredient::column_source`]; consider using that API
    /// instead in new code. It only returns results if the column source is
    /// [`ColumnSource::ExactCopy`] or [`ColumnSource::Union`].
    pub fn resolve(&self, i: usize) -> Option<Vec<(NodeIndex, usize)>> {
        match self.column_source(&[i]) {
            ColumnSource::ExactCopy(ColumnRef { node, columns }) => Some(vec![(node, columns[0])]),
            ColumnSource::Union(refs) => Some(
                refs.into_iter()
                    .map(|ColumnRef { node, columns }| (node, columns[0]))
                    .collect(),
            ),
            _ => None,
        }
    }

    /// Returns true if this operator requires a full materialization
    pub fn requires_full_materialization(&self) -> bool {
        self.as_internal()
            .is_some_and(Ingredient::requires_full_materialization)
    }

    pub fn can_query_through(&self) -> bool {
        self.as_internal()
            .is_some_and(Ingredient::can_query_through)
    }

    pub fn is_join(&self) -> ReadySetResult<bool> {
        Ok(Ingredient::is_join(
            self.as_internal().ok_or(ReadySetError::NonInternalNode)?,
        ))
    }

    pub fn ancestors(&self) -> ReadySetResult<Vec<NodeIndex>> {
        Ok(Ingredient::ancestors(
            self.as_internal().ok_or(ReadySetError::NonInternalNode)?,
        ))
    }

    /// Produce a compact, human-readable description of this node for Graphviz.
    ///
    /// If `detailed` is true, and `self.inner` has is variant `NodeType::Internal`, emit more info.
    ///
    ///  Symbol   Description
    /// --------|-------------
    ///    ⊥    |  Source
    ///    B    |  Base
    ///    ||   |  Concat
    ///    ⧖    |  Latest
    ///    γ    |  Group by
    ///   |*|   |  Count
    ///    𝛴    |  Sum
    ///    ⋈    |  Join
    ///    ⋉    |  Left join
    ///    ⋃    |  Union
    ///    →|   |  Ingress
    ///    |→   |  Egress
    ///    ÷    |  Dropped
    ///    R    |  Reader
    ///    ☒    |  Dropped
    pub fn description(&self, detailed: bool) -> String {
        self.inner.description(detailed)
    }

    pub fn initial_auxiliary_state(&self) -> Option<AuxiliaryNodeState> {
        match &self.inner {
            NodeType::Internal(no) => match no {
                NodeOperator::Aggregation(_) => {
                    Some(AuxiliaryNodeState::Aggregation(Default::default()))
                }
                NodeOperator::Concat(_) => Some(AuxiliaryNodeState::Concat(Default::default())),
                NodeOperator::Extremum(_)
                | NodeOperator::Join(_)
                | NodeOperator::Paginate(_)
                | NodeOperator::Project(_)
                | NodeOperator::Union(_)
                | NodeOperator::Identity(_)
                | NodeOperator::Filter(_)
                | NodeOperator::TopK(_) => None,
            },
            NodeType::Ingress
            | NodeType::Base(_)
            | NodeType::Egress(_)
            | NodeType::Sharder(_)
            | NodeType::Reader(_)
            | NodeType::Source
            | NodeType::Dropped => None,
        }
    }
}

// publicly accessible attributes
impl Node {
    pub fn name(&self) -> &Relation {
        &self.name
    }

    pub fn columns(&self) -> &[Column] {
        &self.columns[..]
    }

    pub fn sharded_by(&self) -> Sharding {
        self.sharded_by
    }

    /// Set this node's sharding property.
    pub fn shard_by(&mut self, s: Sharding) {
        self.sharded_by = s;
    }

    /// Returns the node's inner NodeType as a String.
    pub fn node_type_string(&self) -> String {
        self.inner.to_string()
    }
}

// events
impl Node {
    pub fn take(&mut self) -> DanglingDomainNode {
        debug_assert!(!self.taken);
        debug_assert!(
            (!self.is_internal() && !self.is_base()) || self.domain.is_some(),
            "tried to take unassigned node"
        );

        let inner = self.inner.take();
        let mut n = self.mirror(inner);
        n.index = self.index;
        n.domain = self.domain;
        n.purge = self.purge;
        self.taken = true;

        DanglingDomainNode(n)
    }

    pub fn remove(&mut self) {
        self.inner = NodeType::Dropped;
    }
}

// derefs
impl Node {
    /// If this node is a [`special::Sharder`], return a reference to that sharder, otherwise return
    /// None
    pub fn as_sharder(&self) -> Option<&special::Sharder> {
        match &self.inner {
            NodeType::Sharder(r) => Some(r),
            _ => None,
        }
    }

    /// If this node is a [`Internal`], return a reference to the operator, otherwise return
    /// None
    pub fn as_internal(&self) -> Option<&ops::NodeOperator> {
        match &self.inner {
            NodeType::Internal(i) => Some(i),
            _ => None,
        }
    }

    /// If this node is a [`Internal`], return a mutable reference to the operator, otherwise return
    /// None
    pub fn as_mut_internal(&mut self) -> Option<&mut ops::NodeOperator> {
        match &mut self.inner {
            NodeType::Internal(i) if !self.taken => Some(i),
            _ => None,
        }
    }

    /// If this node is a [`special::Sharder`], return a mutable reference to that sharder,
    /// otherwise return None
    pub fn as_mut_sharder(&mut self) -> Option<&mut special::Sharder> {
        match &mut self.inner {
            NodeType::Sharder(r) => Some(r),
            _ => None,
        }
    }

    /// If this node is a [`special::Egress`], return a reference to that egress, otherwise return
    /// None
    pub fn as_egress(&self) -> Option<&special::Egress> {
        match &self.inner {
            NodeType::Egress(Some(r)) => Some(r),
            _ => None,
        }
    }

    /// If this node is a [`special::Egress`], return a mutable reference to that egress, otherwise
    /// return None
    pub fn as_mut_egress(&mut self) -> Option<&mut special::Egress> {
        match &mut self.inner {
            NodeType::Egress(Some(r)) => Some(r),
            _ => None,
        }
    }

    /// If this node is a [`special::Reader`], return a reference to that reader, otherwise return
    /// None
    pub fn as_reader(&self) -> Option<&special::Reader> {
        match &self.inner {
            NodeType::Reader(r) => Some(r),
            _ => None,
        }
    }

    /// If this node is a [`special::Reader`], return a mutable reference to that reader, otherwise
    /// return None
    pub fn as_mut_reader(&mut self) -> Option<&mut special::Reader> {
        match &mut self.inner {
            NodeType::Reader(r) => Some(r),
            _ => None,
        }
    }

    pub fn get_base(&self) -> Option<&special::Base> {
        if let NodeType::Base(ref b) = self.inner {
            Some(b)
        } else {
            None
        }
    }

    pub fn suggest_indexes(&self, n: NodeIndex) -> HashMap<NodeIndex, LookupIndex> {
        match self.inner {
            NodeType::Internal(ref i) => i.suggest_indexes(n),
            NodeType::Base(ref b) => b.suggest_indexes(n),
            _ => HashMap::new(),
        }
    }
}

// neighbors
impl Node {
    pub(crate) fn children(&self) -> &[LocalNodeIndex] {
        &self.children
    }

    pub(crate) fn parents(&self) -> &[LocalNodeIndex] {
        &self.parents
    }
}

// attributes
impl Node {
    pub(crate) fn beyond_mat_frontier(&self) -> bool {
        self.purge
    }

    pub(crate) fn add_child(&mut self, child: LocalNodeIndex) {
        self.children.push(child);
    }

    pub(crate) fn try_remove_child(&mut self, child: LocalNodeIndex) -> bool {
        for i in 0..self.children.len() {
            if self.children[i] == child {
                self.children.swap_remove(i);
                return true;
            }
        }
        false
    }

    pub fn add_column(&mut self, column: Column) -> usize {
        self.columns.push(column);
        self.columns.len() - 1
    }

    pub fn has_domain(&self) -> bool {
        self.domain.is_some()
    }

    /// Retrieves the index of the domain.
    ///
    /// Invariants:
    ///
    /// * Must call on_connected prior to using this helper function.
    pub fn domain(&self) -> DomainIndex {
        match self.domain {
            Some(domain) => domain,
            None => {
                // Documented invariant.
                unreachable!(
                    "asked for unset domain for {:?} {}",
                    self,
                    self.global_addr().index()
                );
            }
        }
    }

    /// Retrieves the local address for this node.
    ///
    /// Invariants:
    ///
    /// * Must call on_connected prior to using this helper function.
    pub fn local_addr(&self) -> LocalNodeIndex {
        match self.index {
            Some(idx) if idx.has_local() => *idx,
            Some(_) | None => {
                // Documented Invariant.
                unreachable!("asked for unset addr for {:?}", self)
            }
        }
    }

    pub fn has_local_addr(&self) -> bool {
        self.index.iter().any(|idx| idx.has_local())
    }

    /// Retrieves the global address for this node.
    ///
    /// Invariants:
    ///
    /// * Must call on_connected prior to using this helper function.
    pub fn global_addr(&self) -> NodeIndex {
        match self.index {
            Some(ref index) => index.as_global(),
            None => {
                // Documented Invariant.
                unreachable!("asked for unset index for {:?}", self);
            }
        }
    }

    pub fn get_base_mut(&mut self) -> Option<&mut special::Base> {
        if let NodeType::Base(ref mut b) = self.inner {
            Some(b)
        } else {
            None
        }
    }

    pub fn add_to(&mut self, domain: DomainIndex) {
        debug_assert_eq!(self.domain, None);
        debug_assert!(!self.is_dropped());
        self.domain = Some(domain);
    }

    pub fn set_finalized_addr(&mut self, addr: IndexPair) {
        self.index = Some(addr);
    }

    /// Change the type of one of this node's columns
    ///
    /// Returns an error if the given column index is out-of-bounds
    pub fn set_column_type(&mut self, column_index: usize, new_type: DfType) -> ReadySetResult<()> {
        self.columns
            .get_mut(column_index)
            .ok_or_else(|| internal_err!("Column {column_index} out of bounds"))?
            .ty = new_type;

        Ok(())
    }
}

// is this or that?
impl Node {
    pub fn is_dropped(&self) -> bool {
        matches!(self.inner, NodeType::Dropped)
    }

    pub fn is_egress(&self) -> bool {
        matches!(self.inner, NodeType::Egress { .. })
    }

    pub fn is_reader(&self) -> bool {
        matches!(self.inner, NodeType::Reader { .. })
    }

    pub fn is_reader_for(&self, ni: NodeIndex) -> bool {
        self.as_reader().is_some_and(|r| r.is_for() == ni)
    }

    pub fn is_ingress(&self) -> bool {
        matches!(self.inner, NodeType::Ingress)
    }

    pub fn is_sender(&self) -> bool {
        matches!(self.inner, NodeType::Egress { .. } | NodeType::Sharder(..))
    }

    pub fn is_internal(&self) -> bool {
        matches!(self.inner, NodeType::Internal(..))
    }

    pub fn is_source(&self) -> bool {
        matches!(self.inner, NodeType::Source { .. })
    }

    pub fn is_sharder(&self) -> bool {
        matches!(self.inner, NodeType::Sharder { .. })
    }

    /// Returns `true` if self is a base table node
    pub fn is_base(&self) -> bool {
        matches!(self.inner, NodeType::Base(..))
    }

    pub fn is_union(&self) -> bool {
        matches!(self.inner, NodeType::Internal(NodeOperator::Union(_)))
    }

    pub fn is_shard_merger(&self) -> bool {
        if let NodeType::Internal(NodeOperator::Union(ref u)) = self.inner {
            u.is_shard_merger()
        } else {
            false
        }
    }
}
