use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::fmt::{Debug, Display, Error, Formatter};
use std::marker::PhantomData;
use std::rc::Rc;
use std::{iter, mem};

use dataflow::ops;
use dataflow::prelude::ReadySetError;
use nom_sql::analysis::ReferredColumns;
use nom_sql::{ColumnSpecification, SqlIdentifier};
use noria_errors::{internal, internal_err, ReadySetResult};
use petgraph::graph::NodeIndex;
use serde::{Deserialize, Serialize};

pub use self::node_inner::MirNodeInner;
use crate::column::Column;
use crate::{FlowNode, MirNodeRef, MirNodeWeakRef, PAGE_NUMBER_COL};

pub mod node_inner;

/// Helper enum to avoid having separate `make_aggregation_node` and `make_extremum_node` functions
pub enum GroupedNodeType {
    Aggregation(ops::grouped::aggregate::Aggregation),
    Extremum(ops::grouped::extremum::Extremum),
}

#[derive(Serialize, Deserialize)]
pub struct MirNode {
    pub name: SqlIdentifier,
    pub from_version: usize,
    pub inner: MirNodeInner,
    #[serde(skip)]
    pub ancestors: Vec<MirNodeWeakRef>,
    #[serde(skip)]
    pub children: Vec<MirNodeRef>,
    pub flow_node: Option<FlowNode>,
}

impl MirNode {
    pub fn new(
        name: SqlIdentifier,
        v: usize,
        inner: MirNodeInner,
        ancestors: Vec<MirNodeWeakRef>,
        children: Vec<MirNodeRef>,
    ) -> MirNodeRef {
        let mn = MirNode {
            name,
            from_version: v,
            inner,
            ancestors: ancestors.clone(),
            children,
            flow_node: None,
        };

        let rc_mn = Rc::new(RefCell::new(mn));

        // register as child on ancestors
        for ancestor in ancestors.iter().map(|n| n.upgrade().unwrap()) {
            ancestor.borrow_mut().add_child(rc_mn.clone());
        }

        rc_mn
    }

    /// Build a new MIR node that is a reuse of the given MIR node
    pub fn new_reuse(node: MirNodeRef) -> MirNodeRef {
        // Note that we manually build the `MirNode` here, rather than calling `MirNode::new()`
        // because `new()` automatically registers the node as a child with its ancestors. We
        // don't want to do this here because we later re-write the ancestors' child that this
        // node replaces to point to this node.
        let n = node.borrow();
        let node = node.clone();
        Rc::new(RefCell::new(MirNode {
            inner: MirNodeInner::Reuse { node },
            flow_node: None,
            name: n.name.clone(),
            from_version: n.from_version,
            ancestors: n.ancestors.clone(),
            children: n.children.clone(),
        }))
    }

    /// Adapts an existing `Base`-type MIR Node with the specified column additions and removals.
    pub fn adapt_base(
        node: MirNodeRef,
        added_cols: Vec<&ColumnSpecification>,
        removed_cols: Vec<&ColumnSpecification>,
    ) -> MirNodeRef {
        let over_node = node.borrow();
        match &over_node.inner {
            MirNodeInner::Base {
                column_specs,
                primary_key,
                unique_keys,
                ..
            } => {
                let new_column_specs: Vec<(ColumnSpecification, Option<usize>)> = column_specs
                    .iter()
                    .cloned()
                    .filter(|&(ref cs, _)| !removed_cols.contains(&cs))
                    .chain(
                        added_cols
                            .iter()
                            .map(|c| ((*c).clone(), None))
                            .collect::<Vec<(ColumnSpecification, Option<usize>)>>(),
                    )
                    .collect();
                assert_eq!(
                    new_column_specs.len(),
                    over_node.columns().len() + added_cols.len() - removed_cols.len()
                );

                let new_inner = MirNodeInner::Base {
                    column_specs: new_column_specs,
                    primary_key: primary_key.clone(),
                    unique_keys: unique_keys.clone(),
                    adapted_over: Some(BaseNodeAdaptation {
                        over: node.clone(),
                        columns_added: added_cols.into_iter().cloned().collect(),
                        columns_removed: removed_cols.into_iter().cloned().collect(),
                    }),
                };
                MirNode::new(
                    over_node.name.clone(),
                    over_node.from_version,
                    new_inner,
                    vec![],
                    over_node.children.clone(),
                )
            }
            _ => unreachable!(),
        }
    }

    /// Wraps an existing MIR node into a `Reuse` node.
    /// Note that this does *not* wire the reuse node into ancestors or children of the original
    /// node; if required, this is the responsibility of the caller.
    pub fn reuse(node: MirNodeRef, v: usize) -> MirNodeRef {
        let rcn = node.clone();

        let mn = MirNode {
            name: node.borrow().name.clone(),
            from_version: v,
            inner: MirNodeInner::Reuse { node: rcn },
            ancestors: vec![],
            children: vec![],
            flow_node: None, // will be set in `into_flow_parts`
        };

        Rc::new(RefCell::new(mn))
    }

    pub fn can_reuse_as(&self, for_node: &MirNode) -> bool {
        let our_columns = self.columns();
        for_node.columns().iter().all(|c| our_columns.contains(c))
            && self.inner.can_reuse_as(&for_node.inner)
    }

    /// Add a new MIR node to the set of ancestors for this node.
    ///
    /// Note that this does *not* add this node to the set of children for that node - that is the
    /// responsibility of the caller!
    pub fn add_ancestor(&mut self, a: MirNodeRef) {
        self.ancestors.push(MirNodeRef::downgrade(&a))
    }

    pub fn remove_ancestor(&mut self, a: MirNodeRef) {
        if let Some(idx) = self
            .ancestors
            .iter()
            .map(|n| n.upgrade().unwrap())
            .position(|x| x.borrow().versioned_name() == a.borrow().versioned_name())
        {
            self.ancestors.remove(idx);
        }
    }

    /// Add a new MIR node to the set of children for this node.
    ///
    /// Note that this does *not* add this node to the set of ancestors for that node - that is the
    /// responsibility of the caller!
    pub fn add_child(&mut self, c: MirNodeRef) {
        self.children.push(c)
    }

    pub fn remove_child(&mut self, a: MirNodeRef) {
        match self
            .children
            .iter()
            .position(|x| x.borrow().versioned_name() == a.borrow().versioned_name())
        {
            None => (),
            Some(idx) => {
                self.children.remove(idx);
            }
        }
    }

    /// Remove the given MIR node from the graph, replacing all references its children have to it
    /// with references to its own ancestors
    ///
    /// This cannot be called on any node with more than 1 parent.
    pub fn remove(node: MirNodeRef) {
        assert_eq!(node.borrow().ancestors().len(), 1);
        let ancestor = node
            .borrow()
            .ancestors()
            .first()
            .unwrap()
            .upgrade()
            .unwrap();
        ancestor.borrow_mut().remove_child(node.clone());
        for child in node.borrow().children() {
            child.borrow_mut().remove_ancestor(node.clone());
            child.borrow_mut().add_ancestor(ancestor.clone());
            ancestor.borrow_mut().add_child(child.clone())
        }

        node.borrow_mut().children = vec![];
        node.borrow_mut().ancestors = vec![];
    }

    /// Insert the `child` node as a new child between `node` and its children
    ///
    /// For example, if we have:
    ///
    /// ```dot
    /// node -> n_child_1
    /// node -> n_child_2
    /// ```
    ///
    /// After this function is called we will have:
    ///
    /// ```dot
    /// node -> new_child
    /// new_child -> n_child_1
    /// new_child -> n_child_2
    /// ```
    pub fn splice_child_below(node: MirNodeRef, new_child: MirNodeRef) {
        let old_children = mem::take(&mut node.borrow_mut().children);
        for child in old_children {
            child.borrow_mut().remove_ancestor(node.clone());
            child.borrow_mut().add_ancestor(new_child.clone());
            new_child.borrow_mut().add_child(child.clone());
        }
        node.borrow_mut().add_child(new_child.clone());
        new_child.borrow_mut().add_ancestor(node);
    }

    /// Add a new column to the set of emitted columns for this node
    pub fn add_column(&mut self, c: Column) -> ReadySetResult<()> {
        if !self.inner.add_column(c.clone())? {
            self.parent()
                .ok_or_else(|| {
                    internal_err(format!(
                        "MIR node {:?} has the wrong number of parents ({})",
                        self.inner,
                        self.ancestors().len()
                    ))
                })?
                .borrow_mut()
                .add_column(c)?;
        }

        Ok(())
    }

    pub fn first_ancestor(&self) -> Option<MirNodeRef> {
        self.ancestors().first()?.upgrade()
    }

    /// Returns a reference to this MIR node's parent, if it only has a *single* parent
    pub fn parent(&self) -> Option<MirNodeRef> {
        if self.ancestors().len() != 1 {
            return None;
        }
        self.first_ancestor()
    }

    pub fn ancestors(&self) -> &[MirNodeWeakRef] {
        self.ancestors.as_slice()
    }

    pub fn children(&self) -> &[MirNodeRef] {
        self.children.as_slice()
    }

    /// Returns the list of columns in the output of this node
    pub fn columns(&self) -> Vec<Column> {
        let parent_columns = || {
            self.parent()
                .expect("MIR node has the wrong number of parents")
                .borrow()
                .columns()
        };

        match &self.inner {
            MirNodeInner::Base { column_specs, .. } => column_specs
                .iter()
                .map(|(spec, _)| spec.column.clone().into())
                .collect(),
            MirNodeInner::Filter { .. }
            | MirNodeInner::Leaf { .. }
            | MirNodeInner::Identity
            | MirNodeInner::Latest { .. }
            | MirNodeInner::TopK { .. } => parent_columns(),
            MirNodeInner::AliasTable { table } => parent_columns()
                .iter()
                .map(|c| Column {
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
                let mut columns = self
                    .ancestors()
                    .iter()
                    .flat_map(|n| n.upgrade().unwrap().borrow().columns())
                    .collect::<Vec<_>>();

                // Crappy quadratic column deduplication, because we don't have Hash or Ord for
                // Column due to aliases affecting equality
                let mut i = columns.len() - 1;
                while i > 0 {
                    let col = &columns[i];
                    if columns[..i].contains(col) || columns[(i + 1)..].contains(col) {
                        columns.remove(i);
                    } else if i == 0 {
                        break;
                    } else {
                        i -= 1;
                    }
                }

                columns
            }
            MirNodeInner::Project {
                emit,
                expressions,
                literals,
            } => emit
                .iter()
                .cloned()
                .chain(
                    expressions
                        .iter()
                        .map(|(name, _)| name)
                        .chain(literals.iter().map(|(name, _)| name))
                        .map(Column::named),
                )
                .collect(),
            MirNodeInner::Union { emit, .. } => emit
                .first()
                .cloned()
                .expect("Union must have at least one set of emit columns"),
            MirNodeInner::Paginate { .. } => parent_columns()
                .into_iter()
                .chain(iter::once(Column::named(&*PAGE_NUMBER_COL)))
                .collect(),
            MirNodeInner::Distinct { group_by } => group_by
                .iter()
                .cloned()
                // Distinct gets lowered to COUNT, so it emits one extra column at the end - but
                // nobody cares about that column, so just give it a throwaway name here
                .chain(iter::once(Column::named("__distinct_count")))
                .collect(),
            MirNodeInner::Reuse { node } => node.borrow().columns(),
        }
    }

    /// Finds the source of a child column within the node.
    /// This is currently used for locating the source of a projected column.
    pub fn find_source_for_child_column(&self, child: &Column) -> Option<usize> {
        // we give the alias preference here because in a query like
        // SELECT table1.column1 AS my_alias
        // my_alias will be the column name and "table1.column1" will be the alias.
        // This is slightly backwards from what intuition suggests when you first look at the
        // column struct but means its the "alias" that will exist in the parent node,
        // not the column name.
        if child.aliases.is_empty() {
            self.columns().iter().position(|c| c == child)
        } else {
            self.columns()
                .iter()
                .position(|c| child.aliases.contains(c))
                .or_else(|| self.columns().iter().position(|c| c == child))
        }
    }

    pub fn column_id_for_column(&self, c: &Column) -> ReadySetResult<usize> {
        #[allow(clippy::cmp_owned)]
        match self.inner {
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
                .rposition(|cs| Column::from(&cs.0.column) == *c)
            {
                None => Err(ReadySetError::NonExistentColumn {
                    column: format!(
                        "{}{}",
                        match &c.table {
                            Some(table) => format!("{table}."),
                            None => "".to_owned(),
                        },
                        c.name
                    ),
                    node: self.name.to_string(),
                }),
                Some(id) => Ok(column_specs[id]
                    .1
                    .expect("must have an absolute column ID on base")),
            },
            MirNodeInner::Reuse { ref node } => node.borrow().column_id_for_column(c),
            // otherwise, just look up in the column set
            // Compare by name if there is no table
            _ => match {
                if c.table.is_none() {
                    self.columns().iter().position(|cc| cc.name == c.name)
                } else {
                    self.columns().iter().position(|cc| cc == c)
                }
            } {
                Some(id) => Ok(id),
                None => Err(ReadySetError::NonExistentColumn {
                    column: format!(
                        "{}{}",
                        match &c.table {
                            Some(table) => format!("{table}."),
                            None => "".to_owned(),
                        },
                        c.name
                    ),
                    node: self.name.to_string(),
                }),
            },
        }
    }

    /// Returns a slice to the column specifications, if this MIR-Node is a base node.
    /// Otherwise, returns `None`.
    pub fn column_specifications(&self) -> ReadySetResult<&[(ColumnSpecification, Option<usize>)]> {
        match self.inner {
            MirNodeInner::Base {
                ref column_specs, ..
            } => Ok(column_specs.as_slice()),
            _ => internal!("Non-base MIR nodes don't have column specifications!"),
        }
    }

    pub fn flow_node_addr(&self) -> ReadySetResult<NodeIndex> {
        match self.flow_node {
            Some(FlowNode::New(na)) | Some(FlowNode::Existing(na)) => Ok(na),
            None => Err(internal_err(format!(
                "MIR node \"{}\" does not have an associated FlowNode",
                self.versioned_name()
            ))),
        }
    }

    #[allow(dead_code)]
    pub fn is_reused(&self) -> bool {
        matches!(self.inner, MirNodeInner::Reuse { .. })
    }

    pub fn name(&self) -> &SqlIdentifier {
        &self.name
    }

    /// Returns a list of columns *referenced* by this node, ie the columns this node requires from
    /// its parent.
    pub fn referenced_columns(&self) -> Vec<Column> {
        match &self.inner {
            MirNodeInner::Aggregation { on, group_by, .. }
            | MirNodeInner::Extremum { on, group_by, .. } => {
                // Aggregates need the group_by columns and the "over" column
                let mut columns = group_by.clone();
                if !columns.contains(on) {
                    columns.push(on.clone());
                }
                columns
            }
            MirNodeInner::Project {
                emit, expressions, ..
            } => {
                let mut columns = vec![];
                for c in emit {
                    if !columns.contains(c) {
                        columns.push(c.clone());
                    }
                }
                for (_, expr) in expressions {
                    for c in expr.referred_columns() {
                        if !columns.iter().any(|col| col == c) {
                            columns.push(c.clone().into());
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
                let mut columns = self.columns();
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
                let mut columns = self.columns();
                for c in conditions.referred_columns() {
                    if !columns.iter().any(|col| col == c) {
                        columns.push(c.clone().into())
                    }
                }
                columns
            }
            _ => self.columns(),
        }
    }

    pub fn versioned_name(&self) -> String {
        format!("{}_v{}", self.name, self.from_version)
    }

    /// Produce a compact, human-readable description of this node; analogous to the method of the
    /// same name on `Ingredient`.
    pub(crate) fn description(&self) -> String {
        format!(
            "{}: {} / {} columns",
            self.versioned_name(),
            self.inner.description(),
            self.columns().len()
        )
    }

    /// Clones the [`MirNode`] but leaving the `children` and `ancestors` fields empty.
    #[must_use]
    pub fn clone_without_relations(&self) -> MirNode {
        MirNode {
            name: self.name.clone(),
            from_version: self.from_version,
            inner: self.inner.clone(),
            ancestors: Default::default(),
            children: Default::default(),
            flow_node: self.flow_node,
        }
    }

    /// Return an iterator over all the transitive ancestors of this node in topological order
    ///
    /// This iterator will yield nodes children first, starting at the ancestors of this node (but
    /// not including this node itself).
    pub fn topo_ancestors(&self) -> Topo<Ancestors> {
        Topo::new(self)
    }

    /// Return an iterator over all the transitive children of this node in reverse topographical
    /// order
    ///
    /// This iterator will yield nodes ancestors first, starting at the children of this node (but
    /// not including this node itself).
    pub fn topo_descendants(&self) -> Topo<Children> {
        Topo::new(self)
    }

    /// Return an iterator over all transitive root ancestors of this node (ancestor nodes which
    /// themselves don't have parents).
    pub fn root_ancestors(&self) -> impl Iterator<Item = MirNodeRef> {
        self.topo_ancestors()
            .filter(|n| n.borrow().ancestors().is_empty())
    }
}

/// A [`Relationship`] that goes towards the [`children`][] of a node
///
/// [`children`]: MirNode::children
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Children;

/// A [`Relationship`] that goes towards the [`ancestors`][] of a node
///
/// [`ancestors`]: MirNode::ancestors
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Ancestors;

mod sealed {
    pub trait Sealed {}
    impl Sealed for super::Children {}
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
    fn neighbors(node: &MirNode) -> Vec<MirNodeRef>;
}

impl Relationship for Children {
    type Other = Ancestors;
    fn neighbors(node: &MirNode) -> Vec<MirNodeRef> {
        node.children().to_vec()
    }
}

impl Relationship for Ancestors {
    type Other = Children;
    fn neighbors(node: &MirNode) -> Vec<MirNodeRef> {
        node.ancestors()
            .iter()
            .map(|mnr| mnr.upgrade().unwrap())
            .collect()
    }
}

/// An iterator over the transitive relationships of a node in a MIR graph (either children or
/// ancestors). Constructed via the [`Node::topo_ancestors`] and [`Node::topo_children`] function
pub struct Topo<R: Relationship> {
    queue: VecDeque<MirNodeRef>,
    edge_counts: HashMap<String, usize>,
    _phantom: PhantomData<R>,
}

impl<R: Relationship> Topo<R> {
    fn new(node: &MirNode) -> Self {
        Topo {
            queue: R::neighbors(node).into(),
            edge_counts: Default::default(),
            _phantom: PhantomData,
        }
    }
}

impl<R: Relationship> Iterator for Topo<R> {
    type Item = MirNodeRef;

    fn next(&mut self) -> Option<Self::Item> {
        self.queue.pop_front().map(|n| {
            for sib in R::neighbors(&n.borrow()) {
                let nd = sib.borrow().versioned_name();
                let num_edges = if let Some(num_edges) = self.edge_counts.get(&nd) {
                    *num_edges
                } else {
                    R::Other::neighbors(&sib.borrow()).len()
                };

                assert!(num_edges >= 1, "{} has no incoming edges!", nd);
                if num_edges == 1 {
                    self.queue.push_back(sib.clone());
                }
                self.edge_counts.insert(nd, num_edges - 1);
            }
            n
        })
    }
}

/// Specifies the adapatation of an existing base node by column addition/removal.
/// `over` is a `MirNode` of type `Base`.
#[derive(Clone, Serialize, Deserialize)]
pub struct BaseNodeAdaptation {
    pub over: MirNodeRef,
    pub columns_added: Vec<ColumnSpecification>,
    pub columns_removed: Vec<ColumnSpecification>,
}

impl Display for MirNode {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        write!(f, "{}", self.inner.description())
    }
}

impl Debug for MirNode {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        write!(
            f,
            "{}, {} ancestors ({}), {} children ({})",
            self.description(),
            self.ancestors.len(),
            self.ancestors
                .iter()
                .map(|n| n.upgrade().unwrap())
                .map(|a| a.borrow().versioned_name())
                .collect::<Vec<_>>()
                .join(", "),
            self.children.len(),
            self.children
                .iter()
                .map(|c| c.borrow().versioned_name())
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod columns {
        use common::IndexType;
        use dataflow::ops::grouped::aggregate::Aggregation;
        use dataflow::ops::grouped::extremum::Extremum;
        use dataflow::ops::union::DuplicateMode;
        use nom_sql::{BinaryOperator, Expression, OrderType, SqlType};
        use noria::ViewPlaceholder;

        use super::*;

        fn base1() -> MirNodeRef {
            MirNode::new(
                "base".into(),
                1,
                MirNodeInner::Base {
                    column_specs: vec![
                        (
                            ColumnSpecification {
                                column: "base.a".into(),
                                sql_type: SqlType::Int(None),
                                constraints: vec![],
                                comment: None,
                            },
                            None,
                        ),
                        (
                            ColumnSpecification {
                                column: "base.b".into(),
                                sql_type: SqlType::Int(None),
                                constraints: vec![],
                                comment: None,
                            },
                            None,
                        ),
                    ],
                    primary_key: None,
                    unique_keys: vec![].into(),
                    adapted_over: None,
                },
                vec![],
                vec![],
            )
        }

        fn base2() -> MirNodeRef {
            MirNode::new(
                "base2".into(),
                1,
                MirNodeInner::Base {
                    column_specs: vec![
                        (
                            ColumnSpecification {
                                column: "base2.a".into(),
                                sql_type: SqlType::Int(None),
                                constraints: vec![],
                                comment: None,
                            },
                            None,
                        ),
                        (
                            ColumnSpecification {
                                column: "base2.b".into(),
                                sql_type: SqlType::Int(None),
                                constraints: vec![],
                                comment: None,
                            },
                            None,
                        ),
                    ],
                    primary_key: None,
                    unique_keys: vec![].into(),
                    adapted_over: None,
                },
                vec![],
                vec![],
            )
        }

        fn has_columns_single_parent(inner: MirNodeInner, expected_cols: Vec<Column>) {
            let base = base1();
            let node = MirNode::new(
                "n".into(),
                1,
                inner,
                vec![MirNodeRef::downgrade(&base)],
                vec![],
            );
            let cols = node.borrow().columns();
            assert_eq!(cols, expected_cols);
        }

        fn same_columns_as_parent(inner: MirNodeInner) {
            has_columns_single_parent(
                inner,
                vec![
                    Column::new(Some("base"), "a"),
                    Column::new(Some("base"), "b"),
                ],
            )
        }

        #[test]
        fn alias_table() {
            has_columns_single_parent(
                MirNodeInner::AliasTable { table: "at".into() },
                vec![Column::new(Some("at"), "a"), Column::new(Some("at"), "b")],
            )
        }

        #[test]
        fn reuse() {
            let node = MirNode::new(
                "reuse".into(),
                1,
                MirNodeInner::Reuse { node: base1() },
                vec![],
                vec![],
            );
            let cols = node.borrow().columns();
            assert_eq!(
                cols,
                vec![
                    Column::new(Some("base"), "a"),
                    Column::new(Some("base"), "b")
                ]
            );
        }

        #[test]
        fn aggregation() {
            has_columns_single_parent(
                MirNodeInner::Aggregation {
                    on: Column::new(Some("base"), "a"),
                    group_by: vec![Column::new(Some("base"), "b")],
                    output_column: Column::named("agg"),
                    kind: Aggregation::Sum,
                },
                vec![Column::new(Some("base"), "b"), Column::named("agg")],
            );
        }

        #[test]
        fn extremum() {
            has_columns_single_parent(
                MirNodeInner::Extremum {
                    on: Column::new(Some("base"), "a"),
                    group_by: vec![Column::new(Some("base"), "b")],
                    output_column: Column::named("agg"),
                    kind: Extremum::Max,
                },
                vec![Column::new(Some("base"), "b"), Column::named("agg")],
            );
        }

        #[test]
        fn project() {
            has_columns_single_parent(
                MirNodeInner::Project {
                    emit: vec![Column::new(Some("base"), "b")],
                    expressions: vec![(
                        "expr".into(),
                        Expression::BinaryOp {
                            lhs: Box::new(Expression::Column("base.a".into())),
                            op: BinaryOperator::Add,
                            rhs: Box::new(Expression::Literal(1.into())),
                        },
                    )],
                    literals: vec![("lit".into(), 2.into())],
                },
                vec![
                    Column::new(Some("base"), "b"),
                    Column::named("expr"),
                    Column::named("lit"),
                ],
            )
        }

        #[test]
        fn filter() {
            same_columns_as_parent(MirNodeInner::Filter {
                conditions: Expression::BinaryOp {
                    lhs: Box::new(Expression::Column("base.a".into())),
                    op: BinaryOperator::Equal,
                    rhs: Box::new(Expression::Literal(1.into())),
                },
            })
        }

        #[test]
        fn join() {
            let base1 = base1();
            let base2 = base2();

            let project = vec![
                Column {
                    table: Some("base".into()),
                    name: "a".into(),
                    aliases: vec![Column::new(Some("base2"), "a")],
                },
                Column::new(Some("base"), "b"),
                Column::new(Some("base2"), "b"),
            ];
            let j = MirNode::new(
                "j".into(),
                1,
                MirNodeInner::Join {
                    on_left: vec![Column::new(Some("base"), "a")],
                    on_right: vec![Column::new(Some("base2"), "a")],
                    project: project.clone(),
                },
                vec![MirNodeRef::downgrade(&base1), MirNodeRef::downgrade(&base2)],
                vec![],
            );

            let cols = j.borrow().columns();
            assert_eq!(cols, project);
        }

        #[test]
        fn union() {
            let base1 = base1();
            let base2 = base2();

            let base2_alias_table = MirNode::new(
                "base2_alias_table".into(),
                1,
                MirNodeInner::AliasTable {
                    table: "base".into(),
                },
                vec![MirNodeRef::downgrade(&base2)],
                vec![],
            );

            let union = MirNode::new(
                "union".into(),
                1,
                MirNodeInner::Union {
                    emit: vec![
                        vec![
                            Column::new(Some("base"), "a"),
                            Column::new(Some("base"), "b"),
                        ],
                        vec![
                            Column::new(Some("base"), "a"),
                            Column::new(Some("base"), "b"),
                        ],
                    ],
                    duplicate_mode: DuplicateMode::BagUnion,
                },
                vec![
                    MirNodeRef::downgrade(&base1),
                    MirNodeRef::downgrade(&base2_alias_table),
                ],
                vec![],
            );
            let cols = union.borrow().columns();
            assert_eq!(
                cols,
                vec![
                    Column::new(Some("base"), "a"),
                    Column::new(Some("base"), "b")
                ]
            );
        }

        #[test]
        fn leaf() {
            same_columns_as_parent(MirNodeInner::Leaf {
                keys: vec![(Column::new(Some("base"), "a"), ViewPlaceholder::OneToOne(1))],
                index_type: IndexType::HashMap,
                order_by: None,
                limit: None,
                returned_cols: None,
                default_row: None,
                aggregates: None,
            })
        }

        #[test]
        fn topk() {
            same_columns_as_parent(MirNodeInner::TopK {
                order: Some(vec![(
                    Column::new(Some("base"), "a"),
                    OrderType::OrderAscending,
                )]),
                group_by: vec![Column::new(Some("base"), "b")],
                limit: 3,
            })
        }

        #[test]
        fn paginate() {
            has_columns_single_parent(
                MirNodeInner::Paginate {
                    order: Some(vec![(
                        Column::new(Some("base"), "a"),
                        OrderType::OrderAscending,
                    )]),
                    group_by: vec![Column::new(Some("base"), "b")],
                    limit: 3,
                },
                vec![
                    Column::new(Some("base"), "a"),
                    Column::new(Some("base"), "b"),
                    Column::named("__page_number"),
                ],
            )
        }

        #[test]
        fn join_aggregates() {
            let base = base1();
            let count = MirNode::new(
                "count".into(),
                0,
                MirNodeInner::Aggregation {
                    on: Column::new(Some("base"), "a"),
                    group_by: vec![Column::new(Some("base"), "b")],
                    output_column: Column::named("count"),
                    kind: Aggregation::Count { count_nulls: false },
                },
                vec![MirNodeRef::downgrade(&base)],
                vec![],
            );
            let sum = MirNode::new(
                "count".into(),
                0,
                MirNodeInner::Aggregation {
                    on: Column::new(Some("base"), "a"),
                    group_by: vec![Column::new(Some("base"), "b")],
                    output_column: Column::named("sum"),
                    kind: Aggregation::Sum,
                },
                vec![MirNodeRef::downgrade(&base)],
                vec![],
            );
            let ja = MirNode::new(
                "join_aggregates".into(),
                0,
                MirNodeInner::JoinAggregates,
                vec![MirNodeRef::downgrade(&count), MirNodeRef::downgrade(&sum)],
                vec![],
            );

            let columns = ja.borrow().columns();
            assert_eq!(
                columns,
                vec![
                    Column::new(Some("base"), "b"),
                    Column::named("count"),
                    Column::named("sum"),
                ]
            )
        }

        #[test]
        fn distinct() {
            has_columns_single_parent(
                MirNodeInner::Distinct {
                    group_by: vec![Column::new(Some("base"), "a")],
                },
                vec![
                    Column::new(Some("base"), "a"),
                    Column::named("__distinct_count"),
                ],
            )
        }
    }

    mod referenced_columns {
        use dataflow::ops::grouped::aggregate::Aggregation;
        use nom_sql::{Expression, Literal};

        use super::*;

        #[test]
        fn project_doesnt_include_literal_and_expr_names() {
            let node = MirNode::new(
                "project".into(),
                0,
                MirNodeInner::Project {
                    emit: vec![Column::new(Some("base"), "project")],
                    expressions: vec![("expr".into(), Expression::Literal(Literal::from(0)))],
                    literals: vec![("literal".into(), 0.into())],
                },
                vec![],
                vec![],
            );
            let referenced = node.borrow().referenced_columns();
            assert_eq!(referenced, vec![Column::new(Some("base"), "project")])
        }

        #[test]
        fn aggregate() {
            let node = MirNode::new(
                "aggregate".into(),
                0,
                MirNodeInner::Aggregation {
                    on: Column::named("on"),
                    group_by: vec![Column::named("gb_a"), Column::named("gb_b")],
                    output_column: Column::named("output"),
                    kind: Aggregation::Count { count_nulls: true },
                },
                vec![],
                vec![],
            );
            let mut referenced = node.borrow().referenced_columns();
            referenced.sort_by(|a, b| a.name.cmp(&b.name));
            assert_eq!(
                referenced,
                vec![
                    Column::named("gb_a"),
                    Column::named("gb_b"),
                    Column::named("on"),
                ]
            );
        }
    }

    mod find_source_for_child_column {
        use nom_sql::{ColumnSpecification, SqlType};

        use crate::node::node_inner::MirNodeInner;
        use crate::node::MirNode;
        use crate::Column;

        // tests the simple case where the child column has no alias, therefore mapping to the
        // parent column with the same name
        #[test]
        fn with_no_alias() {
            let cspec = |n: &str| -> (ColumnSpecification, Option<usize>) {
                (
                    ColumnSpecification::new(nom_sql::Column::from(n), SqlType::Text),
                    None,
                )
            };

            let a = MirNode {
                name: "a".into(),
                from_version: 0,
                inner: MirNodeInner::Base {
                    column_specs: vec![cspec("c1"), cspec("c2"), cspec("c3")],
                    primary_key: Some([Column::from("c1")].into()),
                    unique_keys: Default::default(),
                    adapted_over: None,
                },
                ancestors: vec![],
                children: vec![],
                flow_node: None,
            };

            let child_column = Column::from("c3");

            let idx = a.find_source_for_child_column(&child_column).unwrap();
            assert_eq!(idx, 2);
        }

        // tests the case where the child column has an alias, therefore mapping to the parent
        // column with the same name as the alias
        #[test]
        fn with_alias() {
            let child_column = Column {
                table: Some("table".into()),
                name: "child".into(),
                aliases: vec![Column {
                    table: Some("table".into()),
                    name: "c3".into(),
                    aliases: vec![],
                }],
            };

            let cspec = |n: &str| -> (ColumnSpecification, Option<usize>) {
                (
                    ColumnSpecification::new(
                        nom_sql::Column {
                            name: n.into(),
                            table: Some("table".into()),
                        },
                        SqlType::Text,
                    ),
                    None,
                )
            };

            let a = MirNode {
                name: "a".into(),
                from_version: 0,
                inner: MirNodeInner::Base {
                    column_specs: vec![cspec("c1"), cspec("c2"), cspec("c3")],
                    primary_key: Some([Column::from("c1")].into()),
                    unique_keys: Default::default(),
                    adapted_over: None,
                },
                ancestors: vec![],
                children: vec![],
                flow_node: None,
            };

            let idx = a.find_source_for_child_column(&child_column).unwrap();
            assert_eq!(idx, 2);
        }

        // tests the case where the child column is named the same thing as a parent column BUT has
        // an alias. Typically, this alias would map to a different parent column however
        // for testing purposes that column is missing here to ensure it will not match with
        // the wrong column.
        #[test]
        fn with_alias_to_parent_column() {
            let child_column = Column {
                table: Some("table".into()),
                name: "c1".into(),
                aliases: vec![Column {
                    table: Some("table".into()),
                    name: "other_name".into(),
                    aliases: vec![],
                }],
            };

            let cspec = |n: &str| -> (ColumnSpecification, Option<usize>) {
                (
                    ColumnSpecification::new(nom_sql::Column::from(n), SqlType::Text),
                    None,
                )
            };

            let a = MirNode {
                name: "a".into(),
                from_version: 0,
                inner: MirNodeInner::Base {
                    column_specs: vec![cspec("c1")],
                    primary_key: Some([Column::from("c1")].into()),
                    unique_keys: Default::default(),
                    adapted_over: None,
                },
                ancestors: vec![],
                children: vec![],
                flow_node: None,
            };

            assert_eq!(a.find_source_for_child_column(&child_column), None);
        }
    }
}
