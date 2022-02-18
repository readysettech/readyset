use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::fmt::{Debug, Display, Error, Formatter};
use std::mem;
use std::rc::Rc;

use dataflow::ops;
use dataflow::prelude::ReadySetError;
use nom_sql::analysis::ReferredColumns;
use nom_sql::{ColumnSpecification, SqlIdentifier};
use noria_errors::{internal, internal_err, ReadySetResult};
use petgraph::graph::NodeIndex;
use serde::{Deserialize, Serialize};

pub use self::node_inner::MirNodeInner;
use crate::column::Column;
use crate::{FlowNode, MirNodeRef, MirNodeWeakRef};

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
    pub columns: Vec<Column>,
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
        columns: Vec<Column>,
        inner: MirNodeInner,
        ancestors: Vec<MirNodeWeakRef>,
        children: Vec<MirNodeRef>,
    ) -> MirNodeRef {
        let mn = MirNode {
            name,
            from_version: v,
            columns,
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
                let new_columns: Vec<Column> = new_column_specs
                    .iter()
                    .map(|&(ref cs, _)| Column::from(&cs.column))
                    .collect();

                assert_eq!(
                    new_column_specs.len(),
                    over_node.columns.len() + added_cols.len() - removed_cols.len()
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
                    new_columns,
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
            columns: node.borrow().columns.clone(),
            inner: MirNodeInner::Reuse { node: rcn },
            ancestors: vec![],
            children: vec![],
            flow_node: None, // will be set in `into_flow_parts`
        };

        Rc::new(RefCell::new(mn))
    }

    pub fn can_reuse_as(&self, for_node: &MirNode) -> bool {
        let mut have_all_columns = true;
        for c in &for_node.columns {
            if !self.columns.contains(c) {
                have_all_columns = false;
                break;
            }
        }

        have_all_columns && self.inner.can_reuse_as(&for_node.inner)
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

    /// Add a new column to the set of emitted columns for this node, and return the resulting index
    /// of that column
    pub fn add_column(&mut self, c: Column) -> ReadySetResult<usize> {
        fn column_pos(node: &MirNode) -> Option<usize> {
            match &node.inner {
                MirNodeInner::Aggregation { .. } => {
                    // the aggregation column must always be the last column
                    Some(node.columns.len() - 1)
                }
                MirNodeInner::Paginate { .. } => {
                    // Paginate nodes emit the page number as the last column. If the paginate node
                    // is preceeded by an aggregate, we use the column position given by the
                    // aggregate node, which is guaranteed to be before the page number.
                    #[allow(clippy::unwrap_used)] // Paginate must have an ancestor
                    column_pos(
                        &node
                            .ancestors()
                            .first()
                            .unwrap()
                            .upgrade()
                            .unwrap()
                            .borrow(),
                    )
                    .or(Some(node.columns.len() - 1))
                }
                MirNodeInner::Project { emit, .. } => {
                    // New projected columns go before all literals and expressions
                    Some(emit.len())
                }
                MirNodeInner::Filter { .. } | MirNodeInner::TopK { .. } => {
                    // Filters and topk follow the column positioning rules of their parents
                    #[allow(clippy::unwrap_used)] // filters and topk both must have a parent
                    column_pos(
                        &node
                            .ancestors()
                            .first()
                            .unwrap()
                            .upgrade()
                            .unwrap()
                            .borrow(),
                    )
                }
                _ => None,
            }
        }

        let pos = if let Some(pos) = column_pos(self) {
            self.columns.insert(pos, c.clone());
            pos
        } else {
            self.columns.push(c.clone());
            self.columns.len()
        };

        self.inner.insert_column(c)?;

        Ok(pos)
    }

    pub fn first_ancestor(&self) -> Option<MirNodeRef> {
        self.ancestors().first()?.upgrade()
    }

    pub fn ancestors(&self) -> &[MirNodeWeakRef] {
        self.ancestors.as_slice()
    }

    pub fn children(&self) -> &[MirNodeRef] {
        self.children.as_slice()
    }

    pub fn columns(&self) -> &[Column] {
        self.columns.as_slice()
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
            self.columns.iter().position(|c| c == child)
        } else {
            self.columns.iter().position(|c| child.aliases.contains(c))
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
                    column: c.name.to_string(),
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
                    self.columns.iter().position(|cc| cc.name == c.name)
                } else {
                    self.columns.iter().position(|cc| cc == c)
                }
            } {
                Some(id) => Ok(id),
                None => Err(ReadySetError::NonExistentColumn {
                    column: c.name.to_string(),
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

    pub fn referenced_columns(&self) -> Vec<Column> {
        // all projected columns
        let mut columns = self.columns.clone();

        // + any parent columns referenced internally by the operator
        match self.inner {
            MirNodeInner::Aggregation { ref on, .. } | MirNodeInner::Extremum { ref on, .. } => {
                // need the "over" column
                if !columns.contains(on) {
                    columns.push(on.clone());
                }
            }
            MirNodeInner::Filter { .. } => {
                // Parents are guarenteed to exist.
                #[allow(clippy::unwrap_used)]
                let parent = self.first_ancestor().unwrap();
                // need all parent columns
                for c in parent.borrow().columns() {
                    if !columns.contains(c) {
                        columns.push(c.clone());
                    }
                }
            }
            MirNodeInner::Project {
                ref emit,
                ref expressions,
                ..
            } => {
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
            }
            _ => (),
        }
        columns
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
            self.columns.len()
        )
    }

    /// Clones the [`MirNode`] but leaving the `children` and `ancestors` fields empty.
    #[must_use]
    pub fn clone_without_relations(&self) -> MirNode {
        MirNode {
            name: self.name.clone(),
            from_version: self.from_version,
            columns: self.columns.clone(),
            inner: self.inner.clone(),
            ancestors: Default::default(),
            children: Default::default(),
            flow_node: self.flow_node.clone(),
        }
    }

    /// Return an iterator over all the transitive ancestors of this node in topological order
    ///
    /// This iterator will yield nodes children first, starting at the ancestors of this node (but
    /// not including this node itself).
    pub fn topo_ancestors(&self) -> TopoAncestors {
        TopoAncestors {
            queue: self
                .ancestors()
                .iter()
                .map(|n| n.upgrade().unwrap())
                .collect(),
            in_edge_counts: Default::default(),
        }
    }

    /// Return an iterator over all transitive root ancestors of this node (ancestor nodes which
    /// themselves don't have parents).
    pub fn root_ancestors(&self) -> impl Iterator<Item = MirNodeRef> {
        self.topo_ancestors()
            .filter(|n| n.borrow().ancestors().is_empty())
    }
}

/// An iterator over the transitive ancestors of a node in a MIR graph. Constructed via the
/// [`Node::topo_ancestors`] function
pub struct TopoAncestors {
    queue: VecDeque<MirNodeRef>,
    in_edge_counts: HashMap<String, usize>,
}

impl Iterator for TopoAncestors {
    type Item = MirNodeRef;

    fn next(&mut self) -> Option<Self::Item> {
        self.queue.pop_front().map(|n| {
            for parent in n.borrow().ancestors() {
                let parent = parent.upgrade().unwrap();
                let nd = parent.borrow().versioned_name();
                let in_edges = if let Some(in_edges) = self.in_edge_counts.get(&nd) {
                    *in_edges
                } else {
                    parent.borrow().children().len()
                };

                assert!(in_edges >= 1, "{} has no incoming edges!", nd);
                if in_edges == 1 {
                    self.queue.push_back(parent.clone());
                }
                self.in_edge_counts.insert(nd, in_edges - 1);
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

            let parent_columns = vec![Column::from("c1"), Column::from("c2"), Column::from("c3")];

            let a = MirNode {
                name: "a".into(),
                from_version: 0,
                columns: parent_columns,
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
            assert_eq!(2, idx);
        }

        // tests the case where the child column has an alias, therefore mapping to the parent
        // column with the same name as the alias
        #[test]
        fn with_alias() {
            let c1 = Column {
                table: Some("table".into()),
                name: "c1".into(),
                function: None,
                aliases: vec![],
            };
            let c2 = Column {
                table: Some("table".into()),
                name: "c2".into(),
                function: None,
                aliases: vec![],
            };
            let c3 = Column {
                table: Some("table".into()),
                name: "c3".into(),
                function: None,
                aliases: vec![],
            };

            let child_column = Column {
                table: Some("table".into()),
                name: "child".into(),
                function: None,
                aliases: vec![Column {
                    table: Some("table".into()),
                    name: "c3".into(),
                    function: None,
                    aliases: vec![],
                }],
            };

            let cspec = |n: &str| -> (ColumnSpecification, Option<usize>) {
                (
                    ColumnSpecification::new(nom_sql::Column::from(n), SqlType::Text),
                    None,
                )
            };

            let parent_columns = vec![c1, c2, c3];

            let a = MirNode {
                name: "a".into(),
                from_version: 0,
                columns: parent_columns,
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
            assert_eq!(2, idx);
        }

        // tests the case where the child column is named the same thing as a parent column BUT has
        // an alias. Typically, this alias would map to a different parent column however
        // for testing purposes that column is missing here to ensure it will not match with
        // the wrong column.
        #[test]
        fn with_alias_to_parent_column() {
            let c1 = Column {
                table: Some("table".into()),
                name: "c1".into(),
                function: None,
                aliases: vec![],
            };

            let child_column = Column {
                table: Some("table".into()),
                name: "c1".into(),
                function: None,
                aliases: vec![Column {
                    table: Some("table".into()),
                    name: "other_name".into(),
                    function: None,
                    aliases: vec![],
                }],
            };

            let cspec = |n: &str| -> (ColumnSpecification, Option<usize>) {
                (
                    ColumnSpecification::new(nom_sql::Column::from(n), SqlType::Text),
                    None,
                )
            };

            let parent_columns = vec![c1];

            let a = MirNode {
                name: "a".into(),
                from_version: 0,
                columns: parent_columns,
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

    mod add_column {
        use dataflow::ops::grouped::aggregate::Aggregation as AggregationKind;
        use nom_sql::{BinaryOperator, Expression, Literal};

        use crate::node::node_inner::MirNodeInner;
        use crate::node::MirNode;
        use crate::{Column, MirNodeRef};

        fn setup_filter(cond: (usize, Expression)) -> (MirNodeRef, MirNodeRef) {
            let cols: Vec<nom_sql::Column> = vec!["x".into(), "agg".into()];

            let condition_expression = Expression::BinaryOp {
                lhs: Box::new(Expression::Column(cols[cond.0].clone())),
                op: BinaryOperator::Equal,
                rhs: Box::new(cond.1),
            };

            let parent = MirNode::new(
                "parent".into(),
                0,
                vec!["x".into(), "agg".into()],
                MirNodeInner::Aggregation {
                    on: "z".into(),
                    group_by: vec!["x".into()],
                    kind: AggregationKind::Count { count_nulls: false },
                },
                vec![],
                vec![],
            );

            // σ [x = 1]
            let filter = MirNode::new(
                "filter".into(),
                0,
                vec!["x".into(), "agg".into()],
                MirNodeInner::Filter {
                    conditions: condition_expression,
                },
                vec![MirNodeRef::downgrade(&parent)],
                vec![],
            );

            (filter, parent)
        }

        #[test]
        fn filter_reorders_condition_lhs() {
            let (node, _parent) = setup_filter((1, Expression::Literal(Literal::Integer(1))));

            let condition_expression = Expression::BinaryOp {
                lhs: Box::new(Expression::Column("agg".into())),
                op: BinaryOperator::Equal,
                rhs: Box::new(Expression::Literal(Literal::Integer(1))),
            };

            node.borrow_mut().add_column("y".into()).unwrap();

            assert_eq!(
                node.borrow().columns(),
                vec![Column::from("x"), Column::from("y"), Column::from("agg")]
            );
            match &node.borrow().inner {
                MirNodeInner::Filter { conditions, .. } => {
                    assert_eq!(&condition_expression, conditions);
                }
                _ => unreachable!(),
            };
        }

        #[test]
        fn filter_reorders_condition_comparison_rhs() {
            let (node, _parent) = setup_filter((0, Expression::Column("y".into())));

            let condition_expression = Expression::BinaryOp {
                lhs: Box::new(Expression::Column("x".into())),
                op: BinaryOperator::Equal,
                rhs: Box::new(Expression::Column("y".into())),
            };

            node.borrow_mut().add_column("y".into()).unwrap();

            assert_eq!(
                node.borrow().columns(),
                vec![Column::from("x"), Column::from("y"), Column::from("agg")]
            );
            match &node.borrow().inner {
                MirNodeInner::Filter { conditions, .. } => {
                    assert_eq!(&condition_expression, conditions);
                }
                _ => unreachable!(),
            };
        }

        #[test]
        fn topk_follows_parent_ordering() {
            // count(z) group by (x)
            let parent = MirNode::new(
                "parent".into(),
                0,
                vec!["x".into(), "agg".into()],
                MirNodeInner::Aggregation {
                    on: "z".into(),
                    group_by: vec!["x".into()],
                    kind: AggregationKind::Count { count_nulls: false },
                },
                vec![],
                vec![],
            );

            // TopK γ[x]
            let node = MirNode::new(
                "topk".into(),
                0,
                vec!["x".into(), "agg".into()],
                MirNodeInner::TopK {
                    order: None,
                    group_by: vec!["x".into()],
                    limit: 3,
                },
                vec![MirNodeRef::downgrade(&parent)],
                vec![],
            );

            node.borrow_mut().add_column("y".into()).unwrap();

            assert_eq!(
                node.borrow().columns(),
                vec![Column::from("x"), Column::from("y"), Column::from("agg")]
            );
        }

        #[test]
        fn maintain_computed_and_page_last() {
            // count(z) group by (x)
            let parent = MirNode::new(
                "parent".into(),
                0,
                vec!["x".into(), "agg".into()],
                MirNodeInner::Aggregation {
                    on: "z".into(),
                    group_by: vec!["x".into()],
                    kind: AggregationKind::Count { count_nulls: false },
                },
                vec![],
                vec![],
            );

            // Paginate γ[x]
            let node = MirNode::new(
                "topk".into(),
                0,
                vec!["x".into(), "agg".into(), "page".into()],
                MirNodeInner::Paginate {
                    order: None,
                    group_by: vec!["x".into()],
                    limit: 3,
                },
                vec![MirNodeRef::downgrade(&parent)],
                vec![],
            );

            node.borrow_mut().add_column("y".into()).unwrap();

            assert_eq!(
                node.borrow().columns(),
                vec![
                    Column::from("x"),
                    Column::from("y"),
                    Column::from("agg"),
                    Column::from("page")
                ]
            );
        }
    }
}
