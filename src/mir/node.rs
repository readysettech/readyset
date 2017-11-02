use nom_sql::{ArithmeticExpression, Column, ColumnSpecification, Operator, OrderType};
use std::cell::RefCell;
use std::fmt::{Debug, Display, Error, Formatter};
use std::rc::Rc;

use flow::Migration;
use core::{DataType, NodeIndex};
use mir::MirNodeRef;
use mir::to_flow::FlowNode;
use mir::to_flow::{adapt_base_node, make_base_node, make_filter_node, make_grouped_node,
                   make_identity_node, make_join_node, make_latest_node, make_project_node,
                   make_topk_node, make_union_node, materialize_leaf_node};
use ops;
use ops::grouped::aggregate::Aggregation as AggregationKind;
use ops::grouped::extremum::Extremum as ExtremumKind;
use ops::join::JoinType;

/// Helper enum to avoid having separate `make_aggregation_node` and `make_extremum_node` functions
pub enum GroupedNodeType {
    Aggregation(ops::grouped::aggregate::Aggregation),
    Extremum(ops::grouped::extremum::Extremum),
    GroupConcat(String),
}

pub struct MirNode {
    pub name: String,
    pub from_version: usize,
    pub columns: Vec<Column>,
    pub(crate) inner: MirNodeType,
    pub ancestors: Vec<MirNodeRef>,
    pub children: Vec<MirNodeRef>,
    pub flow_node: Option<FlowNode>,
}

impl MirNode {
    pub fn new(
        name: &str,
        v: usize,
        columns: Vec<Column>,
        inner: MirNodeType,
        ancestors: Vec<MirNodeRef>,
        children: Vec<MirNodeRef>,
    ) -> MirNodeRef {
        let mn = MirNode {
            name: String::from(name),
            from_version: v,
            columns: columns,
            inner: inner,
            ancestors: ancestors.clone(),
            children: children.clone(),
            flow_node: None,
        };

        let rc_mn = Rc::new(RefCell::new(mn));

        // register as child on ancestors
        for ref ancestor in ancestors {
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
        match over_node.inner {
            MirNodeType::Base {
                ref column_specs,
                ref keys,
                transactional,
                ..
            } => {
                let new_column_specs: Vec<(ColumnSpecification, Option<usize>)> = column_specs
                    .into_iter()
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
                    .map(|&(ref cs, _)| cs.column.clone())
                    .collect();

                assert_eq!(
                    new_column_specs.len(),
                    over_node.columns.len() + added_cols.len() - removed_cols.len()
                );

                let new_inner = MirNodeType::Base {
                    column_specs: new_column_specs,
                    keys: keys.clone(),
                    transactional: transactional,
                    adapted_over: Some(BaseNodeAdaptation {
                        over: node.clone(),
                        columns_added: added_cols.into_iter().cloned().collect(),
                        columns_removed: removed_cols.into_iter().cloned().collect(),
                    }),
                };
                return MirNode::new(
                    &over_node.name,
                    over_node.from_version,
                    new_columns,
                    new_inner,
                    vec![],
                    over_node.children.clone(),
                );
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
            inner: MirNodeType::Reuse { node: rcn },
            ancestors: vec![],
            children: vec![],
            flow_node: None, // will be set in `into_flow_parts`
        };

        let rc_mn = Rc::new(RefCell::new(mn));

        rc_mn
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

    // currently unused
    #[allow(dead_code)]
    pub fn add_ancestor(&mut self, a: MirNodeRef) {
        self.ancestors.push(a)
    }

    pub fn remove_ancestor(&mut self, a: MirNodeRef) {
        match self.ancestors.iter().position(|x| {
            x.borrow().versioned_name() == a.borrow().versioned_name()
        }) {
            None => (),
            Some(idx) => {
                self.ancestors.remove(idx);
            }
        }
    }

    pub fn add_child(&mut self, c: MirNodeRef) {
        self.children.push(c)
    }

    pub fn remove_child(&mut self, a: MirNodeRef) {
        match self.children.iter().position(|x| {
            x.borrow().versioned_name() == a.borrow().versioned_name()
        }) {
            None => (),
            Some(idx) => {
                self.children.remove(idx);
            }
        }
    }

    pub fn add_column(&mut self, c: Column) {
        match self.inner {
            // the aggregation column must always be the last column
            MirNodeType::Aggregation { .. } => {
                let pos = self.columns.len() - 1;
                self.columns.insert(pos, c.clone());
            }
            _ => self.columns.push(c.clone()),
        }
        self.inner.add_column(c);
    }

    pub fn ancestors(&self) -> &[MirNodeRef] {
        self.ancestors.as_slice()
    }

    pub fn children(&self) -> &[MirNodeRef] {
        self.children.as_slice()
    }

    pub fn columns(&self) -> &[Column] {
        self.columns.as_slice()
    }

    pub fn column_id_for_column(&self, c: &Column) -> usize {
        match self.inner {
            // if we're a base, translate to absolute column ID (taking into account deleted
            // columns). We use the column specifications here, which track a tuple of (column
            // spec, absolute column ID).
            // Note that `rposition` is required because multiple columns of the same name might
            // exist if a column has been removed and re-added. We always use the latest column,
            // and assume that only one column of the same name ever exists at the same time.
            MirNodeType::Base {
                ref column_specs, ..
            } => match column_specs.iter().rposition(|cs| cs.0.column == *c) {
                None => panic!(
                    "tried to look up non-existent column {:?} in {}",
                    c,
                    self.name
                ),
                Some(id) => column_specs[id]
                    .1
                    .expect("must have an absolute column ID on base"),
            },
            MirNodeType::Reuse { ref node } => node.borrow().column_id_for_column(c),
            // otherwise, just look up in the column set
            _ => match self.columns
                .iter()
                .position(|cc| cc.name == c.name && cc.table == c.table)
            {
                None => {
                    println!("{:?}, {:?}", c, self.columns);
                    panic!("tried to look up non-existent column {:?}", c.name);
                }
                Some(id) => id,
            },
        }
    }

    pub fn column_specifications(&self) -> &[(ColumnSpecification, Option<usize>)] {
        match self.inner {
            MirNodeType::Base {
                ref column_specs, ..
            } => column_specs.as_slice(),
            _ => panic!("non-base MIR nodes don't have column specifications!"),
        }
    }

    pub fn flow_node_addr(&self) -> Result<NodeIndex, String> {
        match self.flow_node {
            Some(FlowNode::New(na)) | Some(FlowNode::Existing(na)) => Ok(na),
            None => Err(format!(
                "MIR node \"{}\" does not have an associated FlowNode",
                self.versioned_name()
            )),
        }
    }

    #[allow(dead_code)]
    pub fn is_reused(&self) -> bool {
        match self.inner {
            MirNodeType::Reuse { .. } => true,
            _ => false,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn referenced_columns(&self) -> Vec<Column> {
        // all projected columns, minus those with aliases, which we add with their original names
        // below. This is important because we'll otherwise end up searching (and fail to find)
        // aliased columns further up in the MIR graph.
        let mut columns: Vec<Column> = self.columns
            .iter()
            .filter(|c| c.alias.is_none() || c.function.is_some()) // alias ok if computed column
            .cloned()
            .collect();

        // + any parent columns referenced internally by the operator
        match self.inner {
            MirNodeType::Aggregation { ref on, .. } |
            MirNodeType::Extremum { ref on, .. } |
            MirNodeType::GroupConcat { ref on, .. } => {
                // need the "over" column
                if !columns.contains(on) {
                    columns.push(on.clone());
                }
            }
            MirNodeType::Filter { .. } => {
                let parent = self.ancestors.iter().next().unwrap();
                // need all parent columns
                for c in parent.borrow().columns() {
                    if !columns.contains(&c) {
                        columns.push(c.clone());
                    }
                }
            }
            MirNodeType::Project { ref emit, .. } => for c in emit {
                if !columns.contains(&c) {
                    columns.push(c.clone());
                }
            },
            _ => (),
        }
        columns
    }

    pub fn versioned_name(&self) -> String {
        format!("{}_v{}", self.name, self.from_version)
    }

    /// Produce a compact, human-readable description of this node; analogous to the method of the
    /// same name on `Ingredient`.
    fn description(&self) -> String {
        format!(
            "{}: {} / {} columns",
            self.versioned_name(),
            self.inner.description(),
            self.columns.len()
        )
    }

    pub fn into_flow_parts(&mut self, mig: &mut Migration) -> FlowNode {
        let name = self.name.clone();
        match self.flow_node {
            None => {
                let flow_node = match self.inner {
                    MirNodeType::Aggregation {
                        ref on,
                        ref group_by,
                        ref kind,
                    } => {
                        assert_eq!(self.ancestors.len(), 1);
                        let parent = self.ancestors[0].clone();
                        make_grouped_node(
                            &name,
                            parent,
                            self.columns.as_slice(),
                            on,
                            group_by,
                            GroupedNodeType::Aggregation(kind.clone()),
                            mig,
                        )
                    }
                    MirNodeType::Base {
                        ref mut column_specs,
                        ref keys,
                        transactional,
                        ref adapted_over,
                    } => match *adapted_over {
                        None => make_base_node(
                            &name,
                            column_specs.as_mut_slice(),
                            keys,
                            mig,
                            transactional,
                        ),
                        Some(ref bna) => adapt_base_node(
                            bna.over.clone(),
                            mig,
                            column_specs.as_mut_slice(),
                            &bna.columns_added,
                            &bna.columns_removed,
                        ),
                    },
                    MirNodeType::Extremum {
                        ref on,
                        ref group_by,
                        ref kind,
                    } => {
                        assert_eq!(self.ancestors.len(), 1);
                        let parent = self.ancestors[0].clone();
                        make_grouped_node(
                            &name,
                            parent,
                            self.columns.as_slice(),
                            on,
                            group_by,
                            GroupedNodeType::Extremum(kind.clone()),
                            mig,
                        )
                    }
                    MirNodeType::Filter { ref conditions } => {
                        assert_eq!(self.ancestors.len(), 1);
                        let parent = self.ancestors[0].clone();
                        make_filter_node(&name, parent, self.columns.as_slice(), conditions, mig)
                    }
                    MirNodeType::GroupConcat {
                        ref on,
                        ref separator,
                    } => {
                        assert_eq!(self.ancestors.len(), 1);
                        let parent = self.ancestors[0].clone();
                        let group_cols = parent.borrow().columns().iter().cloned().collect();
                        make_grouped_node(
                            &name,
                            parent,
                            self.columns.as_slice(),
                            on,
                            &group_cols,
                            GroupedNodeType::GroupConcat(separator.to_string()),
                            mig,
                        )
                    }
                    MirNodeType::Identity => {
                        assert_eq!(self.ancestors.len(), 1);
                        let parent = self.ancestors[0].clone();
                        make_identity_node(&name, parent, self.columns.as_slice(), mig)
                    }
                    MirNodeType::Join {
                        ref on_left,
                        ref on_right,
                        ref project,
                    } => {
                        assert_eq!(self.ancestors.len(), 2);
                        let left = self.ancestors[0].clone();
                        let right = self.ancestors[1].clone();
                        make_join_node(
                            &name,
                            left,
                            right,
                            self.columns.as_slice(),
                            on_left,
                            on_right,
                            project,
                            JoinType::Inner,
                            mig,
                        )
                    }
                    MirNodeType::Latest { ref group_by } => {
                        assert_eq!(self.ancestors.len(), 1);
                        let parent = self.ancestors[0].clone();
                        make_latest_node(&name, parent, self.columns.as_slice(), group_by, mig)
                    }
                    MirNodeType::Leaf { ref keys, .. } => {
                        assert_eq!(self.ancestors.len(), 1);
                        let parent = self.ancestors[0].clone();
                        materialize_leaf_node(&parent, name, keys, mig);
                        // TODO(malte): below is yucky, but required to satisfy the type system:
                        // each match arm must return a `FlowNode`, so we use the parent's one
                        // here.
                        let node = match *parent.borrow().flow_node.as_ref().unwrap() {
                            FlowNode::New(na) => FlowNode::Existing(na),
                            ref n @ FlowNode::Existing(..) => n.clone(),
                        };
                        node
                    }
                    MirNodeType::LeftJoin {
                        ref on_left,
                        ref on_right,
                        ref project,
                    } => {
                        assert_eq!(self.ancestors.len(), 2);
                        let left = self.ancestors[0].clone();
                        let right = self.ancestors[1].clone();
                        make_join_node(
                            &name,
                            left,
                            right,
                            self.columns.as_slice(),
                            on_left,
                            on_right,
                            project,
                            JoinType::Left,
                            mig,
                        )
                    }
                    MirNodeType::Project {
                        ref emit,
                        ref literals,
                        ref arithmetic,
                    } => {
                        assert_eq!(self.ancestors.len(), 1);
                        let parent = self.ancestors[0].clone();
                        make_project_node(
                            &name,
                            parent,
                            self.columns.as_slice(),
                            emit,
                            arithmetic,
                            literals,
                            mig,
                        )
                    }
                    MirNodeType::Reuse { ref node } => {
                        match *node.borrow()
                           .flow_node
                           .as_ref()
                           .expect("Reused MirNode must have FlowNode") {
                               // "New" => flow node was originally created for the node that we
                               // are reusing
                               FlowNode::New(na) |
                               // "Existing" => flow node was already reused from some other
                               // MIR node
                               FlowNode::Existing(na) => FlowNode::Existing(na),
                        }
                    }
                    MirNodeType::Union { ref emit } => {
                        assert_eq!(self.ancestors.len(), emit.len());
                        make_union_node(&name, self.columns.as_slice(), emit, self.ancestors(), mig)
                    }
                    MirNodeType::TopK {
                        ref order,
                        ref group_by,
                        ref k,
                        ref offset,
                    } => {
                        assert_eq!(self.ancestors.len(), 1);
                        let parent = self.ancestors[0].clone();
                        make_topk_node(
                            &name,
                            parent,
                            self.columns.as_slice(),
                            order,
                            group_by,
                            *k,
                            *offset,
                            mig,
                        )
                    }
                };

                // any new flow nodes have been instantiated by now, so we replace them with
                // existing ones, but still return `FlowNode::New` below in order to notify higher
                // layers of the new nodes.
                self.flow_node = match flow_node {
                    FlowNode::New(na) => Some(FlowNode::Existing(na)),
                    ref n @ FlowNode::Existing(..) => Some(n.clone()),
                };
                flow_node
            }
            Some(ref flow_node) => flow_node.clone(),
        }
    }
}

/// Specifies the adapatation of an existing base node by column addition/removal.
/// `over` is a `MirNode` of type `Base`.
pub struct BaseNodeAdaptation {
    over: MirNodeRef,
    columns_added: Vec<ColumnSpecification>,
    columns_removed: Vec<ColumnSpecification>,
}

pub enum MirNodeType {
    /// over column, group_by columns
    Aggregation {
        on: Column,
        group_by: Vec<Column>,
        kind: AggregationKind,
    },
    /// column specifications, keys (non-compound), tx flag, adapted base
    Base {
        column_specs: Vec<(ColumnSpecification, Option<usize>)>,
        keys: Vec<Column>,
        transactional: bool,
        adapted_over: Option<BaseNodeAdaptation>,
    },
    /// over column, group_by columns
    Extremum {
        on: Column,
        group_by: Vec<Column>,
        kind: ExtremumKind,
    },
    /// filter conditions (one for each parent column)
    Filter {
        conditions: Vec<Option<(Operator, DataType)>>,
    },
    /// over column, separator
    GroupConcat { on: Column, separator: String },
    /// no extra info required
    Identity,
    /// left node, right node, on left columns, on right columns, emit columns
    Join {
        on_left: Vec<Column>,
        on_right: Vec<Column>,
        project: Vec<Column>,
    },
    /// on left column, on right column, emit columns
    // currently unused
    #[allow(dead_code)]
    LeftJoin {
        on_left: Vec<Column>,
        on_right: Vec<Column>,
        project: Vec<Column>,
    },
    /// group columns
    // currently unused
    #[allow(dead_code)]
    Latest { group_by: Vec<Column> },
    /// emit columns
    Project {
        emit: Vec<Column>,
        arithmetic: Vec<(String, ArithmeticExpression)>,
        literals: Vec<(String, DataType)>,
    },
    /// emit columns
    Union { emit: Vec<Vec<Column>> },
    /// order function, group columns, k
    TopK {
        order: Option<Vec<(Column, OrderType)>>,
        group_by: Vec<Column>,
        k: usize,
        offset: usize,
    },
    /// reuse another node
    Reuse { node: MirNodeRef },
    /// leaf (reader) node, keys
    Leaf { node: MirNodeRef, keys: Vec<Column> },
}

impl MirNodeType {
    fn description(&self) -> String {
        format!("{:?}", self)
    }

    fn add_column(&mut self, c: Column) {
        match *self {
            MirNodeType::Aggregation {
                ref mut group_by, ..
            } => {
                group_by.push(c);
            }
            MirNodeType::Base { .. } => panic!("can't add columns to base nodes!"),
            MirNodeType::Extremum {
                ref mut group_by, ..
            } => {
                group_by.push(c);
            }
            MirNodeType::Filter { ref mut conditions } => {
                conditions.push(None);
            }
            MirNodeType::Join {
                ref mut project, ..
            } |
            MirNodeType::LeftJoin {
                ref mut project, ..
            } => {
                project.push(c);
            }
            MirNodeType::Project { ref mut emit, .. } => {
                emit.push(c);
            }
            MirNodeType::Union { .. } => unimplemented!(),
            MirNodeType::TopK {
                ref mut group_by, ..
            } => {
                group_by.push(c);
            }
            _ => (),
        }
    }

    fn can_reuse_as(&self, other: &MirNodeType) -> bool {
        match *self {
            MirNodeType::Reuse { .. } => (), // handled below
            _ => {
                // we're not a `Reuse` ourselves, but the other side might be
                match *other {
                    // it is, so dig deeper
                    MirNodeType::Reuse { ref node } => {
                        // this does not check the projected columns of the inner node for two
                        // reasons:
                        // 1) our own projected columns aren't accessible on `MirNodeType`, but
                        //    only on the outer `MirNode`, which isn't accessible here; but more
                        //    importantly
                        // 2) since this is already a node reuse, the inner, reused node must have
                        //    *at least* a superset of our own (inaccessible) projected columns.
                        // Hence, it is sufficient to check the projected columns on the parent
                        // `MirNode`, and if that check passes, it also holds for the nodes reused
                        // here.
                        return self.can_reuse_as(&node.borrow().inner);
                    }
                    _ => (), // handled below
                }
            }
        }

        match *self {
            MirNodeType::Aggregation {
                on: ref our_on,
                group_by: ref our_group_by,
                kind: ref our_kind,
            } => {
                match *other {
                    MirNodeType::Aggregation {
                        ref on,
                        ref group_by,
                        ref kind,
                    } => {
                        // TODO(malte): this is stricter than it needs to be, as it could cover
                        // COUNT-as-SUM-style relationships.
                        our_on == on && our_group_by == group_by && our_kind == kind
                    }
                    _ => false,
                }
            }
            MirNodeType::Base {
                column_specs: ref our_column_specs,
                keys: ref our_keys,
                transactional: our_transactional,
                adapted_over: ref our_adapted_over,
            } => {
                match *other {
                    MirNodeType::Base {
                        ref column_specs,
                        ref keys,
                        transactional,
                        ..
                    } => {
                        assert_eq!(our_transactional, transactional);
                        // if we are instructed to adapt an earlier base node, we cannot reuse
                        // anything directly; we'll have to keep a new MIR node here.
                        if our_adapted_over.is_some() {
                            // TODO(malte): this is a bit more conservative than it needs to be,
                            // since base node adaptation actually *changes* the underlying base
                            // node, so we will actually reuse. However, returning false here
                            // terminates the reuse search unnecessarily. We should handle this
                            // special case.
                            return false;
                        }
                        // note that as long as we are not adapting a previous base node,
                        // we do *not* need `adapted_over` to *match*, since current reuse
                        // does not depend on how base node was created from an earlier one
                        our_column_specs == column_specs && our_keys == keys
                    }
                    _ => false,
                }
            }
            MirNodeType::Extremum {
                on: ref our_on,
                group_by: ref our_group_by,
                kind: ref our_kind,
            } => match *other {
                MirNodeType::Extremum {
                    ref on,
                    ref group_by,
                    ref kind,
                } => our_on == on && our_group_by == group_by && our_kind == kind,
                _ => false,
            },
            MirNodeType::Filter {
                conditions: ref our_conditions,
            } => match *other {
                MirNodeType::Filter { ref conditions } => our_conditions == conditions,
                _ => false,
            },
            MirNodeType::Join {
                on_left: ref our_on_left,
                on_right: ref our_on_right,
                project: ref our_project,
            } => {
                match *other {
                    MirNodeType::Join {
                        ref on_left,
                        ref on_right,
                        ref project,
                    } => {
                        // TODO(malte): column order does not actually need to match, but this only
                        // succeeds if it does.
                        our_on_left == on_left && our_on_right == on_right && our_project == project
                    }
                    _ => false,
                }
            }
            MirNodeType::Project {
                emit: ref our_emit,
                literals: ref our_literals,
                arithmetic: ref our_arithmetic,
            } => match *other {
                MirNodeType::Project {
                    ref emit,
                    ref literals,
                    ref arithmetic,
                } => our_emit == emit && our_literals == literals && our_arithmetic == arithmetic,
                _ => false,
            },
            MirNodeType::Reuse { node: ref us } => {
                match *other {
                    // both nodes are `Reuse` nodes, so we simply compare the both sides' reuse
                    // target
                    MirNodeType::Reuse { ref node } => us.borrow().can_reuse_as(&*node.borrow()),
                    // we're a `Reuse`, the other side isn't, so see if our reuse target's `inner`
                    // can be reused for the other side. It's sufficient to check the target's
                    // `inner` because reuse implies that our target has at least a superset of our
                    // projected columns (see earlier comment).
                    _ => us.borrow().inner.can_reuse_as(other),
                }
            }
            MirNodeType::TopK {
                order: ref our_order,
                group_by: ref our_group_by,
                k: our_k,
                offset: our_offset,
            } => match *other {
                MirNodeType::TopK {
                    ref order,
                    ref group_by,
                    k,
                    offset,
                } => {
                    order == our_order && group_by == our_group_by && k == our_k
                        && offset == our_offset
                }
                _ => false,
            },
            MirNodeType::Leaf {
                keys: ref our_keys, ..
            } => match *other {
                MirNodeType::Leaf { ref keys, .. } => keys == our_keys,
                _ => false,
            },
            _ => unimplemented!(),
        }
    }
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

impl Debug for MirNodeType {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        match *self {
            MirNodeType::Aggregation {
                ref on,
                ref group_by,
                ref kind,
            } => {
                let op_string = match *kind {
                    AggregationKind::COUNT => format!("|*|({})", on.name.as_str()),
                    AggregationKind::SUM => format!("𝛴({})", on.name.as_str()),
                };
                let group_cols = group_by
                    .iter()
                    .map(|c| c.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "{} γ[{}]", op_string, group_cols)
            }
            MirNodeType::Base {
                ref column_specs,
                ref keys,
                transactional,
                ..
            } => write!(
                f,
                "B{} [{}; ⚷: {}]",
                if transactional { "*" } else { "" },
                column_specs
                    .into_iter()
                    .map(|&(ref cs, _)| cs.column.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", "),
                keys.iter()
                    .map(|c| c.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            MirNodeType::Extremum {
                ref on,
                ref group_by,
                ref kind,
            } => {
                let op_string = match *kind {
                    ExtremumKind::MIN => format!("min({})", on.name.as_str()),
                    ExtremumKind::MAX => format!("max({})", on.name.as_str()),
                };
                let group_cols = group_by
                    .iter()
                    .map(|c| c.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "{} γ[{}]", op_string, group_cols)
            }
            MirNodeType::Filter { ref conditions } => {
                use regex::Regex;

                let escape = |s: &str| {
                    Regex::new("([<>])")
                        .unwrap()
                        .replace_all(s, "\\$1")
                        .to_string()
                };
                write!(
                    f,
                    "σ[{}]",
                    conditions
                        .iter()
                        .enumerate()
                        .filter_map(|(i, ref e)| match e.as_ref() {
                            Some(&(ref op, ref x)) => {
                                Some(format!("f{} {} {}", i, escape(&format!("{}", op)), x))
                            }
                            None => None,
                        })
                        .collect::<Vec<_>>()
                        .as_slice()
                        .join(", ")
                )
            }
            MirNodeType::GroupConcat {
                ref on,
                ref separator,
            } => write!(f, "||([{}], \"{}\")", on.name, separator),
            MirNodeType::Identity => write!(f, "≡"),
            MirNodeType::Join {
                ref on_left,
                ref on_right,
                ref project,
            } => {
                let jc = on_left
                    .iter()
                    .zip(on_right)
                    .map(|(l, r)| format!("{}:{}", l.name, r.name))
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(
                    f,
                    "⋈ [{} on {}]",
                    project
                        .iter()
                        .map(|c| c.name.as_str())
                        .collect::<Vec<_>>()
                        .join(", "),
                    jc
                )
            }
            MirNodeType::Leaf { ref keys, .. } => {
                let key_cols = keys.iter()
                    .map(|k| k.name.clone())
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "Leaf [⚷: {}]", key_cols)
            }
            MirNodeType::LeftJoin {
                ref on_left,
                ref on_right,
                ref project,
            } => {
                let jc = on_left
                    .iter()
                    .zip(on_right)
                    .map(|(l, r)| format!("{}:{}", l.name, r.name))
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(
                    f,
                    "⋉ [{} on {}]",
                    project
                        .iter()
                        .map(|c| c.name.as_str())
                        .collect::<Vec<_>>()
                        .join(", "),
                    jc
                )
            }
            MirNodeType::Latest { ref group_by } => {
                let key_cols = group_by
                    .iter()
                    .map(|k| k.name.clone())
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "⧖ γ[{}]", key_cols)
            }
            MirNodeType::Project {
                ref emit,
                ref literals,
                ref arithmetic,
            } => write!(
                f,
                "π [{}{}{}]",
                emit.iter()
                    .map(|c| c.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", "),
                if arithmetic.is_empty() {
                    format!("")
                } else {
                    format!(
                        ", {}",
                        arithmetic
                            .iter()
                            .map(|&(ref n, ref e)| format!("{}: {:?}", n, e))
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                },
                if literals.is_empty() {
                    format!("")
                } else {
                    format!(
                        ", lit: {}",
                        literals
                            .iter()
                            .map(|&(ref n, ref v)| format!("{}: {}", n, v))
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                },
            ),
            MirNodeType::Reuse { ref node } => write!(
                f,
                "Reuse [{}: {}]",
                node.borrow().versioned_name(),
                node.borrow()
            ),
            MirNodeType::TopK {
                ref order, ref k, ..
            } => write!(f, "TopK [k: {}, {:?}]", k, order),
            MirNodeType::Union { ref emit } => {
                let cols = emit.iter()
                    .map(|c| {
                        c.iter()
                            .map(|e| e.name.clone())
                            .collect::<Vec<_>>()
                            .join(", ")
                    })
                    .collect::<Vec<_>>()
                    .join(" ⋃ ");

                write!(f, "{}", cols)
            }
        }
    }
}
