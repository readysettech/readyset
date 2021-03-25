use nom_sql::analysis::ReferredColumns;
use nom_sql::{BinaryOperator, ColumnSpecification, Expression, Literal, OrderType};
use petgraph::graph::NodeIndex;
use std::cell::RefCell;
use std::fmt::{Debug, Display, Error, Formatter};
use std::rc::Rc;

use crate::column::Column;
use crate::{FlowNode, MirNodeRef};
use common::DataType;
use dataflow::ops;
use dataflow::ops::filter::FilterCondition;
use dataflow::ops::grouped::aggregate::Aggregation as AggregationKind;
use dataflow::ops::grouped::extremum::Extremum as ExtremumKind;
use std::collections::HashMap;

/// Helper enum to avoid having separate `make_aggregation_node` and `make_extremum_node` functions
pub enum GroupedNodeType {
    Aggregation(ops::grouped::aggregate::Aggregation),
    Extremum(ops::grouped::extremum::Extremum),
    // Filter Aggregation MIR node type still exists separate from Aggregation for purpose of
    // optimization and rewrite logic.
    // However, the internal operator is the same as a normal aggregation.
    FilterAggregation(ops::grouped::aggregate::Aggregation),
    GroupConcat(String),
}

pub struct MirNode {
    pub name: String,
    pub from_version: usize,
    pub columns: Vec<Column>,
    pub inner: MirNodeType,
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
            columns,
            inner,
            ancestors: ancestors.clone(),
            children: children.clone(),
            flow_node: None,
        };

        let rc_mn = Rc::new(RefCell::new(mn));

        // register as child on ancestors
        for ancestor in &ancestors {
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

                let new_inner = MirNodeType::Base {
                    column_specs: new_column_specs,
                    keys: keys.clone(),
                    adapted_over: Some(BaseNodeAdaptation {
                        over: node.clone(),
                        columns_added: added_cols.into_iter().cloned().collect(),
                        columns_removed: removed_cols.into_iter().cloned().collect(),
                    }),
                };
                MirNode::new(
                    &over_node.name,
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
            inner: MirNodeType::Reuse { node: rcn },
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

    // currently unused
    #[allow(dead_code)]
    pub fn add_ancestor(&mut self, a: MirNodeRef) {
        self.ancestors.push(a)
    }

    pub fn remove_ancestor(&mut self, a: MirNodeRef) {
        match self
            .ancestors
            .iter()
            .position(|x| x.borrow().versioned_name() == a.borrow().versioned_name())
        {
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

    pub fn add_column(&mut self, c: Column) {
        match &self.inner {
            MirNodeType::Aggregation { .. } | MirNodeType::FilterAggregation { .. } => {
                // the aggregation column must always be the last column
                let pos = self.columns.len() - 1;
                self.columns.insert(pos, c.clone());
            }
            MirNodeType::Project { emit, .. } => {
                // New projected columns go before all literals and expressions
                let pos = emit.len();
                self.columns.insert(pos, c.clone())
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

    /// Finds the source of a child column within the node.
    /// This is currently used for locating the source of a projected column.
    pub fn find_source_for_child_column(
        &self,
        child: &Column,
        table_mapping: Option<&HashMap<(String, Option<String>), String>>,
    ) -> Option<usize> {
        // we give the alias preference here because in a query like
        // SELECT table1.column1 AS my_alias
        // my_alias will be the column name and "table1.column1" will be the alias.
        // This is slightly backwards from what intuition suggests when you first look at the
        // column struct but means its the "alias" that will exist in the parent node,
        // not the column name.
        let parent_index = if child.aliases.is_empty() {
            self.columns.iter().position(|c| c == child)
        } else {
            self.columns.iter().position(|c| child.aliases.contains(c))
        };
        // TODO : ideally, we would prioritize the alias when using the table mapping if we are looking
        // for a child column. However, I am not sure this case is totally possible so for now,
        // we are leaving it as is.
        parent_index.or_else(|| self.get_column_id_from_table_mapping(child, table_mapping))
    }

    pub fn column_id_for_column(
        &self,
        c: &Column,
        table_mapping: Option<&HashMap<(String, Option<String>), String>>,
    ) -> usize {
        match self.inner {
            // if we're a base, translate to absolute column ID (taking into account deleted
            // columns). We use the column specifications here, which track a tuple of (column
            // spec, absolute column ID).
            // Note that `rposition` is required because multiple columns of the same name might
            // exist if a column has been removed and re-added. We always use the latest column,
            // and assume that only one column of the same name ever exists at the same time.
            MirNodeType::Base {
                ref column_specs, ..
            } => match column_specs
                .iter()
                .rposition(|cs| Column::from(&cs.0.column) == *c)
            {
                None => panic!(
                    "tried to look up non-existent column {:?} in {}\ncolumn_specs={:?}",
                    c, self.name, column_specs
                ),
                Some(id) => column_specs[id]
                    .1
                    .expect("must have an absolute column ID on base"),
            },
            MirNodeType::Reuse { ref node } => node.borrow().column_id_for_column(c, table_mapping),
            // otherwise, just look up in the column set
            _ => match self.columns.iter().position(|cc| cc == c) {
                Some(id) => id,
                None => self
                    .get_column_id_from_table_mapping(c, table_mapping)
                    .unwrap_or_else(|| {
                        panic!(
                            "tried to look up non-existent column {:?} on node \
                                 \"{}\" (columns: {:?})",
                            c, self.name, self.columns
                        );
                    }),
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

    fn get_column_id_from_table_mapping(
        &self,
        c: &Column,
        table_mapping: Option<&HashMap<(String, Option<String>), String>>,
    ) -> Option<usize> {
        let get_column_index = |c: &Column, t_name: &str| -> Option<usize> {
            let mut ac = c.clone();
            ac.table = Some(t_name.to_owned());
            self.columns.iter().position(|cc| *cc == ac)
        };
        // See if table mapping was passed in
        table_mapping.and_then(|map|
            // if mapping was passed in, then see if c has an associated table, and check
            // the mapping for a key based on this
            match c.table {
                Some(ref table) => {
                    let key = (c.name.clone(), Some(table.clone()));
                    match map.get(&key) {
                        Some(t_name) => get_column_index(c, t_name),
                        None => map.get(&(c.name.clone(), None)).and_then(|t_name| get_column_index(c, t_name)),
                    }
                }
                None => map.get(&(c.name.clone(), None))
                    .and_then(|t_name| get_column_index(c, t_name)),
            }
        )
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
        // all projected columns
        let mut columns = self.columns.clone();

        // + any parent columns referenced internally by the operator
        match self.inner {
            MirNodeType::Aggregation { ref on, .. }
            | MirNodeType::Extremum { ref on, .. }
            | MirNodeType::GroupConcat { ref on, .. } => {
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
            MirNodeType::FilterAggregation { ref on, .. } => {
                let parent = self.ancestors.iter().next().unwrap();
                // need all parent columns
                for c in parent.borrow().columns() {
                    if !columns.contains(&c) {
                        columns.push(c.clone());
                    }
                }
                // need the "over" columns
                if !columns.contains(on) {
                    columns.push(on.clone());
                }
            }
            MirNodeType::Project {
                ref emit,
                ref expressions,
                ..
            } => {
                for c in emit {
                    if !columns.contains(&c) {
                        columns.push(c.clone());
                    }
                }
                for (_, expr) in expressions {
                    for c in expr.referred_columns() {
                        if !columns.iter().any(|col| col == c.as_ref()) {
                            columns.push(c.into_owned().into());
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
    fn description(&self) -> String {
        format!(
            "{}: {} / {} columns",
            self.versioned_name(),
            self.inner.description(),
            self.columns.len()
        )
    }
}

/// Specifies the adapatation of an existing base node by column addition/removal.
/// `over` is a `MirNode` of type `Base`.
pub struct BaseNodeAdaptation {
    pub over: MirNodeRef,
    pub columns_added: Vec<ColumnSpecification>,
    pub columns_removed: Vec<ColumnSpecification>,
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
        conditions: Vec<(usize, FilterCondition)>,
    },
    /// filter condition and grouping
    // FilterAggregation Mir Node type still exists, due to optimization and rewrite logic
    FilterAggregation {
        on: Column,
        else_on: Option<Literal>,
        group_by: Vec<Column>,
        // kind is same as a normal aggregation (sum, count, avg)
        kind: AggregationKind,
        conditions: Vec<(usize, FilterCondition)>,
    },
    /// over column, separator
    GroupConcat {
        on: Column,
        separator: String,
    },
    /// no extra info required
    Identity,
    /// left node, right node, on left columns, on right columns, emit columns
    Join {
        on_left: Vec<Column>,
        on_right: Vec<Column>,
        project: Vec<Column>,
    },
    /// on left column, on right column, emit columns
    LeftJoin {
        on_left: Vec<Column>,
        on_right: Vec<Column>,
        project: Vec<Column>,
    },
    /// group columns
    // currently unused
    #[allow(dead_code)]
    Latest {
        group_by: Vec<Column>,
    },
    /// emit columns
    Project {
        emit: Vec<Column>,
        expressions: Vec<(String, Expression)>,
        literals: Vec<(String, DataType)>,
    },
    /// emit columns
    Union {
        emit: Vec<Vec<Column>>,
    },
    /// order function, group columns, limit k
    TopK {
        order: Option<Vec<(Column, OrderType)>>,
        group_by: Vec<Column>,
        k: usize,
        offset: usize,
    },
    // Get the distinct element sorted by a specific column
    Distinct {
        group_by: Vec<Column>,
    },
    /// reuse another node
    Reuse {
        node: MirNodeRef,
    },
    /// leaf (reader) node, keys
    Leaf {
        node: MirNodeRef,
        keys: Vec<Column>,
        operator: nom_sql::BinaryOperator,
    },
    /// Rewrite node
    Rewrite {
        value: String,
        column: String,
        key: String,
    },
    /// Param Filter node
    ParamFilter {
        col: Column,
        emit_key: Column,
        operator: BinaryOperator,
    },
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
            MirNodeType::Filter { .. } => {}
            MirNodeType::FilterAggregation {
                ref mut group_by, ..
            } => {
                group_by.push(c);
            }
            MirNodeType::Join {
                ref mut project, ..
            }
            | MirNodeType::LeftJoin {
                ref mut project, ..
            } => {
                project.push(c);
            }
            MirNodeType::Project { ref mut emit, .. } => {
                emit.push(c);
            }
            MirNodeType::Union { ref mut emit } => {
                for e in emit.iter_mut() {
                    e.push(c.clone());
                }
            }
            MirNodeType::Distinct {
                ref mut group_by, ..
            } => {
                group_by.push(c);
            }
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
                if let MirNodeType::Reuse { ref node } = *other {
                    // it is, so dig deeper
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
                } else {
                    // handled below
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
                adapted_over: ref our_adapted_over,
            } => {
                match *other {
                    MirNodeType::Base {
                        ref column_specs,
                        ref keys,
                        ..
                    } => {
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
            MirNodeType::FilterAggregation {
                on: ref our_on,
                else_on: ref our_else_on,
                group_by: ref our_group_by,
                kind: ref our_kind,
                conditions: ref our_conditions,
            } => match *other {
                MirNodeType::FilterAggregation {
                    ref on,
                    ref else_on,
                    ref group_by,
                    ref kind,
                    ref conditions,
                } => {
                    our_on == on
                        && our_else_on == else_on
                        && our_group_by == group_by
                        && our_kind == kind
                        && our_conditions == conditions
                }
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
            MirNodeType::LeftJoin {
                on_left: ref our_on_left,
                on_right: ref our_on_right,
                project: ref our_project,
            } => {
                match *other {
                    MirNodeType::LeftJoin {
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
                expressions: ref our_expressions,
            } => match *other {
                MirNodeType::Project {
                    ref emit,
                    ref literals,
                    ref expressions,
                } => our_emit == emit && our_literals == literals && our_expressions == expressions,
                _ => false,
            },
            MirNodeType::Distinct {
                group_by: ref our_group_by,
            } => match *other {
                MirNodeType::Distinct { ref group_by } => group_by == our_group_by,
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
                    order == our_order
                        && group_by == our_group_by
                        && k == our_k
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
            MirNodeType::Union { emit: ref our_emit } => match *other {
                MirNodeType::Union { ref emit } => emit == our_emit,
                _ => false,
            },
            MirNodeType::Rewrite {
                value: ref our_value,
                key: ref our_key,
                column: ref our_col,
            } => match *other {
                MirNodeType::Rewrite {
                    ref value,
                    ref key,
                    ref column,
                } => (value == our_value && our_key == key && our_col == column),
                _ => false,
            },
            MirNodeType::ParamFilter {
                col: ref our_col,
                emit_key: ref our_emit_key,
                operator: ref our_operator,
            } => match *other {
                MirNodeType::ParamFilter {
                    ref col,
                    ref emit_key,
                    ref operator,
                } => (col == our_col && emit_key == our_emit_key && operator == our_operator),
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
                    AggregationKind::AVG => format!("AVG({})", on.name.as_str()),
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
                ..
            } => write!(
                f,
                "B [{}; ⚷: {}]",
                column_specs
                    .iter()
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
                        .filter_map(|(i, ref cond)| match *cond {
                            FilterCondition::Comparison(ref op, ref x) => {
                                Some(format!("f{} {} {:?}", i, escape(&format!("{}", op)), x))
                            }
                            FilterCondition::In(ref xs) => Some(format!(
                                "f{} IN ({})",
                                i,
                                xs.iter()
                                    .map(|d| format!("{}", d))
                                    .collect::<Vec<_>>()
                                    .join(", ")
                            )),
                        })
                        .collect::<Vec<_>>()
                        .as_slice()
                        .join(", ")
                )
            }
            MirNodeType::FilterAggregation {
                ref on,
                else_on: _,
                ref group_by,
                ref kind,
                conditions: _,
            } => {
                let op_string = match *kind {
                    AggregationKind::COUNT => format!("|*|(filter {})", on.name.as_str()),
                    AggregationKind::SUM => format!("𝛴(filter {})", on.name.as_str()),
                    AggregationKind::AVG => format!("Avg(filter {})", on.name.as_str()),
                };
                let group_cols = group_by
                    .iter()
                    .map(|c| c.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "{} γ[{}]", op_string, group_cols)
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
                let key_cols = keys
                    .iter()
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
                ref expressions,
            } => write!(
                f,
                "π [{}]",
                emit.iter()
                    .map(|c| c.name.clone())
                    .chain(
                        expressions
                            .iter()
                            .map(|&(ref n, ref e)| format!("{}: {}", n, e))
                    )
                    .chain(
                        literals
                            .iter()
                            .map(|&(ref n, ref v)| format!("{}: {}", n, v))
                    )
                    .collect::<Vec<_>>()
                    .join(", "),
            ),
            MirNodeType::Reuse { ref node } => write!(
                f,
                "Reuse [{}: {}]",
                node.borrow().versioned_name(),
                node.borrow()
            ),
            MirNodeType::Distinct { ref group_by } => {
                let key_cols = group_by
                    .iter()
                    .map(|k| k.name.clone())
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "Distinct [γ: {}]", key_cols)
            }
            MirNodeType::TopK {
                ref order, ref k, ..
            } => write!(f, "TopK [k: {}, {:?}]", k, order),
            MirNodeType::Union { ref emit } => {
                let cols = emit
                    .iter()
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
            MirNodeType::Rewrite { ref column, .. } => write!(f, "Rw [{}]", column),
            MirNodeType::ParamFilter {
                ref col,
                ref emit_key,
                ref operator,
            } => write!(f, "σφ [{:?}, {:?}, {:?}]", col, emit_key, operator),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::node::{MirNode, MirNodeType};
    use crate::Column;
    use nom_sql::{ColumnSpecification, SqlType};

    // tests the simple case where the child column has no alias, therefore mapping to the parent
    // column with the same name
    #[test]
    fn find_source_for_child_column_with_no_alias() {
        let cspec = |n: &str| -> (ColumnSpecification, Option<usize>) {
            (
                ColumnSpecification::new(nom_sql::Column::from(n), SqlType::Text),
                None,
            )
        };

        let parent_columns = vec![Column::from("c1"), Column::from("c2"), Column::from("c3")];

        let a = MirNode {
            name: "a".to_string(),
            from_version: 0,
            columns: parent_columns,
            inner: MirNodeType::Base {
                column_specs: vec![cspec("c1"), cspec("c2"), cspec("c3")],
                keys: vec![Column::from("c1")],
                adapted_over: None,
            },
            ancestors: vec![],
            children: vec![],
            flow_node: None,
        };

        let child_column = Column::from("c3");

        let idx = a
            .find_source_for_child_column(&child_column, Option::None)
            .unwrap();
        assert_eq!(2, idx);
    }

    // tests the case where the child column has an alias, therefore mapping to the parent
    // column with the same name as the alias
    #[test]
    fn find_source_for_child_column_with_alias() {
        let c1 = Column {
            table: Some("table".to_string()),
            name: "c1".to_string(),
            function: None,
            aliases: vec![],
        };
        let c2 = Column {
            table: Some("table".to_string()),
            name: "c2".to_string(),
            function: None,
            aliases: vec![],
        };
        let c3 = Column {
            table: Some("table".to_string()),
            name: "c3".to_string(),
            function: None,
            aliases: vec![],
        };

        let child_column = Column {
            table: Some("table".to_string()),
            name: "child".to_string(),
            function: None,
            aliases: vec![Column {
                table: Some("table".to_string()),
                name: "c3".to_string(),
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
            name: "a".to_string(),
            from_version: 0,
            columns: parent_columns,
            inner: MirNodeType::Base {
                column_specs: vec![cspec("c1"), cspec("c2"), cspec("c3")],
                keys: vec![Column::from("c1")],
                adapted_over: None,
            },
            ancestors: vec![],
            children: vec![],
            flow_node: None,
        };

        let idx = a
            .find_source_for_child_column(&child_column, Option::None)
            .unwrap();
        assert_eq!(2, idx);
    }

    // tests the case where the child column is named the same thing as a parent column BUT has an alias.
    // Typically, this alias would map to a different parent column however for testing purposes
    // that column is missing here to ensure it will not match with the wrong column.
    #[test]
    #[should_panic]
    fn find_source_for_child_column_with_alias_to_parent_column() {
        let c1 = Column {
            table: Some("table".to_string()),
            name: "c1".to_string(),
            function: None,
            aliases: vec![],
        };

        let child_column = Column {
            table: Some("table".to_string()),
            name: "c1".to_string(),
            function: None,
            aliases: vec![Column {
                table: Some("table".to_string()),
                name: "other_name".to_string(),
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
            name: "a".to_string(),
            from_version: 0,
            columns: parent_columns,
            inner: MirNodeType::Base {
                column_specs: vec![cspec("c1")],
                keys: vec![Column::from("c1")],
                adapted_over: None,
            },
            ancestors: vec![],
            children: vec![],
            flow_node: None,
        };

        a.find_source_for_child_column(&child_column, Option::None);
    }
}
