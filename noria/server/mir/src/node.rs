use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::{Debug, Display, Error, Formatter};
use std::rc::Rc;

use petgraph::graph::NodeIndex;

use dataflow::ops;
use node_inner::MirNodeInner;
use nom_sql::analysis::ReferredColumns;
use nom_sql::ColumnSpecification;

use crate::{FlowNode, MirNodeRef};
use crate::column::Column;

pub mod node_inner;

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
    pub inner: MirNodeInner,
    pub ancestors: Vec<MirNodeRef>,
    pub children: Vec<MirNodeRef>,
    pub flow_node: Option<FlowNode>,
}

impl MirNode {
    pub fn new(
        name: &str,
        v: usize,
        columns: Vec<Column>,
        inner: MirNodeInner,
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
            MirNodeInner::Base {
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

                let new_inner = MirNodeInner::Base {
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

    /// Add a new column to the set of emitted columns for this node, and return the resulting index
    /// of that column
    pub fn add_column(&mut self, c: Column) -> usize {
        fn column_pos(node: &MirNode) -> Option<usize> {
            match &node.inner {
                MirNodeInner::Aggregation { .. } | MirNodeInner::FilterAggregation { .. } => {
                    // the aggregation column must always be the last column
                    Some(node.columns.len() - 1)
                }
                MirNodeInner::Project { emit, .. } => {
                    // New projected columns go before all literals and expressions
                    Some(emit.len())
                }
                MirNodeInner::Filter { .. } => {
                    // Filters follow the column positioning rules of their parents
                    // unwrap: filters must have a parent
                    column_pos(&node.ancestors().first().unwrap().borrow())
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

        self.inner.insert_column(pos, c);

        pos
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
            MirNodeInner::Base {
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
            MirNodeInner::Reuse { ref node } => {
                node.borrow().column_id_for_column(c, table_mapping)
            }
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
            MirNodeInner::Base {
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
            MirNodeInner::Reuse { .. } => true,
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
            MirNodeInner::Aggregation { ref on, .. }
            | MirNodeInner::Extremum { ref on, .. }
            | MirNodeInner::GroupConcat { ref on, .. } => {
                // need the "over" column
                if !columns.contains(on) {
                    columns.push(on.clone());
                }
            }
            MirNodeInner::Filter { .. } => {
                let parent = self.ancestors.iter().next().unwrap();
                // need all parent columns
                for c in parent.borrow().columns() {
                    if !columns.contains(&c) {
                        columns.push(c.clone());
                    }
                }
            }
            MirNodeInner::FilterAggregation { ref on, .. } => {
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
            MirNodeInner::Project {
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

#[cfg(test)]
mod tests {
    use super::*;

    mod find_source_for_child_column {
        use nom_sql::{ColumnSpecification, SqlType};

        use crate::Column;
        use crate::node::MirNode;
        use crate::node::node_inner::MirNodeInner;

        // tests the simple case where the child column has no alias, therefore mapping to the parent
        // column with the same name
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
                name: "a".to_string(),
                from_version: 0,
                columns: parent_columns,
                inner: MirNodeInner::Base {
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
        fn with_alias() {
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
                inner: MirNodeInner::Base {
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
        fn with_alias_to_parent_column() {
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
                inner: MirNodeInner::Base {
                    column_specs: vec![cspec("c1")],
                    keys: vec![Column::from("c1")],
                    adapted_over: None,
                },
                ancestors: vec![],
                children: vec![],
                flow_node: None,
            };

            assert_eq!(
                a.find_source_for_child_column(&child_column, Option::None),
                None
            );
        }
    }

    mod add_column {
        use dataflow::ops::filter::FilterCondition;
        use dataflow::ops::filter::Value;
        use dataflow::ops::grouped::aggregate::Aggregation as AggregationKind;
        use nom_sql::BinaryOperator;

        use super::*;

        fn setup_filter(cond: (usize, FilterCondition)) -> MirNodeRef {
            let parent = MirNode::new(
                "parent",
                0,
                vec!["x".into(), "agg".into()],
                MirNodeInner::Aggregation {
                    on: "z".into(),
                    group_by: vec!["x".into()],
                    kind: AggregationKind::COUNT,
                },
                vec![],
                vec![],
            );

            // σ [x = 1]
            MirNode::new(
                "filter",
                0,
                vec!["x".into(), "agg".into()],
                MirNodeInner::Filter {
                    conditions: vec![cond],
                },
                vec![parent],
                vec![],
            )
        }

        #[test]
        fn filter_reorders_condition_lhs() {
            let node = setup_filter((
                1,
                FilterCondition::Comparison(BinaryOperator::Equal, Value::Constant(1.into())),
            ));

            node.borrow_mut().add_column("y".into());

            assert_eq!(
                node.borrow().columns(),
                vec![Column::from("x"), Column::from("y"), Column::from("agg")]
            );
            match &node.borrow().inner {
                MirNodeInner::Filter { conditions } => {
                    assert_eq!(conditions[0].0, 2);
                }
                _ => unreachable!(),
            };
        }

        #[test]
        fn filter_reorders_condition_comparison_rhs() {
            let node = setup_filter((
                0,
                FilterCondition::Comparison(BinaryOperator::Equal, Value::Column(1)),
            ));

            node.borrow_mut().add_column("y".into());

            assert_eq!(
                node.borrow().columns(),
                vec![Column::from("x"), Column::from("y"), Column::from("agg")]
            );
            match &node.borrow().inner {
                MirNodeInner::Filter { conditions } => {
                    assert_eq!(
                        conditions[0].1,
                        FilterCondition::Comparison(BinaryOperator::Equal, Value::Column(2))
                    );
                }
                _ => unreachable!(),
            };
        }
    }
}
