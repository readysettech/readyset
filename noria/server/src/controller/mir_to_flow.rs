#![deny(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::unimplemented,
    clippy::unreachable
)]

use std::collections::HashMap;
use std::convert::TryInto;

use common::DataType;
use dataflow::node::Column as DataflowColumn;
use dataflow::ops::grouped::concat::GroupConcat;
use dataflow::ops::join::{Join, JoinType};
use dataflow::ops::latest::Latest;
use dataflow::ops::project::Project;
use dataflow::post_lookup::{PostLookup, PostLookupAggregates};
use dataflow::{node, ops, BuiltinFunction, Expression as DataflowExpression};
use launchpad::redacted::Sensitive;
use mir::node::node_inner::MirNodeInner;
use mir::node::{GroupedNodeType, MirNode};
use mir::query::{MirQuery, QueryFlowParts};
use mir::{Column, FlowNode, MirNodeRef};
use nom_sql::{
    BinaryOperator, ColumnConstraint, ColumnSpecification, Expression, FunctionExpression, InValue,
    OrderType, SqlIdentifier, SqlType, UnaryOperator,
};
use noria::internal::{Index, IndexType};
use noria::ViewPlaceholder;
use noria_errors::{
    internal, internal_err, invariant, invariant_eq, unsupported, ReadySetError, ReadySetResult,
};
use petgraph::graph::NodeIndex;

use crate::controller::Migration;
use crate::manual::ops::grouped::aggregate::Aggregation;

/// Sets the names of dataflow columns using the names determined in MIR to ensure aliases are used
fn set_names(names: &[&str], columns: &mut [DataflowColumn]) -> ReadySetResult<()> {
    invariant_eq!(columns.len(), names.len());
    for (c, n) in columns.iter_mut().zip(names.iter()) {
        c.set_name((*n).into());
    }
    Ok(())
}

pub(super) fn mir_query_to_flow_parts(
    mir_query: &mut MirQuery,
    mig: &mut Migration<'_>,
) -> ReadySetResult<QueryFlowParts> {
    use std::collections::VecDeque;

    let mut new_nodes = Vec::new();
    let mut reused_nodes = Vec::new();

    // starting at the roots, add nodes in topological order
    let mut node_queue = VecDeque::new();
    node_queue.extend(mir_query.roots.iter().cloned());
    let mut in_edge_counts = HashMap::new();
    for n in &node_queue {
        in_edge_counts.insert(n.borrow().versioned_name(), 0);
    }
    while let Some(n) = node_queue.pop_front() {
        let edge_counts = in_edge_counts
            .get(&n.borrow().versioned_name())
            .ok_or_else(|| {
                internal_err(format!(
                    "no in_edge_counts for {}",
                    n.borrow().versioned_name()
                ))
            })?;
        invariant_eq!(*edge_counts, 0);
        let (name, from_version) = {
            let n = n.borrow_mut();
            (n.name.clone(), n.from_version)
        };
        let flow_node = mir_node_to_flow_parts(&mut n.borrow_mut(), mig).map_err(|e| {
            ReadySetError::MirNodeCreationFailed {
                name: name.to_string(),
                from_version,
                source: Box::new(e),
            }
        })?;
        match flow_node {
            FlowNode::New(na) => new_nodes.push(na),
            FlowNode::Existing(na) => reused_nodes.push(na),
        }
        for child in n.borrow().children.iter() {
            let nd = child.borrow().versioned_name();
            let in_edges = if let Some(ine) = in_edge_counts.get(&nd) {
                *ine
            } else {
                child.borrow().ancestors.len()
            };
            invariant!(in_edges >= 1);
            if in_edges == 1 {
                // last edge removed
                node_queue.push_back(child.clone());
            }
            in_edge_counts.insert(nd, in_edges - 1);
        }
    }
    let leaf_na = mir_query
        .leaf
        .borrow()
        .flow_node
        .as_ref()
        .ok_or_else(|| internal_err("Leaf must have FlowNode by now"))?
        .address();

    Ok(QueryFlowParts {
        name: mir_query.name.clone(),
        new_nodes,
        reused_nodes,
        query_leaf: leaf_na,
    })
}

fn mir_node_to_flow_parts(
    mir_node: &mut MirNode,
    mig: &mut Migration<'_>,
) -> ReadySetResult<FlowNode> {
    let name = mir_node.name.clone();
    match mir_node.flow_node {
        None => {
            #[allow(clippy::let_and_return)]
            let flow_node = match mir_node.inner {
                MirNodeInner::Aggregation {
                    ref on,
                    ref group_by,
                    ref kind,
                    ..
                } => {
                    invariant_eq!(mir_node.ancestors.len(), 1);
                    #[allow(clippy::unwrap_used)] // checked by above invariant
                    let parent = mir_node.first_ancestor().unwrap();
                    make_grouped_node(
                        &name,
                        parent,
                        &mir_node.columns(),
                        on,
                        group_by,
                        GroupedNodeType::Aggregation(kind.clone()),
                        mig,
                    )?
                }
                MirNodeInner::Base {
                    ref mut column_specs,
                    ref primary_key,
                    ref unique_keys,
                    ref adapted_over,
                } => match adapted_over {
                    None => make_base_node(
                        &name,
                        column_specs.as_mut_slice(),
                        primary_key.as_deref(),
                        unique_keys,
                        mig,
                    )?,
                    Some(ref bna) => adapt_base_node(
                        bna.over.clone(),
                        mig,
                        column_specs.as_mut_slice(),
                        &bna.columns_added,
                        &bna.columns_removed,
                    )?,
                },
                MirNodeInner::Extremum {
                    ref on,
                    ref group_by,
                    ref kind,
                    ..
                } => {
                    invariant_eq!(mir_node.ancestors.len(), 1);
                    #[allow(clippy::unwrap_used)] // checked by above invariant
                    let parent = mir_node.first_ancestor().unwrap();
                    make_grouped_node(
                        &name,
                        parent,
                        &mir_node.columns(),
                        on,
                        group_by,
                        GroupedNodeType::Extremum(kind.clone()),
                        mig,
                    )?
                }
                MirNodeInner::Filter { ref conditions } => {
                    invariant_eq!(mir_node.ancestors.len(), 1);
                    #[allow(clippy::unwrap_used)] // checked by above invariant
                    let parent = mir_node.first_ancestor().unwrap();
                    make_filter_node(&name, parent, &mir_node.columns(), conditions.clone(), mig)?
                }
                MirNodeInner::Identity => {
                    invariant_eq!(mir_node.ancestors.len(), 1);
                    #[allow(clippy::unwrap_used)] // checked by above invariant
                    let parent = mir_node.first_ancestor().unwrap();
                    make_identity_node(&name, parent, &mir_node.columns(), mig)?
                }
                MirNodeInner::Join {
                    ref on_left,
                    ref on_right,
                    ref project,
                } => {
                    invariant_eq!(mir_node.ancestors.len(), 2);
                    #[allow(clippy::indexing_slicing, clippy::unwrap_used)]
                    let left = mir_node.ancestors[0].upgrade().unwrap();
                    #[allow(clippy::indexing_slicing, clippy::unwrap_used)]
                    let right = mir_node.ancestors[1].upgrade().unwrap();
                    make_join_node(
                        &name,
                        left,
                        right,
                        &mir_node.columns(),
                        on_left,
                        on_right,
                        project,
                        JoinType::Inner,
                        mig,
                    )?
                }
                MirNodeInner::JoinAggregates => {
                    invariant_eq!(mir_node.ancestors.len(), 2);
                    #[allow(clippy::indexing_slicing, clippy::unwrap_used)]
                    let left = mir_node.ancestors[0].upgrade().unwrap();
                    #[allow(clippy::indexing_slicing, clippy::unwrap_used)]
                    let right = mir_node.ancestors[1].upgrade().unwrap();
                    make_join_aggregates_node(&name, left, right, &mir_node.columns(), mig)?
                }
                MirNodeInner::DependentJoin { .. } => {
                    // See the docstring for MirNodeInner::DependentJoin
                    internal!("Encountered dependent join when lowering to dataflow")
                }
                MirNodeInner::Latest { ref group_by } => {
                    invariant_eq!(mir_node.ancestors.len(), 1);
                    #[allow(clippy::unwrap_used)] // checked by above invariant
                    let parent = mir_node.first_ancestor().unwrap();
                    make_latest_node(&name, parent, &mir_node.columns(), group_by, mig)?
                }
                MirNodeInner::Leaf {
                    ref keys,
                    index_type,
                    ref order_by,
                    limit,
                    ref returned_cols,
                    ref default_row,
                    ref aggregates,
                    ..
                } => {
                    invariant_eq!(mir_node.ancestors.len(), 1);
                    #[allow(clippy::unwrap_used)] // checked by above invariant
                    let parent = mir_node.first_ancestor().unwrap();
                    let post_lookup = make_post_lookup(
                        &parent,
                        order_by,
                        limit,
                        returned_cols,
                        default_row.clone(),
                        aggregates,
                    )?;
                    materialize_leaf_node(&parent, name, keys, index_type, post_lookup, mig)?;
                    // TODO(malte): below is yucky, but required to satisfy the type system:
                    // each match arm must return a `FlowNode`, so we use the parent's one
                    // here.
                    let node = match *parent.borrow().flow_node.as_ref().ok_or_else(|| {
                        internal_err("parent of a Leaf mirnodeinner had no flow_node")
                    })? {
                        FlowNode::New(na) => FlowNode::Existing(na),
                        n @ FlowNode::Existing(..) => n,
                    };
                    node
                }
                MirNodeInner::LeftJoin {
                    ref on_left,
                    ref on_right,
                    ref project,
                } => {
                    invariant_eq!(mir_node.ancestors.len(), 2);
                    #[allow(clippy::indexing_slicing, clippy::unwrap_used)]
                    let left = mir_node.ancestors[0].upgrade().unwrap();
                    #[allow(clippy::indexing_slicing, clippy::unwrap_used)]
                    let right = mir_node.ancestors[1].upgrade().unwrap();
                    make_join_node(
                        &name,
                        left,
                        right,
                        &mir_node.columns(),
                        on_left,
                        on_right,
                        project,
                        JoinType::Left,
                        mig,
                    )?
                }
                MirNodeInner::Project {
                    ref emit,
                    ref literals,
                    ref expressions,
                } => {
                    invariant_eq!(mir_node.ancestors.len(), 1);
                    #[allow(clippy::unwrap_used)] // checked by above invariant
                    let parent = mir_node.first_ancestor().unwrap();
                    make_project_node(
                        &name,
                        parent,
                        &mir_node.columns(),
                        emit,
                        expressions,
                        literals,
                        mig,
                    )?
                }
                MirNodeInner::Reuse { ref node } => {
                    match *node.borrow()
                        .flow_node
                        .as_ref()
                        .ok_or_else(|| internal_err("Reused MirNode must have FlowNode"))? {
                        // "New" => flow node was originally created for the node that we
                        // are reusing
                        FlowNode::New(na) |
                        // "Existing" => flow node was already reused from some other
                        // MIR node
                        FlowNode::Existing(na) => FlowNode::Existing(na),
                    }
                }
                MirNodeInner::Union {
                    ref emit,
                    duplicate_mode,
                } => {
                    invariant_eq!(mir_node.ancestors.len(), emit.len());
                    #[allow(clippy::unwrap_used)]
                    make_union_node(
                        &name,
                        &mir_node.columns(),
                        emit,
                        &mir_node
                            .ancestors()
                            .iter()
                            .map(|n| n.upgrade().unwrap())
                            .collect::<Vec<_>>(),
                        duplicate_mode,
                        mig,
                    )?
                }
                MirNodeInner::Distinct { ref group_by } => {
                    invariant_eq!(mir_node.ancestors.len(), 1);
                    #[allow(clippy::unwrap_used)] // checked by above invariant
                    let parent = mir_node.first_ancestor().unwrap();
                    make_distinct_node(&name, parent, &mir_node.columns(), group_by, mig)?
                }
                MirNodeInner::Paginate {
                    ref order,
                    ref group_by,
                    limit,
                    ..
                }
                | MirNodeInner::TopK {
                    ref order,
                    ref group_by,
                    limit,
                } => {
                    invariant_eq!(mir_node.ancestors.len(), 1);
                    #[allow(clippy::unwrap_used)] // checked by above invariant
                    let parent = mir_node.first_ancestor().unwrap();
                    make_paginate_or_topk_node(
                        &name,
                        parent,
                        &mir_node.columns(),
                        order,
                        group_by,
                        limit,
                        matches!(mir_node.inner, MirNodeInner::TopK { .. }),
                        mig,
                    )?
                }
                MirNodeInner::AliasTable { .. } => mir_node
                    .parent()
                    .and_then(|n| n.borrow().flow_node)
                    .ok_or_else(|| internal_err("MirNodeInner::AliasTable must have a parent"))?,
            };

            // any new flow nodes have been instantiated by now, so we replace them with
            // existing ones, but still return `FlowNode::New` below in order to notify higher
            // layers of the new nodes.
            mir_node.flow_node = match flow_node {
                FlowNode::New(na) => Some(FlowNode::Existing(na)),
                n @ FlowNode::Existing(..) => Some(n),
            };
            Ok(flow_node)
        }
        Some(flow_node) => Ok(flow_node),
    }
}

fn adapt_base_node(
    over_node: MirNodeRef,
    mig: &mut Migration<'_>,
    column_specs: &mut [(ColumnSpecification, Option<usize>)],
    add: &[ColumnSpecification],
    remove: &[ColumnSpecification],
) -> ReadySetResult<FlowNode> {
    let na = match over_node.borrow().flow_node {
        None => internal!("adapted base node must have a flow node already!"),
        Some(ref flow_node) => flow_node.address(),
    };

    for a in add.iter() {
        let mut default_value = DataType::None;
        for c in &a.constraints {
            if let ColumnConstraint::DefaultValue(dv) = c {
                default_value = dv.try_into()?;
                break;
            }
        }
        let column_id = mig.add_column(na, DataflowColumn::from(a.clone()), default_value)?;

        // store the new column ID in the column specs for this node
        for &mut (ref cs, ref mut cid) in column_specs.iter_mut() {
            if cs == a {
                invariant!(cid.is_none()); // FIXME(eta): used to be assert_eq
                *cid = Some(column_id);
            }
        }
    }
    for r in remove.iter() {
        let over_node = over_node.borrow();
        let column_specs = over_node.column_specifications()?;
        let pos = column_specs
            .iter()
            .position(|&(ref ecs, _)| ecs == r)
            .ok_or_else(|| {
                internal_err(format!(
                    "could not find ColumnSpecification {:?} in {:?}",
                    r, column_specs
                ))
            })?;
        // pos just came from `position` above
        #[allow(clippy::indexing_slicing)]
        let cid = column_specs[pos]
            .1
            .ok_or_else(|| internal_err("base column ID must be set to remove column"))?;
        mig.drop_column(na, cid)?;
    }

    Ok(FlowNode::Existing(na))
}

fn column_names(cs: &[Column]) -> Vec<&str> {
    cs.iter().map(|c| c.name.as_str()).collect()
}

fn make_base_node(
    name: &str,
    column_specs: &mut [(ColumnSpecification, Option<usize>)],
    primary_key: Option<&[Column]>,
    unique_keys: &[Box<[Column]>],
    mig: &mut Migration<'_>,
) -> ReadySetResult<FlowNode> {
    // remember the absolute base column ID for potential later removal
    for (i, cs) in column_specs.iter_mut().enumerate() {
        cs.1 = Some(i);
    }

    let columns: Vec<DataflowColumn> = column_specs
        .iter()
        .map(|&(ref cs, _)| cs.clone().into())
        .collect();

    // note that this defaults to a "None" (= NULL) default value for columns that do not have one
    // specified; we don't currently handle a "NOT NULL" SQL constraint for defaults
    let default_values = column_specs
        .iter()
        .map(|&(ref cs, _)| {
            for c in &cs.constraints {
                if let ColumnConstraint::DefaultValue(ref dv) = *c {
                    return dv.try_into();
                }
            }
            Ok(DataType::None)
        })
        .collect::<Result<Vec<DataType>, _>>()?;

    let cols_from_spec = |cols: &[Column]| -> ReadySetResult<Vec<usize>> {
        cols.iter()
            .map(|col| {
                column_specs
                    .iter()
                    .position(|(ColumnSpecification { column, .. }, _)| {
                        column.name == col.name && column.table == col.table
                    })
                    .ok_or_else(|| {
                        internal_err(format!("could not find pkey column id for {:?}", col))
                    })
            })
            .collect()
    };

    let primary_key = primary_key.map(cols_from_spec).transpose()?;

    let unique_keys = unique_keys
        .iter()
        .map(|u| cols_from_spec(u))
        .collect::<ReadySetResult<Vec<_>>>()?;

    let base = node::special::Base::new()
        .with_default_values(default_values)
        .with_unique_keys(unique_keys);

    let base = if let Some(pk) = primary_key {
        base.with_primary_key(pk)
    } else {
        base
    };

    Ok(FlowNode::New(mig.add_base(name, columns, base)))
}

fn make_union_node(
    name: &str,
    columns: &[Column],
    emit: &[Vec<Column>],
    ancestors: &[MirNodeRef],
    duplicate_mode: ops::union::DuplicateMode,
    mig: &mut Migration<'_>,
) -> ReadySetResult<FlowNode> {
    let mut emit_column_id: HashMap<NodeIndex, Vec<usize>> = HashMap::new();

    let mut cols = Vec::with_capacity(
        emit.get(0)
            .ok_or_else(|| internal_err("No emit columns"))?
            .len(),
    );

    // column_id_for_column doesn't take into consideration table aliases
    // which might cause improper ordering of columns in a union node
    // eg. Q6 in finkelstein.txt
    for (i, n) in ancestors.iter().enumerate() {
        let emit_cols = emit
            .get(i)
            .ok_or_else(|| internal_err(format!("no index {} in emit cols {:?}", i, emit)))?
            .iter()
            .map(|c| n.borrow().column_id_for_column(c))
            .collect::<ReadySetResult<Vec<_>>>()?;

        let ni = n.borrow().flow_node_addr()?;

        // Union takes columns of first ancestor
        if i == 0 {
            #[allow(clippy::indexing_slicing)] // just got the address
            let parent_cols = mig.dataflow_state.ingredients[ni].columns();
            cols = emit_cols
                .iter()
                .map(|i| {
                    parent_cols
                        .get(*i)
                        .cloned()
                        .ok_or_else(|| internal_err("Invalid index"))
                })
                .collect::<ReadySetResult<Vec<_>>>()?;
        }

        emit_column_id.insert(ni, emit_cols);
    }
    set_names(&column_names(columns), &mut cols)?;

    let node = mig.add_ingredient(
        name,
        cols,
        ops::union::Union::new(emit_column_id, duplicate_mode)?,
    );

    Ok(FlowNode::New(node))
}

fn make_filter_node(
    name: &str,
    parent: MirNodeRef,
    columns: &[Column],
    conditions: Expression,
    mig: &mut Migration<'_>,
) -> ReadySetResult<FlowNode> {
    let parent_na = parent.borrow().flow_node_addr()?;
    let filter_conditions = lower_expression(&parent, conditions)?;
    #[allow(clippy::indexing_slicing)] // just got the address
    let mut parent_cols = mig.dataflow_state.ingredients[parent_na].columns().to_vec();

    set_names(&column_names(columns), &mut parent_cols)?;

    let node = mig.add_ingredient(
        name,
        parent_cols,
        ops::filter::Filter::new(parent_na, filter_conditions),
    );
    Ok(FlowNode::New(node))
}

fn make_grouped_node(
    name: &str,
    parent: MirNodeRef,
    columns: &[Column],
    on: &Column,
    group_by: &[Column],
    kind: GroupedNodeType,
    mig: &mut Migration<'_>,
) -> ReadySetResult<FlowNode> {
    invariant!(!group_by.is_empty());
    let parent_na = parent.borrow().flow_node_addr()?;
    let parent_node = parent.borrow();
    let over_col_indx = parent_node.column_id_for_column(on)?;
    let group_col_indx = group_by
        .iter()
        .map(|c| parent_node.column_id_for_column(c))
        .collect::<ReadySetResult<Vec<_>>>()?;
    invariant!(!group_col_indx.is_empty());

    // Grouped projects the group_by columns followed by computed column
    #[allow(clippy::indexing_slicing)] // just got the address
    let parent_cols = mig.dataflow_state.ingredients[parent_na].columns();

    // group by columns
    let mut cols = group_col_indx
        .iter()
        .map(|i| {
            parent_cols
                .get(*i)
                .cloned()
                .ok_or_else(|| internal_err("Invalid index"))
        })
        .collect::<ReadySetResult<Vec<_>>>()?;

    let over_col_ty = parent_cols
        .get(over_col_indx)
        .ok_or_else(|| internal_err("Invalid index"))?
        .ty();
    let over_col_name = &columns
        .last()
        .ok_or_else(|| internal_err("Grouped has no projections"))?
        .name;

    let na = match kind {
        // This is the product of an incomplete refactor. It simplifies MIR to consider Group_Concat
        // to be an aggregation, however once we are in dataflow land the logic has not been
        // merged yet. For this reason, we need to pattern match for a groupconcat
        // aggregation before we pattern match for a generic aggregation.
        GroupedNodeType::Aggregation(Aggregation::GroupConcat { separator: sep }) => {
            let gc = GroupConcat::new(parent_na, over_col_indx, group_col_indx, sep)?;
            let agg_col = DataflowColumn::new(
                over_col_name.clone(),
                SqlType::Text.into(),
                Some(name.into()),
            );
            cols.push(agg_col);
            set_names(&column_names(columns), &mut cols)?;
            mig.add_ingredient(name, cols, gc)
        }
        GroupedNodeType::Aggregation(agg) => {
            let grouped = agg.over(parent_na, over_col_indx, group_col_indx.as_slice())?;
            let agg_col = grouped
                .output_col_type()
                .map(|ty| DataflowColumn::new(over_col_name.clone(), ty.into(), Some(name.into())))
                .unwrap_or_else(|| {
                    DataflowColumn::new(
                        over_col_name.clone(),
                        over_col_ty.clone(),
                        Some(name.into()),
                    )
                });
            cols.push(agg_col);
            set_names(&column_names(columns), &mut cols)?;
            mig.add_ingredient(name, cols, grouped)
        }
        GroupedNodeType::Extremum(extr) => {
            let grouped = extr.over(parent_na, over_col_indx, group_col_indx.as_slice());
            let agg_col = grouped
                .output_col_type()
                .map(|ty| DataflowColumn::new(over_col_name.clone(), ty.into(), Some(name.into())))
                .unwrap_or_else(|| {
                    DataflowColumn::new(
                        over_col_name.clone(),
                        over_col_ty.clone(),
                        Some(name.into()),
                    )
                });
            cols.push(agg_col);
            set_names(&column_names(columns), &mut cols)?;
            mig.add_ingredient(name, cols, grouped)
        }
    };
    Ok(FlowNode::New(na))
}

fn make_identity_node(
    name: &str,
    parent: MirNodeRef,
    columns: &[Column],
    mig: &mut Migration<'_>,
) -> ReadySetResult<FlowNode> {
    let parent_na = parent.borrow().flow_node_addr()?;
    // Identity mirrors the parent nodes exactly
    #[allow(clippy::indexing_slicing)] // just got the address
    let mut parent_cols = mig.dataflow_state.ingredients[parent_na].columns().to_vec();
    set_names(&column_names(columns), &mut parent_cols)?;

    let node = mig.add_ingredient(
        String::from(name),
        parent_cols,
        ops::identity::Identity::new(parent_na),
    );
    Ok(FlowNode::New(node))
}

fn make_join_node(
    name: &str,
    left: MirNodeRef,
    right: MirNodeRef,
    columns: &[Column],
    on_left: &[Column],
    on_right: &[Column],
    proj_cols: &[Column],
    kind: JoinType,
    mig: &mut Migration<'_>,
) -> ReadySetResult<FlowNode> {
    use dataflow::ops::join::JoinSource;

    invariant_eq!(on_left.len(), on_right.len());

    #[allow(clippy::indexing_slicing)] // just got the address
    let left_cols = mig.dataflow_state.ingredients[left.borrow().flow_node_addr()?].columns();
    #[allow(clippy::indexing_slicing)] // just got the address
    let right_cols = mig.dataflow_state.ingredients[right.borrow().flow_node_addr()?].columns();

    let mut cols = proj_cols
        .iter()
        .map(|c| match left.borrow().column_id_for_column(c) {
            Ok(idx) => left_cols
                .get(idx)
                .cloned()
                .ok_or_else(|| internal_err("Invalid index")),
            Err(_) => match right.borrow().column_id_for_column(c) {
                Ok(idx) => right_cols
                    .get(idx)
                    .cloned()
                    .ok_or_else(|| internal_err("Invalid index")),
                Err(_) => Err(internal_err("Column not found in either parent")),
            },
        })
        .collect::<ReadySetResult<Vec<_>>>()?;

    set_names(&column_names(columns), &mut cols)?;

    let (projected_cols_left, rest): (Vec<Column>, Vec<Column>) = proj_cols
        .iter()
        .cloned()
        .partition(|c| left.borrow().columns().contains(c));
    let (projected_cols_right, rest): (Vec<Column>, Vec<Column>) = rest
        .into_iter()
        .partition(|c| right.borrow().columns().contains(c));
    invariant!(
        rest.is_empty(),
        "could not resolve output columns projected from join: {:?}",
        rest
    );

    invariant_eq!(
        projected_cols_left.len() + projected_cols_right.len(),
        proj_cols.len()
    );

    // this assumes the columns we want to join on appear first in the list
    // of projected columns. this is fine for joins against different tables
    // since we assume unique column names in each table. however, this is
    // not correct for joins against the same table, for example:
    // SELECT r1.a as a1, r2.a as a2 from r as r1, r as r2 where r1.a = r2.b and r2.a = r1.b;
    //
    // the `r1.a = r2.b` join predicate will create a join node with columns: r1.a, r1.b, r2.a, r2,b
    // however, because the way we deal with aliases, we can't distinguish between `r1.a` and `r2.a`
    // at this point in the codebase, so the `r2.a = r1.b` will join on the wrong `a` column.
    let join_col_mappings = on_left
        .iter()
        .zip(on_right)
        .map(|(l, r)| -> ReadySetResult<_> {
            let left_join_col_id = left
                .borrow()
                .columns()
                .iter()
                .position(|lc| lc == l)
                .ok_or_else(|| {
                    internal_err(format!(
                        "missing left-side join column {:#?} in {:#?}",
                        on_left.first(),
                        left.borrow().columns()
                    ))
                })?;

            let right_join_col_id = right
                .borrow()
                .columns()
                .iter()
                .position(|rc| rc == r)
                .ok_or_else(|| {
                    internal_err(format!(
                        "missing right-side join column {:#?} in {:#?}",
                        on_right.first(),
                        right.borrow().columns()
                    ))
                })?;

            Ok((left_join_col_id, right_join_col_id))
        })
        .collect::<Result<HashMap<_, _>, _>>()?;

    let mut from_left = 0;
    let mut from_right = 0;
    let mut join_config: Vec<_> = left
        .borrow()
        .columns()
        .iter()
        .enumerate()
        .filter_map(|(i, c)| {
            if let Some(r) = join_col_mappings.get(&i) {
                from_left += 1;
                Some(JoinSource::B(i, *r))
            } else if projected_cols_left.contains(c) {
                from_left += 1;
                Some(JoinSource::L(i))
            } else {
                None
            }
        })
        .chain(
            right
                .borrow()
                .columns()
                .iter()
                .enumerate()
                .filter_map(|(i, c)| {
                    if projected_cols_right.contains(c) {
                        from_right += 1;
                        Some(JoinSource::R(i))
                    } else {
                        None
                    }
                }),
        )
        .collect();
    invariant_eq!(from_left, projected_cols_left.len());
    invariant_eq!(from_right, projected_cols_right.len());

    let mut left_na = left.borrow().flow_node_addr()?;
    let mut right_na = right.borrow().flow_node_addr()?;

    // If we don't have any join condition, we're making a cross join.
    // Dataflow needs a non-empty join condition, so project out a constant value on both sides to
    // use as our join key
    if join_col_mappings.is_empty() {
        let mut make_cross_join_bogokey = |node: MirNodeRef| {
            let mut node_columns = node.borrow().columns().to_vec();
            node_columns.push(Column::named("cross_join_bogokey"));

            make_project_node(
                &format!("{}_cross_join_bogokey", node.borrow().name()),
                node.clone(),
                &node_columns,
                &node.borrow().columns(),
                &[],
                &[("cross_join_bogokey".into(), DataType::from(0))],
                mig,
            )
        };

        let left_col_idx = left.borrow().columns().len();
        let right_col_idx = right.borrow().columns().len();

        left_na = make_cross_join_bogokey(left)?.address();
        right_na = make_cross_join_bogokey(right)?.address();

        join_config.push(JoinSource::B(left_col_idx, right_col_idx));
        cols.push(DataflowColumn::new(
            "cross_join_bogokey".into(),
            SqlType::Bigint(None).into(),
            Some(name.into()),
        ));
    }

    let j = Join::new(left_na, right_na, kind, join_config);
    let n = mig.add_ingredient(String::from(name), cols, j);

    Ok(FlowNode::New(n))
}

/// Joins two parent aggregate nodes together. Columns that are shared between both parents are
/// assumed to be group_by columns and all unique columns are considered to be the aggregate
/// columns themselves.
fn make_join_aggregates_node(
    name: &str,
    left: MirNodeRef,
    right: MirNodeRef,
    columns: &[Column],
    mig: &mut Migration<'_>,
) -> ReadySetResult<FlowNode> {
    use dataflow::ops::join::JoinSource;

    let left_na = left.borrow().flow_node_addr()?;
    let right_na = right.borrow().flow_node_addr()?;
    #[allow(clippy::indexing_slicing)] // just got the address
    let left_cols = mig.dataflow_state.ingredients[left_na].columns();
    #[allow(clippy::indexing_slicing)] // just got the address
    let right_cols = mig.dataflow_state.ingredients[right_na].columns();

    // We gather up all of the columns from each respective parent. If a column is in both parents,
    // then we know it was a group_by column and create a JoinSource::B type with the indices from
    // both parents. Otherwise if the column is exclusively in the left parent (such as the
    // aggregate column itself), we make a JoinSource::L with the left parent index. We finally
    // iterate through the right parent and add the columns that were exclusively in the right
    // parent as JoinSource::R with the right parent index for each given unique column.
    let join_config = left
        .borrow()
        .columns()
        .iter()
        .enumerate()
        .map(|(i, c)| {
            if let Ok(j) = right.borrow().column_id_for_column(c) {
                // If the column was found in both, it's a group_by column and gets added as
                // JoinSource::B.
                JoinSource::B(i, j)
            } else {
                // Column exclusively in left parent, so gets added as JoinSource::L.
                JoinSource::L(i)
            }
        })
        .chain(
            right
                .borrow()
                .columns()
                .iter()
                .enumerate()
                .filter_map(|(i, c)| {
                    // If column is in left, don't do anything it's already been added.
                    // If it's in right, add it with right index.
                    if left.borrow().column_id_for_column(c).is_ok() {
                        None
                    } else {
                        // Column exclusively in right parent, so gets added as JoinSource::R.
                        Some(JoinSource::R(i))
                    }
                }),
        )
        .collect::<Vec<_>>();

    let mut cols = join_config
        .iter()
        .map(|j| match j {
            JoinSource::B(i, _) | JoinSource::L(i) => left_cols
                .get(*i)
                .cloned()
                .ok_or_else(|| internal_err("Invalid index")),
            JoinSource::R(i) => right_cols
                .get(*i)
                .cloned()
                .ok_or_else(|| internal_err("Invalid index")),
        })
        .collect::<ReadySetResult<Vec<_>>>()?;

    set_names(&column_names(columns), &mut cols)?;

    // Always treated as a JoinType::Inner based on joining on group_by cols, which always match
    // between parents.
    let j = Join::new(left_na, right_na, JoinType::Inner, join_config);
    let n = mig.add_ingredient(String::from(name), cols, j);

    Ok(FlowNode::New(n))
}

fn make_latest_node(
    name: &str,
    parent: MirNodeRef,
    columns: &[Column],
    group_by: &[Column],
    mig: &mut Migration<'_>,
) -> ReadySetResult<FlowNode> {
    let parent_na = parent.borrow().flow_node_addr()?;
    #[allow(clippy::indexing_slicing)] // just got the address
    let mut cols = mig.dataflow_state.ingredients[parent_na].columns().to_vec();

    set_names(&column_names(columns), &mut cols)?;

    let group_col_indx = group_by
        .iter()
        .map(|c| parent.borrow().column_id_for_column(c))
        .collect::<ReadySetResult<Vec<_>>>()?;

    // latest doesn't support compound group by
    if group_col_indx.len() != 1 {
        unsupported!("latest node doesn't support compound GROUP BY")
    }
    #[allow(clippy::indexing_slicing)] // group_col_indx length checked above
    let na = mig.add_ingredient(
        String::from(name),
        cols,
        Latest::new(parent_na, group_col_indx[0]),
    );
    Ok(FlowNode::New(na))
}

/// Lower the given nom_sql AST expression to a DataflowExpression.
///
/// Currently, this involves:
///
/// - Literals being replaced with their corresponding [`DataType`]
/// - [Column references](nom_sql::Column) being resolved into column indices in the parent node.
/// - Function calls being resolved to built-in functions, and arities checked
/// - Desugaring x IN (y, z, ...) to `x = y OR x = z OR ...` and x NOT IN (y, z, ...) to `x != y AND
///   x != z AND ...`
/// - Replacing NEG with (expr * -1)
/// - Replacing NOT with (expr != 1)
fn lower_expression(parent: &MirNodeRef, expr: Expression) -> ReadySetResult<DataflowExpression> {
    match expr {
        Expression::Call(FunctionExpression::Call {
            name: fname,
            arguments,
        }) => Ok(DataflowExpression::Call(
            BuiltinFunction::from_name_and_args(
                &fname,
                arguments
                    .into_iter()
                    .map(|arg| lower_expression(parent, arg))
                    .collect::<Result<Vec<_>, _>>()?,
            )?,
        )),
        Expression::Call(call) => internal!(
            "Unexpected (aggregate?) call node in project expression: {:?}",
            Sensitive(&call)
        ),
        Expression::Literal(lit) => Ok(DataflowExpression::Literal(lit.try_into()?)),
        Expression::Column(nom_sql::Column { name, table, .. }) => Ok(DataflowExpression::Column(
            parent
                .borrow()
                .column_id_for_column(&Column::new(table.as_deref(), &name))?,
        )),
        Expression::BinaryOp { lhs, op, rhs } => Ok(DataflowExpression::Op {
            op,
            left: Box::new(lower_expression(parent, *lhs)?),
            right: Box::new(lower_expression(parent, *rhs)?),
        }),
        Expression::UnaryOp {
            op: UnaryOperator::Neg,
            rhs,
        } => Ok(DataflowExpression::Op {
            op: BinaryOperator::Multiply,
            left: Box::new(lower_expression(parent, *rhs)?),
            right: Box::new(DataflowExpression::Literal(DataType::Int(-1))),
        }),
        Expression::UnaryOp {
            op: UnaryOperator::Not,
            rhs,
        } => Ok(DataflowExpression::Op {
            op: BinaryOperator::NotEqual,
            left: Box::new(lower_expression(parent, *rhs)?),
            right: Box::new(DataflowExpression::Literal(DataType::Int(1))),
        }),
        Expression::Cast { expr, ty, .. } => Ok(DataflowExpression::Cast(
            Box::new(lower_expression(parent, *expr)?),
            ty,
        )),
        Expression::CaseWhen {
            condition,
            then_expr,
            else_expr,
        } => Ok(DataflowExpression::CaseWhen {
            condition: Box::new(lower_expression(parent, *condition)?),
            then_expr: Box::new(lower_expression(parent, *then_expr)?),
            else_expr: match else_expr {
                Some(else_expr) => Box::new(lower_expression(parent, *else_expr)?),
                None => Box::new(DataflowExpression::Literal(DataType::None)),
            },
        }),
        Expression::In {
            lhs,
            rhs: InValue::List(exprs),
            negated,
        } => {
            let mut exprs = exprs.into_iter();
            if let Some(fst) = exprs.next() {
                let (comparison_op, logical_op) = if negated {
                    (BinaryOperator::NotEqual, BinaryOperator::And)
                } else {
                    (BinaryOperator::Equal, BinaryOperator::Or)
                };

                let lhs = lower_expression(parent, *lhs)?;
                let make_comparison = |rhs| -> ReadySetResult<_> {
                    Ok(DataflowExpression::Op {
                        left: Box::new(lhs.clone()),
                        op: comparison_op,
                        right: Box::new(lower_expression(parent, rhs)?),
                    })
                };

                exprs.try_fold(make_comparison(fst)?, |acc, rhs| {
                    Ok(DataflowExpression::Op {
                        left: Box::new(acc),
                        op: logical_op,
                        right: Box::new(make_comparison(rhs)?),
                    })
                })
            } else if negated {
                // x IN () is always false
                Ok(DataflowExpression::Literal(DataType::None))
            } else {
                // x NOT IN () is always false
                Ok(DataflowExpression::Literal(DataType::from(1)))
            }
        }
        Expression::Exists(_) => unsupported!("EXISTS not currently supported"),
        Expression::Variable(_) => unsupported!("Variables not currently supported"),
        Expression::Between { .. } | Expression::NestedSelect(_) | Expression::In { .. } => {
            internal!("Expression should have been desugared earlier: {}", expr)
        }
    }
}

fn make_project_node(
    name: &str,
    parent: MirNodeRef,
    source_columns: &[Column],
    emit: &[Column],
    expressions: &[(SqlIdentifier, Expression)],
    literals: &[(SqlIdentifier, DataType)],
    mig: &mut Migration<'_>,
) -> ReadySetResult<FlowNode> {
    let parent_na = parent.borrow().flow_node_addr()?;
    #[allow(clippy::indexing_slicing)] // just got the address
    let parent_cols = mig.dataflow_state.ingredients[parent_na].columns();

    let projected_column_ids = emit
        .iter()
        .map(|c| {
            parent
                .borrow()
                .find_source_for_child_column(c)
                .ok_or_else(|| {
                    internal_err(format!("could not find source for child column: {:?}", c))
                })
        })
        .collect::<Result<Vec<_>, _>>()?;

    let mut cols = projected_column_ids
        .iter()
        .map(|i| {
            parent_cols
                .get(*i)
                .cloned()
                .ok_or_else(|| internal_err("Invalid index"))
        })
        .collect::<ReadySetResult<Vec<_>>>()?;

    // First set names for emitted columns. `set_names()` is not used because it assumes the two
    // fields have equal lengths
    let column_names = column_names(source_columns);
    for (c, n) in cols.iter_mut().zip(column_names) {
        c.set_name(n.into());
    }

    let (_, literal_values): (Vec<_>, Vec<_>) = literals.iter().cloned().unzip();

    let projected_expressions: Vec<DataflowExpression> = expressions
        .iter()
        .map(|(_, e)| lower_expression(&parent, e.clone()))
        .collect::<Result<Vec<_>, _>>()?;

    let col_type = |idx: usize| -> ReadySetResult<Option<SqlType>> {
        Ok(parent_cols
            .get(idx)
            .ok_or_else(|| {
                internal_err(format!(
                    "Expression referenced invalid index in parent, idx={}",
                    idx
                ))
            })?
            .ty()
            .clone()
            .into())
    };

    let col_names = source_columns
        .iter()
        .skip(cols.len())
        .map(|c| c.name.clone());

    let projected_expression_types = projected_expressions
        .iter()
        .map(|e| e.sql_type(col_type))
        .collect::<ReadySetResult<Vec<_>>>()?;
    let literal_types = literal_values
        .iter()
        .map(|l| l.sql_type())
        .collect::<Vec<_>>();

    cols.extend(
        projected_expression_types
            .iter()
            .chain(literal_types.iter())
            .zip(col_names)
            .map(|(ty, n)| DataflowColumn::new(n, ty.clone().into(), Some(name.into()))),
    );

    // Check here since we did not check in `set_names()`
    invariant_eq!(source_columns.len(), cols.len());

    let n = mig.add_ingredient(
        name,
        cols,
        Project::new(
            parent_na,
            projected_column_ids.as_slice(),
            Some(literal_values),
            Some(projected_expressions),
        ),
    );
    Ok(FlowNode::New(n))
}

fn make_distinct_node(
    name: &str,
    parent: MirNodeRef,
    columns: &[Column],
    group_by: &[Column],
    mig: &mut Migration<'_>,
) -> ReadySetResult<FlowNode> {
    let parent_na = parent.borrow().flow_node_addr()?;
    #[allow(clippy::indexing_slicing)] // just got the address
    let parent_cols = mig.dataflow_state.ingredients[parent_na].columns().to_vec();

    let grp_by_column_ids = group_by
        .iter()
        .map(|c| {
            parent
                .borrow()
                .find_source_for_child_column(c)
                .ok_or_else(|| {
                    internal_err(format!("could not find source for child column: {:?}", c))
                })
        })
        .collect::<Result<Vec<_>, _>>()?;

    let mut cols = grp_by_column_ids
        .iter()
        .map(|i| {
            parent_cols
                .get(*i)
                .cloned()
                .ok_or_else(|| internal_err("Invalid index"))
        })
        .collect::<ReadySetResult<Vec<_>>>()?;

    // distinct count is projected last
    let distinct_count_name = columns
        .last()
        .ok_or_else(|| internal_err("No projected columns for distinct"))?
        .name
        .clone();
    cols.push(DataflowColumn::new(
        distinct_count_name,
        SqlType::Bigint(None).into(),
        Some(name.into()),
    ));
    set_names(&column_names(columns), &mut cols)?;

    let group_by_indx = if group_by.is_empty() {
        // no query parameters, so we index on the first column
        columns
            .iter()
            .map(|c| parent.borrow().column_id_for_column(c))
            .collect::<ReadySetResult<Vec<_>>>()?
    } else {
        group_by
            .iter()
            .map(|c| parent.borrow().column_id_for_column(c))
            .collect::<ReadySetResult<Vec<_>>>()?
    };

    // make the new operator and record its metadata
    let na = mig.add_ingredient(
        String::from(name),
        cols,
        // We're using Count to implement distinct here, because count already keeps track of how
        // many times we have seen a set of values. This means that if we get a row
        // deletion, we won't be removing it from our records of distinct rows unless there are no
        // remaining occurances of the set.
        //
        // We use 0 as a placeholder. The over column is ignored with Count entirely. This should
        // be refactored so Count doesn't take an over column at all.
        // Issue: https://readysettech.atlassian.net/browse/ENG-310
        Aggregation::Count { count_nulls: false }.over(parent_na, 0, &group_by_indx)?,
    );
    Ok(FlowNode::New(na))
}

fn make_paginate_or_topk_node(
    name: &str,
    parent: MirNodeRef,
    columns: &[Column],
    order: &Option<Vec<(Column, OrderType)>>,
    group_by: &[Column],
    limit: usize,
    is_topk: bool,
    mig: &mut Migration<'_>,
) -> ReadySetResult<FlowNode> {
    let parent_na = parent.borrow().flow_node_addr()?;
    #[allow(clippy::indexing_slicing)] // just got the address
    let mut parent_cols = mig.dataflow_state.ingredients[parent_na].columns().to_vec();

    // set names using MIR columns to ensure aliases are used
    let column_names = column_names(columns);
    // create page_number column if this is a paginate node
    if !is_topk {
        #[allow(clippy::unwrap_used)] // column_names must be populated
        parent_cols.push(DataflowColumn::new(
            column_names.last().unwrap().into(),
            SqlType::Bigint(None).into(),
            Some(name.into()),
        ));
    }
    set_names(&column_names, &mut parent_cols)?;

    invariant!(
        !group_by.is_empty(),
        "need bogokey for Paginate without group columns"
    );

    let group_by_indx = group_by
        .iter()
        .map(|c| parent.borrow().column_id_for_column(c))
        .collect::<ReadySetResult<Vec<_>>>()?;

    let cmp_rows = match *order {
        Some(ref o) => {
            let columns = o
                .iter()
                .map(|&(ref c, ref order_type)| {
                    // SQL and Soup disagree on what ascending and descending order means, so do the
                    // conversion here.
                    let reversed_order_type = match *order_type {
                        OrderType::OrderAscending => OrderType::OrderDescending,
                        OrderType::OrderDescending => OrderType::OrderAscending,
                    };
                    parent
                        .borrow()
                        .column_id_for_column(c)
                        .map(|id| (id, reversed_order_type))
                })
                .collect::<ReadySetResult<Vec<_>>>()?;

            columns
        }
        None => Vec::new(),
    };

    // make the new operator and record its metadata
    let na = if is_topk {
        mig.add_ingredient(
            String::from(name),
            parent_cols,
            ops::topk::TopK::new(parent_na, cmp_rows, group_by_indx, limit),
        )
    } else {
        mig.add_ingredient(
            String::from(name),
            parent_cols,
            ops::paginate::Paginate::new(parent_na, cmp_rows, group_by_indx, limit),
        )
    };
    Ok(FlowNode::New(na))
}

fn make_post_lookup(
    parent: &MirNodeRef,
    order_by: &Option<Vec<(Column, OrderType)>>,
    limit: Option<usize>,
    returned_cols: &Option<Vec<Column>>,
    default_row: Option<Vec<DataType>>,
    aggregates: &Option<PostLookupAggregates<Column>>,
) -> ReadySetResult<PostLookup> {
    let order_by = if let Some(order) = order_by.as_ref() {
        Some(
            order
                .iter()
                .map(|(col, ot)| {
                    parent
                        .borrow()
                        .column_id_for_column(col)
                        .map(|id| (id, *ot))
                })
                .collect::<ReadySetResult<Vec<(usize, OrderType)>>>()?,
        )
    } else {
        None
    };
    let returned_cols = if let Some(col) = returned_cols.as_ref() {
        Some(
            col.iter()
                .map(|col| (parent.borrow().column_id_for_column(col)))
                .collect::<ReadySetResult<_>>()?,
        )
    } else {
        None
    };

    let aggregates = aggregates
        .clone()
        .map(|aggs| aggs.map_columns(|col| parent.borrow().column_id_for_column(&col)))
        .transpose()?;

    Ok(PostLookup {
        order_by,
        limit,
        returned_cols,
        default_row,
        aggregates,
    })
}

fn materialize_leaf_node(
    parent: &MirNodeRef,
    name: SqlIdentifier,
    key_cols: &[(Column, ViewPlaceholder)],
    index_type: IndexType,
    post_lookup: PostLookup,
    mig: &mut Migration<'_>,
) -> ReadySetResult<()> {
    let na = parent.borrow().flow_node_addr()?;

    // we must add a new reader for this query. This also requires adding an identity node (at
    // least currently), since a node can only have a single associated reader. However, the
    // identity node exists at the MIR level, so we don't need to consider it here, as it has
    // already been added.

    // TODO(malte): consider the case when the projected columns need reordering

    if !key_cols.is_empty() {
        let columns: Vec<_> = key_cols
            .iter()
            .map(|(c, _)| parent.borrow().column_id_for_column(c))
            .collect::<ReadySetResult<Vec<_>>>()?;

        let placeholder_map = key_cols
            .iter()
            .zip(columns.iter())
            .map(|((_, placeholder), col_index)| (*placeholder, *col_index))
            .collect::<Vec<_>>();

        mig.maintain(
            name,
            na,
            &Index::new(index_type, columns),
            post_lookup,
            placeholder_map,
        );
    } else {
        // if no key specified, default to the first column
        mig.maintain(
            name,
            na,
            &Index::new(index_type, vec![0]),
            post_lookup,
            Vec::default(),
        );
    }
    Ok(())
}
