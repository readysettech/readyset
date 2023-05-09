#![deny(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unimplemented,
    clippy::unreachable
)]

use std::collections::HashMap;
use std::convert::TryInto;

use common::DfValue;
use dataflow::node::Column as DfColumn;
use dataflow::ops::grouped::concat::GroupConcat;
use dataflow::ops::join::{Join, JoinType};
use dataflow::ops::project::Project;
use dataflow::ops::Side;
use dataflow::{node, ops, Expr as DfExpr, PostLookupAggregates, ReaderProcessing};
use itertools::Itertools;
use mir::graph::MirGraph;
use mir::node::node_inner::MirNodeInner;
use mir::node::{GroupedNodeType, ViewKeyColumn};
use mir::query::MirQuery;
use mir::{Column, DfNodeIndex, NodeIndex as MirNodeIndex};
use nom_sql::{ColumnConstraint, ColumnSpecification, Expr, OrderType, Relation, SqlIdentifier};
use petgraph::graph::NodeIndex;
use petgraph::Direction;
use readyset_client::internal::{Index, IndexType};
use readyset_client::ViewPlaceholder;
use readyset_data::{Collation, DfType, Dialect};
use readyset_errors::{
    internal, internal_err, invariant, invariant_eq, ReadySetError, ReadySetResult,
};

use crate::controller::Migration;
use crate::manual::ops::grouped::aggregate::Aggregation;

/// Sets the names of dataflow columns using the names determined in MIR to ensure aliases are used
fn set_names(names: &[&str], columns: &mut [DfColumn]) -> ReadySetResult<()> {
    invariant_eq!(columns.len(), names.len());
    for (c, n) in columns.iter_mut().zip(names.iter()) {
        c.set_name((*n).into());
    }
    Ok(())
}

pub(super) fn mir_query_to_flow_parts(
    mir_query: &mut MirQuery<'_>,
    custom_types: &HashMap<Relation, DfType>,
    mig: &mut Migration<'_>,
) -> ReadySetResult<DfNodeIndex> {
    for n in mir_query.topo_nodes() {
        mir_node_to_flow_parts(mir_query.graph, n, custom_types, mig).map_err(|e| {
            ReadySetError::MirNodeToDataflowFailed {
                index: n.index(),
                source: Box::new(e),
            }
        })?;
    }

    let df_leaf = mir_query
        .dataflow_node()
        .ok_or_else(|| internal_err!("Leaf must have a dataflow node assigned by now"))?;

    Ok(df_leaf)
}

pub(super) fn mir_node_to_flow_parts(
    graph: &mut MirGraph,
    mir_node: MirNodeIndex,
    custom_types: &HashMap<Relation, DfType>,
    mig: &mut Migration<'_>,
) -> ReadySetResult<Option<DfNodeIndex>> {
    use petgraph::visit::EdgeRef;

    let name = graph[mir_node].name().clone();

    let ancestors = graph
        .edges_directed(mir_node, Direction::Incoming)
        .sorted_by_key(|e| e.weight())
        .map(|e| e.source())
        .collect::<Vec<_>>();

    match graph[mir_node].df_node_index() {
        None => {
            let flow_node = match graph[mir_node].inner {
                MirNodeInner::Aggregation {
                    ref on,
                    ref group_by,
                    ref kind,
                    ..
                } => {
                    invariant_eq!(ancestors.len(), 1);
                    let parent = ancestors[0];
                    Some(make_grouped_node(
                        graph,
                        name,
                        parent,
                        &graph.columns(mir_node),
                        on,
                        group_by,
                        GroupedNodeType::Aggregation(kind.clone()),
                        mig,
                    )?)
                }
                MirNodeInner::Base {
                    ref column_specs,
                    ref primary_key,
                    ref unique_keys,
                    ..
                } => Some(make_base_node(
                    name,
                    column_specs.as_slice(),
                    custom_types,
                    primary_key.as_deref(),
                    unique_keys,
                    mig,
                )?),
                MirNodeInner::Extremum {
                    ref on,
                    ref group_by,
                    ref kind,
                    ..
                } => {
                    invariant_eq!(ancestors.len(), 1);
                    let parent = ancestors[0];
                    Some(make_grouped_node(
                        graph,
                        name,
                        parent,
                        &graph.columns(mir_node),
                        on,
                        group_by,
                        GroupedNodeType::Extremum(kind.clone()),
                        mig,
                    )?)
                }
                MirNodeInner::Filter { ref conditions } => {
                    invariant_eq!(ancestors.len(), 1);
                    let parent = ancestors[0];
                    Some(make_filter_node(
                        graph,
                        name,
                        parent,
                        &graph.referenced_columns(mir_node),
                        conditions.clone(),
                        custom_types,
                        mig,
                    )?)
                }
                MirNodeInner::Identity => {
                    invariant_eq!(ancestors.len(), 1);
                    let parent = ancestors[0];
                    Some(make_identity_node(
                        graph,
                        name,
                        parent,
                        &graph.referenced_columns(mir_node),
                        mig,
                    )?)
                }
                MirNodeInner::Join {
                    ref on,
                    ref project,
                    ..
                } => {
                    invariant_eq!(ancestors.len(), 2);
                    let left = ancestors[0];
                    let right = ancestors[1];
                    Some(make_join_node(
                        graph,
                        name,
                        left,
                        right,
                        &graph.referenced_columns(mir_node),
                        on,
                        project,
                        JoinType::Inner,
                        custom_types,
                        mig,
                    )?)
                }
                MirNodeInner::JoinAggregates => {
                    invariant_eq!(ancestors.len(), 2);
                    let left = ancestors[0];
                    let right = ancestors[1];
                    Some(make_join_aggregates_node(
                        graph,
                        name,
                        left,
                        right,
                        &graph.referenced_columns(mir_node),
                        mig,
                    )?)
                }
                MirNodeInner::DependentJoin { .. } => {
                    // See the docstring for MirNodeInner::DependentJoin
                    internal!("Encountered dependent join when lowering to dataflow")
                }
                MirNodeInner::ViewKey { ref key } => {
                    return Err(ReadySetError::UnsupportedPlaceholders {
                        placeholders: key.mapped_ref(
                            |ViewKeyColumn {
                                 placeholder_idx, ..
                             }| *placeholder_idx as _,
                        ),
                    });
                }
                MirNodeInner::Leaf {
                    ref keys,
                    index_type,
                    lowered_to_df,
                    ref order_by,
                    limit,
                    ref returned_cols,
                    ref default_row,
                    ref aggregates,
                    ..
                } => {
                    if !lowered_to_df {
                        invariant_eq!(ancestors.len(), 1);
                        let parent = ancestors[0];
                        let reader_processing = make_reader_processing(
                            graph,
                            &parent,
                            order_by,
                            limit,
                            returned_cols,
                            default_row.clone(),
                            aggregates,
                        )?;
                        materialize_leaf_node(
                            graph,
                            parent,
                            name,
                            keys,
                            index_type,
                            reader_processing,
                            mig,
                        )?;
                    }
                    None
                }
                MirNodeInner::LeftJoin {
                    ref on,
                    ref project,
                    ..
                } => {
                    invariant_eq!(ancestors.len(), 2);
                    let left = ancestors[0];
                    let right = ancestors[1];
                    Some(make_join_node(
                        graph,
                        name,
                        left,
                        right,
                        &graph.columns(mir_node),
                        on,
                        project,
                        JoinType::Left,
                        custom_types,
                        mig,
                    )?)
                }
                MirNodeInner::Project {
                    ref emit,
                    ref literals,
                    ref expressions,
                } => {
                    invariant_eq!(ancestors.len(), 1);
                    let parent = ancestors[0];
                    Some(make_project_node(
                        graph,
                        name,
                        parent,
                        &graph.columns(mir_node),
                        emit,
                        expressions,
                        literals,
                        custom_types,
                        mig,
                    )?)
                }
                MirNodeInner::Union {
                    ref emit,
                    duplicate_mode,
                } => {
                    invariant_eq!(ancestors.len(), emit.len());
                    #[allow(clippy::unwrap_used)]
                    Some(make_union_node(
                        graph,
                        name,
                        &graph.columns(mir_node),
                        emit,
                        &graph
                            .neighbors_directed(mir_node, Direction::Incoming)
                            .collect::<Vec<_>>(),
                        duplicate_mode,
                        mig,
                    )?)
                }
                MirNodeInner::Distinct { ref group_by } => {
                    invariant_eq!(ancestors.len(), 1);
                    let parent = ancestors[0];
                    Some(make_distinct_node(
                        graph,
                        name,
                        parent,
                        &graph.columns(mir_node),
                        group_by,
                        mig,
                    )?)
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
                    invariant_eq!(ancestors.len(), 1);
                    let parent = ancestors[0];
                    Some(make_paginate_or_topk_node(
                        graph,
                        name,
                        parent,
                        &graph.columns(mir_node),
                        order,
                        group_by,
                        limit,
                        matches!(graph[mir_node].inner, MirNodeInner::TopK { .. }),
                        mig,
                    )?)
                }
                MirNodeInner::AliasTable { .. } => None,
            };

            if let MirNodeInner::Leaf {
                ref mut lowered_to_df,
                ..
            } = graph[mir_node].inner
            {
                *lowered_to_df = true;
            }

            // any new flow nodes have been instantiated by now, so we replace them with
            // existing ones, but still return `FlowNode` below in order to notify higher
            // layers of the new nodes.
            if let Some(df_node) = flow_node {
                graph[mir_node].assign_df_node_index(df_node)?;
            }
            Ok(flow_node)
        }
        Some(flow_node) => Ok(Some(flow_node)),
    }
}

fn column_names(cs: &[Column]) -> Vec<&str> {
    cs.iter().map(|c| c.name.as_str()).collect()
}

fn make_base_node(
    name: Relation,
    column_specs: &[ColumnSpecification],
    custom_types: &HashMap<Relation, DfType>,
    primary_key: Option<&[Column]>,
    unique_keys: &[Box<[Column]>],
    mig: &mut Migration<'_>,
) -> ReadySetResult<DfNodeIndex> {
    let columns = column_specs
        .iter()
        .map(|cs| DfColumn::from_spec(cs.clone(), mig.dialect, |ty| custom_types.get(&ty).cloned()))
        .collect::<Result<Vec<_>, _>>()?;

    // note that this defaults to a "None" (= NULL) default value for columns that do not have one
    // specified; we don't currently handle a "NOT NULL" SQL constraint for defaults
    let default_values = column_specs
        .iter()
        .map(|cs| {
            for c in &cs.constraints {
                if let ColumnConstraint::DefaultValue(Expr::Literal(ref dv)) = *c {
                    return dv.try_into();
                }
            }
            Ok(DfValue::None)
        })
        .collect::<Result<Vec<DfValue>, _>>()?;

    let cols_from_spec = |cols: &[Column]| -> ReadySetResult<Vec<usize>> {
        cols.iter()
            .map(|col| {
                column_specs
                    .iter()
                    .position(|ColumnSpecification { column, .. }| {
                        column.name == col.name && column.table == col.table
                    })
                    .ok_or_else(|| internal_err!("could not find pkey column id for {:?}", col))
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

    Ok(DfNodeIndex::new(mig.add_base(name, columns, base)))
}

fn make_union_node(
    graph: &MirGraph,
    name: Relation,
    columns: &[Column],
    emit: &[Vec<Column>],
    ancestors: &[MirNodeIndex],
    duplicate_mode: ops::union::DuplicateMode,
    mig: &mut Migration<'_>,
) -> ReadySetResult<DfNodeIndex> {
    let mut emit_column_id: HashMap<NodeIndex, Vec<usize>> = HashMap::new();

    let mut cols = Vec::with_capacity(
        emit.get(0)
            .ok_or_else(|| internal_err!("No emit columns"))?
            .len(),
    );

    // column_id_for_column doesn't take into consideration table aliases
    // which might cause improper ordering of columns in a union node
    // eg. Q6 in finkelstein.txt
    for (i, n) in ancestors.iter().enumerate() {
        let emit_cols = emit
            .get(i)
            .ok_or_else(|| internal_err!("no index {} in emit cols {:?}", i, emit))?
            .iter()
            .map(|c| graph.column_id_for_column(*n, c))
            .collect::<ReadySetResult<Vec<_>>>()?;

        let ni = graph.resolve_dataflow_node(*n).ok_or_else(|| {
            ReadySetError::MirNodeMustHaveDfNodeAssigned {
                mir_node_index: n.index(),
            }
        })?;

        // Union takes columns of first ancestor
        if i == 0 {
            let parent_cols = mig.dataflow_state.ingredients[ni.address()].columns();
            cols = emit_cols
                .iter()
                .map(|i| {
                    parent_cols
                        .get(*i)
                        .cloned()
                        .ok_or_else(|| internal_err!("Invalid index"))
                })
                .collect::<ReadySetResult<Vec<_>>>()?;
        }

        emit_column_id.insert(ni.address(), emit_cols);
    }
    set_names(&column_names(columns), &mut cols)?;

    let node = mig.add_ingredient(
        name,
        cols,
        ops::union::Union::new(emit_column_id, duplicate_mode)?,
    );

    Ok(DfNodeIndex::new(node))
}

fn make_filter_node(
    graph: &MirGraph,
    name: Relation,
    parent: MirNodeIndex,
    columns: &[Column],
    conditions: Expr,
    custom_types: &HashMap<Relation, DfType>,
    mig: &mut Migration<'_>,
) -> ReadySetResult<DfNodeIndex> {
    let parent_na = graph.resolve_dataflow_node(parent).ok_or_else(|| {
        ReadySetError::MirNodeMustHaveDfNodeAssigned {
            mir_node_index: parent.index(),
        }
    })?;
    let mut parent_cols = mig.dataflow_state.ingredients[parent_na.address()]
        .columns()
        .to_vec();
    let filter_conditions = lower_expression(
        graph,
        parent,
        conditions,
        &parent_cols,
        custom_types,
        mig.dialect,
    )?;

    set_names(&column_names(columns), &mut parent_cols)?;

    let node = mig.add_ingredient(
        name,
        parent_cols,
        ops::filter::Filter::new(parent_na.address(), filter_conditions),
    );
    Ok(DfNodeIndex::new(node))
}

fn make_grouped_node(
    graph: &MirGraph,
    name: Relation,
    parent: MirNodeIndex,
    columns: &[Column],
    on: &Column,
    group_by: &[Column],
    kind: GroupedNodeType,
    mig: &mut Migration<'_>,
) -> ReadySetResult<DfNodeIndex> {
    invariant!(!group_by.is_empty());
    let parent_na = graph.resolve_dataflow_node(parent).ok_or_else(|| {
        ReadySetError::MirNodeMustHaveDfNodeAssigned {
            mir_node_index: parent.index(),
        }
    })?;
    let over_col_indx = graph.column_id_for_column(parent, on)?;
    let group_col_indx = group_by
        .iter()
        .map(|c| graph.column_id_for_column(parent, c))
        .collect::<ReadySetResult<Vec<_>>>()?;
    invariant!(!group_col_indx.is_empty());

    // Grouped projects the group_by columns followed by computed column
    let parent_cols = mig.dataflow_state.ingredients[parent_na.address()].columns();

    // group by columns
    let mut cols = group_col_indx
        .iter()
        .map(|i| {
            parent_cols
                .get(*i)
                .cloned()
                .ok_or_else(|| internal_err!("Invalid index"))
        })
        .collect::<ReadySetResult<Vec<_>>>()?;

    let over_col_ty = parent_cols
        .get(over_col_indx)
        .ok_or_else(|| internal_err!("Invalid index"))?
        .ty();
    let over_col_name = &columns
        .last()
        .ok_or_else(|| internal_err!("Grouped has no projections"))?
        .name;

    let make_agg_col =
        |ty: DfType| -> DfColumn { DfColumn::new(over_col_name.clone(), ty, Some(name.clone())) };

    let na = match kind {
        // This is the product of an incomplete refactor. It simplifies MIR to consider Group_Concat
        // to be an aggregation, however once we are in dataflow land the logic has not been
        // merged yet. For this reason, we need to pattern match for a groupconcat
        // aggregation before we pattern match for a generic aggregation.
        GroupedNodeType::Aggregation(Aggregation::GroupConcat { separator: sep }) => {
            let gc = GroupConcat::new(parent_na.address(), over_col_indx, group_col_indx, sep)?;
            let agg_col = make_agg_col(DfType::Text(/* TODO */ Collation::default()));
            cols.push(agg_col);
            set_names(&column_names(columns), &mut cols)?;
            mig.add_ingredient(name, cols, gc)
        }
        GroupedNodeType::Aggregation(agg) => {
            let grouped = agg.over(
                parent_na.address(),
                over_col_indx,
                group_col_indx.as_slice(),
                over_col_ty,
            )?;
            let agg_col = make_agg_col(grouped.output_col_type().or_ref(over_col_ty).clone());
            cols.push(agg_col);
            set_names(&column_names(columns), &mut cols)?;
            mig.add_ingredient(name, cols, grouped)
        }
        GroupedNodeType::Extremum(extr) => {
            let grouped = extr.over(
                parent_na.address(),
                over_col_indx,
                group_col_indx.as_slice(),
            );
            let agg_col = make_agg_col(grouped.output_col_type().or_ref(over_col_ty).clone());
            cols.push(agg_col);
            set_names(&column_names(columns), &mut cols)?;
            mig.add_ingredient(name, cols, grouped)
        }
    };
    Ok(DfNodeIndex::new(na))
}

fn make_identity_node(
    graph: &MirGraph,
    name: Relation,
    parent: MirNodeIndex,
    columns: &[Column],
    mig: &mut Migration<'_>,
) -> ReadySetResult<DfNodeIndex> {
    let parent_na = graph.resolve_dataflow_node(parent).ok_or_else(|| {
        ReadySetError::MirNodeMustHaveDfNodeAssigned {
            mir_node_index: parent.index(),
        }
    })?;
    // Identity mirrors the parent nodes exactly
    let mut parent_cols = mig.dataflow_state.ingredients[parent_na.address()]
        .columns()
        .to_vec();
    set_names(&column_names(columns), &mut parent_cols)?;

    let node = mig.add_ingredient(
        name,
        parent_cols,
        ops::identity::Identity::new(parent_na.address()),
    );
    Ok(DfNodeIndex::new(node))
}

/// Lower a join MIR node to dataflow
///
/// See [`MirNodeInner::Join`] for documentation on what `on_left`, `on_right`, and `project` mean
/// here
fn make_join_node(
    graph: &MirGraph,
    name: Relation,
    left: MirNodeIndex,
    right: MirNodeIndex,
    columns: &[Column],
    on: &[(Column, Column)],
    proj_cols: &[Column],
    kind: JoinType,
    custom_types: &HashMap<Relation, DfType>,
    mig: &mut Migration<'_>,
) -> ReadySetResult<DfNodeIndex> {
    let mut left_na = graph.resolve_dataflow_node(left).ok_or_else(|| {
        ReadySetError::MirNodeMustHaveDfNodeAssigned {
            mir_node_index: left.index(),
        }
    })?;
    let mut right_na = graph.resolve_dataflow_node(right).ok_or_else(|| {
        ReadySetError::MirNodeMustHaveDfNodeAssigned {
            mir_node_index: right.index(),
        }
    })?;

    let left_cols = mig.dataflow_state.ingredients[left_na.address()].columns();
    let right_cols = mig.dataflow_state.ingredients[right_na.address()].columns();

    let mut on_idxs = Vec::with_capacity(on.len());
    let mut emit = Vec::with_capacity(proj_cols.len());
    let mut cols = Vec::with_capacity(proj_cols.len());
    for c in proj_cols {
        if let Some(join_key_idx) = on.iter().position(|(left_col, _)| left_col == c) {
            // Column is a join key - find its index in the left and the index of the corresponding
            // column in the right, then add it to the join key, and project it from the left
            //
            // We check for columns in the left first here because we have to pick a side, but we
            // don't have to - we could check for the right first if we wanted to
            let l = graph
                .column_id_for_column(left, c)
                .map_err(|_| internal_err!("Left join column must exist in left parent"))?;
            let r = graph.column_id_for_column(right, &on[join_key_idx].1)?;
            on_idxs.push((l, r));
            emit.push((Side::Left, l));
            cols.push(
                left_cols
                    .get(l)
                    .cloned()
                    .ok_or_else(|| internal_err!("Invalid index"))?,
            );
        } else if let Ok(l) = graph.column_id_for_column(left, c) {
            // Column isn't a join key, and comes from the left
            emit.push((Side::Left, l));
            cols.push(
                left_cols
                    .get(l)
                    .cloned()
                    .ok_or_else(|| internal_err!("Invalid index"))?,
            );
        } else if let Ok(r) = graph.column_id_for_column(right, c) {
            // Column isn't a join key, and comes from the right
            emit.push((Side::Right, r));
            cols.push(
                right_cols
                    .get(r)
                    .cloned()
                    .ok_or_else(|| internal_err!("Invalid index"))?,
            );
        } else {
            internal!("Column {c} not found in either parent")
        }
    }

    set_names(&column_names(columns), &mut cols)?;

    // If we don't have any join condition, we're making a cross join.
    // Dataflow needs a non-empty join condition, so project out a constant value on both sides to
    // use as our join key
    if on.is_empty() {
        let mut make_cross_join_bogokey = |graph: &MirGraph, node: MirNodeIndex| {
            let mut node_columns = graph.columns(node);
            node_columns.push(Column::named("cross_join_bogokey"));

            make_project_node(
                graph,
                format!(
                    "{}_cross_join_bogokey",
                    graph[node].name().display_unquoted()
                )
                .into(),
                node,
                &node_columns,
                &graph.columns(node),
                &[],
                &[("cross_join_bogokey".into(), DfValue::from(0))],
                custom_types,
                mig,
            )
        };

        let left_col_idx = graph.columns(left).len();
        let right_col_idx = graph.columns(right).len();

        left_na = make_cross_join_bogokey(graph, left)?;
        right_na = make_cross_join_bogokey(graph, right)?;

        on_idxs.push((left_col_idx, right_col_idx));
        emit.push((Side::Left, left_col_idx));
        cols.push(DfColumn::new(
            "cross_join_bogokey".into(),
            DfType::BigInt,
            Some(name.clone()),
        ));
    }

    let j = Join::new(left_na.address(), right_na.address(), kind, on_idxs, emit);
    let n = mig.add_ingredient(name, cols, j);

    Ok(DfNodeIndex::new(n))
}

/// Joins two parent aggregate nodes together. Columns that are shared between both parents are
/// assumed to be group_by columns and all unique columns are considered to be the aggregate
/// columns themselves.
fn make_join_aggregates_node(
    graph: &MirGraph,
    name: Relation,
    left: MirNodeIndex,
    right: MirNodeIndex,
    columns: &[Column],
    mig: &mut Migration<'_>,
) -> ReadySetResult<DfNodeIndex> {
    let left_na = graph.resolve_dataflow_node(left).ok_or_else(|| {
        ReadySetError::MirNodeMustHaveDfNodeAssigned {
            mir_node_index: left.index(),
        }
    })?;
    let right_na = graph.resolve_dataflow_node(right).ok_or_else(|| {
        ReadySetError::MirNodeMustHaveDfNodeAssigned {
            mir_node_index: right.index(),
        }
    })?;
    let left_cols = mig.dataflow_state.ingredients[left_na.address()].columns();
    let right_cols = mig.dataflow_state.ingredients[right_na.address()].columns();

    let mut on = vec![];
    // We gather up all of the columns from each respective parent. If a column is in both parents,
    // then we know it was a group_by column and add it as a join key. Otherwise if the column is
    // exclusively in the left parent (such as the aggregate column itself), we make a Side::Left
    // with the left parent index. We finally iterate through the right parent and add the columns
    // that were exclusively in the right parent as Side::Right with the right parent index for
    // each given unique column.
    let project = graph
        .columns(left)
        .iter()
        .enumerate()
        .map(|(i, c)| {
            if let Ok(j) = graph.column_id_for_column(right, c) {
                // If the column was found in both, it's a group_by column and gets added as
                // a join key
                on.push((i, j));
                (Side::Left, i)
            } else {
                // Column exclusively in left parent, so gets added as coming from the left.
                (Side::Left, i)
            }
        })
        .chain(
            graph
                .columns(right)
                .iter()
                .enumerate()
                .filter_map(|(i, c)| {
                    // If column is in left, don't do anything it's already been added.
                    // If it's in right, add it with right index.
                    if graph.column_id_for_column(left, c).is_ok() {
                        None
                    } else {
                        // Column exclusively in right parent, so gets added as coming from the
                        // right.
                        Some((Side::Right, i))
                    }
                }),
        )
        .collect::<Vec<_>>();

    let mut cols = project
        .iter()
        .map(|(side, i)| match side {
            Side::Left => left_cols
                .get(*i)
                .cloned()
                .ok_or_else(|| internal_err!("Invalid index")),
            Side::Right => right_cols
                .get(*i)
                .cloned()
                .ok_or_else(|| internal_err!("Invalid index")),
        })
        .collect::<ReadySetResult<Vec<_>>>()?;

    set_names(&column_names(columns), &mut cols)?;

    // Always treated as a JoinType::Inner based on joining on group_by cols, which always match
    // between parents.
    let j = Join::new(
        left_na.address(),
        right_na.address(),
        JoinType::Inner,
        on,
        project,
    );
    let n = mig.add_ingredient(name, cols, j);

    Ok(DfNodeIndex::new(n))
}

#[derive(Clone)]
struct LowerContext<'a> {
    graph: &'a MirGraph,
    parent_node_idx: MirNodeIndex,
    parent_cols: &'a [DfColumn],
    custom_types: &'a HashMap<Relation, DfType>,
}

impl<'a> dataflow::LowerContext for LowerContext<'a> {
    fn resolve_column(&self, col: nom_sql::Column) -> ReadySetResult<(usize, DfType)> {
        let index = self.graph.column_id_for_column(
            self.parent_node_idx,
            &Column::new(col.table.clone(), &col.name),
        )?;
        let ty = self
            .parent_cols
            .get(index)
            .ok_or_else(|| internal_err!("Index exceeds length of parent cols, idx={}", index))?
            .ty()
            .clone();
        Ok((index, ty))
    }

    fn resolve_type(&self, ty: Relation) -> Option<DfType> {
        self.custom_types.get(&ty).cloned()
    }
}

/// Lower the given nom_sql AST expression to a `DfExpr`, resolving columns by looking their
/// index up in the given parent node.
fn lower_expression(
    graph: &MirGraph,
    parent: MirNodeIndex,
    expr: Expr,
    parent_cols: &[DfColumn],
    custom_types: &HashMap<Relation, DfType>,
    dialect: Dialect,
) -> ReadySetResult<DfExpr> {
    DfExpr::lower(
        expr,
        dialect,
        LowerContext {
            graph,
            parent_node_idx: parent,
            parent_cols,
            custom_types,
        },
    )
}

fn make_project_node(
    graph: &MirGraph,
    name: Relation,
    parent: MirNodeIndex,
    source_columns: &[Column],
    emit: &[Column],
    expressions: &[(SqlIdentifier, Expr)],
    literals: &[(SqlIdentifier, DfValue)],
    custom_types: &HashMap<Relation, DfType>,
    mig: &mut Migration<'_>,
) -> ReadySetResult<DfNodeIndex> {
    let parent_na = graph.resolve_dataflow_node(parent).ok_or_else(|| {
        ReadySetError::MirNodeMustHaveDfNodeAssigned {
            mir_node_index: parent.index(),
        }
    })?;
    let parent_cols = mig.dataflow_state.ingredients[parent_na.address()].columns();

    let projected_column_ids = emit
        .iter()
        .map(|c| {
            graph
                .find_source_for_child_column(parent, c)
                .ok_or_else(|| internal_err!("could not find source for child column: {:?}", c))
        })
        .collect::<Result<Vec<_>, _>>()?;

    let mut cols = projected_column_ids
        .iter()
        .map(|i| {
            parent_cols
                .get(*i)
                .cloned()
                .ok_or_else(|| internal_err!("Invalid index"))
        })
        .collect::<ReadySetResult<Vec<_>>>()?;

    // First set names for emitted columns. `set_names()` is not used because it assumes the two
    // fields have equal lengths
    let column_names = column_names(source_columns);
    for (c, n) in cols.iter_mut().zip(column_names) {
        c.set_name(n.into());
    }

    let (_, literal_values): (Vec<_>, Vec<_>) = literals.iter().cloned().unzip();

    let projected_expressions: Vec<DfExpr> = expressions
        .iter()
        .map(|(_, e)| {
            lower_expression(
                graph,
                parent,
                e.clone(),
                parent_cols,
                custom_types,
                mig.dialect,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;

    let col_names = source_columns
        .iter()
        .skip(cols.len())
        .map(|c| c.name.clone());

    let projected_expression_types = projected_expressions
        .iter()
        .map(|e| e.ty().clone())
        .collect::<Vec<_>>();

    let literal_types = literal_values
        .iter()
        .map(DfValue::infer_dataflow_type)
        .collect::<Vec<_>>();

    cols.extend(
        projected_expression_types
            .iter()
            .chain(literal_types.iter())
            .zip(col_names)
            .map(|(ty, n)| DfColumn::new(n, ty.clone(), Some(name.clone()))),
    );

    // Check here since we did not check in `set_names()`
    invariant_eq!(source_columns.len(), cols.len());

    let n = mig.add_ingredient(
        name,
        cols,
        Project::new(
            parent_na.address(),
            projected_column_ids.as_slice(),
            Some(literal_values),
            Some(projected_expressions),
        ),
    );
    Ok(DfNodeIndex::new(n))
}

fn make_distinct_node(
    graph: &MirGraph,
    name: Relation,
    parent: MirNodeIndex,
    columns: &[Column],
    group_by: &[Column],
    mig: &mut Migration<'_>,
) -> ReadySetResult<DfNodeIndex> {
    let parent_na = graph.resolve_dataflow_node(parent).ok_or_else(|| {
        ReadySetError::MirNodeMustHaveDfNodeAssigned {
            mir_node_index: parent.index(),
        }
    })?;
    let parent_cols = mig.dataflow_state.ingredients[parent_na.address()]
        .columns()
        .to_vec();

    let grp_by_column_ids = group_by
        .iter()
        .map(|c| {
            graph
                .find_source_for_child_column(parent, c)
                .ok_or_else(|| internal_err!("could not find source for child column: {:?}", c))
        })
        .collect::<Result<Vec<_>, _>>()?;

    let mut cols = grp_by_column_ids
        .iter()
        .map(|i| {
            parent_cols
                .get(*i)
                .cloned()
                .ok_or_else(|| internal_err!("Invalid index"))
        })
        .collect::<ReadySetResult<Vec<_>>>()?;

    // distinct count is projected last
    let distinct_count_name = columns
        .last()
        .ok_or_else(|| internal_err!("No projected columns for distinct"))?
        .name
        .clone();
    cols.push(DfColumn::new(
        distinct_count_name,
        DfType::BigInt,
        Some(name.clone()),
    ));
    set_names(&column_names(columns), &mut cols)?;

    let group_by_indx = if group_by.is_empty() {
        // no query parameters, so we index on the first column
        columns
            .iter()
            .map(|c| graph.column_id_for_column(parent, c))
            .collect::<ReadySetResult<Vec<_>>>()?
    } else {
        group_by
            .iter()
            .map(|c| graph.column_id_for_column(parent, c))
            .collect::<ReadySetResult<Vec<_>>>()?
    };

    // make the new operator and record its metadata
    let na = mig.add_ingredient(
        name,
        cols,
        // We're using Count to implement distinct here, because count already keeps track of how
        // many times we have seen a set of values. This means that if we get a row
        // deletion, we won't be removing it from our records of distinct rows unless there are no
        // remaining occurances of the set.
        //
        // We use 0 as a placeholder value
        Aggregation::Count.over(parent_na.address(), 0, &group_by_indx, &DfType::Unknown)?,
    );
    Ok(DfNodeIndex::new(na))
}

fn make_paginate_or_topk_node(
    graph: &MirGraph,
    name: Relation,
    parent: MirNodeIndex,
    columns: &[Column],
    order: &Option<Vec<(Column, OrderType)>>,
    group_by: &[Column],
    limit: usize,
    is_topk: bool,
    mig: &mut Migration<'_>,
) -> ReadySetResult<DfNodeIndex> {
    let parent_na = graph.resolve_dataflow_node(parent).ok_or_else(|| {
        ReadySetError::MirNodeMustHaveDfNodeAssigned {
            mir_node_index: parent.index(),
        }
    })?;
    let mut parent_cols = mig.dataflow_state.ingredients[parent_na.address()]
        .columns()
        .to_vec();

    // set names using MIR columns to ensure aliases are used
    let column_names = column_names(columns);
    // create page_number column if this is a paginate node
    if !is_topk {
        #[allow(clippy::unwrap_used)] // column_names must be populated
        parent_cols.push(DfColumn::new(
            column_names.last().unwrap().into(),
            DfType::BigInt,
            Some(name.clone()),
        ));
    }
    set_names(&column_names, &mut parent_cols)?;

    invariant!(
        !group_by.is_empty(),
        "need bogokey for Paginate without group columns"
    );

    let group_by_indx = group_by
        .iter()
        .map(|c| graph.column_id_for_column(parent, c))
        .collect::<ReadySetResult<Vec<_>>>()?;

    let cmp_rows = match *order {
        Some(ref o) => {
            o.iter()
                .map(|(c, order_type)| {
                    // SQL and Soup disagree on what ascending and descending order means, so do the
                    // conversion here.
                    let reversed_order_type = match *order_type {
                        OrderType::OrderAscending => OrderType::OrderDescending,
                        OrderType::OrderDescending => OrderType::OrderAscending,
                    };
                    graph
                        .column_id_for_column(parent, c)
                        .map(|id| (id, reversed_order_type))
                })
                .collect::<ReadySetResult<Vec<_>>>()?
        }
        None => Vec::new(),
    };

    // make the new operator and record its metadata
    let na = if is_topk {
        mig.add_ingredient(
            name,
            parent_cols,
            ops::topk::TopK::new(parent_na.address(), cmp_rows, group_by_indx, limit),
        )
    } else {
        mig.add_ingredient(
            name,
            parent_cols,
            ops::paginate::Paginate::new(parent_na.address(), cmp_rows, group_by_indx, limit),
        )
    };
    Ok(DfNodeIndex::new(na))
}

fn make_reader_processing(
    graph: &MirGraph,
    parent: &MirNodeIndex,
    order_by: &Option<Vec<(Column, OrderType)>>,
    limit: Option<usize>,
    returned_cols: &Option<Vec<Column>>,
    default_row: Option<Vec<DfValue>>,
    aggregates: &Option<PostLookupAggregates<Column>>,
) -> ReadySetResult<ReaderProcessing> {
    let order_by = if let Some(order) = order_by.as_ref() {
        Some(
            order
                .iter()
                .map(|(col, ot)| graph.column_id_for_column(*parent, col).map(|id| (id, *ot)))
                .collect::<ReadySetResult<Vec<(usize, OrderType)>>>()?,
        )
    } else {
        None
    };
    let returned_cols = if let Some(col) = returned_cols.as_ref() {
        let returned_cols = col
            .iter()
            .map(|col| (graph.column_id_for_column(*parent, col)))
            .collect::<ReadySetResult<Vec<_>>>()?;

        // In the future we will avoid reordering column, and must make sure that the returned
        // columns are a contiguous slice at the start of the row
        debug_assert!(returned_cols.iter().enumerate().all(|(i, v)| i == *v));

        Some(returned_cols)
    } else {
        None
    };

    let aggregates = aggregates
        .clone()
        .map(|aggs| aggs.map_columns(|col| graph.column_id_for_column(*parent, &col)))
        .transpose()?;

    ReaderProcessing::new(order_by, limit, returned_cols, default_row, aggregates)
}

fn materialize_leaf_node(
    graph: &MirGraph,
    parent: MirNodeIndex,
    name: Relation,
    key_cols: &[(Column, ViewPlaceholder)],
    index_type: IndexType,
    reader_processing: ReaderProcessing,
    mig: &mut Migration<'_>,
) -> ReadySetResult<()> {
    let na = graph.resolve_dataflow_node(parent).ok_or_else(|| {
        ReadySetError::MirNodeMustHaveDfNodeAssigned {
            mir_node_index: parent.index(),
        }
    })?;

    // we must add a new reader for this query. This also requires adding an identity node (at
    // least currently), since a node can only have a single associated reader. However, the
    // identity node exists at the MIR level, so we don't need to consider it here, as it has
    // already been added.

    // TODO(malte): consider the case when the projected columns need reordering

    if !key_cols.is_empty() {
        let columns: Vec<_> = key_cols
            .iter()
            .map(|(c, _)| graph.column_id_for_column(parent, c))
            .collect::<ReadySetResult<Vec<_>>>()?;

        let placeholder_map = key_cols
            .iter()
            .zip(columns.iter())
            .map(|((_, placeholder), col_index)| (*placeholder, *col_index))
            .collect::<Vec<_>>();

        mig.maintain(
            name,
            na.address(),
            &Index::new(index_type, columns),
            reader_processing,
            placeholder_map,
        );
    } else {
        // if no key specified, default to the first column
        mig.maintain(
            name,
            na.address(),
            &Index::new(index_type, vec![0]),
            reader_processing,
            Vec::default(),
        );
    }
    Ok(())
}
