#![deny(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unimplemented,
    clippy::unreachable
)]

use std::collections::HashMap;
use std::mem;

use common::DfValue;
use dataflow::node::Column as DfColumn;
use dataflow::ops::grouped::accumulator::Accumulator;
use dataflow::ops::join::{Join, JoinType};
use dataflow::ops::project::Project;
use dataflow::ops::window::{Window, WindowOperation, WindowOperationKind};
use dataflow::ops::Side;
use dataflow::{node, ops, Expr as DfExpr, PostLookupAggregates, ReaderProcessing};
use itertools::Itertools;
use mir::graph::MirGraph;
use mir::node::node_inner::MirNodeInner;
use mir::node::{GroupedNodeType, ProjectExpr, ViewKeyColumn};
use mir::query::MirQuery;
use mir::{Column, DfNodeIndex, NodeIndex as MirNodeIndex};
use petgraph::graph::NodeIndex;
use petgraph::Direction;
use readyset_client::internal::{Index, IndexType};
use readyset_client::ViewPlaceholder;
use readyset_data::{Collation, DfType, Dialect, SqlEngine};
use readyset_errors::{
    internal, internal_err, invalid_query, invariant, invariant_eq, unsupported, ReadySetError,
    ReadySetResult,
};
use readyset_sql::ast::{self, ColumnSpecification, Expr, NullOrder, OrderType, Relation};
use readyset_sql::TryIntoDialect as _;

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
                MirNodeInner::Accumulator {
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
                        GroupedNodeType::Accumulation(kind.clone()),
                        mig,
                    )?)
                }
                MirNodeInner::Aggregation {
                    ref on,
                    ref group_by,
                    ref kind,
                    ..
                } => {
                    invariant_eq!(ancestors.len(), 1);
                    antithesis_sdk::assert_reachable!("Create dataflow node for aggregation");
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
                } => {
                    antithesis_sdk::assert_reachable!("Create dataflow base node");
                    Some(make_base_node(
                        name,
                        column_specs.as_slice(),
                        custom_types,
                        primary_key.as_deref(),
                        unique_keys,
                        mig,
                    )?)
                }
                MirNodeInner::Extremum {
                    ref on,
                    ref group_by,
                    ref kind,
                    ..
                } => {
                    invariant_eq!(ancestors.len(), 1);
                    antithesis_sdk::assert_reachable!("Create dataflow node for extremum");
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
                MirNodeInner::Window {
                    ref partition_by,
                    ref group_by,
                    ref order_by,
                    ref function,
                    ref args,
                    ref output_column,
                } => {
                    invariant_eq!(ancestors.len(), 1);
                    antithesis_sdk::assert_reachable!("Create dataflow node for window");
                    Some(make_window_node(
                        graph,
                        name,
                        ancestors[0],
                        output_column,
                        group_by,
                        partition_by,
                        order_by,
                        *function,
                        args,
                        mig,
                    )?)
                }
                MirNodeInner::Filter { ref conditions } => {
                    invariant_eq!(ancestors.len(), 1);
                    antithesis_sdk::assert_reachable!("Create dataflow node for filter");
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
                    antithesis_sdk::assert_reachable!("Create dataflow node for identity");
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
                    antithesis_sdk::assert_reachable!("Create dataflow node for join");
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
                        mig,
                    )?)
                }
                MirNodeInner::JoinAggregates => {
                    invariant_eq!(ancestors.len(), 2);
                    antithesis_sdk::assert_reachable!("Create dataflow node for join aggregate");
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
                MirNodeInner::DependentJoin { .. } | MirNodeInner::DependentLeftJoin { .. } => {
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
                        antithesis_sdk::assert_reachable!("Create dataflow leaf node");
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
                    antithesis_sdk::assert_reachable!("Create dataflow node for left join");
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
                        mig,
                    )?)
                }
                MirNodeInner::Project { ref emit } => {
                    invariant_eq!(ancestors.len(), 1);
                    antithesis_sdk::assert_reachable!("Create dataflow project node");
                    let parent = ancestors[0];
                    Some(make_project_node(
                        graph,
                        name,
                        parent,
                        emit,
                        custom_types,
                        mig,
                    )?)
                }
                MirNodeInner::Union {
                    ref emit,
                    duplicate_mode,
                } => {
                    invariant_eq!(ancestors.len(), emit.len());
                    antithesis_sdk::assert_reachable!("Create dataflow node for union");
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
                    antithesis_sdk::assert_reachable!("Create dataflow distinct node");
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
                    antithesis_sdk::assert_reachable!("Create dataflow paginate/topk node");
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

    // note that this defaults to a "None" (= NULL) default value for columns that don't have a
    // default value or we cannot infer a default value from the SQL type
    let default_values = column_specs
        .iter()
        .map(|cs| {
            let collation = cs
                .get_collation()
                .map(|collation| Collation::get_or_default(mig.dialect, collation));
            if let Some(dv) = cs.get_default_value() {
                let df: DfValue = dv
                    .try_into_dialect(mig.dialect.into())
                    .map_err(|e| internal_err!("Failed to convert default value: {}", e))?;
                let dftype_to = DfType::from_sql_type(
                    &cs.sql_type,
                    mig.dialect,
                    |ty| custom_types.get(&ty).cloned(),
                    collation,
                )
                .map_err(|e| internal_err!("Failed to convert SQL type to DfType: {}", e))?;
                return df.coerce_to(&dftype_to, &df.infer_dataflow_type());
            } else if cs.is_not_null() && mig.dialect == readyset_sql::Dialect::MySQL.into() {
                // this branch is really only to support mysql minimal row-based replication.
                let dftype_to = DfType::from_sql_type(
                    &cs.sql_type,
                    mig.dialect,
                    |ty| custom_types.get(&ty).cloned(),
                    collation,
                )
                .map_err(|e| internal_err!("Failed to convert SQL type to DfType: {}", e))?;

                return Ok(DfValue::implicit_default(
                    collation.unwrap_or(Collation::Utf8),
                    dftype_to,
                    mig.dialect,
                ));
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

    let mut primary_key = primary_key.map(cols_from_spec).transpose()?;
    let mut unique_keys = unique_keys.to_vec();

    // If we don't have a primary key but have unique keys, check if any unique key
    // has all non-null columns and use it as primary key
    if primary_key.is_none() && !unique_keys.is_empty() {
        if let Some((i, uk)) = unique_keys.iter().enumerate().find(|(_, uk)| {
            uk.iter().all(|col| {
                column_specs
                    .iter()
                    .find(|cs| cs.column.name == col.name.as_str())
                    .is_some_and(|cs| cs.is_not_null())
            })
        }) {
            primary_key = Some(cols_from_spec(uk)?);
            unique_keys.swap_remove(i);
        }
    }
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
        emit.first()
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

fn make_window_node(
    graph: &MirGraph,
    name: Relation,
    parent: MirNodeIndex,
    output_column: &Column,
    group_by: &[Column],
    partition_by: &[Column],
    order_by: &[(Column, OrderType, NullOrder)],
    function: WindowOperationKind,
    args: &[Column],
    mig: &mut Migration<'_>,
) -> ReadySetResult<DfNodeIndex> {
    use WindowOperationKind::*;

    let parent_na = graph.resolve_dataflow_node(parent).ok_or_else(|| {
        ReadySetError::MirNodeMustHaveDfNodeAssigned {
            mir_node_index: parent.index(),
        }
    })?;

    let output_col_name = &output_column.name;

    let mut cols = mig.dataflow_state.ingredients[parent_na.address()]
        .columns()
        .to_vec();

    let find_input_type = || -> ReadySetResult<DfType> {
        if args.is_empty() {
            return Err(ReadySetError::Internal(
                "No args for window function".to_string(),
            ));
        }

        let arg_col = &args[0];
        let arg_col_id = graph.column_id_for_column(parent, arg_col)?;
        let current_cols = mig.dataflow_state.ingredients[parent_na.address()].columns();
        if arg_col_id >= current_cols.len() {
            return Err(ReadySetError::Internal(
                "Invalid column index for window function".to_string(),
            ));
        }

        Ok(current_cols[arg_col_id].ty().clone())
    };

    let output_type = match function {
        CountStar | Count | Rank | DenseRank | RowNumber => DfType::BigInt,
        Avg => {
            let ty = find_input_type()?;

            match ty {
                DfType::Int
                | DfType::TinyInt
                | DfType::SmallInt
                | DfType::BigInt
                | DfType::UnsignedInt
                | DfType::UnsignedTinyInt
                | DfType::UnsignedSmallInt
                | DfType::UnsignedBigInt
                | DfType::Numeric { .. } => DfType::DEFAULT_NUMERIC,
                DfType::Float | DfType::Double => DfType::Double,
                t => unsupported!("Unsupported type {t:?} for AVG window function"),
            }
        }
        Sum => {
            let ty = find_input_type()?;

            match mig.dialect.engine() {
                SqlEngine::MySQL => {
                    if ty.is_any_float() {
                        DfType::Double
                    } else if ty.is_any_int() || ty.is_numeric() {
                        DfType::DEFAULT_NUMERIC
                    } else {
                        invalid_query!("Cannot sum over type {}", ty)
                    }
                }
                SqlEngine::PostgreSQL => {
                    if ty.is_any_normal_int() {
                        DfType::BigInt
                    } else if ty.is_any_bigint() || ty.is_numeric() {
                        DfType::DEFAULT_NUMERIC
                    } else if ty.is_any_float() {
                        DfType::Double
                    } else {
                        invalid_query!("Cannot sum over type {}", ty)
                    }
                }
            }
        }
        Min | Max => {
            let ty = find_input_type()?;

            match ty {
                DfType::Int
                | DfType::TinyInt
                | DfType::SmallInt
                | DfType::MediumInt
                | DfType::BigInt
                | DfType::UnsignedTinyInt
                | DfType::UnsignedSmallInt
                | DfType::UnsignedMediumInt
                | DfType::UnsignedInt
                | DfType::UnsignedBigInt
                | DfType::Float
                | DfType::Double
                | DfType::Numeric { .. } => ty,
                t => unsupported!("Unsupported type {t:?} for Min/Max window functions"),
            }
        }
    };

    cols.push(DfColumn::new(
        output_col_name.clone(),
        output_type.clone(),
        Some(name.clone()),
    ));

    let group_col_indexes = group_by
        .iter()
        .map(|c| graph.column_id_for_column(parent, c))
        .collect::<ReadySetResult<Vec<_>>>()?;

    let partition_col_indexes = partition_by
        .iter()
        .map(|col| graph.column_id_for_column(parent, col))
        .collect::<ReadySetResult<Vec<_>>>()?;

    let order_col_indexes = order_by
        .iter()
        .map(|(col, ord, no)| {
            graph
                .column_id_for_column(parent, col)
                .map(|idx| (idx, *ord, *no))
        })
        .collect::<ReadySetResult<Vec<_>>>()?;

    let args_indexes = args
        .iter()
        .map(|col| graph.column_id_for_column(parent, col))
        .collect::<ReadySetResult<Vec<_>>>()?;

    let function = WindowOperation::from_fn(function, args_indexes)?;

    let output_col_index = cols.len() - 1;

    let window = Window::new(
        parent_na.address(),
        group_col_indexes,
        partition_col_indexes,
        order_col_indexes,
        function,
        output_col_index,
        output_type,
    )?;

    Ok(DfNodeIndex::new(mig.add_ingredient(name, cols, window)))
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
        GroupedNodeType::Accumulation(acc) => {
            let grouped = Accumulator::over(
                acc,
                parent_na.address(),
                over_col_indx,
                group_col_indx.as_slice(),
                over_col_ty,
                &mig.dialect,
            )?;
            let agg_col = make_agg_col(grouped.output_col_type().or_ref(over_col_ty).clone());
            cols.push(agg_col);
            set_names(&column_names(columns), &mut cols)?;
            mig.add_ingredient(name, cols, grouped)
        }
        GroupedNodeType::Aggregation(agg) => {
            let grouped = agg.over(
                parent_na.address(),
                over_col_indx,
                group_col_indx.as_slice(),
                over_col_ty,
                &mig.dialect,
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

fn should_swap_join_sides(
    left_cols: &[DfColumn],
    right_cols: &[DfColumn],
    on_idxs: &[(usize, usize)],
) -> ReadySetResult<bool> {
    let mut side_opt = None;
    if on_idxs.iter().all(|(l_idx, r_idx)| {
        match (
            left_cols[*l_idx].ty().is_any_text(),
            right_cols[*r_idx].ty().is_any_text(),
        ) {
            (true, false) => {
                if side_opt.is_none() {
                    side_opt = Some(Side::Left);
                } else if matches!(side_opt, Some(Side::Right)) {
                    return false;
                }
            }
            (false, true) => {
                if side_opt.is_none() {
                    side_opt = Some(Side::Right);
                } else if matches!(side_opt, Some(Side::Left)) {
                    return false;
                }
            }
            _ => {}
        }
        true
    }) {
        Ok(side_opt == Some(Side::Right))
    } else {
        Err(internal_err!("Can not swap join sides"))
    }
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

    let mut left_cols = mig.dataflow_state.ingredients[left_na.address()].columns();
    let mut right_cols = mig.dataflow_state.ingredients[right_na.address()].columns();

    let mut on_idxs = on
        .iter()
        .map(|(left_col, right_col)| {
            Ok((
                graph.column_id_for_column(left, left_col)?,
                graph.column_id_for_column(right, right_col)?,
            ))
        })
        .collect::<ReadySetResult<Vec<_>>>()?;

    let (left, right) = match should_swap_join_sides(left_cols, right_cols, &on_idxs) {
        Ok(true) => {
            if kind == JoinType::Inner {
                mem::swap(&mut left_na, &mut right_na);
                mem::swap(&mut left_cols, &mut right_cols);
                for item in on_idxs.iter_mut() {
                    *item = (item.1, item.0);
                }
                (right, left)
            } else {
                unsupported!("Swapping join sides is currently only supported for inner joins.");
            }
        }
        Err(e) => {
            return Err(e);
        }
        _ => (left, right),
    };

    let mut emit = Vec::with_capacity(proj_cols.len());
    let mut cols = Vec::with_capacity(proj_cols.len());
    for c in proj_cols {
        if let Ok(l) = graph.column_id_for_column(left, c) {
            // Column comes from the left.
            //
            // Note that if it's a join key it might come from the right *as well*, but that's fine
            // - we only need to project from one side (it's equal, by definition)
            emit.push((Side::Left, l));
            cols.push(
                left_cols
                    .get(l)
                    .cloned()
                    .ok_or_else(|| internal_err!("Invalid index"))?,
            );
        } else if let Ok(r) = graph.column_id_for_column(right, c) {
            // Column comes from the right
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

    // we need to check if the rhs parent is fully materialized, as this is needed for straddled join
    // upquery optimizations.
    let rhs_full_mat = mig.dataflow_state.ingredients[right_na.address()].is_full_mat();
    let j = Join::new(
        left_na.address(),
        right_na.address(),
        kind,
        on_idxs,
        emit,
        rhs_full_mat,
    );
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
    let mut left_cols = mig.dataflow_state.ingredients[left_na.address()].columns();
    let mut right_cols = mig.dataflow_state.ingredients[right_na.address()].columns();

    let mut on = vec![];
    // We gather up all of the columns from each respective parent. If a column is in both parents,
    // then we know it was a group_by column and add it as a join key. Otherwise if the column is
    // exclusively in the left parent (such as the aggregate column itself), we make a Side::Left
    // with the left parent index. We finally iterate through the right parent and add the columns
    // that were exclusively in the right parent as Side::Right with the right parent index for
    // each given unique column.
    let mut project = graph
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

    match should_swap_join_sides(left_cols, right_cols, &on) {
        Ok(true) => {
            mem::swap(&mut left_na, &mut right_na);
            mem::swap(&mut left_cols, &mut right_cols);
            for item in on.iter_mut() {
                *item = (item.1, item.0);
            }
            for item in project.iter_mut() {
                item.0 = if item.0 == Side::Left {
                    Side::Right
                } else {
                    Side::Left
                };
            }
        }
        Err(e) => {
            return Err(e);
        }
        _ => {}
    };

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

    // we need to check if the rhs parent is fully materialized, as this is needed for straddled join
    // upquery optimizations.
    let rhs_full_mat = mig.dataflow_state.ingredients[right_na.address()].is_full_mat();
    // Always treated as a JoinType::Inner based on joining on group_by cols, which always match
    // between parents.
    let j = Join::new(
        left_na.address(),
        right_na.address(),
        JoinType::Inner,
        on,
        project,
        rhs_full_mat,
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

impl dataflow::LowerContext for LowerContext<'_> {
    fn resolve_column(&self, col: ast::Column) -> ReadySetResult<(usize, DfType)> {
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
        &LowerContext {
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
    emit: &[ProjectExpr],
    custom_types: &HashMap<Relation, DfType>,
    mig: &mut Migration<'_>,
) -> ReadySetResult<DfNodeIndex> {
    let parent_na = graph.resolve_dataflow_node(parent).ok_or_else(|| {
        ReadySetError::MirNodeMustHaveDfNodeAssigned {
            mir_node_index: parent.index(),
        }
    })?;
    let parent_cols = mig.dataflow_state.ingredients[parent_na.address()].columns();

    let mut cols = Vec::with_capacity(emit.len());
    let mut exprs = Vec::with_capacity(emit.len());
    for expr in emit {
        let (name, ty, source, expr) = match expr {
            ProjectExpr::Column(c) => {
                let index = graph
                    .find_source_for_child_column(parent, c)
                    .ok_or_else(|| {
                        internal_err!("could not find source for child column: {:?}", c)
                    })?;
                let parent = &parent_cols[index];

                (
                    c.name.clone(),
                    parent.ty().clone(),
                    parent.source().cloned(),
                    DfExpr::Column {
                        index,
                        ty: parent.ty().clone(),
                    },
                )
            }
            ProjectExpr::Expr { expr, alias } => {
                let expr = lower_expression(
                    graph,
                    parent,
                    expr.clone(),
                    parent_cols,
                    custom_types,
                    mig.dialect,
                )?;

                (alias.clone(), expr.ty().clone(), None, expr)
            }
        };

        cols.push(DfColumn::new(name, ty, source));
        exprs.push(expr);
    }

    let n = mig.add_ingredient(name, cols, Project::new(parent_na.address(), exprs));
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
        // remaining occurrences of the set.
        //
        // We use 0 as a placeholder value
        Aggregation::Count.over(
            parent_na.address(),
            0,
            &group_by_indx,
            &DfType::Unknown,
            &mig.dialect,
        )?,
    );
    Ok(DfNodeIndex::new(na))
}

fn make_paginate_or_topk_node(
    graph: &MirGraph,
    name: Relation,
    parent: MirNodeIndex,
    columns: &[Column],
    order: &[(Column, OrderType, NullOrder)],
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

    let cmp_rows = order
        .iter()
        .map(|(c, order_type, null_ordering)| {
            // SQL and Readyset disagree on what ascending and descending order means, so do the
            // conversion here.
            let reversed_order_type = match *order_type {
                OrderType::OrderAscending => OrderType::OrderDescending,
                OrderType::OrderDescending => OrderType::OrderAscending,
            };

            let reversed_null_ordering = match *null_ordering {
                NullOrder::NullsFirst => NullOrder::NullsLast,
                NullOrder::NullsLast => NullOrder::NullsFirst,
            };

            graph
                .column_id_for_column(parent, c)
                .map(|id| (id, reversed_order_type, reversed_null_ordering))
        })
        .collect::<ReadySetResult<Vec<_>>>()?;

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
    order_by: &Option<Vec<(Column, OrderType, NullOrder)>>,
    limit: Option<usize>,
    returned_cols: &Option<Vec<Column>>,
    default_row: Option<Vec<DfValue>>,
    aggregates: &Option<PostLookupAggregates<Column>>,
) -> ReadySetResult<ReaderProcessing> {
    let order_by = if let Some(order) = order_by.as_ref() {
        Some(
            order
                .iter()
                .map(|(col, ot, no)| {
                    graph
                        .column_id_for_column(*parent, col)
                        .map(|id| (id, *ot, *no))
                })
                .collect::<ReadySetResult<Vec<_>>>()?,
        )
    } else {
        None
    };
    let returned_cols = if let Some(col) = returned_cols.as_ref() {
        let returned_cols = col
            .iter()
            .map(|col| graph.column_id_for_column(*parent, col))
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
