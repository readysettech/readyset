use std::collections::{HashMap, HashSet};

use dataflow::{PostLookupAggregate, PostLookupAggregateFunction, PostLookupAggregates};
use mir::node::node_inner::MirNodeInner;
use mir::{Column, NodeIndex};
use nom_sql::analysis::ReferredColumns;
use nom_sql::FunctionExpr::*;
use nom_sql::{self, Expr, FieldDefinitionExpr, Relation, SqlIdentifier};
use readyset_errors::{unsupported, ReadySetError, ReadySetResult};
use readyset_sql_passes::is_aggregate;

use crate::controller::sql::mir::join::make_joins_for_aggregates;
use crate::controller::sql::mir::SqlToMirConverter;
use crate::controller::sql::query_graph::QueryGraph;

// Move predicates above grouped_by nodes
pub(super) fn make_predicates_above_grouped<'a>(
    mir_converter: &mut SqlToMirConverter,
    query_name: &Relation,
    name: Relation,
    qg: &QueryGraph,
    column_to_predicates: &HashMap<nom_sql::Column, Vec<&'a Expr>>,
    parent: &mut NodeIndex,
) -> ReadySetResult<Vec<&'a Expr>> {
    let mut created_predicates = Vec::new();

    for expr in qg.aggregates.keys() {
        for over_col in expr.referred_columns() {
            if over_col.table.is_none() {
                return Err(ReadySetError::NoSuchColumn(over_col.name.to_string()));
            }

            if column_to_predicates.contains_key(over_col) {
                let subquery_leaf = mir_converter.predicates_above_group_by(
                    query_name,
                    mir_converter.generate_label(&name),
                    column_to_predicates,
                    over_col,
                    *parent,
                    &mut created_predicates,
                )?;

                *parent = subquery_leaf;
            }
        }
    }

    Ok(created_predicates)
}

/// Normally, projection happens after grouped nodes - however, if aggregates used in grouped
/// expressions reference expressions rather than columns directly, we need to project them out
/// before the grouped nodes.
///
/// This does that projection, and returns a mapping from the expressions themselves to the names of
/// the columns they have been projected to
pub(super) fn make_expressions_above_grouped(
    mir_converter: &mut SqlToMirConverter,
    query_name: &Relation,
    name: &str,
    qg: &QueryGraph,
    prev_node: &mut NodeIndex,
) -> HashMap<Expr, SqlIdentifier> {
    let exprs: Vec<_> = qg
        .aggregates
        .iter()
        .map(|(f, _)| f)
        .filter(|&f| is_aggregate(f))
        .flat_map(|f| f.arguments())
        // We don't need to do any work for bare column expressions
        .filter(|arg| !matches!(arg, Expr::Column(_)))
        .map(|expr| (SqlIdentifier::from(expr.to_string()), expr.clone()))
        .collect();

    if !exprs.is_empty() {
        let cols = mir_converter.columns(*prev_node).to_vec();

        let node = mir_converter.make_project_node(
            query_name,
            mir_converter.generate_label(&name),
            *prev_node,
            cols,
            exprs.clone(),
            vec![],
        );
        *prev_node = node;
        exprs.into_iter().map(|(e, n)| (n, e)).collect()
    } else {
        HashMap::new()
    }
}

pub(super) fn make_grouped(
    mir_converter: &mut SqlToMirConverter,
    query_name: &Relation,
    name: Relation,
    qg: &QueryGraph,
    _: &HashMap<&Relation, NodeIndex>,
    prev_node: &mut NodeIndex,
    projected_exprs: &HashMap<Expr, SqlIdentifier>,
) -> ReadySetResult<Vec<NodeIndex>> {
    let mut agg_nodes: Vec<NodeIndex> = Vec::new();

    if qg.aggregates.is_empty() {
        // Don't need to do anything if we don't have any aggregates
        return Ok(vec![]);
    }
    for (function, alias) in &qg.aggregates {
        // We must also push parameter columns through the group by
        let over_cols = function.referred_columns().peekable();

        let name = mir_converter.generate_label(&name);

        let (parent_node, group_cols) = if !qg.group_by.is_empty() {
            // get any parameter columns that aren't also in the group-by
            // column set
            let param_cols: Vec<_> = qg.relations.values().fold(vec![], |acc, rel| {
                acc.into_iter()
                    .chain(
                        rel.parameters
                            .iter()
                            .map(|param| &param.col)
                            .filter(|c| !qg.group_by.contains(c)),
                    )
                    .collect()
            });
            // combine and dedup
            #[allow(clippy::needless_collect)] // necessary to avoid cloning param_cols
            let dedup_gb_cols: Vec<_> = qg
                .group_by
                .iter()
                .filter(|gbc| !param_cols.contains(gbc))
                .collect();
            let gb_and_param_cols = dedup_gb_cols
                .into_iter()
                .chain(param_cols.into_iter())
                .map(Column::from);

            let mut have_parent_cols = HashSet::new();
            // we cannot have duplicate columns at the data-flow level, as it confuses our
            // migration analysis code.
            let gb_and_param_cols = gb_and_param_cols
                .filter_map(|mut c| {
                    let pc = mir_converter
                        .columns(*prev_node)
                        .iter()
                        .position(|pc| *pc == c);
                    if let Some(pc) = pc {
                        if !have_parent_cols.contains(&pc) {
                            have_parent_cols.insert(pc);
                            let pc = mir_converter.columns(*prev_node)[pc].clone();
                            if pc.name != c.name || pc.table != c.table {
                                // remember the alias with the parent column
                                c.aliases.push(pc);
                            }
                            Some(c)
                        } else {
                            // we already have this column, so eliminate duplicate
                            None
                        }
                    } else {
                        Some(c)
                    }
                })
                .collect();

            (*prev_node, gb_and_param_cols)
        } else {
            let proj_cols_from_target_table = over_cols
                .flat_map(|col| &qg.relations[&col.table.clone().unwrap()].columns)
                .map(Column::from)
                .collect::<Vec<_>>();

            let (group_cols, parent_node) = if proj_cols_from_target_table.is_empty() {
                // slightly messy hack: if there are no group columns and the
                // table on which we compute has no projected columns in the
                // output, we make one up a group column by adding an extra
                // projection node
                let proj_name = format!("{}_prj_hlpr", name).into();
                let fn_cols: Vec<_> = function
                    .referred_columns()
                    .map(|c| Column::from(c.clone()))
                    .collect();
                let proj = mir_converter
                    .make_projection_helper(query_name, proj_name, *prev_node, fn_cols);

                agg_nodes.push(proj);

                let bogo_group_col = Column::named("grp");
                (vec![bogo_group_col], proj)
            } else {
                (proj_cols_from_target_table, *prev_node)
            };

            (parent_node, group_cols)
        };

        let nodes: Vec<NodeIndex> = mir_converter.make_aggregate_node(
            query_name,
            name,
            Column::named(alias.clone()),
            function.clone(),
            group_cols,
            parent_node,
            projected_exprs,
        )?;

        agg_nodes.extend(nodes);
    }

    let joinable_agg_nodes = joinable_aggregate_nodes(mir_converter, &agg_nodes);

    if joinable_agg_nodes.len() >= 2 {
        let join_nodes =
            make_joins_for_aggregates(mir_converter, query_name, &name.name, &joinable_agg_nodes)?;
        agg_nodes.extend(join_nodes);
    }

    if !agg_nodes.is_empty() {
        *prev_node = *agg_nodes.last().unwrap();
    }

    Ok(agg_nodes)
}

// joinable_aggregate_nodes will take in a list of aggregate nodes and return a list of aggregate
// nodes in the same order they appeared in the input list, and filter out nodes that should not be
// joined. For example, we could see a projection node appear as an aggregate node in the case:
//
// ```
// SELECT 5 * sum(col1)
// ```
//
// The projection node would represent the 5 * being applied to sum(col1), which we would not want
// to accidentally join.
fn joinable_aggregate_nodes(
    mir_converter: &SqlToMirConverter,
    agg_nodes: &[NodeIndex],
) -> Vec<NodeIndex> {
    agg_nodes
        .iter()
        .filter_map(|&node| match mir_converter.get_node(node).unwrap().inner {
            MirNodeInner::Aggregation { .. } => Some(node),
            MirNodeInner::Extremum { .. } => Some(node),
            _ => None,
        })
        .collect()
}

/// Build up the set of [`PostLookupAggregates`] for the given query, given as both the query
/// graph itself and the select statement that the query is built from.
///
/// This function is *not* responsible for determining whether the query *requires* post-lookup
/// aggregation - that's the responsibility of the caller. This function will only return [`None`]
/// if the query contains no aggregates.
pub(super) fn post_lookup_aggregates(
    query_graph: &QueryGraph,
    query_name: &Relation,
) -> ReadySetResult<Option<PostLookupAggregates<Column>>> {
    if query_graph.distinct {
        // DISTINCT is the equivalent of grouping by all projected columns but not actually doing
        // any aggregation function
        return Ok(Some(PostLookupAggregates {
            group_by: query_graph
                .fields
                .iter()
                .filter_map(|expr| match expr {
                    FieldDefinitionExpr::Expr {
                        alias: Some(alias), ..
                    } => Some(Column::named(alias.clone()).aliased_as_table(query_name.clone())),
                    FieldDefinitionExpr::Expr {
                        expr: Expr::Column(col),
                        ..
                    } => Some(Column::from(col).aliased_as_table(query_name.clone())),
                    FieldDefinitionExpr::Expr { expr, .. } => {
                        Some(Column::named(expr.to_string()).aliased_as_table(query_name.clone()))
                    }
                    _ => None,
                })
                .collect(),
            aggregates: vec![],
        }));
    }

    if query_graph.aggregates.is_empty() {
        return Ok(None);
    }

    let mut aggregates = vec![];
    for (function, alias) in &query_graph.aggregates {
        aggregates.push(PostLookupAggregate {
            column: Column::named(alias.clone()).aliased_as_table(query_name.clone()),
            function: match function {
                Avg { .. } => {
                    unsupported!("Average is not supported as a post-lookup aggregate")
                }
                // Count and sum are handled the same way, as re-aggregating counts is
                // done by just summing the numbers together
                Count { .. } | CountStar | Sum { .. } => PostLookupAggregateFunction::Sum,
                Max(_) => PostLookupAggregateFunction::Max,
                Min(_) => PostLookupAggregateFunction::Min,
                GroupConcat { separator, .. } => PostLookupAggregateFunction::GroupConcat {
                    separator: separator.clone(),
                },
                Call { .. } | Substring { .. } => continue,
            },
        });
    }

    Ok(Some(PostLookupAggregates {
        group_by: query_graph
            .group_by
            .iter()
            .map(|c| Column::from(c).aliased_as_table(query_name.clone()))
            .collect(),
        aggregates,
    }))
}
