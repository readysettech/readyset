use std::collections::{HashMap, HashSet};

use dataflow::post_lookup::{
    PostLookupAggregate, PostLookupAggregateFunction, PostLookupAggregates,
};
use mir::node::node_inner::MirNodeInner;
use mir::{Column, MirNodeRef};
use nom_sql::analysis::ReferredColumns;
use nom_sql::FunctionExpression::*;
use nom_sql::{self, Expression, FieldDefinitionExpression, SelectStatement, SqlIdentifier};
use noria_errors::{unsupported, ReadySetError};
use noria_sql_passes::is_aggregate;

use crate::controller::sql::mir::join::make_joins_for_aggregates;
use crate::controller::sql::mir::SqlToMirConverter;
use crate::controller::sql::query_graph::QueryGraph;
use crate::ReadySetResult;

// Move predicates above grouped_by nodes
pub(super) fn make_predicates_above_grouped<'a>(
    mir_converter: &SqlToMirConverter,
    name: &SqlIdentifier,
    qg: &QueryGraph,
    node_for_rel: &HashMap<&SqlIdentifier, MirNodeRef>,
    node_count: usize,
    column_to_predicates: &HashMap<nom_sql::Column, Vec<&'a Expression>>,
    prev_node: &mut Option<MirNodeRef>,
) -> ReadySetResult<(Vec<&'a Expression>, Vec<MirNodeRef>)> {
    let mut created_predicates = Vec::new();
    let mut predicates_above_group_by_nodes = Vec::new();
    let mut node_count = node_count;

    for (expr, _) in &qg.aggregates {
        for over_col in expr.referred_columns() {
            let over_table = over_col
                .table
                .as_ref()
                .ok_or_else(|| ReadySetError::NoSuchColumn(over_col.name.to_string()))?;

            if column_to_predicates.contains_key(over_col) {
                let parent = match *prev_node {
                    Some(ref p) => p.clone(),
                    None => node_for_rel[over_table].clone(),
                };

                let new_mpns = mir_converter.predicates_above_group_by(
                    &format!("{}_n{}", name, node_count).into(),
                    column_to_predicates,
                    over_col,
                    parent,
                    &mut created_predicates,
                )?;

                node_count += predicates_above_group_by_nodes.len();
                if !new_mpns.is_empty() {
                    // TODO(ENG-939): updating prev_node here isn't quite right. But moving
                    // this doesn't solve all our problems either. See ticket.
                    *prev_node = Some(new_mpns.last().unwrap().clone());
                    predicates_above_group_by_nodes.extend(new_mpns);
                }
            }
        }
    }

    Ok((created_predicates, predicates_above_group_by_nodes))
}

/// Normally, projection happens after grouped nodes - however, if aggregates used in grouped
/// expressions reference expressions rather than columns directly, we need to project them out
/// before the grouped nodes.
///
/// This does that projection, and returns a mapping from the expressions themselves to the names of
/// the columns they have been projected to
pub(super) fn make_expressions_above_grouped(
    mir_converter: &SqlToMirConverter,
    name: &str,
    qg: &QueryGraph,
    node_count: usize,
    prev_node: &mut Option<MirNodeRef>,
) -> HashMap<Expression, SqlIdentifier> {
    let exprs: Vec<_> = qg
        .aggregates
        .iter()
        .map(|(f, _)| f)
        .filter(|f| is_aggregate(f))
        .flat_map(|f| f.arguments())
        // We don't need to do any work for bare column expressions
        .filter(|arg| !matches!(arg, Expression::Column(_)))
        .map(|expr| (SqlIdentifier::from(expr.to_string()), expr.clone()))
        .collect();

    if !exprs.is_empty() {
        let cols = prev_node.as_ref().unwrap().borrow().columns().to_vec();

        let node = mir_converter.make_project_node(
            &format!("{}_n{}", name, node_count).into(),
            prev_node.clone().unwrap(),
            cols,
            exprs.clone(),
            vec![],
        );
        *prev_node = Some(node);
        exprs.into_iter().map(|(e, n)| (n, e)).collect()
    } else {
        HashMap::new()
    }
}

pub(super) fn make_grouped(
    mir_converter: &SqlToMirConverter,
    name: &SqlIdentifier,
    qg: &QueryGraph,
    node_for_rel: &HashMap<&SqlIdentifier, MirNodeRef>,
    node_count: usize,
    prev_node: &mut Option<MirNodeRef>,
    projected_exprs: &HashMap<Expression, SqlIdentifier>,
) -> ReadySetResult<Vec<MirNodeRef>> {
    let mut agg_nodes: Vec<MirNodeRef> = Vec::new();
    let mut node_count = node_count;

    if qg.aggregates.is_empty() {
        // Don't need to do anything if we don't have any aggregates
        return Ok(vec![]);
    }
    for (function, alias) in &qg.aggregates {
        // We must also push parameter columns through the group by
        let mut over_cols = function.referred_columns().peekable();

        let parent_node = match *prev_node {
            // If no explicit parent node is specified, we extract
            // the base node from the "over" column's specification
            None => {
                // If we don't have a parent node yet, that means no joins or unions can
                // have happened yet, which means there *must* only be one table referred in
                // the aggregate expression. Let's just take the first.
                node_for_rel[over_cols.peek().unwrap().table.as_ref().unwrap()].clone()
            }
            // We have an explicit parent node (likely a projection
            // helper), so use that
            Some(ref node) => node.clone(),
        };

        let name = format!("{}_n{}", name, node_count).into();

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
                    let pn = parent_node.borrow();
                    let pc = pn.columns().iter().position(|pc| *pc == c);
                    if let Some(pc) = pc {
                        if !have_parent_cols.contains(&pc) {
                            have_parent_cols.insert(pc);
                            let pc = pn.columns()[pc].clone();
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

            (parent_node, gb_and_param_cols)
        } else {
            let proj_cols_from_target_table = over_cols
                .flat_map(|col| &qg.relations[col.table.as_ref().unwrap()].columns)
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
                // TODO(grfn) this double-collect is really gross- make_projection_helper takes
                // a Vec<&mir::Column> but we have a Vec<&nom_sql::Column> and there's no way to
                // make the former from the latter without doing some upsetting allocations
                let proj = mir_converter.make_projection_helper(&proj_name, parent_node, fn_cols);

                agg_nodes.push(proj.clone());
                node_count += 1;

                let bogo_group_col = Column::new(None, "grp");
                (vec![bogo_group_col], proj)
            } else {
                (proj_cols_from_target_table, parent_node)
            };

            (parent_node, group_cols)
        };

        let nodes: Vec<MirNodeRef> = mir_converter.make_aggregate_node(
            &name,
            Column::named(alias.clone()),
            function.clone(),
            group_cols,
            parent_node.clone(),
            projected_exprs,
        )?;

        node_count += nodes.len();
        agg_nodes.extend(nodes);
    }

    let joinable_agg_nodes = joinable_aggregate_nodes(&agg_nodes);

    if joinable_agg_nodes.len() >= 2 {
        let join_nodes =
            make_joins_for_aggregates(mir_converter, name, &joinable_agg_nodes, node_count)?;
        agg_nodes.extend(join_nodes);
    }

    if !agg_nodes.is_empty() {
        *prev_node = Some(agg_nodes.last().unwrap().clone());
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
fn joinable_aggregate_nodes(agg_nodes: &[MirNodeRef]) -> Vec<MirNodeRef> {
    agg_nodes
        .iter()
        .filter_map(|node| match node.borrow().inner {
            MirNodeInner::Aggregation { .. } => Some(node.clone()),
            MirNodeInner::Extremum { .. } => Some(node.clone()),
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
    qg: &QueryGraph,
    stmt: &SelectStatement,
) -> ReadySetResult<Option<PostLookupAggregates<Column>>> {
    if stmt.distinct {
        // DISTINCT is the equivalent of grouping by all projected columns but not actually doing
        // any aggregation function
        return Ok(Some(PostLookupAggregates {
            group_by: stmt
                .fields
                .iter()
                .filter_map(|expr| match expr {
                    FieldDefinitionExpression::Expression {
                        alias: Some(alias), ..
                    } => Some(Column::named(alias.clone())),
                    FieldDefinitionExpression::Expression {
                        expr: Expression::Column(col),
                        ..
                    } => Some(Column::from(col)),
                    FieldDefinitionExpression::Expression { expr, .. } => {
                        Some(Column::named(expr.to_string()))
                    }
                    _ => None,
                })
                .collect(),
            aggregates: vec![],
        }));
    }

    if qg.aggregates.is_empty() {
        return Ok(None);
    }

    let mut aggregates = vec![];
    for (function, alias) in &qg.aggregates {
        aggregates.push(PostLookupAggregate {
            column: Column::named(alias.clone()),
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
                Call { .. } => continue,
            },
        });
    }

    Ok(Some(PostLookupAggregates {
        group_by: qg.group_by.iter().map(|c| c.into()).collect(),
        aggregates,
    }))
}
