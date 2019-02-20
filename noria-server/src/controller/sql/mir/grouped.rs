use crate::controller::sql::mir::SqlToMirConverter;
use crate::controller::sql::query_graph::{QueryGraph, QueryGraphEdge};
use mir::{Column, MirNodeRef};
use nom_sql::FunctionExpression::*;
use nom_sql::{self, ConditionExpression, FunctionExpression};
use std::collections::{HashMap, HashSet};
use std::ops::Deref;

fn target_columns_from_computed_column(computed_col: &nom_sql::Column) -> Column {
    use nom_sql::FunctionExpression::*;

    match *computed_col.function.as_ref().unwrap().deref() {
        Avg(ref col, _)
        | Count(ref col, _)
        | GroupConcat(ref col, _)
        | Max(ref col)
        | Min(ref col)
        | Sum(ref col, _) => Column::from(col),
        CountStar => {
            // see comment re COUNT(*) rewriting in make_aggregation_node
            panic!("COUNT(*) should have been rewritten earlier!")
        }
    }
}

// Move predicates above grouped_by nodes
pub(super) fn make_predicates_above_grouped<'a>(
    mir_converter: &SqlToMirConverter,
    name: &str,
    qg: &QueryGraph,
    node_for_rel: &HashMap<&str, MirNodeRef>,
    node_count: usize,
    column_to_predicates: &HashMap<Column, Vec<&'a ConditionExpression>>,
    prev_node: &mut Option<MirNodeRef>,
) -> (Vec<&'a ConditionExpression>, Vec<MirNodeRef>) {
    let mut created_predicates = Vec::new();
    let mut predicates_above_group_by_nodes = Vec::new();
    let mut node_count = node_count;

    match qg.relations.get("computed_columns") {
        None => (),
        Some(computed_cols_cgn) => {
            for ccol in &computed_cols_cgn.columns {
                let over_col = target_columns_from_computed_column(ccol);
                let over_table = over_col.table.as_ref().unwrap().as_str();

                if column_to_predicates.contains_key(&over_col) {
                    let parent = match *prev_node {
                        Some(ref p) => p.clone(),
                        None => node_for_rel[over_table].clone(),
                    };

                    let new_mpns = mir_converter.predicates_above_group_by(
                        &format!("{}_n{}", name, node_count),
                        &column_to_predicates,
                        over_col,
                        parent,
                        &mut created_predicates,
                    );

                    node_count += predicates_above_group_by_nodes.len();
                    *prev_node = Some(new_mpns.last().unwrap().clone());
                    predicates_above_group_by_nodes.extend(new_mpns);
                }
            }
        }
    }

    (created_predicates, predicates_above_group_by_nodes)
}

pub(super) fn make_grouped(
    mir_converter: &SqlToMirConverter,
    name: &str,
    qg: &QueryGraph,
    node_for_rel: &HashMap<&str, MirNodeRef>,
    node_count: usize,
    prev_node: &mut Option<MirNodeRef>,
    is_reconcile: bool,
) -> Vec<MirNodeRef> {
    let mut func_nodes: Vec<MirNodeRef> = Vec::new();
    let mut node_count = node_count;

    match qg.relations.get("computed_columns") {
        None => (),
        Some(computed_cols_cgn) => {
            let gb_edges: Vec<_> = qg
                .edges
                .values()
                .filter(|e| match **e {
                    QueryGraphEdge::Join(_) | QueryGraphEdge::LeftJoin(_) => false,
                    QueryGraphEdge::GroupBy(_) => true,
                })
                .collect();

            for computed_col in computed_cols_cgn.columns.iter() {
                let computed_col = if is_reconcile {
                    let func = computed_col.function.as_ref().unwrap();
                    let new_func = match *func.deref() {
                        Sum(ref col, b) => {
                            let colname =
                                format!("{}.sum({})", col.table.as_ref().unwrap(), col.name);
                            FunctionExpression::Sum(nom_sql::Column::from(colname.as_ref()), b)
                        }
                        Count(ref col, b) => {
                            let colname =
                                format!("{}.count({})", col.clone().table.unwrap(), col.name);
                            FunctionExpression::Sum(nom_sql::Column::from(colname.as_ref()), b)
                        }
                        Max(ref col) => {
                            let colname =
                                format!("{}.max({})", col.clone().table.unwrap(), col.name);
                            FunctionExpression::Max(nom_sql::Column::from(colname.as_ref()))
                        }
                        Min(ref col) => {
                            let colname =
                                format!("{}.min({})", col.clone().table.unwrap(), col.name);
                            FunctionExpression::Min(nom_sql::Column::from(colname.as_ref()))
                        }
                        _ => unimplemented!(),
                    };

                    nom_sql::Column {
                        function: Some(Box::new(new_func)),
                        name: computed_col.name.clone(),
                        alias: computed_col.alias.clone(),
                        table: computed_col.table.clone(),
                    }
                } else {
                    computed_col.clone()
                };

                // We must also push parameter columns through the group by
                let over_col = target_columns_from_computed_column(&computed_col);
                let over_table = over_col.table.as_ref().unwrap().as_str();

                let parent_node = match *prev_node {
                    // If no explicit parent node is specified, we extract
                    // the base node from the "over" column's specification
                    None => node_for_rel[over_table].clone(),
                    // We have an explicit parent node (likely a projection
                    // helper), so use that
                    Some(ref node) => node.clone(),
                };

                let name = &format!("{}_n{}", name, node_count);

                let (parent_node, group_cols) = if !gb_edges.is_empty() {
                    // Function columns with GROUP BY clause
                    let mut gb_cols: Vec<&nom_sql::Column> = Vec::new();

                    for e in &gb_edges {
                        match **e {
                            QueryGraphEdge::GroupBy(ref gbc) => {
                                let table = gbc.first().unwrap().table.as_ref().unwrap();
                                assert!(gbc.iter().all(|c| c.table.as_ref().unwrap() == table));
                                gb_cols.extend(gbc);
                            }
                            _ => unreachable!(),
                        }
                    }

                    // get any parameter columns that aren't also in the group-by
                    // column set
                    let param_cols: Vec<_> = qg.relations.values().fold(vec![], |acc, rel| {
                        acc.into_iter()
                            .chain(rel.parameters.iter().filter(|c| !gb_cols.contains(c)))
                            .collect()
                    });
                    // combine and dedup
                    let dedup_gb_cols: Vec<_> = gb_cols
                        .into_iter()
                        .filter(|gbc| !param_cols.contains(gbc))
                        .collect();
                    let gb_and_param_cols: Vec<Column> = dedup_gb_cols
                        .into_iter()
                        .chain(param_cols.into_iter())
                        .map(Column::from)
                        .collect();

                    let mut have_parent_cols = HashSet::new();
                    // we cannot have duplicate columns at the data-flow level, as it confuses our
                    // migration analysis code.
                    let gb_and_param_cols = gb_and_param_cols
                        .into_iter()
                        .filter_map(|mut c| {
                            let pn = parent_node.borrow();
                            let pc = pn.columns().iter().position(|pc| *pc == c);
                            if pc.is_none() {
                                Some(c)
                            } else if !have_parent_cols.contains(&pc) {
                                have_parent_cols.insert(pc);
                                let pc = pn.columns()[pc.unwrap()].clone();
                                if pc.name != c.name || pc.table != c.table {
                                    // remember the alias with the parent column
                                    c.aliases.push(pc);
                                }
                                Some(c)
                            } else {
                                // we already have this column, so eliminate duplicate
                                None
                            }
                        })
                        .collect();

                    (parent_node, gb_and_param_cols)
                } else {
                    let proj_cols_from_target_table = &qg.relations[over_table].columns;

                    let (group_cols, parent_node) = if proj_cols_from_target_table.is_empty() {
                        // slightly messy hack: if there are no group columns and the
                        // table on which we compute has no projected columns in the
                        // output, we make one up a group column by adding an extra
                        // projection node
                        let proj_name = format!("{}_prj_hlpr", name);
                        let fn_col = target_columns_from_computed_column(&computed_col);

                        let proj =
                            mir_converter.make_projection_helper(&proj_name, parent_node, &fn_col);

                        func_nodes.push(proj.clone());
                        node_count += 1;

                        let bogo_group_col = Column::new(None, "grp");
                        (vec![bogo_group_col], proj)
                    } else {
                        (
                            proj_cols_from_target_table
                                .iter()
                                .map(Column::from)
                                .collect(),
                            parent_node,
                        )
                    };

                    (parent_node, group_cols)
                };

                let nodes: Vec<MirNodeRef> = mir_converter.make_function_node(
                    name,
                    &Column::from(computed_col),
                    group_cols.iter().collect(),
                    parent_node,
                );

                *prev_node = Some(nodes.last().unwrap().clone());
                node_count += nodes.len();
                func_nodes.extend(nodes);
            }
        }
    }

    func_nodes
}
