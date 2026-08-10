use std::iter;

use readyset_client::ViewPlaceholder;
use readyset_errors::{invariant_eq, ReadySetResult};
use readyset_sql::ast::Expr;
use tracing::trace;

use crate::node::node_inner::ProjectExpr;
use crate::node::{MirNode, MirNodeInner};
use crate::query::MirQuery;
use crate::{Column, NodeIndex};

/// A few scenarios where a bogokey (from "bogus key") is needed:
///
/// If the given query has a Leaf but doesn't have any keys, create a key for it by adding a new
/// node to the query that projects out a constant literal value (a "bogokey", from "bogus key") and
/// making that the key for the query.
///
/// This pass will also handle ensuring that any topk or paginate nodes in such queries have
/// `group_by` columns, by lifting the bogokey project node over those nodes and adding the
/// bogokey to their `group_by`
///
/// Consider a join node without a join condition — a cross join, or the LEFT JOIN that
/// subquery decorrelation produces for an uncorrelated subquery; however, all join nodes
/// require a join condition, so we add a bogokey projection to both of the join's parents and
/// use that as a filter condition.
///
/// A query computing more than one aggregate joins them back together, and that join takes its
/// keys from the group columns the aggregates share.  Aggregates that do not group share none, so
/// the same treatment applies: project a bogokey over each of them and group by it.  Three or more
/// aggregates chain such joins, and each one's bogokey already covers the next, so only the
/// ancestors still missing one get a projection.
pub(crate) fn add_bogokey_if_necessary(query: &mut MirQuery<'_>) -> ReadySetResult<()> {
    add_bogokey_leaf(query)?;
    add_bogokey_per_group_limit(query)?;
    add_bogokey_join(query)?;
    add_bogokey_join_aggregates(query)?;

    Ok(())
}

fn insert_bogokey_project_above(
    query: &mut MirQuery<'_>,
    node_to_insert_above: NodeIndex,
) -> ReadySetResult<NodeIndex> {
    let ancestors = query.ancestors(node_to_insert_above)?;
    invariant_eq!(ancestors.len(), 1);
    let parent_idx = *ancestors.first().unwrap();

    let bogo_project = query.insert_above(
        node_to_insert_above,
        MirNode::new(
            format!("{}_bogo_project", query.name().display_unquoted()).into(),
            MirNodeInner::Project {
                emit: query
                    .graph
                    .columns(parent_idx)
                    .into_iter()
                    .map(ProjectExpr::Column)
                    .chain(iter::once(ProjectExpr::Expr {
                        expr: Expr::Literal(0.into()),
                        alias: "bogokey".into(),
                    }))
                    .collect(),
            },
        ),
    )?;
    trace!(?bogo_project, "Added new bogokey project node");

    Ok(bogo_project)
}

fn add_bogokey_per_group_limit(query: &mut MirQuery<'_>) -> ReadySetResult<()> {
    let ungrouped = query
        .node_references()
        .filter(|(_, node)| {
            matches!(
                node,
                MirNode {
                    inner: MirNodeInner::TopK { group_by, .. }
                        | MirNodeInner::Paginate { group_by, .. },
                    ..
                } if group_by.is_empty()
            )
        })
        .map(|(idx, _)| idx)
        .collect::<Vec<_>>();

    ungrouped.iter().try_for_each(|idx| -> ReadySetResult<()> {
        invariant_eq!(query.ancestors(*idx)?.len(), 1);
        insert_bogokey_project_above(query, *idx)?;
        if let MirNodeInner::TopK { group_by, .. } | MirNodeInner::Paginate { group_by, .. } =
            &mut query.get_node_mut(*idx).unwrap().inner
        {
            group_by.push(Column::named("bogokey"));
        }
        Ok(())
    })?;

    Ok(())
}

fn add_bogokey_leaf(query: &mut MirQuery<'_>) -> ReadySetResult<()> {
    match &query.leaf_node().inner {
        MirNodeInner::Leaf { keys, .. } if keys.is_empty() => {}
        _ => {
            // Either the query has a Leaf with keys (so no bogokey is necessary) or the query has
            // no Leaf at all (which is the case for eg VIEWs). Either way, we don't need to do
            // anything
            return Ok(());
        }
    }

    // Find the node we're going to insert the bogokey project node above
    // Usually this'll be the first leaf project node.
    let mut node_to_insert_above = query.leaf();
    while let Some(parent) = query
        .ancestors(node_to_insert_above)?
        .first()
        .filter(|parent| {
            let inner = &query.get_node(**parent).unwrap().inner;
            matches!(inner, MirNodeInner::Project { .. })
        })
    {
        node_to_insert_above = *parent;
        invariant_eq!(query.ancestors(node_to_insert_above)?.len(), 1);
    }
    trace!(
        ?node_to_insert_above,
        "found node to insert bogo_project above"
    );

    insert_bogokey_project_above(query, node_to_insert_above)?;

    if let MirNodeInner::Leaf { keys, .. } = &mut query.leaf_node_mut().inner {
        keys.push((Column::named("bogokey"), ViewPlaceholder::Generated));
    }

    Ok(())
}

fn add_bogokey_join(query: &mut MirQuery<'_>) -> ReadySetResult<()> {
    let keyless_joins = query
        .node_references()
        .filter(|(_, node)| {
            matches!(
                node,
                MirNode {
                    inner: MirNodeInner::Join { on, .. } | MirNodeInner::LeftJoin { on, .. },
                    ..
                } if on.is_empty()
            )
        })
        .map(|(idx, _)| idx)
        .collect::<Vec<_>>();

    keyless_joins
        .iter()
        .try_for_each(|idx| -> ReadySetResult<()> {
            trace!(?idx, "Adding bogokey to keyless join");

            let ancestors = query.ancestors(*idx).unwrap();
            for (i, ancestor) in ancestors.into_iter().enumerate() {
                query.insert_below(
                    ancestor,
                    MirNode::new(
                        format!("{}_bogo_project_{}", query.name().display_unquoted(), i).into(),
                        MirNodeInner::Project {
                            emit: query
                                .graph
                                .columns(ancestor)
                                .into_iter()
                                .map(ProjectExpr::Column)
                                .chain(iter::once(ProjectExpr::Expr {
                                    expr: Expr::Literal(0.into()),
                                    alias: "bogokey".into(),
                                }))
                                .collect(),
                        },
                    ),
                )?;
            }

            match &mut query.get_node_mut(*idx).unwrap().inner {
                MirNodeInner::Join { on, project } | MirNodeInner::LeftJoin { on, project, .. } => {
                    on.push((Column::named("bogokey"), Column::named("bogokey")));
                    project.push(Column::named("bogokey"));
                }
                _ => unreachable!(),
            }

            Ok(())
        })?;

    Ok(())
}

/// Give a `JoinAggregates` without group columns something to join on.
fn add_bogokey_join_aggregates(query: &mut MirQuery<'_>) -> ReadySetResult<()> {
    let keyless_join_aggregates = query
        .topo_nodes()
        .into_iter()
        .filter(|idx| {
            matches!(
                query.get_node(*idx).map(|node| &node.inner),
                Some(MirNodeInner::JoinAggregates { group_by }) if group_by.is_empty()
            )
        })
        .collect::<Vec<_>>();

    keyless_join_aggregates
        .iter()
        .try_for_each(|idx| -> ReadySetResult<()> {
            trace!(
                ?idx,
                "Adding bogokey to JoinAggregates without group columns"
            );

            let ancestors = query.ancestors(*idx)?;
            invariant_eq!(ancestors.len(), 2);
            for (i, ancestor) in ancestors.into_iter().enumerate() {
                if query
                    .graph
                    .columns(ancestor)
                    .iter()
                    .any(|c| c.name == "bogokey")
                {
                    continue;
                }
                query.insert_below(
                    ancestor,
                    MirNode::new(
                        format!("{}_agg_bogo_project_{}", query.name().display_unquoted(), i)
                            .into(),
                        MirNodeInner::Project {
                            emit: query
                                .graph
                                .columns(ancestor)
                                .into_iter()
                                .map(ProjectExpr::Column)
                                .chain(iter::once(ProjectExpr::Expr {
                                    expr: Expr::Literal(0.into()),
                                    alias: "bogokey".into(),
                                }))
                                .collect(),
                        },
                    ),
                )?;
            }

            match &mut query.get_node_mut(*idx).unwrap().inner {
                MirNodeInner::JoinAggregates { group_by } => {
                    group_by.push(Column::named("bogokey"));
                }
                _ => unreachable!(),
            }

            Ok(())
        })?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use common::IndexType;
    use dataflow::ops::grouped::aggregate::Aggregation;
    use petgraph::visit::EdgeRef;
    use petgraph::Direction;
    use readyset_client::ViewPlaceholder;
    use readyset_sql::ast::{
        self, BinaryOperator, ColumnSpecification, Literal, NullOrder, OrderType, Relation, SqlType,
    };

    use super::*;
    use crate::graph::MirGraph;
    use crate::Column;

    #[test]
    fn query_needing_bogokey() {
        let query_name = Relation::from("query_needing_bogokey");
        let mut mir_graph = MirGraph::new();
        let base = mir_graph.add_node(MirNode::new(
            "base".into(),
            MirNodeInner::Base {
                column_specs: vec![ColumnSpecification {
                    column: ast::Column::from("a"),
                    sql_type: SqlType::Int(None),
                    generated: None,
                    constraints: vec![],
                    comment: None,
                    invisible: false,
                }],
                primary_key: Some([Column::from("a")].into()),
                unique_keys: Default::default(),
            },
        ));
        mir_graph[base].add_owner(query_name.clone());

        let alias_table = mir_graph.add_node(MirNode::new(
            "alias_table".into(),
            MirNodeInner::AliasTable {
                table: "query_needing_bogokey".into(),
            },
        ));
        mir_graph[alias_table].add_owner(query_name.clone());
        mir_graph.add_edge(base, alias_table, 0);

        let leaf = mir_graph.add_node(MirNode::new(
            "leaf".into(),
            MirNodeInner::leaf(vec![], IndexType::HashMap),
        ));
        mir_graph[leaf].add_owner(query_name.clone());
        mir_graph.add_edge(alias_table, leaf, 0);

        let mut query = MirQuery::new(query_name, leaf, &mut mir_graph);

        add_bogokey_if_necessary(&mut query).unwrap();

        match &query.leaf_node_mut().inner {
            MirNodeInner::Leaf { keys, .. } => {
                assert_eq!(keys.len(), 1)
            }
            _ => panic!(),
        }

        let bogokey_node = query
            .graph
            .edges_directed(query.leaf(), Direction::Incoming)
            .next()
            .unwrap()
            .source();
        match &query.get_node(bogokey_node).unwrap().inner {
            MirNodeInner::Project { emit } => {
                assert!(emit.iter().any(|emit| matches!(
                    emit,
                    ProjectExpr::Expr {
                        expr: Expr::Literal(Literal::Integer(0)),
                        alias
                    } if alias == "bogokey"
                )))
            }
            _ => panic!("bogo project node should be a Project"),
        }
    }

    #[test]
    fn query_needing_bogokey_with_topk() {
        let query_name = Relation::from("query_needing_bogokey_with_topk");
        let mut mir_graph = MirGraph::new();
        let base = mir_graph.add_node(MirNode::new(
            "base".into(),
            MirNodeInner::Base {
                column_specs: vec![ColumnSpecification {
                    column: ast::Column::from("a"),
                    sql_type: SqlType::Int(None),
                    generated: None,
                    constraints: vec![],
                    comment: None,
                    invisible: false,
                }],
                primary_key: Some([Column::from("a")].into()),
                unique_keys: Default::default(),
            },
        ));
        mir_graph[base].add_owner(query_name.clone());

        let alias_table = mir_graph.add_node(MirNode::new(
            "alias_table".into(),
            MirNodeInner::AliasTable {
                table: "query_needing_bogokey".into(),
            },
        ));
        mir_graph[alias_table].add_owner(query_name.clone());
        mir_graph.add_edge(base, alias_table, 0);

        let topk = mir_graph.add_node(MirNode::new(
            "topk".into(),
            MirNodeInner::TopK {
                order: vec![],
                group_by: vec![],
                limit: 3,
                topk_buffer_multiplier: None,
                query_name: query_name.clone(),
            },
        ));
        mir_graph[topk].add_owner(query_name.clone());
        mir_graph.add_edge(alias_table, topk, 0);

        let leaf = mir_graph.add_node(MirNode::new(
            "leaf".into(),
            MirNodeInner::leaf(vec![], IndexType::HashMap),
        ));
        mir_graph[leaf].add_owner(query_name.clone());
        mir_graph.add_edge(topk, leaf, 0);

        let mut query = MirQuery::new(query_name, leaf, &mut mir_graph);

        add_bogokey_if_necessary(&mut query).unwrap();

        match &query.leaf_node_mut().inner {
            MirNodeInner::Leaf { keys, .. } => {
                assert_eq!(keys.len(), 1)
            }
            _ => panic!(),
        }

        let bogokey_node = query
            .graph
            .edges_directed(topk, Direction::Incoming)
            .next()
            .unwrap()
            .source();
        match &query.get_node(bogokey_node).unwrap().inner {
            MirNodeInner::Project { emit } => {
                assert!(
                    emit.iter().any(|emit| matches!(
                        emit,
                        ProjectExpr::Expr {
                            expr: Expr::Literal(Literal::Integer(0)),
                            alias
                        } if alias == "bogokey"
                    )),
                    "{emit:?}"
                )
            }
            _ => panic!("bogo project node should be a Project"),
        }
    }

    #[test]
    fn query_not_needing_bogokey() {
        let query_name = Relation::from("query_needing_bogokey");
        let mut mir_graph = MirGraph::new();
        let base = mir_graph.add_node(MirNode::new(
            "base".into(),
            MirNodeInner::Base {
                column_specs: vec![ColumnSpecification {
                    column: ast::Column::from("a"),
                    sql_type: SqlType::Int(None),
                    generated: None,
                    constraints: vec![],
                    comment: None,
                    invisible: false,
                }],
                primary_key: Some([Column::from("a")].into()),
                unique_keys: Default::default(),
            },
        ));
        mir_graph[base].add_owner(query_name.clone());

        let alias_table = mir_graph.add_node(MirNode::new(
            "alias_table".into(),
            MirNodeInner::AliasTable {
                table: "query_needing_bogokey".into(),
            },
        ));
        mir_graph[alias_table].add_owner(query_name.clone());
        mir_graph.add_edge(base, alias_table, 0);

        let leaf = mir_graph.add_node(MirNode::new(
            "leaf".into(),
            MirNodeInner::leaf(
                vec![(
                    Column::named("b").aliased_as_table("unprojected_leaf_key"),
                    ViewPlaceholder::OneToOne(1, BinaryOperator::Equal),
                )],
                IndexType::HashMap,
            ),
        ));
        mir_graph[leaf].add_owner(query_name.clone());
        mir_graph.add_edge(alias_table, leaf, 0);

        let mut query = MirQuery::new(query_name, leaf, &mut mir_graph);

        add_bogokey_if_necessary(&mut query).unwrap();

        match &query.leaf_node_mut().inner {
            MirNodeInner::Leaf { keys, .. } => {
                assert_eq!(
                    keys,
                    &[(
                        Column::named("b").aliased_as_table("unprojected_leaf_key"),
                        ViewPlaceholder::OneToOne(1, BinaryOperator::Equal),
                    )]
                )
            }
            _ => panic!(),
        }
    }

    #[test]
    fn test_add_bogokey_to_cross_join_node() {
        let query_name = Relation::from("query_needing_bogokey");
        let mut mir_graph = MirGraph::new();

        let left = mir_graph.add_node(MirNode::new(
            "left_base".into(),
            MirNodeInner::Base {
                column_specs: vec![ColumnSpecification {
                    column: ast::Column::from("a"),
                    sql_type: SqlType::Int(None),
                    generated: None,
                    constraints: vec![],
                    comment: None,
                    invisible: false,
                }],
                primary_key: Some([Column::from("a")].into()),
                unique_keys: Default::default(),
            },
        ));
        mir_graph[left].add_owner(query_name.clone());

        let right = mir_graph.add_node(MirNode::new(
            "right_base".into(),
            MirNodeInner::Base {
                column_specs: vec![ColumnSpecification {
                    column: ast::Column::from("b"),
                    sql_type: SqlType::Int(None),
                    generated: None,
                    constraints: vec![],
                    comment: None,
                    invisible: false,
                }],
                primary_key: Some([Column::from("b")].into()),
                unique_keys: Default::default(),
            },
        ));
        mir_graph[right].add_owner(query_name.clone());

        let join_node = mir_graph.add_node(MirNode::new(
            "join_node".into(),
            MirNodeInner::Join {
                on: vec![], // Empty join condition (i.e. cross join)
                project: vec![Column::named("a"), Column::named("b")],
            },
        ));
        mir_graph[join_node].add_owner(query_name.clone());
        mir_graph.add_edge(left, join_node, 0);
        mir_graph.add_edge(right, join_node, 1);

        let mut query = MirQuery::new(query_name.clone(), join_node, &mut mir_graph);

        add_bogokey_if_necessary(&mut query).unwrap();

        match &query.get_node(join_node).unwrap().inner {
            MirNodeInner::Join { on, .. } => {
                assert_eq!(on.len(), 1, "Expected a bogo key to be added");
                assert_eq!(
                    on[0].0.name, "bogokey",
                    "Bogo key column name should be 'bogokey'"
                );
                assert_eq!(
                    on[0].1.name, "bogokey",
                    "Bogo key column name should be 'bogokey'"
                );
            }
            _ => panic!("Leaf node is not a Join node"),
        }

        // Helper closure to validate parent projections
        let check_projection_node = |parent_name: &str| {
            let parent = mir_graph
                .neighbors_directed(join_node, petgraph::Direction::Incoming)
                .inspect(|p| println!("Parent: {:?}", mir_graph[*p]))
                .find(|&n| mir_graph[n].name() == &Relation::from(parent_name))
                .unwrap_or_else(|| panic!("Expected a projection node for {parent_name}"));

            match &mir_graph[parent].inner {
                MirNodeInner::Project { emit, .. } => {
                    assert!(
                        emit.iter().any(
                            |c| matches!(c, ProjectExpr::Expr{ alias, ..} if alias == "bogokey")
                        ),
                        "{parent_name} projection should output bogokey"
                    );
                }
                _ => panic!("{parent_name} parent is not a Projection node"),
            }
        };

        check_projection_node(format!("{}_bogo_project_0", query_name.display_unquoted()).as_str());
        check_projection_node(format!("{}_bogo_project_1", query_name.display_unquoted()).as_str());
    }

    /// A Paginate with no grouping columns needs a bogokey for the same reason a TopK does:
    /// lowering asserts on `group_by` being populated.
    #[test]
    fn test_add_bogokey_to_paginate_without_group_columns() {
        let query_name = Relation::from("query_needing_bogokey");
        let mut mir_graph = MirGraph::new();

        let base = mir_graph.add_node(MirNode::new(
            "base".into(),
            MirNodeInner::Base {
                column_specs: vec![ColumnSpecification {
                    column: ast::Column::from("a"),
                    sql_type: SqlType::Int(None),
                    generated: None,
                    constraints: vec![],
                    comment: None,
                    invisible: false,
                }],
                primary_key: Some([Column::from("a")].into()),
                unique_keys: Default::default(),
            },
        ));
        mir_graph[base].add_owner(query_name.clone());

        let paginate = mir_graph.add_node(MirNode::new(
            "paginate".into(),
            MirNodeInner::Paginate {
                order: vec![(
                    Column::named("a"),
                    OrderType::OrderAscending,
                    NullOrder::NullsLast,
                )],
                group_by: vec![],
                limit: 10,
            },
        ));
        mir_graph[paginate].add_owner(query_name.clone());
        mir_graph.add_edge(base, paginate, 0);

        let mut query = MirQuery::new(query_name.clone(), paginate, &mut mir_graph);

        add_bogokey_if_necessary(&mut query).unwrap();

        match &query.get_node(paginate).unwrap().inner {
            MirNodeInner::Paginate { group_by, .. } => {
                assert_eq!(group_by.len(), 1, "Expected a bogo key to be added");
                assert_eq!(group_by[0].name, "bogokey");
            }
            _ => panic!("Leaf node is not a Paginate node"),
        }
    }

    /// Decorrelating an uncorrelated select-list subquery produces a LEFT JOIN with no ON
    /// clause. Like a cross join, it needs a bogokey to give the join operator something to
    /// match on.
    #[test]
    fn test_add_bogokey_to_keyless_left_join() {
        let query_name = Relation::from("query_needing_bogokey");
        let mut mir_graph = MirGraph::new();

        let mut base = |name: &str, col: &str| {
            let idx = mir_graph.add_node(MirNode::new(
                name.into(),
                MirNodeInner::Base {
                    column_specs: vec![ColumnSpecification {
                        column: ast::Column::from(col),
                        sql_type: SqlType::Int(None),
                        generated: None,
                        constraints: vec![],
                        comment: None,
                        invisible: false,
                    }],
                    primary_key: Some([Column::from(col)].into()),
                    unique_keys: Default::default(),
                },
            ));
            mir_graph[idx].add_owner(query_name.clone());
            idx
        };
        let left = base("left_base", "a");
        let right = base("right_base", "b");

        let join_node = mir_graph.add_node(MirNode::new(
            "join_node".into(),
            MirNodeInner::LeftJoin {
                on: vec![],
                project: vec![Column::named("a"), Column::named("b")],
                left_local_preds: vec![],
            },
        ));
        mir_graph[join_node].add_owner(query_name.clone());
        mir_graph.add_edge(left, join_node, 0);
        mir_graph.add_edge(right, join_node, 1);

        let mut query = MirQuery::new(query_name.clone(), join_node, &mut mir_graph);

        add_bogokey_if_necessary(&mut query).unwrap();

        match &query.get_node(join_node).unwrap().inner {
            MirNodeInner::LeftJoin { on, .. } => {
                assert_eq!(on.len(), 1, "Expected a bogo key to be added");
                assert_eq!(on[0].0.name, "bogokey");
                assert_eq!(on[0].1.name, "bogokey");
            }
            _ => panic!("Leaf node is not a LeftJoin node"),
        }
    }

    /// Two aggregates over one relation are stitched back together by a `JoinAggregates`, which
    /// takes its keys from the group columns they share.  Aggregates that group by nothing share
    /// none, so the join reaches dataflow with nothing to match on.
    #[test]
    fn test_add_bogokey_to_keyless_join_aggregates() {
        let query_name = Relation::from("query_needing_bogokey");
        let mut mir_graph = MirGraph::new();

        let base = mir_graph.add_node(MirNode::new(
            "base".into(),
            MirNodeInner::Base {
                column_specs: vec![ColumnSpecification {
                    column: ast::Column::from("a"),
                    sql_type: SqlType::Int(None),
                    generated: None,
                    constraints: vec![],
                    comment: None,
                    invisible: false,
                }],
                primary_key: Some([Column::from("a")].into()),
                unique_keys: Default::default(),
            },
        ));
        mir_graph[base].add_owner(query_name.clone());

        let mut aggregate = |name: &str, output: &str| {
            let idx = mir_graph.add_node(MirNode::new(
                name.into(),
                MirNodeInner::Aggregation {
                    on: Column::named("a"),
                    group_by: vec![],
                    output_column: Column::named(output),
                    kind: Aggregation::Count,
                },
            ));
            mir_graph[idx].add_owner(query_name.clone());
            mir_graph.add_edge(base, idx, 0);
            idx
        };
        let left = aggregate("count_a", "count(a)");
        let right = aggregate("count_b", "count(b)");

        let join_aggregates = mir_graph.add_node(MirNode::new(
            "join_aggregates".into(),
            MirNodeInner::JoinAggregates { group_by: vec![] },
        ));
        mir_graph[join_aggregates].add_owner(query_name.clone());
        mir_graph.add_edge(left, join_aggregates, 0);
        mir_graph.add_edge(right, join_aggregates, 1);

        let mut query = MirQuery::new(query_name.clone(), join_aggregates, &mut mir_graph);

        add_bogokey_if_necessary(&mut query).unwrap();

        match &query.get_node(join_aggregates).unwrap().inner {
            MirNodeInner::JoinAggregates { group_by } => {
                assert_eq!(group_by.len(), 1, "Expected a bogo key to be added");
                assert_eq!(group_by[0].name, "bogokey");
            }
            _ => panic!("Leaf node is not a JoinAggregates node"),
        }
    }

    /// Three aggregates chain two `JoinAggregates`.  The lower one's bogokey is already visible to
    /// the upper one, so projecting a second over it would leave the node one dataflow column wider
    /// than it has names for.
    #[test]
    fn test_add_bogokey_to_chained_keyless_join_aggregates() {
        let query_name = Relation::from("query_needing_bogokey");
        let mut mir_graph = MirGraph::new();

        let base = mir_graph.add_node(MirNode::new(
            "base".into(),
            MirNodeInner::Base {
                column_specs: vec![ColumnSpecification {
                    column: ast::Column::from("a"),
                    sql_type: SqlType::Int(None),
                    generated: None,
                    constraints: vec![],
                    comment: None,
                    invisible: false,
                }],
                primary_key: Some([Column::from("a")].into()),
                unique_keys: Default::default(),
            },
        ));
        mir_graph[base].add_owner(query_name.clone());

        let mut aggregate = |name: &str, output: &str| {
            let idx = mir_graph.add_node(MirNode::new(
                name.into(),
                MirNodeInner::Aggregation {
                    on: Column::named("a"),
                    group_by: vec![],
                    output_column: Column::named(output),
                    kind: Aggregation::Count,
                },
            ));
            mir_graph[idx].add_owner(query_name.clone());
            mir_graph.add_edge(base, idx, 0);
            idx
        };
        let first = aggregate("count_a", "count(a)");
        let second = aggregate("count_b", "count(b)");
        let third = aggregate("count_c", "count(c)");

        let mut join_aggregates = |left, right| {
            let idx = mir_graph.add_node(MirNode::new(
                "join_aggregates".into(),
                MirNodeInner::JoinAggregates { group_by: vec![] },
            ));
            mir_graph[idx].add_owner(query_name.clone());
            mir_graph.add_edge(left, idx, 0);
            mir_graph.add_edge(right, idx, 1);
            idx
        };
        let lower = join_aggregates(first, second);
        let upper = join_aggregates(lower, third);

        let mut query = MirQuery::new(query_name.clone(), upper, &mut mir_graph);

        add_bogokey_if_necessary(&mut query).unwrap();

        for idx in [lower, upper] {
            match &query.get_node(idx).unwrap().inner {
                MirNodeInner::JoinAggregates { group_by } => {
                    assert_eq!(group_by.len(), 1, "Expected a bogo key to be added");
                    assert_eq!(group_by[0].name, "bogokey");
                }
                _ => panic!("Node is not a JoinAggregates node"),
            }
        }

        // A second bogokey over a join that already carries one leaves its parent exposing the
        // column twice, which the join deduplicates into fewer names than it has columns.
        for idx in query.topo_nodes() {
            let columns = query.graph.columns(idx);
            for (i, column) in columns.iter().enumerate() {
                assert!(
                    !columns[..i]
                        .iter()
                        .any(|earlier| earlier.name == column.name),
                    "Node exposes {} twice",
                    column.name
                );
            }
        }
    }
}
