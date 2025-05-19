use std::iter;

use readyset_client::ViewPlaceholder;
use readyset_errors::{invariant_eq, ReadySetResult};
use readyset_sql::ast::Expr;
use tracing::trace;

use crate::node::node_inner::ProjectExpr;
use crate::node::{MirNode, MirNodeInner};
use crate::query::MirQuery;
use crate::Column;

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
    //
    // Usually this'll be the first leaf project node, but in the case of topk or paginate with an
    // empty group_by we insert above those instead, since those both happen to need a group_by.
    let mut node_to_insert_above = query.leaf();
    while let Some(parent) = query
        .ancestors(node_to_insert_above)?
        .first()
        .filter(|parent| {
            let inner = &query.get_node(**parent).unwrap().inner;
            matches!(inner, MirNodeInner::Project { .. })
                || matches!(
                    inner,
                    MirNodeInner::TopK { group_by, .. }
                    | MirNodeInner::Paginate { group_by, .. }
                    if group_by.is_empty()
                )
        })
    {
        node_to_insert_above = *parent;
        invariant_eq!(query.ancestors(node_to_insert_above)?.len(), 1);
    }
    trace!(
        ?node_to_insert_above,
        "found node to insert bogo_project above"
    );

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

    if let MirNodeInner::Leaf { keys, .. } = &mut query.leaf_node_mut().inner {
        keys.push((Column::named("bogokey"), ViewPlaceholder::Generated))
    }

    if let MirNodeInner::TopK { group_by, .. } =
        &mut query.get_node_mut(node_to_insert_above).unwrap().inner
    {
        // TODO: Move this up once the if-let chains are stabilized in Rust
        if group_by.is_empty() {
            group_by.push(Column::named("bogokey"))
        }
    };

    Ok(())
}

fn add_bogokey_join(query: &mut MirQuery<'_>) -> ReadySetResult<()> {
    let cross_joins = query
        .node_references()
        .filter(|(_, node)| {
            matches!(
                node,
                MirNode {
                    inner: MirNodeInner::Join { on, .. },
                    ..
                } if on.is_empty()
            )
        })
        .map(|(idx, _)| idx)
        .collect::<Vec<_>>();

    cross_joins
        .iter()
        .try_for_each(|idx| -> ReadySetResult<()> {
            trace!(?idx, "Adding bogokey to cross join");

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
                MirNodeInner::Join { on, project } => {
                    on.push((Column::named("bogokey"), Column::named("bogokey")));
                    project.push(Column::named("bogokey"));
                }
                _ => unreachable!(),
            }

            Ok(())
        })?;

    Ok(())
}

/// A few scenarios where a bogokey (from "bogus key") is needed:
///
/// If the given query has a Leaf but doesn't have any keys, create a key for it by adding a new
/// node to the query that projects out a constant literal value (a "bogokey", from "bogus key") and
/// making that the key for the query.
///
/// This pass will also handle ensuring that any topk or paginate nodes in leaf position in such
/// queries have `group_by` columns, by lifting the bogokey project node over those nodes and adding
/// the bogokey to their `group_by`
///
/// Consider a join node without a join condition (i.e. a cross-join); however, all join nodes
/// require a join condition, so we add a bogokey projection to both of the join's parents and
/// use that as a filter condition..
pub(crate) fn add_bogokey_if_necessary(query: &mut MirQuery<'_>) -> ReadySetResult<()> {
    add_bogokey_leaf(query)?;
    add_bogokey_join(query)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use common::IndexType;
    use petgraph::visit::EdgeRef;
    use petgraph::Direction;
    use readyset_client::ViewPlaceholder;
    use readyset_sql::ast::{
        self, BinaryOperator, ColumnSpecification, Literal, Relation, SqlType,
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
}
