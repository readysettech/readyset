use common::DfValue;
use readyset_client::ViewPlaceholder;
use readyset_errors::{invariant_eq, ReadySetResult};
use tracing::trace;

use crate::node::{MirNode, MirNodeInner};
use crate::query::MirQuery;
use crate::Column;

/// If the given query has a Leaf but doesn't have any keys, create a key for it by adding a new
/// node to the query that projects out a constant literal value (a "bogokey", from "bogus key") and
/// making that the key for the query.
///
/// This pass will also handle ensuring that any topk or paginate nodes in leaf position in such
/// queries have `group_by` columns, by lifting the bogokey project node over those nodes and adding
/// the bogokey to their `group_by`
#[allow(dead_code)]
pub(crate) fn add_bogokey_if_necessary(query: &mut MirQuery<'_>) -> ReadySetResult<()> {
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
                emit: query.graph.columns(parent_idx),
                expressions: vec![],
                literals: vec![("bogokey".into(), DfValue::from(0i32))],
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
        group_by.push(Column::named("bogokey"))
    }

    Ok(())
}

#[cfg(test)]
#[allow(clippy::panic)]
mod tests {
    use common::IndexType;
    use nom_sql::{BinaryOperator, ColumnSpecification, Relation, SqlType};
    use petgraph::visit::EdgeRef;
    use petgraph::Direction;
    use readyset_client::ViewPlaceholder;

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
                    column: nom_sql::Column::from("a"),
                    sql_type: SqlType::Int(None),
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
            MirNodeInner::Project { literals, .. } => {
                assert_eq!(literals, &[("bogokey".into(), DfValue::from(0i32))])
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
                    column: nom_sql::Column::from("a"),
                    sql_type: SqlType::Int(None),
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
                order: None,
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
            MirNodeInner::Project { literals, .. } => {
                assert_eq!(literals, &[("bogokey".into(), DfValue::from(0i32))])
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
                    column: nom_sql::Column::from("a"),
                    sql_type: SqlType::Int(None),
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
}
