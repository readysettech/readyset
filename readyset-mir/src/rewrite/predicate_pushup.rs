use nom_sql::analysis::ReferredColumns;
use nom_sql::Expr;
use petgraph::Direction;
use readyset_errors::{invariant_eq, ReadySetResult};
use tracing::{instrument, trace};

use crate::node::MirNodeInner;
use crate::query::MirQuery;
use crate::{Column, NodeIndex};

fn commutes_with(conditions: &Expr, inner: &MirNodeInner) -> bool {
    match inner {
        MirNodeInner::Aggregation { group_by, .. }
        | MirNodeInner::Paginate { group_by, .. }
        | MirNodeInner::TopK { group_by, .. }
        | MirNodeInner::Distinct { group_by, .. }
        | MirNodeInner::Extremum { group_by, .. } => conditions
            .referred_columns()
            .all(|col| group_by.iter().any(|c| c == col)),

        MirNodeInner::Filter { .. }
        | MirNodeInner::Identity
        | MirNodeInner::Join { .. }
        | MirNodeInner::JoinAggregates
        | MirNodeInner::DependentJoin { .. }
        | MirNodeInner::ViewKey { .. }
        | MirNodeInner::Project { .. }
        | MirNodeInner::Union { .. }
        | MirNodeInner::AliasTable { .. }
        | MirNodeInner::Leaf { .. } => true,

        MirNodeInner::Base { .. }
        | MirNodeInner::LeftJoin { .. }
        | MirNodeInner::DependentLeftJoin { .. } => false,
    }
}

fn plan_push_filter(
    query: &MirQuery,
    filter_idx: NodeIndex,
    conditions: &Expr,
) -> ReadySetResult<Option<(NodeIndex, NodeIndex)>> {
    trace!(filter = %filter_idx.index(), "Planning pushup for filter");
    let ancestors = query.ancestors(filter_idx)?;
    if ancestors.is_empty() {
        // This node's ancestor might not be in the same query! If so, there's no pushup we can
        // do (since we can't remove existing nodes)
        return Ok(None);
    }
    invariant_eq!(
        ancestors.len(),
        1,
        "Filter nodes can only have one parent (node: {})",
        filter_idx.index()
    );
    let parent = ancestors[0];
    let mut new_parent = parent;
    let mut new_child = filter_idx;

    macro_rules! done {
        () => {
            if new_parent == parent {
                return Ok(None);
            } else {
                trace!(
                    filter = %filter_idx.index(),
                    new_parent = %new_parent.index(),
                    "Pushing filter"
                );
                return Ok(Some((new_parent, new_child)));
            }
        };
    }

    let required_columns = conditions
        .referred_columns()
        .map(Column::from)
        .collect::<Vec<_>>();

    loop {
        if !commutes_with(conditions, &query.get_node(new_parent).unwrap().inner) {
            done!()
        }

        if query
            .graph
            .edges_directed(new_parent, Direction::Outgoing)
            .count()
            > 1
        {
            trace!(
                new_parent = %new_parent.index(),
                "Can't push past parent node with more than one outgoing edge"
            );
            done!();
        }

        let ancestors = query.ancestors(new_parent)?;

        let (candidates, project_some_required_columns): (Vec<_>, Vec<_>) = ancestors
            .into_iter()
            // Select only ancestors that have at least some of the columns on the filter
            .filter(|n| {
                required_columns
                    .iter()
                    .any(|c| query.graph.provides_column(*n, c))
            })
            // Partition the ancestors into nodes that have all the required columns and nodes
            // that have only some of the columns on the filter
            .partition(|n| {
                required_columns
                    .iter()
                    .all(|c| query.graph.provides_column(*n, c))
            });

        match candidates.as_slice() {
            [] => done!(),
            // If there is only one candidate but there are other ancestors that project *some* of
            // the columns in the filter, we can't push the filter any further
            [_] if !project_some_required_columns.is_empty() => done!(),
            [candidate] => {
                trace!(ancestor = %candidate.index(), "Considering ancestor");
                new_child = new_parent;
                new_parent = *candidate;
            }
            ancestors => {
                // TODO(aspen): Maybe we can try duplicating the filter here?
                trace!(
                    ?ancestors,
                    "More than one ancestor has all columns; can't push yet"
                );
                done!()
            }
        }
    }
}

/// Push as many filter nodes as high as possible up the graph, to keep the inputs to expensive
/// nodes like joins as small as possible
#[instrument(level = "trace", skip_all)]
pub(crate) fn push_filters_up(query: &mut MirQuery<'_>) -> ReadySetResult<()> {
    for filter_idx in query.topo_nodes() {
        let node = query.get_node(filter_idx).unwrap();
        let MirNodeInner::Filter { conditions } = &node.inner else {
           continue;
       };

        if let Some((new_parent, new_child)) = plan_push_filter(query, filter_idx, conditions)? {
            let filter_node = query
                .remove_node(filter_idx)?
                .expect("Filter came from query");
            query.splice(new_parent, new_child, filter_node)?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use common::IndexType;
    use nom_sql::{BinaryOperator, ColumnSpecification, Relation, SqlType};

    use super::*;
    use crate::graph::MirGraph;
    use crate::node::MirNode;
    use crate::visualize::GraphViz;
    use crate::Column;

    #[test]
    fn local_pred_below_inner_join() {
        readyset_tracing::init_test_logging();
        let query_name = Relation::from("local_pred_below_inner_join");
        let mut graph = MirGraph::new();

        let t1 = graph.add_node(MirNode::new(
            "t1".into(),
            MirNodeInner::Base {
                column_specs: vec![ColumnSpecification {
                    column: nom_sql::Column::from("t1.a"),
                    sql_type: SqlType::Int(None),
                    constraints: vec![],
                    comment: None,
                }],
                primary_key: Some([Column::from("a")].into()),
                unique_keys: Default::default(),
            },
        ));
        graph[t1].add_owner(query_name.clone());

        let t2 = graph.add_node(MirNode::new(
            "t2".into(),
            MirNodeInner::Base {
                column_specs: vec![
                    ColumnSpecification {
                        column: nom_sql::Column::from("t2.a"),
                        sql_type: SqlType::Int(None),
                        constraints: vec![],
                        comment: None,
                    },
                    ColumnSpecification {
                        column: nom_sql::Column::from("t2.b"),
                        sql_type: SqlType::Int(None),
                        constraints: vec![],
                        comment: None,
                    },
                ],
                primary_key: Some([Column::new(Some("t2"), "a")].into()),
                unique_keys: Default::default(),
            },
        ));
        graph[t2].add_owner(query_name.clone());
        // t2 -> ...

        let join = graph.add_node(MirNode::new(
            "join".into(),
            MirNodeInner::Join {
                on: vec![(Column::new(Some("t1"), "a"), Column::new(Some("t2"), "a"))],
                project: vec![
                    Column::new(Some("t1"), "a").aliased_as_table("t2"),
                    Column::new(Some("t2"), "b"),
                ],
            },
        ));
        graph[join].add_owner(query_name.clone());
        graph.add_edge(t1, join, 0);
        graph.add_edge(t2, join, 1);

        let filter = graph.add_node(MirNode::new(
            "filter".into(),
            MirNodeInner::Filter {
                conditions: Expr::BinaryOp {
                    lhs: Box::new(Expr::Column("t2.b".into())),
                    op: BinaryOperator::Equal,
                    rhs: Box::new(Expr::Literal(1.into())),
                },
            },
        ));
        graph[filter].add_owner(query_name.clone());
        graph.add_edge(join, filter, 0);

        let leaf = graph.add_node(MirNode::new(
            "q".into(),
            MirNodeInner::leaf(vec![], IndexType::HashMap),
        ));
        graph[leaf].add_owner(query_name.clone());
        graph.add_edge(filter, leaf, 0);

        let mut query = MirQuery::new(query_name, leaf, &mut graph);

        push_filters_up(&mut query).unwrap();
        eprintln!("{}", query.to_graphviz());

        query
            .get_node(filter)
            .expect("Filter should still be in graph");
        query
            .graph
            .find_edge(t2, filter)
            .expect("Filter should be a direct child of t2");
        query
            .graph
            .find_edge(filter, join)
            .expect("Filter should be a direct parent of join");
        assert!(
            query.graph.find_edge(t2, join).is_none(),
            "No edge should exist from t2 to join"
        );
    }
}
