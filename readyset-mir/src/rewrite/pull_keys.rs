use nom_sql::BinaryOperator;
use readyset_client::ViewPlaceholder;
use readyset_errors::{internal, invariant_eq, unsupported, ReadySetResult};
use tracing::{instrument, trace};

use crate::node::{MirNodeInner, ViewKeyColumn};
use crate::query::MirQuery;
use crate::NodeIndex;

fn push_view_key(query: &mut MirQuery<'_>, node_idx: NodeIndex) -> ReadySetResult<()> {
    let children = query.descendants(node_idx)?;
    if children.len() != 1 {
        // TODO: unions; multi-aggregates
        unsupported!("Don't know how to push view key below multi-child node");
    }
    let child_idx = *children.get(0).unwrap();

    // If we're lifting past an AliasTable node, rewrite all columns referenced by the filter
    // condition that should resolve in that AliasTable node to use the correct table name
    if let MirNodeInner::AliasTable { table } = &query.get_node(child_idx).unwrap().inner {
        let table = table.clone();
        match &mut query.get_node_mut(node_idx).unwrap().inner {
            MirNodeInner::ViewKey { key } => {
                for ViewKeyColumn { column, .. } in key {
                    column.add_table_alias(table.clone());
                }
            }
            _ => internal!("The node passed to push_view_key must be a view_key"),
        }
    }

    let key = match &query.get_node_mut(node_idx).unwrap().inner {
        MirNodeInner::ViewKey { key } => key.clone(),
        _ => internal!("The node passed to push_view_key must be a view_key"),
    };

    match &mut query.get_node_mut(child_idx).unwrap().inner {
        MirNodeInner::Aggregation { group_by, .. }
        | MirNodeInner::Extremum { group_by, .. }
        | MirNodeInner::Distinct { group_by }
        | MirNodeInner::Paginate { group_by, .. }
        | MirNodeInner::TopK { group_by, .. } => {
            for ViewKeyColumn { column, op, .. } in &key {
                invariant_eq!(
                    *op,
                    BinaryOperator::Equal,
                    "TODO: support non-equal ops by adding post-lookup aggs"
                );
                group_by.push(column.clone());
            }
            query.swap_with_child(node_idx)?;
        }
        MirNodeInner::JoinAggregates => todo!(),
        // TODO: left joins are tricky if we're coming from the right side
        MirNodeInner::LeftJoin { .. } => unsupported!(
            "Parameters in subqueries on the right-hand side of LEFT JOIN not supported"
        ),
        // TODO: we might support this already? Will have to see
        MirNodeInner::Union { .. } => {
            unsupported!("Parameters on one side of a UNION not yet supported")
        }
        // Note that we don't need to add any projected columns; these will just be added by the
        // pull_columns pass
        MirNodeInner::Project { .. }
        | MirNodeInner::AliasTable { .. }
        | MirNodeInner::Join { .. }
        | MirNodeInner::DependentJoin { .. }
        | MirNodeInner::DependentLeftJoin { .. }
        | MirNodeInner::Filter { .. }
        | MirNodeInner::Identity => {
            trace!(
                "Pushing `{}` below `{}`",
                node_idx.index(),
                child_idx.index()
            );
            query.swap_with_child(node_idx)?;
        }
        MirNodeInner::ViewKey { key: their_key } => {
            trace!(
                "Merging key from `{}` with `{}`",
                node_idx.index(),
                child_idx.index()
            );
            their_key.extend(key);
            query.remove_node(node_idx)?;
        }
        MirNodeInner::Leaf { keys, .. } => {
            keys.extend(key.into_iter().map(
                |ViewKeyColumn {
                     column,
                     op,
                     placeholder_idx,
                 }| (column, ViewPlaceholder::OneToOne(placeholder_idx, op)),
            ));
            query.remove_node(node_idx)?;
        }
        MirNodeInner::Base { .. } => internal!("Encountered Base node with a parent (???)"),
    }

    Ok(())
}

#[instrument(level = "trace", skip_all, fields(query = %query.name().display_unquoted()))]
pub(crate) fn pull_view_keys_to_leaf(query: &mut MirQuery<'_>) -> ReadySetResult<()> {
    while let Some(view_key) = query
        .topo_nodes()
        .into_iter()
        .find(|&n| query.get_node(n).unwrap().inner.is_view_key())
    {
        trace!(node = ?view_key, "Pushing view key node");
        push_view_key(query, view_key)?;
    }

    Ok(())
}

#[cfg(test)]
#[allow(clippy::panic)]
mod tests {
    use common::IndexType;
    use nom_sql::{ColumnSpecification, Relation, SqlType};
    use vec1::vec1;

    use super::*;
    use crate::graph::MirGraph;
    use crate::node::MirNode;
    use crate::Column;

    #[test]
    fn simple_equality() {
        let mut graph = MirGraph::new();
        let query_name = Relation::from("q");

        let t = graph.add_node(MirNode::new(
            "t".into(),
            MirNodeInner::Base {
                column_specs: vec![ColumnSpecification {
                    column: "t.x".into(),
                    sql_type: SqlType::Int(None),
                    constraints: vec![],
                    comment: None,
                }],
                primary_key: None,
                unique_keys: Default::default(),
            },
        ));
        graph[t].add_owner(query_name.clone());

        let vk = graph.add_node(MirNode::new(
            "vk".into(),
            MirNodeInner::ViewKey {
                key: vec1![ViewKeyColumn {
                    column: Column::new(Some("t"), "x"),
                    op: BinaryOperator::Equal,
                    placeholder_idx: 1
                }],
            },
        ));
        graph[vk].add_owner(query_name.clone());
        graph.add_edge(t, vk, 0);

        let at = graph.add_node(MirNode::new(
            "at".into(),
            MirNodeInner::AliasTable { table: "sq".into() },
        ));
        graph[at].add_owner(query_name.clone());
        graph.add_edge(vk, at, 0);

        let leaf = graph.add_node(MirNode::new(
            "leaf".into(),
            MirNodeInner::leaf(vec![], IndexType::HashMap),
        ));
        graph[leaf].add_owner(query_name.clone());
        graph.add_edge(at, leaf, 0);

        let mut query = MirQuery::new(query_name, leaf, &mut graph);
        pull_view_keys_to_leaf(&mut query).unwrap();

        match &graph.node_weight(leaf).unwrap().inner {
            MirNodeInner::Leaf { keys, .. } => {
                assert!(keys.contains(&(
                    Column::new(Some("sq"), "x"),
                    ViewPlaceholder::OneToOne(1, BinaryOperator::Equal)
                )))
            }
            _ => panic!(),
        }
    }
}
