//! Refuse a compound parameter whose replay enters a join from the side opposite a grouped
//! relation (REA-6983).
//!
//! A join looks each incoming row up in the state of its other parent. It sends a replay for a
//! reader key to its left parent when that parent provides every column of the key, and to the
//! right parent only otherwise (`Join::column_source`), so the two orientations differ. When the
//! left parent is a grouped relation and the key holds a join column beside a column only the right
//! parent provides, the right parent answers the replay alone and the grouped side is never filled
//! for the join column. A row written after the cache exists then looks it up, misses, and is
//! dropped, and the reader answers from the hole from then on. Until the replay fills the grouped
//! side as well, such a query is left to the upstream.

use petgraph::Direction;
use readyset_errors::{unsupported, ReadySetResult};

use crate::node::MirNodeInner;
use crate::query::MirQuery;
use crate::{Column, NodeIndex};

/// Whether a join's lookup into `parent` reaches grouped state. Nodes that keep no state of their
/// own are walked past to the materialization behind them. The walk reads the whole graph rather
/// than this query's nodes, since a view's nodes belong to the view.
fn lookup_reaches_grouped_state(query: &MirQuery<'_>, mut parent: NodeIndex) -> bool {
    loop {
        let Some(node) = query.graph.node_weight(parent) else {
            return false;
        };
        match &node.inner {
            MirNodeInner::Project { .. }
            | MirNodeInner::Filter { .. }
            | MirNodeInner::AliasTable { .. }
            | MirNodeInner::Identity => {
                let mut ancestors = query.graph.neighbors_directed(parent, Direction::Incoming);
                let (Some(next), None) = (ancestors.next(), ancestors.next()) else {
                    return false;
                };
                parent = next;
            }
            MirNodeInner::Distinct { .. }
            | MirNodeInner::Aggregation { .. }
            | MirNodeInner::Extremum { .. }
            | MirNodeInner::Accumulator { .. }
            | MirNodeInner::TopK { .. }
            | MirNodeInner::Paginate { .. } => return true,
            _ => return false,
        }
    }
}

pub(super) fn refuse_parameter_on_grouped_join_key(query: &MirQuery<'_>) -> ReadySetResult<()> {
    let MirNodeInner::Leaf { keys, .. } = &query.leaf_node().inner else {
        return Ok(());
    };
    if keys.len() < 2 {
        return Ok(());
    }
    for join in query.topo_nodes() {
        let Some(MirNodeInner::Join { on, .. }) = query.get_node(join).map(|node| &node.inner)
        else {
            continue;
        };
        let ancestors = query.ancestors(join)?;
        let [left, right] = ancestors.as_slice() else {
            continue;
        };
        if !lookup_reaches_grouped_state(query, *left) {
            continue;
        }

        let is_join_column = |column: &Column| on.iter().any(|(l, r)| l == column || r == column);
        let Some((join_key, _)) = keys.iter().find(|(key, _)| is_join_column(key)) else {
            continue;
        };
        let left_columns = query.graph.columns(*left);
        let right_columns = query.graph.columns(*right);
        let Some((right_only, _)) = keys.iter().find(|(key, _)| {
            !is_join_column(key) && right_columns.contains(key) && !left_columns.contains(key)
        }) else {
            continue;
        };
        unsupported!(
            "a parameter on `{right_only}` beside join key `{join_key}` into a grouped relation is \
             not supported"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use common::IndexType;
    use dataflow::ops::grouped::aggregate::Aggregation;
    use readyset_client::ViewPlaceholder;
    use readyset_errors::ReadySetError;
    use readyset_sql::ast::{BinaryOperator, ColumnSpecification, Relation, SqlType};

    use super::*;
    use crate::graph::MirGraph;
    use crate::node::MirNode;

    /// Where the grouped relation sits in the join.
    #[derive(Clone, Copy, PartialEq)]
    enum Grouped {
        Left,
        Right,
        /// On the left, inside a view: the view owns its nodes, and the query that reads it shares
        /// only the alias table on top.
        LeftInView,
    }

    #[derive(Clone, Copy, PartialEq)]
    enum Kind {
        Inner,
        Left,
    }

    fn base(graph: &mut MirGraph, owner: &Relation, table: &str, columns: &[&str]) -> NodeIndex {
        let node = graph.add_node(MirNode::new(
            table.into(),
            MirNodeInner::Base {
                column_specs: columns
                    .iter()
                    .map(|column| ColumnSpecification {
                        column: format!("{table}.{column}").as_str().into(),
                        sql_type: SqlType::Int(None),
                        generated: None,
                        constraints: vec![],
                        comment: None,
                        invisible: false,
                    })
                    .collect(),
                primary_key: None,
                unique_keys: Default::default(),
            },
        ));
        graph[node].add_owner(owner.clone());
        node
    }

    fn col(table: &str, name: &str) -> Column {
        Column::new(Some(table), name)
    }

    fn param(table: &str, name: &str, idx: usize) -> (Column, ViewPlaceholder) {
        (
            col(table, name),
            ViewPlaceholder::OneToOne(idx, BinaryOperator::Equal),
        )
    }

    /// `votes` counted per user and post, joined to `posts` on `on`, under a leaf keyed by `keys`.
    fn check(
        grouped: Grouped,
        kind: Kind,
        on: Vec<(Column, Column)>,
        keys: Vec<(Column, ViewPlaceholder)>,
    ) -> ReadySetResult<()> {
        let mut graph = MirGraph::new();
        let query = Relation::from("q");
        let view = Relation::from("vc");
        let grouped_owner = if grouped == Grouped::LeftInView {
            &view
        } else {
            &query
        };
        let votes = base(&mut graph, grouped_owner, "votes", &["user_id", "post_id"]);
        let posts = base(
            &mut graph,
            &query,
            "posts",
            &["id", "author_id", "promoted"],
        );
        let mut counts = graph.add_node(MirNode::new(
            "vc".into(),
            MirNodeInner::Aggregation {
                on: col("votes", "post_id"),
                group_by: vec![col("votes", "user_id"), col("votes", "post_id")],
                output_column: col("vc", "c"),
                kind: Aggregation::Count,
            },
        ));
        graph[counts].add_owner(grouped_owner.clone());
        graph.add_edge(votes, counts, 0);
        if grouped == Grouped::LeftInView {
            let alias = graph.add_node(MirNode::new(
                "vc_alias".into(),
                MirNodeInner::AliasTable {
                    table: view.clone(),
                },
            ));
            graph[alias].add_owner(view);
            graph[alias].add_owner(query.clone());
            graph.add_edge(counts, alias, 0);
            counts = alias;
        }
        let project = vec![
            col("votes", "user_id"),
            col("votes", "post_id"),
            col("vc", "c"),
            col("posts", "id"),
            col("posts", "author_id"),
            col("posts", "promoted"),
        ];
        let join = graph.add_node(MirNode::new(
            "j".into(),
            match kind {
                Kind::Inner => MirNodeInner::Join { on, project },
                Kind::Left => MirNodeInner::LeftJoin {
                    on,
                    project,
                    left_local_preds: vec![],
                },
            },
        ));
        graph[join].add_owner(query.clone());
        let (left, right) = if grouped == Grouped::Right {
            (posts, counts)
        } else {
            (counts, posts)
        };
        graph.add_edge(left, join, 0);
        graph.add_edge(right, join, 1);
        let leaf = graph.add_node(MirNode::new(
            "leaf".into(),
            MirNodeInner::leaf(keys, IndexType::HashMap),
        ));
        graph[leaf].add_owner(query.clone());
        graph.add_edge(join, leaf, 0);
        refuse_parameter_on_grouped_join_key(&MirQuery::new(query, leaf, &mut graph))
    }

    fn on_user() -> Vec<(Column, Column)> {
        vec![(col("votes", "user_id"), col("posts", "author_id"))]
    }

    fn author_and_promoted() -> Vec<(Column, ViewPlaceholder)> {
        vec![
            param("posts", "author_id", 1),
            param("posts", "promoted", 2),
        ]
    }

    fn assert_refused(result: ReadySetResult<()>) {
        assert!(
            matches!(result, Err(ReadySetError::Unsupported(_))),
            "{result:?}"
        );
    }

    #[test]
    fn refuses_a_right_only_parameter_beside_a_join_key_into_a_grouped_left_side() {
        assert_refused(check(
            Grouped::Left,
            Kind::Inner,
            on_user(),
            author_and_promoted(),
        ));
    }

    #[test]
    fn refuses_the_same_key_into_a_grouped_view_on_the_left() {
        assert_refused(check(
            Grouped::LeftInView,
            Kind::Inner,
            on_user(),
            author_and_promoted(),
        ));
    }

    #[test]
    fn accepts_the_same_key_with_the_grouped_side_on_the_right() {
        check(
            Grouped::Right,
            Kind::Inner,
            on_user(),
            author_and_promoted(),
        )
        .unwrap();
    }

    #[test]
    fn leaves_a_left_join_alone() {
        check(Grouped::Left, Kind::Left, on_user(), author_and_promoted()).unwrap();
    }

    #[test]
    fn accepts_a_single_join_key_parameter() {
        let keys = vec![param("posts", "author_id", 1)];
        check(Grouped::Left, Kind::Inner, on_user(), keys).unwrap();
    }

    #[test]
    fn accepts_a_second_parameter_the_grouped_side_provides() {
        let keys = vec![param("posts", "author_id", 1), param("vc", "c", 2)];
        check(Grouped::Left, Kind::Inner, on_user(), keys).unwrap();
    }

    #[test]
    fn accepts_parameters_that_are_all_join_keys() {
        let on = vec![
            (col("votes", "user_id"), col("posts", "author_id")),
            (col("votes", "post_id"), col("posts", "id")),
        ];
        let keys = vec![param("posts", "author_id", 1), param("posts", "id", 2)];
        check(Grouped::Left, Kind::Inner, on, keys).unwrap();
    }

    #[test]
    fn accepts_parameters_off_the_join_key() {
        let keys = vec![param("posts", "id", 1), param("posts", "promoted", 2)];
        check(Grouped::Left, Kind::Inner, on_user(), keys).unwrap();
    }
}
