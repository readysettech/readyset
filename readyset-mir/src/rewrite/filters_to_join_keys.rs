use std::collections::BTreeMap;

use nom_sql::{BinaryOperator, Expr};
use readyset_errors::{internal, internal_err, ReadySetResult};
use tracing::{trace, trace_span};

use crate::node::MirNodeInner;
use crate::query::MirQuery;
use crate::Column;

/// Optimization: Convert all filters in the query that *could* be join keys in a join in their
/// parent (because they compare a column on the lhs of the join to a column on the rhs of the join)
/// into join keys.
///
/// For example, this will convert the equivalent of the following query:
///
/// ```sql
/// SELECT * FROM t1, t2 WHERE t1.x = t2.y
/// ```
///
/// into the equivalent of:
///
/// ```sql
/// SELECT * FROM t1 JOIN t2 ON t1.x = t2.y
/// ```
pub(crate) fn convert_filters_to_join_keys(query: &mut MirQuery<'_>) -> ReadySetResult<()> {
    // We'll be constructing a map from join_idx -> Vec<(filter_idx, (left_join_col,
    // right_join_col))>
    let mut filters_to_add = BTreeMap::<_, Vec<_>>::new();

    // First, loop through all the filters in the query where the condition compares one column
    // against another column
    'filter: for (filter_idx, node) in query.node_references() {
        let (mut c1, mut c2) = if let MirNodeInner::Filter {
            conditions:
                Expr::BinaryOp {
                    lhs: box Expr::Column(c1),
                    op: BinaryOperator::Equal,
                    rhs: box Expr::Column(c2),
                },
        } = &node.inner
        {
            (Column::from(c1.clone()), Column::from(c2.clone()))
        } else {
            continue;
        };

        let span = trace_span!("Lifting filter", filter_idx = %filter_idx.index());
        let _guard = span.enter();
        trace!(%c1, %c2, "Trying to lift filter to join key");

        // Now, ascend through that filter's ancestors to:
        //
        // 1. Check that the filter could be moved above that ancestor,
        // 2. remap the columns in the filter through any AliasTable nodes,
        // 3. and finally, if we find a join where the left column comes from the left parent and
        //    the right column comes from the right parent, add it to the `filters_to_add` map to be
        //    removed from a query and turned into a join key later
        for ancestor_idx in query.topo_ancestors(filter_idx)? {
            match &query.get_node(ancestor_idx).unwrap().inner {
                MirNodeInner::Aggregation { group_by, .. }
                | MirNodeInner::Extremum { group_by, .. }
                | MirNodeInner::Distinct { group_by, .. }
                | MirNodeInner::Paginate { group_by, .. }
                | MirNodeInner::TopK { group_by, .. } => {
                    if !(group_by.contains(&c1) && group_by.contains(&c2)) {
                        trace!(
                            "Columns in filter not in group_by of ancestor grouped node; can't \
                             turn filter into join key"
                        );
                        continue 'filter;
                    }
                }
                MirNodeInner::AliasTable { .. } => {
                    let alias_table_parent = *query
                        .ancestors(ancestor_idx)?
                        .get(0)
                        .ok_or_else(|| internal_err!("AliasTable must have a parent"))?;
                    let parent_cols = query.graph.columns(alias_table_parent);
                    let (Ok(new_c1_idx), Ok(new_c2_idx)) = (
                        query.graph.column_id_for_column(ancestor_idx, &c1),
                        query.graph.column_id_for_column(ancestor_idx, &c2)
                    ) else {

                        trace!(
                            ancestor_idx = %ancestor_idx.index(),
                            "Filter columns no longer resolve in ancestor, giving up on filter"
                        );
                        continue 'filter;
                    };

                    c1 = parent_cols
                        .get(new_c1_idx)
                        .ok_or_else(|| internal_err!("Column index out of bounds"))?
                        .clone();
                    c2 = parent_cols
                        .get(new_c2_idx)
                        .ok_or_else(|| internal_err!("Column index out of bounds"))?
                        .clone();

                    trace!(c1 = %c1, c2 = %c2, "Remapped columns through AliasTable ancestor");
                }
                MirNodeInner::LeftJoin { .. } => {
                    // TODO: figure out what to do about left joins
                    continue 'filter;
                }
                MirNodeInner::Union { .. } => {
                    // TODO: figure out what to do about unions
                    continue 'filter;
                }
                MirNodeInner::Join { .. } => {
                    let join_parents = query.ancestors(ancestor_idx)?;
                    let left_parent = *join_parents
                        .get(0)
                        .ok_or_else(|| internal_err!("Joins must have at least two ancestors"))?;
                    let right_parent = *join_parents
                        .get(1)
                        .ok_or_else(|| internal_err!("Joins must have at least two ancestors"))?;

                    // Could this filter be a join key in this join?
                    if c1.table != c2.table {
                        if query.graph.provides_column(left_parent, &c1)
                            && query.graph.provides_column(right_parent, &c2)
                        {
                            // Yes, using `c1` from the left and `c2` from the right!
                            filters_to_add
                                .entry(ancestor_idx)
                                .or_default()
                                .push((filter_idx, (c1, c2)));
                            continue 'filter;
                        } else if query.graph.provides_column(right_parent, &c1)
                            && query.graph.provides_column(left_parent, &c2)
                        {
                            // Yes, using `c2` from the left and `c1` from the right!
                            filters_to_add
                                .entry(ancestor_idx)
                                .or_default()
                                .push((filter_idx, (c2, c1)));
                            continue 'filter;
                        }
                    }
                    trace!(join_idx = %ancestor_idx.index(), "Will make filter a join key");
                }
                MirNodeInner::Base { .. }
                | MirNodeInner::Filter { .. }
                | MirNodeInner::Identity
                | MirNodeInner::JoinAggregates
                | MirNodeInner::DependentJoin { .. }
                | MirNodeInner::ViewKey { .. }
                | MirNodeInner::Project { .. }
                | MirNodeInner::Leaf { .. } => {}
            }
        }
    }

    // Now that we've collected all the information we need, we can actually mutate the query - run
    // through `filters_to_add` and convert all filters to join keys in their respective joins.
    for (join, filters) in filters_to_add {
        for (filter, _) in &filters {
            query.remove_node(*filter)?;
        }

        match &mut query
            .get_node_mut(join)
            .ok_or_else(|| internal_err!("Node must exist"))?
            .inner
        {
            MirNodeInner::Join { on, .. } => {
                for (_, join_key) in filters {
                    if !on.contains(&join_key) {
                        on.push(join_key)
                    }
                }
            }
            _ => {
                internal!("Join node is not a Join")
            }
        }
    }

    Ok(())
}

#[cfg(test)]
#[allow(clippy::panic)]
mod tests {
    use common::IndexType;
    use nom_sql::{parse_expr, ColumnSpecification, Dialect, Relation, SqlType};
    use readyset_client::ViewPlaceholder;

    use super::*;
    use crate::graph::MirGraph;
    use crate::node::MirNode;
    use crate::{Column, NodeIndex};

    fn make_join(query_name: &Relation, mir_graph: &mut MirGraph) -> NodeIndex {
        let t1 = mir_graph.add_node(MirNode::new(
            "t1".into(),
            MirNodeInner::Base {
                pg_meta: None,
                column_specs: vec![
                    ColumnSpecification {
                        column: nom_sql::Column::from("t1.a"),
                        sql_type: SqlType::Int(None),
                        constraints: vec![],
                        comment: None,
                    },
                    ColumnSpecification {
                        column: nom_sql::Column::from("t1.b"),
                        sql_type: SqlType::Int(None),
                        constraints: vec![],
                        comment: None,
                    },
                    ColumnSpecification {
                        column: nom_sql::Column::from("t1.c"),
                        sql_type: SqlType::Int(None),
                        constraints: vec![],
                        comment: None,
                    },
                ],
                primary_key: Some([Column::new(Some("t1"), "a")].into()),
                unique_keys: Default::default(),
            },
        ));
        mir_graph[t1].add_owner(query_name.clone());

        let t1_alias_table = mir_graph.add_node(MirNode::new(
            "t1_alias_table".into(),
            MirNodeInner::AliasTable { table: "t1".into() },
        ));
        mir_graph[t1_alias_table].add_owner(query_name.clone());
        mir_graph.add_edge(t1, t1_alias_table, 0);

        let t2 = mir_graph.add_node(MirNode::new(
            "t2".into(),
            MirNodeInner::Base {
                pg_meta: None,
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
        mir_graph[t2].add_owner(query_name.clone());

        let t2_alias_table = mir_graph.add_node(MirNode::new(
            "t2_alias_table".into(),
            MirNodeInner::AliasTable { table: "t2".into() },
        ));
        mir_graph[t2_alias_table].add_owner(query_name.clone());
        mir_graph.add_edge(t2, t2_alias_table, 0);

        let join = mir_graph.add_node(MirNode::new(
            "join".into(),
            MirNodeInner::Join {
                on: vec![],
                project: vec![
                    Column::new(Some("t1"), "a"),
                    Column::new(Some("t1"), "c"),
                    Column::new(Some("t2"), "b"),
                ],
            },
        ));
        mir_graph[join].add_owner(query_name.clone());
        mir_graph.add_edge(t1_alias_table, join, 0);
        mir_graph.add_edge(t2_alias_table, join, 1);

        join
    }

    #[test]
    fn simple_case() {
        let query_name: Relation = "q".into();
        let mut mir_graph = MirGraph::new();
        let join = make_join(&query_name, &mut mir_graph);

        let filter = mir_graph.add_node(MirNode::new(
            "filter".into(),
            MirNodeInner::Filter {
                conditions: parse_expr(Dialect::MySQL, "t1.a = t2.b").unwrap(),
            },
        ));
        mir_graph[filter].add_owner(query_name.clone());
        mir_graph.add_edge(join, filter, 0);

        let alias_table = mir_graph.add_node(MirNode::new(
            "alias_table".into(),
            MirNodeInner::AliasTable {
                table: "unprojected_leaf_key".into(),
            },
        ));
        mir_graph[alias_table].add_owner(query_name.clone());
        mir_graph.add_edge(filter, alias_table, 0);

        let leaf = mir_graph.add_node(MirNode::new(
            "leaf".into(),
            MirNodeInner::leaf(
                vec![(
                    Column::named("b").aliased_as_table("simple_case"),
                    ViewPlaceholder::OneToOne(1, BinaryOperator::Equal),
                )],
                IndexType::HashMap,
            ),
        ));
        mir_graph[leaf].add_owner(query_name.clone());
        mir_graph.add_edge(alias_table, leaf, 0);

        let mut query = MirQuery::new(query_name, leaf, &mut mir_graph);

        convert_filters_to_join_keys(&mut query).unwrap();
        assert!(!mir_graph.contains_node(filter));
        match &mir_graph[join].inner {
            MirNodeInner::Join { on, .. } => assert_eq!(
                *on,
                vec![(Column::new(Some("t1"), "a"), Column::new(Some("t2"), "b"))]
            ),
            _ => panic!(),
        }
    }

    #[test]
    fn through_alias_table() {
        readyset_tracing::init_test_logging();
        let query_name: Relation = "q".into();
        let mut mir_graph = MirGraph::new();
        let join = make_join(&query_name, &mut mir_graph);

        let alias_table = mir_graph.add_node(MirNode::new(
            "alias_table".into(),
            MirNodeInner::AliasTable { table: "sq".into() },
        ));
        mir_graph[alias_table].add_owner(query_name.clone());
        mir_graph.add_edge(join, alias_table, 0);

        let filter = mir_graph.add_node(MirNode::new(
            "filter".into(),
            MirNodeInner::Filter {
                conditions: parse_expr(Dialect::MySQL, "sq.a = sq.b").unwrap(),
            },
        ));
        mir_graph[filter].add_owner(query_name.clone());
        mir_graph.add_edge(alias_table, filter, 0);

        let leaf = mir_graph.add_node(MirNode::new(
            "leaf".into(),
            MirNodeInner::leaf(
                vec![(
                    Column::named("b").aliased_as_table("through_alias_table"),
                    ViewPlaceholder::OneToOne(1, BinaryOperator::Equal),
                )],
                IndexType::HashMap,
            ),
        ));
        mir_graph[leaf].add_owner(query_name.clone());
        mir_graph.add_edge(filter, leaf, 0);

        let mut query = MirQuery::new(query_name, leaf, &mut mir_graph);

        convert_filters_to_join_keys(&mut query).unwrap();
        assert!(!mir_graph.contains_node(filter));
        match &mir_graph[join].inner {
            MirNodeInner::Join { on, .. } => assert_eq!(
                *on,
                vec![(Column::new(Some("t1"), "a"), Column::new(Some("t2"), "b"))]
            ),
            _ => panic!(),
        }
    }

    #[test]
    fn two_columns_same_table() {
        readyset_tracing::init_test_logging();
        let query_name: Relation = "q".into();
        let mut mir_graph = MirGraph::new();
        let join = make_join(&query_name, &mut mir_graph);

        let filter = mir_graph.add_node(MirNode::new(
            "filter".into(),
            MirNodeInner::Filter {
                conditions: parse_expr(Dialect::MySQL, "t1.a = t1.c").unwrap(),
            },
        ));
        mir_graph[filter].add_owner(query_name.clone());
        mir_graph.add_edge(join, filter, 0);

        let leaf = mir_graph.add_node(MirNode::new(
            "leaf".into(),
            MirNodeInner::leaf(
                vec![(
                    Column::named("b").aliased_as_table("through_alias_table"),
                    ViewPlaceholder::OneToOne(1, BinaryOperator::Equal),
                )],
                IndexType::HashMap,
            ),
        ));
        mir_graph[leaf].add_owner(query_name.clone());
        mir_graph.add_edge(filter, leaf, 0);

        let mut query = MirQuery::new(query_name, leaf, &mut mir_graph);

        convert_filters_to_join_keys(&mut query).unwrap();

        assert!(mir_graph.contains_node(filter), "Filter is not a join key!");
    }

    #[test]
    fn two_columns_same_table_through_alias_table() {
        readyset_tracing::init_test_logging();
        let query_name: Relation = "q".into();
        let mut mir_graph = MirGraph::new();
        let join = make_join(&query_name, &mut mir_graph);

        let alias_table = mir_graph.add_node(MirNode::new(
            "alias_table".into(),
            MirNodeInner::AliasTable { table: "sq".into() },
        ));
        mir_graph[alias_table].add_owner(query_name.clone());
        mir_graph.add_edge(join, alias_table, 0);

        let filter = mir_graph.add_node(MirNode::new(
            "filter".into(),
            MirNodeInner::Filter {
                conditions: parse_expr(Dialect::MySQL, "sq.a = sq.c").unwrap(),
            },
        ));
        mir_graph[filter].add_owner(query_name.clone());
        mir_graph.add_edge(alias_table, filter, 0);

        let leaf = mir_graph.add_node(MirNode::new(
            "leaf".into(),
            MirNodeInner::leaf(
                vec![(
                    Column::named("b").aliased_as_table("through_alias_table"),
                    ViewPlaceholder::OneToOne(1, BinaryOperator::Equal),
                )],
                IndexType::HashMap,
            ),
        ));
        mir_graph[leaf].add_owner(query_name.clone());
        mir_graph.add_edge(filter, leaf, 0);

        let mut query = MirQuery::new(query_name, leaf, &mut mir_graph);

        convert_filters_to_join_keys(&mut query).unwrap();

        assert!(mir_graph.contains_node(filter), "Filter is not a join key!");
    }
}
