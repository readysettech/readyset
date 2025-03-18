use std::collections::BTreeMap;

use petgraph::graph::NodeIndex;
use readyset_errors::{internal, internal_err, ReadySetResult};
use readyset_sql::ast::{BinaryOperator, Expr, FunctionExpr};
use readyset_sql::Dialect;
use tracing::{trace, trace_span};

use crate::node::{MirNode, MirNodeInner, ProjectExpr};
use crate::query::MirQuery;
use crate::{Column, Ix};

fn inject_node(query: &mut MirQuery<'_>, parent: &NodeIndex<Ix>, projections: &[ProjectExpr]) {
    let parent_name = query
        .get_node(*parent)
        .expect("Parent node should exist")
        .name();

    let mut new_node = MirNode::new(
        format!("push_filters_{}", parent_name.name).into(),
        MirNodeInner::Project {
            emit: query
                .graph
                .columns(*parent)
                .iter()
                .map(|c| ProjectExpr::Column(c.clone()))
                .chain(projections.iter().cloned())
                .collect(),
        },
    );

    new_node.add_owner(query.name().clone());

    query
        .graph
        .insert_below(*parent, new_node)
        .expect("Failed to insert node");
}

/// A helper struct that helps in processing functions over join columns.
/// Why a struct? Because we need:
/// 1. A capturing closure (to avoid passing too many args)
/// 2. Recursing function (closures can't recurse) to account for nested functions (e.g. `lower(upper(col)))`)
struct FunctionProcessor<'a> {
    query: &'a MirQuery<'a>,
    projections_to_inject: &'a mut BTreeMap<NodeIndex<Ix>, Vec<ProjectExpr>>,
    dialect: Dialect,
}

impl FunctionProcessor<'_> {
    /// Given a function, drill down to find the column that it refers to
    fn find_column(&self, f: &FunctionExpr) -> ReadySetResult<Option<Column>> {
        let expr = match f {
            FunctionExpr::Lower { expr, .. }
            | FunctionExpr::Upper { expr, .. }
            | FunctionExpr::Extract { expr, .. }
            | FunctionExpr::Substring { string: expr, .. } => expr,
            FunctionExpr::Call { name, arguments }
                if matches!(
                    name.as_str(),
                    // TODO: Support more ?
                    "ascii"
                        | "substring"
                        | "substr"
                        | "lower"
                        | "upper"
                        | "length"
                        | "octet_length"
                        | "char_length"
                        | "character_length"
                        | "hex"
                ) =>
            {
                arguments.first().ok_or_else(|| {
                    internal_err!(
                        "Call to {} must have at least one argument",
                        f.alias(self.dialect).unwrap_or_default()
                    )
                })?
            }
            _ => return Ok(None),
        };

        Ok(match expr {
            Expr::Column(c) => Some(Column::from(c)),
            Expr::Call(func) => self.find_column(func)?,
            _ => None,
        })
    }

    /// Given a function over some column.
    /// 1. Drill down to find the column used in the function.
    /// 2. make sure that column is provided by the specificed parent arg.
    /// 3. Let the caller know that a node should be injected between the join and its parent.
    fn process(&mut self, f: Option<&FunctionExpr>, parent: NodeIndex<Ix>) -> ReadySetResult<bool> {
        match f {
            Some(f) => {
                let Some(col) = self.find_column(f)? else {
                    return Ok(false);
                };

                if !self.query.graph.provides_column(parent, &col) {
                    return Ok(false);
                }

                self.projections_to_inject
                    .entry(parent)
                    .or_default()
                    .push(ProjectExpr::Expr {
                        expr: Expr::Call(f.clone()),
                        alias: f.alias(self.dialect).unwrap_or_default().into(),
                    });

                Ok(true)
            }
            _ => Ok(false),
        }
    }
}

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
///
/// Also works with functions over join columns.
pub(crate) fn convert_filters_to_join_keys(query: &mut MirQuery<'_>) -> ReadySetResult<()> {
    let dialect = Dialect::MySQL;

    // We'll be constructing a map from join_idx -> Vec<(filter_idx, (left_join_col,
    // right_join_col))>
    let mut filters_to_add = BTreeMap::<_, Vec<_>>::new();
    let mut projections_to_inject = BTreeMap::<NodeIndex<_>, Vec<ProjectExpr>>::new();

    // First, loop through all the filters in the query where the condition compares one column
    // against another column.
    // In case function calls are involved, we keep track of them and use their generated aliases
    // as join keys
    'filter: for (filter_idx, node) in query.node_references() {
        let (mut c1, mut c2, f1, f2) = if let MirNodeInner::Filter {
            conditions:
                Expr::BinaryOp {
                    lhs,
                    op: BinaryOperator::Equal,
                    rhs,
                },
        } = &node.inner
        {
            match (lhs.as_ref(), rhs.as_ref()) {
                (Expr::Column(c1), Expr::Column(c2)) => (
                    Column::from(c1.clone()),
                    Column::from(c2.clone()),
                    None,
                    None,
                ),
                (Expr::Call(f1), Expr::Column(c2)) => (
                    Column::named(f1.alias(dialect).unwrap()),
                    Column::from(c2.clone()),
                    Some(f1),
                    None,
                ),
                (Expr::Column(c1), Expr::Call(f2)) => (
                    Column::from(c1.clone()),
                    Column::named(f2.alias(dialect).unwrap()),
                    None,
                    Some(f2),
                ),
                (Expr::Call(f1), Expr::Call(f2)) => (
                    Column::named(f1.alias(dialect).unwrap()),
                    Column::named(f2.alias(dialect).unwrap()),
                    Some(f1),
                    Some(f2),
                ),
                _ => {
                    continue;
                }
            }
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
                        .first()
                        .ok_or_else(|| internal_err!("AliasTable must have a parent"))?;
                    let parent_cols = query.graph.columns(alias_table_parent);
                    let (Ok(new_c1_idx), Ok(new_c2_idx)) = (
                        query.graph.column_id_for_column(ancestor_idx, &c1),
                        query.graph.column_id_for_column(ancestor_idx, &c2),
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
                        .first()
                        .ok_or_else(|| internal_err!("Joins must have at least two ancestors"))?;
                    let right_parent = *join_parents
                        .get(1)
                        .ok_or_else(|| internal_err!("Joins must have at least two ancestors"))?;

                    // If either sides of the filter is a fn(col) we need to push it before the
                    // join. Find the table that provides that column and inject a node between it
                    // and the join. This node will later provide the values for the join condition.
                    let mut fn_processor = FunctionProcessor {
                        query,
                        dialect,
                        projections_to_inject: &mut projections_to_inject,
                    };

                    let mut processed_same_side = fn_processor.process(f1, left_parent)?;
                    processed_same_side |= fn_processor.process(f2, right_parent)?;

                    // left column references the right table or vice versa
                    let mut processed_cross_side = fn_processor.process(f1, right_parent)?;
                    processed_cross_side |= fn_processor.process(f2, left_parent)?;

                    // Nodes need to be injected
                    // Note that ordering only matters when either side is a column.
                    // In case both sides are function calls, then ordering doesn't matter.
                    if processed_same_side || processed_cross_side {
                        filters_to_add.entry(ancestor_idx).or_default().push((
                            filter_idx,
                            if processed_cross_side {
                                (c2.clone(), c1.clone())
                            } else {
                                (c1.clone(), c2.clone())
                            },
                        ));
                    } else if c1.table != c2.table {
                        // No injection needed; filter only contains columns.
                        // Can we turn this filter into a join key?
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
                | MirNodeInner::DependentLeftJoin { .. }
                | MirNodeInner::ViewKey { .. }
                | MirNodeInner::Project { .. }
                | MirNodeInner::Leaf { .. } => {}
            }
        }
    }

    // Inject the new node below the parent.
    for (parent, projections) in projections_to_inject.iter() {
        inject_node(query, parent, projections);
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
            MirNodeInner::Join { on, project } => {
                for (_, join_key) in filters {
                    if !on.contains(&join_key) {
                        on.push(join_key)
                    }
                }

                for (_, projections) in projections_to_inject.iter() {
                    project.extend(
                        projections
                            .iter()
                            .map(|p| -> ReadySetResult<_> {
                                match p {
                                    ProjectExpr::Expr { alias, .. } => Ok(Column::named(alias)),
                                    ProjectExpr::Column(_) => {
                                        internal!("Injected columns can only be expressions")
                                    }
                                }
                            })
                            .collect::<ReadySetResult<Vec<_>>>()?,
                    )
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
mod tests {
    use common::IndexType;
    use readyset_client::ViewPlaceholder;
    use readyset_sql::ast::{self, ColumnSpecification, Relation, SqlType};
    use readyset_sql::Dialect;
    use readyset_sql_parsing::parse_expr;

    use super::*;
    use crate::graph::MirGraph;
    use crate::node::MirNode;
    use crate::{Column, NodeIndex};

    fn make_join(query_name: &Relation, mir_graph: &mut MirGraph) -> NodeIndex {
        let t1 = mir_graph.add_node(MirNode::new(
            "t1".into(),
            MirNodeInner::Base {
                column_specs: vec![
                    ColumnSpecification {
                        column: ast::Column::from("t1.a"),
                        sql_type: SqlType::Int(None),
                        generated: None,
                        constraints: vec![],
                        comment: None,
                    },
                    ColumnSpecification {
                        column: ast::Column::from("t1.b"),
                        sql_type: SqlType::Int(None),
                        generated: None,
                        constraints: vec![],
                        comment: None,
                    },
                    ColumnSpecification {
                        column: ast::Column::from("t1.c"),
                        sql_type: SqlType::Int(None),
                        generated: None,
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
                column_specs: vec![
                    ColumnSpecification {
                        column: ast::Column::from("t2.a"),
                        sql_type: SqlType::Int(None),
                        generated: None,
                        constraints: vec![],
                        comment: None,
                    },
                    ColumnSpecification {
                        column: ast::Column::from("t2.b"),
                        sql_type: SqlType::Int(None),
                        generated: None,
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

    #[test]
    fn function_call_equals_column() {
        readyset_tracing::init_test_logging();
        let query_name: Relation = "q".into();
        let mut mir_graph = MirGraph::new();
        let join = make_join(&query_name, &mut mir_graph);

        let filter = mir_graph.add_node(MirNode::new(
            "filter".into(),
            MirNodeInner::Filter {
                conditions: parse_expr(Dialect::MySQL, "LOWER(t2.a) = t1.c").unwrap(),
            },
        ));
        mir_graph[filter].add_owner(query_name.clone());
        mir_graph.add_edge(join, filter, 0);

        let leaf = mir_graph.add_node(MirNode::new(
            "leaf".into(),
            MirNodeInner::leaf(vec![], IndexType::HashMap),
        ));
        mir_graph[leaf].add_owner(query_name.clone());
        mir_graph.add_edge(filter, leaf, 0);

        let mut query = MirQuery::new(query_name, leaf, &mut mir_graph);

        convert_filters_to_join_keys(&mut query).unwrap();

        assert!(!mir_graph.contains_node(filter));
        match &mir_graph[join].inner {
            MirNodeInner::Join { on, .. } => assert_eq!(
                *on,
                vec![(Column::new(Some("t1"), "c"), Column::named("lower(a)"))]
            ),
            _ => panic!(),
        }
    }

    #[test]
    fn column_equals_function_call() {
        readyset_tracing::init_test_logging();
        let query_name: Relation = "q".into();
        let mut mir_graph = MirGraph::new();
        let join = make_join(&query_name, &mut mir_graph);

        let filter = mir_graph.add_node(MirNode::new(
            "filter".into(),
            MirNodeInner::Filter {
                conditions: parse_expr(Dialect::MySQL, "t2.b = UPPER(t1.a)").unwrap(),
            },
        ));
        mir_graph[filter].add_owner(query_name.clone());
        mir_graph.add_edge(join, filter, 0);

        let leaf = mir_graph.add_node(MirNode::new(
            "leaf".into(),
            MirNodeInner::leaf(vec![], IndexType::HashMap),
        ));
        mir_graph[leaf].add_owner(query_name.clone());
        mir_graph.add_edge(filter, leaf, 0);

        let mut query = MirQuery::new(query_name, leaf, &mut mir_graph);

        convert_filters_to_join_keys(&mut query).unwrap();

        assert!(!mir_graph.contains_node(filter));
        match &mir_graph[join].inner {
            MirNodeInner::Join { on, .. } => assert_eq!(
                *on,
                vec![(Column::named("upper(a)"), Column::new(Some("t2"), "b"))]
            ),
            _ => panic!(),
        }
    }

    #[test]
    fn function_call_equals_function_call() {
        readyset_tracing::init_test_logging();
        let query_name: Relation = "q".into();
        let mut mir_graph = MirGraph::new();
        let join = make_join(&query_name, &mut mir_graph);

        let filter = mir_graph.add_node(MirNode::new(
            "filter".into(),
            MirNodeInner::Filter {
                conditions: parse_expr(Dialect::MySQL, "LOWER(t1.a) = UPPER(t2.b)").unwrap(),
            },
        ));
        mir_graph[filter].add_owner(query_name.clone());
        mir_graph.add_edge(join, filter, 0);

        let leaf = mir_graph.add_node(MirNode::new(
            "leaf".into(),
            MirNodeInner::leaf(vec![], IndexType::HashMap),
        ));
        mir_graph[leaf].add_owner(query_name.clone());
        mir_graph.add_edge(filter, leaf, 0);

        let mut query = MirQuery::new(query_name, leaf, &mut mir_graph);

        convert_filters_to_join_keys(&mut query).unwrap();

        assert!(!mir_graph.contains_node(filter));
        match &mir_graph[join].inner {
            MirNodeInner::Join { on, .. } => assert_eq!(
                *on,
                vec![(Column::named("lower(a)"), Column::named("upper(b)"))]
            ),
            _ => panic!(),
        }
    }

    /// Ideally, such filters should be pushed up to the parent; however,
    /// it's completely fine to have them as join keys in the join node.
    #[test]
    fn function_call_equals_function_call_same_side() {
        readyset_tracing::init_test_logging();
        let query_name: Relation = "q".into();
        let mut mir_graph = MirGraph::new();
        let join = make_join(&query_name, &mut mir_graph);

        let filter = mir_graph.add_node(MirNode::new(
            "filter".into(),
            MirNodeInner::Filter {
                conditions: parse_expr(Dialect::MySQL, "LOWER(t1.a) = LOWER(UPPER(t1.a))").unwrap(),
            },
        ));
        mir_graph[filter].add_owner(query_name.clone());
        mir_graph.add_edge(join, filter, 0);

        let leaf = mir_graph.add_node(MirNode::new(
            "leaf".into(),
            MirNodeInner::leaf(vec![], IndexType::HashMap),
        ));
        mir_graph[leaf].add_owner(query_name.clone());
        mir_graph.add_edge(filter, leaf, 0);

        let mut query = MirQuery::new(query_name, leaf, &mut mir_graph);

        convert_filters_to_join_keys(&mut query).unwrap();

        assert!(!mir_graph.contains_node(filter));
        match &mir_graph[join].inner {
            MirNodeInner::Join { on, .. } => assert_eq!(
                *on,
                vec![(Column::named("lower(upper(a))"), Column::named("lower(a)"))]
            ),
            _ => panic!(),
        }
    }
}
