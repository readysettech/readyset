use nom_sql::analysis::ReferredColumns;
use nom_sql::{BinaryOperator, Expr};
use readyset_errors::{internal, invariant, unsupported, ReadySetResult};
use tracing::{instrument, trace};

use crate::node::MirNodeInner;
use crate::query::MirQuery;
use crate::{Column, NodeIndex};

/// Push the given `node`, which should be a [filter][] node with the given `dependency` on columns
/// on the left-hand side of some `dependent_join`, one step towards being below that dependent
/// join.
///
/// Internally, this uses the following rules to push filters down the graph:
///
/// - [`Project`], [`Join`], [`LeftJoin`], and [`DependentJoin`]s are all totally commutative with
///   filters, so can be swapped in position with those filters with impunity
/// - Grouped nodes ([`Aggregation`] and [`Extremum`]) require adding any *non* dependent columns
///   mentioned in the filter to the group-by of the node.
/// - All other nodes currently return an [unsupported error][] - it *is* theoretically possible to
///   push below any node, but currently we don't have that ability
///
/// Note that except in the case of filters we *don't* have to do any extra work here to get columns
/// projected, as the decorrelation pass runs before [`pull_all_required_columns`][]
///
/// [filter]: MirNodeInner::Filter
/// [`Project`]: MirNodeInner::Project
/// [`Join`]: MirNodeInner::Join
/// [`LeftJoin`]: MirNodeInner::LeftJoin
/// [`DependentJoin`]: MirNodeInner::DependentJoin
/// [`Aggregation`]: MirNodeInner::Aggregation
/// [`Extremum`]: MirNodeInner::Extremum
/// [unsupported error]: noria_errors::ReadySetError::Unsupported
/// [`pull_all_required_columns`]: noria_mir::rewrite::pull_columns::pull_all_required_columns
fn push_dependent_filter(
    query: &mut MirQuery<'_>,
    node_idx: NodeIndex,
    dependency: DependentCondition,
) -> ReadySetResult<()> {
    let children = query.descendants(node_idx)?;
    if children.len() != 1 {
        // TODO: this probably happens for unions; we should try to deal with that at some point
        // (see what the HyPer and SQL Server papers say about disjunctive predicates)
        unsupported!();
    }

    let child_idx = *children.get(0).unwrap();

    // If we're lifting past an AliasTable node, rewrite all columns referenced by the filter
    // condition that should resolve in that AliasTable node to use the correct table name
    if let MirNodeInner::AliasTable { table } = &query.get_node(child_idx).unwrap().inner {
        let table = table.clone();
        match &mut query.get_node_mut(node_idx).unwrap().inner {
            MirNodeInner::Filter { conditions } => {
                for col in conditions.referred_columns_mut() {
                    if dependency
                        .non_dependent_cols
                        .contains(&Column::from(col.clone()))
                    {
                        col.table = Some(table.clone())
                    }
                }
            }
            _ => internal!("The node passed to push_dependent_filter must be a filter"),
        }
    }

    if matches!(
        query.get_node(child_idx).unwrap().inner,
        MirNodeInner::AliasTable { .. }
    ) {
        for col in &dependency.non_dependent_cols {
            query.graph.add_column(child_idx, col.clone())?;
        }
    }

    trace!(
        "Lifting `{}` above `{}`",
        node_idx.index(),
        child_idx.index()
    );
    let should_insert = match &mut query.get_node_mut(child_idx).unwrap().inner {
        MirNodeInner::DependentJoin { .. }
        | MirNodeInner::Project { .. }
        | MirNodeInner::Filter { .. }
        | MirNodeInner::Join { .. }
        | MirNodeInner::LeftJoin { .. }
        | MirNodeInner::AliasTable { .. } => true,
        MirNodeInner::Aggregation { .. } | MirNodeInner::Extremum { .. } => {
            for col in &dependency.non_dependent_cols {
                query.graph.add_column(child_idx, col.clone())?;
            }
            true
        }
        inner => unsupported!(
            "Don't know how to push filter below {} to decorrelate",
            inner.description()
        ),
    };

    if should_insert {
        query.swap_with_child(node_idx)?;
    } else {
        query.remove_node(node_idx)?;
    }

    Ok(())
}

/// A MIR rewrite pass that attempts to eliminate all [dependent joins][] by algebraically pushing
/// any dependent filters below the join.
///
/// This approach is roughly based on the papers [The Complete Story of Joins (In HyPer)][NKL17],
/// [Unnesting Arbitrary Queries][NK15], and [Orthogonal Optimization of Subqueries and
/// Aggregation][GJ01]. Currently it's incomplete (there are several rewrite rules we have yet to
/// implement here) but is a pretty reasonable starting ground for a fully arbitrary query unnesting
/// pass.
///
/// The rough approach is:
///
/// As long as we have dependent joins in the query:
/// 1. Pick the uppermost dependent join in the query, topologically (ideally a dependent join that
///    has no other dependent joins as ancestors)
/// 2. Find a filter in the ancestors of the right hand side of that join that references columns
///    on the left hand side of that join
///    - If we don't find one, then the join no longer needs to be dependent, so convert it to a
///      regular inner join
/// 3. Attempt to push that filter down the graph, using an algebraic rewrite rule (this is done
///    in [`push_dependent_filter`])
///
/// [dependent joins]: MirNodeInner::DependentJoin
/// [NLK17]: http://btw2017.informatik.uni-stuttgart.de/slidesandpapers/F1-10-37/paper_web.pdf
/// [NK15]: https://cs.emis.de/LNI/Proceedings/Proceedings241/383.pdf
/// [GJ01]: http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.563.8492&rep=rep1&type=pdf
///
/// # Future work
///
/// Making this rewrite pass complete likely requires at least:
///
/// - Rewriting not just dependent filters, but other kinds of dependent nodes
/// - Implementing rewrite rules to push dependent filters through all different kinds of nodes, not
///   just the ones for which filtering is trivially commutative (unions seem difficult, for
///   example)
/// - Handling cases where a single filter refers to *two* separate outer queries, and has to be
///   pulled correctly through both dependent joins
///
/// Also, the way this is currently written is simple, and (hopefully) relatively easy to follow,
/// but likely duplicates a lot of work (though this probably doesn't matter hugely, as the
/// coefficients here are usually quite small). There's a lot of stuff that would likely be a lot
/// easier to memoize if it weren't for the way MIR is structured, with [`RefCell`]s throwing borrow
/// errors at runtime basically all the time
#[instrument(skip_all, fields(query = %query.name().display_unquoted()))]
pub(crate) fn eliminate_dependent_joins(query: &mut MirQuery<'_>) -> ReadySetResult<()> {
    // TODO(grfn): lots of opportunity for memoization here, but the MIR RefCell mess makes that a
    // lot harder
    loop {
        let dependent_joins: Vec<NodeIndex> = query
            .topo_nodes()
            .into_iter()
            .filter(|&n| query.get_node(n).unwrap().inner.is_dependent_join())
            .collect::<Vec<_>>();

        // Try to rewrite one dependent join at a time, starting with the topmost
        let join = if let Some(join) = dependent_joins.last() {
            *join
        } else {
            trace!("No more dependent joins left");
            break;
        };

        // Dependent joins allow nodes on the right to reference columns from the left side
        let join_parents = query.ancestors(join)?;
        invariant!(
            join_parents.len() >= 2,
            "Joins must have at least two parents"
        );
        let left_parent = join_parents[0];
        let right_parent = join_parents[1];
        let left_columns = query
            .topo_ancestors(left_parent)?
            .filter(|&n| query.is_root(n))
            .flat_map(|n| query.graph.columns(n))
            .collect::<Vec<_>>();

        // If a node is a dependent filter, return a description of *how* it's a dependent
        // filter
        let dependent_condition = |inner: &MirNodeInner| {
            if let MirNodeInner::Filter { conditions } = inner {
                match conditions {
                    Expr::BinaryOp {
                        lhs: box Expr::Column(left_col),
                        op: BinaryOperator::Equal,
                        rhs: box Expr::Column(right_col),
                    } => {
                        let matches_left = left_columns.iter().any(|c| *c == *left_col);
                        let matches_right = left_columns.iter().any(|c| *c == *right_col);
                        match (matches_left, matches_right) {
                            // Both sides are dependent
                            (true, true) => {
                                return Some(DependentCondition {
                                    non_dependent_cols: vec![],
                                })
                            }
                            (true, false) => {
                                return Some(DependentCondition {
                                    non_dependent_cols: vec![right_col.into()],
                                });
                            }
                            (false, true) => {
                                return Some(DependentCondition {
                                    non_dependent_cols: vec![left_col.into()],
                                });
                            }
                            (false, false) => {}
                        }
                    }
                    expr => {
                        if expr
                            .referred_columns()
                            .any(|expr_col| left_columns.iter().any(|c| *c == *expr_col))
                        {
                            return Some(DependentCondition {
                                non_dependent_cols: expr
                                    .referred_columns()
                                    .filter(|expr_col| {
                                        !left_columns.iter().any(|c| *c == **expr_col)
                                    })
                                    .map(|c| c.into())
                                    .collect(),
                            });
                        }
                    }
                }
            }
            None
        };

        let dependency = match dependent_condition(&query.get_node(right_parent).unwrap().inner) {
            Some(dep) => Some((right_parent, dep)),
            None => query.topo_ancestors(right_parent)?.find_map(|n| {
                dependent_condition(&query.get_node(n).unwrap().inner).map(|dep| (n, dep))
            }),
        };

        if let Some((node, dependency)) = dependency {
            push_dependent_filter(query, node, dependency)?;
        } else {
            trace!(
                dependent_join = %query.get_node(join).unwrap().name().display_unquoted(),
                "Can't find any more dependent nodes, done rewriting!"
            );
            // Can't find any dependent nodes, which means the join isn't dependent anymore! So turn
            // it into a non-dependent join
            let new_inner = match &query.get_node(join).unwrap().inner {
                MirNodeInner::DependentJoin { on, project } => MirNodeInner::Join {
                    on: on.clone(),
                    project: project.clone(),
                },
                _ => unreachable!("Already checked is_dependent_join above"),
            };
            query.get_node_mut(join).unwrap().inner = new_inner;
        };
    }
    Ok(())
}

/// For a dependent filter, a description of *how* that filter depends on columns from the left side
/// of a dependent join
#[derive(Debug)]
struct DependentCondition {
    /// A list of the columns mentioned in the condition that do *not* refer to columns on the
    /// left-hand side of the join
    non_dependent_cols: Vec<Column>,
}

#[cfg(test)]
#[allow(clippy::panic)] // it's a test
mod tests {
    use common::IndexType;
    use dataflow::ops::grouped::aggregate::Aggregation;
    use nom_sql::{BinaryOperator, ColumnSpecification, Expr, Literal, Relation, SqlType};
    use petgraph::Direction;

    use super::*;
    use crate::graph::MirGraph;
    use crate::node::{MirNode, MirNodeInner, ProjectExpr};
    use crate::rewrite::pull_columns::pull_all_required_columns;
    use crate::visualize::GraphViz;
    use crate::Column;

    #[test]
    fn exists_ish() {
        readyset_tracing::init_test_logging();
        // query looks something like:
        //     SELECT t1.a FROM t1 WHERE EXISTS (SELECT * FROM t2 WHERE t2.a = t1.a)
        let mut graph = MirGraph::new();

        let query_name = Relation::from("q");

        let t2 = graph.add_node(MirNode::new(
            "t2".into(),
            MirNodeInner::Base {
                pg_meta: None,
                column_specs: vec![ColumnSpecification {
                    column: nom_sql::Column::from("t2.a"),
                    sql_type: SqlType::Int(None),
                    constraints: vec![],
                    comment: None,
                }],
                primary_key: Some([Column::new(Some("t2"), "a")].into()),
                unique_keys: Default::default(),
            },
        ));
        graph[t2].add_owner(query_name.clone());
        // t2 -> ...

        // -> σ[t2.a = t1.a]
        let t2_filter = graph.add_node(MirNode::new(
            "t2_filter".into(),
            MirNodeInner::Filter {
                conditions: Expr::BinaryOp {
                    lhs: Box::new(Expr::Column("t2.a".into())),
                    op: BinaryOperator::Equal,
                    rhs: Box::new(Expr::Column("t1.a".into())),
                },
            },
        ));
        graph[t2_filter].add_owner(query_name.clone());

        graph.add_edge(t2, t2_filter, 0);

        // -> AliasTable
        let t2_alias_table = graph.add_node(MirNode::new(
            "alias_table".into(),
            MirNodeInner::AliasTable {
                table: "rhs".into(),
            },
        ));
        graph[t2_alias_table].add_owner(query_name.clone());

        graph.add_edge(t2_filter, t2_alias_table, 0);

        // -> π[lit: 0, lit: 0]
        let group_proj = graph.add_node(MirNode::new(
            "q_prj_hlpr".into(),
            MirNodeInner::Project {
                emit: vec![
                    ProjectExpr::Expr {
                        alias: "__count_val".into(),
                        expr: Expr::Literal(0u32.into()),
                    },
                    ProjectExpr::Expr {
                        alias: "__count_grp".into(),
                        expr: Expr::Literal(0u32.into()),
                    },
                ],
            },
        ));
        graph[group_proj].add_owner(query_name.clone());
        graph.add_edge(t2_alias_table, group_proj, 0);
        // -> [0, 0] for each row

        // -> |0| γ[1]
        let exists_count = graph.add_node(MirNode::new(
            "__exists_count".into(),
            MirNodeInner::Aggregation {
                on: Column::named("__count_val"),
                group_by: vec![Column::named("__count_grp")],
                output_column: Column::named("__exists_count"),
                kind: Aggregation::Count,
            },
        ));
        graph[exists_count].add_owner(query_name.clone());
        graph.add_edge(group_proj, exists_count, 0);
        // -> [0, <count>] for each row

        // -> σ[c1 > 0]
        let gt_0_filter = graph.add_node(MirNode::new(
            "count_gt_0".into(),
            MirNodeInner::Filter {
                conditions: Expr::BinaryOp {
                    lhs: Box::new(Expr::Column("__exists_count".into())),
                    op: BinaryOperator::Greater,
                    rhs: Box::new(Expr::Literal(Literal::Integer(0))),
                },
            },
        ));
        graph[gt_0_filter].add_owner(query_name.clone());
        graph.add_edge(exists_count, gt_0_filter, 0);

        let t1 = graph.add_node(MirNode::new(
            "t1".into(),
            MirNodeInner::Base {
                pg_meta: None,
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
        // t1 -> ...

        // -> π[..., lit: 0]
        let left_literal_join_key_proj = graph.add_node(MirNode::new(
            "t1_join_key".into(),
            MirNodeInner::Project {
                emit: vec![
                    ProjectExpr::Column(Column::new(Some("t1"), "a")),
                    ProjectExpr::Expr {
                        alias: "__exists_join_key".into(),
                        expr: Expr::Literal(0u32.into()),
                    },
                ],
            },
        ));
        graph[left_literal_join_key_proj].add_owner(query_name.clone());
        graph.add_edge(t1, left_literal_join_key_proj, 0);

        // -> ⧑ on: l.__exists_join_key ≡ r.__count_grp
        let exists_join = graph.add_node(MirNode::new(
            "exists_join".into(),
            MirNodeInner::DependentJoin {
                on: vec![(
                    Column::named("__exists_join_key"),
                    Column::named("__count_grp"),
                )],
                project: vec![
                    Column::new(Some("t1"), "a"),
                    Column::named("__exists_join_key"),
                    Column::named("__count_grp"),
                    Column::named("__exists_count"),
                ],
            },
        ));
        graph[exists_join].add_owner(query_name.clone());
        graph.add_edge(left_literal_join_key_proj, exists_join, 0);
        graph.add_edge(gt_0_filter, exists_join, 1);

        let leaf = graph.add_node(MirNode::new(
            "q".into(),
            MirNodeInner::leaf(vec![], IndexType::HashMap),
        ));
        graph[leaf].add_owner(query_name.clone());
        graph.add_edge(exists_join, leaf, 0);

        let mut query = MirQuery::new(query_name, leaf, &mut graph);

        eliminate_dependent_joins(&mut query).unwrap();

        eprintln!("{}", query.to_graphviz());

        assert!(
            matches!(&query.graph[exists_join].inner, MirNodeInner::Join { .. }),
            "should have rewritten dependent to non-dependent join (got: {})",
            graph[exists_join].name().display_unquoted()
        );

        assert_eq!(
            query
                .graph
                .neighbors_directed(t2_filter, Direction::Incoming)
                .next()
                .unwrap(),
            exists_join,
            "Dependent filter should be moved below join"
        );
        assert_eq!(
            query
                .graph
                .neighbors_directed(t2_filter, Direction::Outgoing)
                .next()
                .unwrap(),
            leaf,
            "Dependent filter should be inserted below join"
        );

        assert_eq!(
            query.graph[query
                .graph
                .neighbors_directed(t2_alias_table, Direction::Incoming)
                .next()
                .unwrap()]
            .name(),
            &Relation::from("t2"),
            "t2_filter should be moved"
        );

        let pull_result = pull_all_required_columns(&mut query);
        assert!(pull_result.is_ok(), "{}", pull_result.err().unwrap());
    }

    #[test]
    fn multiple_filters_after_agg() {
        readyset_tracing::init_test_logging();
        // query looks something like:
        //     SELECT t1.a FROM t1
        //     WHERE EXISTS (
        //         SELECT 1 FROM t2 WHERE t2.a = t1.a GROUP BY t2.b
        //         HAVING COUNT(t2.b) BETWEEN 4 AND 7
        //     )
        let mut graph = MirGraph::new();

        let query_name = Relation::from("q");

        let t2 = graph.add_node(MirNode::new(
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
        graph[t2].add_owner(query_name.clone());
        // t2 -> ...

        // -> σ[t2.a = t1.a]
        let t2_filter = graph.add_node(MirNode::new(
            "t2_filter".into(),
            MirNodeInner::Filter {
                conditions: Expr::BinaryOp {
                    lhs: Box::new(Expr::Column("t2.a".into())),
                    op: BinaryOperator::Equal,
                    rhs: Box::new(Expr::Column("t1.a".into())),
                },
            },
        ));
        graph[t2_filter].add_owner(query_name.clone());
        graph.add_edge(t2, t2_filter, 0);

        // -> |*|(t2.b) γ[t2.b]
        let t2_count = graph.add_node(MirNode::new(
            "q_t2_count".into(),
            MirNodeInner::Aggregation {
                on: Column::new(Some("t2"), "b"),
                group_by: vec![Column::new(Some("t2"), "b")],
                output_column: Column::named("COUNT(t2.b)"),
                kind: Aggregation::Count,
            },
        ));
        graph[t2_count].add_owner(query_name.clone());
        graph.add_edge(t2_filter, t2_count, 0);

        // -> σ[count(t2.b) >= 4]
        let t2_count_f1 = graph.add_node(MirNode::new(
            "q_t2_count_f1".into(),
            MirNodeInner::Filter {
                conditions: Expr::BinaryOp {
                    lhs: Box::new(Expr::Column(nom_sql::Column {
                        table: None,
                        name: "COUNT(t2.b)".into(),
                    })),
                    op: BinaryOperator::GreaterOrEqual,
                    rhs: Box::new(Expr::Literal(4.into())),
                },
            },
        ));
        graph[t2_count_f1].add_owner(query_name.clone());
        graph.add_edge(t2_count, t2_count_f1, 0);

        // -> σ[count(t2.b) <= 7]
        let t2_count_f2 = graph.add_node(MirNode::new(
            "q_t2_count_f2".into(),
            MirNodeInner::Filter {
                conditions: Expr::BinaryOp {
                    lhs: Box::new(Expr::Column(nom_sql::Column {
                        table: None,
                        name: "COUNT(t2.b)".into(),
                    })),
                    op: BinaryOperator::LessOrEqual,
                    rhs: Box::new(Expr::Literal(7.into())),
                },
            },
        ));
        graph[t2_count_f2].add_owner(query_name.clone());
        graph.add_edge(t2_count_f1, t2_count_f2, 0);

        // -> π[lit: 0, lit: 0]
        let group_proj = graph.add_node(MirNode::new(
            "q_prj_hlpr".into(),
            MirNodeInner::Project {
                emit: vec![
                    ProjectExpr::Expr {
                        alias: "__count_val".into(),
                        expr: Expr::Literal(0u32.into()),
                    },
                    ProjectExpr::Expr {
                        alias: "__count_grp".into(),
                        expr: Expr::Literal(0u32.into()),
                    },
                ],
            },
        ));
        graph[group_proj].add_owner(query_name.clone());
        graph.add_edge(t2_count_f2, group_proj, 0);
        // -> [0, 0] for each row

        // -> |0| γ[1]
        let exists_count = graph.add_node(MirNode::new(
            "__exists_count".into(),
            MirNodeInner::Aggregation {
                on: Column::named("__count_val"),
                group_by: vec![Column::named("__count_grp")],
                output_column: Column::named("__exists_count"),
                kind: Aggregation::Count,
            },
        ));
        graph[exists_count].add_owner(query_name.clone());
        graph.add_edge(group_proj, exists_count, 0);
        // -> [0, <count>] for each row

        // -> σ[c1 > 0]
        let gt_0_filter = graph.add_node(MirNode::new(
            "count_gt_0".into(),
            MirNodeInner::Filter {
                conditions: Expr::BinaryOp {
                    lhs: Box::new(Expr::Column("__exists_count".into())),
                    op: BinaryOperator::Greater,
                    rhs: Box::new(Expr::Literal(Literal::Integer(0))),
                },
            },
        ));
        graph[gt_0_filter].add_owner(query_name.clone());
        graph.add_edge(exists_count, gt_0_filter, 0);

        let t1 = graph.add_node(MirNode::new(
            "t1".into(),
            MirNodeInner::Base {
                pg_meta: None,
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
        // t1 -> ...

        // -> π[..., lit: 0]
        let left_literal_join_key_proj = graph.add_node(MirNode::new(
            "t1_join_key".into(),
            MirNodeInner::Project {
                emit: vec![
                    ProjectExpr::Column(Column::new(Some("t1"), "a")),
                    ProjectExpr::Expr {
                        alias: "__exists_join_key".into(),
                        expr: Expr::Literal(0u32.into()),
                    },
                ],
            },
        ));
        graph[left_literal_join_key_proj].add_owner(query_name.clone());
        graph.add_edge(t1, left_literal_join_key_proj, 0);

        // -> ⧑ on: l.__exists_join_key ≡ r.__count_grp
        let exists_join = graph.add_node(MirNode::new(
            "exists_join".into(),
            MirNodeInner::DependentJoin {
                on: vec![(
                    Column::named("__exists_join_key"),
                    Column::named("__count_grp"),
                )],
                project: vec![
                    Column::new(Some("t1"), "a"),
                    Column::named("__exists_join_key"),
                    Column::named("__count_grp"),
                    Column::named("__exists_count"),
                ],
            },
        ));
        graph[exists_join].add_owner(query_name.clone());
        graph.add_edge(left_literal_join_key_proj, exists_join, 0);
        graph.add_edge(gt_0_filter, exists_join, 1);

        let leaf = graph.add_node(MirNode::new(
            "q".into(),
            MirNodeInner::leaf(vec![], IndexType::HashMap),
        ));
        graph[leaf].add_owner(query_name.clone());
        graph.add_edge(exists_join, leaf, 0);

        let mut query = MirQuery::new(query_name, leaf, &mut graph);

        eliminate_dependent_joins(&mut query).unwrap();

        eprintln!("{}", query.to_graphviz());

        assert_eq!(
            query.graph.columns(t2_count_f2),
            &[
                Column::new(Some("t2"), "b"),
                Column::new(Some("t2"), "a"),
                Column::named("COUNT(t2.b)"),
            ],
            "should update columns of filters recursively"
        );

        assert!(
            matches!(&query.graph[exists_join].inner, MirNodeInner::Join { .. }),
            "should have rewritten dependent to non-dependent join (got: {})",
            graph[exists_join].name().display_unquoted()
        );

        assert_eq!(
            query
                .graph
                .neighbors_directed(t2_filter, Direction::Incoming)
                .next()
                .unwrap(),
            exists_join,
            "Dependent filter should be moved below join"
        );
        assert_eq!(
            query
                .graph
                .neighbors_directed(t2_filter, Direction::Outgoing)
                .next()
                .unwrap(),
            leaf,
            "Dependent filter should be inserted below join"
        );

        pull_all_required_columns(&mut query).unwrap();
    }
}
