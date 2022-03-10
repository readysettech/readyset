use std::iter;

use itertools::Either;
use nom_sql::analysis::ReferredColumns;
use nom_sql::{BinaryOperator, Expression};
use noria_errors::{unsupported, ReadySetResult};
use tracing::{instrument, trace};

use crate::node::{MirNode, MirNodeInner};
use crate::query::MirQuery;
use crate::{Column, MirNodeRef};

/// Push the given `node`, which should be a [filter][] node with the given `dependency` on columns
/// on the left-hand side of the given `dependent_join`, one step towards being below that dependent
/// join.
///
/// Internally, this uses the following rules to push filters down the graph:
///
/// - [`Project`], [`Join`], [`LeftJoin`], and dependent joins *other* than the one this filter
///   depends on are all totally commutative with filters, so can be swapped in position with those
///   filters with impunity
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
/// [`Join`]: MirNodeInner::LeftJoin
/// [`Aggregation`]: MirNodeInner::Aggregation
/// [`Extremum`]: MirNodeInner::Extremum
/// [unsupported error]: noria_errors::ReadySetError::Unsupported
/// [`pull_all_required_columns`]: noria_mir::rewrite::pull_columns::pull_all_required_columns
fn push_dependent_filter(
    node: MirNodeRef,
    dependent_join: MirNodeRef,
    dependency: DependentCondition,
) -> ReadySetResult<()> {
    if node.borrow().children().len() != 1 {
        // TODO: this probably happens for unions; we should try to deal with that at some point
        // (see what the HyPer and SQL Server papers say about disjunctive predicates)
        unsupported!();
    }

    // TODO(grfn): Using cyclical Rc<_> here makes this whole thing *extremely* annoying to
    // deal with without getting crazy runtime borrow errors - this is a prime candidate for
    // completely rewriting once MIR is in a proper graph data structure like petgraph

    let child = node.borrow().children().first().unwrap().clone();
    MirNode::remove(node.clone());

    let dependent_join_name = dependent_join.borrow().versioned_name();
    let mut child_ref = child.borrow_mut();
    let child_name = child_ref.versioned_name();

    trace!(
        "Lifting `{}` above `{}`",
        node.borrow().versioned_name(),
        child_name
    );
    let should_insert = match &mut child_ref.inner {
        MirNodeInner::DependentJoin {
            on_left, on_right, ..
        } if child_name == dependent_join_name => match dependency {
            DependentCondition::JoinKey { lhs, rhs } => {
                on_left.push(lhs.clone());
                on_right.push(rhs.clone());
                child_ref.add_column(lhs)?;
                child_ref.add_column(rhs)?;
                false
            }
            DependentCondition::FullyDependent { .. } => true,
        },
        MirNodeInner::Project { .. }
        | MirNodeInner::Filter { .. }
        | MirNodeInner::Join { .. }
        | MirNodeInner::LeftJoin { .. }
        | MirNodeInner::DependentJoin { .. } => true,
        MirNodeInner::Aggregation { .. } | MirNodeInner::Extremum { .. } => {
            for col in dependency.non_dependent_columns() {
                child_ref.add_column(col.clone())?;
            }
            for child_descendant in child_ref.topo_descendants() {
                if !matches!(
                    child_descendant.borrow().inner,
                    // We'll update these below, outside this match block, since we need to drop
                    // the child ref before we do it to avoid runtime borrow
                    // errors (ugh RefCell!)
                    MirNodeInner::Filter { .. }
                        | MirNodeInner::TopK { .. }
                        | MirNodeInner::Latest { .. }
                        | MirNodeInner::Identity
                ) {
                    for col in dependency.non_dependent_columns() {
                        child_descendant.borrow_mut().add_column(col.clone())?;
                    }
                }
            }
            true
        }
        inner => unsupported!(
            "Don't know how to push filter below {} to decorrelate",
            inner.description()
        ),
    };
    drop(child_ref);

    for descendant in child.borrow().topo_descendants() {
        if matches!(
            descendant.borrow().inner,
            MirNodeInner::Filter { .. }
                | MirNodeInner::TopK { .. }
                | MirNodeInner::Latest { .. }
                | MirNodeInner::Identity
        ) {
            // These nodes all copy their parent's columns verbatim, so we have to update that here
            // (ugh!)
            let columns = descendant
                .borrow()
                .ancestors()
                .first()
                .unwrap()
                .upgrade()
                .unwrap()
                .borrow()
                .columns()
                .to_vec();
            descendant.borrow_mut().columns = columns;
        }
    }

    if should_insert {
        MirNode::splice_child_below(child, node);
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
#[instrument(skip_all, fields(query = %query.name))]
pub(super) fn eliminate_dependent_joins(query: &mut MirQuery) -> ReadySetResult<()> {
    // TODO(grfn): lots of opportunity for memoization here, but the MIR RefCell mess makes that a
    // lot harder
    loop {
        let dependent_joins = query
            .topo_nodes()
            .into_iter()
            .filter(|n| n.borrow().inner.is_dependent_join())
            .collect::<Vec<_>>();

        // Try to rewrite one dependent join at a time, starting with the topmost
        let join = if let Some(join) = dependent_joins.last() {
            join.clone()
        } else {
            trace!("No more dependent joins left");
            break;
        };

        // Dependent joins allow nodes on the right to reference columns from the left side
        let left_parent = join
            .borrow()
            .ancestors()
            .first()
            .expect("Joins must have at least two parents")
            .upgrade()
            .unwrap();
        let right_parent = join
            .borrow()
            .ancestors()
            .get(1)
            .expect("Joins must have at least two parents")
            .upgrade()
            .unwrap();
        let left_roots = left_parent.borrow().root_ancestors();
        let left_columns = left_roots
            .flat_map(|n| n.borrow().columns().to_vec())
            .collect::<Vec<_>>();

        // If a node is a dependent filter, return a description of *how* it's a dependent
        // filter
        let dependent_condition = |node: &MirNodeRef| {
            if let MirNodeInner::Filter { conditions } = &node.borrow().inner {
                match conditions {
                    Expression::BinaryOp {
                        lhs: box Expression::Column(left_col),
                        op: BinaryOperator::Equal,
                        rhs: box Expression::Column(right_col),
                    } => {
                        let matches_left = left_columns.iter().any(|c| *c == *left_col);
                        let matches_right = left_columns.iter().any(|c| *c == *right_col);
                        match (matches_left, matches_right) {
                            // Both sides are dependent
                            (true, true) => {
                                return Some(DependentCondition::FullyDependent {
                                    non_dependent_cols: vec![],
                                })
                            }
                            (true, false) => {
                                return Some(DependentCondition::JoinKey {
                                    lhs: left_col.into(),
                                    rhs: right_col.into(),
                                });
                            }
                            (false, true) => {
                                return Some(DependentCondition::JoinKey {
                                    lhs: right_col.into(),
                                    rhs: left_col.into(),
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
                            return Some(DependentCondition::FullyDependent {
                                non_dependent_cols: expr
                                    .referred_columns()
                                    .filter(|expr_col| {
                                        !left_columns.iter().any(|c| *c == **expr_col)
                                    })
                                    .map(|c| c.into())
                                    .collect(),
                            });
                        } else {
                            {}
                        }
                    }
                }
            }
            None
        };
        drop(left_parent);

        let dependency = dependent_condition(&right_parent)
            .map(|dep| (right_parent.clone(), dep))
            .or_else(|| {
                right_parent
                    .borrow()
                    .topo_ancestors()
                    .find_map(|n| dependent_condition(&n).map(|dep| (n, dep)))
            });

        if let Some((node, dependency)) = dependency {
            push_dependent_filter(node, join, dependency)?;
        } else {
            trace!(
                dependent_join = %join.borrow().versioned_name(),
                "Can't find any more dependent nodes, done rewriting!"
            );
            // Can't find any dependent nodes, which means the join isn't dependent anymore! So turn
            // it into a non-dependent join
            let new_inner = match &join.borrow().inner {
                MirNodeInner::DependentJoin {
                    on_left,
                    on_right,
                    project,
                } => MirNodeInner::Join {
                    on_left: on_left.clone(),
                    on_right: on_right.clone(),
                    project: project.clone(),
                },
                _ => unreachable!("Already checked is_dependent_join above"),
            };
            join.borrow_mut().inner = new_inner;
        };
    }
    Ok(())
}

/// For a dependent filter, a description of *how* that filter depends on columns from the left side
/// of a dependent join
enum DependentCondition {
    /// This filter can be turned into a equi-join key
    ///
    /// TODO: this should be removed at some point in the future and be replaced by a dedicated
    /// pass that turns filter nodes below joins into join keys
    JoinKey { lhs: Column, rhs: Column },
    /// The entire filter is dependent, and must be lifted above the join wholesale
    FullyDependent {
        /// A list of the columns mentioned in the condition that do *not* refer to columns on the
        /// left-hand side of the join
        non_dependent_cols: Vec<Column>,
    },
}

impl DependentCondition {
    /// Return an iterator over all columns referenced by this dependent condition that are *not* in
    /// the left-hand side of the dependent join
    fn non_dependent_columns(&self) -> impl Iterator<Item = &Column> + '_ {
        match self {
            DependentCondition::JoinKey { rhs, .. } => Either::Left(iter::once(rhs)),
            DependentCondition::FullyDependent { non_dependent_cols } => {
                Either::Right(non_dependent_cols.iter())
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::panic)] // it's a test
mod tests {
    use std::rc::Rc;

    use common::{DataType, IndexType};
    use dataflow::ops::grouped::aggregate::Aggregation;
    use nom_sql::{BinaryOperator, ColumnSpecification, Expression, Literal, SqlType};

    use super::*;
    use crate::node::{MirNode, MirNodeInner};
    use crate::rewrite::pull_columns::pull_all_required_columns;
    use crate::visualize::GraphViz;
    use crate::Column;

    #[test]
    fn exists_ish() {
        readyset_logging::init_test_logging();
        // query looks something like:
        //     SELECT t1.a FROM t1 WHERE EXISTS (SELECT * FROM t2 WHERE t2.a = t1.a)

        let t2 = MirNode::new(
            "t2".into(),
            0,
            vec![Column::new(Some("t2"), "a")],
            MirNodeInner::Base {
                column_specs: vec![(
                    ColumnSpecification {
                        column: nom_sql::Column::from("t2.a"),
                        sql_type: SqlType::Int(None),
                        constraints: vec![],
                        comment: None,
                    },
                    None,
                )],
                primary_key: Some([Column::new(Some("t2"), "a")].into()),
                unique_keys: Default::default(),
                adapted_over: None,
            },
            vec![],
            vec![],
        );
        // t2 -> ...

        // -> σ[t2.a = t1.a]
        let t2_filter = MirNode::new(
            "t2_filter".into(),
            0,
            vec![Column::new(Some("t2"), "a")],
            MirNodeInner::Filter {
                conditions: Expression::BinaryOp {
                    lhs: Box::new(Expression::Column("t2.a".into())),
                    op: BinaryOperator::Equal,
                    rhs: Box::new(Expression::Column("t1.a".into())),
                },
            },
            vec![Rc::downgrade(&t2)],
            vec![],
        );

        // -> π[lit: 0, lit: 0]
        let group_proj = MirNode::new(
            "q_prj_hlpr".into(),
            0,
            vec![Column::named("__count_val"), Column::named("__count_grp")],
            MirNodeInner::Project {
                emit: vec![],
                expressions: vec![],
                literals: vec![
                    ("__count_val".into(), DataType::from(0u32)),
                    ("__count_grp".into(), DataType::from(0u32)),
                ],
            },
            vec![Rc::downgrade(&t2_filter)],
            vec![],
        );
        // -> [0, 0] for each row

        // -> |0| γ[1]
        let exists_count = MirNode::new(
            "__exists_count".into(),
            0,
            vec![
                Column::named("__count_val"),
                Column::named("__count_grp"),
                Column::named("__exists_count"),
            ],
            MirNodeInner::Aggregation {
                on: Column::named("__count_val"),
                group_by: vec![Column::named("__count_grp")],
                kind: Aggregation::Count { count_nulls: true },
            },
            vec![Rc::downgrade(&group_proj)],
            vec![],
        );
        // -> [0, <count>] for each row

        // -> σ[c1 > 0]
        let gt_0_filter = MirNode::new(
            "count_gt_0".into(),
            0,
            vec![
                Column::named("__count_val"),
                Column::named("__count_grp"),
                Column::named("__exists_count"),
            ],
            MirNodeInner::Filter {
                conditions: Expression::BinaryOp {
                    lhs: Box::new(Expression::Column("__exists_count".into())),
                    op: BinaryOperator::Greater,
                    rhs: Box::new(Expression::Literal(Literal::Integer(0))),
                },
            },
            vec![Rc::downgrade(&exists_count)],
            vec![],
        );

        let t1 = MirNode::new(
            "t1".into(),
            0,
            vec![Column::new(Some("t1"), "a")],
            MirNodeInner::Base {
                column_specs: vec![(
                    ColumnSpecification {
                        column: nom_sql::Column::from("a"),
                        sql_type: SqlType::Int(None),
                        constraints: vec![],
                        comment: None,
                    },
                    None,
                )],
                primary_key: Some([Column::from("a")].into()),
                unique_keys: Default::default(),
                adapted_over: None,
            },
            vec![],
            vec![],
        );
        // t1 -> ...

        // -> π[..., lit: 0]
        let left_literal_join_key_proj = MirNode::new(
            "t1_join_key".into(),
            0,
            vec![Column::new(Some("t1"), "a")],
            MirNodeInner::Project {
                emit: vec![Column::new(Some("t1"), "a")],
                expressions: vec![],
                literals: vec![("__exists_join_key".into(), DataType::from(0u32))],
            },
            vec![Rc::downgrade(&t1)],
            vec![],
        );

        // -> ⧑ on: l.__exists_join_key ≡ r.__count_grp
        let join_columns = vec![
            Column::new(Some("t1"), "a"),
            Column::named("__exists_join_key"),
            Column::named("__count_grp"),
            Column::named("__exists_count"),
        ];
        let exists_join = MirNode::new(
            "exists_join".into(),
            0,
            join_columns.clone(),
            MirNodeInner::DependentJoin {
                on_left: vec![Column::named("__exists_join_key")],
                on_right: vec![Column::named("__count_grp")],
                project: join_columns.clone(),
            },
            vec![
                Rc::downgrade(&left_literal_join_key_proj),
                Rc::downgrade(&gt_0_filter),
            ],
            vec![],
        );

        let leaf = MirNode::new(
            "q".into(),
            0,
            join_columns,
            MirNodeInner::leaf(vec![], IndexType::HashMap),
            vec![Rc::downgrade(&exists_join)],
            vec![],
        );

        let mut query = MirQuery {
            name: "q".into(),
            roots: vec![t1, t2],
            leaf,
        };

        eliminate_dependent_joins(&mut query).unwrap();

        eprintln!("{}", query.to_graphviz());

        match &exists_join.borrow().inner {
            MirNodeInner::Join {
                on_left, on_right, ..
            } => {
                assert_eq!(on_left.len(), 2);
                assert_eq!(on_right.len(), 2);

                let left_pos = on_left
                    .iter()
                    .position(|col| col.name == "a" && col.table == Some("t1".into()));
                let right_pos = on_right
                    .iter()
                    .position(|col| col.name == "a" && col.table == Some("t2".into()));

                assert!(left_pos.is_some());
                assert!(right_pos.is_some());

                assert_eq!(left_pos.unwrap(), right_pos.unwrap());
            }
            _ => panic!(
                "should have rewritten dependent to non-dependent join (got: {})",
                exists_join.borrow().description()
            ),
        };

        assert_eq!(
            group_proj
                .borrow()
                .ancestors()
                .first()
                .unwrap()
                .upgrade()
                .unwrap()
                .borrow()
                .name(),
            "t2",
            "t2_filter should be removed"
        );

        assert!(pull_all_required_columns(&mut query).is_ok());
    }

    #[test]
    fn multiple_filters_after_agg() {
        readyset_logging::init_test_logging();
        // query looks something like:
        //     SELECT t1.a FROM t1
        //     WHERE EXISTS (
        //         SELECT 1 FROM t2 WHERE t2.a = t1.a GROUP BY t2.b
        //         HAVING COUNT(t2.b) BETWEEN 4 AND 7
        //     )

        let t2 = MirNode::new(
            "t2".into(),
            0,
            vec![Column::new(Some("t2"), "a")],
            MirNodeInner::Base {
                column_specs: vec![
                    (
                        ColumnSpecification {
                            column: nom_sql::Column::from("t2.a"),
                            sql_type: SqlType::Int(None),
                            constraints: vec![],
                            comment: None,
                        },
                        None,
                    ),
                    (
                        ColumnSpecification {
                            column: nom_sql::Column::from("t2.b"),
                            sql_type: SqlType::Int(None),
                            constraints: vec![],
                            comment: None,
                        },
                        None,
                    ),
                ],
                primary_key: Some([Column::new(Some("t2"), "a")].into()),
                unique_keys: Default::default(),
                adapted_over: None,
            },
            vec![],
            vec![],
        );
        // t2 -> ...

        // -> σ[t2.a = t1.a]
        let t2_filter = MirNode::new(
            "t2_filter".into(),
            0,
            vec![Column::new(Some("t2"), "a")],
            MirNodeInner::Filter {
                conditions: Expression::BinaryOp {
                    lhs: Box::new(Expression::Column("t2.a".into())),
                    op: BinaryOperator::Equal,
                    rhs: Box::new(Expression::Column("t1.a".into())),
                },
            },
            vec![Rc::downgrade(&t2)],
            vec![],
        );

        // -> |*|(t2.b) γ[t2.b]
        let t2_count = MirNode::new(
            "q_t2_count".into(),
            0,
            vec![Column::new(Some("t2"), "b"), Column::named("COUNT(t2.b)")],
            MirNodeInner::Aggregation {
                on: Column::new(Some("t2"), "b"),
                group_by: vec![Column::new(Some("t2"), "b")],
                kind: Aggregation::Count { count_nulls: false },
            },
            vec![Rc::downgrade(&t2_filter)],
            vec![],
        );

        // -> σ[count(t2.b) >= 4]
        let t2_count_f1 = MirNode::new(
            "q_t2_count_f1".into(),
            0,
            vec![Column::new(Some("t2"), "b"), Column::named("COUNT(t2.b)")],
            MirNodeInner::Filter {
                conditions: Expression::BinaryOp {
                    lhs: Box::new(Expression::Column(nom_sql::Column {
                        table: None,
                        name: "COUNT(t2.b)".into(),
                    })),
                    op: BinaryOperator::GreaterOrEqual,
                    rhs: Box::new(Expression::Literal(4.into())),
                },
            },
            vec![Rc::downgrade(&t2_count)],
            vec![],
        );

        // -> σ[count(t2.b) <= 7]
        let t2_count_f2 = MirNode::new(
            "q_t2_count_f2".into(),
            0,
            vec![Column::new(Some("t2"), "b"), Column::named("COUNT(t2.b)")],
            MirNodeInner::Filter {
                conditions: Expression::BinaryOp {
                    lhs: Box::new(Expression::Column(nom_sql::Column {
                        table: None,
                        name: "COUNT(t2.b)".into(),
                    })),
                    op: BinaryOperator::LessOrEqual,
                    rhs: Box::new(Expression::Literal(7.into())),
                },
            },
            vec![Rc::downgrade(&t2_count_f1)],
            vec![],
        );

        // -> π[lit: 0, lit: 0]
        let group_proj = MirNode::new(
            "q_prj_hlpr".into(),
            0,
            vec![Column::named("__count_val"), Column::named("__count_grp")],
            MirNodeInner::Project {
                emit: vec![],
                expressions: vec![],
                literals: vec![
                    ("__count_val".into(), DataType::from(0u32)),
                    ("__count_grp".into(), DataType::from(0u32)),
                ],
            },
            vec![Rc::downgrade(&t2_count_f2)],
            vec![],
        );
        // -> [0, 0] for each row

        // -> |0| γ[1]
        let exists_count = MirNode::new(
            "__exists_count".into(),
            0,
            vec![
                Column::named("__count_val"),
                Column::named("__count_grp"),
                Column::named("__exists_count"),
            ],
            MirNodeInner::Aggregation {
                on: Column::named("__count_val"),
                group_by: vec![Column::named("__count_grp")],
                kind: Aggregation::Count { count_nulls: true },
            },
            vec![Rc::downgrade(&group_proj)],
            vec![],
        );
        // -> [0, <count>] for each row

        // -> σ[c1 > 0]
        let gt_0_filter = MirNode::new(
            "count_gt_0".into(),
            0,
            vec![
                Column::named("__count_val"),
                Column::named("__count_grp"),
                Column::named("__exists_count"),
            ],
            MirNodeInner::Filter {
                conditions: Expression::BinaryOp {
                    lhs: Box::new(Expression::Column("__exists_count".into())),
                    op: BinaryOperator::Greater,
                    rhs: Box::new(Expression::Literal(Literal::Integer(0))),
                },
            },
            vec![Rc::downgrade(&exists_count)],
            vec![],
        );

        let t1 = MirNode::new(
            "t1".into(),
            0,
            vec![Column::new(Some("t1"), "a")],
            MirNodeInner::Base {
                column_specs: vec![(
                    ColumnSpecification {
                        column: nom_sql::Column::from("a"),
                        sql_type: SqlType::Int(None),
                        constraints: vec![],
                        comment: None,
                    },
                    None,
                )],
                primary_key: Some([Column::from("a")].into()),
                unique_keys: Default::default(),
                adapted_over: None,
            },
            vec![],
            vec![],
        );
        // t1 -> ...

        // -> π[..., lit: 0]
        let left_literal_join_key_proj = MirNode::new(
            "t1_join_key".into(),
            0,
            vec![Column::new(Some("t1"), "a")],
            MirNodeInner::Project {
                emit: vec![Column::new(Some("t1"), "a")],
                expressions: vec![],
                literals: vec![("__exists_join_key".into(), DataType::from(0u32))],
            },
            vec![Rc::downgrade(&t1)],
            vec![],
        );

        // -> ⧑ on: l.__exists_join_key ≡ r.__count_grp
        let join_columns = vec![
            Column::new(Some("t1"), "a"),
            Column::named("__exists_join_key"),
            Column::named("__count_grp"),
            Column::named("__exists_count"),
        ];
        let exists_join = MirNode::new(
            "exists_join".into(),
            0,
            join_columns.clone(),
            MirNodeInner::DependentJoin {
                on_left: vec![Column::named("__exists_join_key")],
                on_right: vec![Column::named("__count_grp")],
                project: join_columns.clone(),
            },
            vec![
                Rc::downgrade(&left_literal_join_key_proj),
                Rc::downgrade(&gt_0_filter),
            ],
            vec![],
        );

        let leaf = MirNode::new(
            "q".into(),
            0,
            join_columns,
            MirNodeInner::leaf(vec![], IndexType::HashMap),
            vec![Rc::downgrade(&exists_join)],
            vec![],
        );

        let mut query = MirQuery {
            name: "q".into(),
            roots: vec![t1, t2],
            leaf,
        };

        eliminate_dependent_joins(&mut query).unwrap();

        eprintln!("{}", query.to_graphviz());

        assert_eq!(
            t2_count_f2.borrow().columns(),
            &[
                Column::new(Some("t2"), "b"),
                Column::new(Some("t2"), "a"),
                Column::named("COUNT(t2.b)"),
            ],
            "should update columns of filters recursively"
        );

        match &exists_join.borrow().inner {
            MirNodeInner::Join {
                on_left, on_right, ..
            } => {
                assert_eq!(on_left.len(), 2);
                assert_eq!(on_right.len(), 2);

                let left_pos = on_left
                    .iter()
                    .position(|col| col.name == "a" && col.table == Some("t1".into()));
                let right_pos = on_right
                    .iter()
                    .position(|col| col.name == "a" && col.table == Some("t2".into()));

                assert!(left_pos.is_some());
                assert!(right_pos.is_some());

                assert_eq!(left_pos.unwrap(), right_pos.unwrap());
            }
            _ => panic!(
                "should have rewritten dependent to non-dependent join (got: {})",
                exists_join.borrow().description()
            ),
        };

        assert!(pull_all_required_columns(&mut query).is_ok());
    }
}
