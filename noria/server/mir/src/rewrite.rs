use noria::ReadySetResult;

use crate::column::Column;
use crate::query::MirQuery;
use crate::MirNodeRef;

fn has_column(n: &MirNodeRef, column: &Column) -> bool {
    if n.borrow().columns().contains(column) {
        return true;
    } else {
        for a in n.borrow().ancestors().iter().map(|n| n.upgrade().unwrap()) {
            if has_column(&a, column) {
                return true;
            }
        }
    }
    false
}

pub(super) fn pull_required_base_columns(q: &mut MirQuery) -> ReadySetResult<()> {
    let mut queue = vec![q.leaf.clone()];

    while let Some(mn) = queue.pop() {
        // a node needs all of the columns it projects into its output
        // however, it may also need *additional* columns to perform its functionality; consider,
        // e.g., a filter that filters on a column that it doesn't project
        let needed_columns: Vec<Column> = mn
            .borrow()
            .referenced_columns()
            .into_iter()
            .filter(|c| {
                !mn.borrow()
                    .ancestors()
                    .iter()
                    .map(|n| n.upgrade().unwrap())
                    .any(|a| a.borrow().columns().iter().any(|ac| ac == c))
            })
            .collect();

        let mut found: Vec<&Column> = Vec::new();
        for ancestor in mn.borrow().ancestors().iter().map(|n| n.upgrade().unwrap()) {
            if ancestor.borrow().ancestors().is_empty() {
                // base, do nothing
                continue;
            }
            for c in &needed_columns {
                if !found.contains(&c) && has_column(&ancestor, c) {
                    ancestor.borrow_mut().add_column(c.clone())?;
                    found.push(c);
                }
            }
            queue.push(ancestor.clone());
        }
    }
    Ok(())
}

// currently unused
#[allow(dead_code)]
pub(super) fn push_all_base_columns(q: &mut MirQuery) -> ReadySetResult<()> {
    let mut queue = Vec::new();
    queue.extend(q.roots.clone());

    while !queue.is_empty() {
        let mn = queue.pop().unwrap();
        let columns = mn.borrow().columns().to_vec();
        for child in mn.borrow().children() {
            // N.B. this terminates before reaching the actual leaf, since the last node of the
            // query (before the MIR `Leaf` node) already carries the query name. (`Leaf` nodes are
            // virtual nodes that will be removed and converted into materializations.)
            if child.borrow().versioned_name() == q.leaf.borrow().versioned_name() {
                continue;
            }
            for c in &columns {
                // push through if the child doesn't already have this column
                if !child.borrow().columns().contains(c) {
                    child.borrow_mut().add_column(c.clone())?;
                }
            }
            queue.push(child.clone());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    mod pull_required_base_columns {
        use dataflow::ops::grouped::aggregate::Aggregation;
        use nom_sql::{
            BinaryOperator, ColumnSpecification, Expression, FunctionExpression, Literal, SqlType,
        };

        use super::*;
        use crate::node::node_inner::MirNodeInner;
        use crate::node::MirNode;

        #[test]
        fn changing_index() {
            let base = MirNode::new(
                "base",
                0,
                vec!["a".into(), "b".into(), "c".into()],
                MirNodeInner::Base {
                    column_specs: vec![
                        (
                            ColumnSpecification {
                                column: nom_sql::Column::from("a"),
                                sql_type: SqlType::Int(None),
                                constraints: vec![],
                                comment: None,
                            },
                            None,
                        ),
                        (
                            ColumnSpecification {
                                column: nom_sql::Column::from("b"),
                                sql_type: SqlType::Int(None),
                                constraints: vec![],
                                comment: None,
                            },
                            None,
                        ),
                        (
                            ColumnSpecification {
                                column: nom_sql::Column::from("c"),
                                sql_type: SqlType::Int(None),
                                constraints: vec![],
                                comment: None,
                            },
                            None,
                        ),
                    ],
                    primary_key: Some([Column::from("a")].into()),
                    unique_keys: Default::default(),
                    adapted_over: None,
                },
                vec![],
                vec![],
            );

            // SUM(b)
            let grp = MirNode::new(
                "grp",
                0,
                vec!["agg".into()],
                MirNodeInner::Aggregation {
                    on: "b".into(),
                    group_by: vec![],
                    kind: Aggregation::Sum,
                },
                vec![MirNodeRef::downgrade(&base)],
                vec![],
            );

            let condition_expression_1 = Expression::BinaryOp {
                op: BinaryOperator::Equal,
                lhs: Box::new(Expression::Column("a".into())),
                rhs: Box::new(Expression::Literal(Literal::Integer(1))),
            };

            // σ[a = 1]
            let fil = MirNode::new(
                "fil",
                0,
                vec!["a".into(), "agg".into()],
                MirNodeInner::Filter {
                    conditions: condition_expression_1.clone(),
                },
                vec![MirNodeRef::downgrade(&grp)],
                vec![],
            );

            // a, agg, IFNULL(c, 0) as c0
            let prj = MirNode::new(
                "prj",
                0,
                vec!["a".into(), "agg".into(), "c0".into()],
                MirNodeInner::Project {
                    emit: vec!["a".into(), "agg".into()],
                    expressions: vec![(
                        "c0".to_owned(),
                        Expression::Call(FunctionExpression::Call {
                            name: "ifnull".to_owned(),
                            arguments: vec![
                                Expression::Column("c".into()),
                                Expression::Literal(0.into()),
                            ],
                        }),
                    )],
                    literals: vec![],
                },
                vec![MirNodeRef::downgrade(&fil)],
                vec![],
            );

            let mut query = MirQuery {
                name: "changing_index".to_owned(),
                roots: vec![base],
                leaf: prj,
            };

            pull_required_base_columns(&mut query).unwrap();

            assert_eq!(
                grp.borrow().columns(),
                &[Column::from("c"), Column::from("a"), Column::from("agg")]
            );

            // The filter has to add the column in the same place as the aggregate
            assert_eq!(
                fil.borrow().columns(),
                &[Column::from("c"), Column::from("a"), Column::from("agg")]
            );

            // The filter has to filter on the correct field
            match &fil.borrow().inner {
                MirNodeInner::Filter { conditions, .. } => {
                    assert_eq!(conditions, &condition_expression_1)
                }
                _ => unreachable!(),
            };
        }
    }
}
