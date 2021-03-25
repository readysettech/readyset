use nom_sql::{
    BinaryOperator, ConditionBase, ConditionExpression, ConditionTree, ItemPlaceholder, Literal,
    SqlQuery,
};

use launchpad::or_else_result;
use noria::{unsupported, ReadySetResult};
use std::mem;

fn collapse_where_in_recursive(
    leftmost_param_index: &mut usize,
    expr: &mut ConditionExpression,
    rewrite_literals: bool,
) -> ReadySetResult<Option<(usize, Vec<Literal>)>> {
    Ok(match *expr {
        ref x @ ConditionExpression::Arithmetic(_) => {
            unsupported!("arithmetic not supported yet: {}", x)
        }
        ConditionExpression::Base(ConditionBase::Literal(Literal::Placeholder(
            ItemPlaceholder::QuestionMark,
        )))
        | ConditionExpression::Base(ConditionBase::Literal(Literal::Placeholder(
            ItemPlaceholder::DollarNumber(_),
        ))) => {
            *leftmost_param_index += 1;
            None
        }
        ConditionExpression::Base(ConditionBase::NestedSelect(ref mut sq)) => {
            if let Some(ref mut w) = sq.where_clause {
                collapse_where_in_recursive(leftmost_param_index, w, rewrite_literals)?
            } else {
                None
            }
        }
        ConditionExpression::Base(ConditionBase::LiteralList(ref list)) => {
            *leftmost_param_index += list
                .iter()
                .filter(|&l| {
                    matches!(l,
                        Literal::Placeholder(ItemPlaceholder::QuestionMark) |
                        Literal::Placeholder(ItemPlaceholder::DollarNumber(_)
                    ))
                })
                .count();
            None
        }
        ConditionExpression::Base(_) => None,
        ConditionExpression::NegationOp(ref mut ce)
        | ConditionExpression::Bracketed(ref mut ce) => {
            collapse_where_in_recursive(leftmost_param_index, ce, rewrite_literals)?
        }
        ConditionExpression::LogicalOp(ref mut ct) => {
            or_else_result(
                collapse_where_in_recursive(leftmost_param_index, &mut *ct.left, rewrite_literals)?,
                || {
                    // we can't also try rewriting ct.right, as it'd make it hard to recover
                    // literals: if we rewrote WHERE x IN (a, b) in left and WHERE y IN (1, 2) in
                    // right into WHERE x = ? ... y = ?, then what param values should we use?
                    collapse_where_in_recursive(
                        leftmost_param_index,
                        &mut *ct.right,
                        rewrite_literals,
                    )
                },
            )?
        }
        ConditionExpression::ComparisonOp(ref mut ct) if ct.operator != BinaryOperator::In => {
            or_else_result(
                collapse_where_in_recursive(leftmost_param_index, &mut *ct.left, rewrite_literals)?,
                || {
                    collapse_where_in_recursive(
                        leftmost_param_index,
                        &mut *ct.right,
                        rewrite_literals,
                    )
                },
            )?
        }
        ConditionExpression::ComparisonOp(ref mut ct) => {
            let mut do_it = false;
            let literals =
                if let ConditionExpression::Base(ConditionBase::LiteralList(ref mut list)) =
                    *ct.right
                {
                    if rewrite_literals
                        || (list.iter().all(|l| {
                            matches!(l,
                            Literal::Placeholder(ItemPlaceholder::QuestionMark)
                            | Literal::Placeholder(ItemPlaceholder::DollarNumber(_)))
                        }))
                    {
                        do_it = true;
                        mem::replace(list, Vec::new())
                    } else {
                        Vec::new()
                    }
                } else {
                    Vec::new()
                };

            if !do_it {
                return or_else_result(
                    collapse_where_in_recursive(
                        leftmost_param_index,
                        &mut *ct.left,
                        rewrite_literals,
                    )?,
                    || {
                        collapse_where_in_recursive(
                            leftmost_param_index,
                            &mut *ct.right,
                            rewrite_literals,
                        )
                    },
                );
            }

            if let ConditionExpression::Base(ConditionBase::Field(_)) = *ct.left {
            } else {
                unsupported!("unsupported condition expression: {:?}", ct.left);
            }

            if literals.is_empty() {
                // TODO(eta): probably shouldn't be unsupported; was eprintln before
                unsupported!("spotted empty WHERE IN ()");
            }

            let replacement_literal = match literals[0] {
                Literal::Placeholder(ItemPlaceholder::DollarNumber(x)) => {
                    Literal::Placeholder(ItemPlaceholder::DollarNumber(x))
                }
                _ => Literal::Placeholder(ItemPlaceholder::QuestionMark),
            };

            let c = mem::replace(
                &mut ct.left,
                Box::new(ConditionExpression::Base(ConditionBase::Literal(
                    replacement_literal.clone(),
                ))),
            );
            *ct = ConditionTree {
                operator: BinaryOperator::Equal,
                left: c,
                right: Box::new(ConditionExpression::Base(ConditionBase::Literal(
                    replacement_literal,
                ))),
            };

            Some((*leftmost_param_index, literals))
        }
        ref x @ ConditionExpression::ExistsOp(_) => {
            unsupported!("EXISTS not supported yet: {}", x)
        }
        ConditionExpression::Between {
            ref mut operand,
            ref mut min,
            ref mut max,
        } => or_else_result(
            collapse_where_in_recursive(leftmost_param_index, &mut *operand, rewrite_literals)?,
            || {
                or_else_result(
                    collapse_where_in_recursive(leftmost_param_index, &mut *min, rewrite_literals)?,
                    || {
                        collapse_where_in_recursive(
                            leftmost_param_index,
                            &mut *max,
                            rewrite_literals,
                        )
                    },
                )
            },
        )?,
    })
}

pub(crate) fn collapse_where_in(
    query: &mut SqlQuery,
    rewrite_literals: bool,
) -> ReadySetResult<Option<(usize, Vec<Literal>)>> {
    if let SqlQuery::Select(ref mut sq) = *query {
        if let Some(ref mut w) = sq.where_clause {
            let mut left_edge = 0;
            return collapse_where_in_recursive(&mut left_edge, w, rewrite_literals);
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use nom_sql;

    #[test]
    fn collapsed_where_placeholders() {
        let mut q = nom_sql::parse_query("SELECT * FROM x WHERE x.y IN (?, ?, ?)").unwrap();
        let rewritten = collapse_where_in(&mut q, false).unwrap().unwrap();
        assert_eq!(rewritten.0, 0);
        assert_eq!(rewritten.1.len(), 3);
        assert_eq!(
            q,
            nom_sql::parse_query("SELECT * FROM x WHERE x.y = ?").unwrap()
        );

        let mut q = nom_sql::parse_query("SELECT * FROM x WHERE y IN (?, ?, ?)").unwrap();
        let rewritten = collapse_where_in(&mut q, false).unwrap().unwrap();
        assert_eq!(rewritten.0, 0);
        assert_eq!(rewritten.1.len(), 3);
        assert_eq!(
            q,
            nom_sql::parse_query("SELECT * FROM x WHERE y = ?").unwrap()
        );

        let mut q = nom_sql::parse_query("SELECT * FROM x WHERE AVG(y) IN (?, ?, ?)").unwrap();
        let rewritten = collapse_where_in(&mut q, false).unwrap().unwrap();
        assert_eq!(rewritten.0, 0);
        assert_eq!(rewritten.1.len(), 3);
        assert_eq!(
            q,
            nom_sql::parse_query("SELECT * FROM x WHERE AVG(y) = ?").unwrap()
        );

        let mut q = nom_sql::parse_query("SELECT * FROM t WHERE x = ? AND y IN (?, ?, ?) OR z = ?")
            .unwrap();
        let rewritten = collapse_where_in(&mut q, false).unwrap().unwrap();
        assert_eq!(rewritten.0, 1);
        assert_eq!(rewritten.1.len(), 3);
        assert_eq!(
            q,
            nom_sql::parse_query("SELECT * FROM t WHERE x = ? AND y = ? OR z = ?").unwrap()
        );

        let mut q = nom_sql::parse_query(
            "SELECT * FROM t WHERE x IN (SELECT * FROM z WHERE a = ?) AND y IN (?, ?) OR z = ?",
        )
        .unwrap();
        let rewritten = collapse_where_in(&mut q, false).unwrap().unwrap();
        assert_eq!(rewritten.0, 1);
        assert_eq!(rewritten.1.len(), 2);
        assert_eq!(
            q,
            nom_sql::parse_query(
                "SELECT * FROM t WHERE x IN (SELECT * FROM z WHERE a = ?) AND y = ? OR z = ?"
            )
            .unwrap()
        );

        let mut q = nom_sql::parse_query(
            "SELECT * FROM t WHERE x IN (SELECT * FROM z WHERE b = ? AND a IN (?, ?)) OR z = ?",
        )
        .unwrap();
        let rewritten = collapse_where_in(&mut q, false).unwrap().unwrap();
        assert_eq!(rewritten.0, 1);
        assert_eq!(rewritten.1.len(), 2);
        assert_eq!(
            q,
            nom_sql::parse_query(
                "SELECT * FROM t WHERE x IN (SELECT * FROM z WHERE b = ? AND a = ?) OR z = ?",
            )
            .unwrap()
        );
    }

    #[test]
    fn collapsed_where_literals() {
        let mut q = nom_sql::parse_query("SELECT * FROM x WHERE x.y IN (1, 2, 3)").unwrap();
        assert_eq!(collapse_where_in(&mut q, false).unwrap(), None);
        assert_eq!(
            q,
            nom_sql::parse_query("SELECT * FROM x WHERE x.y IN (1, 2, 3)").unwrap()
        );

        let mut q = nom_sql::parse_query("SELECT * FROM x WHERE x.y IN (1, 2, 3)").unwrap();
        let rewritten = collapse_where_in(&mut q, true).unwrap().unwrap();
        assert_eq!(rewritten.0, 0);
        assert_eq!(rewritten.1.len(), 3);
        assert_eq!(
            q,
            nom_sql::parse_query("SELECT * FROM x WHERE x.y = ?").unwrap()
        );

        let mut q = nom_sql::parse_query("SELECT * FROM x WHERE y IN (1, 2, 3)").unwrap();
        let rewritten = collapse_where_in(&mut q, true).unwrap().unwrap();
        assert_eq!(rewritten.0, 0);
        assert_eq!(rewritten.1.len(), 3);
        assert_eq!(
            q,
            nom_sql::parse_query("SELECT * FROM x WHERE y = ?").unwrap()
        );

        let mut q = nom_sql::parse_query("SELECT * FROM x WHERE AVG(y) IN (1, 2, 3)").unwrap();
        let rewritten = collapse_where_in(&mut q, true).unwrap().unwrap();
        assert_eq!(rewritten.0, 0);
        assert_eq!(rewritten.1.len(), 3);
        assert_eq!(
            q,
            nom_sql::parse_query("SELECT * FROM x WHERE AVG(y) = ?").unwrap()
        );
    }

    #[test]
    fn noninterference() {
        let mut q = nom_sql::parse_query("SELECT * FROM x WHERE x.y = 'foo'").unwrap();
        assert_eq!(collapse_where_in(&mut q, true).unwrap(), None);
        assert_eq!(
            q,
            nom_sql::parse_query("SELECT * FROM x WHERE x.y = 'foo'").unwrap()
        );
    }

    #[test]
    fn collapsed_where_dollarsign_placeholders() {
        let mut q = nom_sql::parse_query("SELECT * FROM x WHERE x.y IN ($1, $2, $3)").unwrap();
        let rewritten = collapse_where_in(&mut q, false).unwrap().unwrap();
        assert_eq!(rewritten.0, 0);
        assert_eq!(rewritten.1.len(), 3);
        assert_eq!(
            q,
            nom_sql::parse_query("SELECT * FROM x WHERE x.y = $1").unwrap()
        );

        let mut q = nom_sql::parse_query("SELECT * FROM x WHERE y IN ($1, $2, $3)").unwrap();
        let rewritten = collapse_where_in(&mut q, false).unwrap().unwrap();
        assert_eq!(rewritten.0, 0);
        assert_eq!(rewritten.1.len(), 3);
        assert_eq!(
            q,
            nom_sql::parse_query("SELECT * FROM x WHERE y = $1").unwrap()
        );

        let mut q = nom_sql::parse_query("SELECT * FROM x WHERE AVG(y) IN ($1, $2, $3)").unwrap();
        let rewritten = collapse_where_in(&mut q, false).unwrap().unwrap();
        assert_eq!(rewritten.0, 0);
        assert_eq!(rewritten.1.len(), 3);
        assert_eq!(
            q,
            nom_sql::parse_query("SELECT * FROM x WHERE AVG(y) = $1").unwrap()
        );

        let mut q =
            nom_sql::parse_query("SELECT * FROM t WHERE x = $1 AND y IN ($2, $3, $4) OR z = $5")
                .unwrap();
        let rewritten = collapse_where_in(&mut q, false).unwrap().unwrap();
        assert_eq!(rewritten.0, 1);
        assert_eq!(rewritten.1.len(), 3);
        assert_eq!(
            q,
            nom_sql::parse_query("SELECT * FROM t WHERE x = $1 AND y = $2 OR z = $5").unwrap()
        );

        let mut q = nom_sql::parse_query(
            "SELECT * FROM t WHERE x IN (SELECT * FROM z WHERE a = $1) AND y IN ($2, $3) OR z = $4",
        )
        .unwrap();
        let rewritten = collapse_where_in(&mut q, false).unwrap().unwrap();
        assert_eq!(rewritten.0, 1);
        assert_eq!(rewritten.1.len(), 2);
        assert_eq!(
            q,
            nom_sql::parse_query(
                "SELECT * FROM t WHERE x IN (SELECT * FROM z WHERE a = $1) AND y = $2 OR z = $4"
            )
            .unwrap()
        );

        let mut q = nom_sql::parse_query(
            "SELECT * FROM t WHERE x IN (SELECT * FROM z WHERE b = $1 AND a IN ($2, $3)) OR z = $4",
        )
        .unwrap();
        let rewritten = collapse_where_in(&mut q, false).unwrap().unwrap();
        assert_eq!(rewritten.0, 1);
        assert_eq!(rewritten.1.len(), 2);
        assert_eq!(
            q,
            nom_sql::parse_query(
                "SELECT * FROM t WHERE x IN (SELECT * FROM z WHERE b = $1 AND a = $2) OR z = $4",
            )
            .unwrap()
        );
    }
}
