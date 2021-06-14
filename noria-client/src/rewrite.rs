use nom_sql::{BinaryOperator, Expression, InValue, ItemPlaceholder, Literal, SqlQuery};

use launchpad::or_else_result;
use noria::{unsupported, ReadySetResult};
use std::mem;

fn collapse_where_in_recursive(
    leftmost_param_index: &mut usize,
    expr: &mut Expression,
    rewrite_literals: bool,
) -> ReadySetResult<Option<(usize, Vec<Literal>)>> {
    Ok(match *expr {
        Expression::Literal(Literal::Placeholder(_)) => {
            *leftmost_param_index += 1;
            None
        }
        Expression::NestedSelect(ref mut sq) => {
            if let Some(ref mut w) = sq.where_clause {
                collapse_where_in_recursive(leftmost_param_index, w, rewrite_literals)?
            } else {
                None
            }
        }
        Expression::UnaryOp { ref mut rhs, .. } => {
            collapse_where_in_recursive(leftmost_param_index, rhs, rewrite_literals)?
        }
        Expression::BinaryOp {
            ref mut lhs,
            ref mut rhs,
            ..
        } => {
            or_else_result(
                collapse_where_in_recursive(leftmost_param_index, lhs, rewrite_literals)?,
                || {
                    // we can't also try rewriting ct.right, as it'd make it hard to recover
                    // literals: if we rewrote WHERE x IN (a, b) in left and WHERE y IN (1, 2) in
                    // right into WHERE x = ? ... y = ?, then what param values should we use?
                    // TODO(grfn): what does the above comment mean?
                    collapse_where_in_recursive(leftmost_param_index, rhs, rewrite_literals)
                },
            )?
        }
        Expression::In {
            ref mut lhs,
            ref mut rhs,
            negated: false,
        } => {
            let mut do_it = false;
            let literals = if let InValue::List(list) = rhs {
                if rewrite_literals
                    || (list
                        .iter()
                        .all(|l| matches!(l, Expression::Literal(Literal::Placeholder(_)))))
                {
                    do_it = true;
                    mem::replace(list, Vec::new())
                        .into_iter()
                        .map(|expr| -> ReadySetResult<_> {
                            match expr {
                                Expression::Literal(lit) => Ok(lit),
                                _ => unsupported!("IN only supported on literals, got: {}", expr),
                            }
                        })
                        .collect::<Result<Vec<_>, _>>()?
                } else {
                    Vec::new()
                }
            } else {
                Vec::new()
            };

            if !do_it {
                return or_else_result(
                    collapse_where_in_recursive(leftmost_param_index, lhs, rewrite_literals)?,
                    || match rhs {
                        InValue::Subquery(sq) => {
                            if let Some(ref mut w) = sq.where_clause {
                                collapse_where_in_recursive(
                                    leftmost_param_index,
                                    w,
                                    rewrite_literals,
                                )
                            } else {
                                Ok(None)
                            }
                        }
                        InValue::List(exprs) => {
                            *leftmost_param_index += exprs
                                .iter()
                                .filter(|&l| {
                                    matches!(l, Expression::Literal(Literal::Placeholder(_)))
                                })
                                .count();
                            Ok(None)
                        }
                    },
                );
            }

            if literals.is_empty() {
                // TODO(eta): probably shouldn't be unsupported; was eprintln before
                unsupported!("spotted empty WHERE IN ()");
            }

            *expr = Expression::BinaryOp {
                lhs: mem::replace(lhs, Box::new(Expression::Literal(Literal::Null))),
                op: BinaryOperator::Equal,
                // NOTE: Replacing the right side with ItemPlaceholder::QuestionMark may result in the
                // modified query containing placeholders of mixed types (i.e. with some placeholders
                // of type QuestionMark while others are of type DollarNumber). This will work ok for
                // now because all placeholder types are treated as equivalent by noria and
                // noria-client. In addition, standardizing the placeholder type here may help reduce
                // the impact of certain query reuse bugs.
                rhs: Box::new(Expression::Literal(Literal::Placeholder(
                    ItemPlaceholder::QuestionMark,
                ))),
            };

            Some((*leftmost_param_index, literals))
        }
        Expression::In { negated: true, .. } => unsupported!("NOT IN not supported yet"),
        ref x @ Expression::Exists(_) => {
            unsupported!("EXISTS not supported yet: {}", x)
        }
        Expression::Between {
            ref mut operand,
            ref mut min,
            ref mut max,
            ..
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
        Expression::Column(_) | Expression::Literal(_) => None,
        Expression::Call(_) | Expression::CaseWhen { .. } => {
            unsupported!("Unsupported condition: {}", expr)
        }
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
            nom_sql::parse_query("SELECT * FROM x WHERE x.y = ?").unwrap()
        );

        let mut q = nom_sql::parse_query("SELECT * FROM x WHERE y IN ($1, $2, $3)").unwrap();
        let rewritten = collapse_where_in(&mut q, false).unwrap().unwrap();
        assert_eq!(rewritten.0, 0);
        assert_eq!(rewritten.1.len(), 3);
        assert_eq!(
            q,
            nom_sql::parse_query("SELECT * FROM x WHERE y = ?").unwrap()
        );

        let mut q = nom_sql::parse_query("SELECT * FROM x WHERE AVG(y) IN ($1, $2, $3)").unwrap();
        let rewritten = collapse_where_in(&mut q, false).unwrap().unwrap();
        assert_eq!(rewritten.0, 0);
        assert_eq!(rewritten.1.len(), 3);
        assert_eq!(
            q,
            nom_sql::parse_query("SELECT * FROM x WHERE AVG(y) = ?").unwrap()
        );

        let mut q =
            nom_sql::parse_query("SELECT * FROM t WHERE x = $1 AND y IN ($2, $3, $4) OR z = $5")
                .unwrap();
        let rewritten = collapse_where_in(&mut q, false).unwrap().unwrap();
        assert_eq!(rewritten.0, 1);
        assert_eq!(rewritten.1.len(), 3);
        assert_eq!(
            q,
            nom_sql::parse_query("SELECT * FROM t WHERE x = $1 AND y = ? OR z = $5").unwrap()
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
                "SELECT * FROM t WHERE x IN (SELECT * FROM z WHERE a = $1) AND y = ? OR z = $4"
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
                "SELECT * FROM t WHERE x IN (SELECT * FROM z WHERE b = $1 AND a = ?) OR z = $4",
            )
            .unwrap()
        );
    }
}
