use nom_sql::{BinaryOperator, Expression, InValue, ItemPlaceholder, Literal, SqlQuery};

use launchpad::or_else_result;
use noria::{unsupported, ReadySetResult};
use std::mem;

#[derive(PartialEq, Eq, Clone, Copy)]
enum RewriteLiterals {
    NoRewrite,
    Placeholders,
    BinaryExpression,
}

/// This function replaces the current `value IN (x, y, z, ..)` expression with
/// `value = x OR value = y OR value = z ..` and `value NOT IN (x, y, z, ..)` with
/// `value != x AND value != y AND value = !z ..`
fn where_in_to_or_equal(
    _leftmost_param_index: &mut usize,
    expr: &mut Expression,
) -> ReadySetResult<Option<(usize, Vec<Literal>)>> {
    let (lhs, list, negated) = match *expr {
        Expression::In {
            ref mut lhs,
            rhs: InValue::List(ref mut list),
            negated,
        } => (lhs, list, negated),
        _ => unreachable!("May only be called when expr is `In`"),
    };

    let mut list = mem::take(list); // Take the list to free the mutable reference
    if list.is_empty() {
        unsupported!("Spotted empty WHERE IN ()")
    }

    // We use a combination of `OR` and '=' for `WHERE IN`, and `AND` and '!=' for `NOT WHERE IN`
    let (op, inner_op) = if !negated {
        (BinaryOperator::Or, BinaryOperator::Equal)
    } else {
        (BinaryOperator::And, BinaryOperator::NotEqual)
    };

    let name = mem::replace(lhs, Box::new(Expression::Literal(Literal::Null)));
    let rhs = Box::new(list.remove(0));
    *expr = list.into_iter().fold(
        Expression::BinaryOp {
            lhs: name.clone(),
            rhs,
            op: inner_op,
        },
        |lhs, next| {
            let rhs = Box::new(Expression::BinaryOp {
                lhs: name.clone(),
                rhs: Box::new(next),
                op: inner_op,
            });
            let lhs = Box::new(lhs);
            Expression::BinaryOp { lhs, rhs, op }
        },
    );

    Ok(None) // List is always empty at this point
}

/// This function replaces the current `value IN (x, y, z, ..)` expression with
/// a parametrized query instead. I.e. (value = '?'), returning the literals in the list that were
/// actually replaced, so that the client can provide them as keys to the query
fn where_in_to_placeholders(
    leftmost_param_index: &mut usize,
    expr: &mut Expression,
) -> ReadySetResult<Option<(usize, Vec<Literal>)>> {
    let (lhs, list, negated) = match *expr {
        Expression::In {
            ref mut lhs,
            rhs: InValue::List(ref mut list),
            negated,
        } => (lhs, list, negated),
        _ => unreachable!("May only be called when expr is `In`"),
    };

    if list.is_empty() {
        unsupported!("Spotted empty WHERE IN ()");
    }
    let list_iter = std::mem::take(list).into_iter(); // Take the list to free the mutable reference
    let literals = list_iter
        .map(|e| match e {
            Expression::Literal(lit) => Ok(lit),
            _ => unsupported!("IN only supported on literals, got: {}", e),
        })
        .collect::<ReadySetResult<Vec<_>>>()?;

    let op = if negated {
        BinaryOperator::NotEqual
    } else {
        BinaryOperator::Equal
    };

    *expr = Expression::BinaryOp {
        lhs: mem::replace(lhs, Box::new(Expression::Literal(Literal::Null))),
        op,
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

    Ok(Some((*leftmost_param_index, literals)))
}

fn collapse_where_in_recursive(
    leftmost_param_index: &mut usize,
    expr: &mut Expression,
    mode: RewriteLiterals,
) -> ReadySetResult<Option<(usize, Vec<Literal>)>> {
    Ok(match *expr {
        Expression::Literal(Literal::Placeholder(_)) => {
            *leftmost_param_index += 1;
            None
        }
        Expression::NestedSelect(ref mut sq) => {
            if let Some(ref mut w) = sq.where_clause {
                collapse_where_in_recursive(leftmost_param_index, w, mode)?
            } else {
                None
            }
        }
        Expression::UnaryOp { ref mut rhs, .. } => {
            collapse_where_in_recursive(leftmost_param_index, rhs, mode)?
        }
        Expression::BinaryOp {
            ref mut lhs,
            ref mut rhs,
            ..
        } => {
            or_else_result(
                collapse_where_in_recursive(leftmost_param_index, lhs, mode)?,
                || {
                    // we can't also try rewriting ct.right, as it'd make it hard to recover
                    // literals: if we rewrote WHERE x IN (a, b) in left and WHERE y IN (1, 2) in
                    // right into WHERE x = ? ... y = ?, then what param values should we use?
                    // TODO(grfn): what does the above comment mean?
                    collapse_where_in_recursive(leftmost_param_index, rhs, mode)
                },
            )?
        }

        Expression::In {
            ref mut lhs,
            rhs: InValue::List(ref mut list),
            ..
        } => match mode {
            RewriteLiterals::BinaryExpression => where_in_to_or_equal(leftmost_param_index, expr)?,
            RewriteLiterals::Placeholders => where_in_to_placeholders(leftmost_param_index, expr)?,
            RewriteLiterals::NoRewrite
                if list
                    .iter()
                    .all(|l| matches!(l, Expression::Literal(Literal::Placeholder(_)))) =>
            {
                // If the list contains only placeholders, flatten them anyway
                where_in_to_placeholders(leftmost_param_index, expr)?
            }
            RewriteLiterals::NoRewrite => {
                collapse_where_in_recursive(leftmost_param_index, lhs, mode)?.or_else(|| {
                    *leftmost_param_index += list
                        .iter()
                        .filter(|&l| matches!(l, Expression::Literal(Literal::Placeholder(_))))
                        .count();
                    None
                })
            }
        },
        Expression::In {
            ref mut lhs,
            rhs: InValue::Subquery(ref mut sq),
            negated: false,
        } => or_else_result(
            collapse_where_in_recursive(leftmost_param_index, lhs, mode)?,
            || {
                if let Some(ref mut w) = sq.where_clause {
                    collapse_where_in_recursive(leftmost_param_index, w, mode)
                } else {
                    Ok(None)
                }
            },
        )?,
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
            collapse_where_in_recursive(leftmost_param_index, &mut *operand, mode)?,
            || {
                or_else_result(
                    collapse_where_in_recursive(leftmost_param_index, &mut *min, mode)?,
                    || collapse_where_in_recursive(leftmost_param_index, &mut *max, mode),
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
        // When a `SELECT` statement contains aggregates, such as `SUM` or `COUNT`, we can't use placeholders,
        // as those will aggregate keys lookups into a multi row response, as opposed to a single row response required
        // by aggregates
        let has_aggregates = sq.contains_aggregate_select();

        if let Some(ref mut w) = sq.where_clause {
            let mut left_edge = 0;

            let mode = match (rewrite_literals, has_aggregates) {
                (false, _) => RewriteLiterals::NoRewrite,
                (true, false) => RewriteLiterals::Placeholders,
                (true, true) => RewriteLiterals::BinaryExpression,
            };

            return collapse_where_in_recursive(&mut left_edge, w, mode);
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

        let mut q = nom_sql::parse_query("SELECT COUNT(*) FROM x WHERE id IN (1, 2, 3)").unwrap();
        assert!(collapse_where_in(&mut q, true).unwrap().is_none());
        assert_eq!(
            q,
            nom_sql::parse_query("SELECT COUNT(*) FROM x WHERE (id=1 OR id=2) OR id=3").unwrap()
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
