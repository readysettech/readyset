use nom_sql::{BinaryOperator, Expression, InValue, ItemPlaceholder, Literal, SqlQuery};

use launchpad::or_else_result;
use noria::{unsupported, ReadySetResult};
use std::mem;

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
) -> ReadySetResult<Option<(usize, Vec<Literal>)>> {
    Ok(match *expr {
        Expression::Literal(Literal::Placeholder(_)) => {
            *leftmost_param_index += 1;
            None
        }
        Expression::NestedSelect(ref mut sq) => {
            if let Some(ref mut w) = sq.where_clause {
                collapse_where_in_recursive(leftmost_param_index, w)?
            } else {
                None
            }
        }
        Expression::UnaryOp {
            rhs: ref mut expr, ..
        }
        | Expression::Cast { ref mut expr, .. } => {
            collapse_where_in_recursive(leftmost_param_index, expr)?
        }
        Expression::BinaryOp {
            ref mut lhs,
            ref mut rhs,
            ..
        } => {
            or_else_result(
                collapse_where_in_recursive(leftmost_param_index, lhs)?,
                || {
                    // we can't also try rewriting ct.right, as it'd make it hard to recover
                    // literals: if we rewrote WHERE x IN (a, b) in left and WHERE y IN (1, 2) in
                    // right into WHERE x = ? ... y = ?, then what param values should we use?
                    // TODO(grfn): what does the above comment mean?
                    collapse_where_in_recursive(leftmost_param_index, rhs)
                },
            )?
        }

        Expression::In {
            ref mut lhs,
            rhs: InValue::List(ref mut list),
            ..
        } => {
            if list
                .iter()
                .all(|l| matches!(l, Expression::Literal(Literal::Placeholder(_))))
            {
                // If the list contains only placeholders, flatten them anyway
                where_in_to_placeholders(leftmost_param_index, expr)?
            } else {
                collapse_where_in_recursive(leftmost_param_index, lhs)?.or_else(|| {
                    *leftmost_param_index += list
                        .iter()
                        .filter(|&l| matches!(l, Expression::Literal(Literal::Placeholder(_))))
                        .count();
                    None
                })
            }
        }
        Expression::In {
            ref mut lhs,
            rhs: InValue::Subquery(ref mut sq),
            negated: false,
        } => or_else_result(
            collapse_where_in_recursive(leftmost_param_index, lhs)?,
            || {
                if let Some(ref mut w) = sq.where_clause {
                    collapse_where_in_recursive(leftmost_param_index, w)
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
            collapse_where_in_recursive(leftmost_param_index, &mut *operand)?,
            || {
                or_else_result(
                    collapse_where_in_recursive(leftmost_param_index, &mut *min)?,
                    || collapse_where_in_recursive(leftmost_param_index, &mut *max),
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
) -> ReadySetResult<Option<(usize, Vec<Literal>)>> {
    if let SqlQuery::Select(ref mut sq) = *query {
        if let Some(ref mut w) = sq.where_clause {
            let mut left_edge = 0;
            return collapse_where_in_recursive(&mut left_edge, w);
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use nom_sql::Dialect;

    #[test]
    fn collapsed_where_placeholders() {
        let mut q =
            nom_sql::parse_query(Dialect::MySQL, "SELECT * FROM x WHERE x.y IN (?, ?, ?)").unwrap();
        let rewritten = collapse_where_in(&mut q).unwrap().unwrap();
        assert_eq!(rewritten.0, 0);
        assert_eq!(rewritten.1.len(), 3);
        assert_eq!(
            q,
            nom_sql::parse_query(Dialect::MySQL, "SELECT * FROM x WHERE x.y = ?").unwrap()
        );

        let mut q =
            nom_sql::parse_query(Dialect::MySQL, "SELECT * FROM x WHERE y IN (?, ?, ?)").unwrap();
        let rewritten = collapse_where_in(&mut q).unwrap().unwrap();
        assert_eq!(rewritten.0, 0);
        assert_eq!(rewritten.1.len(), 3);
        assert_eq!(
            q,
            nom_sql::parse_query(Dialect::MySQL, "SELECT * FROM x WHERE y = ?").unwrap()
        );

        let mut q =
            nom_sql::parse_query(Dialect::MySQL, "SELECT * FROM x WHERE AVG(y) IN (?, ?, ?)")
                .unwrap();
        let rewritten = collapse_where_in(&mut q).unwrap().unwrap();
        assert_eq!(rewritten.0, 0);
        assert_eq!(rewritten.1.len(), 3);
        assert_eq!(
            q,
            nom_sql::parse_query(Dialect::MySQL, "SELECT * FROM x WHERE AVG(y) = ?").unwrap()
        );

        let mut q = nom_sql::parse_query(
            Dialect::MySQL,
            "SELECT * FROM t WHERE x = ? AND y IN (?, ?, ?) OR z = ?",
        )
        .unwrap();
        let rewritten = collapse_where_in(&mut q).unwrap().unwrap();
        assert_eq!(rewritten.0, 1);
        assert_eq!(rewritten.1.len(), 3);
        assert_eq!(
            q,
            nom_sql::parse_query(
                Dialect::MySQL,
                "SELECT * FROM t WHERE x = ? AND y = ? OR z = ?"
            )
            .unwrap()
        );

        let mut q = nom_sql::parse_query(
            Dialect::MySQL,
            "SELECT * FROM t WHERE x IN (SELECT * FROM z WHERE a = ?) AND y IN (?, ?) OR z = ?",
        )
        .unwrap();
        let rewritten = collapse_where_in(&mut q).unwrap().unwrap();
        assert_eq!(rewritten.0, 1);
        assert_eq!(rewritten.1.len(), 2);
        assert_eq!(
            q,
            nom_sql::parse_query(
                Dialect::MySQL,
                "SELECT * FROM t WHERE x IN (SELECT * FROM z WHERE a = ?) AND y = ? OR z = ?"
            )
            .unwrap()
        );

        let mut q = nom_sql::parse_query(
            Dialect::MySQL,
            "SELECT * FROM t WHERE x IN (SELECT * FROM z WHERE b = ? AND a IN (?, ?)) OR z = ?",
        )
        .unwrap();
        let rewritten = collapse_where_in(&mut q).unwrap().unwrap();
        assert_eq!(rewritten.0, 1);
        assert_eq!(rewritten.1.len(), 2);
        assert_eq!(
            q,
            nom_sql::parse_query(
                Dialect::MySQL,
                "SELECT * FROM t WHERE x IN (SELECT * FROM z WHERE b = ? AND a = ?) OR z = ?",
            )
            .unwrap()
        );
    }

    #[test]
    fn collapsed_where_literals() {
        let mut q =
            nom_sql::parse_query(Dialect::MySQL, "SELECT * FROM x WHERE x.y IN (1, 2, 3)").unwrap();
        assert_eq!(collapse_where_in(&mut q).unwrap(), None);
        assert_eq!(
            q,
            nom_sql::parse_query(Dialect::MySQL, "SELECT * FROM x WHERE x.y IN (1, 2, 3)").unwrap()
        );
    }

    #[test]
    fn collapsed_where_dollarsign_placeholders() {
        let mut q =
            nom_sql::parse_query(Dialect::MySQL, "SELECT * FROM x WHERE x.y IN ($1, $2, $3)")
                .unwrap();
        let rewritten = collapse_where_in(&mut q).unwrap().unwrap();
        assert_eq!(rewritten.0, 0);
        assert_eq!(rewritten.1.len(), 3);
        assert_eq!(
            q,
            nom_sql::parse_query(Dialect::MySQL, "SELECT * FROM x WHERE x.y = ?").unwrap()
        );

        let mut q = nom_sql::parse_query(Dialect::MySQL, "SELECT * FROM x WHERE y IN ($1, $2, $3)")
            .unwrap();
        let rewritten = collapse_where_in(&mut q).unwrap().unwrap();
        assert_eq!(rewritten.0, 0);
        assert_eq!(rewritten.1.len(), 3);
        assert_eq!(
            q,
            nom_sql::parse_query(Dialect::MySQL, "SELECT * FROM x WHERE y = ?").unwrap()
        );

        let mut q = nom_sql::parse_query(
            Dialect::MySQL,
            "SELECT * FROM x WHERE AVG(y) IN ($1, $2, $3)",
        )
        .unwrap();
        let rewritten = collapse_where_in(&mut q).unwrap().unwrap();
        assert_eq!(rewritten.0, 0);
        assert_eq!(rewritten.1.len(), 3);
        assert_eq!(
            q,
            nom_sql::parse_query(Dialect::MySQL, "SELECT * FROM x WHERE AVG(y) = ?").unwrap()
        );

        let mut q = nom_sql::parse_query(
            Dialect::MySQL,
            "SELECT * FROM t WHERE x = $1 AND y IN ($2, $3, $4) OR z = $5",
        )
        .unwrap();
        let rewritten = collapse_where_in(&mut q).unwrap().unwrap();
        assert_eq!(rewritten.0, 1);
        assert_eq!(rewritten.1.len(), 3);
        assert_eq!(
            q,
            nom_sql::parse_query(
                Dialect::MySQL,
                "SELECT * FROM t WHERE x = $1 AND y = ? OR z = $5"
            )
            .unwrap()
        );

        let mut q = nom_sql::parse_query(
            Dialect::MySQL,
            "SELECT * FROM t WHERE x IN (SELECT * FROM z WHERE a = $1) AND y IN ($2, $3) OR z = $4",
        )
        .unwrap();
        let rewritten = collapse_where_in(&mut q).unwrap().unwrap();
        assert_eq!(rewritten.0, 1);
        assert_eq!(rewritten.1.len(), 2);
        assert_eq!(
            q,
            nom_sql::parse_query(
                Dialect::MySQL,
                "SELECT * FROM t WHERE x IN (SELECT * FROM z WHERE a = $1) AND y = ? OR z = $4"
            )
            .unwrap()
        );

        let mut q = nom_sql::parse_query(
            Dialect::MySQL,
            "SELECT * FROM t WHERE x IN (SELECT * FROM z WHERE b = $1 AND a IN ($2, $3)) OR z = $4",
        )
        .unwrap();
        let rewritten = collapse_where_in(&mut q).unwrap().unwrap();
        assert_eq!(rewritten.0, 1);
        assert_eq!(rewritten.1.len(), 2);
        assert_eq!(
            q,
            nom_sql::parse_query(
                Dialect::MySQL,
                "SELECT * FROM t WHERE x IN (SELECT * FROM z WHERE b = $1 AND a = ?) OR z = $4",
            )
            .unwrap()
        );
    }
}
