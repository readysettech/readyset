use itertools::{Either, Itertools};
use nom_sql::{BinaryOperator, Expression, InValue, ItemPlaceholder, Literal, SqlQuery};
use noria::{unsupported, ReadySetResult};
use std::{cmp::max, iter, mem};

/// Information about a single parametrized IN condition that has been rewritten to an equality
/// condition
#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) struct RewrittenIn {
    /// The index in the parameters of the query of the first rewritten parameter for this condition
    first_param_index: usize,

    /// The list of placeholders in the IN list itself
    literals: Vec<ItemPlaceholder>,
}

/// This function replaces the current `value IN (?, ?, ?, ..)` expression with
/// a parametrized point query, eg (value = '?')
fn where_in_to_placeholders(
    leftmost_param_index: &mut usize,
    expr: &mut Expression,
) -> ReadySetResult<RewrittenIn> {
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
            Expression::Literal(Literal::Placeholder(ph)) => Ok(ph),
            _ => unsupported!("IN only supported on placeholders, got: {}", e),
        })
        .collect::<ReadySetResult<Vec<_>>>()?;

    let first_param_index = *leftmost_param_index;
    *leftmost_param_index += literals.len();

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

    Ok(RewrittenIn {
        first_param_index,
        literals,
    })
}

fn collapse_where_in_recursive(
    leftmost_param_index: &mut usize,
    expr: &mut Expression,
    out: &mut Vec<RewrittenIn>,
) -> ReadySetResult<()> {
    match *expr {
        Expression::Literal(Literal::Placeholder(_)) => {
            *leftmost_param_index += 1;
        }
        Expression::NestedSelect(ref mut sq) => {
            if let Some(ref mut w) = sq.where_clause {
                collapse_where_in_recursive(leftmost_param_index, w, out)?;
            }
        }
        Expression::UnaryOp {
            rhs: ref mut expr, ..
        }
        | Expression::Cast { ref mut expr, .. } => {
            collapse_where_in_recursive(leftmost_param_index, expr, out)?;
        }
        Expression::BinaryOp {
            ref mut lhs,
            ref mut rhs,
            ..
        } => {
            collapse_where_in_recursive(leftmost_param_index, lhs, out)?;
            collapse_where_in_recursive(leftmost_param_index, rhs, out)?;
        }

        Expression::In {
            rhs: InValue::List(ref mut list),
            ..
        } => {
            if list
                .iter()
                .any(|l| matches!(l, Expression::Literal(Literal::Placeholder(_))))
            {
                // If the list contains placeholders, flatten them. `where_in_to_placeholders` takes
                // care of erroring-out if the list contains any *non*-placeholders
                out.push(where_in_to_placeholders(leftmost_param_index, expr)?);
            }
        }
        Expression::In {
            ref mut lhs,
            rhs: InValue::Subquery(ref mut sq),
            negated: false,
        } => {
            collapse_where_in_recursive(leftmost_param_index, lhs, out)?;
            if let Some(ref mut w) = sq.where_clause {
                collapse_where_in_recursive(leftmost_param_index, w, out)?;
            }
        }
        Expression::In { negated: true, .. } => unsupported!("NOT IN not supported yet"),
        ref x @ Expression::Exists(_) => unsupported!("EXISTS not supported yet: {}", x),
        Expression::Between {
            ref mut operand,
            ref mut min,
            ref mut max,
            ..
        } => {
            collapse_where_in_recursive(leftmost_param_index, &mut *operand, out)?;
            collapse_where_in_recursive(leftmost_param_index, &mut *min, out)?;
            collapse_where_in_recursive(leftmost_param_index, &mut *max, out)?;
        }
        Expression::Column(_) | Expression::Literal(_) => {}
        Expression::Call(_) | Expression::CaseWhen { .. } => {
            unsupported!("Unsupported condition: {}", expr);
        }
    }

    Ok(())
}

/// Convert all instances of *parametrized* IN (`x IN (?, ?, ...)`) in the given `query` to a direct
/// equality comparison (`x = ?`), returning a vector of [`RewrittenIn`] giving information about
/// the rewritten in params.
///
/// Given that vector and the params provided by a user, [`explode_params`] can be used to construct
/// a vector of lookup keys for executing that query.
///
/// Note that IN conditions without any placeholders will be left untouched, as these can be handled
/// by regular filter nodes in dataflow
pub(crate) fn collapse_where_in(query: &mut SqlQuery) -> ReadySetResult<Vec<RewrittenIn>> {
    let mut res = vec![];
    if let SqlQuery::Select(ref mut sq) = *query {
        let has_aggregates = sq.contains_aggregate_select();

        if let Some(ref mut w) = sq.where_clause {
            let mut left_edge = 0;
            collapse_where_in_recursive(&mut left_edge, w, &mut res)?;

            // When a `SELECT` statement contains aggregates, such as `SUM` or `COUNT`, we can't use
            // placeholders, as those will aggregate key lookups into a multi row response, as
            // opposed to a single row response required by aggregates. We could support this pretty
            // easily, but for now it's not in-scope
            if !res.is_empty() && has_aggregates {
                unsupported!("Aggregates with parametrized IN are not supported");
            }
        }
    }
    Ok(res)
}

/// Given a vector of parameters provided by the user and the list of [`RewrittenIn`] returned by
/// [`collapse_where_in`] on a query, construct a vector of lookup keys for executing that query
pub(crate) fn explode_params<'a, T>(
    params: Vec<T>,
    rewritten_in_conditions: &'a [RewrittenIn],
) -> impl Iterator<Item = ReadySetResult<Vec<T>>> + 'a
where
    T: Clone + 'a,
{
    if rewritten_in_conditions.is_empty() {
        if params.is_empty() {
            return Either::Left(iter::empty());
        } else {
            return Either::Right(Either::Left(iter::once(Ok(params))));
        };
    }

    Either::Right(Either::Right(
        rewritten_in_conditions
            .iter()
            .map(
                |RewrittenIn {
                     first_param_index,
                     literals,
                 }| {
                    (0..literals.len())
                        .map(move |in_idx| (*first_param_index, in_idx, literals.len()))
                },
            )
            .multi_cartesian_product()
            .map(move |mut ins| {
                ins.sort_by_key(|(first_param_index, _, _)| *first_param_index);
                let mut res = vec![];
                let mut taken = 0;
                for (first_param_index, in_idx, in_len) in ins {
                    res.extend(
                        params
                            .iter()
                            .skip(taken)
                            .take(first_param_index - taken)
                            .cloned(),
                    );
                    res.push(params[first_param_index + in_idx].clone());
                    taken = max(taken, first_param_index + in_len);
                }
                res.extend(params.iter().skip(taken).cloned());
                Ok(res)
            }),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use nom_sql::Dialect;

    #[test]
    fn collapsed_where_placeholders() {
        let mut q =
            nom_sql::parse_query(Dialect::MySQL, "SELECT * FROM x WHERE x.y IN (?, ?, ?)").unwrap();
        let rewritten = collapse_where_in(&mut q).unwrap();
        assert_eq!(
            rewritten,
            vec![RewrittenIn {
                first_param_index: 0,
                literals: vec![ItemPlaceholder::QuestionMark; 3]
            }]
        );
        assert_eq!(
            q,
            nom_sql::parse_query(Dialect::MySQL, "SELECT * FROM x WHERE x.y = ?").unwrap()
        );

        let mut q =
            nom_sql::parse_query(Dialect::MySQL, "SELECT * FROM x WHERE y IN (?, ?, ?)").unwrap();
        let rewritten = collapse_where_in(&mut q).unwrap();
        assert_eq!(
            rewritten,
            vec![RewrittenIn {
                first_param_index: 0,
                literals: vec![ItemPlaceholder::QuestionMark; 3]
            }]
        );
        assert_eq!(
            q,
            nom_sql::parse_query(Dialect::MySQL, "SELECT * FROM x WHERE y = ?").unwrap()
        );

        let mut q =
            nom_sql::parse_query(Dialect::MySQL, "SELECT * FROM x WHERE AVG(y) IN (?, ?, ?)")
                .unwrap();
        let rewritten = collapse_where_in(&mut q).unwrap();
        assert_eq!(
            rewritten,
            vec![RewrittenIn {
                first_param_index: 0,
                literals: vec![ItemPlaceholder::QuestionMark; 3]
            }]
        );
        assert_eq!(
            q,
            nom_sql::parse_query(Dialect::MySQL, "SELECT * FROM x WHERE AVG(y) = ?").unwrap()
        );

        let mut q = nom_sql::parse_query(
            Dialect::MySQL,
            "SELECT * FROM t WHERE x = ? AND y IN (?, ?, ?) OR z = ?",
        )
        .unwrap();
        let rewritten = collapse_where_in(&mut q).unwrap();
        assert_eq!(
            rewritten,
            vec![RewrittenIn {
                first_param_index: 1,
                literals: vec![ItemPlaceholder::QuestionMark; 3]
            }]
        );
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
        let rewritten = collapse_where_in(&mut q).unwrap();
        assert_eq!(
            rewritten,
            vec![RewrittenIn {
                first_param_index: 1,
                literals: vec![ItemPlaceholder::QuestionMark; 2]
            }]
        );
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
        let rewritten = collapse_where_in(&mut q).unwrap();
        assert_eq!(
            rewritten,
            vec![RewrittenIn {
                first_param_index: 1,
                literals: vec![ItemPlaceholder::QuestionMark; 2]
            }]
        );
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
        assert_eq!(collapse_where_in(&mut q).unwrap(), vec![]);
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
        let rewritten = collapse_where_in(&mut q).unwrap();
        assert_eq!(
            rewritten,
            vec![RewrittenIn {
                first_param_index: 0,
                literals: vec![
                    ItemPlaceholder::DollarNumber(1),
                    ItemPlaceholder::DollarNumber(2),
                    ItemPlaceholder::DollarNumber(3),
                ]
            }]
        );
        assert_eq!(
            q,
            nom_sql::parse_query(Dialect::MySQL, "SELECT * FROM x WHERE x.y = ?").unwrap()
        );

        let mut q = nom_sql::parse_query(Dialect::MySQL, "SELECT * FROM x WHERE y IN ($1, $2, $3)")
            .unwrap();
        let rewritten = collapse_where_in(&mut q).unwrap();
        assert_eq!(
            rewritten,
            vec![RewrittenIn {
                first_param_index: 0,
                literals: vec![
                    ItemPlaceholder::DollarNumber(1),
                    ItemPlaceholder::DollarNumber(2),
                    ItemPlaceholder::DollarNumber(3),
                ]
            }]
        );
        assert_eq!(
            q,
            nom_sql::parse_query(Dialect::MySQL, "SELECT * FROM x WHERE y = ?").unwrap()
        );

        let mut q = nom_sql::parse_query(
            Dialect::MySQL,
            "SELECT * FROM x WHERE AVG(y) IN ($1, $2, $3)",
        )
        .unwrap();
        let rewritten = collapse_where_in(&mut q).unwrap();
        assert_eq!(
            rewritten,
            vec![RewrittenIn {
                first_param_index: 0,
                literals: vec![
                    ItemPlaceholder::DollarNumber(1),
                    ItemPlaceholder::DollarNumber(2),
                    ItemPlaceholder::DollarNumber(3),
                ]
            }]
        );
        assert_eq!(
            q,
            nom_sql::parse_query(Dialect::MySQL, "SELECT * FROM x WHERE AVG(y) = ?").unwrap()
        );

        let mut q = nom_sql::parse_query(
            Dialect::MySQL,
            "SELECT * FROM t WHERE x = $1 AND y IN ($2, $3, $4) OR z = $5",
        )
        .unwrap();
        let rewritten = collapse_where_in(&mut q).unwrap();
        assert_eq!(
            rewritten,
            vec![RewrittenIn {
                first_param_index: 1,
                literals: vec![
                    ItemPlaceholder::DollarNumber(2),
                    ItemPlaceholder::DollarNumber(3),
                    ItemPlaceholder::DollarNumber(4),
                ]
            }]
        );
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
        let rewritten = collapse_where_in(&mut q).unwrap();
        assert_eq!(
            rewritten,
            vec![RewrittenIn {
                first_param_index: 1,
                literals: vec![
                    ItemPlaceholder::DollarNumber(2),
                    ItemPlaceholder::DollarNumber(3),
                ]
            }]
        );
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
        let rewritten = collapse_where_in(&mut q).unwrap();
        assert_eq!(
            rewritten,
            vec![RewrittenIn {
                first_param_index: 1,
                literals: vec![
                    ItemPlaceholder::DollarNumber(2),
                    ItemPlaceholder::DollarNumber(3),
                ]
            }]
        );
        assert_eq!(
            q,
            nom_sql::parse_query(
                Dialect::MySQL,
                "SELECT * FROM t WHERE x IN (SELECT * FROM z WHERE b = $1 AND a = ?) OR z = $4",
            )
            .unwrap()
        );
    }

    #[test]
    fn collapse_multiple_where_in() {
        let mut q = nom_sql::parse_query(
            Dialect::MySQL,
            "SELECT * FROM t WHERE x IN (?,?) AND y IN (?,?)",
        )
        .unwrap();
        let rewritten = collapse_where_in(&mut q).unwrap();
        assert_eq!(
            rewritten,
            vec![
                RewrittenIn {
                    first_param_index: 0,
                    literals: vec![ItemPlaceholder::QuestionMark; 2]
                },
                RewrittenIn {
                    first_param_index: 2,
                    literals: vec![ItemPlaceholder::QuestionMark; 2]
                }
            ]
        );
        assert_eq!(
            q,
            nom_sql::parse_query(Dialect::MySQL, "SELECT * FROM t WHERE x = ? AND y = ?").unwrap()
        );
    }

    mod explode_params {
        use super::*;

        #[test]
        fn no_in() {
            let params = vec![1u32, 2, 3];
            let res = explode_params(params, &[])
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert_eq!(res, vec![vec![1, 2, 3]]);
        }

        #[test]
        fn single_in() {
            // SELECT * FROM t WHERE x = ? AND y IN (?, ?) AND z = ?
            // ->
            // SELECT * FROM t WHERE x = ? AND y = ? AND z = ?
            let rewritten_in_conditions = vec![RewrittenIn {
                first_param_index: 1,
                literals: vec![ItemPlaceholder::QuestionMark; 2],
            }];
            let params = vec![1u32, 2, 3, 4];
            let res = explode_params(params, &rewritten_in_conditions)
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert_eq!(res, vec![vec![1, 2, 4], vec![1, 3, 4]]);
        }

        #[test]
        fn multiple_in() {
            // SELECT * FROM t WHERE x = ? AND y IN (?, ?) AND z = ? AND w IN (?, ?) AND q = ?
            // ->
            // SELECT * FROM t WHERE x = ? AND y = ? AND z = ? AND w = ? AND q = ?
            let rewritten_in_conditions = vec![
                RewrittenIn {
                    first_param_index: 1,
                    literals: vec![ItemPlaceholder::QuestionMark; 2],
                },
                RewrittenIn {
                    first_param_index: 4,
                    literals: vec![ItemPlaceholder::QuestionMark; 2],
                },
            ];
            let params = vec![1u32, 2, 3, 4, 5, 6, 7];
            let res = explode_params(params, &rewritten_in_conditions)
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert_eq!(
                res,
                vec![
                    vec![1, 2, 4, 5, 7],
                    vec![1, 2, 4, 6, 7],
                    vec![1, 3, 4, 5, 7],
                    vec![1, 3, 4, 6, 7]
                ]
            );
        }
    }
}
