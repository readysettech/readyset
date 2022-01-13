use itertools::{Either, Itertools};
use nom_sql::{
    analysis::visit::{self, Visitor},
    BinaryOperator, Expression, InValue, ItemPlaceholder, Literal, SelectStatement,
};
use noria_errors::{unsupported, ReadySetError, ReadySetResult};
use std::{
    borrow::Cow,
    cmp::max,
    convert::{TryFrom, TryInto},
    iter, mem,
};

/// Struct storing information about parameters processed from a raw user supplied query, which
/// provides support for converting a user-supplied parameter list into a set of lookup keys to pass
/// to Noria.
///
/// Construct a [`ProcessedQueryParams`] by calling [`process_query`], then pass the list of
/// user-provided parameters to [`ProcessedQueryParams::make_keys`] to make a list of lookup keys to
/// pass to noria.
#[derive(Debug, Clone)]
pub(crate) struct ProcessedQueryParams {
    rewritten_in_conditions: Vec<RewrittenIn>,
    auto_parameters: Vec<(usize, Literal)>,
}

pub(crate) fn process_query(query: &mut SelectStatement) -> ReadySetResult<ProcessedQueryParams> {
    let auto_parameters = auto_parametrize_query(query);
    let rewritten_in_conditions = collapse_where_in(query)?;
    number_placeholders(query)?;
    Ok(ProcessedQueryParams {
        rewritten_in_conditions,
        auto_parameters,
    })
}

impl ProcessedQueryParams {
    pub(crate) fn make_keys<'param, T>(
        &self,
        params: &'param [T],
    ) -> ReadySetResult<Vec<Cow<'param, [T]>>>
    where
        T: Clone + TryFrom<Literal, Error = ReadySetError> + std::fmt::Debug,
    {
        if params.is_empty() && self.auto_parameters.is_empty() {
            return Ok(vec![]);
        }

        let auto_parameters = self
            .auto_parameters
            .clone()
            .into_iter()
            .map(|(i, lit)| -> ReadySetResult<_> { Ok((i, lit.try_into()?)) })
            .collect::<Result<Vec<_>, _>>()?;

        let params = splice_auto_parameters(params, &auto_parameters);

        if self.rewritten_in_conditions.is_empty() {
            return Ok(vec![params]);
        }

        Ok(
            explode_params(params.as_ref(), &self.rewritten_in_conditions)
                .map(|k| Cow::Owned(k.into_owned()))
                .collect(),
        )
    }
}

/// Information about a single parametrized IN condition that has been rewritten to an equality
/// condition
#[derive(Debug, PartialEq, Eq, Clone)]
struct RewrittenIn {
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
        rhs: Box::new(Expression::Literal(Literal::Placeholder(
            ItemPlaceholder::QuestionMark,
        ))),
    };

    Ok(RewrittenIn {
        first_param_index,
        literals,
    })
}

#[derive(Default)]
struct CollapseWhereInVisitor {
    leftmost_param_index: usize,
    out: Vec<RewrittenIn>,
}

impl<'ast> Visitor<'ast> for CollapseWhereInVisitor {
    type Error = ReadySetError;

    fn visit_literal(&mut self, literal: &'ast mut Literal) -> Result<(), Self::Error> {
        if matches!(literal, Literal::Placeholder(_)) {
            self.leftmost_param_index += 1;
        }
        Ok(())
    }

    fn visit_expression(&mut self, expression: &'ast mut Expression) -> Result<(), Self::Error> {
        if let Expression::In {
            rhs: InValue::List(list),
            ..
        } = expression
        {
            if list
                .iter()
                .any(|l| matches!(l, Expression::Literal(Literal::Placeholder(_))))
            {
                // If the list contains placeholders, flatten them. `where_in_to_placeholders` takes
                // care of erroring-out if the list contains any *non*-placeholders
                self.out.push(where_in_to_placeholders(
                    &mut self.leftmost_param_index,
                    expression,
                )?);
                return Ok(());
            }
        }

        visit::walk_expression(self, expression)
    }
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
fn collapse_where_in(query: &mut SelectStatement) -> ReadySetResult<Vec<RewrittenIn>> {
    let mut res = vec![];
    let distinct = query.distinct;
    let has_aggregates = query.contains_aggregate_select();

    if let Some(ref mut w) = query.where_clause {
        let mut visitor = CollapseWhereInVisitor::default();
        visitor.visit_expression(w)?;
        res = visitor.out;

        // When a `SELECT` statement contains aggregates, such as `SUM` or `COUNT` (or `DISTINCT`,
        // which is implemented via COUNT),  we can't use placeholders, as those will aggregate key
        // lookups into a multi row response, as opposed to a single row response required by
        // aggregates. We could support this pretty easily, but for now it's not in-scope
        if !res.is_empty() {
            if has_aggregates {
                unsupported!("Aggregates with parametrized IN are not supported");
            }
            if distinct {
                unsupported!("DISTINCT with parametrized IN is not supported");
            }
        }
    }
    Ok(res)
}

/// Given a slice of parameters provided by the user and the list of [`RewrittenIn`] returned by
/// [`collapse_where_in`] on a query, construct a vector of lookup keys for executing that query
fn explode_params<'param, 'a, T>(
    params: &'param [T],
    rewritten_in_conditions: &'a [RewrittenIn],
) -> impl Iterator<Item = Cow<'param, [T]>> + 'a
where
    T: Clone + 'a,
    'param: 'a,
{
    if rewritten_in_conditions.is_empty() {
        if params.is_empty() {
            return Either::Left(iter::empty());
        } else {
            return Either::Right(Either::Left(iter::once(Cow::Borrowed(params))));
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
                Cow::Owned(res)
            }),
    ))
}

struct NumberPlaceholdersVisitor {
    next_param_number: u32,
    offset: u32,
}

impl<'ast> Visitor<'ast> for NumberPlaceholdersVisitor {
    type Error = ReadySetError;
    fn visit_literal(&mut self, literal: &'ast mut Literal) -> Result<(), Self::Error> {
        if let Literal::Placeholder(item) = literal {
            // client-provided queries aren't allowed to mix question-mark and dollar-number
            // placeholders, but both autoparametrization and collapse-where-in create question mark
            // placeholders, which in the intermediate state does end up with a query that has both
            // placeholder styles - we need to make sure we number those question mark placeholders
            // appropriately to not overlap with existing dollar-number placeholders.
            match item {
                ItemPlaceholder::QuestionMark => {
                    // If we find a dollar-number placeholder, update our index to start numbering
                    // parameters after that placeholder
                    *item = ItemPlaceholder::DollarNumber(self.next_param_number);
                    self.next_param_number += 1;
                    self.offset += 1;
                }
                ItemPlaceholder::DollarNumber(n) => {
                    *n += self.offset;
                    self.next_param_number = *n + 1
                }
                ItemPlaceholder::ColonNumber(_) => {
                    unsupported!("colon-number placeholders aren't supported")
                }
            }
        }
        Ok(())
    }
}

pub fn number_placeholders(query: &mut SelectStatement) -> ReadySetResult<()> {
    let mut visitor = NumberPlaceholdersVisitor {
        next_param_number: 1,
        offset: 0,
    };
    visitor.visit_select_statement(query)?;
    Ok(())
}

struct AnonymizeLiteralsVisitor;
impl<'ast> Visitor<'ast> for AnonymizeLiteralsVisitor {
    type Error = !;
    fn visit_literal(&mut self, literal: &'ast mut Literal) -> Result<(), Self::Error> {
        *literal = Literal::String("<anonymized>".to_owned());
        Ok(())
    }
}

pub fn anonymize_literals(query: &mut SelectStatement) {
    #[allow(clippy::unwrap_used)] // error is !, which can never be returned
    AnonymizeLiteralsVisitor
        .visit_select_statement(query)
        .unwrap();
}

#[derive(Default)]
struct AutoParametrizeVisitor {
    out: Vec<(usize, Literal)>,
    has_aggregates: bool,
    in_supported_position: bool,
    param_index: usize,
    query_depth: u8,
}

impl<'ast> Visitor<'ast> for AutoParametrizeVisitor {
    type Error = !;

    fn visit_literal(&mut self, literal: &'ast mut Literal) -> Result<(), Self::Error> {
        if matches!(literal, Literal::Placeholder(_)) {
            self.param_index += 1;
        }
        Ok(())
    }

    fn visit_select_statement(
        &mut self,
        select_statement: &'ast mut SelectStatement,
    ) -> Result<(), Self::Error> {
        self.query_depth = self.query_depth.saturating_add(1);
        visit::walk_select_statement(self, select_statement)?;
        self.query_depth = self.query_depth.saturating_sub(1);
        Ok(())
    }

    fn visit_where_clause(&mut self, expression: &'ast mut Expression) -> Result<(), Self::Error> {
        // We can only support parameters in the WHERE clause of the top-level query, not any
        // subqueries it contains.
        self.in_supported_position = self.query_depth <= 1;
        self.visit_expression(expression)?;
        self.in_supported_position = false;
        Ok(())
    }

    fn visit_expression(&mut self, expression: &'ast mut Expression) -> Result<(), Self::Error> {
        let was_supported = self.in_supported_position;
        if was_supported {
            match expression {
                Expression::BinaryOp {
                    lhs: box Expression::Column(_),
                    op: BinaryOperator::Equal,
                    rhs: box Expression::Literal(Literal::Placeholder(_)),
                } => {}
                Expression::BinaryOp {
                    lhs: box Expression::Column(_),
                    op: BinaryOperator::Equal,
                    rhs: box Expression::Literal(lit),
                } => {
                    let literal =
                        mem::replace(lit, Literal::Placeholder(ItemPlaceholder::QuestionMark));
                    self.out.push((self.param_index, literal));
                    self.param_index += 1;
                    return Ok(());
                }
                Expression::BinaryOp {
                    lhs: lhs @ box Expression::Literal(_),
                    op: BinaryOperator::Equal,
                    rhs: rhs @ box Expression::Column(_),
                } => {
                    // for lit = col, swap the equality first then revisit
                    mem::swap(lhs, rhs);
                    return self.visit_expression(expression);
                }
                Expression::In {
                    lhs: box Expression::Column(_),
                    rhs: InValue::List(exprs),
                    negated: false,
                } if exprs.iter().all(|e| {
                    matches!(
                        e,
                        Expression::Literal(lit) if !matches!(lit, Literal::Placeholder(_))
                    )
                }) && !self.has_aggregates =>
                {
                    let exprs = mem::replace(
                        exprs,
                        iter::repeat(Expression::Literal(Literal::Placeholder(
                            ItemPlaceholder::QuestionMark,
                        )))
                        .take(exprs.len())
                        .collect(),
                    );
                    let num_exprs = exprs.len();
                    let start_index = self.param_index;
                    self.out
                        .extend(exprs.into_iter().enumerate().filter_map(
                            move |(i, expr)| match expr {
                                Expression::Literal(lit) => Some((i + start_index, lit)),
                                // unreachable since we checked everything in the list is a literal
                                // above, but best not to panic regardless
                                _ => None,
                            },
                        ));
                    self.param_index += num_exprs;
                    return Ok(());
                }
                Expression::BinaryOp {
                    lhs,
                    op: BinaryOperator::And,
                    rhs,
                } => {
                    self.visit_expression(lhs.as_mut())?;
                    self.in_supported_position = true;
                    self.visit_expression(rhs.as_mut())?;
                    self.in_supported_position = true;
                    return Ok(());
                }
                _ => self.in_supported_position = false,
            }
        }

        visit::walk_expression(self, expression)?;
        self.in_supported_position = was_supported;
        Ok(())
    }
}

/// Replace all literals that are in positions we support parameters in the given query with
/// parameters, and return the values for those parameters alongside the index in the parameter list
/// where they appear as a tuple of (placeholder position, value).
fn auto_parametrize_query(query: &mut SelectStatement) -> Vec<(usize, Literal)> {
    // Don't try to auto-parametrize equal-queries that already contain range params for now, since
    // we don't yet allow mixing range and equal parameters in the same query
    if query.where_clause.iter().any(|expr| {
        iter::once(expr)
            .chain(expr.recursive_subexpressions())
            .any(|subexpr| {
                matches!(
                    subexpr,
                    Expression::BinaryOp {
                        op: BinaryOperator::Less
                            | BinaryOperator::Greater
                            | BinaryOperator::LessOrEqual
                            | BinaryOperator::GreaterOrEqual,
                        rhs: box Expression::Literal(Literal::Placeholder(..)),
                        ..
                    }
                )
            })
    }) {
        return vec![];
    }

    let mut visitor = AutoParametrizeVisitor {
        has_aggregates: query.contains_aggregate_select(),
        ..Default::default()
    };
    #[allow(clippy::unwrap_used)] // error is !, which can never be returned
    visitor.visit_select_statement(query).unwrap();
    visitor.out
}

/// Splice the given list of extracted parameters, which should be a tuple of (placeholder position,
/// value) as returned by [`auto_parametrize_query`] into the given list of parameters supplied by
/// the user, by interleaving them into the params based on the placeholder position.
///
/// # Invariants
///
/// `extracted_auto_params` must be sorted by the first index (this is the case with the return
/// value of [`auto_parametrize_query`]).
fn splice_auto_parameters<'param, 'a, T: Clone>(
    mut params: &'param [T],
    extracted_auto_params: &'a [(usize, T)],
) -> Cow<'param, [T]> {
    if extracted_auto_params.is_empty() {
        return Cow::Borrowed(params);
    }

    debug_assert!(extracted_auto_params.is_sorted_by_key(|(i, _)| *i));
    let mut res = Vec::with_capacity(params.len() + extracted_auto_params.len());
    for (idx, extracted) in extracted_auto_params.iter().cloned() {
        let split = params.split_at(idx.saturating_sub(res.len()));
        res.extend(split.0.to_vec());
        res.push(extracted);
        params = split.1;
    }
    res.extend(params.to_vec());
    Cow::Owned(res)
}

#[cfg(test)]
mod tests {
    use super::*;
    use nom_sql::{parse_query, Dialect, SqlQuery};

    fn parse_select_statement(q: &str) -> SelectStatement {
        let q = parse_query(Dialect::MySQL, q).unwrap();
        match q {
            SqlQuery::Select(stmt) => stmt,
            _ => panic!(),
        }
    }

    mod collapse_where {
        use super::*;

        #[test]
        fn collapsed_where_placeholders() {
            let mut q = parse_select_statement("SELECT * FROM x WHERE x.y IN (?, ?, ?)");
            let rewritten = collapse_where_in(&mut q).unwrap();
            assert_eq!(
                rewritten,
                vec![RewrittenIn {
                    first_param_index: 0,
                    literals: vec![ItemPlaceholder::QuestionMark; 3]
                }]
            );
            assert_eq!(q, parse_select_statement("SELECT * FROM x WHERE x.y = ?"));

            let mut q = parse_select_statement("SELECT * FROM x WHERE y IN (?, ?, ?)");
            let rewritten = collapse_where_in(&mut q).unwrap();
            assert_eq!(
                rewritten,
                vec![RewrittenIn {
                    first_param_index: 0,
                    literals: vec![ItemPlaceholder::QuestionMark; 3]
                }]
            );
            assert_eq!(q, parse_select_statement("SELECT * FROM x WHERE y = ?"));

            let mut q = parse_select_statement("SELECT * FROM x WHERE AVG(y) IN (?, ?, ?)");
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
                parse_select_statement("SELECT * FROM x WHERE AVG(y) = ?")
            );

            let mut q =
                parse_select_statement("SELECT * FROM t WHERE x = ? AND y IN (?, ?, ?) OR z = ?");
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
                parse_select_statement("SELECT * FROM t WHERE x = ? AND y = ? OR z = ?")
            );

            let mut q = parse_select_statement(
                "SELECT * FROM t WHERE x IN (SELECT * FROM z WHERE a = ?) AND y IN (?, ?) OR z = ?",
            );
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
                parse_select_statement(
                    "SELECT * FROM t WHERE x IN (SELECT * FROM z WHERE a = ?) AND y = ? OR z = ?"
                )
            );

            let mut q = parse_select_statement(
                "SELECT * FROM t WHERE x IN (SELECT * FROM z WHERE b = ? AND a IN (?, ?)) OR z = ?",
            );
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
                parse_select_statement(
                    "SELECT * FROM t WHERE x IN (SELECT * FROM z WHERE b = ? AND a = ?) OR z = ?",
                )
            );
        }

        #[test]
        fn collapsed_where_literals() {
            let mut q = parse_select_statement("SELECT * FROM x WHERE x.y IN (1, 2, 3)");
            assert_eq!(collapse_where_in(&mut q).unwrap(), vec![]);
            assert_eq!(
                q,
                parse_select_statement("SELECT * FROM x WHERE x.y IN (1, 2, 3)")
            );
        }

        #[test]
        fn collapsed_where_dollarsign_placeholders() {
            let mut q = parse_select_statement("SELECT * FROM x WHERE x.y IN ($1, $2, $3)");
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
            assert_eq!(q, parse_select_statement("SELECT * FROM x WHERE x.y = ?"));

            let mut q = parse_select_statement("SELECT * FROM x WHERE y IN ($1, $2, $3)");
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
            assert_eq!(q, parse_select_statement("SELECT * FROM x WHERE y = ?"));

            let mut q = parse_select_statement("SELECT * FROM x WHERE AVG(y) IN ($1, $2, $3)");
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
                parse_select_statement("SELECT * FROM x WHERE AVG(y) = ?")
            );

            let mut q = parse_select_statement(
                "SELECT * FROM t WHERE x = $1 AND y IN ($2, $3, $4) OR z = $5",
            );
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
                parse_select_statement("SELECT * FROM t WHERE x = $1 AND y = ? OR z = $5")
            );

            let mut q = parse_select_statement(
            "SELECT * FROM t WHERE x IN (SELECT * FROM z WHERE a = $1) AND y IN ($2, $3) OR z = $4",
        );
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
                parse_select_statement(
                    "SELECT * FROM t WHERE x IN (SELECT * FROM z WHERE a = $1) AND y = ? OR z = $4"
                )
            );

            let mut q = parse_select_statement(
            "SELECT * FROM t WHERE x IN (SELECT * FROM z WHERE b = $1 AND a IN ($2, $3)) OR z = $4",
        );
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
                parse_select_statement(
                    "SELECT * FROM t WHERE x IN (SELECT * FROM z WHERE b = $1 AND a = ?) OR z = $4",
                )
            );
        }

        #[test]
        fn collapse_multiple_where_in() {
            let mut q = parse_select_statement("SELECT * FROM t WHERE x IN (?,?) AND y IN (?,?)");
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
                parse_select_statement("SELECT * FROM t WHERE x = ? AND y = ?")
            );
        }
    }

    mod explode_params {
        use super::*;

        #[test]
        fn no_in() {
            let params = vec![1u32, 2, 3];
            let res = explode_params(&params, &[]).collect::<Vec<_>>();
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
            let res = explode_params(&params, &rewritten_in_conditions).collect::<Vec<_>>();
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
            let res = explode_params(&params, &rewritten_in_conditions).collect::<Vec<_>>();
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

    mod anonymize {
        use super::*;

        #[test]
        fn simple_query() {
            let mut query = parse_select_statement(
                "SELECT id + 3 FROM users WHERE credit_card_number = \"look at this PII\"",
            );
            let expected = parse_select_statement(
                "SELECT id + \"<anonymized>\" FROM users WHERE credit_card_number = \"<anonymized>\""
            );
            anonymize_literals(&mut query);
            assert_eq!(query, expected);
        }
    }

    mod parametrize {
        use super::*;

        fn test_auto_parametrize(
            query: &str,
            expected_query: &str,
            expected_parameters: Vec<(usize, Literal)>,
        ) {
            let mut query = parse_select_statement(query);
            let expected = parse_select_statement(expected_query);
            let res = auto_parametrize_query(&mut query);
            assert_eq!(query, expected, "\n  left: {}\n right: {}", query, expected);
            assert_eq!(res, expected_parameters);
        }

        #[test]
        fn no_literals() {
            test_auto_parametrize("SELECT * FROM users", "SELECT * FROM users", vec![]);
        }

        #[test]
        fn simple_parameter() {
            test_auto_parametrize(
                "SELECT id FROM users WHERE id = 1",
                "SELECT id FROM users WHERE id = ?",
                vec![(0, 1.into())],
            );
        }

        #[test]
        fn and_parameters() {
            test_auto_parametrize(
                "SELECT id FROM users WHERE id = 1 AND name = \"bob\"",
                "SELECT id FROM users WHERE id = ? AND name = ?",
                vec![(0, 1.into()), (1, "bob".into())],
            );
        }

        #[test]
        fn existing_param_before() {
            test_auto_parametrize(
                "SELECT id FROM users WHERE x = ? AND id = 1 AND name = \"bob\"",
                "SELECT id FROM users WHERE x = ? AND id = ? AND name = ?",
                vec![(1, 1.into()), (2, "bob".into())],
            );
        }

        #[test]
        fn existing_param_after() {
            test_auto_parametrize(
                "SELECT id FROM users WHERE id = 1 AND name = \"bob\" AND x = ?",
                "SELECT id FROM users WHERE id = ? AND name = ? AND x = ?",
                vec![(0, 1.into()), (1, "bob".into())],
            );
        }

        #[test]
        fn existing_param_between() {
            test_auto_parametrize(
                "SELECT id FROM users WHERE id = 1 AND x = ? AND name = \"bob\"",
                "SELECT id FROM users WHERE id = ? AND x = ? AND name = ?",
                vec![(0, 1.into()), (2, "bob".into())],
            );
        }

        #[test]
        fn literal_in_or() {
            test_auto_parametrize(
                "SELECT id FROM users WHERE (id = 1 OR id = 2) AND name = \"bob\"",
                "SELECT id FROM users WHERE (id = 1 OR id = 2) AND name = ?",
                vec![(0, "bob".into())],
            )
        }

        #[test]
        fn literal_in_subquery_where() {
            test_auto_parametrize(
                "SELECT id FROM users JOIN (SELECT id FROM users WHERE id = 1) s ON users.id = s.id WHERE id = 1",
                "SELECT id FROM users JOIN (SELECT id FROM users WHERE id = 1) s ON users.id = s.id WHERE id = ?",
                vec![(0, 1.into())],
            )
        }

        #[test]
        fn literal_in_field() {
            test_auto_parametrize(
                "SELECT id + 1 FROM users WHERE id = 1",
                "SELECT id + 1 FROM users WHERE id = ?",
                vec![(0, 1.into())],
            )
        }

        #[test]
        fn literal_in_in_rhs() {
            test_auto_parametrize(
                "select hashtags.*, from hashtags inner join invites_hashtags on hashtags.id = invites_hashtags.hashtag_id where invites_hashtags.invite_id in (10,20,31)",
                "select hashtags.*, from hashtags inner join invites_hashtags on hashtags.id = invites_hashtags.hashtag_id where invites_hashtags.invite_id in (?,?,?)",
                    vec![(0, 10.into()), (1, 20.into()), (2, 31.into())],
            );
        }

        #[test]
        fn mixed_in_with_equality() {
            test_auto_parametrize(
                "SELECT id FROM users WHERE id in (1, 2) AND name = 'bob'",
                "SELECT id FROM users WHERE id in (?, ?) AND name = ?",
                vec![(0, 1.into()), (1, 2.into()), (2, "bob".into())],
            );
        }

        #[test]
        fn equal_in_equal() {
            test_auto_parametrize(
                "SELECT id FROM users WHERE x = 'foo' AND id in (1, 2) AND name = 'bob'",
                "SELECT id FROM users WHERE x = ? AND id in (?, ?) AND name = ?",
                vec![
                    (0, "foo".into()),
                    (1, 1.into()),
                    (2, 2.into()),
                    (3, "bob".into()),
                ],
            );
        }

        #[test]
        fn in_with_aggregates() {
            test_auto_parametrize(
                "SELECT count(*) FROM users WHERE id = 1 AND x IN (1, 2)",
                "SELECT count(*) FROM users WHERE id = ? AND x IN (1, 2)",
                vec![(0, 1.into())],
            );
        }

        #[test]
        fn literal_equals_column() {
            test_auto_parametrize(
                "SELECT * FROM users WHERE 1 = id",
                "SELECT * FROM users WHERE id = ?",
                vec![(0, 1.into())],
            );
        }

        #[test]
        fn existing_range_param() {
            test_auto_parametrize(
                "SELECT * FROM posts WHERE id = 1 AND score > ?",
                "SELECT * FROM posts WHERE id = 1 AND score > ?",
                vec![],
            )
        }
    }

    mod splice_auto_parameters {
        use super::*;

        #[test]
        fn single_param() {
            let params = vec![];
            let extracted = vec![(0, 0)];
            let res = splice_auto_parameters(&params, &extracted);
            assert_eq!(res, vec![0]);
        }

        #[test]
        fn consecutive_params() {
            let params = vec![];
            let extracted = vec![(0, 0), (1, 1)];
            let res = splice_auto_parameters(&params, &extracted);
            assert_eq!(res, vec![0, 1]);
        }

        #[test]
        fn params_before() {
            let params = vec![0, 1];
            let extracted = vec![(2, 2), (3, 3)];
            let res = splice_auto_parameters(&params, &extracted);
            assert_eq!(res, vec![0, 1, 2, 3]);
        }

        #[test]
        fn params_after() {
            let params = vec![2, 3];
            let extracted = vec![(0, 0), (1, 1)];
            let res = splice_auto_parameters(&params, &extracted);
            assert_eq!(res, vec![0, 1, 2, 3]);
        }

        #[test]
        fn params_between() {
            let params = vec![1, 2];
            let extracted = vec![(0, 0), (3, 3)];
            let res = splice_auto_parameters(&params, &extracted);
            assert_eq!(res, vec![0, 1, 2, 3]);
        }

        #[test]
        fn params_before_between_and_after() {
            let params = vec![0, 2];
            let extracted = vec![(1, 1), (3, 3)];
            let res = splice_auto_parameters(&params, &extracted);
            assert_eq!(res, vec![0, 1, 2, 3]);
        }
    }

    mod process_query {
        use noria_data::DataType;

        use super::*;

        fn process_and_make_keys(
            query: &str,
            params: Vec<DataType>,
        ) -> (Vec<Vec<DataType>>, SelectStatement) {
            let mut query = parse_select_statement(query);
            let processed = process_query(&mut query).unwrap();
            (
                processed
                    .make_keys(&params)
                    .unwrap()
                    .into_iter()
                    .map(|c| c.to_vec())
                    .collect(),
                query,
            )
        }

        #[test]
        fn no_keys() {
            let (keys, query) = process_and_make_keys("SELECT * FROM test", vec![]);
            assert_eq!(query, parse_select_statement("SELECT * FROM test"));
            assert!(keys.is_empty(), "keys = {:?}", keys);
        }

        #[test]
        fn numbered_auto_params() {
            let (keys, query) = process_and_make_keys("SELECT x, y FROM test WHERE x = 4", vec![]);

            assert_eq!(
                query,
                parse_select_statement("SELECT x, y FROM test WHERE x = $1")
            );

            assert_eq!(keys, vec![vec![4.into()]]);
        }

        #[test]
        fn number_autoparam_number() {
            let (keys, query) = process_and_make_keys(
                "SELECT x, y FROM test WHERE x = $1 AND y = 2 AND z = $2",
                vec![1.into(), 3.into()],
            );

            assert_eq!(
                query,
                parse_select_statement("SELECT x, y FROM test WHERE x = $1 AND y = $2 AND z = $3")
            );

            assert_eq!(keys, vec![vec![1.into(), 2.into(), 3.into()]]);
        }

        #[test]
        fn numbered_where_in_with_auto_params() {
            let (keys, query) = process_and_make_keys(
                "SELECT * FROM users WHERE x = ? AND y in (?, ?, ?) AND z = 4 AND w = 5 AND q = ?",
                vec![0.into(), 1.into(), 2.into(), 3.into(), 6.into()],
            );

            assert_eq!(
                query,
                parse_select_statement(
                    "SELECT * FROM users WHERE x = $1 AND y = $2 AND z = $3 AND w = $4 AND q = $5",
                )
            );

            assert_eq!(
                keys,
                vec![
                    vec![0.into(), 1.into(), 4.into(), 5.into(), 6.into()],
                    vec![0.into(), 2.into(), 4.into(), 5.into(), 6.into()],
                    vec![0.into(), 3.into(), 4.into(), 5.into(), 6.into()],
                ]
            );
        }

        #[test]
        fn numbered_auto_parametrized_in() {
            let (keys, query) = process_and_make_keys(
                "SELECT * FROM users WHERE x = 1 AND y IN (1, 2, 3) AND z = ?",
                vec![1.into()],
            );

            assert_eq!(
                query,
                parse_select_statement("SELECT * FROM users WHERE x = $1 AND y = $2 AND z = $3")
            );

            assert_eq!(
                keys,
                vec![
                    vec![1.into(), 1.into(), 1.into()],
                    vec![1.into(), 2.into(), 1.into()],
                    vec![1.into(), 3.into(), 1.into()],
                ]
            );
        }

        #[test]
        fn numbered_where_in_with_equal() {
            let (keys, query) = process_and_make_keys(
                "SELECT Cats.name FROM Cats WHERE Cats.name = $1 AND Cats.id IN ($2, $3)",
                vec!["Bob".into(), 1.into(), 2.into()],
            );

            assert_eq!(
                query,
                parse_select_statement(
                    "SELECT Cats.name FROM Cats WHERE Cats.name = $1 AND Cats.id = $2"
                )
            );

            assert_eq!(
                keys,
                vec![vec!["Bob".into(), 1.into()], vec!["Bob".into(), 2.into()]]
            );
        }
    }
}
