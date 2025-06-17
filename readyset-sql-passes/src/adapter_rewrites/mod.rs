mod autoparameterize;

use std::borrow::Cow;
use std::convert::{TryFrom, TryInto};
use std::fmt::Debug;
use std::{iter, mem};

pub use autoparameterize::auto_parameterize_query;
use itertools::{repeat_n, Either, Itertools};
use readyset_data::{DfType, DfValue};
use readyset_errors::{
    internal_err, invalid_query_err, unsupported, ReadySetError, ReadySetResult,
};
use readyset_sql::analysis::visit_mut::{self, VisitorMut};
use readyset_sql::ast::{
    BinaryOperator, Expr, InValue, ItemPlaceholder, LimitClause, Literal, OrderClause,
    SelectMetadata, SelectStatement,
};
use readyset_sql::DialectDisplay;
use serde::{Deserialize, Serialize};
use tracing::trace;

/// Struct storing information about parameters processed from a raw user supplied query, which
/// provides support for converting a user-supplied parameter list into a set of lookup keys to pass
/// to ReadySet.
///
/// Construct a [`ProcessedQueryParams`] by calling [`process_query`], then pass the list of
/// user-provided parameters to [`ProcessedQueryParams::make_keys`] to make a list of lookup keys to
/// pass to noria.
#[derive(Debug, Clone)]
pub struct ProcessedQueryParams {
    reordered_placeholders: Option<Vec<usize>>,
    rewritten_in_conditions: Vec<RewrittenIn>,
    auto_parameters: Vec<(usize, Literal)>,
    pagination_parameters: AdapterPaginationParams,
}

#[derive(Debug, Clone)]
struct AdapterPaginationParams {
    /// The values of `LIMIT` and `OFFSET` in the original query
    limit_clause: LimitClause,
    force_paginate_in_adapter: bool,
}

/// This method checks if readyset-server is configured to handle LIMIT/OFFSET queries at the
/// dataflow level. If not then LIMIT and OFFSET will be stripped and executed in the
/// post-processing path.
fn use_fallback_pagination(
    server_supports_pagination: bool,
    server_supports_topk: bool,
    limit_clause: &LimitClause,
    order: &Option<OrderClause>,
) -> bool {
    // Check that the query contains a topk and that topk is enabled.
    // If topk is enabled, and the limit is a placeholder, then we need to fallback to the
    // adapter because topk doesn't support LIMIT placeholders (aka k can't be parameterized)
    // else, topk will handle the LIMIT and the ORDER as expected, and no fallback is needed.
    if server_supports_topk && order.is_some() && limit_clause.is_topk() {
        return matches!(limit_clause.limit(), Some(Literal::Placeholder(_)));
    }

    if server_supports_pagination &&
        // Can't handle bare OFFSET
        !(limit_clause.limit().is_none() && limit_clause.offset().is_some())
    {
        return false;
    }

    true
}

/// Parameters to be passed to [`process_query`].
#[derive(Debug, Copy, Clone, Serialize, Deserialize, Default)]
pub struct AdapterRewriteParams {
    /// The server can create TopK nodes, efficient lookup for non-parameterized
    /// limit and order by. Parameterized limits are done by the reader and therefore
    /// doesn't require TopK
    pub server_supports_topk: bool,
    /// The server can handle (non-parameterized) LIMITs and (parameterized) OFFSETs in the
    /// dataflow graph
    pub server_supports_pagination: bool,
    /// The server allows both equals and range comparisons to be parameterized in a query. If this
    /// flag is true, both equals and range parameters in supported positions will be
    /// autoparameterized during the adapter rewrite passes.
    pub server_supports_mixed_comparisons: bool,
}

/// This rewrite pass accomplishes the following:
/// - Remaps dollar sign placeholders so that they appear in order
/// - Replaces literals with placeholders when they can be used as lookup indices in the noria
///   dataflow representation of the query. Note that this pass may not replace all literals and is
///   therefore cannot guarantee that the rewritten query is free of user PII.
/// - Collapses 'WHERE <expr> IN ?, ... ?' to 'WHERE <expr> = ?'
/// - Removes `OFFSET ?` if there isn't a `LIMIT`
pub fn process_query(
    query: &mut SelectStatement,
    params: AdapterRewriteParams,
) -> ReadySetResult<ProcessedQueryParams> {
    let reordered_placeholders = reorder_numbered_placeholders(query);

    let force_paginate_in_adapter = use_fallback_pagination(
        params.server_supports_pagination,
        params.server_supports_topk,
        &query.limit_clause,
        &query.order,
    );

    let limit_clause = if force_paginate_in_adapter {
        trace!("Will use fallback LIMIT/OFFSET for query");
        mem::take(&mut query.limit_clause)
    } else {
        query.limit_clause.clone()
    };

    let auto_parameters =
        autoparameterize::auto_parameterize_query(query, params.server_supports_mixed_comparisons);
    let rewritten_in_conditions = collapse_where_in(query)?;
    number_placeholders(query)?;
    Ok(ProcessedQueryParams {
        reordered_placeholders,
        rewritten_in_conditions,
        auto_parameters,
        pagination_parameters: AdapterPaginationParams {
            limit_clause,
            force_paginate_in_adapter,
        },
    })
}

impl ProcessedQueryParams {
    /// If the query has values for OFFSET or LIMIT, get their values, returning a tuple of `limit,
    /// offset`
    pub fn limit_offset_params(
        &self,
        params: &[DfValue],
    ) -> ReadySetResult<(Option<usize>, Option<usize>)> {
        let mut params_iter = self
            .reordered_placeholders
            .as_ref()
            .map(|p| Either::Left(p.iter().rev().filter_map(|i| params.get(*i))))
            .unwrap_or_else(|| Either::Right(params.iter().rev()))
            .into_iter();

        let mut get_param = |lit: &Literal| -> ReadySetResult<usize> {
            match lit {
                Literal::Placeholder(_) => match params_iter
                    .next()
                    .ok_or_else(|| invalid_query_err!("Wrong number of parameters"))
                    .and_then(|v| v.coerce_to(&DfType::UnsignedBigInt, &DfType::Unknown))?
                {
                    DfValue::UnsignedInt(v) => Ok(v as usize),
                    _ => unreachable!("Successfully coerced"),
                },
                Literal::Integer(v) => usize::try_from(*v)
                    .map_err(|_| invalid_query_err!("Non negative integer expected")),
                Literal::UnsignedInteger(v) => Ok(*v as usize),
                Literal::Null => Ok(0), // Invalid in MySQL but 0 for Postgres
                Literal::Float(_)
                | Literal::Double(_)
                | Literal::String(_)
                | Literal::Numeric(_, _) => {
                    // All of those are invalid in MySQL, but Postgres coerces to integer
                    match DfValue::try_from(lit)?
                        .coerce_to(&DfType::UnsignedBigInt, &DfType::Unknown)?
                    {
                        DfValue::UnsignedInt(v) => Ok(v as usize),
                        _ => unreachable!("Successfully coerced"),
                    }
                }
                _ => Err(invalid_query_err!("Non negative integer expected")),
            }
        };

        let AdapterPaginationParams {
            limit_clause,
            force_paginate_in_adapter,
        } = &self.pagination_parameters;

        let (limit, offset) = match limit_clause {
            LimitClause::LimitOffset { offset, .. } => {
                let offset = offset.as_ref().map(&mut get_param).transpose()?;
                let limit = limit_clause.limit().map(&mut get_param).transpose()?;
                (limit, offset)
            }
            LimitClause::OffsetCommaLimit { offset, .. } => {
                // Get the limit first, since with this syntax, it's the last param.
                let limit = limit_clause.limit().map(&mut get_param).transpose()?;
                let offset = get_param(offset)?;
                (limit, Some(offset))
            }
        };

        if *force_paginate_in_adapter || limit == Some(0) {
            Ok((limit, offset))
        } else {
            Ok((None, None))
        }
    }

    pub fn make_keys<'param, T>(&self, params: &'param [T]) -> ReadySetResult<Vec<Cow<'param, [T]>>>
    where
        T: Clone + TryFrom<Literal, Error = ReadySetError> + Debug + Default + PartialEq,
    {
        let params = if let Some(order_map) = &self.reordered_placeholders {
            Cow::Owned(reorder_params(params, order_map)?)
        } else {
            Cow::Borrowed(params)
        };

        let mut params = params.as_ref();

        let AdapterPaginationParams {
            limit_clause,
            force_paginate_in_adapter,
        } = &self.pagination_parameters;

        if *force_paginate_in_adapter {
            // When fallback pagination is used, remove the parameters for offset and limit from the
            // list
            if matches!(limit_clause.offset(), Some(Literal::Placeholder(_))) {
                // Skip parameter for offset
                params = &params[..params.len() - 1];
            }
            if matches!(limit_clause.limit(), Some(Literal::Placeholder(_))) {
                // Skip parameter for limit
                params = &params[..params.len() - 1];
            }
        }

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
            return Ok(vec![Cow::Owned(params.into_owned())]);
        }

        Ok(
            explode_params(params.as_ref(), &self.rewritten_in_conditions)
                .map(|k| Cow::Owned(k.into_owned()))
                .collect(),
        )
    }
}

/// Information about a single parameterized IN condition that has been rewritten to an equality
/// condition
#[derive(Debug, PartialEq, Eq, Clone)]
struct RewrittenIn {
    /// The index in the parameters of the query of the first rewritten parameter for this
    /// condition
    first_param_index: usize,

    /// The list of placeholders in the IN list itself
    literals: Vec<ItemPlaceholder>,

    /// If these literals are grouped together in a tuple/row (i.e. `IN ((?, ?))`)
    /// and therefore chunks of size `group_size` should be treated as a single parameter
    group_size: usize,
}

/// This function replaces the current `value IN (?, ?, ?, ..)` expression with
/// a parameterized point query, eg (value = '?')
fn where_in_to_placeholders(
    leftmost_param_index: &mut usize,
    expr: &mut Expr,
) -> ReadySetResult<RewrittenIn> {
    let (lhs, list, negated) = match *expr {
        Expr::In {
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
        .map(|e| -> ReadySetResult<Vec<_>> {
            match e {
                Expr::Literal(Literal::Placeholder(ph)) => Ok(vec![ph]),
                Expr::Row { exprs, .. } => exprs
                    .into_iter()
                    .map(|e| match e {
                        Expr::Literal(Literal::Placeholder(ph)) => Ok(ph),
                        _ => unsupported!("Expected a ROW of placeholders"),
                    })
                    .collect(),
                _ => unsupported!(
                    "IN only supported on placeholders, got: {}",
                    e.display(readyset_sql::Dialect::MySQL)
                ),
            }
        })
        .collect::<ReadySetResult<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();

    let first_param_index = *leftmost_param_index;
    *leftmost_param_index += literals.len();

    let op = if negated {
        BinaryOperator::NotEqual
    } else {
        BinaryOperator::Equal
    };

    let rhs = match **lhs {
        Expr::Row { ref exprs, .. } => Box::new(Expr::Row {
            explicit: false,
            exprs: repeat_n(
                Expr::Literal(Literal::Placeholder(ItemPlaceholder::QuestionMark)),
                exprs.len(),
            )
            .collect(),
        }),
        _ => Box::new(Expr::Literal(Literal::Placeholder(
            ItemPlaceholder::QuestionMark,
        ))),
    };

    let group_size = match *rhs {
        Expr::Row { ref exprs, .. } => exprs.len(),
        _ => 1,
    };

    *expr = Expr::BinaryOp {
        lhs: Box::new(lhs.take()),
        op,
        rhs,
    };

    Ok(RewrittenIn {
        first_param_index,
        literals,
        group_size,
    })
}

#[derive(Default)]
/// Please refer to the documentation of [`collapse_where_in`]
struct CollapseWhereInVisitor {
    leftmost_param_index: usize,
    out: Vec<RewrittenIn>,
}

impl<'ast> VisitorMut<'ast> for CollapseWhereInVisitor {
    type Error = ReadySetError;

    fn visit_select_statement(
        &mut self,
        select_statement: &'ast mut SelectStatement,
    ) -> Result<(), Self::Error> {
        visit_mut::walk_select_statement(self, select_statement)?;
        if !self.out.is_empty() {
            select_statement
                .metadata
                .push(SelectMetadata::CollapsedWhereIn)
        }
        Ok(())
    }

    fn visit_literal(&mut self, literal: &'ast mut Literal) -> Result<(), Self::Error> {
        if matches!(literal, Literal::Placeholder(_)) {
            self.leftmost_param_index += 1;
        }
        Ok(())
    }

    fn visit_expr(&mut self, expression: &'ast mut Expr) -> Result<(), Self::Error> {
        if let Expr::In {
            rhs: InValue::List(list),
            lhs,
            ..
        } = expression
        {
            if let Expr::Row { exprs: ref l, .. } = **lhs {
                if list.iter().any(|expr| match expr {
                    Expr::Row { exprs: r, .. } if l.len() != r.len() => true,
                    Expr::Row { .. } => false,
                    _ => true,
                }) {
                    return Err(ReadySetError::InvalidQuery(
                        "Expected a list of Tuples with the same arity".to_string(),
                    ));
                }
            };

            if list.iter().any(|l| match l {
                Expr::Literal(Literal::Placeholder(_)) => true,
                Expr::Row { exprs, .. }
                    if exprs
                        .iter()
                        .all(|e| matches!(e, Expr::Literal(Literal::Placeholder(_)))) =>
                {
                    true
                }
                _ => false,
            }) {
                // If the list contains placeholders, flatten them. `where_in_to_placeholders` takes
                // care of erroring-out if the list contains any *non*-placeholders
                self.out.push(where_in_to_placeholders(
                    &mut self.leftmost_param_index,
                    expression,
                )?);
                return Ok(());
            }
        }

        visit_mut::walk_expr(self, expression)
    }
}

/// Convert all instances of *parameterized* IN (`x IN (?, ?, ...)`) in the given `query` to a
/// direct equality comparison (`x = ?`), returning a vector of [`RewrittenIn`] giving information
/// about the rewritten in params.
///
/// Given that vector and the params provided by a user, [`explode_params`] can be used to construct
/// a vector of lookup keys for executing that query.
///
/// Note that IN conditions without any placeholders will be left untouched, as these can be handled
/// by regular filter nodes in dataflow
fn collapse_where_in(query: &mut SelectStatement) -> ReadySetResult<Vec<RewrittenIn>> {
    let mut res = vec![];

    if let Some(ref mut w) = query.where_clause {
        let mut visitor = CollapseWhereInVisitor::default();
        visitor.visit_expr(w)?;
        res = visitor.out;

        // If a query contains a subquery that uses `WHERE .. IN`, we tag all selects (outer and
        // sub queries) in the query with `CollapsedWhereIn`.
        //
        // While we only need to tag the outer query, there is no issue in tagging other
        // subqueries, since the outer query is the only one that gets a reader node.
        //
        // Also keep in mind, that subqueries get rewritten to joins, so completely ignoring
        // subqueries's metadata and only focusing on the outer query is fine in this case.
        if !res.is_empty() {
            query.metadata.push(SelectMetadata::CollapsedWhereIn);
        }

        if !res.is_empty() && query.distinct {
            unsupported!("DISTINCT with parameterized IN is not supported");
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

    let mut index = 0;
    let mut chunks: Vec<Vec<Vec<T>>> = Vec::with_capacity(params.len());

    let sorted_conditions = rewritten_in_conditions
        .iter()
        .sorted_by_key(|i| i.first_param_index);

    for RewrittenIn {
        first_param_index,
        literals,
        group_size,
    } in sorted_conditions
    {
        // Push atomic parameters before this rewritten `IN` clause
        if index < *first_param_index {
            chunks.push(vec![params[index..*first_param_index].to_vec()]);
        }

        index = *first_param_index;

        // Handle `(a, b) IN ((?, ?), (?, ?))` by chunking literals based on `group_size`
        // This also works for `a IN (?,?)` because `group_size` will be 1
        chunks.push(
            params[index..index + literals.len()]
                .chunks(*group_size)
                .map(|x| x.to_vec())
                .collect(),
        );

        index += literals.len();
    }

    // Append any remaining parameters
    if index < params.len() {
        chunks.push(vec![params[index..].to_vec()]);
    }

    // generate possible combinations using cartesian product (Vec<Chunks>)
    // flatten the chunks (Vec<T>) to go from Vec<Vec<T>> to Vec<T>
    Either::Right(Either::Right(
        chunks
            .into_iter()
            .multi_cartesian_product()
            .map(|x| x.into_iter().flatten().collect())
            .map(Cow::Owned),
    ))
}

struct ReorderNumberedPlaceholdersVisitor {
    current: u32,
    out: Vec<usize>,
}

impl<'ast> VisitorMut<'ast> for ReorderNumberedPlaceholdersVisitor {
    type Error = std::convert::Infallible;

    fn visit_literal(&mut self, literal: &'ast mut Literal) -> Result<(), Self::Error> {
        if let Literal::Placeholder(ItemPlaceholder::DollarNumber(n)) = literal {
            self.out.push(*n as usize - 1);
            *n = self.current;
            self.current += 1
        }

        Ok(())
    }
}

fn reorder_numbered_placeholders(query: &mut SelectStatement) -> Option<Vec<usize>> {
    let mut visitor = ReorderNumberedPlaceholdersVisitor {
        current: 1,
        out: vec![],
    };

    #[allow(clippy::unwrap_used)] // Error is !, so can't be returned
    visitor.visit_select_statement(query).unwrap();

    // As an optimization, check if the placeholders were *already* ordered and contiguous, and
    // return None if so. This allows us to save some clones on the actual read-path.
    let mut contiguous = true;
    let mut prev = *visitor.out.first()?;
    for n in &visitor.out {
        if prev + 1 != *n {
            contiguous = false;
            break;
        }
        prev = *n;
    }

    if contiguous {
        None
    } else {
        Some(visitor.out)
    }
}

/// Reorder the values in `params` according to `order_map`. `order_map` is a slice of indices where
/// each entry corresponds, in order, to each placeholder in the query, and the value is the index
/// in `params` that should be used for that placeholder.
///
/// The vector returned from this function is the set of parameters to apply to each placeholder in
/// the query in order.
fn reorder_params<T>(params: &[T], order_map: &[usize]) -> ReadySetResult<Vec<T>>
where
    T: Clone,
{
    order_map
        .iter()
        .map(|idx| {
            params.get(*idx).cloned().ok_or_else(|| {
                internal_err!("Should not be making keys with incorrect number of params.")
            })
        })
        .collect()
}

struct NumberPlaceholdersVisitor {
    next_param_number: u32,
}

impl<'ast> VisitorMut<'ast> for NumberPlaceholdersVisitor {
    type Error = ReadySetError;
    fn visit_literal(&mut self, literal: &'ast mut Literal) -> Result<(), Self::Error> {
        if let Literal::Placeholder(item) = literal {
            // client-provided queries aren't allowed to mix question-mark and dollar-number
            // placeholders, but both autoparameterization and collapse-where-in create question
            // mark placeholders, which in the intermediate state does end up with a
            // query that has both placeholder styles.
            match item {
                ItemPlaceholder::QuestionMark => {
                    *item = ItemPlaceholder::DollarNumber(self.next_param_number);
                }
                ItemPlaceholder::DollarNumber(n) => {
                    *n = self.next_param_number;
                }
                ItemPlaceholder::ColonNumber(_) => {
                    unsupported!("colon-number placeholders aren't supported")
                }
            }
            self.next_param_number += 1;
        }
        Ok(())
    }
}

pub fn number_placeholders(query: &mut SelectStatement) -> ReadySetResult<()> {
    let mut visitor = NumberPlaceholdersVisitor {
        next_param_number: 1,
    };
    visitor.visit_select_statement(query)?;
    Ok(())
}

/// Splice the given list of extracted parameters, which should be a tuple of (placeholder position,
/// value) as returned by [`auto_parameterize_query`] into the given list of parameters supplied by
/// the user, by interleaving them into the params based on the placeholder position.
///
/// # Invariants
///
/// `extracted_auto_params` must be sorted by the first index (this is the case with the return
/// value of [`auto_parameterize_query`]).
fn splice_auto_parameters<'param, T: Clone>(
    mut params: &'param [T],
    extracted_auto_params: &[(usize, T)],
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
    struct ClearMetadataVisitor;

    impl<'ast> VisitorMut<'ast> for ClearMetadataVisitor {
        type Error = String;
        fn visit_select_statement(
            &mut self,
            query: &'ast mut SelectStatement,
        ) -> Result<(), Self::Error> {
            query.metadata.clear();
            visit_mut::walk_select_statement(self, query)?;
            Ok(())
        }
    }

    trait ClearMetadata {
        fn clear_metadata(&mut self);
    }

    impl ClearMetadata for SelectStatement {
        /// Asserts that ALL SelectStatements's metadatas contain
        /// `CollapsedWhereIn`, then clear them.
        fn clear_metadata(&mut self) {
            assert!(self
                .metadata
                .iter()
                .contains(&SelectMetadata::CollapsedWhereIn));

            let mut visitor = ClearMetadataVisitor;
            visitor.visit_select_statement(self).unwrap();
        }
    }

    use readyset_sql::Dialect;

    use super::*;

    fn try_parse_select_statement(q: &str, dialect: Dialect) -> Result<SelectStatement, String> {
        readyset_sql_parsing::parse_select(dialect, q).map_err(|err| err.to_string())
    }

    fn parse_select_statement(q: &str, dialect: Dialect) -> SelectStatement {
        try_parse_select_statement(q, dialect).unwrap()
    }

    fn parse_select_statement_mysql(q: &str) -> SelectStatement {
        parse_select_statement(q, Dialect::MySQL)
    }

    fn parse_select_statement_postgres(q: &str) -> SelectStatement {
        parse_select_statement(q, Dialect::PostgreSQL)
    }

    mod collapse_where {
        use pretty_assertions::assert_eq;

        use super::*;

        #[test]
        fn collapsed_where_placeholders() {
            let mut q = parse_select_statement_mysql("SELECT * FROM x WHERE x.y IN (?, ?, ?)");
            let rewritten = collapse_where_in(&mut q).unwrap();
            assert_eq!(
                rewritten,
                vec![RewrittenIn {
                    first_param_index: 0,
                    literals: vec![ItemPlaceholder::QuestionMark; 3],
                    group_size: 1,
                }]
            );

            q.clear_metadata();

            assert_eq!(
                q,
                parse_select_statement_mysql("SELECT * FROM x WHERE x.y = ?")
            );

            let mut q = parse_select_statement_mysql("SELECT * FROM x WHERE y IN (?, ?, ?)");
            let rewritten = collapse_where_in(&mut q).unwrap();
            assert_eq!(
                rewritten,
                vec![RewrittenIn {
                    first_param_index: 0,
                    literals: vec![ItemPlaceholder::QuestionMark; 3],
                    group_size: 1,
                }]
            );

            q.clear_metadata();

            assert_eq!(
                q,
                parse_select_statement_mysql("SELECT * FROM x WHERE y = ?")
            );

            let mut q = parse_select_statement_mysql("SELECT * FROM x WHERE AVG(y) IN (?, ?, ?)");
            let rewritten = collapse_where_in(&mut q).unwrap();
            assert_eq!(
                rewritten,
                vec![RewrittenIn {
                    first_param_index: 0,
                    literals: vec![ItemPlaceholder::QuestionMark; 3],
                    group_size: 1,
                }]
            );

            q.clear_metadata();

            assert_eq!(
                q,
                parse_select_statement_mysql("SELECT * FROM x WHERE AVG(y) = ?")
            );

            let mut q = parse_select_statement_mysql(
                "SELECT * FROM t WHERE x = ? AND y IN (?, ?, ?) OR z = ?",
            );
            let rewritten = collapse_where_in(&mut q).unwrap();
            assert_eq!(
                rewritten,
                vec![RewrittenIn {
                    first_param_index: 1,
                    literals: vec![ItemPlaceholder::QuestionMark; 3],
                    group_size: 1,
                }]
            );

            q.clear_metadata();

            assert_eq!(
                q,
                parse_select_statement_mysql("SELECT * FROM t WHERE x = ? AND y = ? OR z = ?")
            );

            let mut q = parse_select_statement_mysql(
                "SELECT * FROM t WHERE x IN (SELECT * FROM z WHERE a = ?) AND y IN (?, ?) OR z = ?",
            );
            let rewritten = collapse_where_in(&mut q).unwrap();
            assert_eq!(
                rewritten,
                vec![RewrittenIn {
                    first_param_index: 1,
                    literals: vec![ItemPlaceholder::QuestionMark; 2],
                    group_size: 1,
                }]
            );

            q.clear_metadata();

            assert_eq!(
                q,
                parse_select_statement_mysql(
                    "SELECT * FROM t WHERE x IN (SELECT * FROM z WHERE a = ?) AND y = ? OR z = ?"
                )
            );

            let mut q = parse_select_statement_mysql(
                "SELECT * FROM t WHERE x IN (SELECT * FROM z WHERE b = ? AND a IN (?, ?)) OR z = ?",
            );
            let rewritten = collapse_where_in(&mut q).unwrap();
            assert_eq!(
                rewritten,
                vec![RewrittenIn {
                    first_param_index: 1,
                    literals: vec![ItemPlaceholder::QuestionMark; 2],
                    group_size: 1,
                }]
            );

            q.clear_metadata();

            assert_eq!(
                q,
                parse_select_statement_mysql(
                    "SELECT * FROM t WHERE x IN (SELECT * FROM z WHERE b = ? AND a = ?) OR z = ?",
                )
            );
        }

        #[test]
        fn collapsed_where_literals() {
            let mut q = parse_select_statement_mysql("SELECT * FROM x WHERE x.y IN (1, 2, 3)");
            assert_eq!(collapse_where_in(&mut q).unwrap(), vec![]);
            assert_eq!(
                q,
                parse_select_statement_mysql("SELECT * FROM x WHERE x.y IN (1, 2, 3)")
            );
        }

        #[test]
        fn collapsed_where_dollarsign_placeholders() {
            let mut q = parse_select_statement_mysql("SELECT * FROM x WHERE x.y IN ($1, $2, $3)");
            let rewritten = collapse_where_in(&mut q).unwrap();
            assert_eq!(
                rewritten,
                vec![RewrittenIn {
                    first_param_index: 0,
                    literals: vec![
                        ItemPlaceholder::DollarNumber(1),
                        ItemPlaceholder::DollarNumber(2),
                        ItemPlaceholder::DollarNumber(3),
                    ],
                    group_size: 1,
                }]
            );

            q.clear_metadata();
            assert_eq!(
                q,
                parse_select_statement_mysql("SELECT * FROM x WHERE x.y = ?")
            );

            let mut q = parse_select_statement_postgres("SELECT * FROM x WHERE y IN ($1, $2, $3)");
            let rewritten = collapse_where_in(&mut q).unwrap();
            assert_eq!(
                rewritten,
                vec![RewrittenIn {
                    first_param_index: 0,
                    literals: vec![
                        ItemPlaceholder::DollarNumber(1),
                        ItemPlaceholder::DollarNumber(2),
                        ItemPlaceholder::DollarNumber(3),
                    ],
                    group_size: 1,
                }]
            );

            q.clear_metadata();
            assert_eq!(
                q,
                parse_select_statement_mysql("SELECT * FROM x WHERE y = ?")
            );

            let mut q =
                parse_select_statement_postgres("SELECT * FROM x WHERE AVG(y) IN ($1, $2, $3)");
            let rewritten = collapse_where_in(&mut q).unwrap();
            assert_eq!(
                rewritten,
                vec![RewrittenIn {
                    first_param_index: 0,
                    literals: vec![
                        ItemPlaceholder::DollarNumber(1),
                        ItemPlaceholder::DollarNumber(2),
                        ItemPlaceholder::DollarNumber(3),
                    ],
                    group_size: 1,
                }]
            );

            q.clear_metadata();
            assert_eq!(
                q,
                parse_select_statement_mysql("SELECT * FROM x WHERE AVG(y) = ?")
            );

            let mut q = parse_select_statement_postgres(
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
                    ],
                    group_size: 1,
                }]
            );

            q.clear_metadata();
            assert_eq!(
                q,
                parse_select_statement_mysql("SELECT * FROM t WHERE x = $1 AND y = ? OR z = $5")
            );

            let mut q = parse_select_statement_postgres(
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
                    ],
                    group_size: 1,
                }]
            );

            q.clear_metadata();
            assert_eq!(
                q,
                parse_select_statement_mysql(
                    "SELECT * FROM t WHERE x IN (SELECT * FROM z WHERE a = $1) AND y = ? OR z = $4",
                )
            );

            let mut q = parse_select_statement_postgres(
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
                    ],
                    group_size: 1,
                }]
            );
            q.clear_metadata();
            assert_eq!(
                q,
                parse_select_statement_mysql(
                    "SELECT * FROM t WHERE x IN (SELECT * FROM z WHERE b = $1 AND a = ?) OR z = $4",
                )
            );
        }

        #[test]
        fn collapse_multiple_where_in() {
            let mut q =
                parse_select_statement_mysql("SELECT * FROM t WHERE x IN (?,?) AND y IN (?,?)");
            let rewritten = collapse_where_in(&mut q).unwrap();

            assert_eq!(
                rewritten,
                vec![
                    RewrittenIn {
                        first_param_index: 0,
                        literals: vec![ItemPlaceholder::QuestionMark; 2],
                        group_size: 1,
                    },
                    RewrittenIn {
                        first_param_index: 2,
                        literals: vec![ItemPlaceholder::QuestionMark; 2],
                        group_size: 1,
                    }
                ]
            );

            q.clear_metadata();

            assert_eq!(
                q,
                parse_select_statement_mysql("SELECT * FROM t WHERE x = ? AND y = ?")
            );
        }

        #[test]
        fn collapse_where_row_in() {
            let mut q =
                parse_select_statement_mysql("SELECT * FROM t WHERE (x, y) IN ((?, ?), (?,?))");

            let _ = collapse_where_in(&mut q).unwrap();

            q.clear_metadata();

            assert_eq!(
                q,
                parse_select_statement_mysql("SELECT * FROM t WHERE (x, y) = (?, ?)")
            );
        }
    }

    mod explode_params {
        use pretty_assertions::assert_eq;

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
                group_size: 1,
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
                    group_size: 1,
                },
                RewrittenIn {
                    first_param_index: 4,
                    literals: vec![ItemPlaceholder::QuestionMark; 2],
                    group_size: 1,
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

    #[test]
    fn test_trailing_in() {
        // SELECT * FROM t WHERE x = ? AND y = ? AND z IN (?, ?, ?)
        let rewritten_in_conditions = vec![RewrittenIn {
            first_param_index: 2,
            literals: vec![ItemPlaceholder::QuestionMark; 3],
            group_size: 1,
        }];
        let params = vec![1u32, 2, 3, 4, 5];
        let res = explode_params(&params, &rewritten_in_conditions).collect::<Vec<_>>();
        assert_eq!(res, vec![vec![1, 2, 3], vec![1, 2, 4], vec![1, 2, 5],]);
    }

    #[test]
    fn test_leading_in() {
        // SELECT * FROM t WHERE x IN (?, ?, ?) AND y = ? AND z = ?
        let rewritten_in_conditions = vec![RewrittenIn {
            first_param_index: 0,
            literals: vec![ItemPlaceholder::QuestionMark; 3],
            group_size: 1,
        }];
        let params = vec![1u32, 2, 3, 4, 5];
        let res = explode_params(&params, &rewritten_in_conditions).collect::<Vec<_>>();
        assert_eq!(res, vec![vec![1, 4, 5], vec![2, 4, 5], vec![3, 4, 5],]);
    }

    #[test]
    fn test_leading_row_in() {
        // SELECT * FROM t WHERE (x, y) IN ((?, ?), (?,?)) AND z = ? AND a = ?
        let rewritten_in_conditions = vec![RewrittenIn {
            first_param_index: 0,
            literals: vec![ItemPlaceholder::QuestionMark; 4],
            group_size: 2,
        }];
        let params = vec![1u32, 2, 3, 4, 5, 6];
        let res = explode_params(&params, &rewritten_in_conditions).collect::<Vec<_>>();
        assert_eq!(res, vec![vec![1, 2, 5, 6], vec![3, 4, 5, 6]]);
    }

    #[test]
    fn test_trailing_row_in() {
        // SELECT * FROM t WHERE x = ? AND y = ? AND (z, a) IN ((?, ?), (?,?))
        let rewritten_in_conditions = vec![RewrittenIn {
            first_param_index: 2,
            literals: vec![ItemPlaceholder::QuestionMark; 4],
            group_size: 2,
        }];
        let params = vec![1u32, 2, 3, 4, 5, 6];
        let res = explode_params(&params, &rewritten_in_conditions).collect::<Vec<_>>();
        assert_eq!(res, vec![vec![1, 2, 3, 4], vec![1, 2, 5, 6]]);
    }

    mod splice_auto_parameters {
        use pretty_assertions::assert_eq;

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
        use pretty_assertions::assert_eq;
        use readyset_data::DfValue;

        use super::*;

        // impls Default, but default is not const fn and therefore can't be used here
        const PARAMS: AdapterRewriteParams = AdapterRewriteParams {
            server_supports_topk: false,
            server_supports_pagination: false,
            server_supports_mixed_comparisons: false,
        };

        fn process_and_make_keys(
            query: &str,
            params: Vec<DfValue>,
            dialect: readyset_sql::Dialect,
        ) -> (Vec<Vec<DfValue>>, SelectStatement) {
            let mut query = parse_select_statement(query, dialect);
            let processed = process_query(&mut query, PARAMS).unwrap();
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

        fn process_and_make_keys_postgres(
            query: &str,
            params: Vec<DfValue>,
        ) -> (Vec<Vec<DfValue>>, SelectStatement) {
            process_and_make_keys(query, params, readyset_sql::Dialect::PostgreSQL)
        }

        fn process_and_make_keys_mysql(
            query: &str,
            params: Vec<DfValue>,
        ) -> (Vec<Vec<DfValue>>, SelectStatement) {
            process_and_make_keys(query, params, readyset_sql::Dialect::MySQL)
        }

        fn get_lim_off(
            query: &str,
            params: &[DfValue],
            dialect: readyset_sql::Dialect,
        ) -> (Option<usize>, Option<usize>) {
            let proc = process_query(&mut parse_select_statement(query, dialect), PARAMS).unwrap();
            proc.limit_offset_params(params).unwrap()
        }

        fn get_lim_off_postgres(query: &str, params: &[DfValue]) -> (Option<usize>, Option<usize>) {
            get_lim_off(query, params, readyset_sql::Dialect::PostgreSQL)
        }

        fn get_lim_off_mysql(query: &str, params: &[DfValue]) -> (Option<usize>, Option<usize>) {
            get_lim_off(query, params, readyset_sql::Dialect::MySQL)
        }

        #[test]
        fn rewrite_literals() {
            let mut query = parse_select_statement_postgres(
                "SELECT id FROM users WHERE credit_card_number = 'look at this PII' AND id = 3",
            );
            let expected = parse_select_statement_postgres(
                "SELECT id FROM users WHERE credit_card_number = $1 AND id = $2",
            );

            process_query(&mut query, PARAMS).expect("Should be able to rewrite query");
            assert_eq!(
                query.display(readyset_sql::Dialect::PostgreSQL).to_string(),
                expected
                    .display(readyset_sql::Dialect::PostgreSQL)
                    .to_string()
            );
        }

        #[test]
        fn rewrite_literals_range() {
            let mut query = parse_select_statement_postgres(
                "SELECT id FROM users WHERE credit_card_number = 'look at this PII' AND id = 3",
            );
            let expected = parse_select_statement_postgres(
                "SELECT id FROM users WHERE credit_card_number = $1 AND id = $2",
            );

            process_query(&mut query, PARAMS).expect("Should be able to rewrite query");
            assert_eq!(
                query.display(readyset_sql::Dialect::PostgreSQL).to_string(),
                expected
                    .display(readyset_sql::Dialect::PostgreSQL)
                    .to_string()
            );
        }

        #[test]
        fn single_literal() {
            let mut query = parse_select_statement_postgres(
                "SELECT id + 3 FROM users WHERE credit_card_number = 'look at this PII'",
            );
            let expected = parse_select_statement_postgres(
                "SELECT id + 3 FROM users WHERE credit_card_number = $1",
            );
            process_query(&mut query, PARAMS).expect("Should be able to rewrite query");
            assert_eq!(query, expected);
        }

        #[test]
        fn no_keys() {
            let (keys, query) = process_and_make_keys_postgres("SELECT * FROM test", vec![]);
            assert_eq!(query, parse_select_statement_postgres("SELECT * FROM test"));
            assert!(keys.is_empty(), "keys = {keys:?}");

            let (keys, query) = process_and_make_keys_mysql("SELECT * FROM test", vec![]);
            assert_eq!(query, parse_select_statement_mysql("SELECT * FROM test"));
            assert!(keys.is_empty(), "keys = {keys:?}");
        }

        #[test]
        fn numbered_auto_params() {
            let (keys, query) =
                process_and_make_keys_postgres("SELECT x, y FROM test WHERE x = 4", vec![]);

            assert_eq!(
                query,
                parse_select_statement_postgres("SELECT x, y FROM test WHERE x = $1")
            );

            assert_eq!(keys, vec![vec![4.into()]]);
        }

        #[test]
        fn number_autoparam_number() {
            let (keys, query) = process_and_make_keys_postgres(
                "SELECT x, y FROM test WHERE x = $1 AND y = 2 AND z = $2",
                vec![1.into(), 3.into()],
            );

            assert_eq!(
                query,
                parse_select_statement_postgres(
                    "SELECT x, y FROM test WHERE x = $1 AND y = $2 AND z = $3"
                )
            );

            assert_eq!(keys, vec![vec![1.into(), 2.into(), 3.into()]]);
        }

        #[test]
        fn numbered_where_in_with_auto_params() {
            let (keys, mut query) = process_and_make_keys_mysql(
                "SELECT * FROM users WHERE x = ? AND y in (?, ?, ?) AND z = 4 AND w = 5 AND q = ?",
                vec![0.into(), 1.into(), 2.into(), 3.into(), 6.into()],
            );

            query.clear_metadata();

            assert_eq!(
                query,
                parse_select_statement_postgres(
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
        fn numbered_auto_parameterized_in() {
            let (keys, mut query) = process_and_make_keys_mysql(
                "SELECT * FROM users WHERE x = 1 AND y IN (1, 2, 3) AND z = ?",
                vec![1.into()],
            );

            query.clear_metadata();

            assert_eq!(
                query,
                parse_select_statement_postgres(
                    "SELECT * FROM users WHERE x = $1 AND y = $2 AND z = $3"
                )
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
            let (keys, mut query) = process_and_make_keys_postgres(
                "SELECT Cats.name FROM Cats WHERE Cats.name = $1 AND Cats.id IN ($2, $3)",
                vec!["Bob".into(), 1.into(), 2.into()],
            );

            query.clear_metadata();

            assert_eq!(
                query,
                parse_select_statement_postgres(
                    "SELECT Cats.name FROM Cats WHERE Cats.name = $1 AND Cats.id = $2"
                )
            );

            assert_eq!(
                keys,
                vec![vec!["Bob".into(), 1.into()], vec!["Bob".into(), 2.into()]]
            );
        }

        #[test]
        fn numbered_point_following_where_in() {
            let (keys, mut query) = process_and_make_keys_postgres(
                "SELECT a FROM t WHERE b IN ($1, $2) AND c = $3",
                vec![1.into(), 2.into(), 3.into()],
            );

            query.clear_metadata();

            assert_eq!(
                query,
                parse_select_statement_postgres("SELECT a FROM t WHERE b = $1 AND c = $2")
            );

            assert_eq!(
                keys,
                vec![vec![1.into(), 3.into()], vec![2.into(), 3.into()]]
            );
        }

        #[test]
        fn numbered_point_following_where_in_unordered() {
            let (keys, mut query) = process_and_make_keys_postgres(
                "SELECT a FROM t WHERE b IN ($3, $1) AND c = $2",
                vec![2.into(), 3.into(), 1.into()],
            );

            query.clear_metadata();

            assert_eq!(
                query,
                parse_select_statement_postgres("SELECT a FROM t WHERE b = $1 AND c = $2")
            );

            assert_eq!(
                keys,
                vec![vec![1.into(), 3.into()], vec![2.into(), 3.into()]]
            );
        }

        #[test]
        fn numbered_point_following_two_where_in() {
            let (keys, mut query) = process_and_make_keys_postgres(
                "SELECT a FROM t WHERE b IN ($1, $2) AND c IN ($3, $4) AND d = $5",
                vec![1.into(), 2.into(), 3.into(), 4.into(), 5.into()],
            );

            query.clear_metadata();

            assert_eq!(
                query,
                parse_select_statement_postgres(
                    "SELECT a FROM t WHERE b = $1 AND c = $2 AND d = $3"
                )
            );

            assert_eq!(
                keys,
                vec![
                    vec![1.into(), 3.into(), 5.into()],
                    vec![1.into(), 4.into(), 5.into()],
                    vec![2.into(), 3.into(), 5.into()],
                    vec![2.into(), 4.into(), 5.into()]
                ]
            );
        }

        #[test]
        fn numbered_not_in_order_auto_param() {
            let (keys, query) = process_and_make_keys_postgres(
                "SELECT * FROM t WHERE x = $2 AND y = $1 AND z = 'z'",
                vec!["y".into(), "x".into()],
            );

            assert_eq!(
                query,
                parse_select_statement_postgres(
                    "SELECT * FROM t WHERE x = $1 AND y = $2 AND z = $3"
                ),
                "{}",
                query.display(readyset_sql::Dialect::PostgreSQL)
            );

            assert_eq!(keys, vec![vec!["x".into(), "y".into(), "z".into()]]);
        }

        #[test]
        fn numbered_not_in_order() {
            let (keys, query) = process_and_make_keys_postgres(
                "SELECT * FROM t WHERE x = $3 AND y = $1 AND z = $2",
                vec!["y".into(), "z".into(), "x".into()],
            );

            assert_eq!(
                query,
                parse_select_statement_postgres(
                    "SELECT * FROM t WHERE x = $1 AND y = $2 AND z = $3"
                ),
                "{}",
                query.display(readyset_sql::Dialect::PostgreSQL)
            );

            assert_eq!(keys, vec![vec!["x".into(), "y".into(), "z".into()]]);
        }

        #[test]
        fn numbered_not_in_order_starts_in_order() {
            let (keys, query) = process_and_make_keys_postgres(
                "SELECT * FROM t WHERE x = $1 AND y = $3 AND z = $2",
                vec!["x".into(), "z".into(), "y".into()],
            );

            assert_eq!(
                query,
                parse_select_statement_postgres(
                    "SELECT * FROM t WHERE x = $1 AND y = $2 AND z = $3"
                ),
                "{}",
                query.display(readyset_sql::Dialect::PostgreSQL)
            );

            assert_eq!(keys, vec![vec!["x".into(), "y".into(), "z".into()]]);
        }

        #[test]
        fn bare_offset_zero() {
            let (keys, query) = process_and_make_keys_postgres(
                "SELECT * FROM t WHERE x = $2 OFFSET $1",
                vec![0.into(), 1.into()],
            );

            assert_eq!(
                query,
                parse_select_statement_postgres("SELECT * FROM t WHERE x = $1"),
                "{}",
                query.display(readyset_sql::Dialect::PostgreSQL)
            );
            assert_eq!(keys, vec![vec![1.into()]]);
        }

        #[test]
        fn bare_offset_nonzero() {
            let (keys, query) = process_and_make_keys_postgres(
                "SELECT * FROM t WHERE x = $2 OFFSET $1",
                vec![15.into(), 1.into()],
            );

            assert_eq!(
                query,
                parse_select_statement_postgres("SELECT * FROM t WHERE x = $1"),
                "{}",
                query.display(readyset_sql::Dialect::PostgreSQL)
            );
            assert_eq!(keys, vec![vec![1.into()]]);
        }

        #[test]
        fn limit_offset_full_form() {
            assert_eq!(
                get_lim_off_postgres(
                    "SELECT * FROM t WHERE x = $2 LIMIT $3 OFFSET $1",
                    &[1.into(), 2.into(), 3.into()],
                ),
                (Some(3), Some(1))
            );

            assert_eq!(
                get_lim_off_mysql(
                    "SELECT * FROM t WHERE x = ? LIMIT ? OFFSET ?",
                    &[1.into(), 2.into(), 3.into()],
                ),
                (Some(2), Some(3))
            );

            assert_eq!(
                get_lim_off_postgres(
                    "SELECT * FROM t WHERE x = $2 LIMIT 5 OFFSET $1",
                    &[1.into(), 2.into(), 3.into()],
                ),
                (Some(5), Some(1))
            );

            assert_eq!(
                get_lim_off_mysql(
                    "SELECT * FROM t WHERE x = ? LIMIT ? OFFSET 4",
                    &[1.into(), 2.into()],
                ),
                (Some(2), Some(4))
            );

            assert_eq!(
                get_lim_off_mysql(
                    "SELECT * FROM t WHERE x = ? LIMIT 4 OFFSET ?",
                    &[1.into(), 2.into()],
                ),
                (Some(4), Some(2))
            );

            // Test non-integer values

            // Postgres rounds up to the nearest int
            assert_eq!(
                get_lim_off_postgres(
                    "SELECT * FROM t WHERE x = $2 LIMIT 2.5 OFFSET $1",
                    &[1.into(), 3.into()],
                ),
                (Some(3), Some(1))
            );

            try_parse_select_statement(
                "SELECT * FROM t WHERE x = ? LIMIT 1 OFFSET ALL",
                readyset_sql::Dialect::MySQL,
            )
            .unwrap_err();

            try_parse_select_statement(
                "SELECT * FROM t WHERE x = ? LIMIT 1.5 OFFSET 3",
                readyset_sql::Dialect::MySQL,
            )
            .unwrap_err();
        }

        #[test]
        fn limit_offset_limit_only() {
            // Test only limit for both dialects
            assert_eq!(
                get_lim_off_postgres(
                    "SELECT * FROM t WHERE x = $1 LIMIT $2",
                    &[1.into(), 2.into()],
                ),
                (Some(2), None)
            );
            assert_eq!(
                get_lim_off_postgres(
                    "SELECT * FROM t WHERE x = $1 LIMIT 10.5",
                    &[1.into(), 2.into()],
                ),
                (Some(11), None)
            );

            assert_eq!(
                get_lim_off_mysql("SELECT * FROM t WHERE x = ? LIMIT ?", &[1.into(), 2.into()],),
                (Some(2), None)
            );

            try_parse_select_statement(
                "SELECT * FROM t WHERE x = ? LIMIT 1.5",
                readyset_sql::Dialect::MySQL,
            )
            .unwrap_err();
        }

        #[test]
        fn limit_offset_offset_only() {
            assert_eq!(
                get_lim_off_postgres(
                    "SELECT * FROM t WHERE x = $1 OFFSET $2",
                    &[1.into(), 2.into()],
                ),
                (None, Some(2))
            );
            assert_eq!(
                get_lim_off_postgres(
                    "SELECT * FROM t WHERE x = $1 OFFSET 10.5",
                    &[1.into(), 2.into()],
                ),
                (None, Some(11))
            );
        }

        #[test]
        #[ignore = "REA-5766: sqlparser simply removes `LIMIT ALL`"]
        fn limit_offset_postgres_all() {
            assert_eq!(
                get_lim_off_postgres(
                    "SELECT * FROM t WHERE x = $2 LIMIT ALL OFFSET $1",
                    &[1.into(), 2.into()],
                ),
                (None, Some(1))
            );
            assert_eq!(
                get_lim_off_postgres(
                    "SELECT * FROM t WHERE x = $1 LIMIT ALL",
                    &[1.into(), 2.into()],
                ),
                (None, None)
            );
        }

        #[test]
        fn limit_offset_mysql_special() {
            assert_eq!(
                get_lim_off(
                    "SELECT * FROM t WHERE x = ? LIMIT ?, ?",
                    &[1.into(), 2.into(), 3.into()],
                    readyset_sql::Dialect::MySQL,
                ),
                (Some(3), Some(2))
            );

            assert_eq!(
                get_lim_off(
                    "SELECT * FROM t WHERE x = ? LIMIT 4, ?",
                    &[1.into(), 2.into()],
                    readyset_sql::Dialect::MySQL,
                ),
                (Some(2), Some(4))
            );

            assert_eq!(
                get_lim_off(
                    "SELECT * FROM t WHERE x = ? LIMIT ?, 4",
                    &[1.into(), 2.into()],
                    readyset_sql::Dialect::MySQL,
                ),
                (Some(4), Some(2))
            );

            // PostgreSQL doesn't accept this form at all
            try_parse_select_statement(
                "SELECT * FROM t WHERE x = $3 LIMIT $2, $1",
                readyset_sql::Dialect::PostgreSQL,
            )
            .unwrap_err();
            try_parse_select_statement(
                "SELECT * FROM t WHERE x = $3 LIMIT 1, $1",
                readyset_sql::Dialect::PostgreSQL,
            )
            .unwrap_err();
            try_parse_select_statement(
                "SELECT * FROM t WHERE x = $3 LIMIT $1, 2",
                readyset_sql::Dialect::PostgreSQL,
            )
            .unwrap_err();
            try_parse_select_statement(
                "SELECT * FROM t WHERE x = $3 LIMIT 1, 2",
                readyset_sql::Dialect::PostgreSQL,
            )
            .unwrap_err();
        }

        #[test]
        fn reuses_params_basic() {
            let (keys, query) = process_and_make_keys_postgres(
                "SELECT * FROM t WHERE x = $1 AND y = $1",
                vec![0.into()],
            );

            assert_eq!(
                query,
                parse_select_statement_postgres("SELECT * FROM t WHERE x = $1 AND y = $2"),
                "{}",
                query.display(readyset_sql::Dialect::PostgreSQL)
            );
            assert_eq!(keys, vec![vec![0.into(), 0.into()]]);
        }

        #[test]
        fn reuses_params_with_auto_param() {
            let (keys, query) = process_and_make_keys_postgres(
                "SELECT * FROM t WHERE x = $1 AND y = 0 AND z = $1",
                vec![1.into()],
            );

            assert_eq!(
                query,
                parse_select_statement_postgres(
                    "SELECT * FROM t WHERE x = $1 AND y = $2 AND z = $3"
                ),
                "{}",
                query.display(readyset_sql::Dialect::PostgreSQL)
            );
            assert_eq!(keys, vec![vec![1.into(), 0.into(), 1.into()]]);
        }

        #[test]
        fn reuses_params_with_where_in() {
            let (keys, mut query) = process_and_make_keys_postgres(
                "SELECT * FROM t WHERE x = $1 AND y IN ($1, $2)",
                vec![1.into(), 2.into()],
            );

            query.clear_metadata();
            assert_eq!(
                query,
                parse_select_statement_postgres("SELECT * FROM t WHERE x = $1 AND y = $2"),
                "{}",
                query.display(readyset_sql::Dialect::PostgreSQL)
            );
            assert_eq!(
                keys,
                vec![vec![1.into(), 1.into()], vec![1.into(), 2.into()]]
            );
        }
    }
}
