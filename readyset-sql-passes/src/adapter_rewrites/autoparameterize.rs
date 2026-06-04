use std::collections::HashSet;
use std::mem;

use readyset_errors::{ReadySetError, ReadySetResult, unsupported};
use readyset_sql::analysis::visit_mut::{self, VisitorMut};
use readyset_sql::ast::{BinaryOperator, Expr, InValue, ItemPlaceholder, Literal, SelectStatement};

use crate::rewrite_utils::{
    iter_and_conjuncts, predicate_caps_row_number, preserve_row_number_caps,
};

/// Collect top-level WHERE conjuncts that cap a `ROW_NUMBER()` projection, returning a set
/// keyed by value-equality on `Expr`. Both orientations of each cap predicate are inserted
/// so the gate at the top of `visit_expr` also short-circuits the literal-on-left shape
/// that the swap arms produce by recursively revisiting the (now-swapped) expression.
/// Returns an empty set when `preserve_row_number_caps()` is false — the gate is then a
/// no-op and auto-parameterize handles cap predicates with its default literal-swap arms.
fn collect_top_level_caps(query: &SelectStatement) -> HashSet<Expr> {
    let mut set = HashSet::new();
    if !preserve_row_number_caps() {
        return set;
    }
    if let Some(where_clause) = query.where_clause.as_ref() {
        for conjunct in iter_and_conjuncts(where_clause) {
            if predicate_caps_row_number(conjunct, query).is_some() {
                set.insert(conjunct.clone());
                if let Some(flipped) = flip_binary_operands(conjunct) {
                    set.insert(flipped);
                }
            }
        }
    }
    set
}

/// For a `BinaryOp` whose operands the auto-parameterize swap arms would mutate
/// in-place, return the post-swap form. Used to pre-populate the cap-predicate set
/// with both orientations.
///
/// Equal: swap lhs/rhs (operator unchanged, equality is symmetric). Ordering
/// comparisons: swap lhs/rhs and flip the operator (e.g. `10 >= rn` -> `rn <= 10`).
/// Any other shape returns `None`.
fn flip_binary_operands(expr: &Expr) -> Option<Expr> {
    let Expr::BinaryOp { lhs, op, rhs } = expr else {
        return None;
    };
    let new_op = match op {
        BinaryOperator::Equal => *op,
        op if op.is_ordering_comparison() => op.flip_ordering_comparison().ok()?,
        _ => return None,
    };
    Some(Expr::BinaryOp {
        lhs: rhs.clone(),
        op: new_op,
        rhs: lhs.clone(),
    })
}

#[derive(Default)]
struct AutoParameterizeVisitor {
    autoparameterize_equals: bool,
    autoparameterize_ranges: bool,
    out: Vec<(usize, Literal)>,
    in_supported_position: bool,
    param_index: usize,
    query_depth: u8,
    visit_limit_clause: bool,
    /// Top-level WHERE conjuncts (in both orientations) that cap a `ROW_NUMBER()`
    /// projection. The integer literal in such a predicate is the cardinality signal
    /// CBJR consumes downstream; replacing it with a placeholder would destroy the
    /// signal, so the gate at the top of `visit_expr` short-circuits the walk when
    /// the current expression appears in this set.
    cap_predicates: HashSet<Expr>,
}

/// Replace a `Literal::Preserved(inner)` marker with its inner literal, in place. No-op for any
/// other literal.
fn unwrap_preserved(literal: &mut Literal) {
    if matches!(literal, Literal::Preserved(_))
        && let Literal::Preserved(inner) = mem::replace(literal, Literal::Null)
    {
        *literal = *inner;
    }
}

impl AutoParameterizeVisitor {
    fn replace_literal(&mut self, literal: &mut Literal) {
        // A literal frozen by the exclusion pre-pass must not be parameterized. The marker stays
        // in place so the second autoparameterization phase preserves it too; the final sweep at
        // the end of the Readyset rewrite unwraps it.
        if matches!(literal, Literal::Preserved(_)) {
            return;
        }
        let literal = mem::replace(literal, Literal::Placeholder(ItemPlaceholder::QuestionMark));
        self.out.push((self.param_index, literal));
        self.param_index += 1;
    }
}

/// Sweep that unwraps every `Literal::Preserved` marker to its inner literal. Run once at the
/// end of the Readyset rewrite, after both autoparameterization phases have honored the markers,
/// so none survives into the stored or executed form.
pub(super) fn unwrap_all_preserved(query: &mut SelectStatement) {
    struct UnwrapPreservedVisitor;

    impl<'ast> VisitorMut<'ast> for UnwrapPreservedVisitor {
        type Error = std::convert::Infallible;

        fn visit_literal(&mut self, literal: &'ast mut Literal) -> Result<(), Self::Error> {
            unwrap_preserved(literal);
            Ok(())
        }
    }

    let Ok(()) = UnwrapPreservedVisitor.visit_select_statement(query);
}

impl<'ast> VisitorMut<'ast> for AutoParameterizeVisitor {
    type Error = ReadySetError;

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
        visit_mut::walk_select_statement(self, select_statement)?;
        self.query_depth = self.query_depth.saturating_sub(1);
        Ok(())
    }

    fn visit_where_clause(&mut self, expression: &'ast mut Expr) -> Result<(), Self::Error> {
        // We can only support parameters in the WHERE clause of the top-level query, not any
        // subqueries it contains.
        self.in_supported_position = self.query_depth <= 1;
        self.visit_expr(expression)?;
        self.in_supported_position = false;
        Ok(())
    }

    fn visit_expr(&mut self, expression: &'ast mut Expr) -> Result<(), Self::Error> {
        let was_supported = self.in_supported_position;
        // Preserve row-number cap literals: the integer bound is a cardinality signal
        // that CBJR reads from the AST. Skipping the walk here keeps the literal
        // intact rather than rewriting it to a placeholder.
        if was_supported && self.cap_predicates.contains(expression) {
            return Ok(());
        }
        if was_supported {
            match expression {
                Expr::BinaryOp { lhs, op, rhs } => match (lhs.as_mut(), op, rhs.as_mut()) {
                    (Expr::Column(_), BinaryOperator::Equal, Expr::Literal(Literal::Placeholder(_))) => {}
                    (Expr::Row { .. }, BinaryOperator::Equal, Expr::Row { exprs, .. }) => {
                        for expr in exprs {
                            if let Expr::Literal(lit) = expr {
                                match lit {
                                    Literal::Placeholder(_) => continue,
                                    _ => self.replace_literal(lit),
                                }
                            }
                        }
                        return Ok(());
                    }
                    (Expr::Column(_), op, Expr::Literal(Literal::Placeholder(_))) if op.is_ordering_comparison() => {}
                    (Expr::Column(_), BinaryOperator::Equal, Expr::Literal(lit)) => {
                        if self.autoparameterize_equals {
                            self.replace_literal(lit);
                        }
                        return Ok(());
                    }
                    (Expr::Column(_), op, Expr::Literal(lit)) if op.is_ordering_comparison() => {
                        if self.autoparameterize_ranges {
                            self.replace_literal(lit);
                        }
                        return Ok(());
                    }
                    (Expr::Literal(_), BinaryOperator::Equal | BinaryOperator::NotEqual, Expr::Column(_)) => {
                        // for lit = col and lit != col, swap the equality first then revisit
                        mem::swap(lhs, rhs);
                        return self.visit_expr(expression);
                    }
                    (Expr::Literal(_), op, Expr::Column(_)) if op.is_ordering_comparison() => {
                        // for lit <ordering op> col, swap operands and flip operator, then revisit
                        mem::swap(lhs, rhs);
                        // this shouldn't fail as we just did the `op.is_ordering_comparison()`
                        // check
                        *op = op.flip_ordering_comparison().unwrap();
                        return self.visit_expr(expression);
                    }
                    (lhs, BinaryOperator::And, rhs) => {
                        self.visit_expr(lhs)?;
                        self.in_supported_position = true;
                        self.visit_expr(rhs)?;
                        self.in_supported_position = true;
                        return Ok(());
                    }
                    _ => self.in_supported_position = false,
                },
                Expr::In {
                    lhs,
                    rhs: InValue::List(exprs),
                    negated: false,
                } => match lhs.as_ref() {
                    // Case 1: Single-column IN (a IN (1,2,3))
                    Expr::Column(_)
                        if exprs
                            .iter()
                            .all(|e| matches!( e, Expr::Literal(lit) if !matches!(lit, Literal::Placeholder(_)))) =>
                    {
                        if self.autoparameterize_equals {
                            let exprs = mem::replace(
                                exprs,
                                std::iter::repeat_n(
                                    Expr::Literal(Literal::Placeholder(ItemPlaceholder::QuestionMark)),
                                    exprs.len(),
                                )
                                .collect(),
                            );
                            let num_exprs = exprs.len();
                            let start_index = self.param_index;
                            self.out
                                .extend(exprs.into_iter().enumerate().filter_map(move |(i, expr)| match expr {
                                    Expr::Literal(lit) => Some((i + start_index, lit)),
                                    // unreachable since we checked everything in the list is a
                                    // literal above, but best
                                    // not to panic regardless
                                    _ => None,
                                }));
                            self.param_index += num_exprs;
                        }
                        return Ok(());
                    }

                    // Case 2: Tuple IN ((a, b) IN ((1,2), (3,4)))
                    Expr::Row { .. }
                        if exprs.iter().all(|e| {
                            match e {
                                Expr::Row { exprs, .. } => exprs.iter().all(
                                    |e| matches!(e, Expr::Literal(lit) if !matches!(lit, Literal::Placeholder(_))),
                                ),
                                // FIXME(sqlparser): This is a special case because nom parses `(a, b) IN ((1,2))` as
                                // `(a, b) IN (1,2)` instead of `((a, b)) IN ((1,2))`.
                                // This case should be removed once migration to sqlparser is
                                // finalized.
                                // To fix this, we readd the removed parens before proceeding.
                                Expr::Literal(lit) if !matches!(lit, Literal::Placeholder(_)) => true,
                                _ => false,
                            }
                        }) =>
                    {
                        if self.autoparameterize_equals {
                            // FIXME(sqlparser): this handles the special case mentioned in the comment
                            // just before this
                            if !exprs.is_empty() && matches!(exprs[0], Expr::Literal(_)) {
                                let _ = mem::replace(
                                    exprs,
                                    vec![Expr::Row {
                                        exprs: exprs.clone(),
                                        explicit: false,
                                    }],
                                );
                            };

                            let exprs = mem::replace(
                                exprs,
                                exprs
                                    .iter()
                                    .map(|e| -> Result<Expr, ReadySetError> {
                                        match e {
                                            Expr::Row { exprs, .. } => Ok(Expr::Row {
                                                exprs: std::iter::repeat_n(
                                                    Expr::Literal(Literal::Placeholder(ItemPlaceholder::QuestionMark)),
                                                    exprs.len(),
                                                )
                                                .collect(),
                                                explicit: false,
                                            }),
                                            // ideally, this should be fully checked by the guard
                                            // above, unfortunately, it's not because of the workaround
                                            // mentioned above
                                            _ => unsupported!("Expected a ROW of placeholders"),
                                        }
                                    })
                                    .collect::<ReadySetResult<Vec<_>>>()?,
                            );

                            // same as the error above
                            let num_exprs: usize = exprs
                                .iter()
                                .map(|e| match e {
                                    Expr::Row { exprs, .. } => Ok(exprs.len()),
                                    _ => unsupported!("Expected a ROW of placeholders"),
                                })
                                .collect::<ReadySetResult<Vec<_>>>()?
                                .into_iter()
                                .sum();

                            let start_index = self.param_index;
                            let param_offset = 0;

                            self.out.extend(
                                exprs
                                    .into_iter()
                                    .flat_map(|e| match e {
                                        Expr::Row { exprs, .. } => exprs,
                                        _ => unreachable!(), // checked above
                                    })
                                    .enumerate()
                                    .map(|(i, e)| match e {
                                        Expr::Literal(lit) => Ok((start_index + param_offset + i, lit)),
                                        _ => unsupported!("Expected ROWs to only contain Literals"),
                                    })
                                    .collect::<ReadySetResult<Vec<_>>>()?,
                            );

                            self.param_index += num_exprs;
                        }
                        return Ok(());
                    }

                    _ => self.in_supported_position = false,
                },
                _ => self.in_supported_position = false,
            }
        }

        visit_mut::walk_expr(self, expression)?;
        self.in_supported_position = was_supported;
        Ok(())
    }

    fn visit_offset(&mut self, offset: &'ast mut Literal) -> Result<(), Self::Error> {
        if !matches!(offset, Literal::Placeholder(_))
            && self.autoparameterize_equals
            && self.query_depth <= 1
        {
            self.replace_literal(offset);
        }

        visit_mut::walk_offset(self, offset)
    }

    fn visit_limit_clause(
        &mut self,
        limit_clause: &'ast mut readyset_sql::ast::LimitClause,
    ) -> Result<(), Self::Error> {
        if self.visit_limit_clause {
            visit_mut::walk_limit_clause(self, limit_clause)
        } else {
            Ok(())
        }
    }
}

/// Walks through the query to determine whether the query has equals comparisons, range
/// comparisons, equals placeholders, and range placeholders in positions that support
/// autoparameterization.
#[derive(Default)]
struct AnalyzeLiteralsVisitor {
    contains_equal: bool,
    contains_range: bool,
    contains_equal_placeholder: bool,
    contains_range_placeholder: bool,
    query_depth: u8,
    in_supported_position: bool,
    has_aggregates: bool,
    /// Same gate as `AutoParameterizeVisitor::cap_predicates`: skipping cap predicates
    /// here prevents the mode-decision in `auto_parameterize_query` from biasing toward
    /// range-mode when the only range comparison in the query is an RN cap.
    cap_predicates: HashSet<Expr>,
}

impl<'ast> VisitorMut<'ast> for AnalyzeLiteralsVisitor {
    type Error = std::convert::Infallible;

    fn visit_select_statement(
        &mut self,
        select_statement: &'ast mut SelectStatement,
    ) -> Result<(), Self::Error> {
        self.query_depth = self.query_depth.saturating_add(1);
        visit_mut::walk_select_statement(self, select_statement)?;
        self.query_depth = self.query_depth.saturating_sub(1);
        Ok(())
    }

    fn visit_where_clause(&mut self, expression: &'ast mut Expr) -> Result<(), Self::Error> {
        // We can only support parameters in the WHERE clause of the top-level query, not any
        // subqueries it contains.
        self.in_supported_position = self.query_depth <= 1;
        self.visit_expr(expression)?;
        self.in_supported_position = false;
        Ok(())
    }

    fn visit_expr(&mut self, expression: &'ast mut Expr) -> Result<(), Self::Error> {
        let was_supported = self.in_supported_position;
        // Skip cap predicates so they don't count toward the range/equal classification
        // used to pick the autoparameterize mode below.
        if was_supported && self.cap_predicates.contains(expression) {
            return Ok(());
        }
        if was_supported {
            match expression {
                Expr::BinaryOp { lhs, op, rhs } => match (lhs.as_mut(), op, rhs.as_mut()) {
                    // Literals frozen by the exclusion pre-pass won't be parameterized, so they
                    // must not influence the equals/range mixing gate: fall through as unsupported.
                    (Expr::Column(_), BinaryOperator::Equal, Expr::Literal(lit))
                        if !matches!(lit, Literal::Preserved(_)) =>
                    {
                        self.contains_equal = true;
                        if let Literal::Placeholder(_) = lit {
                            self.contains_equal_placeholder = true;
                        }
                        return Ok(());
                    }
                    (Expr::Row { .. }, BinaryOperator::Equal, Expr::Row { exprs, .. }) => {
                        self.contains_equal = true;
                        for expr in exprs {
                            if let Expr::Literal(Literal::Placeholder(_)) = expr {
                                self.contains_equal_placeholder = true;
                            }
                        }
                        return Ok(());
                    }
                    (Expr::Column(_), op, Expr::Literal(lit))
                        if op.is_ordering_comparison()
                            && !matches!(lit, Literal::Preserved(_)) =>
                    {
                        self.contains_range = true;
                        if let Literal::Placeholder(_) = lit {
                            self.contains_range_placeholder = true;
                        }
                        return Ok(());
                    }
                    (Expr::Literal(_), BinaryOperator::Equal | BinaryOperator::NotEqual, Expr::Column(_)) => {
                        // for lit = col and lit != col, swap the equality first then revisit
                        mem::swap(lhs, rhs);
                        return self.visit_expr(expression);
                    }
                    (Expr::Literal(_), op, Expr::Column(_)) if op.is_ordering_comparison() => {
                        // for lit <ordering op> col, swap operands and flip operator, then revisit
                        mem::swap(lhs, rhs);
                        // this shouldn't fail as we just did the `op.is_ordering_comparison()`
                        // check
                        *op = op.flip_ordering_comparison().unwrap();
                        return self.visit_expr(expression);
                    }
                    (lhs, BinaryOperator::And, rhs) => {
                        self.visit_expr(lhs)?;
                        self.in_supported_position = true;
                        self.visit_expr(rhs)?;
                        self.in_supported_position = true;
                        return Ok(());
                    }
                    _ => self.in_supported_position = false,
                },
                Expr::Between { min, max, .. } => match (min.as_ref(), max.as_ref()) {
                    (Expr::Literal(lit), _) | (_, Expr::Literal(lit)) => {
                        self.contains_range = true;
                        if let Literal::Placeholder(_) = lit {
                            self.contains_range_placeholder = true;
                        }
                        return Ok(());
                    }
                    _ => self.in_supported_position = false,
                },
                Expr::In {
                    lhs,
                    rhs: InValue::List(exprs),
                    negated: false,
                } if exprs.iter().all(|e| {
                    match e {
                        // Case 1: Single-column IN (a IN (1,2,3))
                        Expr::Literal(lit) if !matches!(lit, Literal::Placeholder(_)) => true,
                        // Case 2: Multi-column IN ((a,b) IN ((1,2), (3,4)))
                        Expr::Row { exprs, .. }
                            if exprs.iter().all(
                                |inner| matches!(inner, Expr::Literal(lit) if !matches!(lit, Literal::Placeholder(_))),
                            ) =>
                        {
                            true
                        }
                        _ => false,
                    }
                }) && !self.has_aggregates =>
                {
                    match lhs.as_ref() {
                        Expr::Column(_) | Expr::Row { .. } => {
                            self.contains_equal = true;
                            return Ok(());
                        }
                        _ => self.in_supported_position = false,
                    }
                }
                _ => self.in_supported_position = false,
            }
        }

        visit_mut::walk_expr(self, expression)?;
        self.in_supported_position = was_supported;
        Ok(())
    }

    fn visit_offset(&mut self, offset: &'ast mut Literal) -> Result<(), Self::Error> {
        if !matches!(offset, Literal::Placeholder(_)) {
            self.contains_equal = true;
        }

        visit_mut::walk_offset(self, offset)
    }
}

/// Replace all literals in positions we support with placeholders, extracting the literals as
/// parameters in a parameter list of (placeholder position, value).
pub fn auto_parameterize_query(
    query: &mut SelectStatement,
    prev: Vec<(usize, Literal)>,
    server_supports_mixed_comparisons: bool,
    visit_limit_clause: bool,
) -> ReadySetResult<Vec<(usize, Literal)>> {
    let cap_predicates = collect_top_level_caps(query);

    // Don't try to auto-parameterize equal-queries that already contain range params for now, since
    // we don't yet allow mixing range and equal parameters in the same query
    let mut visitor = AnalyzeLiteralsVisitor {
        cap_predicates: cap_predicates.clone(),
        ..Default::default()
    };
    visitor.visit_select_statement(query).unwrap();

    let (autoparameterize_equals, autoparameterize_ranges) = if server_supports_mixed_comparisons {
        (true, true)
    } else if !visitor.contains_range {
        // If a query contains no range comparisons in positions that support
        // autoparameterization, we can just proceed with autoparameterizing equals
        // comparisons
        (true, false)
    } else if !visitor.contains_equal {
        // If a query contains no equals comparisons in positions that support
        // autoparameterization, we can just proceed with autoparameterizing range
        // comparisons
        (false, true)
    } else {
        // If we're here, it means the query has both range and equals comparisons in
        // positions that support autoparameterization

        match (
            visitor.contains_equal_placeholder,
            visitor.contains_range_placeholder,
        ) {
            // If the query contains only equals placeholders, we try to autoparameterize the rest
            // of the equals comparisons in the query
            (true, false) => (true, false),
            // If the query contains only range placeholders, we try to autoparameterize the rest
            // of the range comparisons in the query
            (false, true) => (false, true),
            // If the query contains no placeholderse, we try to autoparameterize the equals
            // comparisons only, since we don't support mixed comparisons yet
            (false, false) => (true, false),
            // If the query contains equal and range placeholders, we bail, since we don't support
            // mixed comparisons yet
            (true, true) => return Ok(vec![]),
        }
    };

    let mut visitor = AutoParameterizeVisitor {
        autoparameterize_equals,
        autoparameterize_ranges,
        param_index: prev.len(),
        out: prev,
        visit_limit_clause,
        cap_predicates,
        ..Default::default()
    };
    visitor.visit_select_statement(query)?;
    Ok(visitor.out)
}

#[derive(Default)]
struct FullyParameterizeVisitor {
    param_idx: usize,
    out: Vec<(usize, Literal)>,
}

impl<'ast> VisitorMut<'ast> for FullyParameterizeVisitor {
    type Error = ReadySetError;

    fn visit_literal(&mut self, literal: &mut Literal) -> Result<(), Self::Error> {
        if *literal == Literal::Null {
            return Ok(());
        }
        if !matches!(literal, Literal::Placeholder(..)) {
            let val = mem::replace(literal, Literal::Placeholder(ItemPlaceholder::QuestionMark));
            self.out.push((self.param_idx, val));
        }
        self.param_idx += 1;
        Ok(())
    }
}

/// Replace all literals with placeholders, extracting the literals as parameters in a parameter
/// list of (placeholder position, value).
pub fn fully_parameterize_query(
    query: &mut SelectStatement,
) -> ReadySetResult<Vec<(usize, Literal)>> {
    let mut visitor = FullyParameterizeVisitor::default();
    visitor.visit_select_statement(query)?;
    Ok(visitor.out)
}

#[cfg(test)]
mod tests {
    use readyset_sql::{Dialect, DialectDisplay};

    use super::*;

    fn try_parse_select_statement(q: &str, dialect: Dialect) -> Result<SelectStatement, String> {
        readyset_sql_parsing::parse_select(dialect, q).map_err(|e| e.to_string())
    }

    fn parse_select_statement(q: &str, dialect: Dialect) -> SelectStatement {
        try_parse_select_statement(q, dialect).unwrap()
    }

    fn test_auto_parameterize(
        query: &str,
        expected_query: &str,
        // These are parameters that are expected to have been added by the autoparameterization
        // rewrite pass
        expected_added_parameters: Vec<(usize, Literal)>,
        dialect: readyset_sql::Dialect,
        server_supports_mixed_comparisons: bool,
    ) {
        let mut query = parse_select_statement(query, dialect);
        let expected = parse_select_statement(expected_query, dialect);
        let res = auto_parameterize_query(
            &mut query,
            Vec::new(),
            server_supports_mixed_comparisons,
            true,
        )
        .unwrap();
        assert_eq!(
            query,
            expected,
            "\n  left: {}\n right: {}",
            query.display(dialect),
            expected.display(dialect),
        );
        assert_eq!(res, expected_added_parameters);
    }

    fn test_auto_parameterize_mysql(
        query: &str,
        expected_query: &str,
        // These are parameters that are expected to have been added by the autoparameterization
        // rewrite pass
        expected_added_parameters: Vec<(usize, Literal)>,
    ) {
        test_auto_parameterize(
            query,
            expected_query,
            expected_added_parameters,
            readyset_sql::Dialect::MySQL,
            false,
        )
    }

    fn test_auto_parameterize_postgres(
        query: &str,
        expected_query: &str,
        // These are parameters that are expected to have been added by the autoparameterization
        // rewrite pass
        expected_added_parameters: Vec<(usize, Literal)>,
    ) {
        test_auto_parameterize(
            query,
            expected_query,
            expected_added_parameters,
            readyset_sql::Dialect::PostgreSQL,
            false,
        )
    }

    #[test]
    fn no_literals() {
        test_auto_parameterize_mysql("SELECT * FROM users", "SELECT * FROM users", vec![]);
        test_auto_parameterize_postgres("SELECT * FROM users", "SELECT * FROM users", vec![]);
    }

    #[test]
    fn simple_parameter() {
        test_auto_parameterize_mysql(
            "SELECT id FROM users WHERE id = 1",
            "SELECT id FROM users WHERE id = ?",
            vec![(0, 1.into())],
        );
    }

    #[test]
    fn and_parameters() {
        test_auto_parameterize_mysql(
            "SELECT id FROM users WHERE id = 1 AND name = \"bob\"",
            "SELECT id FROM users WHERE id = ? AND name = ?",
            vec![(0, 1.into()), (1, "bob".into())],
        );
    }

    /// A literal frozen by the autoparam-exclusion machinery (wrapped in `Literal::Preserved`, as
    /// the pre-pass produces) is kept inline by `auto_parameterize_query` while sibling literals
    /// are still parameterized, and the final sweep unwraps the marker to a plain literal.
    #[test]
    fn autoparam_keeps_preserved_literal_inline() {
        struct FreezeStrings;
        impl<'ast> VisitorMut<'ast> for FreezeStrings {
            type Error = std::convert::Infallible;
            fn visit_literal(&mut self, literal: &'ast mut Literal) -> Result<(), Self::Error> {
                if matches!(literal, Literal::String(_)) {
                    let inner = mem::replace(literal, Literal::Null);
                    *literal = Literal::Preserved(Box::new(inner));
                }
                Ok(())
            }
        }

        let dialect = Dialect::MySQL;
        let mut query = parse_select_statement(
            "SELECT id FROM users WHERE name = \"frozen\" AND id = 5",
            dialect,
        );
        FreezeStrings.visit_select_statement(&mut query).unwrap();

        let params = auto_parameterize_query(&mut query, Vec::new(), false, true).unwrap();
        unwrap_all_preserved(&mut query);

        // `name` stays an inline constant; `id` is autoparameterized. Equality also proves no
        // `Literal::Preserved` survived the sweep (it would not equal the plain `String`).
        let expected = parse_select_statement(
            "SELECT id FROM users WHERE name = \"frozen\" AND id = ?",
            dialect,
        );
        assert_eq!(
            query,
            expected,
            "\n  left: {}\n right: {}",
            query.display(dialect),
            expected.display(dialect),
        );
        assert_eq!(params, vec![(0, 5.into())]);
    }

    /// A frozen literal in a range position is likewise preserved, and (via the analyze-gate
    /// guard) does not flip the query into the unsupported mixed-comparison bail.
    #[test]
    fn autoparam_keeps_preserved_range_literal_inline() {
        struct FreezeIntoPreserved;
        impl<'ast> VisitorMut<'ast> for FreezeIntoPreserved {
            type Error = std::convert::Infallible;
            fn visit_literal(&mut self, literal: &'ast mut Literal) -> Result<(), Self::Error> {
                // Freeze the range bound `10`, leaving the equality literal `5` to parameterize.
                if matches!(literal, Literal::Integer(10) | Literal::UnsignedInteger(10)) {
                    let inner = mem::replace(literal, Literal::Null);
                    *literal = Literal::Preserved(Box::new(inner));
                }
                Ok(())
            }
        }

        let dialect = Dialect::MySQL;
        let mut query =
            parse_select_statement("SELECT id FROM users WHERE id = 5 AND age > 10", dialect);
        FreezeIntoPreserved
            .visit_select_statement(&mut query)
            .unwrap();

        let params = auto_parameterize_query(&mut query, Vec::new(), false, true).unwrap();
        unwrap_all_preserved(&mut query);

        let expected =
            parse_select_statement("SELECT id FROM users WHERE id = ? AND age > 10", dialect);
        assert_eq!(
            query,
            expected,
            "\n  left: {}\n right: {}",
            query.display(dialect),
            expected.display(dialect),
        );
        assert_eq!(params, vec![(0, 5.into())]);
    }

    #[test]
    fn existing_param_before() {
        test_auto_parameterize_mysql(
            "SELECT id FROM users WHERE x = ? AND id = 1 AND name = \"bob\"",
            "SELECT id FROM users WHERE x = ? AND id = ? AND name = ?",
            vec![(1, 1.into()), (2, "bob".into())],
        );
    }

    #[test]
    fn existing_param_after() {
        test_auto_parameterize_mysql(
            "SELECT id FROM users WHERE id = 1 AND name = \"bob\" AND x = ?",
            "SELECT id FROM users WHERE id = ? AND name = ? AND x = ?",
            vec![(0, 1.into()), (1, "bob".into())],
        );
    }

    #[test]
    fn existing_param_between() {
        test_auto_parameterize_mysql(
            "SELECT id FROM users WHERE id = 1 AND x = ? AND name = \"bob\"",
            "SELECT id FROM users WHERE id = ? AND x = ? AND name = ?",
            vec![(0, 1.into()), (2, "bob".into())],
        );
    }

    #[test]
    fn literal_in_or() {
        test_auto_parameterize_mysql(
            "SELECT id FROM users WHERE (id = 1 OR id = 2) AND name = \"bob\"",
            "SELECT id FROM users WHERE (id = 1 OR id = 2) AND name = ?",
            vec![(0, "bob".into())],
        )
    }

    #[test]
    fn literal_in_subquery_where() {
        test_auto_parameterize_mysql(
            "SELECT id FROM users JOIN (SELECT id FROM users WHERE id = 1) s ON users.id = s.id WHERE id = 1",
            "SELECT id FROM users JOIN (SELECT id FROM users WHERE id = 1) s ON users.id = s.id WHERE id = ?",
            vec![(0, 1.into())],
        )
    }

    #[test]
    fn literal_in_field() {
        test_auto_parameterize_mysql(
            "SELECT id + 1 FROM users WHERE id = 1",
            "SELECT id + 1 FROM users WHERE id = ?",
            vec![(0, 1.into())],
        )
    }

    #[test]
    fn row_in_predicate() {
        // FIXME(sqlparser): Read the FIXME above, the expected query gets parsed incorrectly
        // because of nom, but the actual query itself works as expected because of the hardcoded
        // check above
        // test_auto_parameterize_mysql(
        //     "SELECT * FROM t WHERE (a, b) IN ((1, 10))",
        //     "SELECT * FROM t WHERE (a, b) IN ((?, ?))",
        //     vec![(0, 1.into()), (1, 10.into())],
        // );

        test_auto_parameterize_mysql(
            "SELECT * FROM t WHERE (a, b) IN ((1, 'str'),(2, 'string'))",
            "SELECT * FROM t WHERE (a, b) IN ((?, ?), (?, ?))",
            vec![
                (0, 1.into()),
                (1, "str".into()),
                (2, 2.into()),
                (3, "string".into()),
            ],
        );
    }

    #[test]
    fn literal_in_in_rhs() {
        test_auto_parameterize_mysql(
            "select hashtags.* from hashtags inner join invites_hashtags on hashtags.id = invites_hashtags.hashtag_id where invites_hashtags.invite_id in (10,20,31)",
            "select hashtags.* from hashtags inner join invites_hashtags on hashtags.id = invites_hashtags.hashtag_id where invites_hashtags.invite_id in (?,?,?)",
            vec![(0, 10.into()), (1, 20.into()), (2, 31.into())],
        );
    }

    #[test]
    fn mixed_in_with_equality() {
        test_auto_parameterize_mysql(
            "SELECT id FROM users WHERE id in (1, 2) AND name = 'bob'",
            "SELECT id FROM users WHERE id in (?, ?) AND name = ?",
            vec![(0, 1.into()), (1, 2.into()), (2, "bob".into())],
        );
    }

    #[test]
    fn equal_in_equal() {
        test_auto_parameterize_mysql(
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
        test_auto_parameterize_mysql(
            "SELECT count(*) FROM users WHERE id = 1 AND x IN (1, 2)",
            "SELECT count(*) FROM users WHERE id = ? AND x IN (?, ?)",
            vec![(0, 1.into()), (1, 1.into()), (2, 2.into())],
        );
    }

    #[test]
    fn literal_equals_column() {
        test_auto_parameterize_mysql(
            "SELECT * FROM users WHERE 1 = id",
            "SELECT * FROM users WHERE id = ?",
            vec![(0, 1.into())],
        );
    }

    #[test]
    fn literal_not_equals_column() {
        test_auto_parameterize_mysql(
            "SELECT * FROM users WHERE 1 != id",
            "SELECT * FROM users WHERE id != 1",
            vec![],
        );
    }

    #[test]
    fn existing_range_param() {
        test_auto_parameterize_mysql(
            "SELECT * FROM posts WHERE id = 1 AND score > ?",
            "SELECT * FROM posts WHERE id = 1 AND score > ?",
            vec![],
        )
    }

    #[test]
    fn offset() {
        test_auto_parameterize_mysql(
            "SELECT * FROM posts WHERE id = 1 ORDER BY SCORE ASC LIMIT 3 OFFSET 6",
            "SELECT * FROM posts WHERE id = ? ORDER BY SCORE ASC LIMIT 3 OFFSET ?",
            vec![(0, 1.into()), (1, 6.into())],
        );
    }

    #[test]
    fn constant_filter_with_param_betwen() {
        test_auto_parameterize_mysql(
            "SELECT * FROM posts WHERE id = 1 AND created_at BETWEEN ? and ?",
            "SELECT * FROM posts WHERE id = 1 AND created_at BETWEEN ? and ?",
            vec![],
        );
    }

    #[test]
    fn range_query_literals() {
        test_auto_parameterize_mysql(
            "SELECT * FROM posts WHERE score > 0 AND score < 10",
            "SELECT * FROM posts WHERE score > ? AND score < ?",
            vec![(0, 0.into()), (1, 10.into())],
        );
    }

    #[test]
    fn range_query_literals_inclusive() {
        test_auto_parameterize_mysql(
            "SELECT * FROM posts WHERE score >= 0 AND score <= 10",
            "SELECT * FROM posts WHERE score >= ? AND score <= ?",
            vec![(0, 0.into()), (1, 10.into())],
        );
    }

    #[test]
    fn range_query_literals_inclusive_exclusive() {
        test_auto_parameterize_mysql(
            "SELECT * FROM posts WHERE score >= 0 AND score < 10",
            "SELECT * FROM posts WHERE score >= ? AND score < ?",
            vec![(0, 0.into()), (1, 10.into())],
        );
    }

    #[test]
    fn range_query_literals_exclusive_inclusive() {
        test_auto_parameterize_mysql(
            "SELECT * FROM posts WHERE score > 0 AND score <= 10",
            "SELECT * FROM posts WHERE score > ? AND score <= ?",
            vec![(0, 0.into()), (1, 10.into())],
        );
    }

    #[test]
    fn equals_then_range() {
        test_auto_parameterize_mysql(
            "SELECT * FROM posts WHERE id = 1 AND score > 0 AND score < 10",
            "SELECT * FROM posts WHERE id = ? AND score > 0 AND score < 10",
            vec![(0, 1.into())],
        );
    }

    #[test]
    fn range_then_equals() {
        test_auto_parameterize_mysql(
            "SELECT * FROM posts WHERE score > 0 AND score < 10 AND id = 1",
            "SELECT * FROM posts WHERE score > 0 AND score < 10 AND id = ?",
            vec![(0, 1.into())],
        );
    }

    #[test]
    fn range_with_or() {
        test_auto_parameterize_mysql(
            "SELECT * FROM posts WHERE score > 0 OR score < 10",
            "SELECT * FROM posts WHERE score > 0 OR score < 10",
            vec![],
        );
    }

    #[test]
    fn nested_range() {
        test_auto_parameterize_mysql(
            "SELECT * FROM posts WHERE id in (SELECT id FROM posts WHERE score > 0 AND score < 10)",
            "SELECT * FROM posts WHERE id in (SELECT id FROM posts WHERE score > 0 AND score < 10)",
            vec![],
        );
    }

    #[test]
    fn less_than() {
        test_auto_parameterize_mysql(
            "SELECT * FROM posts WHERE id < 10",
            "SELECT * FROM posts WHERE id < ?",
            vec![(0, 10.into())],
        );
    }

    #[test]
    fn less_than_equal() {
        test_auto_parameterize_mysql(
            "SELECT * FROM posts WHERE id <= 10",
            "SELECT * FROM posts WHERE id <= ?",
            vec![(0, 10.into())],
        );
    }

    #[test]
    fn greater_than() {
        test_auto_parameterize_mysql(
            "SELECT * FROM posts WHERE id > 10",
            "SELECT * FROM posts WHERE id > ?",
            vec![(0, 10.into())],
        );
    }

    #[test]
    fn greater_than_equal() {
        test_auto_parameterize_mysql(
            "SELECT * FROM posts WHERE id >= 10",
            "SELECT * FROM posts WHERE id >= ?",
            vec![(0, 10.into())],
        );
    }

    #[test]
    fn range_with_pre_existing_equals_param() {
        test_auto_parameterize_mysql(
            "SELECT * FROM posts WHERE id = ? AND views > 10",
            "SELECT * FROM posts WHERE id = ? AND views > 10",
            vec![],
        );
    }

    #[test]
    fn equals_with_pre_existing_range_param() {
        test_auto_parameterize_mysql(
            "SELECT * FROM posts WHERE id = 10 AND views > ? AND date > 10",
            "SELECT * FROM posts WHERE id = 10 AND views > ? AND date > ?",
            vec![(1, 10.into())],
        );
    }

    #[test]
    fn ranges_only() {
        test_auto_parameterize_mysql(
            "SELECT * FROM posts WHERE id > 10 AND views > 2",
            "SELECT * FROM posts WHERE id > ? AND views > ?",
            vec![(0, 10.into()), (1, 2.into())],
        );
    }

    #[test]
    fn some_equals() {
        test_auto_parameterize_mysql(
            "SELECT * FROM posts WHERE id = ? AND views = 10",
            "SELECT * FROM posts WHERE id = ? AND views = ?",
            vec![(1, 10.into())],
        );
    }

    #[test]
    fn some_equals_with_ranges() {
        test_auto_parameterize_mysql(
            "SELECT * FROM posts WHERE id = ? AND date = 10 AND views > 10",
            "SELECT * FROM posts WHERE id = ? AND date = ? AND views > 10",
            vec![(1, 10.into())],
        );
    }

    #[test]
    fn supported_equals_with_unsupported_ranges() {
        test_auto_parameterize_mysql(
            "SELECT id FROM users JOIN (SELECT id FROM users WHERE id < 1) s ON users.id = s.id WHERE id = 1",
            "SELECT id FROM users JOIN (SELECT id FROM users WHERE id < 1) s ON users.id = s.id WHERE id = ?",
            vec![(0, 1.into())],
        )
    }

    #[test]
    fn supported_ranges_with_unsupported_equals() {
        test_auto_parameterize_mysql(
            "SELECT id FROM users JOIN (SELECT id FROM users WHERE id = 1) s ON users.id = s.id WHERE id < 1",
            "SELECT id FROM users JOIN (SELECT id FROM users WHERE id = 1) s ON users.id = s.id WHERE id < ?",
            vec![(0, 1.into())],
        )
    }

    #[test]
    fn supported_ranges_and_equals_with_unsupported_equals() {
        test_auto_parameterize_mysql(
            "SELECT id FROM users JOIN (SELECT id FROM users WHERE id = 1) s ON users.id = s.id WHERE id = 1 AND age > 21",
            "SELECT id FROM users JOIN (SELECT id FROM users WHERE id = 1) s ON users.id = s.id WHERE id = ? AND age > 21",
            vec![(0, 1.into())],
        )
    }

    #[test]
    fn supported_ranges_and_equals_with_unsupported_ranges() {
        test_auto_parameterize_mysql(
            "SELECT id FROM users JOIN (SELECT id FROM users WHERE age > 50) s ON users.id = s.id WHERE id = 1 AND age > 21",
            "SELECT id FROM users JOIN (SELECT id FROM users WHERE age > 50) s ON users.id = s.id WHERE id = ? AND age > 21",
            vec![(0, 1.into())],
        )
    }

    mod mixed_comparisons {
        use super::*;

        fn test_auto_parameterize_mysql(
            query: &str,
            expected_query: &str,
            // These are parameters that are expected to have been added by the
            // autoparameterization rewrite pass
            expected_added_parameters: Vec<(usize, Literal)>,
        ) {
            test_auto_parameterize(
                query,
                expected_query,
                expected_added_parameters,
                readyset_sql::Dialect::MySQL,
                true,
            )
        }

        #[test]
        fn some_equals_with_ranges() {
            test_auto_parameterize_mysql(
                "SELECT * FROM posts WHERE id = ? AND date = 10 AND views > 9",
                "SELECT * FROM posts WHERE id = ? AND date = ? AND views > ?",
                vec![(1, 10.into()), (2, 9.into())],
            );
        }

        #[test]
        fn range_with_pre_existing_equals_param() {
            test_auto_parameterize_mysql(
                "SELECT * FROM posts WHERE id = ? AND views > 10",
                "SELECT * FROM posts WHERE id = ? AND views > ?",
                vec![(1, 10.into())],
            );
        }

        #[test]
        fn equals_with_pre_existing_range_param() {
            test_auto_parameterize_mysql(
                "SELECT * FROM posts WHERE id = 10 AND views > ? AND date > 9",
                "SELECT * FROM posts WHERE id = ? AND views > ? AND date > ?",
                vec![(0, 10.into()), (2, 9.into())],
            );
        }

        #[test]
        fn supported_equals_with_unsupported_ranges() {
            test_auto_parameterize_mysql(
                "SELECT id FROM users JOIN (SELECT id FROM users WHERE id < 1) s ON users.id = s.id WHERE id = 1",
                "SELECT id FROM users JOIN (SELECT id FROM users WHERE id < 1) s ON users.id = s.id WHERE id = ?",
                vec![(0, 1.into())],
            )
        }

        #[test]
        fn supported_ranges_with_unsupported_equals() {
            test_auto_parameterize_mysql(
                "SELECT id FROM users JOIN (SELECT id FROM users WHERE id = 1) s ON users.id = s.id WHERE id < 1",
                "SELECT id FROM users JOIN (SELECT id FROM users WHERE id = 1) s ON users.id = s.id WHERE id < ?",
                vec![(0, 1.into())],
            )
        }

        #[test]
        fn supported_ranges_and_equals_with_unsupported_equals() {
            test_auto_parameterize_mysql(
                "SELECT id FROM users JOIN (SELECT id FROM users WHERE id = 1) s ON users.id = s.id WHERE id = 1 AND age > 21",
                "SELECT id FROM users JOIN (SELECT id FROM users WHERE id = 1) s ON users.id = s.id WHERE id = ? AND age > ?",
                vec![(0, 1.into()), (1, 21.into())],
            )
        }

        #[test]
        fn supported_ranges_and_equals_with_unsupported_ranges() {
            test_auto_parameterize_mysql(
                "SELECT id FROM users JOIN (SELECT id FROM users WHERE age > 50) s ON users.id = s.id WHERE id = 1 AND age > 21",
                "SELECT id FROM users JOIN (SELECT id FROM users WHERE age > 50) s ON users.id = s.id WHERE id = ? AND age > ?",
                vec![(0, 1.into()), (1, 21.into())],
            )
        }

        #[test]
        fn supported_row_equality_predicates() {
            test_auto_parameterize_mysql(
                "SELECT id FROM users WHERE (name, age) = ('Bob', 27)",
                "SELECT id FROM users WHERE (name, age) = (?, ?)",
                vec![(0, "Bob".into()), (1, 27.into())],
            )
        }
    }

    /// Row-number cap predicates (`rn op K` where `rn` aliases a `ROW_NUMBER()` projection)
    /// must keep their integer literal intact: CBJR reads that literal as a cardinality
    /// signal. These tests pin to sqlparser-only on the MySQL dialect: nom-sql doesn't
    /// support `ROW_NUMBER`, and MySQL's `?` placeholder shape is what the
    /// auto-parameterizer emits.
    mod row_number_caps {
        use readyset_sql_parsing::{ParsingPreset, parse_select_with_config};

        use super::*;

        fn parse_mysql(q: &str) -> SelectStatement {
            parse_select_with_config(ParsingPreset::OnlySqlparser, Dialect::MySQL, q).unwrap()
        }

        fn test_auto_parameterize_rn(
            query: &str,
            expected_query: &str,
            expected_added_parameters: Vec<(usize, Literal)>,
        ) {
            let mut query = parse_mysql(query);
            let expected = parse_mysql(expected_query);
            let res = auto_parameterize_query(&mut query, Vec::new(), false, true).unwrap();
            assert_eq!(
                query,
                expected,
                "\n  left: {}\n right: {}",
                query.display(Dialect::MySQL),
                expected.display(Dialect::MySQL),
            );
            assert_eq!(res, expected_added_parameters);
        }

        #[test]
        fn cap_predicate_literal_preserved() {
            // The outer WHERE references `rn` which resolves to the inner ROW_NUMBER()
            // projection. The literal 10 is the cardinality cap and must survive.
            test_auto_parameterize_rn(
                "SELECT id FROM (SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM t) s \
                 WHERE rn <= 10",
                "SELECT id FROM (SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM t) s \
                 WHERE rn <= 10",
                vec![],
            );
        }

        #[test]
        fn cap_and_regular_filter_mixed() {
            // The cap stays put; the unrelated `id = 5` equality parameterizes normally.
            test_auto_parameterize_rn(
                "SELECT id FROM (SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM t) s \
                 WHERE rn <= 10 AND id = 5",
                "SELECT id FROM (SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM t) s \
                 WHERE rn <= 10 AND id = ?",
                vec![(0, 5.into())],
            );
        }

        #[test]
        fn flipped_cap_literal_left_preserved() {
            // `10 >= rn` is the swap-arm shape; the dual-orientation cap set must catch
            // the post-swap expression on the recursive visit.
            test_auto_parameterize_rn(
                "SELECT id FROM (SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM t) s \
                 WHERE 10 >= rn",
                "SELECT id FROM (SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM t) s \
                 WHERE 10 >= rn",
                vec![],
            );
        }

        #[test]
        fn flipped_cap_equality_literal_left_preserved() {
            // `1 = rn` exercises the equality swap arm (Equal/NotEqual branch).
            test_auto_parameterize_rn(
                "SELECT id FROM (SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM t) s \
                 WHERE 1 = rn",
                "SELECT id FROM (SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM t) s \
                 WHERE 1 = rn",
                vec![],
            );
        }

        #[test]
        fn synthetic_underscore_rn_preserved() {
            // The synthetic `__rn` alias produced by TOP-K rewrite must also be recognized.
            test_auto_parameterize_rn(
                "SELECT id FROM (SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS __rn FROM t) s \
                 WHERE __rn <= 100",
                "SELECT id FROM (SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS __rn FROM t) s \
                 WHERE __rn <= 100",
                vec![],
            );
        }

        #[test]
        fn qualified_cap_reference_preserved() {
            // `s.rn <= 5` resolves via the FROM-alias `s` to the inner subquery's RN
            // projection. The cap literal stays intact.
            test_auto_parameterize_rn(
                "SELECT id FROM (SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM t) s \
                 WHERE s.rn <= 5",
                "SELECT id FROM (SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM t) s \
                 WHERE s.rn <= 5",
                vec![],
            );
        }

        #[test]
        fn non_rn_range_filter_still_parameterized() {
            // Negative control: a plain integer column with the same operator/literal
            // shape still autoparameterizes — the gate is targeted to RN caps only.
            test_auto_parameterize_rn(
                "SELECT id FROM posts WHERE id <= 10",
                "SELECT id FROM posts WHERE id <= ?",
                vec![(0, 10.into())],
            );
        }
    }
}
