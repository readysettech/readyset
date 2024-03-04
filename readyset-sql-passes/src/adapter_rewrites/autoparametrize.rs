use std::{iter, mem};

use nom_sql::analysis::visit_mut::{self, VisitorMut};
use nom_sql::{BinaryOperator, Expr, InValue, ItemPlaceholder, Literal, SelectStatement};

#[derive(Default)]
struct AutoParametrizeVisitor {
    autoparametrize_equals: bool,
    autoparametrize_ranges: bool,
    out: Vec<(usize, Literal)>,
    has_aggregates: bool,
    in_supported_position: bool,
    param_index: usize,
    query_depth: u8,
}

impl AutoParametrizeVisitor {
    fn replace_literal(&mut self, literal: &mut Literal) {
        let literal = mem::replace(literal, Literal::Placeholder(ItemPlaceholder::QuestionMark));
        self.out.push((self.param_index, literal));
        self.param_index += 1;
    }
}

impl<'ast> VisitorMut<'ast> for AutoParametrizeVisitor {
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
        if was_supported {
            match expression {
                Expr::BinaryOp {
                    lhs: box Expr::Column(_),
                    op: BinaryOperator::Equal,
                    rhs: box Expr::Literal(Literal::Placeholder(_)),
                } => {}
                Expr::BinaryOp {
                    lhs: box Expr::Column(_),
                    op,
                    rhs: box Expr::Literal(Literal::Placeholder(_)),
                } if op.is_ordering_comparison() => {}
                Expr::BinaryOp {
                    lhs: box Expr::Column(_),
                    op: BinaryOperator::Equal,
                    rhs: box Expr::Literal(lit),
                } => {
                    if self.autoparametrize_equals {
                        self.replace_literal(lit);
                    }

                    return Ok(());
                }
                Expr::BinaryOp {
                    lhs: box Expr::Column(_),
                    op,
                    rhs: box Expr::Literal(lit),
                } if op.is_ordering_comparison() => {
                    if self.autoparametrize_ranges {
                        self.replace_literal(lit);
                    }

                    return Ok(());
                }
                Expr::BinaryOp {
                    lhs: lhs @ box Expr::Literal(_),
                    op: BinaryOperator::Equal,
                    rhs: rhs @ box Expr::Column(_),
                } => {
                    // for lit = col, swap the equality first then revisit
                    mem::swap(lhs, rhs);
                    return self.visit_expr(expression);
                }
                Expr::BinaryOp {
                    lhs: lhs @ box Expr::Literal(_),
                    op,
                    rhs: rhs @ box Expr::Column(_),
                } if op.is_ordering_comparison() => {
                    // for lit = col, swap the inequality first then revisit
                    mem::swap(lhs, rhs);
                    return self.visit_expr(expression);
                }
                Expr::In {
                    lhs: box Expr::Column(_),
                    rhs: InValue::List(exprs),
                    negated: false,
                } if exprs.iter().all(|e| {
                    matches!(
                        e,
                        Expr::Literal(lit) if !matches!(lit, Literal::Placeholder(_))
                    )
                }) && !self.has_aggregates =>
                {
                    if self.autoparametrize_equals {
                        let exprs = mem::replace(
                            exprs,
                            iter::repeat(Expr::Literal(Literal::Placeholder(
                                ItemPlaceholder::QuestionMark,
                            )))
                            .take(exprs.len())
                            .collect(),
                        );
                        let num_exprs = exprs.len();
                        let start_index = self.param_index;
                        self.out.extend(exprs.into_iter().enumerate().filter_map(
                            move |(i, expr)| match expr {
                                Expr::Literal(lit) => Some((i + start_index, lit)),
                                // unreachable since we checked everything in the list is a literal
                                // above, but best not to panic regardless
                                _ => None,
                            },
                        ));
                        self.param_index += num_exprs;
                    }
                    return Ok(());
                }
                Expr::BinaryOp {
                    lhs,
                    op: BinaryOperator::And,
                    rhs,
                } => {
                    self.visit_expr(lhs.as_mut())?;
                    self.in_supported_position = true;
                    self.visit_expr(rhs.as_mut())?;
                    self.in_supported_position = true;
                    return Ok(());
                }
                _ => self.in_supported_position = false,
            }
        }

        visit_mut::walk_expr(self, expression)?;
        self.in_supported_position = was_supported;
        Ok(())
    }

    fn visit_offset(&mut self, offset: &'ast mut Literal) -> Result<(), Self::Error> {
        if !matches!(offset, Literal::Placeholder(_)) && self.autoparametrize_equals {
            self.replace_literal(offset);
        }

        visit_mut::walk_offset(self, offset)
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
}

impl<'ast> VisitorMut<'ast> for AnalyzeLiteralsVisitor {
    type Error = !;

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
        if was_supported {
            match expression {
                Expr::BinaryOp {
                    lhs: box Expr::Column(_),
                    op: BinaryOperator::Equal,
                    rhs: box Expr::Literal(lit),
                } => {
                    self.contains_equal = true;

                    if let Literal::Placeholder(_) = lit {
                        self.contains_equal_placeholder = true;
                    }

                    return Ok(());
                }
                Expr::BinaryOp {
                    lhs: box Expr::Column(_),
                    op,
                    rhs: box Expr::Literal(lit),
                } if op.is_ordering_comparison() => {
                    self.contains_range = true;

                    if let Literal::Placeholder(_) = lit {
                        self.contains_range_placeholder = true;
                    }

                    return Ok(());
                }
                Expr::Between {
                    min: box Expr::Literal(lit),
                    ..
                }
                | Expr::Between {
                    max: box Expr::Literal(lit),
                    ..
                } => {
                    self.contains_range = true;

                    if let Literal::Placeholder(_) = lit {
                        self.contains_range_placeholder = true;
                    }

                    return Ok(());
                }
                Expr::BinaryOp {
                    lhs: lhs @ box Expr::Literal(_),
                    op: BinaryOperator::Equal,
                    rhs: rhs @ box Expr::Column(_),
                } => {
                    // for lit = col, swap the equality first then revisit
                    mem::swap(lhs, rhs);
                    return self.visit_expr(expression);
                }
                Expr::BinaryOp {
                    lhs: lhs @ box Expr::Literal(_),
                    op,
                    rhs: rhs @ box Expr::Column(_),
                } if op.is_ordering_comparison() => {
                    // for lit = col, swap the equality first then revisit
                    mem::swap(lhs, rhs);
                    return self.visit_expr(expression);
                }
                Expr::In {
                    lhs: box Expr::Column(_),
                    rhs: InValue::List(exprs),
                    negated: false,
                } if exprs.iter().all(|e| {
                    matches!(
                        e,
                        Expr::Literal(lit) if !matches!(lit, Literal::Placeholder(_))
                    )
                }) && !self.has_aggregates =>
                {
                    self.contains_equal = true;
                    return Ok(());
                }
                Expr::BinaryOp {
                    lhs,
                    op: BinaryOperator::And,
                    rhs,
                } => {
                    self.visit_expr(lhs.as_mut())?;
                    self.in_supported_position = true;
                    self.visit_expr(rhs.as_mut())?;
                    self.in_supported_position = true;
                    return Ok(());
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

/// Replace all literals that are in positions we support parameters in the given query with
/// parameters, and return the values for those parameters alongside the index in the parameter list
/// where they appear as a tuple of (placeholder position, value).
pub fn auto_parametrize_query(
    query: &mut SelectStatement,
    server_supports_mixed_comparisons: bool,
) -> Vec<(usize, Literal)> {
    // Don't try to auto-parametrize equal-queries that already contain range params for now, since
    // we don't yet allow mixing range and equal parameters in the same query
    let mut visitor = AnalyzeLiteralsVisitor::default();
    visitor.visit_select_statement(query).unwrap();

    let (autoparametrize_equals, autoparametrize_ranges) = if server_supports_mixed_comparisons {
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
            (true, true) => return vec![],
        }
    };

    let mut visitor = AutoParametrizeVisitor {
        autoparametrize_equals,
        autoparametrize_ranges,
        has_aggregates: query.contains_aggregate_select(),
        ..Default::default()
    };
    #[allow(clippy::unwrap_used)] // error is !, which can never be returned
    visitor.visit_select_statement(query).unwrap();
    visitor.out
}

#[cfg(test)]
mod tests {
    use nom_sql::{Dialect, DialectDisplay};

    use super::*;

    fn try_parse_select_statement(q: &str, dialect: Dialect) -> Result<SelectStatement, String> {
        nom_sql::parse_select_statement(dialect, q)
    }

    fn parse_select_statement(q: &str, dialect: Dialect) -> SelectStatement {
        try_parse_select_statement(q, dialect).unwrap()
    }
    fn test_auto_parametrize(
        query: &str,
        expected_query: &str,
        // These are parameters that are expected to have been added by the autoparameterization
        // rewrite pass
        expected_added_parameters: Vec<(usize, Literal)>,
        dialect: nom_sql::Dialect,
        server_supports_mixed_comparisons: bool,
    ) {
        let mut query = parse_select_statement(query, dialect);
        let expected = parse_select_statement(expected_query, dialect);
        let res = auto_parametrize_query(&mut query, server_supports_mixed_comparisons);
        assert_eq!(
            query,
            expected,
            "\n  left: {}\n right: {}",
            query.display(dialect),
            expected.display(dialect),
        );
        assert_eq!(res, expected_added_parameters);
    }

    fn test_auto_parametrize_mysql(
        query: &str,
        expected_query: &str,
        // These are parameters that are expected to have been added by the autoparameterization
        // rewrite pass
        expected_added_parameters: Vec<(usize, Literal)>,
    ) {
        test_auto_parametrize(
            query,
            expected_query,
            expected_added_parameters,
            nom_sql::Dialect::MySQL,
            false,
        )
    }

    fn test_auto_parametrize_postgres(
        query: &str,
        expected_query: &str,
        // These are parameters that are expected to have been added by the autoparameterization
        // rewrite pass
        expected_added_parameters: Vec<(usize, Literal)>,
    ) {
        test_auto_parametrize(
            query,
            expected_query,
            expected_added_parameters,
            nom_sql::Dialect::PostgreSQL,
            false,
        )
    }

    #[test]
    fn no_literals() {
        test_auto_parametrize_mysql("SELECT * FROM users", "SELECT * FROM users", vec![]);
        test_auto_parametrize_postgres("SELECT * FROM users", "SELECT * FROM users", vec![]);
    }

    #[test]
    fn simple_parameter() {
        test_auto_parametrize_mysql(
            "SELECT id FROM users WHERE id = 1",
            "SELECT id FROM users WHERE id = ?",
            vec![(0, 1.into())],
        );
    }

    #[test]
    fn and_parameters() {
        test_auto_parametrize_mysql(
            "SELECT id FROM users WHERE id = 1 AND name = \"bob\"",
            "SELECT id FROM users WHERE id = ? AND name = ?",
            vec![(0, 1.into()), (1, "bob".into())],
        );
    }

    #[test]
    fn existing_param_before() {
        test_auto_parametrize_mysql(
            "SELECT id FROM users WHERE x = ? AND id = 1 AND name = \"bob\"",
            "SELECT id FROM users WHERE x = ? AND id = ? AND name = ?",
            vec![(1, 1.into()), (2, "bob".into())],
        );
    }

    #[test]
    fn existing_param_after() {
        test_auto_parametrize_mysql(
            "SELECT id FROM users WHERE id = 1 AND name = \"bob\" AND x = ?",
            "SELECT id FROM users WHERE id = ? AND name = ? AND x = ?",
            vec![(0, 1.into()), (1, "bob".into())],
        );
    }

    #[test]
    fn existing_param_between() {
        test_auto_parametrize_mysql(
            "SELECT id FROM users WHERE id = 1 AND x = ? AND name = \"bob\"",
            "SELECT id FROM users WHERE id = ? AND x = ? AND name = ?",
            vec![(0, 1.into()), (2, "bob".into())],
        );
    }

    #[test]
    fn literal_in_or() {
        test_auto_parametrize_mysql(
            "SELECT id FROM users WHERE (id = 1 OR id = 2) AND name = \"bob\"",
            "SELECT id FROM users WHERE (id = 1 OR id = 2) AND name = ?",
            vec![(0, "bob".into())],
        )
    }

    #[test]
    fn literal_in_subquery_where() {
        test_auto_parametrize_mysql(
                "SELECT id FROM users JOIN (SELECT id FROM users WHERE id = 1) s ON users.id = s.id WHERE id = 1",
                "SELECT id FROM users JOIN (SELECT id FROM users WHERE id = 1) s ON users.id = s.id WHERE id = ?",
                vec![(0, 1.into())],
            )
    }

    #[test]
    fn literal_in_field() {
        test_auto_parametrize_mysql(
            "SELECT id + 1 FROM users WHERE id = 1",
            "SELECT id + 1 FROM users WHERE id = ?",
            vec![(0, 1.into())],
        )
    }

    #[test]
    fn literal_in_in_rhs() {
        test_auto_parametrize_mysql(
                "select hashtags.* from hashtags inner join invites_hashtags on hashtags.id = invites_hashtags.hashtag_id where invites_hashtags.invite_id in (10,20,31)",
                "select hashtags.* from hashtags inner join invites_hashtags on hashtags.id = invites_hashtags.hashtag_id where invites_hashtags.invite_id in (?,?,?)",
                    vec![(0, 10.into()), (1, 20.into()), (2, 31.into())],
            );
    }

    #[test]
    fn mixed_in_with_equality() {
        test_auto_parametrize_mysql(
            "SELECT id FROM users WHERE id in (1, 2) AND name = 'bob'",
            "SELECT id FROM users WHERE id in (?, ?) AND name = ?",
            vec![(0, 1.into()), (1, 2.into()), (2, "bob".into())],
        );
    }

    #[test]
    fn equal_in_equal() {
        test_auto_parametrize_mysql(
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
        test_auto_parametrize_mysql(
            "SELECT count(*) FROM users WHERE id = 1 AND x IN (1, 2)",
            "SELECT count(*) FROM users WHERE id = ? AND x IN (1, 2)",
            vec![(0, 1.into())],
        );
    }

    #[test]
    fn literal_equals_column() {
        test_auto_parametrize_mysql(
            "SELECT * FROM users WHERE 1 = id",
            "SELECT * FROM users WHERE id = ?",
            vec![(0, 1.into())],
        );
    }

    #[test]
    fn existing_range_param() {
        test_auto_parametrize_mysql(
            "SELECT * FROM posts WHERE id = 1 AND score > ?",
            "SELECT * FROM posts WHERE id = 1 AND score > ?",
            vec![],
        )
    }

    #[test]
    fn offset() {
        test_auto_parametrize_mysql(
            "SELECT * FROM posts WHERE id = 1 ORDER BY SCORE ASC LIMIT 3 OFFSET 6",
            "SELECT * FROM posts WHERE id = ? ORDER BY SCORE ASC LIMIT 3 OFFSET ?",
            vec![(0, 1.into()), (1, 6.into())],
        );
    }

    #[test]
    fn constant_filter_with_param_betwen() {
        test_auto_parametrize_mysql(
            "SELECT * FROM posts WHERE id = 1 AND created_at BETWEEN ? and ?",
            "SELECT * FROM posts WHERE id = 1 AND created_at BETWEEN ? and ?",
            vec![],
        );
    }

    #[test]
    fn range_query_literals() {
        test_auto_parametrize_mysql(
            "SELECT * FROM posts WHERE score > 0 AND score < 10",
            "SELECT * FROM posts WHERE score > ? AND score < ?",
            vec![(0, 0.into()), (1, 10.into())],
        );
    }

    #[test]
    fn range_query_literals_inclusive() {
        test_auto_parametrize_mysql(
            "SELECT * FROM posts WHERE score >= 0 AND score <= 10",
            "SELECT * FROM posts WHERE score >= ? AND score <= ?",
            vec![(0, 0.into()), (1, 10.into())],
        );
    }

    #[test]
    fn range_query_literals_inclusive_exclusive() {
        test_auto_parametrize_mysql(
            "SELECT * FROM posts WHERE score >= 0 AND score < 10",
            "SELECT * FROM posts WHERE score >= ? AND score < ?",
            vec![(0, 0.into()), (1, 10.into())],
        );
    }

    #[test]
    fn range_query_literals_exclusive_inclusive() {
        test_auto_parametrize_mysql(
            "SELECT * FROM posts WHERE score > 0 AND score <= 10",
            "SELECT * FROM posts WHERE score > ? AND score <= ?",
            vec![(0, 0.into()), (1, 10.into())],
        );
    }

    #[test]
    fn equals_then_range() {
        test_auto_parametrize_mysql(
            "SELECT * FROM posts WHERE id = 1 AND score > 0 AND score < 10",
            "SELECT * FROM posts WHERE id = ? AND score > 0 AND score < 10",
            vec![(0, 1.into())],
        );
    }

    #[test]
    fn range_then_equals() {
        test_auto_parametrize_mysql(
            "SELECT * FROM posts WHERE score > 0 AND score < 10 AND id = 1",
            "SELECT * FROM posts WHERE score > 0 AND score < 10 AND id = ?",
            vec![(0, 1.into())],
        );
    }

    #[test]
    fn range_with_or() {
        test_auto_parametrize_mysql(
            "SELECT * FROM posts WHERE score > 0 OR score < 10",
            "SELECT * FROM posts WHERE score > 0 OR score < 10",
            vec![],
        );
    }

    #[test]
    fn nested_range() {
        test_auto_parametrize_mysql(
            "SELECT * FROM posts WHERE id in (SELECT id FROM posts WHERE score > 0 AND score < 10)",
            "SELECT * FROM posts WHERE id in (SELECT id FROM posts WHERE score > 0 AND score < 10)",
            vec![],
        );
    }

    #[test]
    fn less_than() {
        test_auto_parametrize_mysql(
            "SELECT * FROM posts WHERE id < 10",
            "SELECT * FROM posts WHERE id < ?",
            vec![(0, 10.into())],
        );
    }

    #[test]
    fn less_than_equal() {
        test_auto_parametrize_mysql(
            "SELECT * FROM posts WHERE id <= 10",
            "SELECT * FROM posts WHERE id <= ?",
            vec![(0, 10.into())],
        );
    }

    #[test]
    fn greater_than() {
        test_auto_parametrize_mysql(
            "SELECT * FROM posts WHERE id > 10",
            "SELECT * FROM posts WHERE id > ?",
            vec![(0, 10.into())],
        );
    }

    #[test]
    fn greater_than_equal() {
        test_auto_parametrize_mysql(
            "SELECT * FROM posts WHERE id >= 10",
            "SELECT * FROM posts WHERE id >= ?",
            vec![(0, 10.into())],
        );
    }

    #[test]
    fn range_with_pre_existing_equals_param() {
        test_auto_parametrize_mysql(
            "SELECT * FROM posts WHERE id = ? AND views > 10",
            "SELECT * FROM posts WHERE id = ? AND views > 10",
            vec![],
        );
    }

    #[test]
    fn equals_with_pre_existing_range_param() {
        test_auto_parametrize_mysql(
            "SELECT * FROM posts WHERE id = 10 AND views > ? AND date > 10",
            "SELECT * FROM posts WHERE id = 10 AND views > ? AND date > ?",
            vec![(1, 10.into())],
        );
    }

    #[test]
    fn ranges_only() {
        test_auto_parametrize_mysql(
            "SELECT * FROM posts WHERE id > 10 AND views > 2",
            "SELECT * FROM posts WHERE id > ? AND views > ?",
            vec![(0, 10.into()), (1, 2.into())],
        );
    }

    #[test]
    fn some_equals() {
        test_auto_parametrize_mysql(
            "SELECT * FROM posts WHERE id = ? AND views = 10",
            "SELECT * FROM posts WHERE id = ? AND views = ?",
            vec![(1, 10.into())],
        );
    }

    #[test]
    fn some_equals_with_ranges() {
        test_auto_parametrize_mysql(
            "SELECT * FROM posts WHERE id = ? AND date = 10 AND views > 10",
            "SELECT * FROM posts WHERE id = ? AND date = ? AND views > 10",
            vec![(1, 10.into())],
        );
    }

    #[test]
    fn supported_equals_with_unsupported_ranges() {
        test_auto_parametrize_mysql(
            "SELECT id FROM users JOIN (SELECT id FROM users WHERE id < 1) s ON users.id = s.id WHERE id = 1",
            "SELECT id FROM users JOIN (SELECT id FROM users WHERE id < 1) s ON users.id = s.id WHERE id = ?",
            vec![(0, 1.into())],
        )
    }

    #[test]
    fn supported_ranges_with_unsupported_equals() {
        test_auto_parametrize_mysql(
            "SELECT id FROM users JOIN (SELECT id FROM users WHERE id = 1) s ON users.id = s.id WHERE id < 1",
            "SELECT id FROM users JOIN (SELECT id FROM users WHERE id = 1) s ON users.id = s.id WHERE id < ?",
            vec![(0, 1.into())],
        )
    }

    #[test]
    fn supported_ranges_and_equals_with_unsupported_equals() {
        test_auto_parametrize_mysql(
            "SELECT id FROM users JOIN (SELECT id FROM users WHERE id = 1) s ON users.id = s.id WHERE id = 1 AND age > 21",
            "SELECT id FROM users JOIN (SELECT id FROM users WHERE id = 1) s ON users.id = s.id WHERE id = ? AND age > 21",
            vec![(0, 1.into())],
        )
    }

    #[test]
    fn supported_ranges_and_equals_with_unsupported_ranges() {
        test_auto_parametrize_mysql(
            "SELECT id FROM users JOIN (SELECT id FROM users WHERE age > 50) s ON users.id = s.id WHERE id = 1 AND age > 21",
            "SELECT id FROM users JOIN (SELECT id FROM users WHERE age > 50) s ON users.id = s.id WHERE id = ? AND age > 21",
            vec![(0, 1.into())],
        )
    }

    mod mixed_comparisons {
        use super::*;

        fn test_auto_parametrize_mysql(
            query: &str,
            expected_query: &str,
            // These are parameters that are expected to have been added by the
            // autoparameterization rewrite pass
            expected_added_parameters: Vec<(usize, Literal)>,
        ) {
            test_auto_parametrize(
                query,
                expected_query,
                expected_added_parameters,
                nom_sql::Dialect::MySQL,
                true,
            )
        }

        #[test]
        fn some_equals_with_ranges() {
            test_auto_parametrize_mysql(
                "SELECT * FROM posts WHERE id = ? AND date = 10 AND views > 9",
                "SELECT * FROM posts WHERE id = ? AND date = ? AND views > ?",
                vec![(1, 10.into()), (2, 9.into())],
            );
        }

        #[test]
        fn range_with_pre_existing_equals_param() {
            test_auto_parametrize_mysql(
                "SELECT * FROM posts WHERE id = ? AND views > 10",
                "SELECT * FROM posts WHERE id = ? AND views > ?",
                vec![(1, 10.into())],
            );
        }

        #[test]
        fn equals_with_pre_existing_range_param() {
            test_auto_parametrize_mysql(
                "SELECT * FROM posts WHERE id = 10 AND views > ? AND date > 9",
                "SELECT * FROM posts WHERE id = ? AND views > ? AND date > ?",
                vec![(0, 10.into()), (2, 9.into())],
            );
        }

        #[test]
        fn supported_equals_with_unsupported_ranges() {
            test_auto_parametrize_mysql(
                "SELECT id FROM users JOIN (SELECT id FROM users WHERE id < 1) s ON users.id = s.id WHERE id = 1",
                "SELECT id FROM users JOIN (SELECT id FROM users WHERE id < 1) s ON users.id = s.id WHERE id = ?",
                vec![(0, 1.into())],
            )
        }

        #[test]
        fn supported_ranges_with_unsupported_equals() {
            test_auto_parametrize_mysql(
                "SELECT id FROM users JOIN (SELECT id FROM users WHERE id = 1) s ON users.id = s.id WHERE id < 1",
                "SELECT id FROM users JOIN (SELECT id FROM users WHERE id = 1) s ON users.id = s.id WHERE id < ?",
                vec![(0, 1.into())],
            )
        }

        #[test]
        fn supported_ranges_and_equals_with_unsupported_equals() {
            test_auto_parametrize_mysql(
                "SELECT id FROM users JOIN (SELECT id FROM users WHERE id = 1) s ON users.id = s.id WHERE id = 1 AND age > 21",
                "SELECT id FROM users JOIN (SELECT id FROM users WHERE id = 1) s ON users.id = s.id WHERE id = ? AND age > ?",
                vec![(0, 1.into()), (1, 21.into())],
            )
        }

        #[test]
        fn supported_ranges_and_equals_with_unsupported_ranges() {
            test_auto_parametrize_mysql(
                "SELECT id FROM users JOIN (SELECT id FROM users WHERE age > 50) s ON users.id = s.id WHERE id = 1 AND age > 21",
                "SELECT id FROM users JOIN (SELECT id FROM users WHERE age > 50) s ON users.id = s.id WHERE id = ? AND age > ?",
                vec![(0, 1.into()), (1, 21.into())],
            )
        }
    }
}
