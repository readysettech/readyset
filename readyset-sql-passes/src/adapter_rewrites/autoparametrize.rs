use std::{iter, mem};

use nom_sql::analysis::visit_mut::{self, VisitorMut};
use nom_sql::{BinaryOperator, Expr, InValue, ItemPlaceholder, Literal, SelectStatement};

#[derive(Default)]
struct AutoParametrizeVisitor {
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
                    op: BinaryOperator::Equal,
                    rhs: box Expr::Literal(lit),
                } => {
                    self.replace_literal(lit);
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
                    self.out
                        .extend(exprs.into_iter().enumerate().filter_map(
                            move |(i, expr)| match expr {
                                Expr::Literal(lit) => Some((i + start_index, lit)),
                                // unreachable since we checked everything in the list is a literal
                                // above, but best not to panic regardless
                                _ => None,
                            },
                        ));
                    self.param_index += num_exprs;
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
            self.replace_literal(offset);
        }

        visit_mut::walk_offset(self, offset)
    }
}

/// Replace all literals that are in positions we support parameters in the given query with
/// parameters, and return the values for those parameters alongside the index in the parameter list
/// where they appear as a tuple of (placeholder position, value).
pub fn auto_parametrize_query(query: &mut SelectStatement) -> Vec<(usize, Literal)> {
    // Don't try to auto-parametrize equal-queries that already contain range params for now, since
    // we don't yet allow mixing range and equal parameters in the same query
    if query.where_clause.iter().any(|expr| {
        iter::once(expr)
            .chain(expr.recursive_subexpressions())
            .any(|subexpr| {
                matches!(
                    subexpr,
                    Expr::BinaryOp {
                        op: BinaryOperator::Less
                            | BinaryOperator::Greater
                            | BinaryOperator::LessOrEqual
                            | BinaryOperator::GreaterOrEqual,
                        rhs: box Expr::Literal(Literal::Placeholder(..)),
                        ..
                    } | Expr::Between {
                        min: box Expr::Literal(Literal::Placeholder(..)),
                        ..
                    } | Expr::Between {
                        max: box Expr::Literal(Literal::Placeholder(..)),
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
        expected_parameters: Vec<(usize, Literal)>,
        dialect: nom_sql::Dialect,
    ) {
        let mut query = parse_select_statement(query, dialect);
        let expected = parse_select_statement(expected_query, dialect);
        let res = auto_parametrize_query(&mut query);
        assert_eq!(
            query,
            expected,
            "\n  left: {}\n right: {}",
            query.display(dialect),
            expected.display(dialect),
        );
        assert_eq!(res, expected_parameters);
    }

    fn test_auto_parametrize_mysql(
        query: &str,
        expected_query: &str,
        expected_parameters: Vec<(usize, Literal)>,
    ) {
        test_auto_parametrize(
            query,
            expected_query,
            expected_parameters,
            nom_sql::Dialect::MySQL,
        )
    }

    fn test_auto_parametrize_postgres(
        query: &str,
        expected_query: &str,
        expected_parameters: Vec<(usize, Literal)>,
    ) {
        test_auto_parametrize(
            query,
            expected_query,
            expected_parameters,
            nom_sql::Dialect::PostgreSQL,
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
}
