use std::mem;

use nom_sql::analysis::visit_mut::{self, VisitorMut};
use nom_sql::{BinaryOperator, Expr, ItemPlaceholder, Literal, SelectStatement};
use readyset_errors::{invalid_query_err, ReadySetError};

#[derive(Default)]
struct ExpandTuplesVisitor;

impl<'ast> VisitorMut<'ast> for ExpandTuplesVisitor {
    type Error = ReadySetError;

    // TODO ethan we need to make sure this only touches the where clause; seems like visit_expr is
    // called in HAVING clauses as well
    fn visit_expr(&mut self, expression: &'ast mut Expr) -> Result<(), Self::Error> {
        match expression {
            Expr::BinaryOp {
                // TODO ethan test that using ROW(...) works in actual postgres/mysql
                lhs: box Expr::Row {
                    exprs: lhs_exprs, ..
                },
                op: BinaryOperator::Equal,
                rhs: box Expr::Row {
                    exprs: rhs_exprs, ..
                },
            } => {
                debug_assert!(!lhs_exprs.is_empty());
                debug_assert!(!rhs_exprs.is_empty());

                if lhs_exprs.len() == rhs_exprs.len() {
                    if !lhs_exprs.is_empty() {
                        let last_expr = Expr::BinaryOp {
                            lhs: Box::new(lhs_exprs.pop().unwrap()),
                            op: BinaryOperator::Equal,
                            rhs: Box::new(rhs_exprs.pop().unwrap()),
                        };
                        let lhs_iter = lhs_exprs.iter_mut();
                        let rhs_iter = rhs_exprs.iter_mut();

                        // create new expr
                        // iterate over zipped lhs and rhs, anding everything tg
                        let new_expression = lhs_iter
                            .zip(rhs_iter)
                            .map(|(lhs, rhs)| Expr::BinaryOp {
                                lhs: Box::new(lhs.take()),
                                op: BinaryOperator::Equal,
                                rhs: Box::new(rhs.take()),
                            })
                            .rfold(last_expr, |acc, expr| Expr::BinaryOp {
                                lhs: Box::new(expr),
                                op: BinaryOperator::And,
                                rhs: Box::new(acc),
                            });
                        let _ = mem::replace(expression, new_expression);
                    }
                } else {
                    return Err(invalid_query_err!(
                        "Cannot compare row expressions of unequal lengths"
                    ));
                }
            }
            // TODO ethan test with empty Row; also do same for above
            Expr::BinaryOp {
                lhs: box Expr::Row {
                    exprs: lhs_exprs, ..
                },
                op: BinaryOperator::Equal,
                rhs: box Expr::Literal(Literal::Placeholder(_)),
            } if lhs_exprs.iter().all(|expr| matches!(expr, Expr::Column(_))) => {
                let last_expr = lhs_exprs.pop().unwrap();
                let lhs_iter = lhs_exprs.iter_mut();

                let last_expr = Expr::BinaryOp {
                    lhs: Box::new(last_expr),
                    op: BinaryOperator::Equal,
                    rhs: Box::new(Expr::Literal(Literal::Placeholder(
                        ItemPlaceholder::QuestionMark,
                    ))),
                };

                let new_expression = lhs_iter
                    .map(|lhs| Expr::BinaryOp {
                        lhs: Box::new(lhs.take()),
                        op: BinaryOperator::Equal,
                        rhs: Box::new(Expr::Literal(Literal::Placeholder(
                            ItemPlaceholder::QuestionMark,
                        ))),
                    })
                    .rfold(last_expr, |acc, expr| Expr::BinaryOp {
                        lhs: Box::new(expr),
                        op: BinaryOperator::And,
                        rhs: Box::new(acc),
                    });
                let _ = mem::replace(expression, new_expression);
            }
            _ => (),
        };

        visit_mut::walk_expr(self, expression)?;
        Ok(())
    }
}

pub(super) fn expand_tuples(query: &mut SelectStatement) {
    let mut visitor = ExpandTuplesVisitor;
    visitor.visit_select_statement(query).unwrap();
}

#[cfg(test)]
mod tests {
    use nom_sql::{Dialect, DialectDisplay};

    use super::*;

    const DIALECT: Dialect = Dialect::PostgreSQL;

    fn try_parse_select_statement(q: &str) -> Result<SelectStatement, String> {
        nom_sql::parse_select_statement(DIALECT, q)
    }

    fn parse_select_statement(q: &str) -> SelectStatement {
        try_parse_select_statement(q).unwrap()
    }

    fn assert_expected(query: &str, expected: &str) {
        let mut actual = parse_select_statement(query);
        super::expand_tuples(&mut actual);

        assert_eq!(
            expected,
            actual.where_clause.unwrap().display(DIALECT).to_string(),
        );
    }

    #[test]
    fn test_placeholder() {
        assert_expected(
            "SELECT * FROM t WHERE (w, x) = $1",
            r#"(("w" = ?) AND ("x" = ?))"#,
        );
    }

    #[test]
    fn test_placeholder_many_columns() {
        assert_expected(
            "SELECT * FROM t WHERE (w, x, y, z) = $1",
            r#"(("w" = ?) AND (("x" = ?) AND (("y" = ?) AND ("z" = ?))))"#,
        );
    }

    #[test]
    fn test_placeholder_row_syntax() {
        assert_expected(
            "SELECT * FROM t WHERE ROW(w, x, y, z) = $1",
            r#"(("w" = ?) AND (("x" = ?) AND (("y" = ?) AND ("z" = ?))))"#,
        );
    }

    #[test]
    fn test_tuple_equal_tuple() {
        assert_expected(
            "SELECT * FROM t WHERE (w, x) = (1, 2)",
            r#"(("w" = 1) AND ("x" = 2))"#,
        );
    }

    #[test]
    fn test_tuple_equal_tuple_many_columns() {
        assert_expected(
            "SELECT * FROM t WHERE (w, x, y, z) = (1, 2, 3, 4)",
            r#"(("w" = 1) AND (("x" = 2) AND (("y" = 3) AND ("z" = 4))))"#,
        );
    }

    #[test]
    fn test_tuple_equal_tuple_row_syntax() {
        assert_expected(
            "SELECT * FROM t WHERE ROW(w, x, y, z) = ROW(1, 2, 3, 4)",
            r#"(("w" = 1) AND (("x" = 2) AND (("y" = 3) AND ("z" = 4))))"#,
        );
    }
}
