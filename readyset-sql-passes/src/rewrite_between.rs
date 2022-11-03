use nom_sql::analysis::visit_mut::{self, VisitorMut};
use nom_sql::{
    BinaryOperator, DeleteStatement, Expr, SelectStatement, SqlQuery, UnaryOperator,
    UpdateStatement,
};
use Expr::*;

/// Things that contain subexpressions of type `ConditionExpr` that can be targeted for the
/// desugaring of BETWEEN
pub trait RewriteBetween {
    /// Recursively rewrite all BETWEEN conditions in the given query into an ANDed pair of
    /// (inclusive) comparison operators. For example, the following query:
    ///
    /// ```sql
    /// SELECT * FROM t WHERE n BETWEEN 1 AND 2;
    /// ```
    ///
    /// becomes:
    ///
    /// ```sql
    /// SELECT * FROM t WHERE n >= 1 AND n >= 2;
    /// ```
    ///
    /// Invariant: The return value will have no recursive subexpressions of type
    /// [`Expr::Between`]
    #[must_use]
    fn rewrite_between(self) -> Self;
}

fn rewrite_between_condition(operand: Expr, min: Expr, max: Expr) -> Expr {
    Expr::BinaryOp {
        lhs: Box::new(Expr::BinaryOp {
            lhs: Box::new(operand.clone()),
            op: BinaryOperator::GreaterOrEqual,
            rhs: Box::new(min),
        }),
        op: BinaryOperator::And,
        rhs: Box::new(Expr::BinaryOp {
            lhs: Box::new(operand),
            op: BinaryOperator::LessOrEqual,
            rhs: Box::new(max),
        }),
    }
}

struct RewriteBetweenVisitor;

impl<'ast> VisitorMut<'ast> for RewriteBetweenVisitor {
    type Error = !;

    fn visit_expr(&mut self, expr: &'ast mut Expr) -> Result<(), Self::Error> {
        match expr {
            Between {
                operand,
                min,
                max,
                negated: false,
            } => {
                *expr =
                    rewrite_between_condition((**operand).clone(), (**min).clone(), (**max).clone())
            }
            Between {
                operand,
                min,
                max,
                negated: true,
            } => {
                *expr = UnaryOp {
                    op: UnaryOperator::Not,
                    rhs: Box::new(rewrite_between_condition(
                        (**operand).clone(),
                        (**min).clone(),
                        (**max).clone(),
                    )),
                }
            }
            _ => {}
        }

        visit_mut::walk_expr(self, expr)
    }
}

impl RewriteBetween for SelectStatement {
    fn rewrite_between(mut self) -> Self {
        let Ok(()) = RewriteBetweenVisitor.visit_select_statement(&mut self);
        self
    }
}

impl RewriteBetween for DeleteStatement {
    fn rewrite_between(mut self) -> Self {
        let Ok(()) = RewriteBetweenVisitor.visit_delete_statement(&mut self);
        self
    }
}

impl RewriteBetween for UpdateStatement {
    fn rewrite_between(mut self) -> Self {
        let Ok(()) = RewriteBetweenVisitor.visit_update_statement(&mut self);
        self
    }
}

impl RewriteBetween for SqlQuery {
    fn rewrite_between(mut self) -> Self {
        let Ok(()) = RewriteBetweenVisitor.visit_sql_query(&mut self);
        self
    }
}

#[cfg(test)]
mod tests {
    use nom_sql::{parse_query, Dialect};

    use super::*;

    #[test]
    fn test_rewrite_top_level_between_in_select() {
        let query = parse_query(
            Dialect::MySQL,
            "SELECT id FROM things WHERE frobulation BETWEEN 10 AND 17;",
        )
        .unwrap();
        let expected = parse_query(
            Dialect::MySQL,
            "SELECT id FROM things WHERE frobulation >= 10 AND frobulation <= 17;",
        )
        .unwrap();
        let result = query.rewrite_between();
        assert_eq!(result, expected, "result = {}", result);
    }
}
