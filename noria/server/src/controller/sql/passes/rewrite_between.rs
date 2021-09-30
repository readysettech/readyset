use nom_sql::{
    BinaryOperator, DeleteStatement, Expression, FunctionExpression, InValue, SelectStatement,
    SqlQuery, UnaryOperator, UpdateStatement,
};
use Expression::*;

/// Things that contain subexpressions of type `ConditionExpression` that can be targeted for the
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
    /// [`Expression::Between`]
    fn rewrite_between(self) -> Self;
}

impl RewriteBetween for SelectStatement {
    fn rewrite_between(mut self) -> Self {
        if let Some(where_clause) = self.where_clause {
            self.where_clause = Some(rewrite_expression(where_clause));
        }
        self
    }
}

impl RewriteBetween for DeleteStatement {
    fn rewrite_between(mut self) -> Self {
        if let Some(where_clause) = self.where_clause {
            self.where_clause = Some(rewrite_expression(where_clause));
        }
        self
    }
}

impl RewriteBetween for UpdateStatement {
    fn rewrite_between(mut self) -> Self {
        if let Some(where_clause) = self.where_clause {
            self.where_clause = Some(rewrite_expression(where_clause));
        }
        self
    }
}

impl RewriteBetween for SqlQuery {
    fn rewrite_between(self) -> Self {
        match self {
            SqlQuery::Select(select) => SqlQuery::Select(select.rewrite_between()),
            SqlQuery::Delete(del) => SqlQuery::Delete(del.rewrite_between()),
            SqlQuery::CompoundSelect(mut compound_select) => {
                for (_, ref mut select) in compound_select.selects.iter_mut() {
                    *select = select.clone().rewrite_between()
                }
                SqlQuery::CompoundSelect(compound_select)
            }
            SqlQuery::Update(upd) => SqlQuery::Update(upd.rewrite_between()),
            _ => self,
        }
    }
}

fn rewrite_expression(expr: Expression) -> Expression {
    match expr {
        BinaryOp { lhs, rhs, op } => BinaryOp {
            lhs: Box::new(rewrite_expression(*lhs)),
            op,
            rhs: Box::new(rewrite_expression(*rhs)),
        },
        UnaryOp { op, rhs } => UnaryOp {
            op,
            rhs: Box::new(rewrite_expression(*rhs)),
        },
        Between {
            operand,
            min,
            max,
            negated: false,
        } => rewrite_between_condition(*operand, *min, *max),
        Between {
            operand,
            min,
            max,
            negated: true,
        } => UnaryOp {
            op: UnaryOperator::Not,
            rhs: Box::new(rewrite_between_condition(*operand, *min, *max)),
        },
        Exists(select_stmt) => Exists(Box::new(select_stmt.rewrite_between())),
        Call(fexpr) => Call(match fexpr {
            FunctionExpression::Avg { expr, distinct } => FunctionExpression::Avg {
                expr: Box::new(rewrite_expression(*expr)),
                distinct,
            },
            FunctionExpression::Count {
                expr,
                distinct,
                count_nulls,
            } => FunctionExpression::Count {
                expr: Box::new(rewrite_expression(*expr)),
                distinct,
                count_nulls,
            },
            FunctionExpression::CountStar => FunctionExpression::CountStar,
            FunctionExpression::Sum { expr, distinct } => FunctionExpression::Sum {
                expr: Box::new(rewrite_expression(*expr)),
                distinct,
            },
            FunctionExpression::Max(expr) => {
                FunctionExpression::Max(Box::new(rewrite_expression(*expr)))
            }
            FunctionExpression::Min(expr) => {
                FunctionExpression::Min(Box::new(rewrite_expression(*expr)))
            }
            FunctionExpression::GroupConcat { expr, separator } => {
                FunctionExpression::GroupConcat {
                    expr: Box::new(rewrite_expression(*expr)),
                    separator,
                }
            }
            FunctionExpression::Call { name, arguments } => FunctionExpression::Call {
                name,
                arguments: arguments.into_iter().map(rewrite_expression).collect(),
            },
        }),
        Literal(_) | Column(_) => expr,
        CaseWhen {
            condition,
            then_expr,
            else_expr,
        } => CaseWhen {
            condition: Box::new(rewrite_expression(*condition)),
            then_expr: Box::new(rewrite_expression(*then_expr)),
            else_expr: else_expr.map(|else_expr| Box::new(rewrite_expression(*else_expr))),
        },
        NestedSelect(sel) => NestedSelect(Box::new(sel.rewrite_between())),
        In { lhs, rhs, negated } => In {
            lhs: Box::new(rewrite_expression(*lhs)),
            rhs: match rhs {
                InValue::Subquery(sel) => InValue::Subquery(Box::new(sel.rewrite_between())),
                InValue::List(exprs) => {
                    InValue::List(exprs.into_iter().map(rewrite_expression).collect())
                }
            },
            negated,
        },
        Cast { expr, ty } => Cast {
            expr: Box::new(rewrite_expression(*expr)),
            ty,
        },
    }
}

fn rewrite_between_condition(operand: Expression, min: Expression, max: Expression) -> Expression {
    Expression::BinaryOp {
        lhs: Box::new(Expression::BinaryOp {
            lhs: Box::new(operand.clone()),
            op: BinaryOperator::GreaterOrEqual,
            rhs: Box::new(rewrite_expression(min)),
        }),
        op: BinaryOperator::And,
        rhs: Box::new(Expression::BinaryOp {
            lhs: Box::new(operand),
            op: BinaryOperator::LessOrEqual,
            rhs: Box::new(rewrite_expression(max)),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nom_sql::{parse_query, Dialect};

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
