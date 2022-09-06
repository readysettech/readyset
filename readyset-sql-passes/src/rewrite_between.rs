use nom_sql::{
    BinaryOperator, DeleteStatement, Expr, FunctionExpr, InValue, SelectStatement, SqlQuery,
    UnaryOperator, UpdateStatement,
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

fn rewrite_expression(expr: Expr) -> Expr {
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
            FunctionExpr::Avg { expr, distinct } => FunctionExpr::Avg {
                expr: Box::new(rewrite_expression(*expr)),
                distinct,
            },
            FunctionExpr::Count { expr, distinct } => FunctionExpr::Count {
                expr: Box::new(rewrite_expression(*expr)),
                distinct,
            },
            FunctionExpr::CountStar => FunctionExpr::CountStar,
            FunctionExpr::Sum { expr, distinct } => FunctionExpr::Sum {
                expr: Box::new(rewrite_expression(*expr)),
                distinct,
            },
            FunctionExpr::Max(expr) => FunctionExpr::Max(Box::new(rewrite_expression(*expr))),
            FunctionExpr::Min(expr) => FunctionExpr::Min(Box::new(rewrite_expression(*expr))),
            FunctionExpr::GroupConcat { expr, separator } => FunctionExpr::GroupConcat {
                expr: Box::new(rewrite_expression(*expr)),
                separator,
            },
            FunctionExpr::Call { name, arguments } => FunctionExpr::Call {
                name,
                arguments: arguments.into_iter().map(rewrite_expression).collect(),
            },
        }),
        Literal(_) | Column(_) | Variable(_) => expr,
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
        Cast {
            expr,
            ty,
            postgres_style,
        } => Cast {
            expr: Box::new(rewrite_expression(*expr)),
            ty,
            postgres_style,
        },
    }
}

fn rewrite_between_condition(operand: Expr, min: Expr, max: Expr) -> Expr {
    Expr::BinaryOp {
        lhs: Box::new(Expr::BinaryOp {
            lhs: Box::new(operand.clone()),
            op: BinaryOperator::GreaterOrEqual,
            rhs: Box::new(rewrite_expression(min)),
        }),
        op: BinaryOperator::And,
        rhs: Box::new(Expr::BinaryOp {
            lhs: Box::new(operand),
            op: BinaryOperator::LessOrEqual,
            rhs: Box::new(rewrite_expression(max)),
        }),
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
