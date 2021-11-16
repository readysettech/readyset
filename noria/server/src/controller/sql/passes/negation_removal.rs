use crate::controller::sql::query_utils::is_logical_op;
use nom_sql::{BinaryOperator, Expression, JoinConstraint, SqlQuery, UnaryOperator};
use noria_errors::{internal, unsupported, ReadySetResult};

pub trait NegationRemoval {
    fn remove_negation(self) -> ReadySetResult<SqlQuery>;
}

fn normalize_expr(ce: &mut Expression, negate: bool) -> ReadySetResult<()> {
    match *ce {
        Expression::BinaryOp {
            ref mut op,
            ref mut lhs,
            ref mut rhs,
        } => {
            if negate {
                *op = match *op {
                    BinaryOperator::And => BinaryOperator::Or,
                    BinaryOperator::Or => BinaryOperator::And,
                    BinaryOperator::Equal => BinaryOperator::NotEqual,
                    BinaryOperator::NotEqual => BinaryOperator::Equal,
                    BinaryOperator::Greater => BinaryOperator::LessOrEqual,
                    BinaryOperator::GreaterOrEqual => BinaryOperator::Less,
                    BinaryOperator::Less => BinaryOperator::GreaterOrEqual,
                    BinaryOperator::LessOrEqual => BinaryOperator::Greater,
                    BinaryOperator::Like => BinaryOperator::NotLike,
                    BinaryOperator::NotLike => BinaryOperator::Like,
                    BinaryOperator::ILike => BinaryOperator::NotILike,
                    BinaryOperator::NotILike => BinaryOperator::ILike,
                    BinaryOperator::Is => BinaryOperator::IsNot,
                    BinaryOperator::IsNot => BinaryOperator::Is,
                    BinaryOperator::Add
                    | BinaryOperator::Subtract
                    | BinaryOperator::Multiply
                    | BinaryOperator::Divide => {
                        // TODO(grfn): Replace with proper typechecking
                        unsupported!("Cannot apply NOT operator to expression: {}", ce)
                    }
                };
            }

            normalize_expr(lhs, is_logical_op(op) && negate)?;
            normalize_expr(rhs, is_logical_op(op) && negate)?;
        }
        Expression::UnaryOp {
            op: UnaryOperator::Not,
            ref mut rhs,
        } => {
            *ce = std::mem::replace(rhs, Expression::Literal(nom_sql::Literal::Null));
            normalize_expr(ce, !negate)?;
        }
        Expression::Between { .. } => {
            internal!("BETWEEN should have been removed earlier")
        }
        Expression::CaseWhen {
            ref mut then_expr,
            ref mut else_expr,
            ..
        } => {
            normalize_expr(then_expr.as_mut(), negate)?;
            if let Some(else_expr) = else_expr {
                normalize_expr(else_expr.as_mut(), negate)?;
            }
        }
        Expression::In {
            ref mut negated, ..
        } => {
            if negate {
                *negated = !*negated;
            }
        }
        Expression::Cast { ref mut expr, .. } => {
            //TODO: should negate depend on the type of the CAST?
            normalize_expr(expr, negate)?;
        }
        Expression::Call(_)
        | Expression::Literal(_)
        | Expression::Variable(_)
        | Expression::Column(_)
        | Expression::Exists(_)
        | Expression::NestedSelect(_)
        | Expression::UnaryOp {
            op: UnaryOperator::Neg,
            ..
        } => {
            if negate {
                unsupported!("Cannot apply NOT operator to expression: {}", ce);
            }
        }
    }

    Ok(())
}

impl NegationRemoval for SqlQuery {
    fn remove_negation(mut self) -> ReadySetResult<SqlQuery> {
        if let SqlQuery::Select(ref mut s) = self {
            if let Some(ref mut w) = s.where_clause {
                normalize_expr(w, false)?;
            }

            for j in s.join.iter_mut() {
                if let JoinConstraint::On(ref mut ce) = j.constraint {
                    normalize_expr(ce, false)?;
                }
            }
        }
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use nom_sql::{parse_query, Dialect};

    use super::*;

    #[test]
    fn it_normalizes() {
        let mut expr = Expression::UnaryOp {
            op: UnaryOperator::Not,
            rhs: Box::new(Expression::BinaryOp {
                op: BinaryOperator::And,
                lhs: Box::new(Expression::BinaryOp {
                    op: BinaryOperator::Less,
                    lhs: Box::new(Expression::Column("a".into())),
                    rhs: Box::new(Expression::Column("b".into())),
                }),
                rhs: Box::new(Expression::BinaryOp {
                    op: BinaryOperator::Equal,
                    lhs: Box::new(Expression::Column("c".into())),
                    rhs: Box::new(Expression::Column("b".into())),
                }),
            }),
        };

        let target = Expression::BinaryOp {
            op: BinaryOperator::Or,
            lhs: Box::new(Expression::BinaryOp {
                op: BinaryOperator::GreaterOrEqual,
                lhs: Box::new(Expression::Column("a".into())),
                rhs: Box::new(Expression::Column("b".into())),
            }),
            rhs: Box::new(Expression::BinaryOp {
                op: BinaryOperator::NotEqual,
                lhs: Box::new(Expression::Column("c".into())),
                rhs: Box::new(Expression::Column("b".into())),
            }),
        };

        normalize_expr(&mut expr, false).unwrap();
        assert_eq!(expr, target, "expected = {}\nactual = {}", target, expr);
    }

    #[test]
    fn normalize_in_with_not() {
        let statement =
            parse_query(Dialect::MySQL, "SELECT * FROM t WHERE NOT id IN (1, 2)").unwrap();
        let expected =
            parse_query(Dialect::MySQL, "SELECT * FROM t WHERE id NOT IN (1, 2)").unwrap();
        let res = statement.remove_negation().unwrap();
        assert_eq!(res, expected)
    }

    #[test]
    fn normalize_in_without_not() {
        let statement = parse_query(Dialect::MySQL, "SELECT * FROM t WHERE id IN (1, 2)").unwrap();
        let expected = statement.clone();
        let res = statement.remove_negation().unwrap();
        assert_eq!(res, expected)
    }
}
