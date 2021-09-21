use std::convert::TryFrom;

pub use nom_sql::analysis::{contains_aggregate, is_aggregate};
use nom_sql::{
    BinaryOperator, Column, Expression, FunctionExpression, InValue, LimitClause, Literal,
};
use noria::{unsupported, ReadySetResult};

#[must_use]
pub(crate) fn map_aggregates(expr: &mut Expression) -> Vec<(FunctionExpression, String)> {
    let mut ret = Vec::new();
    match expr {
        Expression::Call(f) if is_aggregate(f) => {
            let name = f.to_string();
            ret.push((f.clone(), name.clone()));
            *expr = Expression::Column(Column {
                name,
                table: None,
                function: None,
            });
        }
        Expression::CaseWhen {
            condition,
            then_expr,
            else_expr,
        } => {
            ret.append(&mut map_aggregates(condition));
            ret.append(&mut map_aggregates(then_expr));
            if let Some(else_expr) = else_expr {
                ret.append(&mut map_aggregates(else_expr));
            }
        }
        Expression::Call(_) | Expression::Literal(_) | Expression::Column(_) => {}
        Expression::BinaryOp { lhs, rhs, .. } => {
            ret.append(&mut map_aggregates(lhs));
            ret.append(&mut map_aggregates(rhs));
        }
        Expression::UnaryOp { rhs, .. } => {
            ret.append(&mut map_aggregates(rhs));
        }
        Expression::Exists(_) => {}
        Expression::NestedSelect(_) => {}
        Expression::Between {
            operand, min, max, ..
        } => {
            ret.append(&mut map_aggregates(operand));
            ret.append(&mut map_aggregates(min));
            ret.append(&mut map_aggregates(max));
        }
        Expression::In { lhs, rhs, .. } => {
            ret.append(&mut map_aggregates(lhs));
            match rhs {
                InValue::Subquery(_) => {}
                InValue::List(exprs) => {
                    for expr in exprs {
                        ret.append(&mut map_aggregates(expr));
                    }
                }
            }
        }
    }
    ret
}

/// Returns true if the given binary operator is a (boolean-valued) predicate
///
/// TODO(grfn): Replace this with actual typechecking at some point
pub(crate) fn is_predicate(op: &BinaryOperator) -> bool {
    use BinaryOperator::*;

    matches!(
        op,
        Like | NotLike
            | ILike
            | NotILike
            | Equal
            | NotEqual
            | Greater
            | GreaterOrEqual
            | Less
            | LessOrEqual
            | Is
            | IsNot
    )
}

/// Returns true if the given binary operator is a (boolean-valued) logical operator
///
/// TODO(grfn): Replace this with actual typechecking at some point
pub(crate) fn is_logical_op(op: &BinaryOperator) -> bool {
    use BinaryOperator::*;

    matches!(op, And | Or)
}

/// Boolean-valued logical operators
pub(crate) enum LogicalOp {
    And,
    Or,
}

impl TryFrom<BinaryOperator> for LogicalOp {
    type Error = BinaryOperator;

    fn try_from(value: BinaryOperator) -> Result<Self, Self::Error> {
        match value {
            BinaryOperator::And => Ok(Self::And),
            BinaryOperator::Or => Ok(Self::Or),
            _ => Err(value),
        }
    }
}

pub(crate) fn extract_limit(limit: &LimitClause) -> ReadySetResult<usize> {
    if let Expression::Literal(Literal::Integer(k)) = limit.limit {
        if k < 0 {
            unsupported!("Invalid value for limit: {}", k);
        }
        Ok(k as usize)
    } else {
        unsupported!(
            "Only literal limits are supported ({} supplied)",
            limit.limit
        );
    }
}
