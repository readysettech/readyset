use nom_sql::analysis::function_arguments;
use nom_sql::{Expression, FunctionExpression};

/// Returns true if the given [`FunctionExpression`] represents an aggregate function
pub(crate) fn is_aggregate(function: &FunctionExpression) -> bool {
    match function {
        FunctionExpression::Avg(_, _)
        | FunctionExpression::Count(_, _)
        | FunctionExpression::CountStar
        | FunctionExpression::Sum(_, _)
        | FunctionExpression::Max(_)
        | FunctionExpression::Min(_)
        | FunctionExpression::GroupConcat(_, _) => true,
        FunctionExpression::Cast(_, _) => false,
        // For now, assume all "generic" function calls are not aggregates
        FunctionExpression::Generic(_, _) => false,
    }
}

/// Rturns true if *any* of the recursive subexpressions of the given [`Expression`] contain an
/// aggregate
pub(crate) fn contains_aggregate(expr: &Expression) -> bool {
    match expr {
        Expression::Arithmetic(_) => false,
        Expression::Call(f) => {
            is_aggregate(f)
                || function_arguments(f).any(|arg| contains_aggregate(&arg.clone().into()))
        }
        Expression::Literal(_) => false,
        Expression::Column { .. } => false,
    }
}
