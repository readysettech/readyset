use nom_sql::{Column, Expression, FunctionExpression};

/// Returns true if the given [`FunctionExpression`] represents an aggregate function
pub(crate) fn is_aggregate(function: &FunctionExpression) -> bool {
    match function {
        FunctionExpression::Avg { .. }
        | FunctionExpression::Count { .. }
        | FunctionExpression::CountStar
        | FunctionExpression::Sum { .. }
        | FunctionExpression::Max(_)
        | FunctionExpression::Min(_)
        | FunctionExpression::GroupConcat { .. } => true,
        FunctionExpression::Cast(_, _) => false,
        // For now, assume all "generic" function calls are not aggregates
        FunctionExpression::Call { .. } => false,
    }
}

/// Rturns true if *any* of the recursive subexpressions of the given [`Expression`] contain an
/// aggregate
pub(crate) fn contains_aggregate(expr: &Expression) -> bool {
    match expr {
        Expression::Arithmetic(_) => false,
        Expression::Call(f) => {
            is_aggregate(f)
                || f.arguments()
                    .any(|arg| contains_aggregate(&arg.clone().into()))
        }
        Expression::Literal(_) => false,
        Expression::Column { .. } => false,
        Expression::CaseWhen {
            condition: _,
            then_expr,
            else_expr,
        } => {
            // FIXME(grfn): ignoring conditions here is incorrect, since they can contain function
            // call nodes - that's a conscious concession until we can replace ConditionExpr with
            // Expression and simplify expression traversal significantly
            contains_aggregate(then_expr)
                || else_expr
                    .iter()
                    .any(|expr| contains_aggregate(expr.as_ref()))
        }
    }
}

#[must_use]
pub(crate) fn map_aggregates(expr: &mut Expression) -> Vec<(FunctionExpression, String)> {
    let mut ret = Vec::new();
    match expr {
        Expression::Arithmetic(ari) => {
            ret.append(&mut map_aggregates(&mut ari.left));
            ret.append(&mut map_aggregates(&mut ari.right));
        }
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
            condition: _,
            then_expr,
            else_expr,
        } => {
            // FIXME(grfn): see above
            ret.append(&mut map_aggregates(then_expr));
            if let Some(else_expr) = else_expr {
                ret.append(&mut map_aggregates(else_expr));
            }
        }
        Expression::Call(_) | Expression::Literal(_) | Expression::Column(_) => {}
    }
    ret
}
