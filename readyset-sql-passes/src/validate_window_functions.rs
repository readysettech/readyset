use readyset_errors::{invalid_query, unsupported, ReadySetResult};
use readyset_sql::ast::{Expr, FieldDefinitionExpr, FieldReference, OrderBy, SelectStatement};

/// Check if an expression contains any WindowFunction expressions
fn contains_window_function(expr: &Expr) -> bool {
    match expr {
        Expr::WindowFunction { .. } => true,
        Expr::BinaryOp { lhs, rhs, .. } => {
            contains_window_function(lhs) || contains_window_function(rhs)
        }
        Expr::UnaryOp { rhs, .. } => contains_window_function(rhs),
        Expr::CaseWhen {
            branches,
            else_expr,
            ..
        } => {
            branches.iter().any(|branch| {
                contains_window_function(&branch.condition)
                    || contains_window_function(&branch.body)
            }) || else_expr
                .as_ref()
                .is_some_and(|arg| contains_window_function(arg))
        }
        Expr::In { lhs, rhs, .. } => {
            contains_window_function(lhs)
                || match rhs {
                    readyset_sql::ast::InValue::List(exprs) => {
                        exprs.iter().any(contains_window_function)
                    }
                    readyset_sql::ast::InValue::Subquery(_) => false,
                }
        }
        Expr::Between {
            operand, min, max, ..
        } => {
            contains_window_function(operand)
                || contains_window_function(min)
                || contains_window_function(max)
        }
        Expr::Cast { expr, .. } | Expr::Collate { expr, .. } | Expr::ConvertUsing { expr, .. } => {
            contains_window_function(expr)
        }
        Expr::Array(exprs) | Expr::Row { exprs, .. } => exprs.iter().any(contains_window_function),
        Expr::Call(f) => f.arguments().any(contains_window_function),
        Expr::Literal(_)
        | Expr::Column(_)
        | Expr::Exists(_)
        | Expr::NestedSelect(_)
        | Expr::Variable(_) => false,
        Expr::OpAny { lhs, rhs, .. }
        | Expr::OpAll { lhs, rhs, .. }
        | Expr::OpSome { lhs, rhs, .. } => {
            contains_window_function(lhs) || contains_window_function(rhs)
        }
    }
}

/// Check if a WindowFunction expression contains any nested WindowFunction expressions
fn contains_nested_window_function(expr: &Expr) -> bool {
    match expr {
        Expr::WindowFunction {
            function,
            partition_by,
            order_by,
            ..
        } => {
            // Check partition_by expressions
            for partition_expr in partition_by {
                if contains_window_function(partition_expr) {
                    return true;
                }
            }

            // Check order_by expressions
            for (order_expr, _, _) in order_by {
                if contains_window_function(order_expr) {
                    return true;
                }
            }

            // Check the function arguments
            function.arguments().any(contains_window_function)
        }
        _ => false,
    }
}

/// Validate that WindowFunction expressions are not used in invalid contexts (WHERE, HAVING, GROUP BY, ORDER BY, JOIN ON)
fn validate_window_function_usage(stmt: &SelectStatement) -> ReadySetResult<()> {
    // Check WHERE clause
    if let Some(where_expr) = &stmt.where_clause {
        if contains_window_function(where_expr) {
            invalid_query!("Window functions are not allowed in WHERE clauses")
        }
    }

    // Check HAVING clause
    if let Some(having_expr) = &stmt.having {
        if contains_window_function(having_expr) {
            invalid_query!("Window functions are not allowed in HAVING clauses")
        }
    }

    // Check GROUP BY clause
    if let Some(group_by) = &stmt.group_by {
        for field in &group_by.fields {
            if let FieldReference::Expr(expr) = field {
                if contains_window_function(expr) {
                    invalid_query!("Window functions are not allowed in GROUP BY clauses")
                }
            }
        }
    }

    // Check ORDER BY clause
    if let Some(order) = &stmt.order {
        for OrderBy { field, .. } in &order.order_by {
            if let FieldReference::Expr(expr) = field {
                if contains_window_function(expr) {
                    unsupported!("Window functions in ORDER BY clauses are not supported")
                }
            }
        }
    }

    // Check JOIN ON conditions
    for join in &stmt.join {
        if let readyset_sql::ast::JoinConstraint::On(expr) = &join.constraint {
            if contains_window_function(expr) {
                invalid_query!("Window functions are not allowed in JOIN ON conditions")
            }
        }
    }

    // Check for nested window functions in SELECT fields
    for field in &stmt.fields {
        if let FieldDefinitionExpr::Expr { expr, .. } = field {
            if let Expr::WindowFunction { .. } = expr {
                if contains_nested_window_function(expr) {
                    invalid_query!("Window functions cannot contain nested window functions")
                }
            }
        }
    }

    Ok(())
}

pub trait ValidateWindowFunctions: Sized {
    /// Validate that WindowFunction expressions are only used in SELECT clauses
    fn validate_window_functions(&mut self) -> ReadySetResult<&mut Self>;
}

impl ValidateWindowFunctions for SelectStatement {
    fn validate_window_functions(&mut self) -> ReadySetResult<&mut Self> {
        validate_window_function_usage(self)?;
        Ok(self)
    }
}
