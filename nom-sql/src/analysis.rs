use maplit::hashset;
use std::collections::HashSet;

use crate::{Column, Expression, FunctionExpression, InValue, SqlQuery, Table};

pub trait ReferredTables {
    fn referred_tables(&self) -> HashSet<Table>;
}

impl ReferredTables for SqlQuery {
    fn referred_tables(&self) -> HashSet<Table> {
        match *self {
            SqlQuery::CreateTable(ref ctq) => hashset![ctq.table.clone()],
            SqlQuery::AlterTable(ref atq) => hashset![atq.table.clone()],
            SqlQuery::Insert(ref iq) => hashset![iq.table.clone()],
            SqlQuery::Select(ref sq) => sq.tables.iter().cloned().collect(),
            SqlQuery::CompoundSelect(ref csq) => csq
                .selects
                .iter()
                .flat_map(|(_, sq)| &sq.tables)
                .cloned()
                .collect(),
            _ => unreachable!(),
        }
    }
}

impl ReferredTables for Expression {
    fn referred_tables(&self) -> HashSet<Table> {
        self.referred_columns()
            .filter_map(|col| col.table.clone())
            .map(|name| Table {
                name,
                ..Default::default()
            })
            .collect()
    }
}

#[derive(Clone)]
pub struct ReferredColumnsIter<'a> {
    exprs_to_visit: Vec<&'a Expression>,
    columns_to_visit: Vec<&'a Column>,
}

impl<'a> ReferredColumnsIter<'a> {
    fn visit_expr(&mut self, expr: &'a Expression) -> Option<&'a Column> {
        match expr {
            Expression::Call(fexpr) => self.visit_function_expression(fexpr),
            Expression::Literal(_) => None,
            Expression::Column(col) => Some(col),
            Expression::CaseWhen {
                condition,
                then_expr,
                else_expr,
            } => {
                self.exprs_to_visit.push(condition);
                self.exprs_to_visit.push(then_expr);
                if let Some(else_expr) = else_expr {
                    self.visit_expr(else_expr)
                } else {
                    None
                }
            }
            Expression::BinaryOp { lhs, rhs, .. } => {
                self.exprs_to_visit.push(lhs);
                self.visit_expr(rhs)
            }
            Expression::UnaryOp { rhs, .. } => self.visit_expr(rhs),
            Expression::Exists { .. } => None,
            Expression::Between {
                operand, min, max, ..
            } => {
                self.exprs_to_visit.push(operand);
                self.exprs_to_visit.push(min);
                self.visit_expr(max)
            }
            Expression::In { lhs, rhs, .. } => {
                self.exprs_to_visit.push(lhs);
                match rhs {
                    InValue::Subquery(_) => None,
                    InValue::List(exprs) => {
                        self.exprs_to_visit.extend(exprs.iter().skip(1));
                        if let Some(expr) = exprs.first() {
                            self.visit_expr(expr)
                        } else {
                            None
                        }
                    }
                }
            }
            Expression::NestedSelect(_) => None,
        }
    }

    fn visit_function_expression(&mut self, fexpr: &'a FunctionExpression) -> Option<&'a Column> {
        use FunctionExpression::*;

        match fexpr {
            Avg { expr, .. } => self.visit_expr(expr),
            Count { expr, .. } => self.visit_expr(expr),
            CountStar => None,
            Sum { expr, .. } => self.visit_expr(expr),
            Max(arg) => self.visit_expr(arg),
            Min(arg) => self.visit_expr(arg),
            GroupConcat { expr, .. } => self.visit_expr(expr),
            Cast(arg, _) => self.visit_expr(arg),
            Call { arguments, .. } => arguments.first().and_then(|first_arg| {
                if arguments.len() >= 2 {
                    self.exprs_to_visit.extend(arguments.iter().skip(1));
                }
                self.visit_expr(first_arg)
            }),
        }
    }

    fn finished(&self) -> bool {
        self.exprs_to_visit.is_empty() && self.columns_to_visit.is_empty()
    }
}

impl<'a> Iterator for ReferredColumnsIter<'a> {
    type Item = &'a Column;

    fn next(&mut self) -> Option<Self::Item> {
        while !self.finished() {
            let next = self
                .exprs_to_visit
                .pop()
                .and_then(|expr| self.visit_expr(expr))
                .or_else(|| self.columns_to_visit.pop());
            if next.is_some() {
                return next;
            }
        }
        None
    }
}

pub trait ReferredColumns {
    fn referred_columns(&self) -> ReferredColumnsIter;
}

impl ReferredColumns for Expression {
    fn referred_columns(&self) -> ReferredColumnsIter {
        ReferredColumnsIter {
            exprs_to_visit: vec![self],
            columns_to_visit: vec![],
        }
    }
}

/// Returns true if the given [`FunctionExpression`] represents an aggregate function
pub fn is_aggregate(function: &FunctionExpression) -> bool {
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
pub fn contains_aggregate(expr: &Expression) -> bool {
    match expr {
        Expression::Call(f) => is_aggregate(f) || f.arguments().any(contains_aggregate),
        Expression::Literal(_) => false,
        Expression::Column { .. } => false,
        Expression::CaseWhen {
            condition,
            then_expr,
            else_expr,
        } => {
            contains_aggregate(condition)
                || contains_aggregate(then_expr)
                || else_expr
                    .iter()
                    .any(|expr| contains_aggregate(expr.as_ref()))
        }
        Expression::BinaryOp { lhs, rhs, .. } => contains_aggregate(lhs) || contains_aggregate(rhs),
        Expression::UnaryOp { rhs, .. } => contains_aggregate(rhs),
        Expression::Exists(_) => false,
        Expression::Between {
            operand, min, max, ..
        } => contains_aggregate(operand) || contains_aggregate(min) || contains_aggregate(max),
        Expression::NestedSelect(_) => false,
        Expression::In { lhs, rhs, .. } => {
            contains_aggregate(lhs)
                || match rhs {
                    InValue::Subquery(_) => false,
                    InValue::List(exprs) => exprs.iter().any(contains_aggregate),
                }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BinaryOperator;

    mod referred_columns {
        use super::*;
        use crate::Literal;
        use Expression::{Call, Column as ColExpr, Literal as LitExpr};

        #[test]
        fn literal() {
            assert!(LitExpr(Literal::Integer(1))
                .referred_columns()
                .next()
                .is_none());
        }

        #[test]
        fn column() {
            assert_eq!(
                ColExpr(Column::from("test"))
                    .referred_columns()
                    .collect::<Vec<_>>(),
                vec![&Column::from("test")],
            )
        }

        #[test]
        fn aggregate_with_column() {
            assert_eq!(
                Call(FunctionExpression::Sum {
                    expr: Box::new(Expression::Column(Column::from("test"))),
                    distinct: false
                })
                .referred_columns()
                .collect::<Vec<_>>(),
                vec![&Column::from("test")]
            )
        }

        #[test]
        fn generic_with_multiple_columns() {
            assert_eq!(
                Call(FunctionExpression::Call {
                    name: "ifnull".to_owned(),
                    arguments: vec![
                        Expression::Column(Column::from("col1")),
                        Expression::Column(Column::from("col2")),
                    ]
                })
                .referred_columns()
                .collect::<Vec<_>>(),
                vec![&Column::from("col1"), &Column::from("col2")]
            )
        }

        #[test]
        fn nested_function_call() {
            assert_eq!(
                Call(FunctionExpression::Count {
                    expr: Box::new(Expression::Call(FunctionExpression::Call {
                        name: "ifnull".to_owned(),
                        arguments: vec![
                            Expression::Column(Column::from("col1")),
                            Expression::Column(Column::from("col2")),
                        ]
                    })),
                    distinct: false,
                    count_nulls: false,
                })
                .referred_columns()
                .collect::<Vec<_>>(),
                vec![&Column::from("col1"), &Column::from("col2"),]
            );
        }

        #[test]
        fn binary_op() {
            assert_eq!(
                Expression::BinaryOp {
                    lhs: Box::new(Expression::Column(Column::from("sign"))),
                    op: BinaryOperator::Greater,
                    rhs: Box::new(Expression::Literal(Literal::Integer(0)))
                }
                .referred_columns()
                .collect::<Vec<_>>(),
                vec![&Column::from("sign")]
            );
        }
    }
}
