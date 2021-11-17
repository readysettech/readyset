pub mod visit;

use maplit::hashset;
use std::collections::{HashSet, VecDeque};
use std::iter;

use crate::create::CreateQueryCacheStatement;
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
            SqlQuery::Select(ref sq)
            | SqlQuery::CreateQueryCache(CreateQueryCacheStatement {
                statement: ref sq, ..
            }) => sq.tables.iter().cloned().collect(),
            SqlQuery::CompoundSelect(ref csq) => csq
                .selects
                .iter()
                .flat_map(|(_, sq)| &sq.tables)
                .cloned()
                .collect(),
            SqlQuery::RenameTable(ref rt) => rt
                .ops
                .iter()
                // Only including op.from because referred_tables() callers expect to get a list of
                // _existing_ tables that are utilized in the query.
                .map(|op| op.from.clone())
                .collect(),
            // If the type does not have any referred tables, we return an
            // empty hashset.
            SqlQuery::CreateView(_)
            | SqlQuery::Delete(_)
            | SqlQuery::DropTable(_)
            | SqlQuery::Update(_)
            | SqlQuery::Set(_)
            | SqlQuery::StartTransaction(_)
            | SqlQuery::Commit(_)
            | SqlQuery::Rollback(_)
            | SqlQuery::Use(_)
            | SqlQuery::Show(_)
            | SqlQuery::Explain(_) => HashSet::new(),
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
            Expression::UnaryOp { rhs: expr, .. } | Expression::Cast { expr, .. } => {
                self.visit_expr(expr)
            }
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
            Expression::Variable(_) => None,
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
        Expression::UnaryOp { rhs: expr, .. } | Expression::Cast { expr, .. } => {
            contains_aggregate(expr)
        }
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
        Expression::Variable(_) => false,
    }
}

pub struct Subexpressions<'a> {
    subexpr_iterators: VecDeque<Box<dyn Iterator<Item = &'a Expression> + 'a>>,
}

impl<'a> Iterator for Subexpressions<'a> {
    type Item = &'a Expression;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(subexprs) = self.subexpr_iterators.front_mut() {
            match subexprs.next() {
                Some(expr) => {
                    self.subexpr_iterators
                        .push_back(expr.immediate_subexpressions());
                    return Some(expr);
                }
                None => {
                    self.subexpr_iterators.pop_front();
                }
            }
        }
        None
    }
}

impl Expression {
    /// Construct an iterator over all the *immediate* subexpressions of the given Expression.
    ///
    /// # Examaples
    ///
    /// ```rust
    /// use nom_sql::{Expression, UnaryOperator, Column};
    ///
    /// let expr = Expression::UnaryOp {
    ///     op: UnaryOperator::Not,
    ///     rhs: Box::new(Expression::Column("x".into()))
    /// };
    ///
    /// let subexprs = expr.immediate_subexpressions().collect::<Vec<_>>();
    /// assert_eq!(subexprs, vec![&Expression::Column("x".into())])
    /// ````
    pub fn immediate_subexpressions<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Expression> + 'a> {
        match self {
            Expression::Literal(_)
            | Expression::Column(_)
            | Expression::Exists(_)
            | Expression::NestedSelect(_)
            | Expression::Variable(_) => Box::new(iter::empty()) as _,
            Expression::Call(fexpr) => Box::new(fexpr.arguments()) as _,
            Expression::BinaryOp { lhs, rhs, .. } => {
                Box::new(vec![lhs, rhs].into_iter().map(AsRef::as_ref)) as _
            }
            Expression::UnaryOp { rhs: expr, .. } | Expression::Cast { expr, .. } => {
                Box::new(iter::once(expr.as_ref())) as _
            }
            Expression::CaseWhen {
                condition,
                then_expr,
                else_expr,
            } => Box::new(
                vec![condition, then_expr]
                    .into_iter()
                    .chain(else_expr)
                    .map(AsRef::as_ref),
            ) as _,
            Expression::Between {
                operand, min, max, ..
            } => Box::new(vec![operand, min, max].into_iter().map(AsRef::as_ref)) as _,
            Expression::In {
                lhs,
                rhs: InValue::List(exprs),
                ..
            } => Box::new(iter::once(lhs.as_ref()).chain(exprs)) as _,
            Expression::In {
                lhs,
                rhs: InValue::Subquery(_),
                ..
            } => Box::new(iter::once(lhs.as_ref())) as _,
        }
    }

    /// Construct an iterator over all *recursive* subexpressions of the given Expression, excluding
    /// the expression itself. Iteration order is unspecified.
    pub fn recursive_subexpressions(&self) -> Subexpressions {
        let mut subexpr_iterators = VecDeque::with_capacity(1);
        subexpr_iterators.push_back(self.immediate_subexpressions());
        Subexpressions { subexpr_iterators }
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
