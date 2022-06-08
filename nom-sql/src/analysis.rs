pub mod visit;

use std::collections::{HashSet, VecDeque};
use std::iter;

use maplit::hashset;

use crate::{
    CacheInner, Column, CreateCacheStatement, Expr, FieldDefinitionExpr, FieldReference,
    FunctionExpr, InValue, JoinConstraint, SelectStatement, SqlQuery, Table,
};

/// Extension trait providing the `referred_tables` method to various parts of the AST
pub trait ReferredTables {
    /// Return a set of all tables referred to in `self`
    fn referred_tables(&self) -> HashSet<Table>;
}

impl ReferredTables for SqlQuery {
    fn referred_tables(&self) -> HashSet<Table> {
        match *self {
            SqlQuery::CreateTable(ref ctq) => hashset![ctq.table.clone()],
            SqlQuery::AlterTable(ref atq) => hashset![atq.table.clone()],
            SqlQuery::Insert(ref iq) => hashset![iq.table.clone()],
            SqlQuery::Select(ref sq) => sq.tables.iter().cloned().collect(),
            SqlQuery::CreateCache(CreateCacheStatement { inner: ref i, .. }) => match i {
                CacheInner::Statement(sq) => sq.tables.iter().cloned().collect(),
                CacheInner::Id(_) => HashSet::new(),
            },
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
            | SqlQuery::DropView(_)
            | SqlQuery::Update(_)
            | SqlQuery::Set(_)
            | SqlQuery::StartTransaction(_)
            | SqlQuery::Commit(_)
            | SqlQuery::Rollback(_)
            | SqlQuery::Use(_)
            | SqlQuery::Show(_)
            | SqlQuery::Explain(_)
            | SqlQuery::DropCache(_)
            | SqlQuery::DropAllCaches(_) => HashSet::new(),
        }
    }
}

impl ReferredTables for Expr {
    fn referred_tables(&self) -> HashSet<Table> {
        self.referred_columns()
            .filter_map(|col| col.table.clone())
            .collect()
    }
}

#[derive(Clone)]
pub struct ReferredColumnsIter<'a> {
    exprs_to_visit: Vec<&'a Expr>,
    columns_to_visit: Vec<&'a Column>,
}

impl<'a> ReferredColumnsIter<'a> {
    fn visit_expr(&mut self, expr: &'a Expr) -> Option<&'a Column> {
        match expr {
            Expr::Call(fexpr) => self.visit_function_expression(fexpr),
            Expr::Literal(_) => None,
            Expr::Column(col) => Some(col),
            Expr::CaseWhen {
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
            Expr::BinaryOp { lhs, rhs, .. } => {
                self.exprs_to_visit.push(lhs);
                self.visit_expr(rhs)
            }
            Expr::UnaryOp { rhs: expr, .. } | Expr::Cast { expr, .. } => self.visit_expr(expr),
            Expr::Exists { .. } => None,
            Expr::Between {
                operand, min, max, ..
            } => {
                self.exprs_to_visit.push(operand);
                self.exprs_to_visit.push(min);
                self.visit_expr(max)
            }
            Expr::In { lhs, rhs, .. } => {
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
            Expr::NestedSelect(_) => None,
            Expr::Variable(_) => None,
        }
    }

    fn visit_function_expression(&mut self, fexpr: &'a FunctionExpr) -> Option<&'a Column> {
        use FunctionExpr::*;

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

pub struct ReferredColumnsMut<'a> {
    exprs_to_visit: Vec<&'a mut Expr>,
    columns_to_visit: Vec<&'a mut Column>,
}

impl<'a> ReferredColumnsMut<'a> {
    fn visit_expr(&mut self, expr: &'a mut Expr) -> Option<&'a mut Column> {
        match expr {
            Expr::Call(fexpr) => self.visit_function_expression(fexpr),
            Expr::Literal(_) => None,
            Expr::Column(col) => Some(col),
            Expr::CaseWhen {
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
            Expr::BinaryOp { lhs, rhs, .. } => {
                self.exprs_to_visit.push(lhs);
                self.visit_expr(rhs)
            }
            Expr::UnaryOp { rhs: expr, .. } | Expr::Cast { expr, .. } => self.visit_expr(expr),
            Expr::Exists { .. } => None,
            Expr::Between {
                operand, min, max, ..
            } => {
                self.exprs_to_visit.push(operand);
                self.exprs_to_visit.push(min);
                self.visit_expr(max)
            }
            Expr::In { lhs, rhs, .. } => {
                self.exprs_to_visit.push(lhs);
                match rhs {
                    InValue::Subquery(_) => None,
                    InValue::List(exprs) => exprs.split_first_mut().and_then(|(expr, exprs)| {
                        self.exprs_to_visit.extend(exprs);
                        self.visit_expr(expr)
                    }),
                }
            }
            Expr::NestedSelect(_) => None,
            Expr::Variable(_) => None,
        }
    }

    fn visit_function_expression(&mut self, fexpr: &'a mut FunctionExpr) -> Option<&'a mut Column> {
        use FunctionExpr::*;

        match fexpr {
            Avg { expr, .. } => self.visit_expr(expr),
            Count { expr, .. } => self.visit_expr(expr),
            CountStar => None,
            Sum { expr, .. } => self.visit_expr(expr),
            Max(arg) => self.visit_expr(arg),
            Min(arg) => self.visit_expr(arg),
            GroupConcat { expr, .. } => self.visit_expr(expr),
            Call { arguments, .. } => arguments.split_first_mut().and_then(|(first_arg, args)| {
                self.exprs_to_visit.extend(args);
                self.visit_expr(first_arg)
            }),
        }
    }

    fn finished(&self) -> bool {
        self.exprs_to_visit.is_empty() && self.columns_to_visit.is_empty()
    }
}

impl<'a> Iterator for ReferredColumnsMut<'a> {
    type Item = &'a mut Column;

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
    fn referred_columns_mut(&mut self) -> ReferredColumnsMut<'_>;
}

impl ReferredColumns for Expr {
    fn referred_columns(&self) -> ReferredColumnsIter {
        ReferredColumnsIter {
            exprs_to_visit: vec![self],
            columns_to_visit: vec![],
        }
    }

    fn referred_columns_mut(&mut self) -> ReferredColumnsMut {
        ReferredColumnsMut {
            exprs_to_visit: vec![self],
            columns_to_visit: vec![],
        }
    }
}

impl ReferredColumns for FunctionExpr {
    fn referred_columns(&self) -> ReferredColumnsIter {
        let mut iter = ReferredColumnsIter {
            exprs_to_visit: vec![],
            columns_to_visit: vec![],
        };
        let initial_columns = iter.visit_function_expression(self);
        iter.columns_to_visit.extend(initial_columns);
        iter
    }

    fn referred_columns_mut(&mut self) -> ReferredColumnsMut {
        let mut iter = ReferredColumnsMut {
            exprs_to_visit: vec![],
            columns_to_visit: vec![],
        };
        let initial_columns = iter.visit_function_expression(self);
        iter.columns_to_visit.extend(initial_columns);
        iter
    }
}

impl SelectStatement {
    /// Construct an iterator over all the columns referred to in self, without recursing into
    /// subqueries in any position.
    pub fn outermost_referred_columns(&self) -> ReferredColumnsIter {
        let mut columns_to_visit = vec![];
        let exprs_to_visit = self
            .fields
            .iter()
            .filter_map(|fde| match fde {
                FieldDefinitionExpr::Expr { expr, .. } => Some(expr),
                FieldDefinitionExpr::All | FieldDefinitionExpr::AllInTable(_) => None,
            })
            .chain(self.join.iter().filter_map(|join| match &join.constraint {
                JoinConstraint::On(expr) => Some(expr),
                JoinConstraint::Using(cols) => {
                    columns_to_visit.extend(cols);
                    None
                }
                JoinConstraint::Empty => None,
            }))
            .chain(&self.where_clause)
            .chain(&self.having)
            .chain(self.group_by.iter().flat_map(|gb| {
                gb.fields.iter().filter_map(|f| match f {
                    FieldReference::Expr(expr) => Some(expr),
                    _ => None,
                })
            }))
            .chain(self.order.iter().flat_map(|oc| {
                oc.order_by.iter().filter_map(|(f, _)| match f {
                    FieldReference::Expr(expr) => Some(expr),
                    _ => None,
                })
            }))
            .collect();

        ReferredColumnsIter {
            exprs_to_visit,
            columns_to_visit,
        }
    }
}

/// Returns true if the given [`FunctionExpr`] represents an aggregate function
pub fn is_aggregate(function: &FunctionExpr) -> bool {
    match function {
        FunctionExpr::Avg { .. }
        | FunctionExpr::Count { .. }
        | FunctionExpr::CountStar
        | FunctionExpr::Sum { .. }
        | FunctionExpr::Max(_)
        | FunctionExpr::Min(_)
        | FunctionExpr::GroupConcat { .. } => true,
        // For now, assume all "generic" function calls are not aggregates
        FunctionExpr::Call { .. } => false,
    }
}

/// Rturns true if *any* of the recursive subexpressions of the given [`Expr`] contain an
/// aggregate
pub fn contains_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::Call(f) => is_aggregate(f) || f.arguments().any(contains_aggregate),
        Expr::Literal(_) => false,
        Expr::Column { .. } => false,
        Expr::CaseWhen {
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
        Expr::BinaryOp { lhs, rhs, .. } => contains_aggregate(lhs) || contains_aggregate(rhs),
        Expr::UnaryOp { rhs: expr, .. } | Expr::Cast { expr, .. } => contains_aggregate(expr),
        Expr::Exists(_) => false,
        Expr::Between {
            operand, min, max, ..
        } => contains_aggregate(operand) || contains_aggregate(min) || contains_aggregate(max),
        Expr::NestedSelect(_) => false,
        Expr::In { lhs, rhs, .. } => {
            contains_aggregate(lhs)
                || match rhs {
                    InValue::Subquery(_) => false,
                    InValue::List(exprs) => exprs.iter().any(contains_aggregate),
                }
        }
        Expr::Variable(_) => false,
    }
}

pub struct Subexpressions<'a> {
    subexpr_iterators: VecDeque<Box<dyn Iterator<Item = &'a Expr> + 'a>>,
}

impl<'a> Iterator for Subexpressions<'a> {
    type Item = &'a Expr;

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

impl Expr {
    /// Construct an iterator over all the *immediate* subexpressions of the given `Expr`.
    ///
    /// # Examaples
    ///
    /// ```rust
    /// use nom_sql::{Column, Expr, UnaryOperator};
    ///
    /// let expr = Expr::UnaryOp {
    ///     op: UnaryOperator::Not,
    ///     rhs: Box::new(Expr::Column("x".into())),
    /// };
    ///
    /// let subexprs = expr.immediate_subexpressions().collect::<Vec<_>>();
    /// assert_eq!(subexprs, vec![&Expr::Column("x".into())])
    /// ````
    pub fn immediate_subexpressions<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Expr> + 'a> {
        match self {
            Expr::Literal(_)
            | Expr::Column(_)
            | Expr::Exists(_)
            | Expr::NestedSelect(_)
            | Expr::Variable(_) => Box::new(iter::empty()) as _,
            Expr::Call(fexpr) => Box::new(fexpr.arguments()) as _,
            Expr::BinaryOp { lhs, rhs, .. } => {
                Box::new(vec![lhs, rhs].into_iter().map(AsRef::as_ref)) as _
            }
            Expr::UnaryOp { rhs: expr, .. } | Expr::Cast { expr, .. } => {
                Box::new(iter::once(expr.as_ref())) as _
            }
            Expr::CaseWhen {
                condition,
                then_expr,
                else_expr,
            } => Box::new(
                vec![condition, then_expr]
                    .into_iter()
                    .chain(else_expr)
                    .map(AsRef::as_ref),
            ) as _,
            Expr::Between {
                operand, min, max, ..
            } => Box::new(vec![operand, min, max].into_iter().map(AsRef::as_ref)) as _,
            Expr::In {
                lhs,
                rhs: InValue::List(exprs),
                ..
            } => Box::new(iter::once(lhs.as_ref()).chain(exprs)) as _,
            Expr::In {
                lhs,
                rhs: InValue::Subquery(_),
                ..
            } => Box::new(iter::once(lhs.as_ref())) as _,
        }
    }

    /// Construct an iterator over all *recursive* subexpressions of the given Expr, excluding
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
        use Expr::{Call, Column as ColExpr, Literal as LitExpr};

        use super::*;
        use crate::Literal;

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
                Call(FunctionExpr::Sum {
                    expr: Box::new(Expr::Column(Column::from("test"))),
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
                Call(FunctionExpr::Call {
                    name: "ifnull".to_owned(),
                    arguments: vec![
                        Expr::Column(Column::from("col1")),
                        Expr::Column(Column::from("col2")),
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
                Call(FunctionExpr::Count {
                    expr: Box::new(Expr::Call(FunctionExpr::Call {
                        name: "ifnull".to_owned(),
                        arguments: vec![
                            Expr::Column(Column::from("col1")),
                            Expr::Column(Column::from("col2")),
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
                Expr::BinaryOp {
                    lhs: Box::new(Expr::Column(Column::from("sign"))),
                    op: BinaryOperator::Greater,
                    rhs: Box::new(Expr::Literal(Literal::Integer(0)))
                }
                .referred_columns()
                .collect::<Vec<_>>(),
                vec![&Column::from("sign")]
            );
        }
    }
}
