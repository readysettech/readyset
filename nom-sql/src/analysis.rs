use std::borrow::Cow;
use std::iter;

use crate::{
    Arithmetic, ArithmeticBase, ArithmeticExpression, ArithmeticItem, CaseWhenExpression, Column,
    ColumnOrLiteral, ConditionBase, ConditionExpression, ConditionTree, Expression,
    FunctionArgument, FunctionArguments, FunctionExpression, SqlQuery, Table,
};
use itertools::Either;

pub trait ReferredTables {
    fn referred_tables(&self) -> Vec<Table>;
}

impl ReferredTables for SqlQuery {
    fn referred_tables(&self) -> Vec<Table> {
        match *self {
            SqlQuery::CreateTable(ref ctq) => vec![ctq.table.clone()],
            SqlQuery::AlterTable(ref atq) => vec![atq.table.clone()],
            SqlQuery::Insert(ref iq) => vec![iq.table.clone()],
            SqlQuery::Select(ref sq) => sq.tables.to_vec(),
            SqlQuery::CompoundSelect(ref csq) => {
                csq.selects
                    .iter()
                    .fold(Vec::new(), |mut acc, &(_, ref sq)| {
                        acc.extend(sq.tables.to_vec());
                        acc
                    })
            }
            _ => unreachable!(),
        }
    }
}

impl ReferredTables for ConditionExpression {
    fn referred_tables(&self) -> Vec<Table> {
        let mut tables = Vec::new();
        match *self {
            ConditionExpression::LogicalOp(ref ct) | ConditionExpression::ComparisonOp(ref ct) => {
                for t in ct
                    .left
                    .referred_tables()
                    .into_iter()
                    .chain(ct.right.referred_tables().into_iter())
                {
                    if !tables.contains(&t) {
                        tables.push(t);
                    }
                }
            }
            ConditionExpression::Base(ConditionBase::Field(ref f)) => {
                if let Some(ref t) = f.table {
                    let t = Table::from(t.as_ref());
                    if !tables.contains(&t) {
                        tables.push(t);
                    }
                }
            }
            ConditionExpression::Bracketed(ref ce) => tables.extend(ce.referred_tables()),
            _ => unimplemented!(),
        }
        tables
    }
}

/// Returns an iterator over all the direct arguments passed to the given function call expression
pub fn function_arguments(func: &FunctionExpression) -> impl Iterator<Item = &FunctionArgument> {
    match func {
        FunctionExpression::Avg(arg, _)
        | FunctionExpression::Count(arg, _)
        | FunctionExpression::Sum(arg, _)
        | FunctionExpression::Max(arg)
        | FunctionExpression::Min(arg)
        | FunctionExpression::GroupConcat(arg, _)
        | FunctionExpression::Cast(arg, _) => Either::Left(iter::once(arg)),
        FunctionExpression::CountStar => Either::Right(Either::Left(iter::empty())),
        FunctionExpression::Generic(_, FunctionArguments { arguments }) => {
            Either::Right(Either::Right(arguments.iter()))
        }
    }
}

#[derive(Clone)]
pub struct ReferredColumnsIter<'a> {
    exprs_to_visit: Vec<&'a Expression>,
    // TODO(grfn): Once FunctionArgument and Arithmetic go away and are replaced with Expression
    // these next two fields can go away too
    function_arguments_to_visit: Vec<&'a FunctionArgument>,
    arithmetic_to_visit: Vec<&'a Arithmetic>,
    condition_expressions_to_visit: Vec<&'a ConditionExpression>,
    columns_to_visit: Vec<&'a Column>,
}

impl<'a> ReferredColumnsIter<'a> {
    fn visit_expr(&mut self, expr: &'a Expression) -> Option<Cow<'a, Column>> {
        match expr {
            Expression::Arithmetic(ArithmeticExpression { ari, .. }) => {
                self.visit_arithmetic(ari).map(Cow::Borrowed)
            }
            Expression::Call(fexpr) => self.visit_function_expression(fexpr),
            Expression::Literal(_) => None,
            Expression::Column { name, table } => Some(Cow::Owned(Column {
                name: name.to_string(),
                table: table.clone(),
                alias: None,
                function: None,
            })),
        }
    }

    fn visit_function_expression(
        &mut self,
        fexpr: &'a FunctionExpression,
    ) -> Option<Cow<'a, Column>> {
        use FunctionExpression::*;

        match fexpr {
            Avg(arg, _) => self.visit_function_argument(arg),
            Count(arg, _) => self.visit_function_argument(arg),
            CountStar => None,
            Sum(arg, _) => self.visit_function_argument(arg),
            Max(arg) => self.visit_function_argument(arg),
            Min(arg) => self.visit_function_argument(arg),
            GroupConcat(arg, _) => self.visit_function_argument(arg),
            Cast(arg, _) => self.visit_function_argument(arg),
            Generic(_, FunctionArguments { arguments }) => {
                arguments.first().and_then(|first_arg| {
                    if arguments.len() >= 2 {
                        self.function_arguments_to_visit
                            .extend(arguments.iter().skip(1));
                    }
                    self.visit_function_argument(first_arg)
                })
            }
        }
    }

    fn visit_function_argument(&mut self, farg: &'a FunctionArgument) -> Option<Cow<'a, Column>> {
        match farg {
            FunctionArgument::Column(Column {
                function: Some(f), ..
            }) => self.visit_function_expression(f),
            FunctionArgument::Column(col) => Some(Cow::Borrowed(col)),
            FunctionArgument::Conditional(CaseWhenExpression {
                condition,
                then_expr,
                else_expr,
            }) => {
                self.condition_expressions_to_visit.push(condition);
                if let ColumnOrLiteral::Column(col) = then_expr {
                    self.columns_to_visit.push(col)
                }
                match else_expr {
                    Some(ColumnOrLiteral::Column(col)) => Some(Cow::Borrowed(col)),
                    _ => None,
                }
            }
            FunctionArgument::Literal(_) => None,
            FunctionArgument::Call(fun) => self.visit_function_expression(fun),
        }
    }

    fn visit_condition_expression(
        &mut self,
        ce: &'a ConditionExpression,
    ) -> Option<Cow<'a, Column>> {
        match ce {
            ConditionExpression::ComparisonOp(ConditionTree { left, right, .. })
            | ConditionExpression::LogicalOp(ConditionTree { left, right, .. }) => {
                self.condition_expressions_to_visit.push(left);
                self.visit_condition_expression(right)
            }
            ConditionExpression::NegationOp(ce) => self.visit_condition_expression(ce),
            ConditionExpression::ExistsOp(_) => unimplemented!("EXISTS is not implemented yet"),
            ConditionExpression::Base(ConditionBase::Field(col)) => Some(Cow::Borrowed(col)),
            ConditionExpression::Base(
                ConditionBase::Literal(_) | ConditionBase::LiteralList(_),
            ) => None,
            ConditionExpression::Base(ConditionBase::NestedSelect(_)) => {
                unimplemented!("Nested selects are not implemented yet")
            }
            ConditionExpression::Arithmetic(ae) => {
                self.visit_arithmetic(&ae.ari).map(Cow::Borrowed)
            }
            ConditionExpression::Bracketed(ce) => self.visit_condition_expression(ce),
            ConditionExpression::Between { operand, min, max } => {
                self.condition_expressions_to_visit.push(operand);
                self.condition_expressions_to_visit.push(min);
                self.visit_condition_expression(max)
            }
        }
    }

    fn visit_arithmetic_item(&mut self, ai: &'a ArithmeticItem) -> Option<&'a Column> {
        match ai {
            ArithmeticItem::Base(ArithmeticBase::Column(col)) => Some(col),
            ArithmeticItem::Base(ArithmeticBase::Scalar(_)) => None,
            ArithmeticItem::Base(ArithmeticBase::Bracketed(ari)) | ArithmeticItem::Expr(ari) => {
                self.visit_arithmetic(ari)
            }
        }
    }

    fn visit_arithmetic(&mut self, ari: &'a Arithmetic) -> Option<&'a Column> {
        if let Some(col) = self.visit_arithmetic_item(&ari.left) {
            self.columns_to_visit.push(col)
        }

        self.visit_arithmetic_item(&ari.right)
    }

    fn finished(&self) -> bool {
        self.exprs_to_visit.is_empty()
            && self.function_arguments_to_visit.is_empty()
            && self.arithmetic_to_visit.is_empty()
            && self.condition_expressions_to_visit.is_empty()
            && self.columns_to_visit.is_empty()
    }
}

impl<'a> Iterator for ReferredColumnsIter<'a> {
    type Item = Cow<'a, Column>;

    fn next(&mut self) -> Option<Self::Item> {
        while !self.finished() {
            let next = self
                .exprs_to_visit
                .pop()
                .and_then(|expr| self.visit_expr(expr))
                .or_else(|| {
                    self.function_arguments_to_visit
                        .pop()
                        .and_then(|farg| self.visit_function_argument(farg))
                })
                .or_else(|| {
                    self.arithmetic_to_visit
                        .pop()
                        .and_then(|ari| self.visit_arithmetic(ari).map(Cow::Borrowed))
                })
                .or_else(|| {
                    self.condition_expressions_to_visit
                        .pop()
                        .and_then(|ce| self.visit_condition_expression(ce))
                })
                .or_else(|| self.columns_to_visit.pop().map(Cow::Borrowed));
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
            function_arguments_to_visit: vec![],
            arithmetic_to_visit: vec![],
            condition_expressions_to_visit: vec![],
            columns_to_visit: vec![],
        }
    }
}

impl ReferredColumns for FunctionArgument {
    fn referred_columns(&self) -> ReferredColumnsIter {
        ReferredColumnsIter {
            exprs_to_visit: vec![],
            function_arguments_to_visit: vec![&self],
            arithmetic_to_visit: vec![],
            condition_expressions_to_visit: vec![],
            columns_to_visit: vec![],
        }
    }
}

impl ReferredColumns for ConditionExpression {
    fn referred_columns(&self) -> ReferredColumnsIter {
        ReferredColumnsIter {
            exprs_to_visit: vec![],
            function_arguments_to_visit: vec![],
            arithmetic_to_visit: vec![],
            condition_expressions_to_visit: vec![&self],
            columns_to_visit: vec![],
        }
    }
}

/// Recursively traverses `ce`, a [`ConditionExpression`], to find all function calls inside,
/// putting found function calls into `out`.
pub fn find_function_calls<'a>(out: &mut Vec<&'a Column>, ce: &'a ConditionExpression) {
    fn on_arithmetic<'a>(out: &mut Vec<&'a Column>, ari: &'a Arithmetic) {
        for item in &[&ari.left, &ari.right] {
            match **item {
                ArithmeticItem::Base(ArithmeticBase::Column(ref col)) => {
                    if col.function.is_some() {
                        out.push(col);
                    }
                }
                ArithmeticItem::Base(ArithmeticBase::Scalar(_)) => {}
                ArithmeticItem::Base(ArithmeticBase::Bracketed(ref ari)) => {
                    on_arithmetic(out, &*ari);
                }
                ArithmeticItem::Expr(ref ari) => {
                    on_arithmetic(out, &*ari);
                }
            }
        }
    }
    match *ce {
        ConditionExpression::ComparisonOp(ref ct) | ConditionExpression::LogicalOp(ref ct) => {
            find_function_calls(out, &ct.left);
            find_function_calls(out, &ct.right);
        }
        ConditionExpression::NegationOp(ref ce) | ConditionExpression::Bracketed(ref ce) => {
            find_function_calls(out, &*ce)
        }
        ConditionExpression::Base(ref cb) => {
            if let ConditionBase::Field(ref c) = cb {
                if c.function.is_some() {
                    out.push(c);
                }
            }
        }
        ConditionExpression::Arithmetic(ref ae) => {
            on_arithmetic(out, &ae.ari);
        }
        ConditionExpression::Between {
            ref operand,
            ref min,
            ref max,
        } => {
            find_function_calls(out, &*operand);
            find_function_calls(out, &*min);
            find_function_calls(out, &*max);
        }
        // unsupported, but also there aren't function calls in there anyway
        ConditionExpression::ExistsOp(_) => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BinaryOperator, Literal};

    mod referred_columns {
        use super::*;
        use crate::Literal;
        use Expression::{Call, Column as ColExpr, Literal as LitExpr};

        #[test]
        fn literal() {
            assert_eq!(
                LitExpr(Literal::Integer(1))
                    .referred_columns()
                    .collect::<Vec<_>>(),
                vec![]
            );
        }

        #[test]
        fn column() {
            assert_eq!(
                ColExpr {
                    name: "test".to_owned(),
                    table: None
                }
                .referred_columns()
                .collect::<Vec<_>>(),
                vec![Cow::Owned(Column::from("test"))],
            )
        }

        #[test]
        fn aggregate_with_column() {
            assert_eq!(
                Call(FunctionExpression::Sum(
                    FunctionArgument::Column(Column::from("test")),
                    false
                ))
                .referred_columns()
                .collect::<Vec<_>>(),
                vec![Cow::Owned(Column::from("test"))]
            )
        }

        #[test]
        fn generic_with_multiple_columns() {
            assert_eq!(
                Call(FunctionExpression::Generic(
                    "ifnull".to_owned(),
                    FunctionArguments {
                        arguments: vec![
                            FunctionArgument::Column(Column::from("col1")),
                            FunctionArgument::Column(Column::from("col2")),
                        ]
                    }
                ))
                .referred_columns()
                .collect::<Vec<_>>(),
                vec![
                    Cow::Owned(Column::from("col1")),
                    Cow::Owned(Column::from("col2"))
                ]
            )
        }

        #[test]
        fn nested_function_call() {
            assert_eq!(
                Call(FunctionExpression::Count(
                    FunctionArgument::Column(Column {
                        name: "".to_owned(),
                        table: None,
                        alias: None,
                        function: Some(Box::new(FunctionExpression::Generic(
                            "ifnull".to_owned(),
                            FunctionArguments {
                                arguments: vec![
                                    FunctionArgument::Column(Column::from("col1")),
                                    FunctionArgument::Column(Column::from("col2")),
                                ]
                            }
                        )))
                    }),
                    false
                ))
                .referred_columns()
                .collect::<Vec<_>>(),
                vec![
                    Cow::Owned(Column::from("col1")),
                    Cow::Owned(Column::from("col2")),
                ]
            );
        }
    }

    #[test]
    fn condition_expr() {
        assert_eq!(
            ConditionExpression::ComparisonOp(ConditionTree {
                left: Box::new(ConditionExpression::Base(ConditionBase::Field(
                    Column::from("sign")
                ))),
                operator: BinaryOperator::Greater,
                right: Box::new(ConditionExpression::Base(ConditionBase::Literal(
                    Literal::Integer(0)
                )))
            })
            .referred_columns()
            .collect::<Vec<_>>(),
            vec![Cow::Owned(Column::from("sign"))]
        );
    }

    #[test]
    fn find_funcalls_basic() {
        let col = Column {
            name: "test".to_string(),
            alias: None,
            table: None,
            function: Some(Box::new(FunctionExpression::CountStar)),
        };
        let cexpr = ConditionExpression::ComparisonOp(ConditionTree {
            left: Box::new(ConditionExpression::Base(ConditionBase::Field(col.clone()))),
            operator: BinaryOperator::Greater,
            right: Box::new(ConditionExpression::Base(ConditionBase::Literal(
                Literal::Integer(0),
            ))),
        });
        let mut out = vec![];
        find_function_calls(&mut out, &cexpr);
        assert_eq!(out, vec![&col]);
    }
}
