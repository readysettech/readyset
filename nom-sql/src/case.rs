use nom::bytes::complete::tag_no_case;
use nom::combinator::opt;
use nom::sequence::{delimited, terminated, tuple};
use nom::IResult;

use crate::expression::expression;
use crate::whitespace::{whitespace0, whitespace1};
use crate::{Dialect, Expression};

pub fn case_when(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], Expression> {
    move |i| {
        let (remaining_input, (_, _, _, _, condition, _, _, _, then_expr, _, else_expr, _)) =
            tuple((
                tag_no_case("case"),
                whitespace1,
                tag_no_case("when"),
                whitespace0,
                expression(dialect),
                whitespace0,
                tag_no_case("then"),
                whitespace0,
                expression(dialect),
                whitespace0,
                opt(delimited(
                    terminated(tag_no_case("else"), whitespace0),
                    expression(dialect),
                    whitespace0,
                )),
                tag_no_case("end"),
            ))(i)?;

        Ok((
            remaining_input,
            Expression::CaseWhen {
                condition: Box::new(condition),
                then_expr: Box::new(then_expr),
                else_expr: else_expr.map(Box::new),
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BinaryOperator, Column, Literal};

    #[test]
    fn it_displays() {
        let c1 = Column {
            name: "foo".into(),
            table: None,
        };

        let exp = Expression::CaseWhen {
            condition: Box::new(Expression::BinaryOp {
                op: BinaryOperator::Equal,
                lhs: Box::new(Expression::Column(c1.clone())),
                rhs: Box::new(Expression::Literal(Literal::Integer(0))),
            }),
            then_expr: Box::new(Expression::Column(c1.clone())),
            else_expr: Some(Box::new(Expression::Literal(Literal::Integer(1)))),
        };

        assert_eq!(
            format!("{}", exp),
            "CASE WHEN (`foo` = 0) THEN `foo` ELSE 1 END"
        );

        let exp_no_else = Expression::CaseWhen {
            condition: Box::new(Expression::BinaryOp {
                op: BinaryOperator::Equal,
                lhs: Box::new(Expression::Column(c1.clone())),
                rhs: Box::new(Expression::Literal(Literal::Integer(0))),
            }),
            then_expr: Box::new(Expression::Column(c1)),
            else_expr: None,
        };

        assert_eq!(
            format!("{}", exp_no_else),
            "CASE WHEN (`foo` = 0) THEN `foo` END"
        );
    }
}
