use nom::bytes::complete::tag_no_case;
use nom::combinator::opt;
use nom::sequence::{delimited, terminated, tuple};
use nom::IResult;

use crate::expression::expression;
use crate::whitespace::{whitespace0, whitespace1};
use crate::{Dialect, Expr};

pub fn case_when(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], Expr> {
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
            Expr::CaseWhen {
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

        let exp = Expr::CaseWhen {
            condition: Box::new(Expr::BinaryOp {
                op: BinaryOperator::Equal,
                lhs: Box::new(Expr::Column(c1.clone())),
                rhs: Box::new(Expr::Literal(Literal::Integer(0))),
            }),
            then_expr: Box::new(Expr::Column(c1.clone())),
            else_expr: Some(Box::new(Expr::Literal(Literal::Integer(1)))),
        };

        assert_eq!(
            exp.to_string(),
            "CASE WHEN (`foo` = 0) THEN `foo` ELSE 1 END"
        );

        let exp_no_else = Expr::CaseWhen {
            condition: Box::new(Expr::BinaryOp {
                op: BinaryOperator::Equal,
                lhs: Box::new(Expr::Column(c1.clone())),
                rhs: Box::new(Expr::Literal(Literal::Integer(0))),
            }),
            then_expr: Box::new(Expr::Column(c1)),
            else_expr: None,
        };

        assert_eq!(
            exp_no_else.to_string(),
            "CASE WHEN (`foo` = 0) THEN `foo` END"
        );
    }
}
