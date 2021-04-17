use crate::condition::condition_expr;
use crate::expression::expression;
use crate::Expression;

use nom::bytes::complete::tag_no_case;
use nom::character::complete::{multispace0, multispace1};
use nom::combinator::opt;
use nom::sequence::{delimited, terminated, tuple};
use nom::IResult;

pub fn case_when(i: &[u8]) -> IResult<&[u8], Expression> {
    let (remaining_input, (_, _, _, _, condition, _, _, _, then_expr, _, else_expr, _)) = tuple((
        tag_no_case("case"),
        multispace1,
        tag_no_case("when"),
        multispace0,
        condition_expr,
        multispace0,
        tag_no_case("then"),
        multispace0,
        expression,
        multispace0,
        opt(delimited(
            terminated(tag_no_case("else"), multispace0),
            expression,
            multispace0,
        )),
        tag_no_case("end"),
    ))(i)?;

    Ok((
        remaining_input,
        Expression::CaseWhen {
            condition,
            then_expr: Box::new(then_expr),
            else_expr: else_expr.map(Box::new),
        },
    ))
}

#[cfg(test)]
mod tests {
    use crate::{
        BinaryOperator, Column, ConditionBase, ConditionExpression, ConditionTree, Literal,
    };

    use super::*;

    #[test]
    fn it_displays() {
        let c1 = Column {
            name: String::from("foo"),
            alias: None,
            table: None,
            function: None,
        };

        let exp = Expression::CaseWhen {
            condition: ConditionExpression::ComparisonOp(ConditionTree {
                operator: BinaryOperator::Equal,
                left: Box::new(ConditionExpression::Base(ConditionBase::Field(c1.clone()))),
                right: Box::new(ConditionExpression::Base(ConditionBase::Literal(
                    Literal::Integer(0),
                ))),
            }),
            then_expr: Box::new(Expression::Column(c1.clone())),
            else_expr: Some(Box::new(Expression::Literal(Literal::Integer(1)))),
        };

        assert_eq!(format!("{}", exp), "CASE WHEN foo = 0 THEN foo ELSE 1 END");

        let exp_no_else = Expression::CaseWhen {
            condition: ConditionExpression::ComparisonOp(ConditionTree {
                operator: BinaryOperator::Equal,
                left: Box::new(ConditionExpression::Base(ConditionBase::Field(c1.clone()))),
                right: Box::new(ConditionExpression::Base(ConditionBase::Literal(
                    Literal::Integer(0),
                ))),
            }),
            then_expr: Box::new(Expression::Column(c1)),
            else_expr: None,
        };

        assert_eq!(format!("{}", exp_no_else), "CASE WHEN foo = 0 THEN foo END");
    }
}
