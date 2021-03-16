use std::fmt;

use crate::column::Column;
use crate::common::{column_identifier_no_alias, literal, Literal};
use crate::condition::{condition_expr, ConditionExpression};

use nom::bytes::complete::tag_no_case;
use nom::character::complete::multispace0;
use nom::combinator::opt;
use nom::sequence::{delimited, terminated, tuple};
use nom::IResult;

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum ColumnOrLiteral {
    Column(Column),
    Literal(Literal),
}

impl fmt::Display for ColumnOrLiteral {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ColumnOrLiteral::Column(ref c) => write!(f, "{}", c)?,
            ColumnOrLiteral::Literal(ref l) => write!(f, "{}", l.to_string())?,
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct CaseWhenExpression {
    pub condition: ConditionExpression,
    pub then_expr: ColumnOrLiteral,
    pub else_expr: Option<ColumnOrLiteral>,
}

impl fmt::Display for CaseWhenExpression {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CASE WHEN {} THEN {}", self.condition, self.then_expr)?;
        if let Some(ref expr) = self.else_expr {
            write!(f, " ELSE {}", expr)?;
        }
        write!(f, " END")?;
        Ok(())
    }
}

pub fn case_when_column(i: &[u8]) -> IResult<&[u8], CaseWhenExpression> {
    let (remaining_input, (_, _, condition, _, _, _, column, _, else_val, _)) = tuple((
        tag_no_case("case when"),
        multispace0,
        condition_expr,
        multispace0,
        tag_no_case("then"),
        multispace0,
        column_identifier_no_alias,
        multispace0,
        opt(delimited(
            terminated(tag_no_case("else"), multispace0),
            literal,
            multispace0,
        )),
        tag_no_case("end"),
    ))(i)?;

    let then_expr = ColumnOrLiteral::Column(column);
    let else_expr = else_val.map(|v| ColumnOrLiteral::Literal(v));

    Ok((
        remaining_input,
        CaseWhenExpression {
            condition,
            then_expr,
            else_expr,
        },
    ))
}

#[cfg(test)]
mod tests {
    use crate::{BinaryOperator, ConditionBase, ConditionTree, Literal};

    use super::*;

    #[test]
    fn it_displays() {
        let c1 = Column {
            name: String::from("foo"),
            alias: None,
            table: None,
            function: None,
        };

        let exp = CaseWhenExpression {
            condition: ConditionExpression::ComparisonOp(ConditionTree {
                operator: BinaryOperator::Equal,
                left: Box::new(ConditionExpression::Base(ConditionBase::Field(c1.clone()))),
                right: Box::new(ConditionExpression::Base(ConditionBase::Literal(
                    Literal::Integer(0),
                ))),
            }),
            then_expr: ColumnOrLiteral::Column(c1.clone()),
            else_expr: Some(ColumnOrLiteral::Literal(Literal::Integer(1))),
        };

        assert_eq!(format!("{}", exp), "CASE WHEN foo = 0 THEN foo ELSE 1 END");

        let exp_no_else = CaseWhenExpression {
            else_expr: None,
            ..exp
        };

        assert_eq!(format!("{}", exp_no_else), "CASE WHEN foo = 0 THEN foo END");
    }
}
