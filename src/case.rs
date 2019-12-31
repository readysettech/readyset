use std::fmt;

use column::Column;
use common::{column_identifier_no_alias, literal, Literal};
use condition::{condition_expr, ConditionExpression};

use nom::character::complete::multispace0;

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
        Ok(())
    }
}

named!(pub case_when_column<&[u8], CaseWhenExpression>,
       do_parse!(
           tag_no_case!("case when") >>
           multispace0 >>
           cond: condition_expr >>
           multispace0 >>
           tag_no_case!("then") >>
           multispace0 >>
           column: column_identifier_no_alias >>
           multispace0 >>
           else_value: opt!(do_parse!(
               tag_no_case!("else") >>
               multispace0 >>
               else_val: literal >>
               multispace0 >>
               (else_val)
           )) >>
           tag_no_case!("end") >>
           (CaseWhenExpression {
               condition: cond,
               then_expr: ColumnOrLiteral::Column(column),
               else_expr: else_value.map(|v| ColumnOrLiteral::Literal(v)),
           })
       )
);
