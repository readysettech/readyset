use nom::types::CompleteByteSlice;
use std::fmt;

use column::Column;
use common::{column_identifier_no_alias, literal, opt_multispace, Literal};
use condition::{condition_expr, ConditionExpression};

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

named!(pub case_when_column<CompleteByteSlice, CaseWhenExpression>,
       do_parse!(
           tag_no_case!("case when") >>
           opt_multispace >>
           cond: condition_expr >>
           opt_multispace >>
           tag_no_case!("then") >>
           opt_multispace >>
           column: column_identifier_no_alias >>
           opt_multispace >>
           else_value: opt!(do_parse!(
               tag_no_case!("else") >>
               opt_multispace >>
               else_val: literal >>
               opt_multispace >>
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
