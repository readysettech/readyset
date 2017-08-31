use nom::multispace;
use std::str;

use common::{integer_literal, sql_identifier, Literal};
use column::Column;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArithmeticOperator {
    Add,
    Subtract,
    Multiply,
    Divide,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ArithmeticBase {
    Column(Column),
    Scalar(Literal),
}

#[derive(Debug, Clone, PartialEq)]
pub struct ArithmeticExpression {
    op: ArithmeticOperator,
    left: ArithmeticBase,
    right: ArithmeticBase,
}

impl ArithmeticExpression {
    pub fn new(op: ArithmeticOperator, left: ArithmeticBase, right: ArithmeticBase) -> Self {
        Self {
            op: op,
            left: left,
            right: right,
        }
    }
}

/// Parse standard math operators.
named!(pub arithmetic_operator<&[u8], ArithmeticOperator>,
    alt_complete!(
          map!(tag!("+"), |_| ArithmeticOperator::Add)
        | map!(tag!("-"), |_| ArithmeticOperator::Subtract)
        | map!(tag!("*"), |_| ArithmeticOperator::Multiply)
        | map!(tag!("/"), |_| ArithmeticOperator::Divide)
    )
);

/// Base case for nested arithmetic expressions: column name or literal.
named!(pub arithmetic_base<&[u8], ArithmeticBase>,
    alt_complete!(
          map!(integer_literal, |il| ArithmeticBase::Scalar(il))
        | map!(sql_identifier, |si| {
              let c = Column::from(str::from_utf8(si).unwrap());
              ArithmeticBase::Column(c)
          })
    )
);

/// Parse simple arithmetic expressions combining literals, and columns and literals.
named!(pub arithmetic_expression<&[u8], ArithmeticExpression>,
    complete!(chain!(
        left: arithmetic_base ~
        multispace? ~
        op: arithmetic_operator ~
        multispace? ~
        right: arithmetic_base,
        || {
            ArithmeticExpression {
                op: op,
                left: left,
                right: right
            }
        }
    ))
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_parses_arithmetic_expressions() {
        use super::ArithmeticOperator::*;
        use super::ArithmeticBase::*;

        let lit_ae = ["5 + 42", "5+42", "5 * 42", "5 - 42", "5 / 42"];
        let col_lit_ae = ["foo+5", "foo + 5", "5 + foo"];

        let expected_lit_ae = [
            ArithmeticExpression::new(Add, Scalar(5.into()), Scalar(42.into())),
            ArithmeticExpression::new(Add, Scalar(5.into()), Scalar(42.into())),
            ArithmeticExpression::new(Multiply, Scalar(5.into()), Scalar(42.into())),
            ArithmeticExpression::new(Subtract, Scalar(5.into()), Scalar(42.into())),
            ArithmeticExpression::new(Divide, Scalar(5.into()), Scalar(42.into())),
        ];
        let expected_col_lit_ae = [
            ArithmeticExpression::new(Add, Column("foo".into()), Scalar(5.into())),
            ArithmeticExpression::new(Add, Column("foo".into()), Scalar(5.into())),
            ArithmeticExpression::new(Add, Scalar(5.into()), Column("foo".into())),
        ];

        for (i, e) in lit_ae.iter().enumerate() {
            let res = arithmetic_expression(e.as_bytes());
            assert!(res.is_done());
            assert_eq!(res.unwrap().1, expected_lit_ae[i]);
        }

        for (i, e) in col_lit_ae.iter().enumerate() {
            let res = arithmetic_expression(e.as_bytes());
            assert!(res.is_done());
            assert_eq!(res.unwrap().1, expected_col_lit_ae[i]);
        }

    }
}
