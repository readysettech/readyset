use nom::multispace;
use std::str;

use common::{as_alias, column_identifier_no_alias, integer_literal, Literal};
use column::Column;

#[derive(Debug, Clone, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub enum ArithmeticOperator {
    Add,
    Subtract,
    Multiply,
    Divide,
}

#[derive(Debug, Clone, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub enum ArithmeticBase {
    Column(Column),
    Scalar(Literal),
}

#[derive(Debug, Clone, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct ArithmeticExpression {
    pub op: ArithmeticOperator,
    pub left: ArithmeticBase,
    pub right: ArithmeticBase,
    pub alias: Option<String>,
}

impl ArithmeticExpression {
    pub fn new(
        op: ArithmeticOperator,
        left: ArithmeticBase,
        right: ArithmeticBase,
        alias: Option<String>,
    ) -> Self {
        Self {
            op: op,
            left: left,
            right: right,
            alias: alias,
        }
    }
}

/// Parse standard math operators.
/// TODO(malte): this doesn't currently observe operator precedence.
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
        | map!(column_identifier_no_alias, |ci| ArithmeticBase::Column(ci))
    )
);

/// Parse simple arithmetic expressions combining literals, and columns and literals.
/// TODO(malte): this doesn't currently support nested expressions.
named!(pub arithmetic_expression<&[u8], ArithmeticExpression>,
    complete!(chain!(
        left: arithmetic_base ~
        multispace? ~
        op: arithmetic_operator ~
        multispace? ~
        right: arithmetic_base ~
        alias: opt!(as_alias),
        || {
            ArithmeticExpression {
                op: op,
                left: left,
                right: right,
                alias: match alias {
                    None => None,
                    Some(a) => Some(String::from(a)),
                },
            }
        }
    ))
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_parses_arithmetic_expressions() {
        use column::FunctionExpression;
        use super::ArithmeticOperator::*;
        use super::ArithmeticBase::Scalar;
        use super::ArithmeticBase::Column as ABColumn;

        let lit_ae = [
            "5 + 42",
            "5+42",
            "5 * 42",
            "5 - 42",
            "5 / 42",
            "2 * 10 AS twenty ",
        ];

        // N.B. trailing space in "5 + foo " is required because `sql_identifier`'s keyword
        // detection requires a follow-up character (in practice, there always is one because we
        // use semicolon-terminated queries).
        let col_lit_ae = [
            "foo+5",
            "foo + 5",
            "5 + foo ",
            "foo * bar AS foobar",
            "MAX(foo)-3333",
        ];

        let expected_lit_ae = [
            ArithmeticExpression::new(Add, Scalar(5.into()), Scalar(42.into()), None),
            ArithmeticExpression::new(Add, Scalar(5.into()), Scalar(42.into()), None),
            ArithmeticExpression::new(Multiply, Scalar(5.into()), Scalar(42.into()), None),
            ArithmeticExpression::new(Subtract, Scalar(5.into()), Scalar(42.into()), None),
            ArithmeticExpression::new(Divide, Scalar(5.into()), Scalar(42.into()), None),
            ArithmeticExpression::new(
                Multiply,
                Scalar(2.into()),
                Scalar(10.into()),
                Some(String::from("twenty")),
            ),
        ];
        let expected_col_lit_ae = [
            ArithmeticExpression::new(Add, ABColumn("foo".into()), Scalar(5.into()), None),
            ArithmeticExpression::new(Add, ABColumn("foo".into()), Scalar(5.into()), None),
            ArithmeticExpression::new(Add, Scalar(5.into()), ABColumn("foo".into()), None),
            ArithmeticExpression::new(
                Multiply,
                ABColumn("foo".into()),
                ABColumn("bar".into()),
                Some(String::from("foobar")),
            ),
            ArithmeticExpression::new(
                Subtract,
                ABColumn(Column {
                    name: String::from("max(foo)"),
                    alias: None,
                    table: None,
                    function: Some(Box::new(FunctionExpression::Max("foo".into()))),
                }),
                Scalar(3333.into()),
                None,
            ),
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
