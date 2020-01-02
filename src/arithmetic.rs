use nom::character::complete::{multispace0, multispace1};
use std::{fmt, str};

use column::Column;
use common::FieldValueExpression::Arithmetic;
use common::{
    as_alias, column_identifier_no_alias, integer_literal, type_identifier, Literal, SqlType,
};
use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case};
use nom::combinator::{map, opt};
use nom::sequence::{terminated, tuple};
use nom::IResult;

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
            op,
            left,
            right,
            alias,
        }
    }
}

impl fmt::Display for ArithmeticOperator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ArithmeticOperator::Add => write!(f, "+"),
            ArithmeticOperator::Subtract => write!(f, "-"),
            ArithmeticOperator::Multiply => write!(f, "*"),
            ArithmeticOperator::Divide => write!(f, "/"),
        }
    }
}

impl fmt::Display for ArithmeticBase {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ArithmeticBase::Column(ref col) => write!(f, "{}", col),
            ArithmeticBase::Scalar(ref lit) => write!(f, "{}", lit.to_string()),
        }
    }
}

impl fmt::Display for ArithmeticExpression {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.alias {
            Some(ref alias) => write!(f, "{} {} {} AS {}", self.left, self.op, self.right, alias),
            None => write!(f, "{} {} {}", self.left, self.op, self.right),
        }
    }
}

fn arithmetic_cast_helper(i: &[u8]) -> IResult<&[u8], (ArithmeticBase, Option<SqlType>)> {
    let (remaining_input, (_, _, _, _, a_base, _, _, _, _sign, sql_type, _, _)) = tuple((
        tag_no_case("cast"),
        multispace0,
        tag("("),
        multispace0,
        // TODO(malte): should be arbitrary expr
        arithmetic_base,
        multispace1,
        tag_no_case("as"),
        multispace1,
        opt(terminated(tag_no_case("signed"), multispace1)),
        type_identifier,
        multispace0,
        tag(")"),
    ))(i)?;

    Ok((remaining_input, (a_base, Some(sql_type))))
}

pub fn arithmetic_cast(i: &[u8]) -> IResult<&[u8], (ArithmeticBase, Option<SqlType>)> {
    alt((arithmetic_cast_helper, map(arithmetic_base, |v| (v, None))))(i)
}

// Parse standard math operators.
// TODO(malte): this doesn't currently observe operator precedence.
pub fn arithmetic_operator(i: &[u8]) -> IResult<&[u8], ArithmeticOperator> {
    alt((
        map(tag("+"), |_| ArithmeticOperator::Add),
        map(tag("-"), |_| ArithmeticOperator::Subtract),
        map(tag("*"), |_| ArithmeticOperator::Multiply),
        map(tag("/"), |_| ArithmeticOperator::Divide),
    ))(i)
}

// Base case for nested arithmetic expressions: column name or literal.
pub fn arithmetic_base(i: &[u8]) -> IResult<&[u8], ArithmeticBase> {
    alt((
        map(integer_literal, |il| ArithmeticBase::Scalar(il)),
        map(column_identifier_no_alias, |ci| ArithmeticBase::Column(ci)),
    ))(i)
}

// Parse simple arithmetic expressions combining literals, and columns and literals.
// TODO(malte): this doesn't currently support nested expressions.
pub fn arithmetic_expression(i: &[u8]) -> IResult<&[u8], ArithmeticExpression> {
    let (remaining_input, (left, _, op, _, right, opt_alias)) = tuple((
        arithmetic_cast,
        multispace0,
        arithmetic_operator,
        multispace0,
        arithmetic_cast,
        opt(as_alias),
    ))(i)?;

    let alias = match opt_alias {
        None => None,
        Some(a) => Some(String::from(a)),
    };

    Ok((
        remaining_input,
        ArithmeticExpression {
            left: left.0,
            right: right.0,
            op,
            alias,
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_parses_arithmetic_expressions() {
        use super::ArithmeticBase::Column as ABColumn;
        use super::ArithmeticBase::Scalar;
        use super::ArithmeticOperator::*;
        use column::{FunctionArguments, FunctionExpression};

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
                    function: Some(Box::new(FunctionExpression::Max(
                        FunctionArguments::Column("foo".into()),
                    ))),
                }),
                Scalar(3333.into()),
                None,
            ),
        ];

        for (i, e) in lit_ae.iter().enumerate() {
            let res = arithmetic_expression(e.as_bytes());
            assert!(res.is_ok());
            assert_eq!(res.unwrap().1, expected_lit_ae[i]);
        }

        for (i, e) in col_lit_ae.iter().enumerate() {
            let res = arithmetic_expression(e.as_bytes());
            assert!(res.is_ok());
            assert_eq!(res.unwrap().1, expected_col_lit_ae[i]);
        }
    }

    #[test]
    fn it_displays_arithmetic_expressions() {
        use super::ArithmeticBase::Column as ABColumn;
        use super::ArithmeticBase::Scalar;
        use super::ArithmeticOperator::*;

        let expressions = [
            ArithmeticExpression::new(Add, ABColumn("foo".into()), Scalar(5.into()), None),
            ArithmeticExpression::new(Subtract, Scalar(5.into()), ABColumn("foo".into()), None),
            ArithmeticExpression::new(
                Multiply,
                ABColumn("foo".into()),
                ABColumn("bar".into()),
                None,
            ),
            ArithmeticExpression::new(Divide, Scalar(10.into()), Scalar(2.into()), None),
            ArithmeticExpression::new(
                Add,
                Scalar(10.into()),
                Scalar(2.into()),
                Some(String::from("bob")),
            ),
        ];

        let expected_strings = ["foo + 5", "5 - foo", "foo * bar", "10 / 2", "10 + 2 AS bob"];
        for (i, e) in expressions.iter().enumerate() {
            assert_eq!(expected_strings[i], format!("{}", e));
        }
    }

    #[test]
    fn it_parses_arithmetic_casts() {
        use super::ArithmeticBase::Column as ABColumn;
        use super::ArithmeticBase::Scalar;
        use super::ArithmeticOperator::*;

        let exprs = [
            "CAST(`t`.`foo` AS signed int) + CAST(`t`.`bar` AS signed int) ",
            "CAST(5 AS bigint) - foo ",
            "CAST(5 AS bigint) - foo AS 5_minus_foo",
        ];

        // XXX(malte): currently discards the cast and type information!
        let expected = [
            ArithmeticExpression::new(
                Add,
                ABColumn(Column::from("t.foo")),
                ABColumn(Column::from("t.bar")),
                None,
            ),
            ArithmeticExpression::new(Subtract, Scalar(5.into()), ABColumn("foo".into()), None),
            ArithmeticExpression::new(
                Subtract,
                Scalar(5.into()),
                ABColumn("foo".into()),
                Some("5_minus_foo".into()),
            ),
        ];

        for (i, e) in exprs.iter().enumerate() {
            let res = arithmetic_expression(e.as_bytes());
            assert!(res.is_ok(), "{} failed to parse", e);
            assert_eq!(res.unwrap().1, expected[i]);
        }
    }
}
