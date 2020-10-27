use std::{fmt, str};

use nom::{
    branch::alt,
    bytes::complete::{tag, tag_no_case},
    character::complete::{multispace0, multispace1},
    combinator::{map, opt},
    lib::std::fmt::Formatter,
    multi::many0,
    sequence::{delimited, pair, preceded, separated_pair, terminated, tuple},
    Err::Error,
    IResult,
};

use crate::{
    column::Column,
    common::{
        as_alias, column_identifier_no_alias, integer_literal, type_identifier, Literal, SqlType,
    },
};

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
    Bracketed(Box<Arithmetic>),
}

#[derive(Debug, Clone, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub enum ArithmeticItem {
    Base(ArithmeticBase),
    Expr(Box<Arithmetic>),
}

#[derive(Debug, Clone, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct Arithmetic {
    pub op: ArithmeticOperator,
    pub left: ArithmeticItem,
    pub right: ArithmeticItem,
}

impl Arithmetic {
    pub fn new(op: ArithmeticOperator, left: ArithmeticBase, right: ArithmeticBase) -> Self {
        Self {
            op,
            left: ArithmeticItem::Base(left),
            right: ArithmeticItem::Base(right),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct ArithmeticExpression {
    pub ari: Arithmetic,
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
            ari: Arithmetic {
                op,
                left: ArithmeticItem::Base(left),
                right: ArithmeticItem::Base(right),
            },
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
            ArithmeticBase::Bracketed(ref ari) => write!(f, "({})", ari),
        }
    }
}

impl fmt::Display for ArithmeticItem {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ArithmeticItem::Base(ref b) => write!(f, "{}", b),
            ArithmeticItem::Expr(ref expr) => write!(f, "{}", expr),
        }
    }
}

impl fmt::Display for Arithmetic {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{} {} {}", self.left, self.op, self.right)
    }
}

impl fmt::Display for ArithmeticExpression {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.alias {
            Some(ref alias) => write!(f, "{} AS {}", self.ari, alias),
            None => write!(f, "{}", self.ari),
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

pub fn add_sub_operator(i: &[u8]) -> IResult<&[u8], ArithmeticOperator> {
    alt((
        map(tag("+"), |_| ArithmeticOperator::Add),
        map(tag("-"), |_| ArithmeticOperator::Subtract),
    ))(i)
}

pub fn mul_div_operator(i: &[u8]) -> IResult<&[u8], ArithmeticOperator> {
    alt((
        map(tag("*"), |_| ArithmeticOperator::Multiply),
        map(tag("/"), |_| ArithmeticOperator::Divide),
    ))(i)
}

// Base case for nested arithmetic expressions: column name or literal.
pub fn arithmetic_base(i: &[u8]) -> IResult<&[u8], ArithmeticBase> {
    alt((
        map(integer_literal, ArithmeticBase::Scalar),
        map(column_identifier_no_alias, ArithmeticBase::Column),
        map(
            delimited(
                terminated(tag("("), multispace0),
                arithmetic,
                preceded(multispace0, tag(")")),
            ),
            |ari| ArithmeticBase::Bracketed(Box::new(ari)),
        ),
    ))(i)
}

fn arithmetic(i: &[u8]) -> IResult<&[u8], Arithmetic> {
    let res = expr(i)?;
    match res.1 {
        ArithmeticItem::Base(ArithmeticBase::Column(_))
        | ArithmeticItem::Base(ArithmeticBase::Scalar(_)) => {
            Err(Error((res.0, nom::error::ErrorKind::Tag)))
        } // no operator
        ArithmeticItem::Base(ArithmeticBase::Bracketed(expr)) => Ok((res.0, *expr)),
        ArithmeticItem::Expr(expr) => Ok((res.0, *expr)),
    }
}

fn expr(i: &[u8]) -> IResult<&[u8], ArithmeticItem> {
    map(pair(term, many0(expr_rest)), |(item, rs)| {
        rs.into_iter().fold(item, |acc, (o, r)| {
            ArithmeticItem::Expr(Box::new(Arithmetic {
                op: o,
                left: acc,
                right: r,
            }))
        })
    })(i)
}

fn expr_rest(i: &[u8]) -> IResult<&[u8], (ArithmeticOperator, ArithmeticItem)> {
    separated_pair(preceded(multispace0, add_sub_operator), multispace0, term)(i)
}

fn term(i: &[u8]) -> IResult<&[u8], ArithmeticItem> {
    map(pair(arithmetic_cast, many0(term_rest)), |(b, rs)| {
        rs.into_iter()
            .fold(ArithmeticItem::Base(b.0), |acc, (o, r)| {
                ArithmeticItem::Expr(Box::new(Arithmetic {
                    op: o,
                    left: acc,
                    right: r,
                }))
            })
    })(i)
}

fn term_rest(i: &[u8]) -> IResult<&[u8], (ArithmeticOperator, ArithmeticItem)> {
    separated_pair(
        preceded(multispace0, mul_div_operator),
        multispace0,
        map(arithmetic_cast, |b| ArithmeticItem::Base(b.0)),
    )(i)
}

// Parse simple arithmetic expressions combining literals, and columns and literals.
pub fn arithmetic_expression(i: &[u8]) -> IResult<&[u8], ArithmeticExpression> {
    map(pair(arithmetic, opt(as_alias)), |(ari, opt_alias)| {
        ArithmeticExpression {
            ari,
            alias: opt_alias.map(String::from),
        }
    })(i)
}

#[cfg(test)]
mod tests {
    use crate::arithmetic::{
        ArithmeticBase::Scalar,
        ArithmeticOperator::{Add, Divide, Multiply, Subtract},
    };

    use super::*;

    #[test]
    fn it_parses_arithmetic_expressions() {
        use super::{
            ArithmeticBase::{Column as ABColumn, Scalar},
            ArithmeticOperator::*,
        };
        use crate::column::{FunctionArguments, FunctionExpression};

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
        use super::{
            ArithmeticBase::{Column as ABColumn, Scalar},
            ArithmeticOperator::*,
        };

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
        use super::{
            ArithmeticBase::{Column as ABColumn, Scalar},
            ArithmeticOperator::*,
        };

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

    #[test]
    fn nested_arithmetic() {
        let qs = [
            "1 + 1",
            "1 + 2 - 3",
            "1 + 2 * 3",
            "2 * 3 - 1 / 3",
            "3 * (1 + 2)",
        ];

        let expects =
            [
                Arithmetic::new(Add, Scalar(1.into()), Scalar(1.into())),
                Arithmetic {
                    op: Subtract,
                    left: ArithmeticItem::Expr(Box::new(Arithmetic::new(
                        Add,
                        Scalar(1.into()),
                        Scalar(2.into()),
                    ))),
                    right: ArithmeticItem::Base(Scalar(3.into())),
                },
                Arithmetic {
                    op: Add,
                    left: ArithmeticItem::Base(Scalar(1.into())),
                    right: ArithmeticItem::Expr(Box::new(Arithmetic::new(
                        Multiply,
                        Scalar(2.into()),
                        Scalar(3.into()),
                    ))),
                },
                Arithmetic {
                    op: Subtract,
                    left: ArithmeticItem::Expr(Box::new(Arithmetic::new(
                        Multiply,
                        Scalar(2.into()),
                        Scalar(3.into()),
                    ))),
                    right: ArithmeticItem::Expr(Box::new(Arithmetic::new(
                        Divide,
                        Scalar(1.into()),
                        Scalar(3.into()),
                    ))),
                },
                Arithmetic {
                    op: Multiply,
                    left: ArithmeticItem::Base(Scalar(3.into())),
                    right: ArithmeticItem::Base(ArithmeticBase::Bracketed(Box::new(
                        Arithmetic::new(Add, Scalar(1.into()), Scalar(2.into())),
                    ))),
                },
            ];

        for (i, e) in qs.iter().enumerate() {
            let res = arithmetic(e.as_bytes());
            let ari = res.unwrap().1;
            assert_eq!(ari, expects[i]);
            assert_eq!(format!("{}", ari), qs[i]);
        }
    }
}
