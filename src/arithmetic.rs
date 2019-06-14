use nom::multispace;
use nom::types::CompleteByteSlice;
use std::{fmt, str};

use column::Column;
use common::{
    as_alias, column_identifier_no_alias, integer_literal, opt_multispace, type_identifier,
    Literal, SqlType,
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

named!(pub arithmetic_cast<CompleteByteSlice, (ArithmeticBase, Option<SqlType>)>,
    alt!(
        do_parse!(
            tag_no_case!("cast") >>
            opt_multispace >>
            tag!("(") >>
            opt_multispace >>
            // TODO(malte): should be arbitrary expr
            v: arithmetic_base >>
            multispace >>
            tag_no_case!("as") >>
            multispace >>
            _sign: opt!(terminated!(tag_no_case!("signed"), multispace)) >>
            typ: type_identifier >>
            opt_multispace >>
            tag!(")") >>
            (v, Some(typ))
        ) |
        map!(arithmetic_base, |v| (v, None))
    )
);

// Parse standard math operators.
// TODO(malte): this doesn't currently observe operator precedence.
named!(pub arithmetic_operator<CompleteByteSlice, ArithmeticOperator>,
    alt!(
          map!(tag!("+"), |_| ArithmeticOperator::Add)
        | map!(tag!("-"), |_| ArithmeticOperator::Subtract)
        | map!(tag!("*"), |_| ArithmeticOperator::Multiply)
        | map!(tag!("/"), |_| ArithmeticOperator::Divide)
    )
);

// Base case for nested arithmetic expressions: column name or literal.
named!(pub arithmetic_base<CompleteByteSlice, ArithmeticBase>,
    alt!(
          map!(integer_literal, |il| ArithmeticBase::Scalar(il))
        | map!(column_identifier_no_alias, |ci| ArithmeticBase::Column(ci))
    )
);

// Parse simple arithmetic expressions combining literals, and columns and literals.
// TODO(malte): this doesn't currently support nested expressions.
named!(pub arithmetic_expression<CompleteByteSlice, ArithmeticExpression>,
    do_parse!(
        left: arithmetic_cast >>
        opt_multispace >>
        op: arithmetic_operator >>
        opt_multispace >>
        right: arithmetic_cast >>
        alias: opt!(as_alias) >>
        (ArithmeticExpression {
            op: op,
            // TODO(malte): discards casts
            left: left.0,
            right: right.0,
            alias: match alias {
                None => None,
                Some(a) => Some(String::from(a)),
            },
        })
    )
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_parses_arithmetic_expressions() {
        use super::ArithmeticBase::Column as ABColumn;
        use super::ArithmeticBase::Scalar;
        use super::ArithmeticOperator::*;
        use column::FunctionExpression;

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
            let res = arithmetic_expression(CompleteByteSlice(e.as_bytes()));
            assert!(res.is_ok());
            assert_eq!(res.unwrap().1, expected_lit_ae[i]);
        }

        for (i, e) in col_lit_ae.iter().enumerate() {
            let res = arithmetic_expression(CompleteByteSlice(e.as_bytes()));
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
            let res = arithmetic_expression(CompleteByteSlice(e.as_bytes()));
            assert!(res.is_ok(), "{} failed to parse", e);
            assert_eq!(res.unwrap().1, expected[i]);
        }
    }

}
