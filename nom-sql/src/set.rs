use std::{fmt, str};

use itertools::Itertools;
use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case};
use nom::character::complete::{multispace0, multispace1};
use nom::combinator::{map, opt};
use nom::multi::separated_list1;
use nom::sequence::{separated_pair, terminated, tuple};
use nom::IResult;
use serde::{Deserialize, Serialize};

use crate::common::statement_terminator;
use crate::expression::{expression, scoped_var};
use crate::{Dialect, Expression};

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum SetStatement {
    Variable(SetVariables),
    Names(SetNames),
}

impl fmt::Display for SetStatement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SET ")?;
        match self {
            Self::Variable(set) => write!(f, "{}", set)?,
            Self::Names(set) => write!(f, "{}", set)?,
        };
        Ok(())
    }
}

impl SetStatement {
    pub fn variables(&self) -> Option<&[(Variable, Expression)]> {
        match self {
            SetStatement::Names(_) => None,
            SetStatement::Variable(set) => Some(&set.variables),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum Variable {
    User(String),
    Local(String),
    Global(String),
    Session(String),
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct SetVariables {
    /// A list of variables and their assigned values
    pub variables: Vec<(Variable, Expression)>,
}

impl Variable {
    /// If the variable is one of Local, Global or Session, returns the variable name
    pub fn as_non_user_var(&self) -> Option<&str> {
        match self {
            Variable::Local(v) | Variable::Session(v) | Variable::Global(v) => Some(v.as_str()),
            _ => None,
        }
    }

    /// If the variable is a User variable, returns the variable name
    pub fn as_user_var(&self) -> Option<&str> {
        match self {
            Variable::User(v) => Some(v.as_str()),
            _ => None,
        }
    }
}

impl fmt::Display for Variable {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Variable::Global(var) => write!(f, "@@GLOBAL.{}", var),
            Variable::Session(var) => write!(f, "@@SESSION.{}", var),
            Variable::Local(var) => write!(f, "@@LOCAL.{}", var),
            Variable::User(var) => write!(f, "@{}", var),
        }
    }
}

impl fmt::Display for SetVariables {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            self.variables
                .iter()
                .map(|(var, value)| format!("{} = {}", var, value))
                .join(", ")
        )
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct SetNames {
    pub charset: String,
    pub collation: Option<String>,
}

impl fmt::Display for SetNames {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "NAMES '{}'", &self.charset)?;
        if let Some(collation) = self.collation.as_ref() {
            write!(f, " COLLATE '{}'", collation)?;
        }
        Ok(())
    }
}

/// check for one of three ways to specify scope and reformat to a single formatting. Returns none
/// if scope is not specified
pub fn varkind(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], Variable> {
    move |i| {
        let (i, scope) = alt((
            scoped_var(dialect),
            map(
                separated_pair(tag_no_case("GLOBAL"), multispace1, dialect.identifier()),
                |(_, ident)| Variable::Global(ident.to_ascii_lowercase()),
            ),
            map(
                separated_pair(tag_no_case("SESSION"), multispace1, dialect.identifier()),
                |(_, ident)| Variable::Session(ident.to_ascii_lowercase()),
            ),
            map(
                separated_pair(tag_no_case("LOCAL"), multispace1, dialect.identifier()),
                |(_, ident)| Variable::Local(ident.to_ascii_lowercase()),
            ),
            map(dialect.identifier(), |ident| {
                Variable::Local(ident.to_ascii_lowercase())
            }),
        ))(i)?;

        //scope may be followed by '.' or multispace
        let (i, _) = alt((tag("."), multispace0))(i)?;
        Ok((i, scope))
    }
}

pub fn set(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], SetStatement> {
    move |i| {
        let (i, _) = tag_no_case("set")(i)?;
        let (i, _) = multispace1(i)?;
        let (i, statement) = alt((
            map(set_variable(dialect), SetStatement::Variable),
            map(set_names(dialect), SetStatement::Names),
        ))(i)?;

        Ok((i, statement))
    }
}

fn set_variable(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], SetVariables> {
    move |i| {
        let (remaining_input, variables) = terminated(
            separated_list1(
                tuple((tag_no_case(","), multispace0)),
                map(
                    tuple((
                        varkind(dialect),
                        multispace0,
                        alt((tag_no_case("="), tag_no_case(":="))),
                        multispace0,
                        expression(dialect),
                    )),
                    |(variable, _, _, _, value)| (variable, value),
                ),
            ),
            statement_terminator,
        )(i)?;

        Ok((remaining_input, SetVariables { variables }))
    }
}

fn set_names(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], SetNames> {
    move |i| {
        let (i, _) = tag_no_case("names")(i)?;
        let (i, _) = multispace1(i)?;
        let (i, charset) = dialect.utf8_string_literal()(i)?;
        let (i, collation) = opt(move |i| {
            let (i, _) = multispace1(i)?;
            let (i, _) = tag_no_case("collate")(i)?;
            let (i, _) = multispace1(i)?;
            let (i, collation) = dialect.utf8_string_literal()(i)?;
            Ok((i, collation))
        })(i)?;

        Ok((i, SetNames { charset, collation }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_set() {
        let qstring = "SET SQL_AUTO_IS_NULL = 0;";
        let res = set(Dialect::MySQL)(qstring.as_bytes());
        assert_eq!(
            res.unwrap().1,
            SetStatement::Variable(SetVariables {
                variables: vec!((
                    Variable::Local("sql_auto_is_null".to_owned()),
                    Expression::Literal(0.into())
                )),
            })
        );
    }

    #[test]
    fn user_defined_vars() {
        let qstring = "SET @var = 123;";
        let res = set(Dialect::MySQL)(qstring.as_bytes());
        assert_eq!(
            res.unwrap().1,
            SetStatement::Variable(SetVariables {
                variables: vec!((
                    Variable::User("var".to_owned()),
                    Expression::Literal(123.into())
                )),
            })
        );
    }

    #[test]
    fn format_set() {
        let qstring = "set autocommit=1";
        let expected = "SET @@LOCAL.autocommit = 1";
        let res = set(Dialect::MySQL)(qstring.as_bytes());
        assert_eq!(format!("{}", res.unwrap().1), expected);
    }

    #[test]
    fn global_set() {
        let qstring1 = "set gloBal var = 2";
        let qstring2 = "set @@gLobal.var = 2";
        let expected = "SET @@GLOBAL.var = 2";
        let res1 = set(Dialect::MySQL)(qstring1.as_bytes());
        let res2 = set(Dialect::MySQL)(qstring2.as_bytes());
        assert_eq!(format!("{}", res1.unwrap().1), expected);
        assert_eq!(format!("{}", res2.unwrap().1), expected);
    }

    #[test]
    fn session_set() {
        let qstring1 = "set @@Session.var = 1";
        let qstring2 = "set @@var = 1";
        let qstring3 = "set SeSsion var = 1";
        let expected = "SET @@SESSION.var = 1";
        let res1 = set(Dialect::MySQL)(qstring1.as_bytes());
        let res2 = set(Dialect::MySQL)(qstring2.as_bytes());
        let res3 = set(Dialect::MySQL)(qstring3.as_bytes());
        assert_eq!(format!("{}", res1.unwrap().1), expected);
        assert_eq!(format!("{}", res2.unwrap().1), expected);
        assert_eq!(format!("{}", res3.unwrap().1), expected);
    }

    #[test]
    fn local_set() {
        let qstring1 = "set lOcal var = 2";
        let qstring2 = "set @@local.var = 2";
        let expected = "SET @@LOCAL.var = 2";
        let res1 = set(Dialect::MySQL)(qstring1.as_bytes());
        let res2 = set(Dialect::MySQL)(qstring2.as_bytes());
        assert_eq!(format!("{}", res1.unwrap().1), expected);
        assert_eq!(format!("{}", res2.unwrap().1), expected);
    }

    #[test]
    fn set_names() {
        let qstring1 = "SET NAMES 'iso8660'";
        let qstring2 = "set names 'utf8mb4' collate 'utf8mb4_unicode_ci'";
        let res1 = set(Dialect::MySQL)(qstring1.as_bytes()).unwrap().1;
        let res2 = set(Dialect::MySQL)(qstring2.as_bytes()).unwrap().1;
        assert_eq!(
            res1,
            SetStatement::Names(SetNames {
                charset: "iso8660".to_string(),
                collation: None
            })
        );
        assert_eq!(
            res2,
            SetStatement::Names(SetNames {
                charset: "utf8mb4".to_string(),
                collation: Some("utf8mb4_unicode_ci".to_string())
            })
        );
    }

    #[test]
    fn expression_set() {
        let qstring = "SET @myvar = 100 + 200;";
        let res = set(Dialect::MySQL)(qstring.as_bytes());
        assert_eq!(
            res.unwrap().1,
            SetStatement::Variable(SetVariables {
                variables: vec!((
                    Variable::User("myvar".to_owned()),
                    Expression::BinaryOp {
                        lhs: Box::new(Expression::Literal(100.into())),
                        op: crate::BinaryOperator::Add,
                        rhs: Box::new(Expression::Literal(200.into())),
                    }
                )),
            })
        );
    }

    #[test]
    fn list_set() {
        let qstring = "SET @myvar = 100 + 200, @@notmyvar = 'value', @@Global.g = @@global.V;";
        let res = set(Dialect::MySQL)(qstring.as_bytes());
        assert_eq!(
            res.unwrap().1,
            SetStatement::Variable(SetVariables {
                variables: vec!(
                    (
                        Variable::User("myvar".to_owned()),
                        Expression::BinaryOp {
                            lhs: Box::new(Expression::Literal(100.into())),
                            op: crate::BinaryOperator::Add,
                            rhs: Box::new(Expression::Literal(200.into())),
                        }
                    ),
                    (
                        Variable::Session("notmyvar".to_owned()),
                        Expression::Literal("value".into()),
                    ),
                    (
                        Variable::Global("g".to_owned()),
                        Expression::Variable(Variable::Global("v".into())),
                    )
                ),
            })
        );
    }
}
