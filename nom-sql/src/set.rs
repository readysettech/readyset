use std::fmt::Display;
use std::{fmt, str};

use itertools::Itertools;
use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case};
use nom::combinator::{map, opt};
use nom::multi::separated_list1;
use nom::sequence::{terminated, tuple};
use nom::{IResult, Parser};
use serde::{Deserialize, Serialize};

use crate::common::{literal, statement_terminator};
use crate::expression::expression;
use crate::whitespace::{whitespace0, whitespace1};
use crate::{Dialect, Expression, Literal, SqlIdentifier};

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum SetStatement {
    Variable(SetVariables),
    Names(SetNames),
    PostgresParameter(SetPostgresParameter),
}

impl Display for SetStatement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SET ")?;
        match self {
            Self::Variable(set) => write!(f, "{}", set),
            Self::Names(set) => write!(f, "{}", set),
            Self::PostgresParameter(set) => write!(f, "{}", set),
        }
    }
}

impl SetStatement {
    pub fn variables(&self) -> Option<&[(Variable, Expression)]> {
        match self {
            SetStatement::Names(_) | SetStatement::PostgresParameter { .. } => None,
            SetStatement::Variable(set) => Some(&set.variables),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum PostgresParameterScope {
    Session,
    Local,
}

impl Display for PostgresParameterScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PostgresParameterScope::Session => write!(f, "SESSION"),
            PostgresParameterScope::Local => write!(f, "LOCAL"),
        }
    }
}

fn postgres_parameter_scope(i: &[u8]) -> IResult<&[u8], PostgresParameterScope> {
    alt((
        map(tag_no_case("session"), |_| PostgresParameterScope::Session),
        map(tag_no_case("local"), |_| PostgresParameterScope::Local),
    ))(i)
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum SetPostgresParameterValue {
    Default,
    Value(PostgresParameterValue),
}

impl Display for SetPostgresParameterValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SetPostgresParameterValue::Default => write!(f, "DEFAULT"),
            SetPostgresParameterValue::Value(val) => write!(f, "{}", val),
        }
    }
}

fn set_postgres_parameter_value(i: &[u8]) -> IResult<&[u8], SetPostgresParameterValue> {
    alt((
        map(tag_no_case("default"), |_| {
            SetPostgresParameterValue::Default
        }),
        map(postgres_parameter_value, SetPostgresParameterValue::Value),
    ))(i)
}

/// A *single* value which can be used as the value for a postgres parameter
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum PostgresParameterValueInner {
    Identifier(SqlIdentifier),
    Literal(Literal),
}

impl Display for PostgresParameterValueInner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PostgresParameterValueInner::Identifier(ident) => write!(f, "{}", ident),
            PostgresParameterValueInner::Literal(lit) => write!(f, "{}", lit),
        }
    }
}

/// The value for a postgres parameter, which can either be an identifier, a literal, or a list of
/// those
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum PostgresParameterValue {
    Single(PostgresParameterValueInner),
    List(Vec<PostgresParameterValueInner>),
}

impl PostgresParameterValue {
    /// Construct a [`PostgresParameterValue`] for a single literal
    pub fn literal<L>(literal: L) -> Self
    where
        Literal: From<L>,
    {
        Self::Single(PostgresParameterValueInner::Literal(literal.into()))
    }

    /// Construct a [`PostgresParameterValue`] for a single identifier
    pub fn identifier<I>(ident: I) -> Self
    where
        SqlIdentifier: From<I>,
    {
        Self::Single(PostgresParameterValueInner::Identifier(ident.into()))
    }

    /// Construct a [`PostgresParameterValue`] from a list of literal values
    pub fn list<L, I>(vals: L) -> Self
    where
        L: IntoIterator<Item = I>,
        Literal: From<I>,
    {
        Self::List(
            vals.into_iter()
                .map(|v| PostgresParameterValueInner::Literal(v.into()))
                .collect(),
        )
    }
}

impl Display for PostgresParameterValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PostgresParameterValue::Single(v) => write!(f, "{}", v),
            PostgresParameterValue::List(l) => write!(f, "{}", l.iter().join(", ")),
        }
    }
}

fn postgres_parameter_value_inner(i: &[u8]) -> IResult<&[u8], PostgresParameterValueInner> {
    alt((
        map(
            literal(Dialect::PostgreSQL),
            PostgresParameterValueInner::Literal,
        ),
        map(
            Dialect::PostgreSQL.identifier(),
            PostgresParameterValueInner::Identifier,
        ),
        map(tag_no_case("on"), |_| {
            PostgresParameterValueInner::Identifier("on".into())
        }),
    ))(i)
}

fn postgres_parameter_value(i: &[u8]) -> IResult<&[u8], PostgresParameterValue> {
    let (i, vals) = separated_list1(
        terminated(tag(","), whitespace0),
        postgres_parameter_value_inner,
    )(i)?;

    if vals.len() == 1 {
        Ok((
            i,
            PostgresParameterValue::Single(vals.into_iter().next().unwrap()),
        ))
    } else {
        Ok((i, PostgresParameterValue::List(vals)))
    }
}

/// Scope for a [`Variable`]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum VariableScope {
    User,
    Local,
    Global,
    Session,
}

impl Display for VariableScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            VariableScope::User => Ok(()),
            VariableScope::Local => write!(f, "LOCAL"),
            VariableScope::Global => write!(f, "GLOBAL"),
            VariableScope::Session => write!(f, "SESSION"),
        }
    }
}

pub(crate) fn variable_scope_prefix(i: &[u8]) -> IResult<&[u8], VariableScope> {
    alt((
        map(tag_no_case("@@LOCAL."), |_| VariableScope::Local),
        map(tag_no_case("@@GLOBAL."), |_| VariableScope::Global),
        map(tag_no_case("@@SESSION."), |_| VariableScope::Session),
        map(tag_no_case("@@"), |_| VariableScope::Session),
        map(tag_no_case("@"), |_| VariableScope::User),
    ))(i)
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct Variable {
    pub scope: VariableScope,
    pub name: SqlIdentifier,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct SetVariables {
    /// A list of variables and their assigned values
    pub variables: Vec<(Variable, Expression)>,
}

impl Variable {
    /// If the variable is one of Local, Global or Session, returns the variable name
    pub fn as_non_user_var(&self) -> Option<&str> {
        if self.scope == VariableScope::User {
            None
        } else {
            Some(&self.name)
        }
    }
}

impl Display for Variable {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.scope == VariableScope::User {
            write!(f, "@")?;
        } else {
            write!(f, "@@{}.", self.scope)?;
        }
        write!(f, "{}", self.name)
    }
}

impl Display for SetVariables {
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

impl Display for SetNames {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "NAMES '{}'", &self.charset)?;
        if let Some(collation) = self.collation.as_ref() {
            write!(f, " COLLATE '{}'", collation)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct SetPostgresParameter {
    pub scope: Option<PostgresParameterScope>,
    pub name: SqlIdentifier,
    pub value: SetPostgresParameterValue,
}

impl Display for SetPostgresParameter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(scope) = self.scope {
            write!(f, "{} ", scope)?;
        }
        write!(f, "{} = {}", self.name, self.value)
    }
}

fn set_variable_scope_prefix(i: &[u8]) -> IResult<&[u8], VariableScope> {
    alt((
        variable_scope_prefix,
        map(terminated(tag_no_case("GLOBAL"), whitespace1), |_| {
            VariableScope::Global
        }),
        map(terminated(tag_no_case("SESSION"), whitespace1), |_| {
            VariableScope::Session
        }),
        map(terminated(tag_no_case("LOCAL"), whitespace1), |_| {
            VariableScope::Local
        }),
    ))(i)
}

/// check for one of three ways to specify scope and reformat to a single formatting. Returns none
/// if scope is not specified
fn variable(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], Variable> {
    move |i| {
        let (i, scope) = set_variable_scope_prefix
            .or(|i| Ok((i, VariableScope::Local)))
            .parse(i)?;
        let (i, name) = dialect
            .identifier()
            .map(|ident| ident.to_ascii_lowercase().into())
            .parse(i)?;
        Ok((i, Variable { scope, name }))
    }
}

pub fn set(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], SetStatement> {
    move |i| {
        let (i, _) = tag_no_case("set")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, statement) = alt((
            move |i| {
                if dialect == Dialect::PostgreSQL {
                    set_postgres_parameter
                        .map(SetStatement::PostgresParameter)
                        .parse(i)
                } else {
                    Err(nom::Err::Error(nom::error::Error::new(
                        i,
                        nom::error::ErrorKind::Tag,
                    )))
                }
            },
            map(set_variables(dialect), SetStatement::Variable),
            map(set_names(dialect), SetStatement::Names),
        ))(i)?;

        Ok((i, statement))
    }
}

fn set_variable(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], (Variable, Expression)> {
    move |i| {
        let (i, variable) = variable(dialect)(i)?;
        let (i, _) = whitespace0(i)?;
        let (i, _) = alt((tag_no_case("="), tag_no_case(":=")))(i)?;
        let (i, _) = whitespace0(i)?;
        let (i, value) = expression(dialect)(i)?;
        Ok((i, (variable, value)))
    }
}

fn set_variables(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], SetVariables> {
    move |i| {
        let (remaining_input, variables) = terminated(
            separated_list1(
                tuple((tag_no_case(","), whitespace0)),
                set_variable(dialect),
            ),
            statement_terminator,
        )(i)?;

        Ok((remaining_input, SetVariables { variables }))
    }
}

fn set_names(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], SetNames> {
    move |i| {
        let (i, _) = tag_no_case("names")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, charset) = dialect.utf8_string_literal()(i)?;
        let (i, collation) = opt(move |i| {
            let (i, _) = whitespace1(i)?;
            let (i, _) = tag_no_case("collate")(i)?;
            let (i, _) = whitespace1(i)?;
            let (i, collation) = dialect.utf8_string_literal()(i)?;
            Ok((i, collation))
        })(i)?;

        Ok((i, SetNames { charset, collation }))
    }
}

fn set_postgres_parameter(i: &[u8]) -> IResult<&[u8], SetPostgresParameter> {
    let (i, scope) = opt(terminated(postgres_parameter_scope, whitespace1))(i)?;
    let (i, name) = Dialect::PostgreSQL.identifier()(i)?;
    let (i, _) = whitespace0(i)?;
    let (i, _) = alt((terminated(tag_no_case("to"), whitespace1), tag("=")))(i)?;
    let (i, _) = whitespace0(i)?;
    let (i, value) = set_postgres_parameter_value(i)?;

    Ok((i, SetPostgresParameter { scope, name, value }))
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
                    Variable {
                        scope: VariableScope::Local,
                        name: "sql_auto_is_null".into()
                    },
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
                    Variable {
                        scope: VariableScope::User,
                        name: "var".into()
                    },
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
        let res1 = test_parse!(set(Dialect::MySQL), qstring1.as_bytes());
        let res2 = test_parse!(set(Dialect::MySQL), qstring2.as_bytes());
        assert_eq!(format!("{}", res1), expected);
        assert_eq!(format!("{}", res2), expected);
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
                    Variable {
                        scope: VariableScope::User,
                        name: "myvar".into()
                    },
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
                        Variable {
                            scope: VariableScope::User,
                            name: "myvar".into()
                        },
                        Expression::BinaryOp {
                            lhs: Box::new(Expression::Literal(100.into())),
                            op: crate::BinaryOperator::Add,
                            rhs: Box::new(Expression::Literal(200.into())),
                        }
                    ),
                    (
                        Variable {
                            scope: VariableScope::Session,
                            name: "notmyvar".into()
                        },
                        Expression::Literal("value".into()),
                    ),
                    (
                        Variable {
                            scope: VariableScope::Global,
                            name: "g".into()
                        },
                        Expression::Variable(Variable {
                            scope: VariableScope::Global,
                            name: "v".into()
                        }),
                    )
                ),
            })
        );
    }

    /// https://www.postgresql.org/docs/current/sql-set.html
    mod postgres {
        use super::*;

        #[test]
        fn set_client_min_messages() {
            let res = test_parse!(
                set(Dialect::PostgreSQL),
                b"SET client_min_messages TO 'warning'"
            );
            let roundtripped = res.to_string();
            assert_eq!(roundtripped, "SET client_min_messages = 'warning'");

            assert_eq!(
                res,
                SetStatement::PostgresParameter(SetPostgresParameter {
                    scope: None,
                    name: "client_min_messages".into(),
                    value: SetPostgresParameterValue::Value(PostgresParameterValue::Single(
                        PostgresParameterValueInner::Literal("warning".into())
                    ))
                })
            );
        }

        #[test]
        fn set_session_timezone() {
            let res = test_parse!(set(Dialect::PostgreSQL), b"SET SESSION timezone TO 'UTC'");
            let roundtripped = res.to_string();
            assert_eq!(roundtripped, "SET SESSION timezone = 'UTC'");

            assert_eq!(
                res,
                SetStatement::PostgresParameter(SetPostgresParameter {
                    scope: Some(PostgresParameterScope::Session),
                    name: "timezone".into(),
                    value: SetPostgresParameterValue::Value(PostgresParameterValue::Single(
                        PostgresParameterValueInner::Literal("UTC".into())
                    ))
                })
            );
        }

        #[test]
        fn set_names() {
            let res = test_parse!(set(Dialect::PostgreSQL), b"SET NAMES 'UTF8'");
            let roundtripped = res.to_string();
            assert_eq!(roundtripped, "SET NAMES 'UTF8'");

            assert_eq!(
                res,
                SetStatement::Names(SetNames {
                    charset: "UTF8".to_string(),
                    collation: None
                })
            );
        }

        #[test]
        fn set_default() {
            let res1 = test_parse!(set(Dialect::PostgreSQL), b"SET SESSION timezone TO DEFAULT");
            let res2 = test_parse!(set(Dialect::PostgreSQL), b"SET SESSION timezone = DEFAULT");
            let roundtripped = res1.to_string();
            assert_eq!(roundtripped, "SET SESSION timezone = DEFAULT");

            assert_eq!(
                res1,
                SetStatement::PostgresParameter(SetPostgresParameter {
                    scope: Some(PostgresParameterScope::Session),
                    name: "timezone".into(),
                    value: SetPostgresParameterValue::Default
                })
            );
            assert_eq!(res1, res2);
        }

        #[test]
        fn set_list() {
            let res = test_parse!(set(Dialect::PostgreSQL), b"SET LOCAL whatever = 'x', 1, hi");
            let roundtripped = res.to_string();
            assert_eq!(roundtripped, "SET LOCAL whatever = 'x', 1, hi");

            assert_eq!(
                res,
                SetStatement::PostgresParameter(SetPostgresParameter {
                    scope: Some(PostgresParameterScope::Local),
                    name: "whatever".into(),
                    value: SetPostgresParameterValue::Value(PostgresParameterValue::List(vec![
                        PostgresParameterValueInner::Literal("x".into()),
                        PostgresParameterValueInner::Literal(1.into()),
                        PostgresParameterValueInner::Identifier("hi".into()),
                    ]))
                })
            );
        }

        #[test]
        fn set_on() {
            let res = test_parse!(
                set(Dialect::PostgreSQL),
                b"SET standard_conforming_strings = on"
            );
            assert_eq!(
                res,
                SetStatement::PostgresParameter(SetPostgresParameter {
                    scope: None,
                    name: "standard_conforming_strings".into(),
                    value: SetPostgresParameterValue::Value(PostgresParameterValue::Single(
                        PostgresParameterValueInner::Identifier("on".into())
                    ))
                })
            )
        }
    }
}
