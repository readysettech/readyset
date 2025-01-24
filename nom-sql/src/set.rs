use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case};
use nom::combinator::{map, opt};
use nom::multi::separated_list1;
use nom::sequence::{terminated, tuple};
use nom::Parser;
use nom_locate::LocatedSpan;
use readyset_sql::{ast::*, Dialect};

use crate::common::statement_terminator;
use crate::dialect::DialectParser;
use crate::expression::expression;
use crate::literal::literal;
use crate::whitespace::{whitespace0, whitespace1};
use crate::{NomSqlError, NomSqlResult};

fn postgres_parameter_scope(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], PostgresParameterScope> {
    alt((
        map(tag_no_case("session"), |_| PostgresParameterScope::Session),
        map(tag_no_case("local"), |_| PostgresParameterScope::Local),
    ))(i)
}

fn set_postgres_parameter_value(
    i: LocatedSpan<&[u8]>,
) -> NomSqlResult<&[u8], SetPostgresParameterValue> {
    alt((
        map(tag_no_case("default"), |_| {
            SetPostgresParameterValue::Default
        }),
        map(postgres_parameter_value, SetPostgresParameterValue::Value),
    ))(i)
}

fn postgres_parameter_value_inner(
    i: LocatedSpan<&[u8]>,
) -> NomSqlResult<&[u8], PostgresParameterValueInner> {
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

fn postgres_parameter_value(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], PostgresParameterValue> {
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

pub(crate) fn variable_scope_prefix(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], VariableScope> {
    alt((
        map(tag_no_case("@@LOCAL."), |_| VariableScope::Local),
        map(tag_no_case("@@GLOBAL."), |_| VariableScope::Global),
        map(tag_no_case("@@SESSION."), |_| VariableScope::Session),
        map(tag_no_case("@@"), |_| VariableScope::Session),
        map(tag_no_case("@"), |_| VariableScope::User),
    ))(i)
}

fn set_variable_scope_prefix(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], VariableScope> {
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
fn variable(dialect: Dialect) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Variable> {
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

pub fn set(dialect: Dialect) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], SetStatement> {
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
                    Err(nom::Err::Error(NomSqlError {
                        input: i,
                        kind: nom::error::ErrorKind::Tag,
                    }))
                }
            },
            map(set_variables(dialect), SetStatement::Variable),
            map(set_names(dialect), SetStatement::Names),
        ))(i)?;

        Ok((i, statement))
    }
}

fn set_variable(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], (Variable, Expr)> {
    move |i| {
        let (i, variable) = variable(dialect)(i)?;
        let (i, _) = whitespace0(i)?;
        let (i, _) = alt((tag_no_case("="), tag_no_case(":=")))(i)?;
        let (i, _) = whitespace0(i)?;
        let (i, value) = expression(dialect)(i)?;
        Ok((i, (variable, value)))
    }
}

fn set_variables(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], SetVariables> {
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

fn set_names(dialect: Dialect) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], SetNames> {
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

fn set_postgres_parameter(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], SetPostgresParameter> {
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
    use readyset_sql::DialectDisplay;

    use super::*;

    #[test]
    fn simple_set() {
        let qstring = "SET SQL_AUTO_IS_NULL = 0;";
        let res = set(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
        assert_eq!(
            res.unwrap().1,
            SetStatement::Variable(SetVariables {
                variables: vec!((
                    Variable {
                        scope: VariableScope::Local,
                        name: "sql_auto_is_null".into()
                    },
                    Expr::Literal(0.into())
                )),
            })
        );
    }

    #[test]
    fn user_defined_vars() {
        let qstring = "SET @var = 123;";
        let res = set(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
        assert_eq!(
            res.unwrap().1,
            SetStatement::Variable(SetVariables {
                variables: vec!((
                    Variable {
                        scope: VariableScope::User,
                        name: "var".into()
                    },
                    Expr::Literal(123.into())
                )),
            })
        );
    }

    #[test]
    fn format_set() {
        let qstring = "set autocommit=1";
        let expected = "SET @@LOCAL.autocommit = 1";
        let res = set(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
        assert_eq!(res.unwrap().1.display(Dialect::MySQL).to_string(), expected);
    }

    #[test]
    fn global_set() {
        let qstring1 = "set gloBal var = 2";
        let qstring2 = "set @@gLobal.var = 2";
        let expected = "SET @@GLOBAL.var = 2";
        let res1 = test_parse!(set(Dialect::MySQL), qstring1.as_bytes());
        let res2 = test_parse!(set(Dialect::MySQL), qstring2.as_bytes());
        assert_eq!(res1.display(Dialect::MySQL).to_string(), expected);
        assert_eq!(res2.display(Dialect::MySQL).to_string(), expected);
    }

    #[test]
    fn session_set() {
        let qstring1 = "set @@Session.var = 1";
        let qstring2 = "set @@var = 1";
        let qstring3 = "set SeSsion var = 1";
        let expected = "SET @@SESSION.var = 1";
        let res1 = set(Dialect::MySQL)(LocatedSpan::new(qstring1.as_bytes()));
        let res2 = set(Dialect::MySQL)(LocatedSpan::new(qstring2.as_bytes()));
        let res3 = set(Dialect::MySQL)(LocatedSpan::new(qstring3.as_bytes()));
        assert_eq!(
            res1.unwrap().1.display(Dialect::MySQL).to_string(),
            expected
        );
        assert_eq!(
            res2.unwrap().1.display(Dialect::MySQL).to_string(),
            expected
        );
        assert_eq!(
            res3.unwrap().1.display(Dialect::MySQL).to_string(),
            expected
        );
    }

    #[test]
    fn local_set() {
        let qstring1 = "set lOcal var = 2";
        let qstring2 = "set @@local.var = 2";
        let expected = "SET @@LOCAL.var = 2";
        let res1 = set(Dialect::MySQL)(LocatedSpan::new(qstring1.as_bytes()));
        let res2 = set(Dialect::MySQL)(LocatedSpan::new(qstring2.as_bytes()));
        assert_eq!(
            res1.unwrap().1.display(Dialect::MySQL).to_string(),
            expected
        );
        assert_eq!(
            res2.unwrap().1.display(Dialect::MySQL).to_string(),
            expected
        );
    }

    #[test]
    fn set_names() {
        let qstring1 = "SET NAMES 'iso8660'";
        let qstring2 = "set names 'utf8mb4' collate 'utf8mb4_unicode_ci'";
        let res1 = set(Dialect::MySQL)(LocatedSpan::new(qstring1.as_bytes()))
            .unwrap()
            .1;
        let res2 = set(Dialect::MySQL)(LocatedSpan::new(qstring2.as_bytes()))
            .unwrap()
            .1;
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
        let res = set(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
        assert_eq!(
            res.unwrap().1,
            SetStatement::Variable(SetVariables {
                variables: vec!((
                    Variable {
                        scope: VariableScope::User,
                        name: "myvar".into()
                    },
                    Expr::BinaryOp {
                        lhs: Box::new(Expr::Literal(100.into())),
                        op: BinaryOperator::Add,
                        rhs: Box::new(Expr::Literal(200.into())),
                    }
                )),
            })
        );
    }

    #[test]
    fn list_set() {
        let qstring = "SET @myvar = 100 + 200, @@notmyvar = 'value', @@Global.g = @@global.V;";
        let res = set(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
        assert_eq!(
            res.unwrap().1,
            SetStatement::Variable(SetVariables {
                variables: vec!(
                    (
                        Variable {
                            scope: VariableScope::User,
                            name: "myvar".into()
                        },
                        Expr::BinaryOp {
                            lhs: Box::new(Expr::Literal(100.into())),
                            op: BinaryOperator::Add,
                            rhs: Box::new(Expr::Literal(200.into())),
                        }
                    ),
                    (
                        Variable {
                            scope: VariableScope::Session,
                            name: "notmyvar".into()
                        },
                        Expr::Literal("value".into()),
                    ),
                    (
                        Variable {
                            scope: VariableScope::Global,
                            name: "g".into()
                        },
                        Expr::Variable(Variable {
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

        test_format_parse_round_trip!(
            rt_variable(variable, Variable, Dialect::PostgreSQL, {
                // Only allow Variables with names that aren't keywords
                |s: &Variable| {
                    let name = s.name.to_string();
                    Dialect::PostgreSQL
                        .identifier()
                        .map(|ident| ident.to_ascii_lowercase())
                        .parse(LocatedSpan::new(name.as_bytes())).is_ok()
                }
            });
        );

        #[test]
        fn set_client_min_messages() {
            let res = test_parse!(
                set(Dialect::PostgreSQL),
                b"SET client_min_messages TO 'warning'"
            );
            let roundtripped = res.display(Dialect::PostgreSQL).to_string();
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
            let roundtripped = res.display(Dialect::PostgreSQL).to_string();
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
            let roundtripped = res.display(Dialect::PostgreSQL).to_string();
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
            let roundtripped = res1.display(Dialect::PostgreSQL).to_string();
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
            let roundtripped = res.display(Dialect::PostgreSQL).to_string();
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
