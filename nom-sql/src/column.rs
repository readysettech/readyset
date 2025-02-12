use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case};
use nom::combinator::{map, opt};
use nom::multi::many0;
use nom::sequence::{delimited, preceded, terminated, tuple};
use nom_locate::LocatedSpan;
use readyset_sql::{ast::*, Dialect};

use crate::common::{column_identifier_no_alias, parse_comment};
use crate::dialect::DialectParser;
use crate::expression::expression;
use crate::sql_type::type_identifier;
use crate::whitespace::{whitespace0, whitespace1};
use crate::{literal, NomSqlResult};

fn default(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], ColumnConstraint> {
    move |i| {
        let (i, _) = whitespace0(i)?;
        let (i, _) = tag_no_case("default")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, def) = expression(dialect)(i)?;
        let (i, _) = whitespace0(i)?;

        Ok((i, ColumnConstraint::DefaultValue(def)))
    }
}

pub fn on_update_current_timestamp(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], ColumnConstraint> {
    move |i| {
        let (i, _) = tag_no_case("on")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, _) = tag_no_case("update")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, _) = alt((
            tag_no_case("current_timestamp"),
            tag_no_case("now"),
            tag_no_case("localtime"),
            tag_no_case("localtimestamp"),
        ))(i)?;

        let (i, opt_lit) = opt(delimited(
            tuple((whitespace0, tag("("), whitespace0)),
            literal(dialect),
            tuple((whitespace0, tag(")"), whitespace0)),
        ))(i)?;
        Ok((i, ColumnConstraint::OnUpdateCurrentTimestamp(opt_lit)))
    }
}

pub fn column_constraint(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], ColumnConstraint> {
    move |i| {
        let not_null = map(
            delimited(whitespace0, tag_no_case("not null"), whitespace0),
            |_| ColumnConstraint::NotNull,
        );
        let null = map(
            delimited(whitespace0, tag_no_case("null"), whitespace0),
            |_| ColumnConstraint::Null,
        );
        let auto_increment = map(
            delimited(whitespace0, tag_no_case("auto_increment"), whitespace0),
            |_| ColumnConstraint::AutoIncrement,
        );
        let primary_key = map(
            delimited(whitespace0, tag_no_case("primary key"), whitespace0),
            |_| ColumnConstraint::PrimaryKey,
        );
        let unique = map(
            delimited(
                whitespace0,
                delimited(tag_no_case("unique"), whitespace0, opt(tag_no_case("key"))),
                whitespace0,
            ),
            |_| ColumnConstraint::Unique,
        );
        let character_set = map(
            preceded(
                delimited(whitespace0, tag_no_case("character set"), whitespace1),
                dialect.identifier(),
            ),
            |cs| {
                let char_set = cs.to_string();
                ColumnConstraint::CharacterSet(char_set)
            },
        );
        let collate = map(
            preceded(
                delimited(whitespace0, tag_no_case("collate"), whitespace1),
                dialect.identifier(),
            ),
            |c| {
                let collation = c.to_string();
                ColumnConstraint::Collation(collation)
            },
        );

        alt((
            not_null,
            null,
            auto_increment,
            default(dialect),
            primary_key,
            unique,
            character_set,
            collate,
            on_update_current_timestamp(dialect),
        ))(i)
    }
}

/// Parse rule for a column specification
pub fn column_specification(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], ColumnSpecification> {
    move |i| {
        let (i, (column, field_type)) = tuple((
            column_identifier_no_alias(dialect),
            opt(delimited(
                whitespace1,
                type_identifier(dialect),
                whitespace0,
            )),
        ))(i)?;

        let (i, generated) = if matches!(dialect, Dialect::MySQL) {
            opt(preceded(whitespace0, generated_column(dialect)))(i)?
        } else {
            (i, None)
        };

        let (i, constraints) = many0(preceded(whitespace0, column_constraint(dialect)))(i)?;

        let (i, comment) = if matches!(dialect, Dialect::MySQL) {
            opt(parse_comment(dialect))(i)?
        } else {
            (i, None)
        };

        let sql_type = match field_type {
            None => SqlType::Text,
            Some(ref t) => t.clone(),
        };

        Ok((
            i,
            ColumnSpecification {
                column,
                sql_type,
                generated,
                constraints,
                comment,
            },
        ))
    }
}

/// Parse rule for a generated column specification
fn generated_column(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], GeneratedColumn> {
    move |i| {
        let (i, _) = opt(terminated(tag_no_case("generated always"), whitespace0))(i)?;
        let (i, _) = terminated(tag_no_case("as"), whitespace0)(i)?;
        let (i, expr) = delimited(tag("("), expression(dialect), tag(")"))(i)?;
        let (i, stored) = preceded(
            whitespace0,
            opt(alt((
                map(tag_no_case("stored"), |_| true),
                map(tag_no_case("virtual"), |_| false),
            ))),
        )(i)?;

        Ok((
            i,
            GeneratedColumn {
                expr,
                stored: stored.unwrap_or(false),
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod mysql {
        use readyset_sql::DialectDisplay;

        use super::*;

        #[test]
        fn multiple_generated_column() {
            let mut default_gen_col = GeneratedColumn {
                expr: Expr::BinaryOp {
                    lhs: Box::new(Expr::Literal(Literal::Integer(1))),
                    op: BinaryOperator::Add,
                    rhs: Box::new(Expr::Literal(Literal::Integer(1))),
                },
                stored: true,
            };

            // Without GENERATED ALWAYS
            let (_, res) =
                generated_column(Dialect::MySQL)(LocatedSpan::new(b"AS (1 + 1) STORED")).unwrap();
            assert_eq!(res, default_gen_col);

            // With GENERATED ALWAYS and STORED
            let (_, res) = generated_column(Dialect::MySQL)(LocatedSpan::new(
                b"GENERATED ALWAYS AS (1 + 1) STORED",
            ))
            .unwrap();
            assert_eq!(res, default_gen_col);

            // With GENERATED ALWAYS and VIRTUAL
            default_gen_col.stored = false;
            let (_, res) = generated_column(Dialect::MySQL)(LocatedSpan::new(
                b"GENERATED ALWAYS AS (1 + 1) VIRTUAL",
            ))
            .unwrap();

            assert_eq!(res, default_gen_col);

            // Without STORED or VIRTUAL (defaults to VIRTUAL)
            let (_, res) =
                generated_column(Dialect::MySQL)(LocatedSpan::new(b"GENERATED ALWAYS AS (1 + 1)"))
                    .unwrap();

            assert_eq!(res, default_gen_col);

            // Column specification with generated column
            let mut col_spec = ColumnSpecification {
                column: Column {
                    name: "col1".into(),
                    table: None,
                },
                sql_type: SqlType::Int(None),
                generated: Some(default_gen_col),
                comment: None,
                constraints: vec![ColumnConstraint::NotNull],
            };
            let (_, res) = column_specification(Dialect::MySQL)(LocatedSpan::new(
                b"`col1` INT GENERATED ALWAYS AS (1 + 1) VIRTUAL NOT NULL",
            ))
            .unwrap();
            assert_eq!(res, col_spec);

            // Column specification with generated column and PK
            col_spec.constraints.push(ColumnConstraint::PrimaryKey);
            let (_, res) = column_specification(Dialect::MySQL)(LocatedSpan::new(
                b"`col1` INT GENERATED ALWAYS AS (1 + 1) VIRTUAL NOT NULL PRIMARY KEY",
            ))
            .unwrap();
            assert_eq!(res, col_spec);
        }

        #[test]
        fn multiple_constraints() {
            let (_, res) = column_specification(Dialect::MySQL)(LocatedSpan::new(
                b"`created_at` timestamp NOT NULL DEFAULT current_timestamp()",
            ))
            .unwrap();
            assert_eq!(
                res,
                ColumnSpecification {
                    column: Column {
                        name: "created_at".into(),
                        table: None,
                    },
                    sql_type: SqlType::Timestamp,
                    generated: None,
                    comment: None,
                    constraints: vec![
                        ColumnConstraint::NotNull,
                        ColumnConstraint::DefaultValue(Expr::Call(FunctionExpr::Call {
                            name: "current_timestamp".into(),
                            arguments: vec![]
                        })),
                    ]
                }
            );
        }

        #[test]
        fn null_round_trip() {
            let input = b"`c` INT(32) NULL";
            let cspec = column_specification(Dialect::MySQL)(LocatedSpan::new(input))
                .unwrap()
                .1;
            let res = cspec.display(Dialect::MySQL).to_string();
            assert_eq!(res, String::from_utf8(input.to_vec()).unwrap());
        }

        #[test]
        fn default_booleans() {
            let input = b"`c` bool DEFAULT FALSE";
            let cspec = column_specification(Dialect::MySQL)(LocatedSpan::new(input))
                .unwrap()
                .1;
            assert_eq!(cspec.constraints.len(), 1);
            assert!(matches!(
                cspec.constraints[0],
                ColumnConstraint::DefaultValue(Expr::Literal(Literal::Boolean(false)))
            ));

            let input = b"`c` bool DEFAULT true";
            let cspec = column_specification(Dialect::MySQL)(LocatedSpan::new(input))
                .unwrap()
                .1;
            assert_eq!(cspec.constraints.len(), 1);
            assert!(matches!(
                cspec.constraints[0],
                ColumnConstraint::DefaultValue(Expr::Literal(Literal::Boolean(true)))
            ));
        }

        #[test]
        fn on_update_current_timestamp_no_precision() {
            let input = b"`lastModified` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP";
            let (_, res) = column_specification(Dialect::MySQL)(LocatedSpan::new(input)).unwrap();
            let cspec = ColumnSpecification {
                column: Column {
                    name: "lastModified".into(),
                    table: None,
                },
                sql_type: SqlType::DateTime(Some(6)),
                generated: None,
                comment: None,
                constraints: vec![
                    ColumnConstraint::NotNull,
                    ColumnConstraint::DefaultValue(Expr::Call(FunctionExpr::Call {
                        name: "CURRENT_TIMESTAMP".into(),
                        arguments: vec![Expr::Literal(Literal::Integer(6))],
                    })),
                    ColumnConstraint::OnUpdateCurrentTimestamp(None),
                ],
            };
            assert_eq!(res, cspec);
            let res = cspec.display(Dialect::MySQL).to_string();
            assert_eq!(res, String::from_utf8(input.to_vec()).unwrap());
        }

        #[test]
        fn on_update_current_timestamp_precision() {
            let canonical = "`lastModified` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)";
            let inputs = vec![
                canonical,
                "`lastModified` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP (6) ",
                "`lastModified` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP( 6 )",
                "`lastModified` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6 ) ",
                "`lastModified` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP ( 6)",
            ];
            for input in inputs {
                let (_, res) =
                    column_specification(Dialect::MySQL)(LocatedSpan::new(input.as_bytes()))
                        .unwrap();
                let cspec = ColumnSpecification {
                    column: Column {
                        name: "lastModified".into(),
                        table: None,
                    },
                    sql_type: SqlType::DateTime(Some(6)),
                    generated: None,
                    comment: None,
                    constraints: vec![
                        ColumnConstraint::NotNull,
                        ColumnConstraint::DefaultValue(Expr::Call(FunctionExpr::Call {
                            name: "CURRENT_TIMESTAMP".into(),
                            arguments: vec![Expr::Literal(Literal::Integer(6))],
                        })),
                        ColumnConstraint::OnUpdateCurrentTimestamp(Some(Literal::Integer(6))),
                    ],
                };
                assert_eq!(res, cspec);
                let res = cspec.display(Dialect::MySQL).to_string();
                assert_eq!(res, canonical);
            }
        }
    }

    mod postgres {
        use super::*;

        #[test]
        fn multiple_constraints() {
            let (_, res) = column_specification(Dialect::PostgreSQL)(LocatedSpan::new(
                b"\"created_at\" timestamp NOT NULL DEFAULT current_timestamp()",
            ))
            .unwrap();
            assert_eq!(
                res,
                ColumnSpecification {
                    column: Column {
                        name: "created_at".into(),
                        table: None,
                    },
                    sql_type: SqlType::Timestamp,
                    generated: None,
                    comment: None,
                    constraints: vec![
                        ColumnConstraint::NotNull,
                        ColumnConstraint::DefaultValue(Expr::Call(FunctionExpr::Call {
                            name: "current_timestamp".into(),
                            arguments: vec![]
                        })),
                    ]
                }
            );
        }

        #[test]
        fn default_now() {
            let (_, res1) = column_specification(Dialect::PostgreSQL)(LocatedSpan::new(
                b"c timestamp NOT NULL DEFAULT NOW()",
            ))
            .unwrap();

            assert_eq!(
                res1,
                ColumnSpecification {
                    column: Column {
                        name: "c".into(),
                        table: None,
                    },
                    sql_type: SqlType::Timestamp,
                    generated: None,
                    comment: None,
                    constraints: vec![
                        ColumnConstraint::NotNull,
                        ColumnConstraint::DefaultValue(Expr::Call(FunctionExpr::Call {
                            name: "NOW".into(),
                            arguments: vec![]
                        })),
                    ]
                }
            );
        }
    }
}
