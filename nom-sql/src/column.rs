use std::cmp::Ordering;
use std::{fmt, str};

use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case};
use nom::combinator::{map, opt};
use nom::multi::many0;
use nom::sequence::{delimited, preceded, terminated, tuple};
use nom_locate::LocatedSpan;
use readyset_util::fmt::fmt_with;
use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

use crate::common::{column_identifier_no_alias, parse_comment};
use crate::expression::expression;
use crate::sql_type::type_identifier;
use crate::whitespace::{whitespace0, whitespace1};
use crate::{
    literal, Dialect, DialectDisplay, Expr, Literal, NomSqlResult, Relation, SqlIdentifier, SqlType,
};

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct Column {
    pub name: SqlIdentifier,
    pub table: Option<Relation>,
}

impl From<SqlIdentifier> for Column {
    fn from(name: SqlIdentifier) -> Self {
        Column { name, table: None }
    }
}

impl From<&'_ str> for Column {
    fn from(c: &str) -> Column {
        match c.split_once('.') {
            None => Column {
                name: c.into(),
                table: None,
            },
            Some((table_name, col_name)) => Column {
                name: col_name.into(),
                table: Some(table_name.into()),
            },
        }
    }
}

impl Ord for Column {
    fn cmp(&self, other: &Column) -> Ordering {
        match (self.table.as_ref(), other.table.as_ref()) {
            (Some(s), Some(o)) => (s, &self.name).cmp(&(o, &other.name)),
            _ => self.name.cmp(&other.name),
        }
    }
}

impl PartialOrd for Column {
    fn partial_cmp(&self, other: &Column) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl DialectDisplay for Column {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| {
            if let Some(ref table) = self.table {
                write!(f, "{}.", table.display(dialect))?;
            }
            write!(f, "{}", dialect.quote_identifier(&self.name))
        })
    }
}

impl Column {
    /// Like [`display()`](Self::display) except the schema, table, and column name will not be
    /// quoted.
    ///
    /// This should not be used to emit SQL code and instead should mostly be for error messages.
    pub fn display_unquoted(&self) -> impl fmt::Display + Copy + '_ {
        fmt_with(move |f| {
            if let Some(ref table) = self.table {
                write!(f, "{}.", table.display_unquoted())?;
            }
            write!(f, "{}", self.name)
        })
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub enum ColumnConstraint {
    Null,
    NotNull,
    CharacterSet(String),
    Collation(String),
    DefaultValue(Expr),
    AutoIncrement,
    PrimaryKey,
    Unique,
    /// NOTE(aspen): Yes, this really is its own special thing, not just an expression - see
    /// <https://dev.mysql.com/doc/refman/8.0/en/timestamp-initialization.html>
    OnUpdateCurrentTimestamp(Option<Literal>),
}

impl DialectDisplay for ColumnConstraint {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| match self {
            Self::Null => write!(f, "NULL"),
            Self::NotNull => write!(f, "NOT NULL"),
            Self::CharacterSet(charset) => write!(f, "CHARACTER SET {}", charset),
            Self::Collation(collation) => write!(f, "COLLATE {}", collation),
            Self::DefaultValue(expr) => write!(f, "DEFAULT {}", expr.display(dialect)),
            Self::AutoIncrement => write!(f, "AUTO_INCREMENT"),
            Self::PrimaryKey => write!(f, "PRIMARY KEY"),
            Self::Unique => write!(f, "UNIQUE"),
            Self::OnUpdateCurrentTimestamp(opt) => {
                write!(f, "ON UPDATE CURRENT_TIMESTAMP")?;
                if let Some(lit) = opt {
                    write!(f, "({})", lit.display(dialect))?;
                }
                Ok(())
            }
        })
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct ColumnSpecification {
    pub column: Column,
    pub sql_type: SqlType,
    pub generated: Option<GeneratedColumn>,
    pub constraints: Vec<ColumnConstraint>,
    pub comment: Option<String>,
}

impl ColumnSpecification {
    pub fn new(column: Column, sql_type: SqlType) -> ColumnSpecification {
        ColumnSpecification {
            column,
            sql_type,
            generated: None,
            constraints: vec![],
            comment: None,
        }
    }

    pub fn with_constraints(
        column: Column,
        sql_type: SqlType,
        constraints: Vec<ColumnConstraint>,
    ) -> ColumnSpecification {
        ColumnSpecification {
            column,
            sql_type,
            generated: None,
            constraints,
            comment: None,
        }
    }

    pub fn has_default(&self) -> Option<&Literal> {
        self.constraints.iter().find_map(|c| match c {
            ColumnConstraint::DefaultValue(Expr::Literal(ref l)) => Some(l),
            _ => None,
        })
    }

    /// Returns the character set for the column, if one is set.
    pub fn get_charset(&self) -> Option<&str> {
        // Character set is a constraint in Text fields only
        if !self.sql_type.is_any_text() {
            return None;
        }
        self.constraints.iter().find_map(|c| match c {
            ColumnConstraint::CharacterSet(ref charset) => Some(charset.as_str()),
            _ => None,
        })
    }

    /// Returns the collation for the column, if one is set.
    pub fn get_collation(&self) -> Option<&str> {
        // Collation is a constraint in Text fields only
        if !self.sql_type.is_any_text() {
            return None;
        }
        self.constraints.iter().find_map(|c| match c {
            ColumnConstraint::Collation(ref collation) => Some(collation.as_str()),
            _ => None,
        })
    }
}

impl DialectDisplay for ColumnSpecification {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| {
            write!(
                f,
                "{} {}",
                dialect.quote_identifier(&self.column.name),
                self.sql_type.display(dialect)
            )?;

            for constraint in &self.constraints {
                write!(f, " {}", constraint.display(dialect))?;
            }

            if let Some(ref comment) = self.comment {
                write!(f, " COMMENT '{}'", comment)?;
            }

            Ok(())
        })
    }
}

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

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct GeneratedColumn {
    pub expr: Expr,
    pub stored: bool,
}

impl fmt::Display for GeneratedColumn {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "GENERATED ALWAYS AS ({}) {} ",
            self.expr.display(Dialect::MySQL),
            if self.stored { "STORED" } else { "VIRTUAL" }
        )
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
            opt(map(
                alt((tag_no_case("stored"), tag_no_case("virtual"))),
                |i: LocatedSpan<&[u8]>| {
                    std::str::from_utf8(i.fragment())
                        .unwrap()
                        .to_string()
                        .to_lowercase()
                        == "stored"
                },
            )),
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
        use super::*;
        use crate::{BinaryOperator, FunctionExpr};

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
        use crate::FunctionExpr;

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
