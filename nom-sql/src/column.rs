use std::cmp::Ordering;
use std::str::FromStr;
use std::{fmt, str};

use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case, take_until};
use nom::character::complete::digit1;
use nom::combinator::{map, map_res, opt};
use nom::multi::many0;
use nom::sequence::{delimited, preceded, terminated, tuple};
use nom::IResult;
use serde::{Deserialize, Serialize};

use crate::common::{column_identifier_no_alias, parse_comment, type_identifier, SqlType};
use crate::whitespace::{whitespace0, whitespace1};
use crate::{Dialect, Double, Literal, SqlIdentifier, Table};

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct Column {
    pub name: SqlIdentifier,
    pub table: Option<Table>,
}

impl fmt::Display for Column {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(ref table) = self.table {
            write!(f, "{}.", table)?;
        }
        write!(f, "`{}`", self.name)
    }
}

impl<'a> From<&'a str> for Column {
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

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum ColumnConstraint {
    Null,
    NotNull,
    CharacterSet(String),
    Collation(String),
    DefaultValue(Literal),
    AutoIncrement,
    PrimaryKey,
    Unique,
    /// NOTE(grfn): Yes, this really is its own special thing, not just an expression - see
    /// <https://dev.mysql.com/doc/refman/8.0/en/timestamp-initialization.html>
    OnUpdateCurrentTimestamp,
}

impl fmt::Display for ColumnConstraint {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ColumnConstraint::Null => write!(f, "NULL"),
            ColumnConstraint::NotNull => write!(f, "NOT NULL"),
            ColumnConstraint::CharacterSet(ref charset) => write!(f, "CHARACTER SET {}", charset),
            ColumnConstraint::Collation(ref collation) => write!(f, "COLLATE {}", collation),
            ColumnConstraint::DefaultValue(ref literal) => {
                write!(f, "DEFAULT {}", literal)
            }
            ColumnConstraint::AutoIncrement => write!(f, "AUTO_INCREMENT"),
            ColumnConstraint::PrimaryKey => write!(f, "PRIMARY KEY"),
            ColumnConstraint::Unique => write!(f, "UNIQUE"),
            ColumnConstraint::OnUpdateCurrentTimestamp => write!(f, "ON UPDATE CURRENT_TIMESTAMP"),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct ColumnSpecification {
    pub column: Column,
    pub sql_type: SqlType,
    pub constraints: Vec<ColumnConstraint>,
    pub comment: Option<String>,
}

impl fmt::Display for ColumnSpecification {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "`{}` {}", &self.column.name, self.sql_type)?;
        for constraint in self
            .constraints
            .iter()
            // Don't output PRIMARY KEY, because it will be formatted as table level key instead
            .filter(|c| !matches!(c, ColumnConstraint::PrimaryKey))
        {
            write!(f, " {}", constraint)?;
        }
        if let Some(ref comment) = self.comment {
            write!(f, " COMMENT '{}'", comment)?;
        }
        Ok(())
    }
}

impl ColumnSpecification {
    pub fn new(column: Column, sql_type: SqlType) -> ColumnSpecification {
        ColumnSpecification {
            column,
            sql_type,
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
            constraints,
            comment: None,
        }
    }

    pub fn has_default(&self) -> Option<&Literal> {
        self.constraints.iter().find_map(|c| match c {
            ColumnConstraint::DefaultValue(ref l) => Some(l),
            _ => None,
        })
    }
}

fn fixed_point(i: &[u8]) -> IResult<&[u8], Literal> {
    let (remaining_input, (int, _, f)) = tuple((
        map_res(map_res(digit1, str::from_utf8), i32::from_str),
        tag("."),
        digit1,
    ))(i)?;
    let precision = f.len();
    let dec = map_res(map_res(digit1, str::from_utf8), i32::from_str)(f)?.1;
    let value = (int as f64) + (dec as f64) / 10.0_f64.powf(precision as f64);
    Ok((
        remaining_input,
        Literal::Double(Double {
            value,
            precision: precision as u8,
        }),
    ))
}

fn default(i: &[u8]) -> IResult<&[u8], ColumnConstraint> {
    let (remaining_input, (_, _, _, def, _)) = tuple((
        whitespace0,
        tag_no_case("default"),
        whitespace1,
        // TODO(grfn): This really should just be a generic expression parser T.T
        // https://app.clubhouse.io/readysettech/story/101/unify-the-expression-ast
        alt((
            map(
                map_res(
                    delimited(tag("'"), take_until("'"), tag("'")),
                    str::from_utf8,
                ),
                |s: &str| Literal::String(String::from(s)),
            ),
            fixed_point,
            map(
                map_res(map_res(digit1, str::from_utf8), i64::from_str),
                Literal::Integer,
            ),
            map(tag("''"), |_| Literal::String(String::from(""))),
            map(tag_no_case("null"), |_| Literal::Null),
            map(tag_no_case("true"), |_| Literal::Boolean(true)),
            map(tag_no_case("false"), |_| Literal::Boolean(false)),
            map(
                terminated(tag_no_case("current_timestamp"), opt(tag("()"))),
                |_| Literal::CurrentTimestamp,
            ),
        )),
        whitespace0,
    ))(i)?;

    Ok((remaining_input, ColumnConstraint::DefaultValue(def)))
}

pub fn on_update_current_timestamp(i: &[u8]) -> IResult<&[u8], ColumnConstraint> {
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
    let (i, _) = opt(tag("()"))(i)?;
    Ok((i, ColumnConstraint::OnUpdateCurrentTimestamp))
}

pub fn column_constraint(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], ColumnConstraint> {
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
            default,
            primary_key,
            unique,
            character_set,
            collate,
            on_update_current_timestamp,
        ))(i)
    }
}

/// Parse rule for a column specification
pub fn column_specification(
    dialect: Dialect,
) -> impl Fn(&[u8]) -> IResult<&[u8], ColumnSpecification> {
    move |i| {
        let (remaining_input, (column, field_type, constraints, comment)) = tuple((
            column_identifier_no_alias(dialect),
            opt(delimited(
                whitespace1,
                type_identifier(dialect),
                whitespace0,
            )),
            many0(column_constraint(dialect)),
            opt(parse_comment),
        ))(i)?;

        let sql_type = match field_type {
            None => SqlType::Text,
            Some(ref t) => t.clone(),
        };

        Ok((
            remaining_input,
            ColumnSpecification {
                column,
                sql_type,
                constraints,
                comment,
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod mysql {
        use super::*;

        #[test]
        fn multiple_constraints() {
            let (_, res) = column_specification(Dialect::MySQL)(
                b"`created_at` timestamp NOT NULL DEFAULT current_timestamp()",
            )
            .unwrap();
            assert_eq!(
                res,
                ColumnSpecification {
                    column: Column {
                        name: "created_at".into(),
                        table: None,
                    },
                    sql_type: SqlType::Timestamp,
                    comment: None,
                    constraints: vec![
                        ColumnConstraint::NotNull,
                        ColumnConstraint::DefaultValue(Literal::CurrentTimestamp),
                    ]
                }
            );
        }

        #[test]
        fn null_round_trip() {
            let input = b"`c` INT(32) NULL";
            let cspec = column_specification(Dialect::MySQL)(input).unwrap().1;
            let res = cspec.to_string();
            assert_eq!(res, String::from_utf8(input.to_vec()).unwrap());
        }

        #[test]
        fn default_booleans() {
            let input = b"`c` bool DEFAULT FALSE";
            let cspec = column_specification(Dialect::MySQL)(input).unwrap().1;
            assert_eq!(cspec.constraints.len(), 1);
            assert!(matches!(
                cspec.constraints[0],
                ColumnConstraint::DefaultValue(Literal::Boolean(false))
            ));

            let input = b"`c` bool DEFAULT true";
            let cspec = column_specification(Dialect::MySQL)(input).unwrap().1;
            assert_eq!(cspec.constraints.len(), 1);
            assert!(matches!(
                cspec.constraints[0],
                ColumnConstraint::DefaultValue(Literal::Boolean(true))
            ));
        }
    }

    mod postgres {
        use super::*;

        #[test]
        fn multiple_constraints() {
            let (_, res) = column_specification(Dialect::PostgreSQL)(
                b"\"created_at\" timestamp NOT NULL DEFAULT current_timestamp()",
            )
            .unwrap();
            assert_eq!(
                res,
                ColumnSpecification {
                    column: Column {
                        name: "created_at".into(),
                        table: None,
                    },
                    sql_type: SqlType::Timestamp,
                    comment: None,
                    constraints: vec![
                        ColumnConstraint::NotNull,
                        ColumnConstraint::DefaultValue(Literal::CurrentTimestamp),
                    ]
                }
            );
        }
    }
}
