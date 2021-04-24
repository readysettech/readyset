use std::cmp::Ordering;
use std::fmt;
use std::str;
use std::str::FromStr;

use crate::common::{column_identifier_no_alias, parse_comment, sql_identifier, Literal, SqlType};
use crate::keywords::escape_if_keyword;
use crate::FunctionExpression;
use nom::bytes::complete::{tag_no_case, take_until};
use nom::character::complete::{multispace0, multispace1};
use nom::combinator::{map, opt};
use nom::multi::many0;
use nom::sequence::{delimited, preceded, terminated, tuple};
use nom::{alt, complete, do_parse, map, named, opt, tag, tag_no_case, IResult};
use nom::{branch::alt, bytes::complete::tag, character::complete::digit1};

use crate::{common::type_identifier, Real};

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub table: Option<String>,
    pub function: Option<Box<FunctionExpression>>,
}

impl fmt::Display for Column {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(ref table) = self.table {
            write!(
                f,
                "{}.{}",
                escape_if_keyword(table),
                escape_if_keyword(&self.name)
            )?;
        } else if let Some(ref function) = self.function {
            write!(f, "{}", *function)?;
        } else {
            write!(f, "{}", escape_if_keyword(&self.name))?;
        }
        Ok(())
    }
}

impl<'a> From<&'a str> for Column {
    fn from(c: &str) -> Column {
        match c.find('.') {
            None => Column {
                name: String::from(c),
                table: None,
                function: None,
            },
            Some(i) => Column {
                name: String::from(&c[i + 1..]),
                table: Some(String::from(&c[0..i])),
                function: None,
            },
        }
    }
}

impl Ord for Column {
    fn cmp(&self, other: &Column) -> Ordering {
        if self.table.is_some() && other.table.is_some() {
            match self.table.cmp(&other.table) {
                Ordering::Equal => self.name.cmp(&other.name),
                x => x,
            }
        } else {
            self.name.cmp(&other.name)
        }
    }
}

impl PartialOrd for Column {
    fn partial_cmp(&self, other: &Column) -> Option<Ordering> {
        if self.table.is_some() && other.table.is_some() {
            match self.table.cmp(&other.table) {
                Ordering::Equal => Some(self.name.cmp(&other.name)),
                x => Some(x),
            }
        } else if self.table.is_none() && other.table.is_none() {
            Some(self.name.cmp(&other.name))
        } else {
            None
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum ColumnConstraint {
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
            ColumnConstraint::NotNull => write!(f, "NOT NULL"),
            ColumnConstraint::CharacterSet(ref charset) => write!(f, "CHARACTER SET {}", charset),
            ColumnConstraint::Collation(ref collation) => write!(f, "COLLATE {}", collation),
            ColumnConstraint::DefaultValue(ref literal) => {
                write!(f, "DEFAULT {}", literal.to_string())
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
        write!(
            f,
            "{} {}",
            escape_if_keyword(&self.column.name),
            self.sql_type
        )?;
        for constraint in self.constraints.iter() {
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
}

fn fixed_point(i: &[u8]) -> IResult<&[u8], Literal> {
    let (remaining_input, (i, _, f)) = tuple((digit1, tag("."), digit1))(i)?;

    Ok((
        remaining_input,
        Literal::FixedPoint(Real {
            integral: i32::from_str(str::from_utf8(i).unwrap()).unwrap(),
            fractional: i32::from_str(str::from_utf8(f).unwrap()).unwrap(),
        }),
    ))
}

fn default(i: &[u8]) -> IResult<&[u8], Option<ColumnConstraint>> {
    let (remaining_input, (_, _, _, def, _)) = tuple((
        multispace0,
        tag_no_case("default"),
        multispace1,
        // TODO(grfn): This really should just be a generic expression parser T.T
        // https://app.clubhouse.io/readysettech/story/101/unify-the-expression-ast
        alt((
            map(
                delimited(tag("'"), take_until("'"), tag("'")),
                |s: &[u8]| Literal::String(String::from_utf8(s.to_vec()).unwrap()),
            ),
            fixed_point,
            map(digit1, |d| {
                let d_i64 = i64::from_str(str::from_utf8(d).unwrap()).unwrap();
                Literal::Integer(d_i64)
            }),
            map(tag("''"), |_| Literal::String(String::from(""))),
            map(tag_no_case("null"), |_| Literal::Null),
            map(
                terminated(tag_no_case("current_timestamp"), opt(tag("()"))),
                |_| Literal::CurrentTimestamp,
            ),
        )),
        multispace0,
    ))(i)?;

    Ok((remaining_input, Some(ColumnConstraint::DefaultValue(def))))
}

named!(
    on_update_current_timestamp(&[u8]) -> ColumnConstraint,
    do_parse!(
        complete!(tag_no_case!("on"))
            >> multispace1
            >> complete!(tag_no_case!("update"))
            >> multispace1
            >> alt!(
                tag_no_case!("current_timestamp")
                    | tag_no_case!("now")
                    | tag_no_case!("localtime")
                    | tag_no_case!("localtimestamp")
            )
            >> opt!(tag!("()"))
            >> (ColumnConstraint::OnUpdateCurrentTimestamp)
    )
);

pub fn column_constraint(i: &[u8]) -> IResult<&[u8], Option<ColumnConstraint>> {
    let not_null = map(
        delimited(multispace0, tag_no_case("not null"), multispace0),
        |_| Some(ColumnConstraint::NotNull),
    );
    let null = map(
        delimited(multispace0, tag_no_case("null"), multispace0),
        |_| None,
    );
    let auto_increment = map(
        delimited(multispace0, tag_no_case("auto_increment"), multispace0),
        |_| Some(ColumnConstraint::AutoIncrement),
    );
    let primary_key = map(
        delimited(multispace0, tag_no_case("primary key"), multispace0),
        |_| Some(ColumnConstraint::PrimaryKey),
    );
    let unique = map(
        delimited(multispace0, tag_no_case("unique"), multispace0),
        |_| Some(ColumnConstraint::Unique),
    );
    let character_set = map(
        preceded(
            delimited(multispace0, tag_no_case("character set"), multispace1),
            sql_identifier,
        ),
        |cs| {
            let char_set = str::from_utf8(cs).unwrap().to_owned();
            Some(ColumnConstraint::CharacterSet(char_set))
        },
    );
    let collate = map(
        preceded(
            delimited(multispace0, tag_no_case("collate"), multispace1),
            sql_identifier,
        ),
        |c| {
            let collation = str::from_utf8(c).unwrap().to_owned();
            Some(ColumnConstraint::Collation(collation))
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
        |i| map!(i, on_update_current_timestamp, Some),
    ))(i)
}

/// Parse rule for a column specification
pub fn column_specification(i: &[u8]) -> IResult<&[u8], ColumnSpecification> {
    let (remaining_input, (column, field_type, constraints, comment)) = tuple((
        column_identifier_no_alias,
        opt(delimited(multispace1, type_identifier, multispace0)),
        many0(column_constraint),
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
            constraints: constraints.into_iter().filter_map(|m| m).collect(),
            comment,
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Expression;

    #[test]
    fn column_from_str() {
        let s = "table.col";
        let c = Column::from(s);

        assert_eq!(
            c,
            Column {
                name: String::from("col"),
                table: Some(String::from("table")),
                function: None,
            }
        );
    }

    #[test]
    fn print_function_column() {
        let c2 = Column {
            name: "".into(), // must be present, but will be ignored
            table: None,
            function: Some(Box::new(FunctionExpression::CountStar)),
        };
        let c3 = Column {
            name: "".into(), // must be present, but will be ignored
            table: None,
            function: Some(Box::new(FunctionExpression::Sum {
                expr: Box::new(Expression::Column(Column::from("mytab.foo"))),
                distinct: false,
            })),
        };

        assert_eq!(format!("{}", c2), "count(*)");
        assert_eq!(format!("{}", c3), "sum(mytab.foo)");
    }

    #[test]
    fn multiple_constraints() {
        let (_, res) =
            column_specification(b"`created_at` timestamp NOT NULL DEFAULT current_timestamp()")
                .unwrap();
        assert_eq!(
            res,
            ColumnSpecification {
                column: Column {
                    name: "created_at".to_owned(),
                    table: None,
                    function: None
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
