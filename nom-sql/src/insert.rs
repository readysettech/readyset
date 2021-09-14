use nom::character::complete::{multispace0, multispace1};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str;

use crate::column::Column;
use crate::common::{
    assignment_expr_list, field_list, schema_table_reference, statement_terminator, value_list,
    ws_sep_comma, Literal,
};
use crate::keywords::escape_if_keyword;
use crate::table::Table;
use crate::Dialect;
use crate::Expression;
use nom::bytes::complete::{tag, tag_no_case};
use nom::combinator::opt;
use nom::multi::many1;
use nom::sequence::{delimited, preceded, tuple};
use nom::IResult;

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct InsertStatement {
    pub table: Table,
    pub fields: Option<Vec<Column>>,
    pub data: Vec<Vec<Literal>>,
    pub ignore: bool,
    pub on_duplicate: Option<Vec<(Column, Expression)>>,
}

impl fmt::Display for InsertStatement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "INSERT INTO {}", escape_if_keyword(&self.table.name))?;
        if let Some(ref fields) = self.fields {
            write!(
                f,
                " ({})",
                fields
                    .iter()
                    .map(|col| escape_if_keyword(&col.name))
                    .collect::<Vec<_>>()
                    .join(", ")
            )?;
        }
        write!(
            f,
            " VALUES {}",
            self.data
                .iter()
                .map(|datas| format!(
                    "({})",
                    datas
                        .iter()
                        .map(|l| l.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                ))
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

fn fields(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], Vec<Column>> {
    move |i| {
        delimited(
            preceded(tag("("), multispace0),
            field_list(dialect),
            delimited(multispace0, tag(")"), multispace1),
        )(i)
    }
}

fn data(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], Vec<Literal>> {
    move |i| {
        delimited(
            tag("("),
            value_list(dialect),
            preceded(tag(")"), opt(ws_sep_comma)),
        )(i)
    }
}

fn on_duplicate(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], Vec<(Column, Expression)>> {
    move |i| {
        preceded(
            multispace0,
            preceded(
                tag_no_case("on duplicate key update"),
                preceded(multispace1, assignment_expr_list(dialect)),
            ),
        )(i)
    }
}

// Parse rule for a SQL insert query.
// TODO(malte): support REPLACE, nested selection, DEFAULT VALUES
pub fn insertion(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], InsertStatement> {
    move |i| {
        let (
            remaining_input,
            (_, ignore_res, _, _, _, table, _, fields, _, _, data, on_duplicate, _),
        ) = tuple((
            tag_no_case("insert"),
            opt(preceded(multispace1, tag_no_case("ignore"))),
            multispace1,
            tag_no_case("into"),
            multispace1,
            schema_table_reference(dialect),
            multispace0,
            opt(fields(dialect)),
            tag_no_case("values"),
            multispace0,
            many1(data(dialect)),
            opt(on_duplicate(dialect)),
            statement_terminator,
        ))(i)?;
        assert!(table.alias.is_none());
        let ignore = ignore_res.is_some();

        Ok((
            remaining_input,
            InsertStatement {
                table,
                fields,
                data,
                ignore,
                on_duplicate,
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column::Column;
    use crate::common::ItemPlaceholder;
    use crate::table::Table;

    #[test]
    fn insert_with_parameters() {
        let qstring = "INSERT INTO users (id, name) VALUES (?, ?);";

        let res = insertion(Dialect::MySQL)(qstring.as_bytes());
        assert_eq!(
            res.unwrap().1,
            InsertStatement {
                table: Table::from("users"),
                fields: Some(vec![Column::from("id"), Column::from("name")]),
                data: vec![vec![
                    Literal::Placeholder(ItemPlaceholder::QuestionMark),
                    Literal::Placeholder(ItemPlaceholder::QuestionMark)
                ]],
                ..Default::default()
            }
        );
    }

    mod mysql {
        use super::*;
        use crate::column::Column;
        use crate::common::ItemPlaceholder;
        use crate::table::Table;
        use crate::BinaryOperator;

        #[test]
        fn simple_insert() {
            let qstring = "INSERT INTO users VALUES (42, \"test\");";

            let res = insertion(Dialect::MySQL)(qstring.as_bytes());
            assert_eq!(
                res.unwrap().1,
                InsertStatement {
                    table: Table::from("users"),
                    fields: None,
                    data: vec![vec![42.into(), "test".into()]],
                    ..Default::default()
                }
            );
        }

        #[test]
        fn complex_insert() {
            let qstring = "INSERT INTO users VALUES (42, 'test', 'test', CURRENT_TIMESTAMP);";

            let res = insertion(Dialect::MySQL)(qstring.as_bytes());
            assert_eq!(
                res.unwrap().1,
                InsertStatement {
                    table: Table::from("users"),
                    fields: None,
                    data: vec![vec![
                        42.into(),
                        "test".into(),
                        "test".into(),
                        Literal::CurrentTimestamp,
                    ],],
                    ..Default::default()
                }
            );
        }

        #[test]
        fn insert_with_field_names() {
            let qstring = "INSERT INTO users (id, name) VALUES (42, 'test');";

            let res = insertion(Dialect::MySQL)(qstring.as_bytes());
            assert_eq!(
                res.unwrap().1,
                InsertStatement {
                    table: Table::from("users"),
                    fields: Some(vec![Column::from("id"), Column::from("name")]),
                    data: vec![vec![42.into(), "test".into()]],
                    ..Default::default()
                }
            );
        }

        // Issue #3
        #[test]
        fn insert_without_spaces() {
            let qstring = "INSERT INTO users(id, name) VALUES(42, 'test');";

            let res = insertion(Dialect::MySQL)(qstring.as_bytes());
            assert_eq!(
                res.unwrap().1,
                InsertStatement {
                    table: Table::from("users"),
                    fields: Some(vec![Column::from("id"), Column::from("name")]),
                    data: vec![vec![42.into(), "test".into()]],
                    ..Default::default()
                }
            );
        }

        #[test]
        fn simple_insert_schema() {
            let qstring = "INSERT INTO db1.users VALUES (42, \"test\");";

            let res = insertion(Dialect::MySQL)(qstring.as_bytes());
            assert_eq!(
                res.unwrap().1,
                InsertStatement {
                    table: Table::from(("db1", "users")),
                    fields: None,
                    data: vec![vec![42.into(), "test".into()]],
                    ..Default::default()
                }
            );
        }

        #[test]
        fn multi_insert() {
            let qstring = "INSERT INTO users (id, name) VALUES (42, \"test\"),(21, \"test2\");";

            let res = insertion(Dialect::MySQL)(qstring.as_bytes());
            assert_eq!(
                res.unwrap().1,
                InsertStatement {
                    table: Table::from("users"),
                    fields: Some(vec![Column::from("id"), Column::from("name")]),
                    data: vec![
                        vec![42.into(), "test".into()],
                        vec![21.into(), "test2".into()],
                    ],
                    ..Default::default()
                }
            );
        }

        #[test]
        fn insert_with_on_dup_update() {
            let qstring = "INSERT INTO keystores (`key`, `value`) VALUES ($1, :2) \
                       ON DUPLICATE KEY UPDATE `value` = `value` + 1";

            let res = insertion(Dialect::MySQL)(qstring.as_bytes());
            assert_eq!(
                res.unwrap().1,
                InsertStatement {
                    table: Table::from("keystores"),
                    fields: Some(vec![Column::from("key"), Column::from("value")]),
                    data: vec![vec![
                        Literal::Placeholder(ItemPlaceholder::DollarNumber(1)),
                        Literal::Placeholder(ItemPlaceholder::ColonNumber(2))
                    ]],
                    on_duplicate: Some(vec![(
                        Column::from("value"),
                        Expression::BinaryOp {
                            op: BinaryOperator::Add,
                            lhs: Box::new(Expression::Column(Column::from("value"))),
                            rhs: Box::new(Expression::Literal(1.into()))
                        },
                    ),]),
                    ..Default::default()
                }
            );
        }

        #[test]
        fn insert_with_leading_value_whitespace() {
            let qstring = "INSERT INTO users (id, name) VALUES ( 42, \"test\");";

            let res = insertion(Dialect::MySQL)(qstring.as_bytes());
            assert_eq!(
                res.unwrap().1,
                InsertStatement {
                    table: Table::from("users"),
                    fields: Some(vec![Column::from("id"), Column::from("name")]),
                    data: vec![vec![42.into(), "test".into()]],
                    ..Default::default()
                }
            );
        }

        #[test]
        fn stringify_insert_with_reserved_keyword_col() {
            let orig = b"INSERT INTO users (`id`, `name`, `key`) VALUES (1, 'bob', 1);";
            let parsed = test_parse!(insertion(Dialect::MySQL), orig);
            let stringified = parsed.to_string();
            let parsed_again = test_parse!(insertion(Dialect::MySQL), stringified.as_bytes());
            assert_eq!(parsed, parsed_again);
        }
    }

    mod postgres {
        use super::*;
        use crate::column::Column;
        use crate::common::ItemPlaceholder;
        use crate::table::Table;
        use crate::BinaryOperator;

        #[test]
        fn simple_insert() {
            let qstring = "INSERT INTO users VALUES (42, 'test');";

            let res = insertion(Dialect::PostgreSQL)(qstring.as_bytes());
            assert_eq!(
                res.unwrap().1,
                InsertStatement {
                    table: Table::from("users"),
                    fields: None,
                    data: vec![vec![42.into(), "test".into()]],
                    ..Default::default()
                }
            );
        }

        #[test]
        fn complex_insert() {
            let qstring = "INSERT INTO users VALUES (42, 'test', 'test', CURRENT_TIMESTAMP);";

            let res = insertion(Dialect::PostgreSQL)(qstring.as_bytes());
            assert_eq!(
                res.unwrap().1,
                InsertStatement {
                    table: Table::from("users"),
                    fields: None,
                    data: vec![vec![
                        42.into(),
                        "test".into(),
                        "test".into(),
                        Literal::CurrentTimestamp,
                    ],],
                    ..Default::default()
                }
            );
        }

        #[test]
        fn insert_with_field_names() {
            let qstring = "INSERT INTO users (id, name) VALUES (42, 'test');";

            let res = insertion(Dialect::PostgreSQL)(qstring.as_bytes());
            assert_eq!(
                res.unwrap().1,
                InsertStatement {
                    table: Table::from("users"),
                    fields: Some(vec![Column::from("id"), Column::from("name")]),
                    data: vec![vec![42.into(), "test".into()]],
                    ..Default::default()
                }
            );
        }

        // Issue #3
        #[test]
        fn insert_without_spaces() {
            let qstring = "INSERT INTO users(id, name) VALUES(42, 'test');";

            let res = insertion(Dialect::PostgreSQL)(qstring.as_bytes());
            assert_eq!(
                res.unwrap().1,
                InsertStatement {
                    table: Table::from("users"),
                    fields: Some(vec![Column::from("id"), Column::from("name")]),
                    data: vec![vec![42.into(), "test".into()]],
                    ..Default::default()
                }
            );
        }

        #[test]
        fn simple_insert_schema() {
            let qstring = "INSERT INTO db1.users VALUES (42, 'test');";

            let res = insertion(Dialect::PostgreSQL)(qstring.as_bytes());
            assert_eq!(
                res.unwrap().1,
                InsertStatement {
                    table: Table::from(("db1", "users")),
                    fields: None,
                    data: vec![vec![42.into(), "test".into()]],
                    ..Default::default()
                }
            );
        }

        #[test]
        fn multi_insert() {
            let qstring = "INSERT INTO users (id, name) VALUES (42, 'test'),(21, 'test2');";

            let res = insertion(Dialect::PostgreSQL)(qstring.as_bytes());
            assert_eq!(
                res.unwrap().1,
                InsertStatement {
                    table: Table::from("users"),
                    fields: Some(vec![Column::from("id"), Column::from("name")]),
                    data: vec![
                        vec![42.into(), "test".into()],
                        vec![21.into(), "test2".into()],
                    ],
                    ..Default::default()
                }
            );
        }

        #[test]
        fn insert_with_on_dup_update() {
            let qstring = "INSERT INTO keystores (\"key\", \"value\") VALUES ($1, :2) \
                       ON DUPLICATE KEY UPDATE \"value\" = \"value\" + 1";

            let res = insertion(Dialect::PostgreSQL)(qstring.as_bytes());
            assert_eq!(
                res.unwrap().1,
                InsertStatement {
                    table: Table::from("keystores"),
                    fields: Some(vec![Column::from("key"), Column::from("value")]),
                    data: vec![vec![
                        Literal::Placeholder(ItemPlaceholder::DollarNumber(1)),
                        Literal::Placeholder(ItemPlaceholder::ColonNumber(2))
                    ]],
                    on_duplicate: Some(vec![(
                        Column::from("value"),
                        Expression::BinaryOp {
                            op: BinaryOperator::Add,
                            lhs: Box::new(Expression::Column(Column::from("value"))),
                            rhs: Box::new(Expression::Literal(1.into()))
                        },
                    ),]),
                    ..Default::default()
                }
            );
        }

        #[test]
        fn insert_with_leading_value_whitespace() {
            let qstring = "INSERT INTO users (id, name) VALUES ( 42, 'test');";

            let res = insertion(Dialect::PostgreSQL)(qstring.as_bytes());
            assert_eq!(
                res.unwrap().1,
                InsertStatement {
                    table: Table::from("users"),
                    fields: Some(vec![Column::from("id"), Column::from("name")]),
                    data: vec![vec![42.into(), "test".into()]],
                    ..Default::default()
                }
            );
        }
    }
}
