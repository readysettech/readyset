use std::{fmt, str};

use nom::bytes::complete::{tag, tag_no_case};
use nom::combinator::opt;
use nom::multi::separated_list1;
use nom::sequence::{delimited, preceded, terminated, tuple};
use nom::IResult;
use serde::{Deserialize, Serialize};

use crate::column::Column;
use crate::common::{
    assignment_expr_list, field_list, schema_table_reference_no_alias, statement_terminator,
    value_list, ws_sep_comma,
};
use crate::table::Table;
use crate::whitespace::{whitespace0, whitespace1};
use crate::{Dialect, Expression, Literal};

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
        write!(f, "INSERT INTO `{}`", self.table.name)?;
        if let Some(ref fields) = self.fields {
            write!(
                f,
                " ({})",
                fields
                    .iter()
                    .map(|col| format!("`{}`", col.name))
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
            preceded(tag("("), whitespace0),
            field_list(dialect),
            delimited(whitespace0, tag(")"), whitespace1),
        )(i)
    }
}

fn data(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], Vec<Literal>> {
    move |i| {
        delimited(
            terminated(tag("("), whitespace0),
            value_list(dialect),
            preceded(whitespace0, tag(")")),
        )(i)
    }
}

fn on_duplicate(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], Vec<(Column, Expression)>> {
    move |i| {
        preceded(
            whitespace0,
            preceded(
                tag_no_case("on duplicate key update"),
                preceded(whitespace1, assignment_expr_list(dialect)),
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
            opt(preceded(whitespace1, tag_no_case("ignore"))),
            whitespace1,
            tag_no_case("into"),
            whitespace1,
            schema_table_reference_no_alias(dialect),
            whitespace0,
            opt(fields(dialect)),
            tag_no_case("values"),
            whitespace0,
            separated_list1(ws_sep_comma, data(dialect)),
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
    use crate::literal::ItemPlaceholder;
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
        use crate::literal::ItemPlaceholder;
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
