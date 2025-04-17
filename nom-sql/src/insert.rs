use nom::bytes::complete::{tag, tag_no_case};
use nom::combinator::opt;
use nom::multi::separated_list1;
use nom::sequence::{delimited, preceded, terminated, tuple};
use nom_locate::LocatedSpan;
use readyset_sql::{ast::*, Dialect};

use crate::common::{
    assignment_expr_list, field_list, statement_terminator, value_list, ws_sep_comma,
};
use crate::table::relation;
use crate::whitespace::{whitespace0, whitespace1};
use crate::NomSqlResult;

fn fields(dialect: Dialect) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Vec<Column>> {
    move |i| {
        delimited(
            preceded(tag("("), whitespace0),
            field_list(dialect),
            delimited(whitespace0, tag(")"), whitespace1),
        )(i)
    }
}

fn data(dialect: Dialect) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Vec<Expr>> {
    move |i| {
        delimited(
            terminated(tag("("), whitespace0),
            value_list(dialect),
            preceded(whitespace0, tag(")")),
        )(i)
    }
}

#[allow(clippy::type_complexity)]
fn on_duplicate(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Vec<(Column, Expr)>> {
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
pub fn insertion(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], InsertStatement> {
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
            relation(dialect),
            whitespace0,
            opt(fields(dialect)),
            tag_no_case("values"),
            whitespace0,
            separated_list1(ws_sep_comma, data(dialect)),
            opt(on_duplicate(dialect)),
            statement_terminator,
        ))(i)?;
        let ignore = ignore_res.is_some();

        Ok((
            remaining_input,
            InsertStatement {
                table,
                fields: fields.unwrap_or_default(),
                data,
                ignore,
                on_duplicate,
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use readyset_sql::DialectDisplay;

    use super::*;

    #[test]
    fn insert_with_parameters() {
        let qstring = "INSERT INTO users (id, name) VALUES (?, ?);";

        let res = insertion(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
        assert_eq!(
            res.unwrap().1,
            InsertStatement {
                table: Relation::from("users"),
                fields: vec![Column::from("id"), Column::from("name")],
                data: vec![vec![
                    Expr::Literal(Literal::Placeholder(ItemPlaceholder::QuestionMark)),
                    Expr::Literal(Literal::Placeholder(ItemPlaceholder::QuestionMark))
                ]],
                on_duplicate: None,
                ignore: false
            }
        );
    }

    mod mysql {
        use super::*;

        #[test]
        fn simple_insert() {
            let qstring = "INSERT INTO users VALUES (42, \"test\");";

            let res = insertion(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
            assert_eq!(
                res.unwrap().1,
                InsertStatement {
                    table: Relation::from("users"),
                    fields: vec![],
                    data: vec![vec![Expr::Literal(42.into()), Expr::Literal("test".into())]],
                    on_duplicate: None,
                    ignore: false
                }
            );
        }

        #[test]
        fn complex_insert() {
            let res = test_parse!(
                insertion(Dialect::MySQL),
                b"INSERT INTO users VALUES (42, 'test', 'test', CURRENT_TIMESTAMP);"
            );

            assert_eq!(
                res,
                InsertStatement {
                    table: Relation::from("users"),
                    fields: vec![],
                    data: vec![vec![
                        Expr::Literal(42.into()),
                        Expr::Literal("test".into()),
                        Expr::Literal("test".into()),
                        Expr::Call(FunctionExpr::Call {
                            name: "CURRENT_TIMESTAMP".into(),
                            arguments: vec![]
                        }),
                    ],],
                    on_duplicate: None,
                    ignore: false
                }
            );
        }

        #[test]
        fn insert_with_field_names() {
            let qstring = "INSERT INTO users (id, name) VALUES (42, 'test');";

            let res = insertion(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
            assert_eq!(
                res.unwrap().1,
                InsertStatement {
                    table: Relation::from("users"),
                    fields: vec![Column::from("id"), Column::from("name")],
                    data: vec![vec![Expr::Literal(42.into()), Expr::Literal("test".into())]],
                    on_duplicate: None,
                    ignore: false
                }
            );
        }

        // Issue #3
        #[test]
        fn insert_without_spaces() {
            let qstring = "INSERT INTO users(id, name) VALUES(42, 'test');";

            let res = insertion(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
            assert_eq!(
                res.unwrap().1,
                InsertStatement {
                    table: Relation::from("users"),
                    fields: vec![Column::from("id"), Column::from("name")],
                    data: vec![vec![Expr::Literal(42.into()), Expr::Literal("test".into())]],
                    on_duplicate: None,
                    ignore: false
                }
            );
        }

        #[test]
        fn simple_insert_schema() {
            let qstring = "INSERT INTO db1.users VALUES (42, \"test\");";

            let res = insertion(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
            assert_eq!(
                res.unwrap().1,
                InsertStatement {
                    table: Relation {
                        schema: Some("db1".into()),
                        name: "users".into(),
                    },
                    fields: vec![],
                    data: vec![vec![Expr::Literal(42.into()), Expr::Literal("test".into())]],
                    on_duplicate: None,
                    ignore: false,
                }
            );
        }

        #[test]
        fn multi_insert() {
            let qstring = "INSERT INTO users (id, name) VALUES (42, \"test\"),(21, \"test2\");";

            let res = insertion(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
            assert_eq!(
                res.unwrap().1,
                InsertStatement {
                    table: Relation::from("users"),
                    fields: vec![Column::from("id"), Column::from("name")],
                    data: vec![
                        vec![Expr::Literal(42.into()), Expr::Literal("test".into())],
                        vec![Expr::Literal(21.into()), Expr::Literal("test2".into())],
                    ],
                    on_duplicate: None,
                    ignore: false,
                }
            );
        }

        #[test]
        fn insert_with_on_dup_update() {
            let qstring = "INSERT INTO keystores (`key`, `value`) VALUES ($1, :2) \
                       ON DUPLICATE KEY UPDATE `value` = `value` + 1";

            let res = insertion(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
            assert_eq!(
                res.unwrap().1,
                InsertStatement {
                    table: Relation::from("keystores"),
                    fields: vec![Column::from("key"), Column::from("value")],
                    data: vec![vec![
                        Expr::Literal(Literal::Placeholder(ItemPlaceholder::DollarNumber(1))),
                        Expr::Literal(Literal::Placeholder(ItemPlaceholder::ColonNumber(2)))
                    ]],
                    on_duplicate: Some(vec![(
                        Column::from("value"),
                        Expr::BinaryOp {
                            op: BinaryOperator::Add,
                            lhs: Box::new(Expr::Column(Column::from("value"))),
                            rhs: Box::new(Expr::Literal(1.into()))
                        },
                    )]),
                    ignore: false,
                }
            );
        }

        #[test]
        fn insert_with_leading_value_whitespace() {
            let qstring = "INSERT INTO users (id, name) VALUES ( 42, \"test\");";

            let res = insertion(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
            assert_eq!(
                res.unwrap().1,
                InsertStatement {
                    table: Relation::from("users"),
                    fields: vec![Column::from("id"), Column::from("name")],
                    data: vec![vec![Expr::Literal(42.into()), Expr::Literal("test".into())]],
                    on_duplicate: None,
                    ignore: false,
                }
            );
        }

        #[test]
        fn stringify_insert_with_reserved_keyword_col() {
            let orig = b"INSERT INTO users (`id`, `name`, `key`) VALUES (1, 'bob', 1);";
            let parsed = test_parse!(insertion(Dialect::MySQL), orig);
            let stringified = parsed.display(Dialect::MySQL).to_string();
            let parsed_again = test_parse!(insertion(Dialect::MySQL), stringified.as_bytes());
            assert_eq!(parsed, parsed_again);
        }
    }

    mod postgres {
        use super::*;

        #[test]
        fn simple_insert() {
            let qstring = "INSERT INTO users VALUES (42, 'test');";

            let res = insertion(Dialect::PostgreSQL)(LocatedSpan::new(qstring.as_bytes()));
            assert_eq!(
                res.unwrap().1,
                InsertStatement {
                    table: Relation::from("users"),
                    fields: vec![],
                    data: vec![vec![Expr::Literal(42.into()), Expr::Literal("test".into())]],
                    on_duplicate: None,
                    ignore: false,
                }
            );
        }

        #[test]
        fn complex_insert() {
            let qstring = "INSERT INTO users VALUES (42, 'test', 'test', CURRENT_TIMESTAMP);";

            let res = insertion(Dialect::PostgreSQL)(LocatedSpan::new(qstring.as_bytes()));
            assert_eq!(
                res.unwrap().1,
                InsertStatement {
                    table: Relation::from("users"),
                    fields: vec![],
                    data: vec![vec![
                        Expr::Literal(42.into()),
                        Expr::Literal("test".into()),
                        Expr::Literal("test".into()),
                        Expr::Call(FunctionExpr::Call {
                            name: "current_timestamp".into(),
                            arguments: vec![],
                        }),
                    ],],
                    on_duplicate: None,
                    ignore: false,
                }
            );
        }

        #[test]
        fn insert_with_field_names() {
            let qstring = "INSERT INTO users (id, name) VALUES (42, 'test');";

            let res = insertion(Dialect::PostgreSQL)(LocatedSpan::new(qstring.as_bytes()));
            assert_eq!(
                res.unwrap().1,
                InsertStatement {
                    table: Relation::from("users"),
                    fields: vec![Column::from("id"), Column::from("name")],
                    data: vec![vec![Expr::Literal(42.into()), Expr::Literal("test".into())]],
                    on_duplicate: None,
                    ignore: false,
                }
            );
        }

        // Issue #3
        #[test]
        fn insert_without_spaces() {
            let qstring = "INSERT INTO users(id, name) VALUES(42, 'test');";

            let res = insertion(Dialect::PostgreSQL)(LocatedSpan::new(qstring.as_bytes()));
            assert_eq!(
                res.unwrap().1,
                InsertStatement {
                    table: Relation::from("users"),
                    fields: vec![Column::from("id"), Column::from("name")],
                    data: vec![vec![Expr::Literal(42.into()), Expr::Literal("test".into())]],
                    on_duplicate: None,
                    ignore: false,
                }
            );
        }

        #[test]
        fn simple_insert_schema() {
            let qstring = "INSERT INTO db1.users VALUES (42, 'test');";

            let res = insertion(Dialect::PostgreSQL)(LocatedSpan::new(qstring.as_bytes()));
            assert_eq!(
                res.unwrap().1,
                InsertStatement {
                    table: Relation {
                        schema: Some("db1".into()),
                        name: "users".into(),
                    },
                    fields: vec![],
                    data: vec![vec![Expr::Literal(42.into()), Expr::Literal("test".into())]],
                    on_duplicate: None,
                    ignore: false,
                }
            );
        }

        #[test]
        fn multi_insert() {
            let qstring = "INSERT INTO users (id, name) VALUES (42, 'test'),(21, 'test2');";

            let res = insertion(Dialect::PostgreSQL)(LocatedSpan::new(qstring.as_bytes()));
            assert_eq!(
                res.unwrap().1,
                InsertStatement {
                    table: Relation::from("users"),
                    fields: vec![Column::from("id"), Column::from("name")],
                    data: vec![
                        vec![Expr::Literal(42.into()), Expr::Literal("test".into())],
                        vec![Expr::Literal(21.into()), Expr::Literal("test2".into())],
                    ],
                    ignore: false,
                    on_duplicate: None
                }
            );
        }

        #[test]
        fn insert_with_on_dup_update() {
            let qstring = "INSERT INTO keystores (\"key\", \"value\") VALUES ($1, :2) \
                       ON DUPLICATE KEY UPDATE \"value\" = \"value\" + 1";

            let res = insertion(Dialect::PostgreSQL)(LocatedSpan::new(qstring.as_bytes()));
            assert_eq!(
                res.unwrap().1,
                InsertStatement {
                    table: Relation::from("keystores"),
                    fields: vec![Column::from("key"), Column::from("value")],
                    data: vec![vec![
                        Expr::Literal(Literal::Placeholder(ItemPlaceholder::DollarNumber(1))),
                        Expr::Literal(Literal::Placeholder(ItemPlaceholder::ColonNumber(2)))
                    ]],
                    on_duplicate: Some(vec![(
                        Column::from("value"),
                        Expr::BinaryOp {
                            op: BinaryOperator::Add,
                            lhs: Box::new(Expr::Column(Column::from("value"))),
                            rhs: Box::new(Expr::Literal(1.into()))
                        },
                    ),]),
                    ignore: false
                }
            );
        }

        #[test]
        fn insert_with_leading_value_whitespace() {
            let qstring = "INSERT INTO users (id, name) VALUES ( 42, 'test');";

            let res = insertion(Dialect::PostgreSQL)(LocatedSpan::new(qstring.as_bytes()));
            assert_eq!(
                res.unwrap().1,
                InsertStatement {
                    table: Relation::from("users"),
                    fields: vec![Column::from("id"), Column::from("name")],
                    data: vec![vec![Expr::Literal(42.into()), Expr::Literal("test".into())]],
                    ignore: false,
                    on_duplicate: None
                }
            );
        }
    }
}
