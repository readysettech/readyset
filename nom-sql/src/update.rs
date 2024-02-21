use std::{fmt, str};

use itertools::Itertools;
use nom::bytes::complete::tag_no_case;
use nom::combinator::opt;
use nom::sequence::tuple;
use nom_locate::LocatedSpan;
use readyset_util::fmt::fmt_with;
use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

use crate::column::Column;
use crate::common::{assignment_expr_list, statement_terminator};
use crate::select::where_clause;
use crate::table::{relation, Relation};
use crate::whitespace::{whitespace0, whitespace1};
use crate::{Dialect, DialectDisplay, Expr, NomSqlResult};

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct UpdateStatement {
    pub table: Relation,
    pub fields: Vec<(Column, Expr)>,
    pub where_clause: Option<Expr>,
}

impl DialectDisplay for UpdateStatement {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| {
            write!(f, "UPDATE {} ", self.table.display(dialect))?;

            // TODO: Consider using `Vec1`.
            assert!(!self.fields.is_empty());
            write!(
                f,
                "SET {}",
                self.fields
                    .iter()
                    .map(|(col, literal)| format!(
                        "{} = {}",
                        col.display(dialect),
                        literal.display(dialect)
                    ))
                    .join(", ")
            )?;

            if let Some(ref where_clause) = self.where_clause {
                write!(f, " WHERE ")?;
                write!(f, "{}", where_clause.display(dialect))?;
            }

            Ok(())
        })
    }
}

pub fn updating(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], UpdateStatement> {
    move |i| {
        let (remaining_input, (_, _, table, _, _, _, fields, _, where_clause, _)) = tuple((
            tag_no_case("update"),
            whitespace1,
            relation(dialect),
            whitespace1,
            tag_no_case("set"),
            whitespace1,
            assignment_expr_list(dialect),
            whitespace0,
            opt(where_clause(dialect)),
            statement_terminator,
        ))(i)?;
        Ok((
            remaining_input,
            UpdateStatement {
                table,
                fields,
                where_clause,
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column::Column;
    use crate::table::Relation;
    use crate::{BinaryOperator, ItemPlaceholder, Literal};

    #[test]
    fn simple_update() {
        let qstring = "UPDATE users SET id = 42, name = 'test'";

        let res = updating(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
        assert_eq!(
            res.unwrap().1,
            UpdateStatement {
                table: Relation::from("users"),
                fields: vec![
                    (Column::from("id"), Expr::Literal(42.into())),
                    (Column::from("name"), Expr::Literal("test".into())),
                ],
                where_clause: None
            }
        );
    }

    #[test]
    fn update_with_where_clause() {
        let qstring = "UPDATE users SET id = 42, name = 'test' WHERE id = 1";

        let res = updating(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
        let expected_left = Expr::Column(Column::from("id"));
        let expected_where_cond = Some(Expr::BinaryOp {
            lhs: Box::new(expected_left),
            rhs: Box::new(Expr::Literal(Literal::Integer(1))),
            op: BinaryOperator::Equal,
        });
        assert_eq!(
            res.unwrap().1,
            UpdateStatement {
                table: Relation::from("users"),
                fields: vec![
                    (Column::from("id"), Expr::Literal(Literal::from(42)),),
                    (Column::from("name"), Expr::Literal(Literal::from("test",)),),
                ],
                where_clause: expected_where_cond,
            }
        );
    }

    #[test]
    fn update_with_arithmetic_and_where() {
        let qstring = "UPDATE users SET karma = karma + 1 WHERE users.id = ?;";

        let res = updating(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
        let expected_where_cond = Some(Expr::BinaryOp {
            lhs: Box::new(Expr::Column(Column::from("users.id"))),
            rhs: Box::new(Expr::Literal(Literal::Placeholder(
                ItemPlaceholder::QuestionMark,
            ))),
            op: BinaryOperator::Equal,
        });
        assert_eq!(
            res.unwrap().1,
            UpdateStatement {
                table: Relation::from("users"),
                fields: vec![(
                    Column::from("karma"),
                    Expr::BinaryOp {
                        op: BinaryOperator::Add,
                        lhs: Box::new(Expr::Column(Column::from("karma"))),
                        rhs: Box::new(Expr::Literal(1.into()))
                    },
                ),],
                where_clause: expected_where_cond,
            }
        );
    }

    mod mysql {
        use super::*;
        use crate::column::Column;
        use crate::table::Relation;
        use crate::{BinaryOperator, Double, FunctionExpr, ItemPlaceholder};

        #[test]
        fn updated_with_neg_float() {
            let qstring =
                "UPDATE `stories` SET `hotness` = -19216.5479744 WHERE `stories`.`id` = ?";

            let res = updating(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
            let expected_left = Expr::Column(Column::from("stories.id"));
            let expected_where_cond = Some(Expr::BinaryOp {
                lhs: Box::new(expected_left),
                rhs: Box::new(Expr::Literal(Literal::Placeholder(
                    ItemPlaceholder::QuestionMark,
                ))),
                op: BinaryOperator::Equal,
            });
            assert_eq!(
                res.unwrap().1,
                UpdateStatement {
                    table: Relation::from("stories"),
                    fields: vec![(
                        Column::from("hotness"),
                        Expr::Literal(Literal::Double(Double {
                            value: -19216.5479744,
                            precision: 7,
                        }))
                    )],
                    where_clause: expected_where_cond,
                }
            );
        }

        #[test]
        fn update_with_arithmetic() {
            let qstring = "UPDATE users SET karma = karma + 1;";

            let res = updating(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
            assert_eq!(
                res.unwrap().1,
                UpdateStatement {
                    table: Relation::from("users"),
                    fields: vec![(
                        Column::from("karma"),
                        Expr::BinaryOp {
                            op: BinaryOperator::Add,
                            lhs: Box::new(Expr::Column(Column::from("karma"))),
                            rhs: Box::new(Expr::Literal(1.into()))
                        },
                    ),],
                    where_clause: None
                }
            );
        }

        #[test]
        fn format_update_with_where_clause() {
            let qstring = "UPDATE users SET id = 42, name = 'test' WHERE id = 1";
            let expected = "UPDATE `users` SET `id` = 42, `name` = 'test' WHERE (`id` = 1)";
            let res = updating(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
            assert_eq!(res.unwrap().1.display(Dialect::MySQL).to_string(), expected);
        }

        #[test]
        fn flarum_update_1() {
            let qstring = b"update `group_permission` set `permission` = REPLACE(permission,  'viewDiscussions', 'viewForum') where `permission` LIKE '%viewDiscussions'";
            let res = test_parse!(updating(Dialect::MySQL), qstring);
            assert_eq!(
                res,
                UpdateStatement {
                    table: Relation::from("group_permission"),
                    fields: vec![(
                        Column::from("permission"),
                        Expr::Call(FunctionExpr::Call {
                            name: "REPLACE".into(),
                            arguments: vec![
                                Expr::Column(Column::from("permission")),
                                Expr::Literal(Literal::String("viewDiscussions".into())),
                                Expr::Literal(Literal::String("viewForum".into())),
                            ]
                        })
                    )],
                    where_clause: Some(Expr::BinaryOp {
                        lhs: Box::new(Expr::Column(Column::from("permission"))),
                        op: BinaryOperator::Like,
                        rhs: Box::new(Expr::Literal(Literal::String("%viewDiscussions".into()))),
                    }),
                }
            );
        }
    }

    mod postgres {
        use super::*;
        use crate::column::Column;
        use crate::table::Relation;
        use crate::{BinaryOperator, Double};

        #[test]
        fn updated_with_neg_float() {
            let qstring =
                "UPDATE \"stories\" SET \"hotness\" = -19216.5479744 WHERE \"stories\".\"id\" = ?";

            let res = updating(Dialect::PostgreSQL)(LocatedSpan::new(qstring.as_bytes()));
            let expected_left = Expr::Column(Column::from("stories.id"));
            let expected_where_cond = Some(Expr::BinaryOp {
                lhs: Box::new(expected_left),
                rhs: Box::new(Expr::Literal(Literal::Placeholder(
                    ItemPlaceholder::QuestionMark,
                ))),
                op: BinaryOperator::Equal,
            });
            assert_eq!(
                res.unwrap().1,
                UpdateStatement {
                    table: Relation::from("stories"),
                    fields: vec![(
                        Column::from("hotness"),
                        Expr::Literal(Literal::Double(Double {
                            value: -19216.5479744,
                            precision: 7,
                        }))
                    ),],
                    where_clause: expected_where_cond,
                }
            );
        }

        #[test]
        fn update_with_arithmetic() {
            let qstring = "UPDATE users SET karma = karma + 1;";

            let res = updating(Dialect::PostgreSQL)(LocatedSpan::new(qstring.as_bytes()));
            assert_eq!(
                res.unwrap().1,
                UpdateStatement {
                    table: Relation::from("users"),
                    fields: vec![(
                        Column::from("karma"),
                        Expr::BinaryOp {
                            op: BinaryOperator::Add,
                            lhs: Box::new(Expr::Column(Column::from("karma"))),
                            rhs: Box::new(Expr::Literal(1.into()))
                        },
                    ),],
                    where_clause: None
                }
            );
        }

        #[test]
        fn format_update_with_where_clause() {
            let qstring = "UPDATE users SET id = 42, name = 'test' WHERE id = 1";
            let expected = "UPDATE \"users\" SET \"id\" = 42, \"name\" = 'test' WHERE (\"id\" = 1)";
            let res = updating(Dialect::PostgreSQL)(LocatedSpan::new(qstring.as_bytes()));
            assert_eq!(
                res.unwrap().1.display(Dialect::PostgreSQL).to_string(),
                expected
            );
        }
    }
}
