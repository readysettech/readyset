use nom::character::complete::{multispace0, multispace1};
use std::{fmt, str};

use crate::column::Column;
use crate::common::{assignment_expr_list, statement_terminator, table_reference};
use crate::keywords::escape_if_keyword;
use crate::select::where_clause;
use crate::table::Table;
use crate::{Dialect, Expression};
use nom::bytes::complete::tag_no_case;
use nom::combinator::opt;
use nom::sequence::tuple;
use nom::IResult;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct UpdateStatement {
    pub table: Table,
    pub fields: Vec<(Column, Expression)>,
    pub where_clause: Option<Expression>,
}

impl fmt::Display for UpdateStatement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "UPDATE {} ", escape_if_keyword(&self.table.name))?;
        assert!(!self.fields.is_empty());
        write!(
            f,
            "SET {}",
            self.fields
                .iter()
                .map(|&(ref col, ref literal)| format!("{} = {}", col, literal.to_string()))
                .collect::<Vec<_>>()
                .join(", ")
        )?;
        if let Some(ref where_clause) = self.where_clause {
            write!(f, " WHERE ")?;
            write!(f, "{}", where_clause)?;
        }
        Ok(())
    }
}

pub fn updating(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], UpdateStatement> {
    move |i| {
        let (remaining_input, (_, _, table, _, _, _, fields, _, where_clause, _)) = tuple((
            tag_no_case("update"),
            multispace1,
            table_reference(dialect),
            multispace1,
            tag_no_case("set"),
            multispace1,
            assignment_expr_list(dialect),
            multispace0,
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
    use crate::common::{ItemPlaceholder, Literal};
    use crate::table::Table;
    use crate::BinaryOperator;

    #[test]
    fn simple_update() {
        let qstring = "UPDATE users SET id = 42, name = 'test'";

        let res = updating(Dialect::MySQL)(qstring.as_bytes());
        assert_eq!(
            res.unwrap().1,
            UpdateStatement {
                table: Table::from("users"),
                fields: vec![
                    (Column::from("id"), Expression::Literal(42.into())),
                    (Column::from("name"), Expression::Literal("test".into())),
                ],
                ..Default::default()
            }
        );
    }

    #[test]
    fn update_with_where_clause() {
        let qstring = "UPDATE users SET id = 42, name = 'test' WHERE id = 1";

        let res = updating(Dialect::MySQL)(qstring.as_bytes());
        let expected_left = Expression::Column(Column::from("id"));
        let expected_where_cond = Some(Expression::BinaryOp {
            lhs: Box::new(expected_left),
            rhs: Box::new(Expression::Literal(Literal::Integer(1))),
            op: BinaryOperator::Equal,
        });
        assert_eq!(
            res.unwrap().1,
            UpdateStatement {
                table: Table::from("users"),
                fields: vec![
                    (Column::from("id"), Expression::Literal(Literal::from(42)),),
                    (
                        Column::from("name"),
                        Expression::Literal(Literal::from("test",)),
                    ),
                ],
                where_clause: expected_where_cond,
            }
        );
    }

    #[test]
    fn format_update_with_where_clause() {
        let qstring = "UPDATE users SET id = 42, name = 'test' WHERE id = 1";
        let expected = "UPDATE users SET id = 42, name = 'test' WHERE (id = 1)";
        let res = updating(Dialect::MySQL)(qstring.as_bytes());
        assert_eq!(format!("{}", res.unwrap().1), expected);
    }

    #[test]
    fn update_with_arithmetic_and_where() {
        let qstring = "UPDATE users SET karma = karma + 1 WHERE users.id = ?;";

        let res = updating(Dialect::MySQL)(qstring.as_bytes());
        let expected_where_cond = Some(Expression::BinaryOp {
            lhs: Box::new(Expression::Column(Column::from("users.id"))),
            rhs: Box::new(Expression::Literal(Literal::Placeholder(
                ItemPlaceholder::QuestionMark,
            ))),
            op: BinaryOperator::Equal,
        });
        assert_eq!(
            res.unwrap().1,
            UpdateStatement {
                table: Table::from("users"),
                fields: vec![(
                    Column::from("karma"),
                    Expression::BinaryOp {
                        op: BinaryOperator::Add,
                        lhs: Box::new(Expression::Column(Column::from("karma"))),
                        rhs: Box::new(Expression::Literal(1.into()))
                    },
                ),],
                where_clause: expected_where_cond,
            }
        );
    }

    mod mysql {
        use super::*;
        use crate::column::Column;
        use crate::common::{ItemPlaceholder, Literal};
        use crate::table::Table;
        use crate::{BinaryOperator, Double, FunctionExpression};

        #[test]
        fn updated_with_neg_float() {
            let qstring =
                "UPDATE `stories` SET `hotness` = -19216.5479744 WHERE `stories`.`id` = ?";

            let res = updating(Dialect::MySQL)(qstring.as_bytes());
            let expected_left = Expression::Column(Column::from("stories.id"));
            let expected_where_cond = Some(Expression::BinaryOp {
                lhs: Box::new(expected_left),
                rhs: Box::new(Expression::Literal(Literal::Placeholder(
                    ItemPlaceholder::QuestionMark,
                ))),
                op: BinaryOperator::Equal,
            });
            assert_eq!(
                res.unwrap().1,
                UpdateStatement {
                    table: Table::from("stories"),
                    fields: vec![(
                        Column::from("hotness"),
                        Expression::Literal(Literal::Double(Double {
                            value: -19216.5479744,
                            precision: 7,
                        })),
                    )],
                    where_clause: expected_where_cond,
                    ..Default::default()
                }
            );
        }

        #[test]
        fn update_with_arithmetic() {
            let qstring = "UPDATE users SET karma = karma + 1;";

            let res = updating(Dialect::MySQL)(qstring.as_bytes());
            assert_eq!(
                res.unwrap().1,
                UpdateStatement {
                    table: Table::from("users"),
                    fields: vec![(
                        Column::from("karma"),
                        Expression::BinaryOp {
                            op: BinaryOperator::Add,
                            lhs: Box::new(Expression::Column(Column::from("karma"))),
                            rhs: Box::new(Expression::Literal(1.into()))
                        },
                    ),],
                    ..Default::default()
                }
            );
        }

        #[test]
        fn flarum_update_1() {
            let qstring = b"update `group_permission` set `permission` = REPLACE(permission,  'viewDiscussions', 'viewForum') where `permission` LIKE '%viewDiscussions'";
            let res = test_parse!(updating(Dialect::MySQL), qstring);
            assert_eq!(
                res,
                UpdateStatement {
                    table: Table::from("group_permission"),
                    fields: vec![(
                        Column::from("permission"),
                        Expression::Call(FunctionExpression::Call {
                            name: "REPLACE".to_string(),
                            arguments: vec![
                                Expression::Column(Column::from("permission")),
                                Expression::Literal(Literal::String("viewDiscussions".into())),
                                Expression::Literal(Literal::String("viewForum".into())),
                            ]
                        })
                    )],
                    where_clause: Some(Expression::BinaryOp {
                        lhs: Box::new(Expression::Column(Column::from("permission"))),
                        op: BinaryOperator::Like,
                        rhs: Box::new(Expression::Literal(Literal::String(
                            "%viewDiscussions".into()
                        ))),
                    }),
                    ..Default::default()
                }
            );
        }
    }

    mod postgres {
        use super::*;
        use crate::column::Column;
        use crate::common::{ItemPlaceholder, Literal};
        use crate::table::Table;
        use crate::{BinaryOperator, Double};

        #[test]
        fn updated_with_neg_float() {
            let qstring =
                "UPDATE \"stories\" SET \"hotness\" = -19216.5479744 WHERE \"stories\".\"id\" = ?";

            let res = updating(Dialect::PostgreSQL)(qstring.as_bytes());
            let expected_left = Expression::Column(Column::from("stories.id"));
            let expected_where_cond = Some(Expression::BinaryOp {
                lhs: Box::new(expected_left),
                rhs: Box::new(Expression::Literal(Literal::Placeholder(
                    ItemPlaceholder::QuestionMark,
                ))),
                op: BinaryOperator::Equal,
            });
            assert_eq!(
                res.unwrap().1,
                UpdateStatement {
                    table: Table::from("stories"),
                    fields: vec![(
                        Column::from("hotness"),
                        Expression::Literal(Literal::Double(Double {
                            value: -19216.5479744,
                            precision: 7,
                        })),
                    ),],
                    where_clause: expected_where_cond,
                    ..Default::default()
                }
            );
        }

        #[test]
        fn update_with_arithmetic() {
            let qstring = "UPDATE users SET karma = karma + 1;";

            let res = updating(Dialect::PostgreSQL)(qstring.as_bytes());
            assert_eq!(
                res.unwrap().1,
                UpdateStatement {
                    table: Table::from("users"),
                    fields: vec![(
                        Column::from("karma"),
                        Expression::BinaryOp {
                            op: BinaryOperator::Add,
                            lhs: Box::new(Expression::Column(Column::from("karma"))),
                            rhs: Box::new(Expression::Literal(1.into()))
                        },
                    ),],
                    ..Default::default()
                }
            );
        }
    }
}
