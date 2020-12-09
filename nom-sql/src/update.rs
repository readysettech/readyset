use nom::character::complete::{multispace0, multispace1};
use std::{fmt, str};

use column::Column;
use common::{assignment_expr_list, statement_terminator, table_reference, FieldValueExpression};
use condition::ConditionExpression;
use keywords::escape_if_keyword;
use nom::bytes::complete::tag_no_case;
use nom::combinator::opt;
use nom::sequence::tuple;
use nom::IResult;
use select::where_clause;
use table::Table;

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct UpdateStatement {
    pub table: Table,
    pub fields: Vec<(Column, FieldValueExpression)>,
    pub where_clause: Option<ConditionExpression>,
}

impl fmt::Display for UpdateStatement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "UPDATE {} ", escape_if_keyword(&self.table.name))?;
        assert!(self.fields.len() > 0);
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

pub fn updating(i: &[u8]) -> IResult<&[u8], UpdateStatement> {
    let (remaining_input, (_, _, table, _, _, _, fields, _, where_clause, _)) = tuple((
        tag_no_case("update"),
        multispace1,
        table_reference,
        multispace1,
        tag_no_case("set"),
        multispace1,
        assignment_expr_list,
        multispace0,
        opt(where_clause),
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

#[cfg(test)]
mod tests {
    use super::*;
    use arithmetic::{ArithmeticBase, ArithmeticExpression, ArithmeticOperator};
    use column::Column;
    use common::{ItemPlaceholder, Literal, LiteralExpression, Operator, Real};
    use condition::ConditionBase::*;
    use condition::ConditionExpression::*;
    use condition::ConditionTree;
    use table::Table;

    #[test]
    fn simple_update() {
        let qstring = "UPDATE users SET id = 42, name = 'test'";

        let res = updating(qstring.as_bytes());
        assert_eq!(
            res.unwrap().1,
            UpdateStatement {
                table: Table::from("users"),
                fields: vec![
                    (
                        Column::from("id"),
                        FieldValueExpression::Literal(LiteralExpression::from(Literal::from(42))),
                    ),
                    (
                        Column::from("name"),
                        FieldValueExpression::Literal(LiteralExpression::from(Literal::from(
                            "test",
                        ))),
                    ),
                ],
                ..Default::default()
            }
        );
    }

    #[test]
    fn update_with_where_clause() {
        let qstring = "UPDATE users SET id = 42, name = 'test' WHERE id = 1";

        let res = updating(qstring.as_bytes());
        let expected_left = Base(Field(Column::from("id")));
        let expected_where_cond = Some(ComparisonOp(ConditionTree {
            left: Box::new(expected_left),
            right: Box::new(Base(Literal(Literal::Integer(1)))),
            operator: Operator::Equal,
        }));
        assert_eq!(
            res.unwrap().1,
            UpdateStatement {
                table: Table::from("users"),
                fields: vec![
                    (
                        Column::from("id"),
                        FieldValueExpression::Literal(LiteralExpression::from(Literal::from(42))),
                    ),
                    (
                        Column::from("name"),
                        FieldValueExpression::Literal(LiteralExpression::from(Literal::from(
                            "test",
                        ))),
                    ),
                ],
                where_clause: expected_where_cond,
                ..Default::default()
            }
        );
    }

    #[test]
    fn format_update_with_where_clause() {
        let qstring = "UPDATE users SET id = 42, name = 'test' WHERE id = 1";
        let expected = "UPDATE users SET id = 42, name = 'test' WHERE id = 1";
        let res = updating(qstring.as_bytes());
        assert_eq!(format!("{}", res.unwrap().1), expected);
    }

    #[test]
    fn updated_with_neg_float() {
        let qstring = "UPDATE `stories` SET `hotness` = -19216.5479744 WHERE `stories`.`id` = ?";

        let res = updating(qstring.as_bytes());
        let expected_left = Base(Field(Column::from("stories.id")));
        let expected_where_cond = Some(ComparisonOp(ConditionTree {
            left: Box::new(expected_left),
            right: Box::new(Base(Literal(Literal::Placeholder(
                ItemPlaceholder::QuestionMark,
            )))),
            operator: Operator::Equal,
        }));
        assert_eq!(
            res.unwrap().1,
            UpdateStatement {
                table: Table::from("stories"),
                fields: vec![(
                    Column::from("hotness"),
                    FieldValueExpression::Literal(LiteralExpression::from(Literal::FixedPoint(
                        Real {
                            integral: -19216,
                            fractional: 5479744,
                        }
                    ),)),
                ),],
                where_clause: expected_where_cond,
                ..Default::default()
            }
        );
    }

    #[test]
    fn update_with_arithmetic_and_where() {
        let qstring = "UPDATE users SET karma = karma + 1 WHERE users.id = ?;";

        let res = updating(qstring.as_bytes());
        let expected_where_cond = Some(ComparisonOp(ConditionTree {
            left: Box::new(Base(Field(Column::from("users.id")))),
            right: Box::new(Base(Literal(Literal::Placeholder(
                ItemPlaceholder::QuestionMark,
            )))),
            operator: Operator::Equal,
        }));
        let expected_ae = ArithmeticExpression::new(
            ArithmeticOperator::Add,
            ArithmeticBase::Column(Column::from("karma")),
            ArithmeticBase::Scalar(1.into()),
            None,
        );
        assert_eq!(
            res.unwrap().1,
            UpdateStatement {
                table: Table::from("users"),
                fields: vec![(
                    Column::from("karma"),
                    FieldValueExpression::Arithmetic(expected_ae),
                ),],
                where_clause: expected_where_cond,
                ..Default::default()
            }
        );
    }

    #[test]
    fn update_with_arithmetic() {
        let qstring = "UPDATE users SET karma = karma + 1;";

        let res = updating(qstring.as_bytes());
        let expected_ae = ArithmeticExpression::new(
            ArithmeticOperator::Add,
            ArithmeticBase::Column(Column::from("karma")),
            ArithmeticBase::Scalar(1.into()),
            None,
        );
        assert_eq!(
            res.unwrap().1,
            UpdateStatement {
                table: Table::from("users"),
                fields: vec![(
                    Column::from("karma"),
                    FieldValueExpression::Arithmetic(expected_ae),
                ),],
                ..Default::default()
            }
        );
    }
}
