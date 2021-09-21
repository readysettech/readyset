use nom::character::complete::{multispace0, multispace1};
use std::fmt;
use std::str;

use crate::common::{opt_delimited, statement_terminator};
use crate::order::{order_clause, OrderClause};
use crate::select::{limit_clause, nested_selection, LimitClause, SelectStatement};
use crate::Dialect;
use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case};
use nom::combinator::{map, opt};
use nom::multi::many1;
use nom::sequence::{delimited, preceded, tuple};
use nom::IResult;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, Hash, PartialEq, Deserialize, Serialize)]
pub enum CompoundSelectOperator {
    Union,
    DistinctUnion,
    Intersect,
    Except,
}

impl fmt::Display for CompoundSelectOperator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            CompoundSelectOperator::Union => write!(f, "UNION"),
            CompoundSelectOperator::DistinctUnion => write!(f, "UNION DISTINCT"),
            CompoundSelectOperator::Intersect => write!(f, "INTERSECT"),
            CompoundSelectOperator::Except => write!(f, "EXCEPT"),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Deserialize, Serialize)]
pub struct CompoundSelectStatement {
    pub selects: Vec<(Option<CompoundSelectOperator>, SelectStatement)>,
    pub order: Option<OrderClause>,
    pub limit: Option<LimitClause>,
}

impl fmt::Display for CompoundSelectStatement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for (ref op, ref sel) in &self.selects {
            if let Some(o) = op {
                write!(f, " {}", o)?;
            }
            write!(f, " {}", sel)?;
        }
        if let Some(ord) = &self.order {
            write!(f, " {}", ord)?;
        }
        if let Some(lim) = &self.limit {
            write!(f, " {}", lim)?;
        }
        Ok(())
    }
}

// Parse compound operator
fn compound_op(i: &[u8]) -> IResult<&[u8], CompoundSelectOperator> {
    alt((
        map(
            preceded(
                tag_no_case("union"),
                opt(preceded(
                    multispace1,
                    alt((
                        map(tag_no_case("all"), |_| false),
                        map(tag_no_case("distinct"), |_| true),
                    )),
                )),
            ),
            |distinct| match distinct {
                // DISTINCT is the default in both MySQL and SQLite
                None => CompoundSelectOperator::DistinctUnion,
                Some(d) => {
                    if d {
                        CompoundSelectOperator::DistinctUnion
                    } else {
                        CompoundSelectOperator::Union
                    }
                }
            },
        ),
        map(tag_no_case("intersect"), |_| {
            CompoundSelectOperator::Intersect
        }),
        map(tag_no_case("except"), |_| CompoundSelectOperator::Except),
    ))(i)
}

fn other_selects(
    dialect: Dialect,
) -> impl Fn(&[u8]) -> IResult<&[u8], (Option<CompoundSelectOperator>, SelectStatement)> {
    move |i| {
        let (remaining_input, (_, op, _, select)) = tuple((
            multispace0,
            compound_op,
            multispace1,
            opt_delimited(
                tag("("),
                delimited(multispace0, nested_selection(dialect), multispace0),
                tag(")"),
            ),
        ))(i)?;

        Ok((remaining_input, (Some(op), select)))
    }
}

// Parse compound selection
pub fn compound_selection(
    dialect: Dialect,
) -> impl Fn(&[u8]) -> IResult<&[u8], CompoundSelectStatement> {
    move |i| {
        let (remaining_input, (first_select, other_selects, _, order, limit, _)) = tuple((
            opt_delimited(tag("("), nested_selection(dialect), tag(")")),
            many1(other_selects(dialect)),
            multispace0,
            opt(order_clause(dialect)),
            opt(limit_clause(dialect)),
            statement_terminator,
        ))(i)?;

        let mut selects = vec![(None, first_select)];
        selects.extend(other_selects);

        Ok((
            remaining_input,
            CompoundSelectStatement {
                selects,
                order,
                limit,
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column::Column;
    use crate::common::{FieldDefinitionExpression, Literal};
    use crate::table::Table;
    use crate::Expression;

    #[test]
    fn union() {
        let qstr = "SELECT id, 1 FROM Vote UNION SELECT id, stars from Rating;";
        let qstr2 = "(SELECT id, 1 FROM Vote) UNION (SELECT id, stars from Rating);";
        let res = compound_selection(Dialect::MySQL)(qstr.as_bytes());
        let res2 = compound_selection(Dialect::MySQL)(qstr2.as_bytes());

        let first_select = SelectStatement {
            tables: vec![Table::from("Vote")],
            fields: vec![
                FieldDefinitionExpression::from(Column::from("id")),
                FieldDefinitionExpression::from(Expression::Literal(Literal::Integer(1))),
            ],
            ..Default::default()
        };
        let second_select = SelectStatement {
            tables: vec![Table::from("Rating")],
            fields: vec![
                FieldDefinitionExpression::from(Column::from("id")),
                FieldDefinitionExpression::from(Column::from("stars")),
            ],
            ..Default::default()
        };
        let expected = CompoundSelectStatement {
            selects: vec![
                (None, first_select),
                (Some(CompoundSelectOperator::DistinctUnion), second_select),
            ],
            order: None,
            limit: None,
        };

        assert_eq!(res.unwrap().1, expected);
        assert_eq!(res2.unwrap().1, expected);
    }

    #[test]
    fn union_strict() {
        let qstr = "SELECT id, 1 FROM Vote);";
        let qstr2 = "(SELECT id, 1 FROM Vote;";
        let qstr3 = "SELECT id, 1 FROM Vote) UNION (SELECT id, stars from Rating;";
        let res = compound_selection(Dialect::MySQL)(qstr.as_bytes());
        let res2 = compound_selection(Dialect::MySQL)(qstr2.as_bytes());
        let res3 = compound_selection(Dialect::MySQL)(qstr3.as_bytes());

        assert!(&res.is_err());
        assert_eq!(
            res.unwrap_err(),
            nom::Err::Error((");".as_bytes(), nom::error::ErrorKind::Tag))
        );
        assert!(&res2.is_err());
        assert_eq!(
            res2.unwrap_err(),
            nom::Err::Error((";".as_bytes(), nom::error::ErrorKind::Tag))
        );
        assert!(&res3.is_err());
        assert_eq!(
            res3.unwrap_err(),
            nom::Err::Error((
                ") UNION (SELECT id, stars from Rating;".as_bytes(),
                nom::error::ErrorKind::Tag
            ))
        );
    }

    #[test]
    fn multi_union() {
        let qstr = "SELECT id, 1 FROM Vote \
                    UNION SELECT id, stars from Rating \
                    UNION DISTINCT SELECT 42, 5 FROM Vote;";
        let res = compound_selection(Dialect::MySQL)(qstr.as_bytes());

        let first_select = SelectStatement {
            tables: vec![Table::from("Vote")],
            fields: vec![
                FieldDefinitionExpression::from(Column::from("id")),
                FieldDefinitionExpression::from(Expression::Literal(Literal::Integer(1))),
            ],
            ..Default::default()
        };
        let second_select = SelectStatement {
            tables: vec![Table::from("Rating")],
            fields: vec![
                FieldDefinitionExpression::from(Column::from("id")),
                FieldDefinitionExpression::from(Column::from("stars")),
            ],
            ..Default::default()
        };
        let third_select = SelectStatement {
            tables: vec![Table::from("Vote")],
            fields: vec![
                FieldDefinitionExpression::from(Expression::Literal(Literal::Integer(42))),
                FieldDefinitionExpression::from(Expression::Literal(Literal::Integer(5))),
            ],
            ..Default::default()
        };

        let expected = CompoundSelectStatement {
            selects: vec![
                (None, first_select),
                (Some(CompoundSelectOperator::DistinctUnion), second_select),
                (Some(CompoundSelectOperator::DistinctUnion), third_select),
            ],
            order: None,
            limit: None,
        };

        assert_eq!(res.unwrap().1, expected);
    }

    #[test]
    fn union_all() {
        let qstr = "SELECT id, 1 FROM Vote UNION ALL SELECT id, stars from Rating;";
        let res = compound_selection(Dialect::MySQL)(qstr.as_bytes());

        let first_select = SelectStatement {
            tables: vec![Table::from("Vote")],
            fields: vec![
                FieldDefinitionExpression::from(Column::from("id")),
                FieldDefinitionExpression::from(Expression::Literal(Literal::Integer(1))),
            ],
            ..Default::default()
        };
        let second_select = SelectStatement {
            tables: vec![Table::from("Rating")],
            fields: vec![
                FieldDefinitionExpression::from(Column::from("id")),
                FieldDefinitionExpression::from(Column::from("stars")),
            ],
            ..Default::default()
        };
        let expected = CompoundSelectStatement {
            selects: vec![
                (None, first_select),
                (Some(CompoundSelectOperator::Union), second_select),
            ],
            order: None,
            limit: None,
        };

        assert_eq!(res.unwrap().1, expected);
    }
}
