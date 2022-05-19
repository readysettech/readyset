use std::{fmt, str};

use itertools::Itertools;
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::combinator::{map, opt};
use nom::multi::separated_list1;
use nom::sequence::preceded;
use nom::IResult;
use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

use crate::common::{field_reference, ws_sep_comma};
use crate::whitespace::{whitespace0, whitespace1};
use crate::{Dialect, FieldReference};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub enum OrderType {
    OrderAscending,
    OrderDescending,
}

impl fmt::Display for OrderType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            OrderType::OrderAscending => write!(f, "ASC"),
            OrderType::OrderDescending => write!(f, "DESC"),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct OrderClause {
    pub order_by: Vec<(FieldReference, Option<OrderType>)>,
}

impl fmt::Display for OrderClause {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ORDER BY ")?;
        write!(
            f,
            "{}",
            self.order_by
                .iter()
                .map(|&(ref c, ref o)| format!(
                    "{}{}",
                    c,
                    if let Some(ot) = o {
                        format!(" {}", ot)
                    } else {
                        "".to_owned()
                    }
                ))
                .join(", ")
        )
    }
}

pub fn order_type(i: &[u8]) -> IResult<&[u8], OrderType> {
    alt((
        map(tag_no_case("desc"), |_| OrderType::OrderDescending),
        map(tag_no_case("asc"), |_| OrderType::OrderAscending),
    ))(i)
}

fn order_field(
    dialect: Dialect,
) -> impl Fn(&[u8]) -> IResult<&[u8], (FieldReference, Option<OrderType>)> {
    move |i| {
        let (i, field) = field_reference(dialect)(i)?;
        let (i, ord_typ) = opt(preceded(whitespace1, order_type))(i)?;
        Ok((i, (field, ord_typ)))
    }
}

// Parse ORDER BY clause
pub fn order_clause(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], OrderClause> {
    move |i| {
        let (i, _) = whitespace0(i)?;
        let (i, _) = tag_no_case("order")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, _) = tag_no_case("by")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, order_by) = separated_list1(ws_sep_comma, order_field(dialect))(i)?;

        Ok((i, OrderClause { order_by }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::select::selection;
    use crate::Expression;

    #[test]
    fn order_clause() {
        let qstring1 = "select * from users order by name desc\n";
        let qstring2 = "select * from users order by name asc, age desc\n";
        let qstring3 = "select * from users order by name\n";

        let expected_ord1 = OrderClause {
            order_by: vec![(
                FieldReference::Expression(Expression::Column("name".into())),
                Some(OrderType::OrderDescending),
            )],
        };
        let expected_ord2 = OrderClause {
            order_by: vec![
                (
                    FieldReference::Expression(Expression::Column("name".into())),
                    Some(OrderType::OrderAscending),
                ),
                (
                    FieldReference::Expression(Expression::Column("age".into())),
                    Some(OrderType::OrderDescending),
                ),
            ],
        };
        let expected_ord3 = OrderClause {
            order_by: vec![(
                FieldReference::Expression(Expression::Column("name".into())),
                None,
            )],
        };

        let res1 = selection(Dialect::MySQL)(qstring1.as_bytes());
        let res2 = selection(Dialect::MySQL)(qstring2.as_bytes());
        let res3 = selection(Dialect::MySQL)(qstring3.as_bytes());
        assert_eq!(res1.unwrap().1.order, Some(expected_ord1));
        assert_eq!(res2.unwrap().1.order, Some(expected_ord2));
        assert_eq!(res3.unwrap().1.order, Some(expected_ord3));
    }

    #[test]
    fn order_prints_column_table() {
        let clause = OrderClause {
            order_by: vec![(
                FieldReference::Expression(Expression::Column("t.n".into())),
                Some(OrderType::OrderDescending),
            )],
        };
        assert_eq!(clause.to_string(), "ORDER BY `t`.`n` DESC");
    }
}
