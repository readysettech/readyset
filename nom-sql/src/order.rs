use nom::character::complete::{multispace0, multispace1};
use std::fmt;
use std::str;

use column::Column;
use common::{column_identifier_no_alias, ws_sep_comma};
use keywords::escape_if_keyword;
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::combinator::{map, opt};
use nom::multi::many0;
use nom::sequence::{preceded, tuple};
use nom::IResult;

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
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
    pub columns: Vec<(Column, OrderType)>, // TODO(malte): can this be an arbitrary expr?
}

impl fmt::Display for OrderClause {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ORDER BY ")?;
        write!(
            f,
            "{}",
            self.columns
                .iter()
                .map(|&(ref c, ref o)| format!("{} {}", escape_if_keyword(&c.name), o))
                .collect::<Vec<_>>()
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

fn order_expr(i: &[u8]) -> IResult<&[u8], (Column, OrderType)> {
    let (remaining_input, (field_name, ordering, _)) = tuple((
        column_identifier_no_alias,
        opt(preceded(multispace0, order_type)),
        opt(ws_sep_comma),
    ))(i)?;

    Ok((
        remaining_input,
        (field_name, ordering.unwrap_or(OrderType::OrderAscending)),
    ))
}

// Parse ORDER BY clause
pub fn order_clause(i: &[u8]) -> IResult<&[u8], OrderClause> {
    let (remaining_input, (_, _, _, columns)) = tuple((
        multispace0,
        tag_no_case("order by"),
        multispace1,
        many0(order_expr),
    ))(i)?;

    Ok((remaining_input, OrderClause { columns }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use select::selection;

    #[test]
    fn order_clause() {
        let qstring1 = "select * from users order by name desc\n";
        let qstring2 = "select * from users order by name asc, age desc\n";
        let qstring3 = "select * from users order by name\n";

        let expected_ord1 = OrderClause {
            columns: vec![("name".into(), OrderType::OrderDescending)],
        };
        let expected_ord2 = OrderClause {
            columns: vec![
                ("name".into(), OrderType::OrderAscending),
                ("age".into(), OrderType::OrderDescending),
            ],
        };
        let expected_ord3 = OrderClause {
            columns: vec![("name".into(), OrderType::OrderAscending)],
        };

        let res1 = selection(qstring1.as_bytes());
        let res2 = selection(qstring2.as_bytes());
        let res3 = selection(qstring3.as_bytes());
        assert_eq!(res1.unwrap().1.order, Some(expected_ord1));
        assert_eq!(res2.unwrap().1.order, Some(expected_ord2));
        assert_eq!(res3.unwrap().1.order, Some(expected_ord3));
    }
}
