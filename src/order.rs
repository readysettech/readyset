use nom::multispace;
use nom::types::CompleteByteSlice;
use std::fmt;
use std::str;

use column::Column;
use common::{column_identifier_no_alias, opt_multispace};
use keywords::escape_if_keyword;

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

named!(pub order_type<CompleteByteSlice, OrderType>,
    alt!(
          map!(tag_no_case!("desc"), |_| OrderType::OrderDescending)
        | map!(tag_no_case!("asc"), |_| OrderType::OrderAscending)
    )
);

// Parse ORDER BY clause
named!(pub order_clause<CompleteByteSlice, OrderClause>,
    do_parse!(
        opt_multispace >>
        tag_no_case!("order by") >>
        multispace >>
        order_expr: many0!(
            do_parse!(
                fieldname: column_identifier_no_alias >>
                ordering: opt!(
                    do_parse!(
                        opt_multispace >>
                        ordering: order_type >>
                        (ordering)
                    )
                ) >>
                opt!(
                    do_parse!(
                        opt_multispace >>
                        tag!(",") >>
                        opt_multispace >>
                        ()
                    )
                ) >>
                (fieldname, ordering.unwrap_or(OrderType::OrderAscending))
            )
        ) >>
        (OrderClause {
            columns: order_expr,
            // order: match ordering {
            //     None => OrderType::OrderAscending,
            //     Some(ref o) => o.clone(),
            // },
        })
    )
);

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

        let res1 = selection(CompleteByteSlice(qstring1.as_bytes()));
        let res2 = selection(CompleteByteSlice(qstring2.as_bytes()));
        let res3 = selection(CompleteByteSlice(qstring3.as_bytes()));
        assert_eq!(res1.unwrap().1.order, Some(expected_ord1));
        assert_eq!(res2.unwrap().1.order, Some(expected_ord2));
        assert_eq!(res3.unwrap().1.order, Some(expected_ord3));
    }
}
