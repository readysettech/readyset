use std::cmp::Ordering;
use std::fmt::Display;
use std::{fmt, str};

use itertools::Itertools;
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::combinator::{map, opt, value};
use nom::multi::separated_list1;
use nom::sequence::preceded;
use nom_locate::LocatedSpan;
use proptest::collection::size_range;
use readyset_util::fmt::fmt_with;
use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

use crate::common::{field_reference, ws_sep_comma};
use crate::whitespace::{whitespace0, whitespace1};
use crate::{Dialect, DialectDisplay, FieldReference, NomSqlResult};

#[derive(
    Clone, Copy, Debug, Eq, Hash, PartialEq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub enum OrderType {
    OrderAscending,
    OrderDescending,
}

impl OrderType {
    /// Reverse the provided [`Ordering`] if this [`OrderType`] if of type
    /// [`OrderType::OrderDescending`], otherwise do nothing
    #[inline(always)]
    pub fn apply(&self, ord: Ordering) -> Ordering {
        match self {
            OrderType::OrderAscending => ord,
            OrderType::OrderDescending => ord.reverse(),
        }
    }
}

impl Display for OrderType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            OrderType::OrderAscending => write!(f, "ASC"),
            OrderType::OrderDescending => write!(f, "DESC"),
        }
    }
}

#[derive(
    Clone, Copy, Debug, Eq, Hash, PartialEq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub enum NullOrder {
    NullsFirst,
    NullsLast,
}

impl NullOrder {
    /// Returns `true` if this is the default null order for the given order type.
    ///
    /// From [the postgres docs][pg-docs]:
    ///
    /// > By default, null values sort as if larger than any non-null value; that is, `NULLS FIRST`
    /// > is the default for `DESC` order, and `NULLS LAST` otherwise.
    ///
    /// [pg-docs]: https://www.postgresql.org/docs/current/queries-order.html
    pub fn is_default_for(self, ot: OrderType) -> bool {
        self == match ot {
            OrderType::OrderDescending => Self::NullsFirst,
            OrderType::OrderAscending => Self::NullsLast,
        }
    }
}

impl Display for NullOrder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NullOrder::NullsFirst => write!(f, "NULLS FIRST"),
            NullOrder::NullsLast => write!(f, "NULLS LAST"),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct OrderBy {
    pub field: FieldReference,
    pub order_type: Option<OrderType>,
    pub null_order: Option<NullOrder>,
}

impl OrderBy {
    pub fn display(&self, dialect: Dialect) -> impl Display + Copy + '_ {
        fmt_with(move |f| {
            write!(f, "{}", self.field.display(dialect))?;
            if let Some(ot) = self.order_type {
                write!(f, " {}", ot)?;
            }

            Ok(())
        })
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub struct OrderClause {
    pub order_by: Vec<OrderBy>,
}

impl DialectDisplay for OrderClause {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + Copy + '_ {
        fmt_with(move |f| {
            write!(
                f,
                "ORDER BY {}",
                self.order_by
                    .iter()
                    .map(|ob| ob.display(dialect))
                    .join(", ")
            )
        })
    }
}

pub fn order_type(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], OrderType> {
    alt((
        map(tag_no_case("desc"), |_| OrderType::OrderDescending),
        map(tag_no_case("asc"), |_| OrderType::OrderAscending),
    ))(i)
}

fn null_order(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], NullOrder> {
    let (i, _) = tag_no_case("nulls")(i)?;
    let (i, _) = whitespace1(i)?;
    alt((
        value(NullOrder::NullsFirst, tag_no_case("first")),
        value(NullOrder::NullsLast, tag_no_case("last")),
    ))(i)
}

fn order_by(dialect: Dialect) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], OrderBy> {
    move |i| {
        let (i, field) = field_reference(dialect)(i)?;
        let (i, order_type) = opt(preceded(whitespace1, order_type))(i)?;
        let (i, null_order) = opt(preceded(whitespace1, null_order))(i)?;
        Ok((
            i,
            OrderBy {
                field,
                order_type,
                null_order,
            },
        ))
    }
}

// Parse ORDER BY clause
pub fn order_clause(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], OrderClause> {
    move |i| {
        let (i, _) = whitespace0(i)?;
        let (i, _) = tag_no_case("order")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, _) = tag_no_case("by")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, order_by) = separated_list1(ws_sep_comma, order_by(dialect))(i)?;

        Ok((i, OrderClause { order_by }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::select::selection;
    use crate::Expr;

    #[test]
    fn order_clause() {
        let qstring1 = "select * from users order by name desc\n";
        let qstring2 = "select * from users order by name asc, age desc\n";
        let qstring3 = "select * from users order by name\n";

        let expected_ord1 = OrderClause {
            order_by: vec![OrderBy {
                field: FieldReference::Expr(Expr::Column("name".into())),
                order_type: Some(OrderType::OrderDescending),
                null_order: None,
            }],
        };
        let expected_ord2 = OrderClause {
            order_by: vec![
                OrderBy {
                    field: FieldReference::Expr(Expr::Column("name".into())),
                    order_type: Some(OrderType::OrderAscending),
                    null_order: None,
                },
                OrderBy {
                    field: FieldReference::Expr(Expr::Column("age".into())),
                    order_type: Some(OrderType::OrderDescending),
                    null_order: None,
                },
            ],
        };
        let expected_ord3 = OrderClause {
            order_by: vec![OrderBy {
                field: FieldReference::Expr(Expr::Column("name".into())),
                order_type: None,
                null_order: None,
            }],
        };

        let res1 = selection(Dialect::MySQL)(LocatedSpan::new(qstring1.as_bytes()));
        let res2 = selection(Dialect::MySQL)(LocatedSpan::new(qstring2.as_bytes()));
        let res3 = selection(Dialect::MySQL)(LocatedSpan::new(qstring3.as_bytes()));
        assert_eq!(res1.unwrap().1.order, Some(expected_ord1));
        assert_eq!(res2.unwrap().1.order, Some(expected_ord2));
        assert_eq!(res3.unwrap().1.order, Some(expected_ord3));
    }

    #[test]
    fn nulls_first() {
        let res = test_parse!(
            super::order_clause(Dialect::PostgreSQL),
            b"ORDER BY t1.x ASC NULLS FIRST"
        );

        assert_eq!(
            res.order_by,
            vec![OrderBy {
                field: FieldReference::Expr(Expr::Column("t1.x".into())),
                order_type: Some(OrderType::OrderAscending),
                null_order: Some(NullOrder::NullsFirst)
            }]
        )
    }

    mod mysql {
        use super::*;

        #[test]
        fn order_prints_column_table() {
            let clause = OrderClause {
                order_by: vec![OrderBy {
                    field: FieldReference::Expr(Expr::Column("t.n".into())),
                    order_type: Some(OrderType::OrderDescending),
                    null_order: None,
                }],
            };
            assert_eq!(
                clause.display(Dialect::MySQL).to_string(),
                "ORDER BY `t`.`n` DESC"
            );
        }
    }

    mod postgres {
        use super::*;

        test_format_parse_round_trip! {
            rt_order_clause(order_clause, OrderClause, Dialect::PostgreSQL);
        }

        #[test]
        fn order_prints_column_table() {
            let clause = OrderClause {
                order_by: vec![OrderBy {
                    field: FieldReference::Expr(Expr::Column("t.n".into())),
                    order_type: Some(OrderType::OrderDescending),
                    null_order: None,
                }],
            };
            assert_eq!(
                clause.display(Dialect::PostgreSQL).to_string(),
                "ORDER BY \"t\".\"n\" DESC"
            );
        }
    }
}
