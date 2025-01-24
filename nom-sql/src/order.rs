use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::combinator::{map, opt, value};
use nom::multi::separated_list1;
use nom::sequence::preceded;
use nom_locate::LocatedSpan;
use readyset_sql::{ast::*, Dialect};

use crate::common::{field_reference, ws_sep_comma};
use crate::whitespace::{whitespace0, whitespace1};
use crate::NomSqlResult;

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
    use readyset_sql::DialectDisplay;

    use super::*;
    use crate::select::selection;

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
