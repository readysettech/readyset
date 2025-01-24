use nom::bytes::complete::tag_no_case;
use nom::combinator::opt;
use nom::sequence::{delimited, tuple};
use nom_locate::LocatedSpan;
use readyset_sql::{ast::*, Dialect};

use crate::common::statement_terminator;
use crate::select::where_clause;
use crate::table::relation;
use crate::whitespace::whitespace1;
use crate::NomSqlResult;

pub fn deletion(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], DeleteStatement> {
    move |i| {
        let (remaining_input, (_, _, table, where_clause, _)) = tuple((
            tag_no_case("delete"),
            delimited(whitespace1, tag_no_case("from"), whitespace1),
            relation(dialect),
            opt(where_clause(dialect)),
            statement_terminator,
        ))(i)?;

        Ok((
            remaining_input,
            DeleteStatement {
                table,
                where_clause,
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use readyset_sql::DialectDisplay;

    use super::*;

    #[test]
    fn simple_delete() {
        let qstring = "DELETE FROM users;";
        let res = deletion(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
        assert_eq!(
            res.unwrap().1,
            DeleteStatement {
                table: Relation::from("users"),
                where_clause: None,
            }
        );
    }

    #[test]
    fn simple_delete_schema() {
        let qstring = "DELETE FROM db1.users;";
        let res = deletion(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
        assert_eq!(
            res.unwrap().1,
            DeleteStatement {
                table: Relation {
                    schema: Some("db1".into()),
                    name: "users".into(),
                },
                where_clause: None,
            }
        );
    }

    #[test]
    fn delete_with_where_clause() {
        let qstring = "DELETE FROM users WHERE id = 1;";
        let res = deletion(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
        let expected_left = Expr::Column(Column::from("id"));
        let expected_where_cond = Some(Expr::BinaryOp {
            lhs: Box::new(expected_left),
            rhs: Box::new(Expr::Literal(Literal::Integer(1))),
            op: BinaryOperator::Equal,
        });
        assert_eq!(
            res.unwrap().1,
            DeleteStatement {
                table: Relation::from("users"),
                where_clause: expected_where_cond,
            }
        );
    }

    mod mysql {
        use super::*;

        #[test]
        fn format_delete() {
            let qstring = "DELETE FROM users WHERE id = 1";
            let expected = "DELETE FROM `users` WHERE (`id` = 1)";
            let res = deletion(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
            assert_eq!(res.unwrap().1.display(Dialect::MySQL).to_string(), expected);
        }
    }

    mod postgres {
        use super::*;

        #[test]
        fn format_delete() {
            let qstring = "DELETE FROM users WHERE id = 1";
            let expected = "DELETE FROM \"users\" WHERE (\"id\" = 1)";
            let res = deletion(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));
            assert_eq!(
                res.unwrap().1.display(Dialect::PostgreSQL).to_string(),
                expected
            );
        }
    }
}
