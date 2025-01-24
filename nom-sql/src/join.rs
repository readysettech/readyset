use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::combinator::map;
use nom_locate::LocatedSpan;
use readyset_sql::ast::*;

use crate::NomSqlResult;

// Parse binary comparison operators
pub fn join_operator(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], JoinOperator> {
    alt((
        map(tag_no_case("join"), |_| JoinOperator::Join),
        map(tag_no_case("left join"), |_| JoinOperator::LeftJoin),
        map(tag_no_case("left outer join"), |_| {
            JoinOperator::LeftOuterJoin
        }),
        map(tag_no_case("right join"), |_| JoinOperator::RightJoin),
        map(tag_no_case("inner join"), |_| JoinOperator::InnerJoin),
        map(tag_no_case("cross join"), |_| JoinOperator::CrossJoin),
        map(tag_no_case("straight_join"), |_| JoinOperator::StraightJoin),
    ))(i)
}

#[cfg(test)]
mod tests {
    use readyset_sql::{Dialect, DialectDisplay};

    use super::*;
    use crate::select::selection;

    mod mysql {

        use super::*;

        #[test]
        fn inner_join() {
            let qstring = "SELECT tags.* FROM tags \
                        INNER JOIN taggings ON (tags.id = taggings.tag_id)";
            let expected = "SELECT `tags`.* FROM `tags` \
                        INNER JOIN `taggings` ON (`tags`.`id` = `taggings`.`tag_id`)";

            let res = selection(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));

            let join_cond = Expr::BinaryOp {
                lhs: Box::new(Expr::Column(Column::from("tags.id"))),
                op: BinaryOperator::Equal,
                rhs: Box::new(Expr::Column(Column::from("taggings.tag_id"))),
            };
            let expected_stmt = SelectStatement {
                tables: vec![TableExpr::from(Relation::from("tags"))],
                fields: vec![FieldDefinitionExpr::AllInTable("tags".into())],
                join: vec![JoinClause {
                    operator: JoinOperator::InnerJoin,
                    right: JoinRightSide::Table(TableExpr::from(Relation::from("taggings"))),
                    constraint: JoinConstraint::On(join_cond),
                }],
                ..Default::default()
            };

            let q = res.unwrap().1;
            assert_eq!(q, expected_stmt);
            assert_eq!(expected, q.display(Dialect::MySQL).to_string());
        }
    }

    mod postgres {
        use super::*;

        #[test]
        fn inner_join() {
            let qstring = "SELECT tags.* FROM tags \
                        INNER JOIN taggings ON (tags.id = taggings.tag_id)";
            let expected = "SELECT \"tags\".* FROM \"tags\" \
                        INNER JOIN \"taggings\" ON (\"tags\".\"id\" = \"taggings\".\"tag_id\")";

            let res = selection(Dialect::PostgreSQL)(LocatedSpan::new(qstring.as_bytes()));

            let join_cond = Expr::BinaryOp {
                lhs: Box::new(Expr::Column(Column::from("tags.id"))),
                op: BinaryOperator::Equal,
                rhs: Box::new(Expr::Column(Column::from("taggings.tag_id"))),
            };
            let expected_stmt = SelectStatement {
                tables: vec![TableExpr::from(Relation::from("tags"))],
                fields: vec![FieldDefinitionExpr::AllInTable("tags".into())],
                join: vec![JoinClause {
                    operator: JoinOperator::InnerJoin,
                    right: JoinRightSide::Table(TableExpr::from(Relation::from("taggings"))),
                    constraint: JoinConstraint::On(join_cond),
                }],
                ..Default::default()
            };

            let q = res.unwrap().1;
            assert_eq!(q, expected_stmt);
            assert_eq!(expected, q.display(Dialect::PostgreSQL).to_string());
        }
    }
}
