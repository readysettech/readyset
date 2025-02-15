use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case};
use nom::combinator::{map, opt};
use nom::multi::many0;
use nom::sequence::{delimited, preceded, tuple};
use nom_locate::LocatedSpan;
use readyset_sql::{ast::*, Dialect};

use crate::common::{opt_delimited, terminated_with_statement_terminator};
use crate::order::order_clause;
use crate::select::{limit_offset_clause, nested_selection};
use crate::whitespace::{whitespace0, whitespace1};
use crate::NomSqlResult;

// Parse compound operator
fn compound_op(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], CompoundSelectOperator> {
    alt((
        map(
            preceded(
                tag_no_case("union"),
                opt(preceded(
                    whitespace1,
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

#[allow(clippy::type_complexity)]
fn other_selects(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], (Option<CompoundSelectOperator>, SelectStatement)>
{
    move |i| {
        let (remaining_input, (_, op, _, select)) = tuple((
            whitespace0,
            compound_op,
            whitespace1,
            opt_delimited(
                tag("("),
                delimited(whitespace0, nested_selection(dialect), whitespace0),
                tag(")"),
            ),
        ))(i)?;

        Ok((remaining_input, (Some(op), select)))
    }
}

pub fn simple_or_compound_selection(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], SelectSpecification> {
    move |i| terminated_with_statement_terminator(nested_compound_selection(dialect))(i)
}

// Parse compound selection
pub fn nested_compound_selection(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], SelectSpecification> {
    move |i| {
        let (remaining_input, (first_select, other_selects, _, order, limit_clause)) =
            tuple((
                opt_delimited(tag("("), nested_selection(dialect), tag(")")),
                many0(other_selects(dialect)),
                whitespace0,
                opt(order_clause(dialect)),
                opt(limit_offset_clause(dialect)),
            ))(i)?;

        if other_selects.is_empty() {
            return Ok((remaining_input, SelectSpecification::Simple(first_select)));
        }

        let mut selects = vec![(None, first_select)];
        selects.extend(other_selects);

        Ok((
            remaining_input,
            SelectSpecification::Compound(CompoundSelectStatement {
                selects,
                order,
                limit_clause: limit_clause.unwrap_or_default(),
            }),
        ))
    }
}

#[cfg(test)]
mod tests {
    use nom::error::Error;

    use super::*;
    use crate::to_nom_result;

    #[test]
    fn union() {
        let qstr = "SELECT id, 1 FROM Vote UNION SELECT id, stars from Rating;";
        let qstr2 = "(SELECT id, 1 FROM Vote) UNION (SELECT id, stars from Rating);";
        let res = nested_compound_selection(Dialect::MySQL)(LocatedSpan::new(qstr.as_bytes()));
        let res2 = nested_compound_selection(Dialect::MySQL)(LocatedSpan::new(qstr2.as_bytes()));

        let first_select = SelectStatement {
            tables: vec![TableExpr::from(Relation::from("Vote"))],
            fields: vec![
                FieldDefinitionExpr::from(Column::from("id")),
                FieldDefinitionExpr::from(Expr::Literal(Literal::Integer(1))),
            ],
            ..Default::default()
        };
        let second_select = SelectStatement {
            tables: vec![TableExpr::from(Relation::from("Rating"))],
            fields: vec![
                FieldDefinitionExpr::from(Column::from("id")),
                FieldDefinitionExpr::from(Column::from("stars")),
            ],
            ..Default::default()
        };
        let expected = CompoundSelectStatement {
            selects: vec![
                (None, first_select),
                (Some(CompoundSelectOperator::DistinctUnion), second_select),
            ],
            order: None,
            limit_clause: LimitClause::LimitOffset {
                limit: None,
                offset: None,
            },
        };

        assert_eq!(
            res.unwrap().1,
            SelectSpecification::Compound(expected.clone())
        );
        assert_eq!(res2.unwrap().1, SelectSpecification::Compound(expected));
    }

    #[test]
    fn union_strict() {
        let qstr = "SELECT id, 1 FROM Vote);";
        let qstr2 = "(SELECT id, 1 FROM Vote;";
        let qstr3 = "SELECT id, 1 FROM Vote) UNION (SELECT id, stars from Rating;";
        let res = to_nom_result(simple_or_compound_selection(Dialect::MySQL)(
            LocatedSpan::new(qstr.as_bytes()),
        ));
        let res2 = to_nom_result(simple_or_compound_selection(Dialect::MySQL)(
            LocatedSpan::new(qstr2.as_bytes()),
        ));
        let res3 = to_nom_result(simple_or_compound_selection(Dialect::MySQL)(
            LocatedSpan::new(qstr3.as_bytes()),
        ));

        assert!(&res.is_err());
        assert_eq!(
            res.unwrap_err(),
            nom::Err::Error(Error {
                input: ");".as_bytes(),
                code: nom::error::ErrorKind::Eof
            })
        );
        assert!(&res2.is_err());
        assert_eq!(
            res2.unwrap_err(),
            nom::Err::Error(Error {
                input: ";".as_bytes(),
                code: nom::error::ErrorKind::Tag
            })
        );
        assert!(&res3.is_err());
        assert_eq!(
            res3.unwrap_err(),
            nom::Err::Error(Error {
                input: ") UNION (SELECT id, stars from Rating;".as_bytes(),
                code: nom::error::ErrorKind::Eof
            })
        );
    }

    #[test]
    fn multi_union() {
        let qstr = "SELECT id, 1 FROM Vote \
                    UNION SELECT id, stars from Rating \
                    UNION DISTINCT SELECT 42, 5 FROM Vote;";
        let res = nested_compound_selection(Dialect::MySQL)(LocatedSpan::new(qstr.as_bytes()));

        let first_select = SelectStatement {
            tables: vec![TableExpr::from(Relation::from("Vote"))],
            fields: vec![
                FieldDefinitionExpr::from(Column::from("id")),
                FieldDefinitionExpr::from(Expr::Literal(Literal::Integer(1))),
            ],
            ..Default::default()
        };
        let second_select = SelectStatement {
            tables: vec![TableExpr::from(Relation::from("Rating"))],
            fields: vec![
                FieldDefinitionExpr::from(Column::from("id")),
                FieldDefinitionExpr::from(Column::from("stars")),
            ],
            ..Default::default()
        };
        let third_select = SelectStatement {
            tables: vec![TableExpr::from(Relation::from("Vote"))],
            fields: vec![
                FieldDefinitionExpr::from(Expr::Literal(Literal::Integer(42))),
                FieldDefinitionExpr::from(Expr::Literal(Literal::Integer(5))),
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
            limit_clause: LimitClause::default(),
        };

        assert_eq!(res.unwrap().1, SelectSpecification::Compound(expected));
    }

    #[test]
    fn union_all() {
        let qstr = "SELECT id, 1 FROM Vote UNION ALL SELECT id, stars from Rating;";
        let res = nested_compound_selection(Dialect::MySQL)(LocatedSpan::new(qstr.as_bytes()));

        let first_select = SelectStatement {
            tables: vec![TableExpr::from(Relation::from("Vote"))],
            fields: vec![
                FieldDefinitionExpr::from(Column::from("id")),
                FieldDefinitionExpr::from(Expr::Literal(Literal::Integer(1))),
            ],
            ..Default::default()
        };
        let second_select = SelectStatement {
            tables: vec![TableExpr::from(Relation::from("Rating"))],
            fields: vec![
                FieldDefinitionExpr::from(Column::from("id")),
                FieldDefinitionExpr::from(Column::from("stars")),
            ],
            ..Default::default()
        };
        let expected = CompoundSelectStatement {
            selects: vec![
                (None, first_select),
                (Some(CompoundSelectOperator::Union), second_select),
            ],
            order: None,
            limit_clause: LimitClause::default(),
        };

        assert_eq!(res.unwrap().1, SelectSpecification::Compound(expected));
    }

    #[test]
    #[ignore]
    fn union_flarum_1() {
        let qstring = b"(select `discussions`.* from `discussions` where (`discussions`.`id` not in (select `discussion_id` from `discussion_tag` where `tag_id` not in (select `tags`.`id` from `tags` where (`tags`.`id` in (select `perm_tags`.`id` from `tags` as `perm_tags` where (`perm_tags`.`is_restricted` = ? and 0 = 1) or `perm_tags`.`is_restricted` = ?) and (`tags`.`parent_id` in (select `perm_tags`.`id` from `tags` as `perm_tags` where (`perm_tags`.`is_restricted` = ? and 0 = 1) or `perm_tags`.`is_restricted` = ?) or `tags`.`parent_id` is null))))) and (`discussions`.`is_private` = ? or (((`discussions`.`is_approved` = ? and (`discussions`.`user_id` = ? or ((`discussions`.`id` not in (select `discussion_id` from `discussion_tag` where `tag_id` not in (select `tags`.`id` from `tags` where (`tags`.`id` in (select `perm_tags`.`id` from `tags` as `perm_tags` where (`perm_tags`.`is_restricted` = ? and 0 = 1)) and (`tags`.`parent_id` in (select `perm_tags`.`id` from `tags` as `perm_tags` where (`perm_tags`.`is_restricted` = ? and 0 = 1)) or `tags`.`parent_id` is null))))) and exists (select * from `tags` inner join `discussion_tag` on `tags`.`id` = `discussion_tag`.`tag_id` where `discussions`.`id` = `discussion_tag`.`discussion_id`))))))) and (`discussions`.`hidden_at` is null or `discussions`.`user_id` = ? or ((`discussions`.`id` not in (select `discussion_id` from `discussion_tag` where `tag_id` not in (select `tags`.`id` from `tags` where (`tags`.`id` in (select `perm_tags`.`id` from `tags` as `perm_tags` where (`perm_tags`.`is_restricted` = ? and 0 = 1)) and (`tags`.`parent_id` in (select `perm_tags`.`id` from `tags` as `perm_tags` where (`perm_tags`.`is_restricted` = ? and 0 = 1)) or `tags`.`parent_id` is null))))) and exists (select * from `tags` inner join `discussion_tag` on `tags`.`id` = `discussion_tag`.`tag_id` where `discussions`.`id` = `discussion_tag`.`discussion_id`))) and (`discussions`.`comment_count` > ? or `discussions`.`user_id` = ? or ((`discussions`.`id` not in (select `discussion_id` from `discussion_tag` where `tag_id` not in (select `tags`.`id` from `tags` where (`tags`.`id` in (select `perm_tags`.`id` from `tags` as `perm_tags` where (`perm_tags`.`is_restricted` = ? and 0 = 1)) and (`tags`.`parent_id` in (select `perm_tags`.`id` from `tags` as `perm_tags` where (`perm_tags`.`is_restricted` = ? and 0 = 1)) or `tags`.`parent_id` is null))))) and exists (select * from `tags` inner join `discussion_tag` on `tags`.`id` = `discussion_tag`.`tag_id` where `discussions`.`id` = `discussion_tag`.`discussion_id`))) and not exists (select 1 from `discussion_user` where `discussions`.`id` = `discussion_id` and `user_id` = ? and `subscription` = ?) and `discussions`.`id` not in (select `discussion_id` from `discussion_tag` where 0 = 1) order by `last_posted_at` desc limit 21) union (select `discussions`.* from `discussions` where (`discussions`.`id` not in (select `discussion_id` from `discussion_tag` where `tag_id` not in (select `tags`.`id` from `tags` where (`tags`.`id` in (select `perm_tags`.`id` from `tags` as `perm_tags` where (`perm_tags`.`is_restricted` = ? and 0 = 1) or `perm_tags`.`is_restricted` = ?) and (`tags`.`parent_id` in (select `perm_tags`.`id` from `tags` as `perm_tags` where (`perm_tags`.`is_restricted` = ? and 0 = 1) or `perm_tags`.`is_restricted` = ?) or `tags`.`parent_id` is null))))) and (`discussions`.`is_private` = ? or (((`discussions`.`is_approved` = ? and (`discussions`.`user_id` = ? or ((`discussions`.`id` not in (select `discussion_id` from `discussion_tag` where `tag_id` not in (select `tags`.`id` from `tags` where (`tags`.`id` in (select `perm_tags`.`id` from `tags` as `perm_tags` where (`perm_tags`.`is_restricted` = ? and 0 = 1)) and (`tags`.`parent_id` in (select `perm_tags`.`id` from `tags` as `perm_tags` where (`perm_tags`.`is_restricted` = ? and 0 = 1)) or `tags`.`parent_id` is null))))) and exists (select * from `tags` inner join `discussion_tag` on `tags`.`id` = `discussion_tag`.`tag_id` where `discussions`.`id` = `discussion_tag`.`discussion_id`))))))) and (`discussions`.`hidden_at` is null or `discussions`.`user_id` = ? or ((`discussions`.`id` not in (select `discussion_id` from `discussion_tag` where `tag_id` not in (select `tags`.`id` from `tags` where (`tags`.`id` in (select `perm_tags`.`id` from `tags` as `perm_tags` where (`perm_tags`.`is_restricted` = ? and 0 = 1)) and (`tags`.`parent_id` in (select `perm_tags`.`id` from `tags` as `perm_tags` where (`perm_tags`.`is_restricted` = ? and 0 = 1)) or `tags`.`parent_id` is null))))) and exists (select * from `tags` inner join `discussion_tag` on `tags`.`id` = `discussion_tag`.`tag_id` where `discussions`.`id` = `discussion_tag`.`discussion_id`))) and (`discussions`.`comment_count` > ? or `discussions`.`user_id` = ? or ((`discussions`.`id` not in (select `discussion_id` from `discussion_tag` where `tag_id` not in (select `tags`.`id` from `tags` where (`tags`.`id` in (select `perm_tags`.`id` from `tags` as `perm_tags` where (`perm_tags`.`is_restricted` = ? and 0 = 1)) and (`tags`.`parent_id` in (select `perm_tags`.`id` from `tags` as `perm_tags` where (`perm_tags`.`is_restricted` = ? and 0 = 1)) or `tags`.`parent_id` is null))))) and exists (select * from `tags` inner join `discussion_tag` on `tags`.`id` = `discussion_tag`.`tag_id` where `discussions`.`id` = `discussion_tag`.`discussion_id`))) and `is_sticky` = ? limit 21) order by is_sticky and not exists (select 1 from `discussion_user` as `sticky` where `sticky`.`discussion_id` = `id` and `sticky`.`user_id` = ? and `sticky`.`last_read_post_number` >= `last_post_number`) and last_posted_at > ? desc, `last_posted_at` desc limit 21";
        let _res = test_parse!(simple_or_compound_selection(Dialect::MySQL), qstring);
        // TODO:  assert_eq!(res, ...)
        // TODO:  assert_eq!(res.display(Dialect::MySQL).to_string(), ...)
    }

    #[test]
    #[ignore = "nom-sql is incorrect"]
    fn union_order_limit_outer() {
        // This should effectively get parsed like "(SELECT A) UNION (SELECT B) ORDER BY ...", but
        // nom-sql incorrectly interprets it like "(SELECT A) UNION (SELECT B ORDER BY ...)"
        let qstr = "SELECT id FROM foo WHERE col = 'abc' UNION SELECT id FROM foo WHERE othercol >= 3 ORDER BY id ASC LIMIT 1;";
        let res = simple_or_compound_selection(Dialect::MySQL)(LocatedSpan::new(qstr.as_bytes()));

        let first_select = SelectStatement {
            tables: vec![TableExpr::from(Relation::from("foo"))],
            fields: vec![FieldDefinitionExpr::from(Column::from("id"))],
            where_clause: Some(Expr::BinaryOp {
                lhs: Box::new(Expr::Column(Column::from("col"))),
                op: BinaryOperator::Equal,
                rhs: Box::new(Expr::Literal(Literal::String("abc".to_string()))),
            }),
            ..Default::default()
        };
        let second_select = SelectStatement {
            tables: vec![TableExpr::from(Relation::from("foo"))],
            fields: vec![FieldDefinitionExpr::from(Column::from("id"))],
            where_clause: Some(Expr::BinaryOp {
                lhs: Box::new(Expr::Column(Column::from("othercol"))),
                op: BinaryOperator::GreaterOrEqual,
                rhs: Box::new(Expr::Literal(Literal::Integer(3))),
            }),
            ..Default::default()
        };
        let expected = CompoundSelectStatement {
            selects: vec![
                (None, first_select),
                (Some(CompoundSelectOperator::DistinctUnion), second_select),
            ],
            order: Some(OrderClause {
                order_by: vec![OrderBy {
                    field: FieldReference::Expr(Expr::Column(Column::from("id"))),
                    order_type: Some(OrderType::OrderAscending),
                    null_order: None,
                }],
            }),
            limit_clause: LimitClause::LimitOffset {
                limit: Some(LimitValue::Literal(Literal::Integer(1))),
                offset: None,
            },
        };

        assert_eq!(res.unwrap().1, SelectSpecification::Compound(expected));
    }

    #[test]
    fn union_order_limit_outer_parens() {
        let qstr = "SELECT id FROM foo WHERE col = 'abc' UNION (SELECT id FROM foo WHERE othercol >= 3) ORDER BY id ASC LIMIT 1;";
        let res = simple_or_compound_selection(Dialect::MySQL)(LocatedSpan::new(qstr.as_bytes()));

        let first_select = SelectStatement {
            tables: vec![TableExpr::from(Relation::from("foo"))],
            fields: vec![FieldDefinitionExpr::from(Column::from("id"))],
            where_clause: Some(Expr::BinaryOp {
                lhs: Box::new(Expr::Column(Column::from("col"))),
                op: BinaryOperator::Equal,
                rhs: Box::new(Expr::Literal(Literal::String("abc".to_string()))),
            }),
            ..Default::default()
        };
        let second_select = SelectStatement {
            tables: vec![TableExpr::from(Relation::from("foo"))],
            fields: vec![FieldDefinitionExpr::from(Column::from("id"))],
            where_clause: Some(Expr::BinaryOp {
                lhs: Box::new(Expr::Column(Column::from("othercol"))),
                op: BinaryOperator::GreaterOrEqual,
                rhs: Box::new(Expr::Literal(Literal::Integer(3))),
            }),
            ..Default::default()
        };
        let expected = CompoundSelectStatement {
            selects: vec![
                (None, first_select),
                (Some(CompoundSelectOperator::DistinctUnion), second_select),
            ],
            order: Some(OrderClause {
                order_by: vec![OrderBy {
                    field: FieldReference::Expr(Expr::Column(Column::from("id"))),
                    order_type: Some(OrderType::OrderAscending),
                    null_order: None,
                }],
            }),
            limit_clause: LimitClause::LimitOffset {
                limit: Some(LimitValue::Literal(Literal::Integer(1))),
                offset: None,
            },
        };

        assert_eq!(res.unwrap().1, SelectSpecification::Compound(expected));
    }
}
