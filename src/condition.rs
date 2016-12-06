use nom::{alphanumeric, digit, multispace};
use std::collections::{HashSet, VecDeque};
use std::str;

use column::Column;
use common::{binary_comparison_operator, binary_logical_operator, column_identifier,
             unary_negation_operator, Operator};

#[derive(Clone, Debug, PartialEq)]
pub enum ConditionBase {
    Field(Column),
    Literal(String),
    Placeholder,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ConditionTree {
    pub operator: Operator,
    pub left: Option<Box<ConditionExpression>>,
    pub right: Option<Box<ConditionExpression>>,
}

impl<'a> ConditionTree {
    fn contained_columns(&self) -> HashSet<&'a Column> {
        let mut s = HashSet::new();
        let mut q = VecDeque::<&ConditionTree>::new();
        q.push_back(self);
        while let Some(ref ct) = q.pop_front() {
            match **ct.left.as_ref().unwrap() {
                ConditionExpression::Base(ConditionBase::Field(ref c)) => {
                    s.insert(c);
                }
                ConditionExpression::LogicalOp(ref ct) |
                ConditionExpression::ComparisonOp(ref ct) => q.push_back(ct),
                _ => (),
            }
        }
        s
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum ConditionExpression {
    ComparisonOp(ConditionTree),
    LogicalOp(ConditionTree),
    Base(ConditionBase),
}

fn fold_cond_exprs(initial: ConditionExpression,
                   remainder: Vec<(Operator, ConditionExpression)>)
                   -> ConditionExpression {
    remainder.into_iter().fold(initial, |acc, pair| {
        let (oper, expr) = pair;
        match oper {
            Operator::Equal | Operator::Less | Operator::Greater | Operator::LessOrEqual |
            Operator::Like | Operator::GreaterOrEqual | Operator::NotEqual => {
                ConditionExpression::ComparisonOp(ConditionTree {
                    operator: oper.clone(),
                    left: Some(Box::new(acc)),
                    right: Some(Box::new(expr)),
                })
            }
            Operator::And | Operator::Or => {
                ConditionExpression::LogicalOp(ConditionTree {
                    operator: oper.clone(),
                    left: Some(Box::new(acc)),
                    right: Some(Box::new(expr)),
                })
            }
            o => {
                println!("unsupported op {:?}", o);
                unimplemented!()
            }
        }
    })
}

/// Parse a conditional expression into a condition tree structure
named!(pub condition_expr<&[u8], ConditionExpression>,
    chain!(
        neg_op: opt!(unary_negation_operator) ~
        initial: boolean_primary ~
        remainder: many0!(
            complete!(
                chain!(
                    log_op: delimited!(opt!(multispace),
                                       binary_logical_operator,
                                       opt!(multispace)) ~
                    right_expr: condition_expr,
                    || {
                        (log_op, right_expr)
                    }
                )
            )
        ),
        || {
            if let Some(Operator::Not) = neg_op {
                ConditionExpression::LogicalOp(
                    ConditionTree {
                        operator: Operator::Not,
                        left: Some(Box::new(fold_cond_exprs(initial, remainder))),
                        right: None,
                    }
                )
            } else {
                fold_cond_exprs(initial, remainder)
            }
        }
    )
);

named!(boolean_primary<&[u8], ConditionExpression>,
    chain!(
        initial: predicate ~
        remainder:
        many0!(
            complete!(
                chain!(
                    op: delimited!(opt!(multispace),
                                   binary_comparison_operator,
                                   opt!(multispace)) ~
                    right_expr: boolean_primary,
                    || {
                        (op, right_expr)
                    }
                )
            )
        ),
        || { fold_cond_exprs(initial, remainder) }
    )
);


named!(predicate<&[u8], ConditionExpression>,
    alt_complete!(
            chain!(
                delimited!(opt!(multispace), tag!("?"), opt!(multispace)),
                || {
                    ConditionExpression::Base(
                        ConditionBase::Placeholder
                    )
                }
            )
        |   chain!(
                field: delimited!(opt!(multispace), digit, opt!(multispace)),
                || {
                    ConditionExpression::Base(
                        ConditionBase::Literal(String::from(str::from_utf8(field).unwrap()))
                    )
                }
            )
        |   chain!(
                field: delimited!(opt!(multispace),
                                  alt_complete!(
                                        delimited!(tag!("\""), alphanumeric, tag!("\""))
                                      | delimited!(tag!("'"), alphanumeric, tag!("'"))
                                  ),
                                  opt!(multispace)),
                || {
                    ConditionExpression::Base(
                        ConditionBase::Literal(String::from(str::from_utf8(field).unwrap()))
                    )
                }
            )
        |   chain!(
                field: delimited!(opt!(multispace), column_identifier, opt!(multispace)),
                || {
                    ConditionExpression::Base(
                        ConditionBase::Field(field)
                    )
                }
            )
    )
);

#[cfg(test)]
mod tests {
    use super::*;
    use column::Column;
    use common::Operator;

    fn flat_condition_tree(op: Operator,
                           l: ConditionBase,
                           r: ConditionBase)
                           -> ConditionExpression {
        ConditionExpression::ComparisonOp(ConditionTree {
            operator: op,
            left: Some(Box::new(ConditionExpression::Base(l))),
            right: Some(Box::new(ConditionExpression::Base(r))),
        })
    }

    #[test]
    fn ct_contained_columns() {
        use std::collections::HashSet;

        let cond = "a.foo = ? and b.bar = 42";

        let res = condition_expr(cond.as_bytes());
        let expected_cols = HashSet::new();
        expected_cols.insert(&Column::from("a.foo"));
        expected_cols.insert(&Column::from("b.bar"));
        let cols = match res.unwrap().1 {
            ConditionExpression::LogicalOp(ref ct) => ct.contained_columns(),
            _ => panic!(),
        };
        assert_eq!(cols, expected_cols);
    }

    #[test]
    fn equality_placeholder() {
        let cond = "foo = ?";

        let res = condition_expr(cond.as_bytes());
        assert_eq!(res.unwrap().1,
                   flat_condition_tree(Operator::Equal,
                                       ConditionBase::Field(Column::from("foo")),
                                       ConditionBase::Placeholder)
                  );
    }

    #[test]
    fn equality_literals() {
        let cond1 = "foo = 42";
        let cond2 = "foo = \"hello\"";

        let res1 = condition_expr(cond1.as_bytes());
        assert_eq!(res1.unwrap().1,
                   flat_condition_tree(Operator::Equal,
                                       ConditionBase::Field(Column::from("foo")),
                                       ConditionBase::Literal(String::from("42"))
                                      )
                   );

        let res2 = condition_expr(cond2.as_bytes());
        assert_eq!(res2.unwrap().1,
                   flat_condition_tree(Operator::Equal,
                                       ConditionBase::Field(Column::from("foo")),
                                       ConditionBase::Literal(String::from("hello"))
                                      )
                   );
    }

    #[test]
    fn inequality_literals() {
        let cond1 = "foo >= 42";
        let cond2 = "foo <= 5";

        let res1 = condition_expr(cond1.as_bytes());
        assert_eq!(res1.unwrap().1,
                   flat_condition_tree(Operator::GreaterOrEqual,
                                       ConditionBase::Field(Column::from("foo")),
                                       ConditionBase::Literal(String::from("42"))
                                      )
                   );

        let res2 = condition_expr(cond2.as_bytes());
        assert_eq!(res2.unwrap().1,
                   flat_condition_tree(Operator::LessOrEqual,
                                       ConditionBase::Field(Column::from("foo")),
                                       ConditionBase::Literal(String::from("5"))
                                      )
                   );
    }
}
