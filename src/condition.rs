use nom::{alphanumeric, digit, multispace};
use std::str;

use common::{binary_comparison_operator, binary_logical_operator, unary_negation_operator};
use parser::{ConditionBase, ConditionExpression, ConditionTree};

fn fold_cond_exprs(initial: ConditionExpression,
                   remainder: Vec<(&str, ConditionExpression)>)
                   -> ConditionExpression {
    remainder.into_iter().fold(initial, |acc, pair| {
        let (oper, expr) = pair;
        match oper {
            "=" | ">" | "<" | ">=" | "<=" => {
                ConditionExpression::ComparisonOp(ConditionTree {
                    operator: String::from(oper.clone()),
                    left: Some(Box::new(acc)),
                    right: Some(Box::new(expr)),
                })
            }
            "and" | "or" => {
                ConditionExpression::LogicalOp(ConditionTree {
                    operator: String::from(oper.clone()),
                    left: Some(Box::new(acc)),
                    right: Some(Box::new(expr)),
                })
            }
            o => {
                println!("unsupported op {}", o);
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
            if let Some(no) = neg_op {
                ConditionExpression::LogicalOp(
                    ConditionTree {
                        operator: String::from(no),
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
                                  delimited!(tag!("\""), alphanumeric, tag!("\"")),
                                  opt!(multispace)),
                || {
                    ConditionExpression::Base(
                        ConditionBase::Literal(String::from(str::from_utf8(field).unwrap()))
                    )
                }
            )
        |   chain!(
                field: delimited!(opt!(multispace), alphanumeric, opt!(multispace)),
                || {
                    ConditionExpression::Base(
                        ConditionBase::Field(String::from(str::from_utf8(field).unwrap()))
                    )
                }
            )
    )
);

#[cfg(test)]
mod tests {
    use super::*;
    use parser::{ConditionBase, ConditionExpression, ConditionTree};

    fn flat_condition_tree(op: String, l: ConditionBase, r: ConditionBase) -> ConditionExpression {
        ConditionExpression::ComparisonOp(ConditionTree {
            operator: op,
            left: Some(Box::new(ConditionExpression::Base(l))),
            right: Some(Box::new(ConditionExpression::Base(r))),
        })
    }

    #[test]
    fn equality_placeholder() {
        let cond = "foo = ?";

        let res = condition_expr(cond.as_bytes());
        assert_eq!(res.unwrap().1,
                   flat_condition_tree(String::from("="),
                                       ConditionBase::Field(String::from("foo")),
                                       ConditionBase::Placeholder)
                  );
    }

    #[test]
    fn equality_literals() {
        let cond1 = "foo = 42";
        let cond2 = "foo = \"hello\"";

        let res1 = condition_expr(cond1.as_bytes());
        assert_eq!(res1.unwrap().1,
                   flat_condition_tree(String::from("="),
                                       ConditionBase::Field(String::from("foo")),
                                       ConditionBase::Literal(String::from("42"))
                                      )
                   );

        let res2 = condition_expr(cond2.as_bytes());
        assert_eq!(res2.unwrap().1,
                   flat_condition_tree(String::from("="),
                                       ConditionBase::Field(String::from("foo")),
                                       ConditionBase::Literal(String::from("hello"))
                                      )
                   );
    }
}
