use nom::{alphanumeric, eof, line_ending, multispace, space};
use nom::{IResult, Err, ErrorKind, Needed};
use std::str;

use caseless_tag::*;
use parser::{ConditionBase, ConditionExpression, ConditionTree};
use parser::{binary_comparison_operator, binary_logical_operator, unary_negation_operator};
use parser::{fieldlist, unsigned_number};

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
named!(pub condition_expr<&[u8], ConditionExpression>, dbg_dmp!(chain!(
    neg_op: opt!(unary_negation_operator) ~
    initial: boolean_primary ~
    remainder: many0!(
        chain!(
            log_op: delimited!(opt!(multispace), binary_logical_operator, opt!(multispace)) ~
            right_expr: condition_expr,
            || {
                println!("logical comparison");
                (log_op, right_expr)
            }
        )
	),
    || {
        if let Some(ref no) = neg_op {
            ConditionExpression::LogicalOp(
                ConditionTree {
                    operator: String::from(neg_op.unwrap()),
                    left: Some(Box::new(fold_cond_exprs(initial, remainder))),
                    right: None,
                }
            )
        } else {
            fold_cond_exprs(initial, remainder)
        }
    }
))
);

named!(boolean_primary<&[u8], ConditionExpression>, dbg_dmp!(chain!(
	initial: predicate ~
	remainder: many0!(
		chain!(
			op: delimited!(opt!(multispace), binary_comparison_operator, opt!(multispace)) ~
			right_expr: predicate,
			|| {
				(op, right_expr)
			}
		)
	),
	|| {
		println!("binary comparison");
        fold_cond_exprs(initial, remainder)
	}
))
);


named!(predicate<&[u8], ConditionExpression>,
    alt_complete!(
            chain!(
                delimited!(opt!(multispace), tag!("?"), opt!(multispace)),
                || {
                    println!("pred: placeholder");
                    ConditionExpression::Expr(
                        ConditionBase::Placeholder
                    )
                }
            )
        |   chain!(
                field: delimited!(opt!(multispace), alphanumeric, opt!(multispace)),
                || {
                    println!("pred: field {:?}", str::from_utf8(field).unwrap());
                    ConditionExpression::Expr(
                        ConditionBase::Field(String::from(str::from_utf8(field).unwrap()))
                    )
                }
            )
	)
);

// named!(pub condition_base<&[u8], ConditionExpression>,
// );

// Parse a conditional expression into a condition tree structure
// named!(pub condition_tree_rhs<&[u8], Option<(&str, &str, ConditionExpression)> >,
// );
