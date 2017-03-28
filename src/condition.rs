use nom::{alphanumeric, digit, multispace};
use nom::{IResult, Err, ErrorKind, Needed};
use std::collections::{HashSet, VecDeque};
use std::str;

use column::Column;
use common::{binary_comparison_operator, column_identifier, Operator};

#[derive(Clone, Debug, Hash, PartialEq)]
pub enum ConditionBase {
    Field(Column),
    Literal(String),
    Placeholder,
}

#[derive(Clone, Debug, Hash, PartialEq)]
pub struct ConditionTree {
    pub operator: Operator,
    pub left: Option<Box<ConditionExpression>>,
    pub right: Option<Box<ConditionExpression>>,
}

impl<'a> ConditionTree {
    pub fn contained_columns(&'a self) -> HashSet<&'a Column> {
        let mut s = HashSet::new();
        let mut q = VecDeque::<&'a ConditionTree>::new();
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
            match **ct.right.as_ref().unwrap() {
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

#[derive(Clone, Debug, Hash, PartialEq)]
pub enum ConditionExpression {
    ComparisonOp(ConditionTree),
    LogicalOp(ConditionTree),
    NegationOp(Box<ConditionExpression>),
    Base(ConditionBase),
}

/// Parse a conditional expression into a condition tree structure
named!(pub condition_expr<&[u8], ConditionExpression>,
       alt_complete!(
           chain!(
               left: and_expr ~
               caseless_tag!("or") ~
               multispace ~
               right: condition_expr,
               || {
                   ConditionExpression::LogicalOp(
                       ConditionTree {
                           operator: Operator::Or,
                           left: Some(Box::new(left)),
                           right: Some(Box::new(right)),
                       }
                   )
               }
           )
       |   and_expr)
);

named!(pub and_expr<&[u8], ConditionExpression>,
       alt_complete!(
           chain!(
               left: parenthetical_expr ~
               caseless_tag!("and") ~
               multispace ~
               right: and_expr,
               || {
                   ConditionExpression::LogicalOp(
                       ConditionTree {
                           operator: Operator::And,
                           left: Some(Box::new(left)),
                           right: Some(Box::new(right)),
                       }
                   )
               }
           )
       |   parenthetical_expr)
);

named!(pub parenthetical_expr<&[u8], ConditionExpression>,
       alt_complete!(
           delimited!(tag!("("), condition_expr, chain!(tag!(")") ~ multispace?, ||{}))
       |   not_expr)
);

named!(pub not_expr<&[u8], ConditionExpression>,
       alt_complete!(
           chain!(
               caseless_tag!("not") ~
               multispace ~
               right: parenthetical_expr,
               || {
                   ConditionExpression::NegationOp(Box::new(right))
               }
           )
       |   boolean_primary)
);

named!(boolean_primary<&[u8], ConditionExpression>,
    chain!(
        left: predicate ~
        multispace? ~
        op: binary_comparison_operator ~
        multispace? ~
        right: predicate,
        || {
            ConditionExpression::ComparisonOp(
                ConditionTree {
                    operator: op,
                    left: Some(Box::new(left)),
                    right: Some(Box::new(right)),
                }
            )

        }
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
                                        delimited!(tag!("\""), opt!(alphanumeric), tag!("\""))
                                      | delimited!(tag!("'"), opt!(alphanumeric), tag!("'"))
                                  ),
                                  opt!(multispace)),
                || {
                    let field = field.unwrap_or("".as_bytes());
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
        let c1 = Column::from("a.foo");
        let c2 = Column::from("b.bar");
        let mut expected_cols = HashSet::new();
        expected_cols.insert(&c1);
        expected_cols.insert(&c2);
        match res.unwrap().1 {
            ConditionExpression::LogicalOp(ct) => {
                assert_eq!(ct.contained_columns(), expected_cols);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn equality_placeholder() {
        let cond = "foo = ?";

        let res = condition_expr(cond.as_bytes());
        assert_eq!(res.unwrap().1,
                   flat_condition_tree(Operator::Equal,
                                       ConditionBase::Field(Column::from("foo")),
                                       ConditionBase::Placeholder));
    }

    #[test]
    fn equality_literals() {
        let cond1 = "foo = 42";
        let cond2 = "foo = \"hello\"";

        let res1 = condition_expr(cond1.as_bytes());
        assert_eq!(res1.unwrap().1,
                   flat_condition_tree(Operator::Equal,
                                       ConditionBase::Field(Column::from("foo")),
                                       ConditionBase::Literal(String::from("42"))));

        let res2 = condition_expr(cond2.as_bytes());
        assert_eq!(res2.unwrap().1,
                   flat_condition_tree(Operator::Equal,
                                       ConditionBase::Field(Column::from("foo")),
                                       ConditionBase::Literal(String::from("hello"))));
    }

    #[test]
    fn inequality_literals() {
        let cond1 = "foo >= 42";
        let cond2 = "foo <= 5";

        let res1 = condition_expr(cond1.as_bytes());
        assert_eq!(res1.unwrap().1,
                   flat_condition_tree(Operator::GreaterOrEqual,
                                       ConditionBase::Field(Column::from("foo")),
                                       ConditionBase::Literal(String::from("42"))));

        let res2 = condition_expr(cond2.as_bytes());
        assert_eq!(res2.unwrap().1,
                   flat_condition_tree(Operator::LessOrEqual,
                                       ConditionBase::Field(Column::from("foo")),
                                       ConditionBase::Literal(String::from("5"))));
    }

    #[test]
    fn empty_string_literal() {
        let cond = "foo = ''";

        let res = condition_expr(cond.as_bytes());
        assert_eq!(res.unwrap().1,
                   flat_condition_tree(Operator::Equal,
                                       ConditionBase::Field(Column::from("foo")),
                                       ConditionBase::Literal(String::from(""))));
    }

    #[test]
    fn parenthesis() {
        let cond = "(foo = ? or bar = 12) and foobar = 'a'";

        use ConditionExpression::*;
        use ConditionBase::*;

        let a = ComparisonOp(ConditionTree {
            operator: Operator::Equal,
            left: Some(Box::new(Base(Field("foo".into())))),
            right: Some(Box::new(Base(Placeholder))),
        });

        let b = ComparisonOp(ConditionTree {
            operator: Operator::Equal,
            left: Some(Box::new(Base(Field("bar".into())))),
            right: Some(Box::new(Base(Literal("12".into())))),
        });

        let left = LogicalOp(ConditionTree {
            operator: Operator::Or,
            left: Some(Box::new(a)),
            right: Some(Box::new(b)),
        });

        let right = ComparisonOp(ConditionTree {
            operator: Operator::Equal,
            left: Some(Box::new(Base(Field("foobar".into())))),
            right: Some(Box::new(Base(Literal("a".into())))),
        });

        let complete = LogicalOp(ConditionTree {
            operator: Operator::And,
            left: Some(Box::new(left)),
            right: Some(Box::new(right)),
        });

        let res = condition_expr(cond.as_bytes());
        assert_eq!(res.unwrap().1, complete);
    }

    #[test]
    fn order_of_operations() {
        let cond = "foo = ? and bar = 12 or foobar = 'a'";

        use ConditionExpression::*;
        use ConditionBase::*;

        let a = ComparisonOp(ConditionTree {
            operator: Operator::Equal,
            left: Some(Box::new(Base(Field("foo".into())))),
            right: Some(Box::new(Base(Placeholder))),
        });

        let b = ComparisonOp(ConditionTree {
            operator: Operator::Equal,
            left: Some(Box::new(Base(Field("bar".into())))),
            right: Some(Box::new(Base(Literal("12".into())))),
        });

        let left = LogicalOp(ConditionTree {
            operator: Operator::And,
            left: Some(Box::new(a)),
            right: Some(Box::new(b)),
        });

        let right = ComparisonOp(ConditionTree {
            operator: Operator::Equal,
            left: Some(Box::new(Base(Field("foobar".into())))),
            right: Some(Box::new(Base(Literal("a".into())))),
        });

        let complete = LogicalOp(ConditionTree {
            operator: Operator::Or,
            left: Some(Box::new(left)),
            right: Some(Box::new(right)),
        });

        let res = condition_expr(cond.as_bytes());
        assert_eq!(res.unwrap().1, complete);
    }

    #[test]
    fn negation() {
        let cond = "not bar = 12 or foobar = 'a'";

        use ConditionExpression::*;
        use ConditionBase::*;

        let left = NegationOp(Box::new(ComparisonOp(ConditionTree {
            operator: Operator::Equal,
            left: Some(Box::new(Base(Field("bar".into())))),
            right: Some(Box::new(Base(Literal("12".into())))),
        })));

        let right = ComparisonOp(ConditionTree {
            operator: Operator::Equal,
            left: Some(Box::new(Base(Field("foobar".into())))),
            right: Some(Box::new(Base(Literal("a".into())))),
        });

        let complete = LogicalOp(ConditionTree {
            operator: Operator::Or,
            left: Some(Box::new(left)),
            right: Some(Box::new(right)),
        });

        let res = condition_expr(cond.as_bytes());
        assert_eq!(res.unwrap().1, complete);
    }

}
