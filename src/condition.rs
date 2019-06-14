use nom::multispace;
use nom::types::CompleteByteSlice;
use std::collections::{HashSet, VecDeque};
use std::fmt;
use std::str;

use arithmetic::{arithmetic_expression, ArithmeticExpression};
use column::Column;
use common::{
    binary_comparison_operator, column_identifier, literal, opt_multispace, value_list, Literal,
    Operator,
};

use select::{nested_selection, SelectStatement};

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum ConditionBase {
    Field(Column),
    Literal(Literal),
    LiteralList(Vec<Literal>),
    NestedSelect(Box<SelectStatement>),
}

impl fmt::Display for ConditionBase {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ConditionBase::Field(ref col) => write!(f, "{}", col),
            ConditionBase::Literal(ref literal) => write!(f, "{}", literal.to_string()),
            ConditionBase::LiteralList(ref ll) => write!(
                f,
                "({})",
                ll.iter()
                    .map(|l| l.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            ConditionBase::NestedSelect(ref select) => write!(f, "{}", select),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct ConditionTree {
    pub operator: Operator,
    pub left: Box<ConditionExpression>,
    pub right: Box<ConditionExpression>,
}

impl<'a> ConditionTree {
    pub fn contained_columns(&'a self) -> HashSet<&'a Column> {
        let mut s = HashSet::new();
        let mut q = VecDeque::<&'a ConditionTree>::new();
        q.push_back(self);
        while let Some(ref ct) = q.pop_front() {
            match *ct.left.as_ref() {
                ConditionExpression::Base(ConditionBase::Field(ref c)) => {
                    s.insert(c);
                }
                ConditionExpression::LogicalOp(ref ct)
                | ConditionExpression::ComparisonOp(ref ct) => q.push_back(ct),
                _ => (),
            }
            match *ct.right.as_ref() {
                ConditionExpression::Base(ConditionBase::Field(ref c)) => {
                    s.insert(c);
                }
                ConditionExpression::LogicalOp(ref ct)
                | ConditionExpression::ComparisonOp(ref ct) => q.push_back(ct),
                _ => (),
            }
        }
        s
    }
}

impl fmt::Display for ConditionTree {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.left)?;
        write!(f, " {} ", self.operator)?;
        write!(f, "{}", self.right)
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum ConditionExpression {
    ComparisonOp(ConditionTree),
    LogicalOp(ConditionTree),
    NegationOp(Box<ConditionExpression>),
    Base(ConditionBase),
    Arithmetic(Box<ArithmeticExpression>),
    Bracketed(Box<ConditionExpression>),
}

impl fmt::Display for ConditionExpression {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ConditionExpression::ComparisonOp(ref tree) => write!(f, "{}", tree),
            ConditionExpression::LogicalOp(ref tree) => write!(f, "{}", tree),
            ConditionExpression::NegationOp(ref expr) => write!(f, "NOT {}", expr),
            ConditionExpression::Bracketed(ref expr) => write!(f, "({})", expr),
            ConditionExpression::Base(ref base) => write!(f, "{}", base),
            ConditionExpression::Arithmetic(ref expr) => write!(f, "{}", expr),
        }
    }
}

// Parse a conditional expression into a condition tree structure
named!(pub condition_expr<CompleteByteSlice, ConditionExpression>,
       alt!(
           do_parse!(
               left: and_expr >>
               opt_multispace >>
               tag_no_case!("or") >>
               multispace >>
               right: condition_expr >>
               (ConditionExpression::LogicalOp(
                   ConditionTree {
                       operator: Operator::Or,
                       left: Box::new(left),
                       right: Box::new(right),
                   }
               ))
           )
       |   and_expr)
);

named!(pub and_expr<CompleteByteSlice, ConditionExpression>,
       alt!(
           do_parse!(
               left: parenthetical_expr >>
               opt_multispace >>
               tag_no_case!("and") >>
               multispace >>
               right: and_expr >>
               (ConditionExpression::LogicalOp(
                   ConditionTree {
                       operator: Operator::And,
                       left: Box::new(left),
                       right: Box::new(right),
                   }
               ))
           )
       |   parenthetical_expr)
);

named!(pub parenthetical_expr<CompleteByteSlice, ConditionExpression>,
       alt!(
           do_parse!(
                tag!("(") >>
                opt_multispace >>
                left: simple_expr >>
                opt_multispace >>
                tag!(")") >>
                opt_multispace >>
                op: binary_comparison_operator >>
                opt_multispace >>
                right: simple_expr >>
                (ConditionExpression::ComparisonOp(
                    ConditionTree {
                        operator: op,
                        left: Box::new(
                            ConditionExpression::Bracketed(Box::new(left))
                        ),
                        right: Box::new(right)
                    })
                )
            )
        |    map!(
               delimited!(
                   do_parse!(tag!("(") >> opt_multispace >> ()),
                   condition_expr,
                   do_parse!(opt_multispace >> tag!(")") >> opt_multispace >> ())
               ),
               |inner| (ConditionExpression::Bracketed(Box::new(inner)))
            )
       |   not_expr)
);

named!(pub not_expr<CompleteByteSlice, ConditionExpression>,
       alt!(
           do_parse!(
               tag_no_case!("not") >>
               multispace >>
               right: parenthetical_expr >>
               (ConditionExpression::NegationOp(Box::new(right)))
           )
       |   boolean_primary)
);

named!(boolean_primary<CompleteByteSlice, ConditionExpression>,
    alt!(
        do_parse!(
            left: predicate >>
            opt_multispace >>
            rest: alt!(
                do_parse!(tag_no_case!("is") >>
                          opt_multispace >>
                          not: opt!(tag_no_case!("not")) >>
                          opt_multispace >>
                          tag_no_case!("null") >>
                          (
                              // XXX(malte): bit of a hack; would consumers ever need to know
                              // about "IS NULL" vs. "= NULL"?
                              if not.is_some() {
                                  Operator::NotEqual
                              } else {
                                  Operator::Equal
                              },
                              ConditionExpression::Base(
                                  ConditionBase::Literal(Literal::Null)
                              )
                          )

                ) |
                do_parse!(op: binary_comparison_operator >>
                          opt_multispace >>
                          right: predicate >>
                          (op, right)
                )
            ) >>
            (ConditionExpression::ComparisonOp(
                    ConditionTree {
                        operator: rest.0,
                        left: Box::new(left),
                        right: Box::new(rest.1),
                    })
            )
        ) |
        predicate
    )
);

named!(predicate<CompleteByteSlice, ConditionExpression>,
    do_parse!(
        left: simple_expr >>
        op_right: opt!(
            alt!(
                  do_parse!(
                      neg: opt!(preceded!(opt_multispace, tag_no_case!("not"))) >>
                      multispace >>
                      tag_no_case!("in") >>
                      multispace >>
                      sq: nested_selection >>
                      ({
                          let nested = ConditionExpression::Base(
                              ConditionBase::NestedSelect(Box::new(sq))
                          );
                          if neg.is_some() {
                              ConditionExpression::NegationOp(Box::new(nested))
                          } else {
                              nested
                          }
                      })
                  )
                | do_parse!(
                      neg: opt!(preceded!(opt_multispace, tag_no_case!("not"))) >>
                      multispace >>
                      tag_no_case!("in") >>
                      multispace >>
                      vl: delimited!(tag!("("), value_list, tag!(")")) >>
                      ({
                          let list = ConditionExpression::Base(ConditionBase::LiteralList(vl));
                          if neg.is_some() {
                              ConditionExpression::NegationOp(Box::new(list))
                          } else {
                              list
                          }
                      })
                  )
            )
        ) >>
        (match op_right {
            Some(right) => ConditionExpression::ComparisonOp(
                ConditionTree {
                    operator: Operator::In,
                    left: Box::new(left),
                    right: Box::new(right),
                }),
            None => left,
        })
    )
);

named!(simple_expr<CompleteByteSlice, ConditionExpression>,
    alt!(
            do_parse!(
                arit_expr: arithmetic_expression >>
                (ConditionExpression::Arithmetic(Box::new(arit_expr)))
            )
        |   do_parse!(
                tag!("(") >>
                opt_multispace >>
                arit_expr: arithmetic_expression >>
                opt_multispace >>
                tag!(")") >>
                (ConditionExpression::Bracketed(Box::new(
                    ConditionExpression::Arithmetic(Box::new(arit_expr))
                )))
            )
        |    do_parse!(
                lit: literal >>
                (ConditionExpression::Base(ConditionBase::Literal(lit)))
            )
        |   do_parse!(
                field: column_identifier >>
                (ConditionExpression::Base(
                    ConditionBase::Field(field)
                ))
            )
        |   do_parse!(
                select: delimited!(tag!("("), nested_selection, tag!(")")) >>
                (ConditionExpression::Base(
                    ConditionBase::NestedSelect(Box::new(select))
                ))
            )
    )
);

#[cfg(test)]
mod tests {
    use super::*;
    use column::Column;
    use common::{FieldDefinitionExpression, Literal, Operator};
    use arithmetic::{ArithmeticBase, ArithmeticOperator};

    fn columns(cols: &[&str]) -> Vec<FieldDefinitionExpression> {
        cols.iter()
            .map(|c| FieldDefinitionExpression::Col(Column::from(*c)))
            .collect()
    }

    fn flat_condition_tree(
        op: Operator,
        l: ConditionBase,
        r: ConditionBase,
    ) -> ConditionExpression {
        ConditionExpression::ComparisonOp(ConditionTree {
            operator: op,
            left: Box::new(ConditionExpression::Base(l)),
            right: Box::new(ConditionExpression::Base(r)),
        })
    }

    #[test]
    fn ct_contained_columns() {
        use std::collections::HashSet;

        let cond = "a.foo = ? and b.bar = 42";

        let res = condition_expr(CompleteByteSlice(cond.as_bytes()));
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

        let res = condition_expr(CompleteByteSlice(cond.as_bytes()));
        assert_eq!(
            res.unwrap().1,
            flat_condition_tree(
                Operator::Equal,
                ConditionBase::Field(Column::from("foo")),
                ConditionBase::Literal(Literal::Placeholder)
            )
        );
    }

    fn x_operator_value(op: ArithmeticOperator, value: Literal) -> ConditionExpression {
        ConditionExpression::Arithmetic(
            Box::new(ArithmeticExpression::new(
                op,
                ArithmeticBase::Column(Column::from("x")),
                ArithmeticBase::Scalar(value),
                None
                )
            )
        )
    }
    #[test]
    fn simple_arithmetic_expression() {
        let cond = "x + 3";

        let res = simple_expr(CompleteByteSlice(cond.as_bytes()));
        assert_eq!(
            res.unwrap().1,
            x_operator_value(ArithmeticOperator::Add, 3.into())
        );
    }

    #[test]
    fn simple_arithmetic_expression_with_parenthesis() {
        let cond = "( x - 2 )";

        let res = simple_expr(CompleteByteSlice(cond.as_bytes()));
        assert_eq!(
            res.unwrap().1,
            ConditionExpression::Bracketed(Box::new(
                x_operator_value(ArithmeticOperator::Subtract, 2.into())
            ))
        );
    }

    #[test]
    fn parenthetical_arithmetic_expression() {
        let cond = "( x * 5 )";

        let res = parenthetical_expr(CompleteByteSlice(cond.as_bytes()));
        assert_eq!(
            res.unwrap().1,
            ConditionExpression::Bracketed(Box::new(
                x_operator_value(ArithmeticOperator::Multiply, 5.into())
            ))
        );
    }

    #[test]
    fn condition_expression_with_arithmetics() {
        let cond = "x * 3 = 21";

        let res = condition_expr(CompleteByteSlice(cond.as_bytes()));
        assert_eq!(
            res.unwrap().1,
            ConditionExpression::ComparisonOp(ConditionTree {
                operator: Operator::Equal,
                left: Box::new(
                    x_operator_value(ArithmeticOperator::Multiply, 3.into())
                ),
                right: Box::new(
                    ConditionExpression::Base(ConditionBase::Literal(21.into()))
                )
            })
        );
    }
    #[test]
    fn condition_expression_with_arithmetics_and_parenthesis() {
        let cond = "(x - 7 = 15)";

        let res = condition_expr(CompleteByteSlice(cond.as_bytes()));
        assert_eq!(
            res.unwrap().1,
            ConditionExpression::Bracketed(Box::new(
                ConditionExpression::ComparisonOp(ConditionTree {
                    operator: Operator::Equal,
                    left: Box::new(
                        x_operator_value(ArithmeticOperator::Subtract, 7.into())
                    ),
                    right: Box::new(
                        ConditionExpression::Base(ConditionBase::Literal(15.into()))
                    )
                })
            ))
        );
    }

    #[test]
    fn condition_expression_with_arithmetics_in_parenthesis() {
        let cond = "( x + 2) = 15";

        let res = condition_expr(CompleteByteSlice(cond.as_bytes()));
        assert_eq!(
            res.unwrap().1,
            ConditionExpression::ComparisonOp(ConditionTree {
                operator: Operator::Equal,
                left: Box::new(
                    ConditionExpression::Bracketed(Box::new(
                        x_operator_value(ArithmeticOperator::Add, 2.into())
                    ))
                ),
                right: Box::new(
                    ConditionExpression::Base(ConditionBase::Literal(15.into()))
                )
            })
        );
    }

    #[test]
    fn condition_expression_with_arithmetics_in_parenthesis_in_both_side() {
        let cond = "( x + 2) =(x*3)";

        let res = condition_expr(CompleteByteSlice(cond.as_bytes()));
        assert_eq!(
            res.unwrap().1,
            ConditionExpression::ComparisonOp(ConditionTree {
                operator: Operator::Equal,
                left: Box::new(
                    ConditionExpression::Bracketed(Box::new(
                        x_operator_value(ArithmeticOperator::Add, 2.into())
                    ))
                ),
                right: Box::new(
                    ConditionExpression::Bracketed(Box::new(
                        x_operator_value(ArithmeticOperator::Multiply, 3.into())
                    ))
                )
            })
        );
    }



    #[test]
    fn equality_literals() {
        let cond1 = "foo = 42";
        let cond2 = "foo = \"hello\"";

        let res1 = condition_expr(CompleteByteSlice(cond1.as_bytes()));
        assert_eq!(
            res1.unwrap().1,
            flat_condition_tree(
                Operator::Equal,
                ConditionBase::Field(Column::from("foo")),
                ConditionBase::Literal(Literal::Integer(42 as i64))
            )
        );

        let res2 = condition_expr(CompleteByteSlice(cond2.as_bytes()));
        assert_eq!(
            res2.unwrap().1,
            flat_condition_tree(
                Operator::Equal,
                ConditionBase::Field(Column::from("foo")),
                ConditionBase::Literal(Literal::String(String::from("hello")))
            )
        );
    }

    #[test]
    fn inequality_literals() {
        let cond1 = "foo >= 42";
        let cond2 = "foo <= 5";

        let res1 = condition_expr(CompleteByteSlice(cond1.as_bytes()));
        assert_eq!(
            res1.unwrap().1,
            flat_condition_tree(
                Operator::GreaterOrEqual,
                ConditionBase::Field(Column::from("foo")),
                ConditionBase::Literal(Literal::Integer(42 as i64))
            )
        );

        let res2 = condition_expr(CompleteByteSlice(cond2.as_bytes()));
        assert_eq!(
            res2.unwrap().1,
            flat_condition_tree(
                Operator::LessOrEqual,
                ConditionBase::Field(Column::from("foo")),
                ConditionBase::Literal(Literal::Integer(5 as i64))
            )
        );
    }

    #[test]
    fn empty_string_literal() {
        let cond = "foo = ''";

        let res = condition_expr(CompleteByteSlice(cond.as_bytes()));
        assert_eq!(
            res.unwrap().1,
            flat_condition_tree(
                Operator::Equal,
                ConditionBase::Field(Column::from("foo")),
                ConditionBase::Literal(Literal::String(String::from("")))
            )
        );
    }

    #[test]
    fn parenthesis() {
        let cond = "(foo = ? or bar = 12) and foobar = 'a'";

        use common::Literal;
        use ConditionBase::*;
        use ConditionExpression::*;

        let a = ComparisonOp(ConditionTree {
            operator: Operator::Equal,
            left: Box::new(Base(Field("foo".into()))),
            right: Box::new(Base(Literal(Literal::Placeholder))),
        });

        let b = ComparisonOp(ConditionTree {
            operator: Operator::Equal,
            left: Box::new(Base(Field("bar".into()))),
            right: Box::new(Base(Literal(Literal::Integer(12.into())))),
        });

        let left = Bracketed(Box::new(LogicalOp(ConditionTree {
            operator: Operator::Or,
            left: Box::new(a),
            right: Box::new(b),
        })));

        let right = ComparisonOp(ConditionTree {
            operator: Operator::Equal,
            left: Box::new(Base(Field("foobar".into()))),
            right: Box::new(Base(Literal(Literal::String("a".into())))),
        });

        let complete = LogicalOp(ConditionTree {
            operator: Operator::And,
            left: Box::new(left),
            right: Box::new(right),
        });

        let res = condition_expr(CompleteByteSlice(cond.as_bytes()));
        assert_eq!(res.unwrap().1, complete);
    }

    #[test]
    fn order_of_operations() {
        let cond = "foo = ? and bar = 12 or foobar = 'a'";

        use common::Literal;
        use ConditionBase::*;
        use ConditionExpression::*;

        let a = ComparisonOp(ConditionTree {
            operator: Operator::Equal,
            left: Box::new(Base(Field("foo".into()))),
            right: Box::new(Base(Literal(Literal::Placeholder))),
        });

        let b = ComparisonOp(ConditionTree {
            operator: Operator::Equal,
            left: Box::new(Base(Field("bar".into()))),
            right: Box::new(Base(Literal(Literal::Integer(12.into())))),
        });

        let left = LogicalOp(ConditionTree {
            operator: Operator::And,
            left: Box::new(a),
            right: Box::new(b),
        });

        let right = ComparisonOp(ConditionTree {
            operator: Operator::Equal,
            left: Box::new(Base(Field("foobar".into()))),
            right: Box::new(Base(Literal(Literal::String("a".into())))),
        });

        let complete = LogicalOp(ConditionTree {
            operator: Operator::Or,
            left: Box::new(left),
            right: Box::new(right),
        });

        let res = condition_expr(CompleteByteSlice(cond.as_bytes()));
        assert_eq!(res.unwrap().1, complete);
    }

    #[test]
    fn negation() {
        let cond = "not bar = 12 or foobar = 'a'";

        use common::Literal::*;
        use ConditionBase::*;
        use ConditionExpression::*;

        let left = NegationOp(Box::new(ComparisonOp(ConditionTree {
            operator: Operator::Equal,
            left: Box::new(Base(Field("bar".into()))),
            right: Box::new(Base(Literal(Integer(12.into())))),
        })));

        let right = ComparisonOp(ConditionTree {
            operator: Operator::Equal,
            left: Box::new(Base(Field("foobar".into()))),
            right: Box::new(Base(Literal(String("a".into())))),
        });

        let complete = LogicalOp(ConditionTree {
            operator: Operator::Or,
            left: Box::new(left),
            right: Box::new(right),
        });

        let res = condition_expr(CompleteByteSlice(cond.as_bytes()));
        assert_eq!(res.unwrap().1, complete);
    }

    #[test]
    fn nested_select() {
        use select::SelectStatement;
        use std::default::Default;
        use table::Table;
        use ConditionBase::*;

        let cond = "bar in (select col from foo)";

        let res = condition_expr(CompleteByteSlice(cond.as_bytes()));

        let nested_select = Box::new(SelectStatement {
            tables: vec![Table::from("foo")],
            fields: columns(&["col"]),
            ..Default::default()
        });

        let expected = flat_condition_tree(
            Operator::In,
            Field("bar".into()),
            NestedSelect(nested_select),
        );

        assert_eq!(res.unwrap().1, expected);
    }

    #[test]
    fn and_with_nested_select() {
        use select::SelectStatement;
        use std::default::Default;
        use table::Table;
        use ConditionBase::*;

        let cond = "paperId in (select paperId from PaperConflict) and size > 0";

        let res = condition_expr(CompleteByteSlice(cond.as_bytes()));

        let nested_select = Box::new(SelectStatement {
            tables: vec![Table::from("PaperConflict")],
            fields: columns(&["paperId"]),
            ..Default::default()
        });

        let left = flat_condition_tree(
            Operator::In,
            Field("paperId".into()),
            NestedSelect(nested_select),
        );

        let right = flat_condition_tree(Operator::Greater, Field("size".into()), Literal(0.into()));

        let expected = ConditionExpression::LogicalOp(ConditionTree {
            left: Box::new(left),
            right: Box::new(right),
            operator: Operator::And,
        });

        assert_eq!(res.unwrap().1, expected);
    }

    #[test]
    fn in_list_of_values() {
        use ConditionBase::*;

        let cond = "bar in (0)";

        let res = condition_expr(CompleteByteSlice(cond.as_bytes()));

        let expected = flat_condition_tree(
            Operator::In,
            Field("bar".into()),
            LiteralList(vec![0.into()]),
        );

        assert_eq!(res.unwrap().1, expected);
    }

    #[test]
    fn is_null() {
        use common::Literal;
        use ConditionBase::*;

        let cond = "bar IS NULL";

        let res = condition_expr(CompleteByteSlice(cond.as_bytes()));
        let expected =
            flat_condition_tree(Operator::Equal, Field("bar".into()), Literal(Literal::Null));
        assert_eq!(res.unwrap().1, expected);

        let cond = "bar IS NOT NULL";

        let res = condition_expr(CompleteByteSlice(cond.as_bytes()));
        let expected = flat_condition_tree(
            Operator::NotEqual,
            Field("bar".into()),
            Literal(Literal::Null),
        );
        assert_eq!(res.unwrap().1, expected);
    }

    #[test]
    fn complex_bracketing() {
        use common::Literal;
        use ConditionBase::*;

        let cond = "`read_ribbons`.`is_following` = 1 \
                    AND `comments`.`user_id` <> `read_ribbons`.`user_id` \
                    AND `saldo` >= 0 \
                    AND ( `parent_comments`.`user_id` = `read_ribbons`.`user_id` \
                    OR ( `parent_comments`.`user_id` IS NULL \
                    AND `stories`.`user_id` = `read_ribbons`.`user_id` ) ) \
                    AND ( `parent_comments`.`id` IS NULL \
                    OR `saldo` >= 0 ) \
                    AND `read_ribbons`.`user_id` = ?";

        let res = condition_expr(CompleteByteSlice(cond.as_bytes()));
        let expected = ConditionExpression::LogicalOp(ConditionTree {
            operator: Operator::And,
            left: Box::new(flat_condition_tree(
                Operator::Equal,
                Field("read_ribbons.is_following".into()),
                Literal(Literal::Integer(1.into())),
            )),
            right: Box::new(ConditionExpression::LogicalOp(ConditionTree {
                operator: Operator::And,
                left: Box::new(flat_condition_tree(
                    Operator::NotEqual,
                    Field("comments.user_id".into()),
                    Field("read_ribbons.user_id".into()),
                )),
                right: Box::new(ConditionExpression::LogicalOp(ConditionTree {
                    operator: Operator::And,
                    left: Box::new(flat_condition_tree(
                        Operator::GreaterOrEqual,
                        Field("saldo".into()),
                        Literal(Literal::Integer(0.into())),
                    )),
                    right: Box::new(ConditionExpression::LogicalOp(ConditionTree {
                        operator: Operator::And,
                        left: Box::new(ConditionExpression::Bracketed(Box::new(
                            ConditionExpression::LogicalOp(ConditionTree {
                                operator: Operator::Or,
                                left: Box::new(flat_condition_tree(
                                    Operator::Equal,
                                    Field("parent_comments.user_id".into()),
                                    Field("read_ribbons.user_id".into()),
                                )),
                                right: Box::new(ConditionExpression::Bracketed(Box::new(
                                    ConditionExpression::LogicalOp(ConditionTree {
                                        operator: Operator::And,
                                        left: Box::new(flat_condition_tree(
                                            Operator::Equal,
                                            Field("parent_comments.user_id".into()),
                                            Literal(Literal::Null),
                                        )),
                                        right: Box::new(flat_condition_tree(
                                            Operator::Equal,
                                            Field("stories.user_id".into()),
                                            Field("read_ribbons.user_id".into()),
                                        )),
                                    }),
                                ))),
                            }),
                        ))),
                        right: Box::new(ConditionExpression::LogicalOp(ConditionTree {
                            operator: Operator::And,
                            left: Box::new(ConditionExpression::Bracketed(Box::new(
                                ConditionExpression::LogicalOp(ConditionTree {
                                    operator: Operator::Or,
                                    left: Box::new(flat_condition_tree(
                                        Operator::Equal,
                                        Field("parent_comments.id".into()),
                                        Literal(Literal::Null),
                                    )),
                                    right: Box::new(flat_condition_tree(
                                        Operator::GreaterOrEqual,
                                        Field("saldo".into()),
                                        Literal(Literal::Integer(0)),
                                    )),
                                }),
                            ))),
                            right: Box::new(flat_condition_tree(
                                Operator::Equal,
                                Field("read_ribbons.user_id".into()),
                                Literal(Literal::Placeholder),
                            )),
                        })),
                    })),
                })),
            })),
        });
        let res = res.unwrap().1;
        assert_eq!(res, expected);
    }
}
