use nom::multispace;
use nom::{Err, ErrorKind, IResult, Needed};
use std::collections::{HashSet, VecDeque};
use std::str;
use std::fmt;

use column::Column;
use common::{binary_comparison_operator, column_identifier, integer_literal, string_literal,
             Literal, Operator};

use select::{nested_selection, SelectStatement};

#[derive(Clone, Debug, Hash, PartialEq, Serialize, Deserialize)]
pub enum ConditionBase {
    Field(Column),
    Literal(Literal),
    Placeholder,
    NestedSelect(Box<SelectStatement>),
}

impl fmt::Display for ConditionBase {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ConditionBase::Field(ref col) => write!(f, "{}", col),
            ConditionBase::Literal(ref literal) => write!(f, "{}", literal.to_string()),
            ConditionBase::Placeholder => write!(f, "?"),
            ConditionBase::NestedSelect(ref select) => write!(f, "{}", select),
        }
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Serialize, Deserialize)]
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
                ConditionExpression::LogicalOp(ref ct) |
                ConditionExpression::ComparisonOp(ref ct) => q.push_back(ct),
                _ => (),
            }
            match *ct.right.as_ref() {
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

impl fmt::Display for ConditionTree {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.left)?;
        write!(f, " {} ", self.operator)?;
        write!(f, "{}", self.right)
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Serialize, Deserialize)]
pub enum ConditionExpression {
    ComparisonOp(ConditionTree),
    LogicalOp(ConditionTree),
    NegationOp(Box<ConditionExpression>),
    Base(ConditionBase),
}

impl fmt::Display for ConditionExpression {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ConditionExpression::ComparisonOp(ref tree) => write!(f, "{}", tree),
            ConditionExpression::LogicalOp(ref tree) => write!(f, "{}", tree),
            ConditionExpression::NegationOp(ref expr) => write!(f, "NOT {}", expr),
            ConditionExpression::Base(ref base) => write!(f, "{}", base),
        }
    }
}


/// Parse a conditional expression into a condition tree structure
named!(pub condition_expr<&[u8], ConditionExpression>,
       alt_complete!(
           chain!(
               left: and_expr ~
               multispace? ~
               caseless_tag!("or") ~
               multispace ~
               right: condition_expr,
               || {
                   ConditionExpression::LogicalOp(
                       ConditionTree {
                           operator: Operator::Or,
                           left: Box::new(left),
                           right: Box::new(right),
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
               multispace? ~
               caseless_tag!("and") ~
               multispace ~
               right: and_expr,
               || {
                   ConditionExpression::LogicalOp(
                       ConditionTree {
                           operator: Operator::And,
                           left: Box::new(left),
                           right: Box::new(right),
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
                    left: Box::new(left),
                    right: Box::new(right),
                }
            )

        }
    )
);


named!(predicate<&[u8], ConditionExpression>,
    delimited!(
        opt!(multispace),
        alt_complete!(
                chain!(
                    tag!("?"),
                    || {
                        ConditionExpression::Base(
                            ConditionBase::Placeholder
                        )
                    }
                )
            |   chain!(
                    field: integer_literal,
                    || {
                        ConditionExpression::Base(ConditionBase::Literal(field))
                    }
                )
            |   chain!(
                    field: string_literal,
                    || {
                        ConditionExpression::Base(ConditionBase::Literal(field))
                    }
                )
            |   chain!(
                    field: column_identifier,
                    || {
                        ConditionExpression::Base(
                            ConditionBase::Field(field)
                        )
                    }
                )
            |   chain!(
                    select: delimited!(tag!("("), nested_selection, tag!(")")),
                    || {
                        ConditionExpression::Base(
                            ConditionBase::NestedSelect(Box::new(select))
                        )
                    }
                )
        ),
        opt!(multispace)
    )
);

#[cfg(test)]
mod tests {
    use super::*;
    use column::Column;
    use common::{FieldExpression, Literal, Operator};

    fn columns(cols: &[&str]) -> Vec<FieldExpression> {
        cols.iter()
            .map(|c| FieldExpression::Col(Column::from(*c)))
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
        assert_eq!(
            res.unwrap().1,
            flat_condition_tree(
                Operator::Equal,
                ConditionBase::Field(Column::from("foo")),
                ConditionBase::Placeholder
            )
        );
    }

    #[test]
    fn equality_literals() {
        let cond1 = "foo = 42";
        let cond2 = "foo = \"hello\"";

        let res1 = condition_expr(cond1.as_bytes());
        assert_eq!(
            res1.unwrap().1,
            flat_condition_tree(
                Operator::Equal,
                ConditionBase::Field(Column::from("foo")),
                ConditionBase::Literal(Literal::Integer(42 as i64))
            )
        );

        let res2 = condition_expr(cond2.as_bytes());
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

        let res1 = condition_expr(cond1.as_bytes());
        assert_eq!(
            res1.unwrap().1,
            flat_condition_tree(
                Operator::GreaterOrEqual,
                ConditionBase::Field(Column::from("foo")),
                ConditionBase::Literal(Literal::Integer(42 as i64))
            )
        );

        let res2 = condition_expr(cond2.as_bytes());
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

        let res = condition_expr(cond.as_bytes());
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

        use ConditionExpression::*;
        use ConditionBase::*;
        use common::Literal;

        let a = ComparisonOp(ConditionTree {
            operator: Operator::Equal,
            left: Box::new(Base(Field("foo".into()))),
            right: Box::new(Base(Placeholder)),
        });

        let b = ComparisonOp(ConditionTree {
            operator: Operator::Equal,
            left: Box::new(Base(Field("bar".into()))),
            right: Box::new(Base(Literal(Literal::Integer(12.into())))),
        });

        let left = LogicalOp(ConditionTree {
            operator: Operator::Or,
            left: Box::new(a),
            right: Box::new(b),
        });

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

        let res = condition_expr(cond.as_bytes());
        assert_eq!(res.unwrap().1, complete);
    }

    #[test]
    fn order_of_operations() {
        let cond = "foo = ? and bar = 12 or foobar = 'a'";

        use ConditionExpression::*;
        use ConditionBase::*;
        use common::Literal;

        let a = ComparisonOp(ConditionTree {
            operator: Operator::Equal,
            left: Box::new(Base(Field("foo".into()))),
            right: Box::new(Base(Placeholder)),
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

        let res = condition_expr(cond.as_bytes());
        assert_eq!(res.unwrap().1, complete);
    }

    #[test]
    fn negation() {
        let cond = "not bar = 12 or foobar = 'a'";

        use ConditionExpression::*;
        use ConditionBase::*;
        use common::Literal::*;

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

        let res = condition_expr(cond.as_bytes());
        assert_eq!(res.unwrap().1, complete);
    }

    #[test]
    fn nested_select() {
        use select::SelectStatement;
        use table::Table;
        use ConditionBase::*;
        use std::default::Default;

        let cond = "bar in (select col from foo)";

        let res = condition_expr(cond.as_bytes());

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
        use table::Table;
        use ConditionBase::*;
        use std::default::Default;

        let cond = "paperId in (select paperId from PaperConflict) and size > 0";

        let res = condition_expr(cond.as_bytes());

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

}
