use std::collections::{HashSet, VecDeque};
use std::fmt;
use std::str;

use crate::arithmetic::{arithmetic_expression, ArithmeticExpression};
use crate::column::Column;
use crate::common::{
    binary_comparison_operator, column_identifier, literal, value_list, BinaryOperator, Literal,
};

use crate::select::{nested_selection, SelectStatement};
use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case};
use nom::combinator::{map, opt};
use nom::sequence::{delimited, pair, preceded, separated_pair, terminated, tuple};
use nom::IResult;
use nom::{
    alt,
    character::complete::{multispace0, multispace1},
    delimited, do_parse, map, named, preceded, tag, tag_no_case, terminated,
};

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
    pub operator: BinaryOperator,
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
    ExistsOp(Box<SelectStatement>),
    Base(ConditionBase),
    Arithmetic(Box<ArithmeticExpression>),
    Bracketed(Box<ConditionExpression>),
    Between {
        operand: Box<ConditionExpression>,
        min: Box<ConditionExpression>,
        max: Box<ConditionExpression>,
    },
}

impl fmt::Display for ConditionExpression {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ConditionExpression::ComparisonOp(ref tree) => write!(f, "{}", tree),
            ConditionExpression::LogicalOp(ref tree) => write!(f, "{}", tree),
            ConditionExpression::NegationOp(ref expr) => write!(f, "NOT {}", expr),
            ConditionExpression::ExistsOp(ref expr) => write!(f, "EXISTS {}", expr),
            ConditionExpression::Bracketed(ref expr) => write!(f, "({})", expr),
            ConditionExpression::Base(ref base) => write!(f, "{}", base),
            ConditionExpression::Arithmetic(ref expr) => write!(f, "{}", expr),
            ConditionExpression::Between {
                ref operand,
                ref min,
                ref max,
            } => {
                write!(f, "{} BETWEEN {} AND {}", operand, min, max)
            }
        }
    }
}

// Parse a conditional expression into a condition tree structure
pub fn condition_expr(i: &[u8]) -> IResult<&[u8], ConditionExpression> {
    let cond = map(
        separated_pair(
            and_expr,
            delimited(multispace0, tag_no_case("or"), multispace1),
            condition_expr,
        ),
        |p| {
            ConditionExpression::LogicalOp(ConditionTree {
                operator: BinaryOperator::Or,
                left: Box::new(p.0),
                right: Box::new(p.1),
            })
        },
    );

    alt((between_expr, cond, and_expr))(i)
}

pub fn and_expr(i: &[u8]) -> IResult<&[u8], ConditionExpression> {
    let cond = map(
        separated_pair(
            parenthetical_expr,
            delimited(multispace0, tag_no_case("and"), multispace1),
            and_expr,
        ),
        |p| {
            ConditionExpression::LogicalOp(ConditionTree {
                operator: BinaryOperator::And,
                left: Box::new(p.0),
                right: Box::new(p.1),
            })
        },
    );

    alt((cond, parenthetical_expr))(i)
}

fn parenthetical_expr_helper(i: &[u8]) -> IResult<&[u8], ConditionExpression> {
    let (remaining_input, (_, _, left_expr, _, _, _, operator, _, right_expr)) = tuple((
        tag("("),
        multispace0,
        simple_expr,
        multispace0,
        tag(")"),
        multispace0,
        binary_comparison_operator,
        multispace0,
        simple_expr,
    ))(i)?;

    let left = Box::new(ConditionExpression::Bracketed(Box::new(left_expr)));
    let right = Box::new(right_expr);
    let cond = ConditionExpression::ComparisonOp(ConditionTree {
        operator,
        left,
        right,
    });

    Ok((remaining_input, cond))
}

pub fn parenthetical_expr(i: &[u8]) -> IResult<&[u8], ConditionExpression> {
    alt((
        parenthetical_expr_helper,
        map(
            delimited(
                terminated(tag("("), multispace0),
                condition_expr,
                delimited(multispace0, tag(")"), multispace0),
            ),
            |inner| ConditionExpression::Bracketed(Box::new(inner)),
        ),
        not_expr,
    ))(i)
}

pub fn not_expr(i: &[u8]) -> IResult<&[u8], ConditionExpression> {
    alt((
        map(
            preceded(pair(tag_no_case("not"), multispace1), parenthetical_expr),
            |right| ConditionExpression::NegationOp(Box::new(right)),
        ),
        boolean_primary,
    ))(i)
}

fn is_null(i: &[u8]) -> IResult<&[u8], (BinaryOperator, ConditionExpression)> {
    let (remaining_input, (_, _, not, _, _)) = tuple((
        tag_no_case("is"),
        multispace0,
        opt(tag_no_case("not")),
        multispace0,
        tag_no_case("null"),
    ))(i)?;

    // XXX(malte): bit of a hack; would consumers ever need to know
    // about "IS NULL" vs. "= NULL"?
    Ok((
        remaining_input,
        (
            if not.is_some() {
                BinaryOperator::NotEqual
            } else {
                BinaryOperator::Equal
            },
            ConditionExpression::Base(ConditionBase::Literal(Literal::Null)),
        ),
    ))
}

fn in_operation(i: &[u8]) -> IResult<&[u8], (BinaryOperator, ConditionExpression)> {
    map(
        separated_pair(
            opt(terminated(tag_no_case("not"), multispace1)),
            terminated(tag_no_case("in"), multispace0),
            alt((
                map(delimited(tag("("), nested_selection, tag(")")), |s| {
                    ConditionBase::NestedSelect(Box::new(s))
                }),
                map(delimited(tag("("), value_list, tag(")")), |vs| {
                    ConditionBase::LiteralList(vs)
                }),
            )),
        ),
        |p| {
            let nested = ConditionExpression::Base(p.1);
            if (p.0).is_some() {
                (BinaryOperator::NotIn, nested)
            } else {
                (BinaryOperator::In, nested)
            }
        },
    )(i)
}

fn boolean_primary_rest(i: &[u8]) -> IResult<&[u8], (BinaryOperator, ConditionExpression)> {
    alt((
        is_null,
        in_operation,
        separated_pair(binary_comparison_operator, multispace0, predicate),
    ))(i)
}

fn boolean_primary(i: &[u8]) -> IResult<&[u8], ConditionExpression> {
    alt((
        map(
            separated_pair(predicate, multispace0, boolean_primary_rest),
            |e: (ConditionExpression, (BinaryOperator, ConditionExpression))| {
                ConditionExpression::ComparisonOp(ConditionTree {
                    operator: (e.1).0,
                    left: Box::new(e.0),
                    right: Box::new((e.1).1),
                })
            },
        ),
        predicate,
    ))(i)
}

fn predicate(i: &[u8]) -> IResult<&[u8], ConditionExpression> {
    let nested_exists = map(
        tuple((
            opt(delimited(multispace0, tag_no_case("not"), multispace1)),
            delimited(multispace0, tag_no_case("exists"), multispace0),
            delimited(
                pair(tag("("), multispace0),
                nested_selection,
                pair(multispace0, tag(")")),
            ),
        )),
        |p| {
            let nested = ConditionExpression::ExistsOp(Box::new(p.2));
            if (p.0).is_some() {
                ConditionExpression::NegationOp(Box::new(nested))
            } else {
                nested
            }
        },
    );

    alt((simple_expr, nested_exists))(i)
}

named!(
    simple_expr<ConditionExpression>,
    alt!(
        delimited!(
            terminated!(tag!("("), multispace0),
            arithmetic_expression,
            preceded!(multispace0, tag!(")"))
        ) => { |e|
          ConditionExpression::Bracketed(Box::new(ConditionExpression::Arithmetic(Box::new(
            e,
          ))))
        } |
        arithmetic_expression => { |e| ConditionExpression::Arithmetic(Box::new(e)) } |
        literal => { |lit| ConditionExpression::Base(ConditionBase::Literal(lit)) } |
        column_identifier => { |f| ConditionExpression::Base(ConditionBase::Field(f)) } |
        delimited!(tag!("("), nested_selection, tag!(")")) => {
            |s| ConditionExpression::Base(ConditionBase::NestedSelect(Box::new(s)))
        }
    )
);

named!(
    between_expr<ConditionExpression>,
    do_parse!(
        operand: map!(simple_expr, Box::new)
            >> multispace1
            >> tag_no_case!("between")
            >> multispace1
            >> min: map!(simple_expr, Box::new)
            >> multispace1
            >> tag_no_case!("and")
            >> multispace1
            >> max: map!(simple_expr, Box::new)
            >> (ConditionExpression::Between { operand, min, max })
    )
);

#[cfg(test)]
mod tests {
    use crate::ArithmeticItem;

    use super::*;
    use crate::arithmetic::{ArithmeticBase, ArithmeticOperator};
    use crate::column::Column;
    use crate::common::{BinaryOperator, FieldDefinitionExpression, ItemPlaceholder, Literal};
    use ConditionBase::*;
    use ConditionExpression::*;

    fn columns(cols: &[&str]) -> Vec<FieldDefinitionExpression> {
        cols.iter()
            .map(|c| FieldDefinitionExpression::Col(Column::from(*c)))
            .collect()
    }

    fn flat_condition_tree(
        op: BinaryOperator,
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
        x_equality_variable_placeholder(
            "foo = ?",
            Literal::Placeholder(ItemPlaceholder::QuestionMark),
        );
    }

    #[test]
    fn equality_variable_placeholder() {
        x_equality_variable_placeholder(
            "foo = :12",
            Literal::Placeholder(ItemPlaceholder::ColonNumber(12)),
        );
    }

    #[test]
    fn equality_variable_placeholder_with_dollar_sign() {
        x_equality_variable_placeholder(
            "foo = $12",
            Literal::Placeholder(ItemPlaceholder::DollarNumber(12)),
        );
    }

    fn x_equality_variable_placeholder(cond: &str, literal: Literal) {
        let res = condition_expr(cond.as_bytes());
        assert_eq!(
            res.unwrap().1,
            flat_condition_tree(
                BinaryOperator::Equal,
                ConditionBase::Field(Column::from("foo")),
                ConditionBase::Literal(literal)
            )
        );
    }

    fn x_operator_value(op: ArithmeticOperator, value: Literal) -> ConditionExpression {
        ConditionExpression::Arithmetic(Box::new(ArithmeticExpression::new(
            op,
            ArithmeticBase::Column(Column::from("x")),
            ArithmeticBase::Scalar(value),
            None,
        )))
    }
    #[test]
    fn simple_arithmetic_expression() {
        let cond = "x + 3";

        let res = simple_expr(cond.as_bytes());
        assert_eq!(
            res.unwrap().1,
            x_operator_value(ArithmeticOperator::Add, 3.into())
        );
    }

    #[test]
    fn simple_arithmetic_expression_with_parenthesis() {
        let cond = "( x - 2 )";

        let res = simple_expr(cond.as_bytes());
        assert_eq!(
            res.unwrap().1,
            ConditionExpression::Bracketed(Box::new(x_operator_value(
                ArithmeticOperator::Subtract,
                2.into()
            )))
        );
    }

    #[test]
    fn parenthetical_arithmetic_expression() {
        let cond = "( x * 5 )";

        let res = parenthetical_expr(cond.as_bytes());
        assert_eq!(
            res.unwrap().1,
            ConditionExpression::Bracketed(Box::new(x_operator_value(
                ArithmeticOperator::Multiply,
                5.into()
            )))
        );
    }

    #[test]
    fn condition_expression_with_arithmetics() {
        let cond = "x * 3 = 21";

        let res = condition_expr(cond.as_bytes());
        assert_eq!(
            res.unwrap().1,
            ConditionExpression::ComparisonOp(ConditionTree {
                operator: BinaryOperator::Equal,
                left: Box::new(x_operator_value(ArithmeticOperator::Multiply, 3.into())),
                right: Box::new(ConditionExpression::Base(ConditionBase::Literal(21.into())))
            })
        );
    }
    #[test]
    fn condition_expression_with_arithmetics_and_parenthesis() {
        let cond = "(x - 7 = 15)";

        let res = condition_expr(cond.as_bytes());
        assert_eq!(
            res.unwrap().1,
            ConditionExpression::Bracketed(Box::new(ConditionExpression::ComparisonOp(
                ConditionTree {
                    operator: BinaryOperator::Equal,
                    left: Box::new(x_operator_value(ArithmeticOperator::Subtract, 7.into())),
                    right: Box::new(ConditionExpression::Base(ConditionBase::Literal(15.into())))
                }
            )))
        );
    }

    #[test]
    fn condition_expression_with_arithmetics_in_parenthesis() {
        let cond = "( x + 2) = 15";

        let res = condition_expr(cond.as_bytes());
        assert_eq!(
            res.unwrap().1,
            ConditionExpression::ComparisonOp(ConditionTree {
                operator: BinaryOperator::Equal,
                left: Box::new(ConditionExpression::Bracketed(Box::new(x_operator_value(
                    ArithmeticOperator::Add,
                    2.into()
                )))),
                right: Box::new(ConditionExpression::Base(ConditionBase::Literal(15.into())))
            })
        );
    }

    #[test]
    fn condition_expression_with_arithmetics_in_parenthesis_in_both_side() {
        let cond = "( x + 2) =(x*3)";

        let res = condition_expr(cond.as_bytes());
        assert_eq!(
            res.unwrap().1,
            ConditionExpression::ComparisonOp(ConditionTree {
                operator: BinaryOperator::Equal,
                left: Box::new(ConditionExpression::Bracketed(Box::new(x_operator_value(
                    ArithmeticOperator::Add,
                    2.into()
                )))),
                right: Box::new(ConditionExpression::Bracketed(Box::new(x_operator_value(
                    ArithmeticOperator::Multiply,
                    3.into()
                ))))
            })
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
                BinaryOperator::Equal,
                ConditionBase::Field(Column::from("foo")),
                ConditionBase::Literal(Literal::Integer(42 as i64))
            )
        );

        let res2 = condition_expr(cond2.as_bytes());
        assert_eq!(
            res2.unwrap().1,
            flat_condition_tree(
                BinaryOperator::Equal,
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
                BinaryOperator::GreaterOrEqual,
                ConditionBase::Field(Column::from("foo")),
                ConditionBase::Literal(Literal::Integer(42 as i64))
            )
        );

        let res2 = condition_expr(cond2.as_bytes());
        assert_eq!(
            res2.unwrap().1,
            flat_condition_tree(
                BinaryOperator::LessOrEqual,
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
                BinaryOperator::Equal,
                ConditionBase::Field(Column::from("foo")),
                ConditionBase::Literal(Literal::String(String::from("")))
            )
        );
    }

    #[test]
    fn parenthesis() {
        let cond = "(foo = ? or bar = 12) and foobar = 'a'";

        use crate::common::Literal;
        use ConditionBase::*;
        use ConditionExpression::*;

        let a = ComparisonOp(ConditionTree {
            operator: BinaryOperator::Equal,
            left: Box::new(Base(Field("foo".into()))),
            right: Box::new(Base(Literal(Literal::Placeholder(
                ItemPlaceholder::QuestionMark,
            )))),
        });

        let b = ComparisonOp(ConditionTree {
            operator: BinaryOperator::Equal,
            left: Box::new(Base(Field("bar".into()))),
            right: Box::new(Base(Literal(Literal::Integer(12.into())))),
        });

        let left = Bracketed(Box::new(LogicalOp(ConditionTree {
            operator: BinaryOperator::Or,
            left: Box::new(a),
            right: Box::new(b),
        })));

        let right = ComparisonOp(ConditionTree {
            operator: BinaryOperator::Equal,
            left: Box::new(Base(Field("foobar".into()))),
            right: Box::new(Base(Literal(Literal::String("a".into())))),
        });

        let complete = LogicalOp(ConditionTree {
            operator: BinaryOperator::And,
            left: Box::new(left),
            right: Box::new(right),
        });

        let res = condition_expr(cond.as_bytes());
        assert_eq!(res.unwrap().1, complete);
    }

    #[test]
    fn order_of_operations() {
        let cond = "foo = ? and bar = 12 or foobar = 'a'";

        use crate::common::Literal;
        use ConditionBase::*;
        use ConditionExpression::*;

        let a = ComparisonOp(ConditionTree {
            operator: BinaryOperator::Equal,
            left: Box::new(Base(Field("foo".into()))),
            right: Box::new(Base(Literal(Literal::Placeholder(
                ItemPlaceholder::QuestionMark,
            )))),
        });

        let b = ComparisonOp(ConditionTree {
            operator: BinaryOperator::Equal,
            left: Box::new(Base(Field("bar".into()))),
            right: Box::new(Base(Literal(Literal::Integer(12.into())))),
        });

        let left = LogicalOp(ConditionTree {
            operator: BinaryOperator::And,
            left: Box::new(a),
            right: Box::new(b),
        });

        let right = ComparisonOp(ConditionTree {
            operator: BinaryOperator::Equal,
            left: Box::new(Base(Field("foobar".into()))),
            right: Box::new(Base(Literal(Literal::String("a".into())))),
        });

        let complete = LogicalOp(ConditionTree {
            operator: BinaryOperator::Or,
            left: Box::new(left),
            right: Box::new(right),
        });

        let res = condition_expr(cond.as_bytes());
        assert_eq!(res.unwrap().1, complete);
    }

    #[test]
    fn negation() {
        let cond = "not bar = 12 or foobar = 'a'";

        use crate::common::Literal::*;
        use ConditionBase::*;
        use ConditionExpression::*;

        let left = NegationOp(Box::new(ComparisonOp(ConditionTree {
            operator: BinaryOperator::Equal,
            left: Box::new(Base(Field("bar".into()))),
            right: Box::new(Base(Literal(Integer(12.into())))),
        })));

        let right = ComparisonOp(ConditionTree {
            operator: BinaryOperator::Equal,
            left: Box::new(Base(Field("foobar".into()))),
            right: Box::new(Base(Literal(String("a".into())))),
        });

        let complete = LogicalOp(ConditionTree {
            operator: BinaryOperator::Or,
            left: Box::new(left),
            right: Box::new(right),
        });

        let res = condition_expr(cond.as_bytes());
        assert_eq!(res.unwrap().1, complete);
    }

    #[test]
    fn nested_select() {
        use crate::select::SelectStatement;
        use crate::table::Table;
        use ConditionBase::*;

        let cond = "bar in (select col from foo)";

        let res = condition_expr(cond.as_bytes());

        let nested_select = Box::new(SelectStatement {
            tables: vec![Table::from("foo")],
            fields: columns(&["col"]),
            ..Default::default()
        });

        let expected = flat_condition_tree(
            BinaryOperator::In,
            Field("bar".into()),
            NestedSelect(nested_select),
        );

        assert_eq!(res.unwrap().1, expected);
    }

    #[test]
    fn exists_in_select() {
        use crate::select::SelectStatement;
        use crate::table::Table;

        let cond = "exists (  select col from foo  )";

        let res = condition_expr(cond.as_bytes());

        let nested_select = Box::new(SelectStatement {
            tables: vec![Table::from("foo")],
            fields: columns(&["col"]),
            ..Default::default()
        });

        let expected = ConditionExpression::ExistsOp(nested_select);

        assert_eq!(res.unwrap().1, expected);
    }

    #[test]
    fn not_exists_in_select() {
        use crate::select::SelectStatement;
        use crate::table::Table;

        let cond = "not exists (select col from foo)";

        let res = condition_expr(cond.as_bytes());

        let nested_select = Box::new(SelectStatement {
            tables: vec![Table::from("foo")],
            fields: columns(&["col"]),
            ..Default::default()
        });

        let expected =
            ConditionExpression::NegationOp(Box::new(ConditionExpression::ExistsOp(nested_select)));

        assert_eq!(res.unwrap().1, expected);
    }

    #[test]
    fn and_with_nested_select() {
        use crate::select::SelectStatement;
        use crate::table::Table;
        use ConditionBase::*;

        let cond = "paperId in (select paperId from PaperConflict) and size > 0";

        let res = condition_expr(cond.as_bytes());

        let nested_select = Box::new(SelectStatement {
            tables: vec![Table::from("PaperConflict")],
            fields: columns(&["paperId"]),
            ..Default::default()
        });

        let left = flat_condition_tree(
            BinaryOperator::In,
            Field("paperId".into()),
            NestedSelect(nested_select),
        );

        let right = flat_condition_tree(
            BinaryOperator::Greater,
            Field("size".into()),
            Literal(0.into()),
        );

        let expected = ConditionExpression::LogicalOp(ConditionTree {
            left: Box::new(left),
            right: Box::new(right),
            operator: BinaryOperator::And,
        });

        assert_eq!(res.unwrap().1, expected);
    }

    #[test]
    fn in_list_of_values() {
        use ConditionBase::*;

        let cond = "bar in (0)";

        let res = condition_expr(cond.as_bytes());

        let expected = flat_condition_tree(
            BinaryOperator::In,
            Field("bar".into()),
            LiteralList(vec![0.into()]),
        );

        assert_eq!(res.unwrap().1, expected);
    }

    #[test]
    fn is_null() {
        use crate::common::Literal;
        use ConditionBase::*;

        let cond = "bar IS NULL";

        let res = condition_expr(cond.as_bytes());
        let expected = flat_condition_tree(
            BinaryOperator::Equal,
            Field("bar".into()),
            Literal(Literal::Null),
        );
        assert_eq!(res.unwrap().1, expected);

        let cond = "bar IS NOT NULL";

        let res = condition_expr(cond.as_bytes());
        let expected = flat_condition_tree(
            BinaryOperator::NotEqual,
            Field("bar".into()),
            Literal(Literal::Null),
        );
        assert_eq!(res.unwrap().1, expected);
    }

    #[test]
    fn complex_bracketing() {
        use crate::common::Literal;
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

        let res = condition_expr(cond.as_bytes());
        let expected = ConditionExpression::LogicalOp(ConditionTree {
            operator: BinaryOperator::And,
            left: Box::new(flat_condition_tree(
                BinaryOperator::Equal,
                Field("read_ribbons.is_following".into()),
                Literal(Literal::Integer(1.into())),
            )),
            right: Box::new(ConditionExpression::LogicalOp(ConditionTree {
                operator: BinaryOperator::And,
                left: Box::new(flat_condition_tree(
                    BinaryOperator::NotEqual,
                    Field("comments.user_id".into()),
                    Field("read_ribbons.user_id".into()),
                )),
                right: Box::new(ConditionExpression::LogicalOp(ConditionTree {
                    operator: BinaryOperator::And,
                    left: Box::new(flat_condition_tree(
                        BinaryOperator::GreaterOrEqual,
                        Field("saldo".into()),
                        Literal(Literal::Integer(0.into())),
                    )),
                    right: Box::new(ConditionExpression::LogicalOp(ConditionTree {
                        operator: BinaryOperator::And,
                        left: Box::new(ConditionExpression::Bracketed(Box::new(
                            ConditionExpression::LogicalOp(ConditionTree {
                                operator: BinaryOperator::Or,
                                left: Box::new(flat_condition_tree(
                                    BinaryOperator::Equal,
                                    Field("parent_comments.user_id".into()),
                                    Field("read_ribbons.user_id".into()),
                                )),
                                right: Box::new(ConditionExpression::Bracketed(Box::new(
                                    ConditionExpression::LogicalOp(ConditionTree {
                                        operator: BinaryOperator::And,
                                        left: Box::new(flat_condition_tree(
                                            BinaryOperator::Equal,
                                            Field("parent_comments.user_id".into()),
                                            Literal(Literal::Null),
                                        )),
                                        right: Box::new(flat_condition_tree(
                                            BinaryOperator::Equal,
                                            Field("stories.user_id".into()),
                                            Field("read_ribbons.user_id".into()),
                                        )),
                                    }),
                                ))),
                            }),
                        ))),
                        right: Box::new(ConditionExpression::LogicalOp(ConditionTree {
                            operator: BinaryOperator::And,
                            left: Box::new(ConditionExpression::Bracketed(Box::new(
                                ConditionExpression::LogicalOp(ConditionTree {
                                    operator: BinaryOperator::Or,
                                    left: Box::new(flat_condition_tree(
                                        BinaryOperator::Equal,
                                        Field("parent_comments.id".into()),
                                        Literal(Literal::Null),
                                    )),
                                    right: Box::new(flat_condition_tree(
                                        BinaryOperator::GreaterOrEqual,
                                        Field("saldo".into()),
                                        Literal(Literal::Integer(0)),
                                    )),
                                }),
                            ))),
                            right: Box::new(flat_condition_tree(
                                BinaryOperator::Equal,
                                Field("read_ribbons.user_id".into()),
                                Literal(Literal::Placeholder(ItemPlaceholder::QuestionMark)),
                            )),
                        })),
                    })),
                })),
            })),
        });
        let res = res.unwrap().1;
        assert_eq!(res, expected);
    }

    #[test]
    fn not_in_comparison() {
        use ConditionBase::*;

        let qs1 = b"id not in (1,2)";
        let res1 = condition_expr(qs1);

        let c1 = res1.unwrap().1;
        let expected1 = flat_condition_tree(
            BinaryOperator::NotIn,
            Field("id".into()),
            LiteralList(vec![1.into(), 2.into()]),
        );
        assert_eq!(c1, expected1);

        let expected1 = "id NOT IN (1, 2)";
        assert_eq!(format!("{}", c1), expected1);
    }

    #[test]
    fn between_simple() {
        let qs = b"foo between 1 and 2";
        let expected = Between {
            operand: Box::new(Base(Field("foo".into()))),
            min: Box::new(Base(Literal(1.into()))),
            max: Box::new(Base(Literal(2.into()))),
        };
        let (remaining, result) = condition_expr(qs).unwrap();
        assert_eq!(std::str::from_utf8(remaining).unwrap(), "");
        assert_eq!(result, expected);
    }

    #[test]
    fn between_with_arithmetic() {
        let qs = b"foo between (1 + 2) and 3 + 5";
        let expected = Between {
            operand: Box::new(Base(Field("foo".into()))),
            min: Box::new(Bracketed(Box::new(Arithmetic(Box::new(
                ArithmeticExpression {
                    ari: crate::arithmetic::Arithmetic {
                        op: ArithmeticOperator::Add,
                        left: ArithmeticItem::Base(ArithmeticBase::Scalar(Literal::Integer(1))),
                        right: ArithmeticItem::Base(ArithmeticBase::Scalar(Literal::Integer(2))),
                    },
                    alias: None,
                },
            ))))),
            max: Box::new(Arithmetic(Box::new(ArithmeticExpression {
                ari: crate::arithmetic::Arithmetic {
                    op: ArithmeticOperator::Add,
                    left: ArithmeticItem::Base(ArithmeticBase::Scalar(Literal::Integer(3))),
                    right: ArithmeticItem::Base(ArithmeticBase::Scalar(Literal::Integer(5))),
                },
                alias: None,
            }))),
        };
        let (remaining, result) = condition_expr(qs).unwrap();
        assert_eq!(std::str::from_utf8(remaining).unwrap(), "");
        assert_eq!(result, expected);
    }

    #[test]
    fn ilike() {
        let qs = b"name ILIKE ?";
        let expected = flat_condition_tree(
            BinaryOperator::ILike,
            ConditionBase::Field("name".into()),
            ConditionBase::Literal(Literal::Placeholder(ItemPlaceholder::QuestionMark)),
        );
        let (remaining, result) = condition_expr(qs).unwrap();
        assert_eq!(std::str::from_utf8(remaining).unwrap(), "");
        assert_eq!(result, expected);
    }
}
