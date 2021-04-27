use std::{fmt, str};

use nom::character::complete::multispace0;
use nom::lib::std::fmt::Formatter;
use nom::{alt, char, complete, do_parse, many0, map, tuple};
use nom::{named, preceded};
use pratt::{Affix, Associativity, PrattParser, Precedence};

use crate::expression::simple_expr;
use crate::Expression;

#[derive(Debug, Clone, Copy, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub enum ArithmeticOperator {
    Add,
    Subtract,
    Multiply,
    Divide,
}

#[derive(Debug, Clone, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct Arithmetic {
    pub op: ArithmeticOperator,
    pub left: Box<Expression>,
    pub right: Box<Expression>,
}

impl fmt::Display for ArithmeticOperator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ArithmeticOperator::Add => write!(f, "+"),
            ArithmeticOperator::Subtract => write!(f, "-"),
            ArithmeticOperator::Multiply => write!(f, "*"),
            ArithmeticOperator::Divide => write!(f, "/"),
        }
    }
}

impl fmt::Display for Arithmetic {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "({} {} {})", self.left, self.op, self.right)
    }
}

#[derive(Debug)]
enum TokenTree {
    Infix(ArithmeticOperator),
    Primary(Expression),
    Group(Vec<TokenTree>),
}

named!(infix(&[u8]) -> TokenTree, complete!(map!(alt!(
    char!('+') => { |_| ArithmeticOperator::Add } |
    char!('-') => { |_| ArithmeticOperator::Subtract } |
    char!('*') => { |_| ArithmeticOperator::Multiply } |
    char!('/') => { |_| ArithmeticOperator::Divide }
), TokenTree::Infix)));

named!(primary(&[u8]) -> TokenTree, alt!(
    do_parse!(
        multispace0 >>
            char!('(') >>
            multispace0 >>
            group: token_tree >>
            multispace0 >>
            char!(')') >>
            (TokenTree::Group(group))
    ) |
    preceded!(multispace0, simple_expr) => { |s| TokenTree::Primary(s) }
));

named!(rest(&[u8]) -> Vec<(TokenTree, TokenTree)>, many0!(tuple!(
    preceded!(multispace0, infix),
    primary
)));

named!(token_tree(&[u8]) -> Vec<TokenTree>, do_parse!(
    primary: primary
        >> rest: rest
        >> ({
            let mut res = Vec::with_capacity(1 + rest.len() * 2);
            res.push(primary);
            for (infix, primary) in rest {
                res.push(infix);
                res.push(primary);
            }
            res
        })
));

struct ExprParser;

impl<I> PrattParser<I> for ExprParser
where
    I: Iterator<Item = TokenTree>,
{
    type Error = pratt::NoError;
    type Input = TokenTree;
    type Output = Expression;

    fn query(&mut self, input: &Self::Input) -> Result<Affix, Self::Error> {
        use ArithmeticOperator::*;
        use TokenTree::*;

        Ok(match input {
            Infix(Add) => Affix::Infix(Precedence(6), Associativity::Left),
            Infix(Subtract) => Affix::Infix(Precedence(6), Associativity::Left),
            Infix(Multiply) => Affix::Infix(Precedence(7), Associativity::Left),
            Infix(Divide) => Affix::Infix(Precedence(7), Associativity::Left),
            Primary(_) => Affix::Nilfix,
            Group(_) => Affix::Nilfix,
        })
    }

    fn primary(&mut self, input: Self::Input) -> Result<Self::Output, Self::Error> {
        use TokenTree::*;

        Ok(match input {
            Primary(expr) => expr,
            // unwrap: ok because there are no errors possible
            Group(group) => self.parse(&mut group.into_iter()).unwrap(),
            _ => unreachable!("Invalid fixity for non-primary token"),
        })
    }

    fn infix(
        &mut self,
        lhs: Self::Output,
        op: Self::Input,
        rhs: Self::Output,
    ) -> Result<Self::Output, Self::Error> {
        let op = match op {
            TokenTree::Infix(op) => op,
            _ => unreachable!("Invalid fixity for infix op"),
        };
        Ok(Expression::Arithmetic(Arithmetic {
            op,
            left: Box::new(lhs),
            right: Box::new(rhs),
        }))
    }

    fn prefix(
        &mut self,
        _op: Self::Input,
        _rhs: Self::Output,
    ) -> Result<Self::Output, Self::Error> {
        unreachable!("No prefix operators yet")
    }

    fn postfix(
        &mut self,
        _lhs: Self::Output,
        _op: Self::Input,
    ) -> Result<Self::Output, Self::Error> {
        unreachable!("No postfix operators yet")
    }
}

named!(pub(crate) arithmetic(&[u8]) -> Expression, map!(token_tree, |tt| {
    ExprParser.parse(&mut tt.into_iter()).unwrap()
}));

#[cfg(test)]
mod tests {
    use crate::arithmetic::ArithmeticOperator::{Add, Divide, Multiply, Subtract};
    use crate::Expression;

    use super::*;
    use nom::AsBytes;
    use pretty_assertions::assert_eq;

    #[test]
    fn nested_arithmetic() {
        let qs = [
            "1 + 1",
            "1 + 2 - 3",
            "1 + 2 * 3",
            "2 * 3 - 1 / 3",
            "3 * (1 + 2)",
        ];

        let expects = [
            Arithmetic {
                op: Add,
                left: Box::new(Expression::Literal(1.into())),
                right: Box::new(Expression::Literal(1.into())),
            },
            Arithmetic {
                op: Subtract,
                left: Box::new(Expression::Arithmetic(Arithmetic {
                    op: Add,
                    left: Box::new(Expression::Literal(1.into())),
                    right: Box::new(Expression::Literal(2.into())),
                })),
                right: Box::new(Expression::Literal(3.into())),
            },
            Arithmetic {
                op: Add,
                left: Box::new(Expression::Literal(1.into())),
                right: Box::new(Expression::Arithmetic(Arithmetic {
                    op: Multiply,
                    left: Box::new(Expression::Literal(2.into())),
                    right: Box::new(Expression::Literal(3.into())),
                })),
            },
            Arithmetic {
                op: Subtract,
                left: Box::new(Expression::Arithmetic(Arithmetic {
                    op: Multiply,
                    left: Box::new(Expression::Literal(2.into())),
                    right: Box::new(Expression::Literal(3.into())),
                })),
                right: Box::new(Expression::Arithmetic(Arithmetic {
                    op: Divide,
                    left: Box::new(Expression::Literal(1.into())),
                    right: Box::new(Expression::Literal(3.into())),
                })),
            },
            Arithmetic {
                op: Multiply,
                left: Box::new(Expression::Literal(3.into())),
                right: Box::new(Expression::Arithmetic(Arithmetic {
                    op: Add,
                    left: Box::new(Expression::Literal(1.into())),
                    right: Box::new(Expression::Literal(2.into())),
                })),
            },
        ];

        for (src, expected) in qs.iter().zip(&expects) {
            let res = arithmetic(src.as_bytes());
            let ari = res.unwrap().1;
            let expected = Expression::Arithmetic(expected.clone());
            assert_eq!(ari, expected);
            let res2 = arithmetic(ari.to_string().as_bytes()).unwrap().1;
            assert_eq!(res2, expected);
        }
    }

    #[test]
    fn arithmetic_with_column() {
        let res = arithmetic(b"x + 3");
        assert_eq!(
            res.unwrap().1,
            Expression::Arithmetic(Arithmetic {
                op: Add,
                left: Box::new(Expression::Column("x".into())),
                right: Box::new(Expression::Literal(3.into()))
            })
        )
    }
}
