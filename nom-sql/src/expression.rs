use std::fmt::{self, Display};
use std::iter;

use concrete_iter::concrete_iter;
use derive_more::From;
use itertools::Itertools;
use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case};
use nom::character::complete::char;
use nom::combinator::{complete, map, opt};
use nom::multi::{many0, separated_list0};
use nom::sequence::{delimited, pair, preceded, terminated, tuple};
use nom::{IResult, Parser};
use pratt::{Affix, Associativity, PrattParser, Precedence};
use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

use crate::case::case_when;
use crate::common::{column_identifier_no_alias, function_expr, ws_sep_comma};
use crate::literal::literal;
use crate::select::nested_selection;
use crate::set::{variable_scope_prefix, Variable};
use crate::sql_type::{mysql_int_cast_targets, type_identifier};
use crate::whitespace::{whitespace0, whitespace1};
use crate::{Column, Dialect, Literal, SelectStatement, SqlIdentifier, SqlType};

/// Function call expressions
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Hash, Serialize, Deserialize)]
pub enum FunctionExpr {
    /// `AVG` aggregation. The boolean argument is `true` if `DISTINCT`
    Avg { expr: Box<Expr>, distinct: bool },

    /// `COUNT` aggregation
    Count { expr: Box<Expr>, distinct: bool },

    /// `COUNT(*)` aggregation
    CountStar,

    /// `SUM` aggregation
    Sum { expr: Box<Expr>, distinct: bool },

    /// `MAX` aggregation
    Max(Box<Expr>),

    /// `MIN` aggregation
    Min(Box<Expr>),

    /// `GROUP_CONCAT` aggregation. The second argument is the separator
    GroupConcat { expr: Box<Expr>, separator: String },

    /// The SQL `SUBSTRING`/`SUBSTR` function.
    ///
    /// The supported syntax is one of:
    ///
    /// `SUBSTR[ING](string FROM pos FOR len)`
    /// `SUBSTR[ING](string FROM pos)`
    /// `SUBSTR[ING](string, pos)`
    /// `SUBSTR[ING](string, pos, len)`
    Substring {
        string: Box<Expr>,
        pos: Option<Box<Expr>>,
        len: Option<Box<Expr>>,
    },

    /// Generic function call expression
    Call {
        name: SqlIdentifier,
        arguments: Vec<Expr>,
    },
}

impl FunctionExpr {
    /// Returns an iterator over all the direct arguments passed to the given function call
    /// expression
    #[concrete_iter]
    pub fn arguments<'a>(&'a self) -> impl Iterator<Item = &'a Expr> {
        match self {
            FunctionExpr::Avg { expr: arg, .. }
            | FunctionExpr::Count { expr: arg, .. }
            | FunctionExpr::Sum { expr: arg, .. }
            | FunctionExpr::Max(arg)
            | FunctionExpr::Min(arg)
            | FunctionExpr::GroupConcat { expr: arg, .. } => {
                concrete_iter!(iter::once(arg.as_ref()))
            }
            FunctionExpr::CountStar => concrete_iter!(iter::empty()),
            FunctionExpr::Call { arguments, .. } => concrete_iter!(arguments.iter()),
            FunctionExpr::Substring { string, pos, len } => {
                concrete_iter!(iter::once(string.as_ref())
                    .chain(pos.iter().map(|p| p.as_ref()))
                    .chain(len.iter().map(|p| p.as_ref())))
            }
        }
    }
}

impl Display for FunctionExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FunctionExpr::Avg {
                expr,
                distinct: true,
            } => write!(f, "avg(distinct {})", expr),
            FunctionExpr::Count {
                expr,
                distinct: true,
            } => write!(f, "count(distinct {})", expr),
            FunctionExpr::Sum {
                expr,
                distinct: true,
            } => write!(f, "sum(distinct {})", expr),
            FunctionExpr::Avg { expr, .. } => write!(f, "avg({})", expr),
            FunctionExpr::Count { expr, .. } => write!(f, "count({})", expr),
            FunctionExpr::CountStar => write!(f, "count(*)"),
            FunctionExpr::Sum { expr, .. } => write!(f, "sum({})", expr),
            FunctionExpr::Max(col) => write!(f, "max({})", col),
            FunctionExpr::Min(col) => write!(f, "min({})", col),
            FunctionExpr::GroupConcat { expr, separator } => {
                write!(f, "group_concat({} separator '{}')", expr, separator)
            }
            FunctionExpr::Call { name, arguments } => {
                write!(f, "{}({})", name, arguments.iter().join(", "))
            }
            FunctionExpr::Substring { string, pos, len } => {
                write!(f, "substring({string}")?;
                if let Some(pos) = pos {
                    write!(f, " from {pos}")?;
                }

                if let Some(len) = len {
                    write!(f, " for {len}")?;
                }

                write!(f, ")")
            }
        }
    }
}

/// Binary infix operators with [`Expr`] on both the left- and right-hand sides
///
/// This type is used as the operator in [`Expr::BinaryOp`].
///
/// Note that because all binary operators have expressions on both sides, SQL `IN` is not a binary
/// operator - since it must have either a subquery or a list of expressions on its right-hand side
#[derive(
    Clone, Copy, Debug, Eq, Hash, PartialEq, PartialOrd, Serialize, Deserialize, Arbitrary,
)]
pub enum BinaryOperator {
    /// `AND`
    And,
    /// `OR`
    Or,
    /// `LIKE`
    Like,
    /// `NOT LIKE`
    NotLike,
    /// `ILIKE`
    ILike,
    /// `NOT ILIKE`
    NotILike,
    /// `=`
    Equal,
    /// `!=` or `<>`
    NotEqual,
    /// `>`
    Greater,
    /// `>=`
    GreaterOrEqual,
    /// `<`
    Less,
    /// `<=`
    LessOrEqual,
    /// `IS`
    Is,
    /// `IS NOT`
    IsNot,
    /// `+`
    Add,
    /// `-`
    Subtract,
    /// `*`
    Multiply,
    /// `/`
    Divide,
}

impl BinaryOperator {
    /// Returns true if this operator represents an ordered comparison
    pub fn is_comparison(&self) -> bool {
        use BinaryOperator::*;
        matches!(self, Greater | GreaterOrEqual | Less | LessOrEqual)
    }

    /// If this operator is an ordered comparison, invert its meaning. (i.e. Greater becomes
    /// Less)
    pub fn flip_comparison(self) -> Result<Self, Self> {
        use BinaryOperator::*;
        match self {
            Greater => Ok(Less),
            GreaterOrEqual => Ok(LessOrEqual),
            Less => Ok(Greater),
            LessOrEqual => Ok(GreaterOrEqual),
            _ => Err(self),
        }
    }
}

impl Display for BinaryOperator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let op = match *self {
            BinaryOperator::And => "AND",
            BinaryOperator::Or => "OR",
            BinaryOperator::Like => "LIKE",
            BinaryOperator::NotLike => "NOT LIKE",
            BinaryOperator::ILike => "LIKE",
            BinaryOperator::NotILike => "NOT LIKE",
            BinaryOperator::Equal => "=",
            BinaryOperator::NotEqual => "!=",
            BinaryOperator::Greater => ">",
            BinaryOperator::GreaterOrEqual => ">=",
            BinaryOperator::Less => "<",
            BinaryOperator::LessOrEqual => "<=",
            BinaryOperator::Is => "IS",
            BinaryOperator::IsNot => "IS NOT",
            BinaryOperator::Add => "+",
            BinaryOperator::Subtract => "-",
            BinaryOperator::Multiply => "*",
            BinaryOperator::Divide => "/",
        };
        write!(f, "{}", op)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum UnaryOperator {
    Neg,
    Not,
}

impl Display for UnaryOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UnaryOperator::Neg => write!(f, "-"),
            UnaryOperator::Not => write!(f, "NOT"),
        }
    }
}

/// Right-hand side of IN
#[derive(Debug, PartialEq, Eq, PartialOrd, Hash, Clone, Serialize, Deserialize, From)]
pub enum InValue {
    Subquery(Box<SelectStatement>),
    List(Vec<Expr>),
}

impl Display for InValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InValue::Subquery(stmt) => write!(f, "{}", stmt),
            InValue::List(exprs) => write!(f, "{}", exprs.iter().join(", ")),
        }
    }
}

/// SQL Expression AST
#[derive(Debug, PartialEq, Eq, PartialOrd, Hash, Clone, Serialize, Deserialize, From)]
pub enum Expr {
    /// Function call expressions
    ///
    /// TODO(grfn): Eventually, the members of FunctionExpr should be inlined here
    Call(FunctionExpr),

    /// Literal values
    Literal(Literal),

    /// Binary operator
    BinaryOp {
        lhs: Box<Expr>,
        op: BinaryOperator,
        rhs: Box<Expr>,
    },

    /// Unary operator
    UnaryOp { op: UnaryOperator, rhs: Box<Expr> },

    /// CASE WHEN condition THEN then_expr ELSE else_expr
    CaseWhen {
        condition: Box<Expr>,
        then_expr: Box<Expr>,
        else_expr: Option<Box<Expr>>,
    },

    /// A reference to a column
    ///
    /// TODO(grfn): Inline Column here once we get a chance to get rid of the `alias` attribute on
    /// Column. Until then, an invariant is that `function = None` for all columns in this enum
    Column(Column),

    /// EXISTS (select)
    #[from(ignore)]
    Exists(Box<SelectStatement>),

    /// operand BETWEEN min AND max
    Between {
        operand: Box<Expr>,
        min: Box<Expr>,
        max: Box<Expr>,
        negated: bool,
    },

    /// A nested SELECT query
    NestedSelect(Box<SelectStatement>),

    /// An IN (or NOT IN) predicate
    ///
    /// Per the ANSI SQL standard, IN is its own AST node, not a binary operator
    In {
        lhs: Box<Expr>,
        rhs: InValue,
        negated: bool,
    },

    /// `CAST(expression AS type)`.
    Cast {
        expr: Box<Expr>,
        ty: SqlType,
        /// If true indicates that the expression used the Postgres syntax (expr::type)
        postgres_style: bool,
    },

    /// A variable reference
    Variable(Variable),
}

impl Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expr::Call(fe) => fe.fmt(f),
            Expr::Literal(l) => write!(f, "{}", l),
            Expr::Column(col) => col.fmt(f),
            Expr::CaseWhen {
                condition,
                then_expr,
                else_expr,
            } => {
                write!(f, "CASE WHEN {} THEN {}", condition, then_expr)?;
                if let Some(else_expr) = else_expr {
                    write!(f, " ELSE {}", else_expr)?;
                }
                write!(f, " END")
            }
            Expr::BinaryOp { lhs, op, rhs } => write!(f, "({} {} {})", lhs, op, rhs),
            Expr::UnaryOp {
                op: UnaryOperator::Neg,
                rhs,
            } => write!(f, "(-{})", rhs),
            Expr::UnaryOp { op, rhs } => write!(f, "({} {})", op, rhs),
            Expr::Exists(statement) => write!(f, "EXISTS ({})", statement),

            Expr::Between {
                operand,
                min,
                max,
                negated,
            } => {
                write!(
                    f,
                    "{} {}BETWEEN {} AND {}",
                    operand,
                    if *negated { "NOT " } else { "" },
                    min,
                    max
                )
            }
            Expr::In { lhs, rhs, negated } => {
                write!(f, "{}", lhs)?;
                if *negated {
                    write!(f, " NOT")?;
                }
                write!(f, " IN ({})", rhs)
            }
            Expr::NestedSelect(q) => write!(f, "({})", q),
            Expr::Cast {
                expr,
                ty,
                postgres_style,
            } if *postgres_style => write!(f, "({}::{})", expr, ty),
            Expr::Cast { expr, ty, .. } => write!(f, "CAST({} as {})", expr, ty),
            Expr::Variable(var) => write!(f, "{}", var),
        }
    }
}

impl Expr {
    /// If this expression is a [binary operator application](Expr::BinaryOp), returns a tuple
    /// of the left-hand side, the operator, and the right-hand side, otherwise returns None
    pub fn as_binary_op(&self) -> Option<(&Expr, BinaryOperator, &Expr)> {
        match self {
            Expr::BinaryOp { lhs, op, rhs } => Some((lhs.as_ref(), *op, rhs.as_ref())),
            _ => None,
        }
    }

    /// Returns true if any variables are present in the expression
    pub fn contains_vars(&self) -> bool {
        match self {
            Expr::Variable(_) => true,
            _ => self.recursive_subexpressions().any(Self::contains_vars),
        }
    }
}

/// A lexed sequence of tokens containing expressions and operators, pre-interpretation of operator
/// precedence but post-interpretation of parentheses
#[derive(Debug, Clone)]
enum TokenTree {
    Infix(BinaryOperator),
    Prefix(UnaryOperator),
    Primary(Expr),
    Group(Vec<TokenTree>),
    PgsqlCast(Box<TokenTree>, SqlType),
}

// no_and_or variants of `infix`, `rest`, and `token_tree` allow parsing (binary op) expressions in
// the right-hand side of a BETWEEN, eg:
//     foo between (1 + 2) and 3 + 5
// should parse the same as:
//     foo between (1 + 2) and (3 + 5)
// , but:
//     foo between (1 + 2) and 8 and bar
// should parse the same as:
//     (foo between (1 + 2) and 8) and bar

fn infix_no_and_or(i: &[u8]) -> IResult<&[u8], TokenTree> {
    let (i, operator) = alt((
        map(terminated(tag_no_case("like"), whitespace1), |_| {
            BinaryOperator::Like
        }),
        move |i| {
            let (i, _) = tag_no_case("not")(i)?;
            let (i, _) = whitespace1(i)?;
            let (i, _) = tag_no_case("like")(i)?;
            let (i, _) = whitespace1(i)?;

            Ok((i, BinaryOperator::NotLike))
        },
        move |i| {
            let (i, _) = tag_no_case("ilike")(i)?;
            let (i, _) = whitespace1(i)?;

            Ok((i, BinaryOperator::ILike))
        },
        move |i| {
            let (i, _) = tag_no_case("not")(i)?;
            let (i, _) = whitespace1(i)?;
            let (i, _) = tag_no_case("ilike")(i)?;
            let (i, _) = whitespace1(i)?;

            Ok((i, BinaryOperator::NotLike))
        },
        map(char('='), |_| BinaryOperator::Equal),
        map(tag("!="), |_| BinaryOperator::NotEqual),
        map(tag("<>"), |_| BinaryOperator::NotEqual),
        map(tag(">="), |_| BinaryOperator::GreaterOrEqual),
        map(tag("<="), |_| BinaryOperator::LessOrEqual),
        map(char('>'), |_| BinaryOperator::Greater),
        map(char('<'), |_| BinaryOperator::Less),
        move |i| {
            let (i, _) = tag_no_case("is")(i)?;
            let (i, _) = whitespace1(i)?;
            let (i, _) = tag_no_case("not")(i)?;
            let (i, _) = whitespace1(i)?;

            Ok((i, BinaryOperator::IsNot))
        },
        map(pair(tag_no_case("is"), whitespace1), |_| BinaryOperator::Is),
        map(char('+'), |_| BinaryOperator::Add),
        map(char('-'), |_| BinaryOperator::Subtract),
        map(char('*'), |_| BinaryOperator::Multiply),
        map(char('/'), |_| BinaryOperator::Divide),
    ))(i)?;

    Ok((i, TokenTree::Infix(operator)))
}

fn infix(i: &[u8]) -> IResult<&[u8], TokenTree> {
    complete(alt((
        map(terminated(tag_no_case("and"), whitespace1), |_| {
            TokenTree::Infix(BinaryOperator::And)
        }),
        map(terminated(tag_no_case("or"), whitespace1), |_| {
            TokenTree::Infix(BinaryOperator::Or)
        }),
        infix_no_and_or,
    )))(i)
}

fn prefix(i: &[u8]) -> IResult<&[u8], TokenTree> {
    map(
        alt((
            map(complete(char('-')), |_| UnaryOperator::Neg),
            map(terminated(tag_no_case("not"), whitespace1), |_| {
                UnaryOperator::Not
            }),
        )),
        TokenTree::Prefix,
    )(i)
}

fn primary_inner(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], TokenTree> {
    move |i| {
        alt((
            move |i| {
                let (i, _) = char('(')(i)?;
                let (i, _) = whitespace0(i)?;
                let (i, tree) = token_tree(dialect)(i)?;
                let (i, _) = whitespace0(i)?;
                let (i, _) = char(')')(i)?;

                Ok((i, TokenTree::Group(tree)))
            },
            move |i| {
                let (i, _) = whitespace0(i)?;
                let (i, expr) = simple_expr(dialect)(i)?;
                Ok((i, TokenTree::Primary(expr)))
            },
        ))(i)
    }
}

fn primary(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], TokenTree> {
    move |i| {
        let (i, expr) = primary_inner(dialect)(i)?;
        let (i, t) = opt(move |i| {
            let (i, _) = whitespace0(i)?;
            let (i, _) = tag("::")(i)?;
            let (i, _) = whitespace0(i)?;
            type_identifier(dialect)(i)
        })(i)?;

        Ok((
            i,
            t.map(|n| TokenTree::PgsqlCast(Box::new(expr.clone()), n))
                .unwrap_or(expr),
        ))
    }
}

fn rest(
    dialect: Dialect,
) -> impl Fn(&[u8]) -> IResult<&[u8], Vec<(TokenTree, Vec<TokenTree>, TokenTree)>> {
    move |i| {
        many0(move |i| {
            let (i, _) = whitespace0(i)?;
            let (i, infix_tree) = infix(i)?;
            let (i, _) = whitespace0(i)?;
            let (i, prefix_tree) = many0(prefix)(i)?;
            let (i, _) = whitespace0(i)?;
            let (i, primary_tree) = primary(dialect)(i)?;

            Ok((i, (infix_tree, prefix_tree, primary_tree)))
        })(i)
    }
}

fn token_tree(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], Vec<TokenTree>> {
    move |i| {
        let (i, prefix) = many0(prefix)(i)?;
        let (i, primary) = primary(dialect)(i)?;
        let (i, rest) = rest(dialect)(i)?;
        let mut res = prefix;
        res.push(primary);
        for (infix, mut prefix, primary) in rest {
            res.push(infix);
            res.append(&mut prefix);
            res.push(primary);
        }
        Ok((i, res))
    }
}

fn rest_no_and_or(
    dialect: Dialect,
) -> impl Fn(&[u8]) -> IResult<&[u8], Vec<(TokenTree, Vec<TokenTree>, TokenTree)>> {
    move |i| {
        many0(tuple((
            preceded(whitespace0, infix_no_and_or),
            delimited(whitespace0, many0(prefix), whitespace0),
            primary(dialect),
        )))(i)
    }
}

fn token_tree_no_and_or(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], Vec<TokenTree>> {
    move |i| {
        let (i, prefix) = many0(prefix)(i)?;
        let (i, primary) = primary(dialect)(i)?;
        let (i, rest) = rest_no_and_or(dialect)(i)?;
        let mut res = prefix;
        res.push(primary);
        for (infix, mut prefix, primary) in rest {
            res.push(infix);
            res.append(&mut prefix);
            res.push(primary);
        }
        Ok((i, res))
    }
}

/// A [`pratt`] operator-precedence parser for [`Expr`]s.
///
/// This type exists only to hold the implementation of the [`PrattParser`] trait for operator
/// precedence of expressions, and otherwise contains no data
struct ExprParser;

impl<I> PrattParser<I> for ExprParser
where
    I: Iterator<Item = TokenTree>,
{
    type Error = pratt::NoError;
    type Input = TokenTree;
    type Output = Expr;

    fn query(&mut self, input: &Self::Input) -> Result<Affix, Self::Error> {
        use BinaryOperator::*;
        use TokenTree::*;
        use UnaryOperator::*;

        // https://dev.mysql.com/doc/refman/8.0/en/operator-precedence.html
        Ok(match input {
            Infix(And) => Affix::Infix(Precedence(4), Associativity::Right),
            Infix(Or) => Affix::Infix(Precedence(2), Associativity::Right),
            Infix(Like) => Affix::Infix(Precedence(7), Associativity::Right),
            Infix(NotLike) => Affix::Infix(Precedence(7), Associativity::Right),
            Infix(ILike) => Affix::Infix(Precedence(7), Associativity::Right),
            Infix(NotILike) => Affix::Infix(Precedence(7), Associativity::Right),
            Infix(Equal) => Affix::Infix(Precedence(7), Associativity::Right),
            Infix(NotEqual) => Affix::Infix(Precedence(7), Associativity::Right),
            Infix(Greater) => Affix::Infix(Precedence(7), Associativity::Right),
            Infix(GreaterOrEqual) => Affix::Infix(Precedence(7), Associativity::Right),
            Infix(Less) => Affix::Infix(Precedence(7), Associativity::Right),
            Infix(LessOrEqual) => Affix::Infix(Precedence(7), Associativity::Right),
            Infix(Is) => Affix::Infix(Precedence(7), Associativity::Right),
            Infix(IsNot) => Affix::Infix(Precedence(7), Associativity::Right),
            Infix(Add) => Affix::Infix(Precedence(11), Associativity::Right),
            Infix(Subtract) => Affix::Infix(Precedence(11), Associativity::Right),
            Infix(Multiply) => Affix::Infix(Precedence(12), Associativity::Right),
            Infix(Divide) => Affix::Infix(Precedence(12), Associativity::Right),
            Prefix(Not) => Affix::Prefix(Precedence(6)),
            Prefix(Neg) => Affix::Prefix(Precedence(5)),
            Primary(_) => Affix::Nilfix,
            Group(_) => Affix::Nilfix,
            PgsqlCast(_, _) => Affix::Nilfix,
        })
    }

    fn primary(&mut self, input: Self::Input) -> Result<Self::Output, Self::Error> {
        use TokenTree::*;
        Ok(match input {
            Primary(expr) => expr,
            // unwrap: ok because there are no errors possible
            Group(group) => self.parse(&mut group.into_iter()).unwrap(),
            PgsqlCast(box expr, ty) => {
                let tt = self.parse(&mut vec![expr].into_iter()).unwrap();
                Expr::Cast {
                    expr: tt.into(),
                    ty,
                    postgres_style: true,
                }
            }
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

        Ok(Expr::BinaryOp {
            lhs: Box::new(lhs),
            op,
            rhs: Box::new(rhs),
        })
    }

    fn prefix(&mut self, op: Self::Input, rhs: Self::Output) -> Result<Self::Output, Self::Error> {
        let op = match op {
            TokenTree::Prefix(op) => op,
            _ => unreachable!("Invalid fixity for prefix op"),
        };

        Ok(Expr::UnaryOp {
            op,
            rhs: Box::new(rhs),
        })
    }

    fn postfix(
        &mut self,
        _lhs: Self::Output,
        _op: Self::Input,
    ) -> Result<Self::Output, Self::Error> {
        unreachable!("No postfix operators yet")
    }
}

fn in_lhs(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], Expr> {
    move |i| {
        alt((
            map(function_expr(dialect), Expr::Call),
            map(literal(dialect), Expr::Literal),
            case_when(dialect),
            map(column_identifier_no_alias(dialect), Expr::Column),
        ))(i)
    }
}

fn in_rhs(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], InValue> {
    move |i| {
        alt((
            map(nested_selection(dialect), |sel| {
                InValue::Subquery(Box::new(sel))
            }),
            map(
                separated_list0(ws_sep_comma, expression(dialect)),
                InValue::List,
            ),
        ))(i)
    }
}

fn in_expr(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], Expr> {
    move |i| {
        let (i, lhs) = terminated(in_lhs(dialect), whitespace1)(i)?;

        let (i, not) = opt(terminated(tag_no_case("not"), whitespace1))(i)?;
        let (i, _) = tag_no_case("in")(i)?;
        let (i, _) = whitespace0(i)?;

        let (i, _) = char('(')(i)?;
        let (i, _) = whitespace0(i)?;
        let (i, rhs) = in_rhs(dialect)(i)?;
        let (i, _) = whitespace0(i)?;
        let (i, _) = char(')')(i)?;

        Ok((
            i,
            Expr::In {
                lhs: Box::new(lhs),
                rhs,
                negated: not.is_some(),
            },
        ))
    }
}

fn between_operand(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], Expr> {
    move |i| {
        alt((
            parenthesized_expr(dialect),
            map(function_expr(dialect), Expr::Call),
            map(literal(dialect), Expr::Literal),
            case_when(dialect),
            map(column_identifier_no_alias(dialect), Expr::Column),
        ))(i)
    }
}

fn between_max(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], Expr> {
    move |i| {
        alt((
            map(token_tree_no_and_or(dialect), |tt| {
                ExprParser.parse(&mut tt.into_iter()).unwrap()
            }),
            simple_expr(dialect),
        ))(i)
    }
}

fn between_expr(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], Expr> {
    move |i| {
        let (i, operand) = map(between_operand(dialect), Box::new)(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, not) = opt(terminated(tag_no_case("not"), whitespace1))(i)?;
        let (i, _) = tag_no_case("between")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, min) = map(simple_expr(dialect), Box::new)(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, _) = tag_no_case("and")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, max) = map(between_max(dialect), Box::new)(i)?;

        Ok((
            i,
            Expr::Between {
                operand,
                min,
                max,
                negated: not.is_some(),
            },
        ))
    }
}

fn exists_expr(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], Expr> {
    move |i| {
        let (i, _) = tag_no_case("exists")(i)?;
        let (i, _) = whitespace0(i)?;

        let (i, _) = char('(')(i)?;
        let (i, _) = whitespace0(i)?;
        let (i, statement) = nested_selection(dialect)(i)?;
        let (i, _) = whitespace0(i)?;
        let (i, _) = char(')')(i)?;

        Ok((i, Expr::Exists(Box::new(statement))))
    }
}

fn cast(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], Expr> {
    move |i| {
        let (i, _) = tag_no_case("cast")(i)?;
        let (i, _) = whitespace0(i)?;
        let (i, _) = char('(')(i)?;

        let (i, arg) = expression(dialect)(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, _) = tag_no_case("as")(i)?;
        let (i, _) = whitespace1(i)?;

        let (i, ty) = if dialect == Dialect::MySQL {
            // Note that MySQL actually doesn't support every valid type identifier in CASTs; it
            // somewhat arbitrarily restrictive in what it accepts. However, it's not necessarily
            // harmful to allow casting to any type in ReadySet, and it's easier to allow it than
            // it is to restrict which types are allowed in this function, so we accept any valid
            // type here (along with MySQL-specific CAST targets via mysql_int_cast_targets()).
            alt((type_identifier(dialect), mysql_int_cast_targets()))(i)?
        } else {
            type_identifier(dialect)(i)?
        };

        let (i, _) = char(')')(i)?;

        Ok((
            i,
            Expr::Cast {
                expr: Box::new(arg),
                ty,
                postgres_style: false,
            },
        ))
    }
}

fn nested_select(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], Expr> {
    move |i| {
        let (i, _) = char('(')(i)?;
        let (i, _) = whitespace0(i)?;
        let (i, statement) = nested_selection(dialect)(i)?;
        let (i, _) = whitespace0(i)?;
        let (i, _) = char(')')(i)?;

        Ok((i, Expr::NestedSelect(Box::new(statement))))
    }
}

fn parenthesized_expr(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], Expr> {
    move |i| {
        let (i, _) = char('(')(i)?;
        let (i, _) = whitespace0(i)?;
        let (i, expr) = expression(dialect)(i)?;
        let (i, _) = whitespace0(i)?;
        let (i, _) = char(')')(i)?;

        Ok((i, expr))
    }
}

pub(crate) fn scoped_var(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], Variable> {
    move |i| {
        let (i, scope) = variable_scope_prefix(i)?;
        let (i, name) = dialect
            .identifier()
            .map(|ident| ident.to_ascii_lowercase().into())
            .parse(i)?;

        Ok((i, Variable { scope, name }))
    }
}

// Expressions without (binary or unary) operators
pub(crate) fn simple_expr(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], Expr> {
    move |i| {
        alt((
            parenthesized_expr(dialect),
            nested_select(dialect),
            exists_expr(dialect),
            between_expr(dialect),
            in_expr(dialect),
            map(function_expr(dialect), Expr::Call),
            map(literal(dialect), Expr::Literal),
            case_when(dialect),
            map(column_identifier_no_alias(dialect), Expr::Column),
            cast(dialect),
            map(scoped_var(dialect), Expr::Variable),
        ))(i)
    }
}

pub(crate) fn expression(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], Expr> {
    move |i| {
        alt((
            map(token_tree(dialect), |tt| {
                ExprParser.parse(&mut tt.into_iter()).unwrap()
            }),
            simple_expr(dialect),
        ))(i)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn column_then_column() {
        let (rem, res) = expression(Dialect::MySQL)(b"x y").unwrap();
        assert_eq!(res, Expr::Column("x".into()));
        assert_eq!(rem, b" y");
    }

    pub mod precedence {
        use super::*;

        pub fn parses_same(dialect: Dialect, implicit: &str, explicit: &str) {
            let implicit_res = expression(dialect)(implicit.as_bytes()).unwrap().1;
            let explicit_res = expression(dialect)(explicit.as_bytes()).unwrap().1;
            assert_eq!(implicit_res, explicit_res);
        }

        #[test]
        fn plus_times() {
            parses_same(Dialect::MySQL, "1 + 2 * 3", "(1 + (2 * 3))");
        }

        #[test]
        fn between_and_or() {
            parses_same(
                Dialect::MySQL,
                "x between y and z or w",
                "(x between y and z) or w",
            );
        }

        #[test]
        fn not_between_or() {
            parses_same(
                Dialect::MySQL,
                "(table_1.column_2 NOT BETWEEN 1 AND 5 OR table_1.column_2 NOT BETWEEN 1 AND 5)",
                "(table_1.column_2 NOT BETWEEN 1 AND 5) OR (table_1.column_2 NOT BETWEEN 1 AND 5)",
            )
        }
    }

    pub mod cast {
        use super::*;

        #[test]
        fn postgres_cast() {
            let res = expression(Dialect::PostgreSQL)(br#"-128::INTEGER"#);
            assert_eq!(
                res.unwrap().1,
                Expr::UnaryOp {
                    op: UnaryOperator::Neg,
                    rhs: Box::new(Expr::Cast {
                        expr: Box::new(Expr::Literal(Literal::UnsignedInteger(128))),
                        ty: SqlType::Int(None),
                        postgres_style: true
                    }),
                }
            );

            let res = expression(Dialect::PostgreSQL)(br#"515*128::TEXT"#);
            assert_eq!(
                res.unwrap().1,
                Expr::BinaryOp {
                    op: BinaryOperator::Multiply,
                    lhs: Box::new(Expr::Literal(Literal::UnsignedInteger(515))),
                    rhs: Box::new(Expr::Cast {
                        expr: Box::new(Expr::Literal(Literal::UnsignedInteger(128))),
                        ty: SqlType::Text,
                        postgres_style: true
                    }),
                }
            );

            let res = expression(Dialect::PostgreSQL)(br#"(515*128)::TEXT"#);
            assert_eq!(
                res.unwrap().1,
                Expr::Cast {
                    expr: Box::new(Expr::BinaryOp {
                        op: BinaryOperator::Multiply,
                        lhs: Box::new(Expr::Literal(Literal::UnsignedInteger(515))),
                        rhs: Box::new(Expr::Literal(Literal::UnsignedInteger(128)))
                    }),
                    ty: SqlType::Text,
                    postgres_style: true,
                },
            );

            let res = expression(Dialect::PostgreSQL)(br#"200*postgres.column::DOUBLE PRECISION"#);
            assert_eq!(
                res.unwrap().1,
                Expr::BinaryOp {
                    op: BinaryOperator::Multiply,
                    lhs: Box::new(Expr::Literal(Literal::UnsignedInteger(200))),
                    rhs: Box::new(Expr::Cast {
                        expr: Box::new(Expr::Column(Column::from("postgres.column"))),
                        ty: SqlType::Double,
                        postgres_style: true,
                    }),
                }
            );
        }

        #[test]
        fn mysql_cast() {
            let res = expression(Dialect::MySQL)(br#"CAST(-128 AS UNSIGNED)"#);
            assert_eq!(
                res.unwrap().1,
                Expr::Cast {
                    expr: Box::new(Expr::UnaryOp {
                        op: UnaryOperator::Neg,
                        rhs: Box::new(Expr::Literal(Literal::UnsignedInteger(128))),
                    }),
                    ty: SqlType::UnsignedBigInt(None),
                    postgres_style: false
                }
            );
        }
    }

    mod conditions {
        use super::*;
        use crate::{FieldDefinitionExpr, ItemPlaceholder, TableExpr};

        fn columns(cols: &[&str]) -> Vec<FieldDefinitionExpr> {
            cols.iter()
                .map(|c| FieldDefinitionExpr::from(Column::from(*c)))
                .collect()
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
            let res = expression(Dialect::MySQL)(cond.as_bytes());
            assert_eq!(
                res.unwrap().1,
                Expr::BinaryOp {
                    lhs: Box::new(Expr::Column(Column::from("foo"))),
                    op: BinaryOperator::Equal,
                    rhs: Box::new(Expr::Literal(literal))
                }
            );
        }

        fn x_operator_value(op: BinaryOperator, value: Literal) -> Expr {
            Expr::BinaryOp {
                lhs: Box::new(Expr::Column(Column::from("x"))),
                op,
                rhs: Box::new(Expr::Literal(value)),
            }
        }

        #[test]
        fn simple_arithmetic_expression() {
            let cond = "x + 3";

            let res = expression(Dialect::MySQL)(cond.as_bytes());
            assert_eq!(
                res.unwrap().1,
                x_operator_value(BinaryOperator::Add, 3_u32.into())
            );
        }

        #[test]
        fn simple_arithmetic_expression_with_parenthesis() {
            let cond = "( x - 2 )";

            let res = expression(Dialect::MySQL)(cond.as_bytes());
            assert_eq!(
                res.unwrap().1,
                x_operator_value(BinaryOperator::Subtract, 2_u32.into())
            );
        }

        #[test]
        fn parenthetical_arithmetic_expression() {
            let cond = "( x * 5 )";

            let res = expression(Dialect::MySQL)(cond.as_bytes());
            assert_eq!(
                res.unwrap().1,
                x_operator_value(BinaryOperator::Multiply, 5_u32.into())
            );
        }

        #[test]
        fn expression_with_arithmetics() {
            let cond = "x * 3 = 21";

            let res = expression(Dialect::MySQL)(cond.as_bytes());
            assert_eq!(
                res.unwrap().1,
                Expr::BinaryOp {
                    op: BinaryOperator::Equal,
                    lhs: Box::new(x_operator_value(BinaryOperator::Multiply, 3_u32.into())),
                    rhs: Box::new(Expr::Literal(21_u32.into()))
                }
            );
        }
        #[test]
        fn expression_with_arithmetics_and_parenthesis() {
            let cond = "(x - 7 = 15)";

            let res = expression(Dialect::MySQL)(cond.as_bytes());
            assert_eq!(
                res.unwrap().1,
                Expr::BinaryOp {
                    op: BinaryOperator::Equal,
                    lhs: Box::new(x_operator_value(BinaryOperator::Subtract, 7_u32.into())),
                    rhs: Box::new(Expr::Literal(15_u32.into()))
                }
            );
        }

        #[test]
        fn expression_with_arithmetics_in_parenthesis() {
            let cond = "( x + 2) = 15";

            let res = expression(Dialect::MySQL)(cond.as_bytes());
            assert_eq!(
                res.unwrap().1,
                Expr::BinaryOp {
                    op: BinaryOperator::Equal,
                    lhs: Box::new(x_operator_value(BinaryOperator::Add, 2_u32.into())),
                    rhs: Box::new(Expr::Literal(15_u32.into()))
                }
            );
        }

        #[test]
        fn expression_with_arithmetics_in_parenthesis_in_both_side() {
            let cond = "( x + 2) =(x*3)";

            let res = expression(Dialect::MySQL)(cond.as_bytes());
            assert_eq!(
                res.unwrap().1,
                Expr::BinaryOp {
                    op: BinaryOperator::Equal,
                    lhs: Box::new(x_operator_value(BinaryOperator::Add, 2_u32.into())),
                    rhs: Box::new(x_operator_value(BinaryOperator::Multiply, 3_u32.into()))
                }
            );
        }

        #[test]
        fn inequality_literals() {
            let cond1 = "foo >= 42";
            let cond2 = "foo <= 5";

            let res1 = expression(Dialect::MySQL)(cond1.as_bytes());
            assert_eq!(
                res1.unwrap().1,
                Expr::BinaryOp {
                    lhs: Box::new(Expr::Column(Column::from("foo"))),
                    op: BinaryOperator::GreaterOrEqual,
                    rhs: Box::new(Expr::Literal(Literal::UnsignedInteger(42)))
                }
            );

            let res2 = expression(Dialect::MySQL)(cond2.as_bytes());
            assert_eq!(
                res2.unwrap().1,
                Expr::BinaryOp {
                    lhs: Box::new(Expr::Column(Column::from("foo"))),
                    op: BinaryOperator::LessOrEqual,
                    rhs: Box::new(Expr::Literal(Literal::UnsignedInteger(5)))
                }
            );
        }

        #[test]
        fn empty_string_literal() {
            let cond = "foo = ''";

            let res = expression(Dialect::MySQL)(cond.as_bytes());
            assert_eq!(
                res.unwrap().1,
                Expr::BinaryOp {
                    lhs: Box::new(Expr::Column(Column::from("foo"))),
                    op: BinaryOperator::Equal,
                    rhs: Box::new(Expr::Literal(Literal::String(String::from(""))))
                }
            );
        }

        #[test]
        fn parenthesis() {
            let cond = "(foo = ? or bar = 12) and foobar = 'a'";

            let a = Expr::BinaryOp {
                op: BinaryOperator::Equal,
                lhs: Box::new(Expr::Column("foo".into())),
                rhs: Box::new(Expr::Literal(Literal::Placeholder(
                    ItemPlaceholder::QuestionMark,
                ))),
            };

            let b = Expr::BinaryOp {
                op: BinaryOperator::Equal,
                lhs: Box::new(Expr::Column("bar".into())),
                rhs: Box::new(Expr::Literal(Literal::UnsignedInteger(12))),
            };

            let left = Expr::BinaryOp {
                op: BinaryOperator::Or,
                lhs: Box::new(a),
                rhs: Box::new(b),
            };

            let right = Expr::BinaryOp {
                op: BinaryOperator::Equal,
                lhs: Box::new(Expr::Column("foobar".into())),
                rhs: Box::new(Expr::Literal(Literal::String("a".into()))),
            };

            let complete = Expr::BinaryOp {
                op: BinaryOperator::And,
                lhs: Box::new(left),
                rhs: Box::new(right),
            };

            let res = expression(Dialect::MySQL)(cond.as_bytes());
            assert_eq!(res.unwrap().1, complete);
        }

        #[test]
        fn order_of_operations() {
            let cond = "foo = ? and bar = 12 or foobar = 'a'";

            let a = Expr::BinaryOp {
                op: BinaryOperator::Equal,
                lhs: Box::new(Expr::Column("foo".into())),
                rhs: Box::new(Expr::Literal(Literal::Placeholder(
                    ItemPlaceholder::QuestionMark,
                ))),
            };

            let b = Expr::BinaryOp {
                op: BinaryOperator::Equal,
                lhs: Box::new(Expr::Column("bar".into())),
                rhs: Box::new(Expr::Literal(Literal::UnsignedInteger(12))),
            };

            let left = Expr::BinaryOp {
                op: BinaryOperator::And,
                lhs: Box::new(a),
                rhs: Box::new(b),
            };

            let right = Expr::BinaryOp {
                op: BinaryOperator::Equal,
                lhs: Box::new(Expr::Column("foobar".into())),
                rhs: Box::new(Expr::Literal(Literal::String("a".into()))),
            };

            let complete = Expr::BinaryOp {
                op: BinaryOperator::Or,
                lhs: Box::new(left),
                rhs: Box::new(right),
            };

            let res = expression(Dialect::MySQL)(cond.as_bytes());
            assert_eq!(res.unwrap().1, complete);
        }

        #[test]
        fn negation() {
            let cond = "not bar = 12 or foobar = 'a'";

            let left = Expr::UnaryOp {
                op: UnaryOperator::Not,
                rhs: Box::new(Expr::BinaryOp {
                    op: BinaryOperator::Equal,
                    lhs: Box::new(Expr::Column("bar".into())),
                    rhs: Box::new(Expr::Literal(Literal::UnsignedInteger(12))),
                }),
            };

            let right = Expr::BinaryOp {
                op: BinaryOperator::Equal,
                lhs: Box::new(Expr::Column("foobar".into())),
                rhs: Box::new(Expr::Literal(Literal::String("a".into()))),
            };

            let complete = Expr::BinaryOp {
                op: BinaryOperator::Or,
                lhs: Box::new(left),
                rhs: Box::new(right),
            };

            let res = expression(Dialect::MySQL)(cond.as_bytes());
            assert_eq!(res.unwrap().1, complete);
        }

        #[test]
        fn nested_select() {
            use crate::select::SelectStatement;
            use crate::table::Relation;

            let cond = "bar in (select col from foo)";

            let res = expression(Dialect::MySQL)(cond.as_bytes());

            let nested_select = Box::new(SelectStatement {
                tables: vec![TableExpr::from(Relation::from("foo"))],
                fields: columns(&["col"]),
                ..Default::default()
            });

            let expected = Expr::In {
                lhs: Box::new(Expr::Column("bar".into())),
                rhs: InValue::Subquery(nested_select),
                negated: false,
            };

            assert_eq!(res.unwrap().1, expected);
        }

        #[test]
        fn exists_in_select() {
            use crate::select::SelectStatement;
            use crate::table::Relation;

            let cond = "exists (  select col from foo  )";

            let res = expression(Dialect::MySQL)(cond.as_bytes());

            let nested_select = Box::new(SelectStatement {
                tables: vec![TableExpr::from(Relation::from("foo"))],
                fields: columns(&["col"]),
                ..Default::default()
            });

            let expected = Expr::Exists(nested_select);

            assert_eq!(res.unwrap().1, expected);
        }

        #[test]
        fn not_exists_in_select() {
            use crate::select::SelectStatement;
            use crate::table::Relation;

            let cond = "not exists (select col from foo)";

            let res = expression(Dialect::MySQL)(cond.as_bytes());

            let nested_select = Box::new(SelectStatement {
                tables: vec![TableExpr::from(Relation::from("foo"))],
                fields: columns(&["col"]),
                ..Default::default()
            });

            let expected = Expr::UnaryOp {
                op: UnaryOperator::Not,
                rhs: Box::new(Expr::Exists(nested_select)),
            };

            assert_eq!(res.unwrap().1, expected);
        }

        #[test]
        fn and_with_nested_select() {
            use crate::select::SelectStatement;
            use crate::table::Relation;

            let cond = "paperId in (select paperId from PaperConflict) and size > 0";

            let res = expression(Dialect::MySQL)(cond.as_bytes());

            let nested_select = Box::new(SelectStatement {
                tables: vec![TableExpr::from(Relation::from("PaperConflict"))],
                fields: columns(&["paperId"]),
                ..Default::default()
            });

            let left = Expr::In {
                lhs: Box::new(Expr::Column("paperId".into())),
                rhs: InValue::Subquery(nested_select),
                negated: false,
            };

            let right = Expr::BinaryOp {
                lhs: Box::new(Expr::Column("size".into())),
                op: BinaryOperator::Greater,
                rhs: Box::new(Expr::Literal(0_u32.into())),
            };

            let expected = Expr::BinaryOp {
                lhs: Box::new(left),
                rhs: Box::new(right),
                op: BinaryOperator::And,
            };

            assert_eq!(res.unwrap().1, expected);
        }

        #[test]
        fn in_list_of_values() {
            let cond = "bar in (0, 1)";

            let res = expression(Dialect::MySQL)(cond.as_bytes());

            let expected = Expr::In {
                lhs: Box::new(Expr::Column("bar".into())),
                rhs: InValue::List(vec![
                    Expr::Literal(0_u32.into()),
                    Expr::Literal(1_u32.into()),
                ]),
                negated: false,
            };

            assert_eq!(res.unwrap().1, expected);
        }

        #[test]
        fn is_null() {
            let cond = "bar IS NULL";

            let res = expression(Dialect::MySQL)(cond.as_bytes());

            let expected = Expr::BinaryOp {
                lhs: Box::new(Expr::Column("bar".into())),
                op: BinaryOperator::Is,
                rhs: Box::new(Expr::Literal(Literal::Null)),
            };
            assert_eq!(res.unwrap().1, expected);
        }

        #[test]
        fn is_not_null() {
            let cond = "bar IS NOT NULL";

            let res = expression(Dialect::MySQL)(cond.as_bytes());
            let expected = Expr::BinaryOp {
                lhs: Box::new(Expr::Column("bar".into())),
                op: BinaryOperator::IsNot,
                rhs: Box::new(Expr::Literal(Literal::Null)),
            };
            assert_eq!(res.unwrap().1, expected);
        }

        #[test]
        fn not_in_comparison() {
            let qs1 = b"id not in (1,2)";
            let res1 = expression(Dialect::MySQL)(qs1);

            let c1 = res1.unwrap().1;
            let expected1 = Expr::In {
                lhs: Box::new(Expr::Column("id".into())),
                rhs: InValue::List(vec![
                    Expr::Literal(1_u32.into()),
                    Expr::Literal(2_u32.into()),
                ]),
                negated: true,
            };
            assert_eq!(c1, expected1);

            let expected1 = "`id` NOT IN (1, 2)";
            assert_eq!(c1.to_string(), expected1);
        }

        #[test]
        fn between_simple() {
            let qs = b"foo between 1 and 2";
            let expected = Expr::Between {
                operand: Box::new(Expr::Column("foo".into())),
                min: Box::new(Expr::Literal(1_u32.into())),
                max: Box::new(Expr::Literal(2_u32.into())),
                negated: false,
            };
            let (remaining, result) = expression(Dialect::MySQL)(qs).unwrap();
            assert_eq!(std::str::from_utf8(remaining).unwrap(), "");
            assert_eq!(result, expected);
        }

        #[test]
        fn not_between() {
            let qs = b"foo not between 1 and 2";
            let expected = Expr::Between {
                operand: Box::new(Expr::Column("foo".into())),
                min: Box::new(Expr::Literal(1_u32.into())),
                max: Box::new(Expr::Literal(2_u32.into())),
                negated: true,
            };
            let (remaining, result) = expression(Dialect::MySQL)(qs).unwrap();
            assert_eq!(std::str::from_utf8(remaining).unwrap(), "");
            assert_eq!(result, expected);
        }

        #[test]
        fn between_function_call() {
            let qs = b"f(foo, bar) between 1 and 2";
            let expected = Expr::Between {
                operand: Box::new(Expr::Call(FunctionExpr::Call {
                    name: "f".into(),
                    arguments: vec![
                        Expr::Column(Column::from("foo")),
                        Expr::Column(Column::from("bar")),
                    ],
                })),
                min: Box::new(Expr::Literal(1_u32.into())),
                max: Box::new(Expr::Literal(2_u32.into())),
                negated: false,
            };
            let (remaining, result) = expression(Dialect::MySQL)(qs).unwrap();
            assert_eq!(String::from_utf8_lossy(remaining), "");
            assert_eq!(result, expected);
        }

        #[test]
        fn between_with_arithmetic() {
            let qs = b"foo between (1 + 2) and 3 + 5";
            let expected = Expr::Between {
                operand: Box::new(Expr::Column("foo".into())),
                min: Box::new(Expr::BinaryOp {
                    op: BinaryOperator::Add,
                    lhs: Box::new(Expr::Literal(Literal::UnsignedInteger(1))),
                    rhs: Box::new(Expr::Literal(Literal::UnsignedInteger(2))),
                }),
                max: Box::new(Expr::BinaryOp {
                    op: BinaryOperator::Add,
                    lhs: Box::new(Expr::Literal(Literal::UnsignedInteger(3))),
                    rhs: Box::new(Expr::Literal(Literal::UnsignedInteger(5))),
                }),
                negated: false,
            };
            let res = expression(Dialect::MySQL)(qs);
            let (remaining, result) = res.unwrap();
            assert_eq!(std::str::from_utf8(remaining).unwrap(), "");
            eprintln!("{}", result);
            assert_eq!(result, expected);
        }

        #[test]
        fn ilike() {
            let qs = b"name ILIKE ?";
            let expected = Expr::BinaryOp {
                lhs: Box::new(Expr::Column("name".into())),
                op: BinaryOperator::ILike,
                rhs: Box::new(Expr::Literal(Literal::Placeholder(
                    ItemPlaceholder::QuestionMark,
                ))),
            };
            let (remaining, result) = expression(Dialect::MySQL)(qs).unwrap();
            assert_eq!(std::str::from_utf8(remaining).unwrap(), "");
            assert_eq!(result, expected);
        }

        #[test]
        fn and_not() {
            let qs = b"x and not y";
            let expected = Expr::BinaryOp {
                lhs: Box::new(Expr::Column("x".into())),
                op: BinaryOperator::And,
                rhs: Box::new(Expr::UnaryOp {
                    op: UnaryOperator::Not,
                    rhs: Box::new(Expr::Column("y".into())),
                }),
            };
            let (remaining, result) = expression(Dialect::MySQL)(qs).unwrap();
            assert_eq!(std::str::from_utf8(remaining).unwrap(), "");
            assert_eq!(result, expected);
        }
    }

    mod negation {
        use super::*;

        #[test]
        fn neg_integer() {
            let qs = b"-256";
            let expected = Expr::UnaryOp {
                op: UnaryOperator::Neg,
                rhs: Box::new(Expr::Literal(Literal::UnsignedInteger(256))),
            };
            let (remaining, result) = expression(Dialect::MySQL)(qs).unwrap();
            assert_eq!(std::str::from_utf8(remaining).unwrap(), "");
            assert_eq!(result, expected);
        }

        #[test]
        fn neg_in_expression() {
            let qs = b"x + -y";
            let expected = Expr::BinaryOp {
                op: BinaryOperator::Add,
                lhs: Box::new(Expr::Column("x".into())),
                rhs: Box::new(Expr::UnaryOp {
                    op: UnaryOperator::Neg,
                    rhs: Box::new(Expr::Column("y".into())),
                }),
            };
            let (remaining, result) = expression(Dialect::MySQL)(qs).unwrap();
            assert_eq!(std::str::from_utf8(remaining).unwrap(), "");
            assert_eq!(result, expected);
        }

        #[test]
        fn neg_column() {
            let qs = b"NOT -id";
            let expected = Expr::UnaryOp {
                op: UnaryOperator::Not,
                rhs: Box::new(Expr::UnaryOp {
                    op: UnaryOperator::Neg,
                    rhs: Box::new(Expr::Column("id".into())),
                }),
            };
            let (remaining, result) = expression(Dialect::MySQL)(qs).unwrap();
            assert_eq!(std::str::from_utf8(remaining).unwrap(), "");
            assert_eq!(result, expected);
        }

        #[test]
        fn neg_not() {
            let qs = b"NOT -1";
            let expected = Expr::UnaryOp {
                op: UnaryOperator::Not,
                rhs: Box::new(Expr::UnaryOp {
                    op: UnaryOperator::Neg,
                    rhs: Box::new(Expr::Literal(Literal::UnsignedInteger(1))),
                }),
            };
            let (remaining, result) = expression(Dialect::MySQL)(qs).unwrap();
            assert_eq!(std::str::from_utf8(remaining).unwrap(), "");
            assert_eq!(result, expected);
        }

        #[test]
        fn neg_neg() {
            let qs = b"--1";
            let expected = Expr::UnaryOp {
                op: UnaryOperator::Neg,
                rhs: Box::new(Expr::UnaryOp {
                    op: UnaryOperator::Neg,
                    rhs: Box::new(Expr::Literal(Literal::UnsignedInteger(1))),
                }),
            };
            let (remaining, result) = expression(Dialect::MySQL)(qs).unwrap();
            assert_eq!(std::str::from_utf8(remaining).unwrap(), "");
            assert_eq!(result, expected);
        }
    }

    mod mysql {
        use super::*;

        mod precedence {
            use super::tests::precedence::parses_same;
            use crate::Dialect;

            #[test]
            fn is_and_between() {
                parses_same(
                    Dialect::MySQL,
                    "`h`.`x` is null
                 and month(`a`.`b`) between `t2`.`start` and `t2`.`end`
                 and dayofweek(`c`.`d`) between 1 and 3",
                    "((`h`.`x` is null)
                 and (month(`a`.`b`) between `t2`.`start` and `t2`.`end`)
                 and (dayofweek(`c`.`d`) between 1 and 3))",
                )
            }
        }

        mod conditions {
            use super::*;
            use crate::ItemPlaceholder;

            #[test]
            fn complex_bracketing() {
                let cond = "`read_ribbons`.`is_following` = 1 \
                    AND `comments`.`user_id` <> `read_ribbons`.`user_id` \
                    AND `saldo` >= 0 \
                    AND ( `parent_comments`.`user_id` = `read_ribbons`.`user_id` \
                    OR ( `parent_comments`.`user_id` IS NULL \
                    AND `stories`.`user_id` = `read_ribbons`.`user_id` ) ) \
                    AND ( `parent_comments`.`id` IS NULL \
                    OR `saldo` >= 0 ) \
                    AND `read_ribbons`.`user_id` = ?";

                let res = expression(Dialect::MySQL)(cond.as_bytes());
                let expected = Expr::BinaryOp {
                    op: BinaryOperator::And,
                    lhs: Box::new(Expr::BinaryOp {
                        lhs: Box::new(Expr::Column("read_ribbons.is_following".into())),
                        op: BinaryOperator::Equal,
                        rhs: Box::new(Expr::Literal(1_u32.into())),
                    }),
                    rhs: Box::new(Expr::BinaryOp {
                        op: BinaryOperator::And,
                        lhs: Box::new(Expr::BinaryOp {
                            lhs: Box::new(Expr::Column("comments.user_id".into())),
                            op: BinaryOperator::NotEqual,
                            rhs: Box::new(Expr::Column("read_ribbons.user_id".into())),
                        }),
                        rhs: Box::new(Expr::BinaryOp {
                            op: BinaryOperator::And,
                            lhs: Box::new(Expr::BinaryOp {
                                lhs: Box::new(Expr::Column("saldo".into())),
                                op: BinaryOperator::GreaterOrEqual,
                                rhs: Box::new(Expr::Literal(0_u32.into())),
                            }),
                            rhs: Box::new(Expr::BinaryOp {
                                op: BinaryOperator::And,
                                lhs: Box::new(Expr::BinaryOp {
                                    op: BinaryOperator::Or,
                                    lhs: Box::new(Expr::BinaryOp {
                                        lhs: Box::new(Expr::Column(
                                            "parent_comments.user_id".into(),
                                        )),
                                        op: BinaryOperator::Equal,
                                        rhs: Box::new(Expr::Column("read_ribbons.user_id".into())),
                                    }),
                                    rhs: Box::new(Expr::BinaryOp {
                                        op: BinaryOperator::And,
                                        lhs: Box::new(Expr::BinaryOp {
                                            lhs: Box::new(Expr::Column(
                                                "parent_comments.user_id".into(),
                                            )),
                                            op: BinaryOperator::Is,
                                            rhs: Box::new(Expr::Literal(Literal::Null)),
                                        }),
                                        rhs: Box::new(Expr::BinaryOp {
                                            lhs: Box::new(Expr::Column("stories.user_id".into())),
                                            op: BinaryOperator::Equal,
                                            rhs: Box::new(Expr::Column(
                                                "read_ribbons.user_id".into(),
                                            )),
                                        }),
                                    }),
                                }),
                                rhs: Box::new(Expr::BinaryOp {
                                    op: BinaryOperator::And,
                                    lhs: Box::new(Expr::BinaryOp {
                                        op: BinaryOperator::Or,
                                        lhs: Box::new(Expr::BinaryOp {
                                            lhs: Box::new(Expr::Column(
                                                "parent_comments.id".into(),
                                            )),
                                            op: BinaryOperator::Is,
                                            rhs: Box::new(Expr::Literal(Literal::Null)),
                                        }),
                                        rhs: Box::new(Expr::BinaryOp {
                                            lhs: Box::new(Expr::Column("saldo".into())),
                                            op: BinaryOperator::GreaterOrEqual,
                                            rhs: Box::new(Expr::Literal(0_u32.into())),
                                        }),
                                    }),
                                    rhs: Box::new(Expr::BinaryOp {
                                        op: BinaryOperator::Equal,
                                        lhs: Box::new(Expr::Column("read_ribbons.user_id".into())),
                                        rhs: Box::new(Expr::Literal(Literal::Placeholder(
                                            ItemPlaceholder::QuestionMark,
                                        ))),
                                    }),
                                }),
                            }),
                        }),
                    }),
                };
                let (rem, res) = res.unwrap();
                assert_eq!(std::str::from_utf8(rem).unwrap(), "");
                assert_eq!(res, expected);
            }

            #[test]
            fn equality_literals() {
                let cond1 = "foo = 42";
                let cond2 = "foo = \"hello\"";

                let res1 = expression(Dialect::MySQL)(cond1.as_bytes());
                assert_eq!(
                    res1.unwrap().1,
                    Expr::BinaryOp {
                        lhs: Box::new(Expr::Column(Column::from("foo"))),
                        op: BinaryOperator::Equal,
                        rhs: Box::new(Expr::Literal(Literal::UnsignedInteger(42)))
                    }
                );

                let res2 = expression(Dialect::MySQL)(cond2.as_bytes());
                assert_eq!(
                    res2.unwrap().1,
                    Expr::BinaryOp {
                        lhs: Box::new(Expr::Column(Column::from("foo"))),
                        op: BinaryOperator::Equal,
                        rhs: Box::new(Expr::Literal(Literal::String(String::from("hello"))))
                    }
                );
            }
        }
    }

    mod postgres {
        use super::*;

        mod precedence {
            use super::tests::precedence::parses_same;
            use crate::Dialect;

            #[test]
            fn is_and_between() {
                parses_same(
                    Dialect::PostgreSQL,
                    "\"h\".\"x\" is null
                 and month(\"a\".\"b\") between \"t2\".\"start\" and \"t2\".\"end\"
                 and dayofweek(\"c\".\"d\") between 1 and 3",
                    "((\"h\".\"x\" is null)
                 and (month(\"a\".\"b\") between \"t2\".\"start\" and \"t2\".\"end\")
                 and (dayofweek(\"c\".\"d\") between 1 and 3))",
                )
            }
        }

        mod conditions {
            use super::*;
            use crate::ItemPlaceholder;

            #[test]
            fn complex_bracketing() {
                let cond = "\"read_ribbons\".\"is_following\" = 1 \
                    AND \"comments\".\"user_id\" <> \"read_ribbons\".\"user_id\" \
                    AND \"saldo\" >= 0 \
                    AND ( \"parent_comments\".\"user_id\" = \"read_ribbons\".\"user_id\" \
                    OR ( \"parent_comments\".\"user_id\" IS NULL \
                    AND \"stories\".\"user_id\" = \"read_ribbons\".\"user_id\" ) ) \
                    AND ( \"parent_comments\".\"id\" IS NULL \
                    OR \"saldo\" >= 0 ) \
                    AND \"read_ribbons\".\"user_id\" = ?";

                let res = expression(Dialect::PostgreSQL)(cond.as_bytes());
                let expected = Expr::BinaryOp {
                    op: BinaryOperator::And,
                    lhs: Box::new(Expr::BinaryOp {
                        lhs: Box::new(Expr::Column("read_ribbons.is_following".into())),
                        op: BinaryOperator::Equal,
                        rhs: Box::new(Expr::Literal(1_u32.into())),
                    }),
                    rhs: Box::new(Expr::BinaryOp {
                        op: BinaryOperator::And,
                        lhs: Box::new(Expr::BinaryOp {
                            lhs: Box::new(Expr::Column("comments.user_id".into())),
                            op: BinaryOperator::NotEqual,
                            rhs: Box::new(Expr::Column("read_ribbons.user_id".into())),
                        }),
                        rhs: Box::new(Expr::BinaryOp {
                            op: BinaryOperator::And,
                            lhs: Box::new(Expr::BinaryOp {
                                lhs: Box::new(Expr::Column("saldo".into())),
                                op: BinaryOperator::GreaterOrEqual,
                                rhs: Box::new(Expr::Literal(0_u32.into())),
                            }),
                            rhs: Box::new(Expr::BinaryOp {
                                op: BinaryOperator::And,
                                lhs: Box::new(Expr::BinaryOp {
                                    op: BinaryOperator::Or,
                                    lhs: Box::new(Expr::BinaryOp {
                                        lhs: Box::new(Expr::Column(
                                            "parent_comments.user_id".into(),
                                        )),
                                        op: BinaryOperator::Equal,
                                        rhs: Box::new(Expr::Column("read_ribbons.user_id".into())),
                                    }),
                                    rhs: Box::new(Expr::BinaryOp {
                                        op: BinaryOperator::And,
                                        lhs: Box::new(Expr::BinaryOp {
                                            lhs: Box::new(Expr::Column(
                                                "parent_comments.user_id".into(),
                                            )),
                                            op: BinaryOperator::Is,
                                            rhs: Box::new(Expr::Literal(Literal::Null)),
                                        }),
                                        rhs: Box::new(Expr::BinaryOp {
                                            lhs: Box::new(Expr::Column("stories.user_id".into())),
                                            op: BinaryOperator::Equal,
                                            rhs: Box::new(Expr::Column(
                                                "read_ribbons.user_id".into(),
                                            )),
                                        }),
                                    }),
                                }),
                                rhs: Box::new(Expr::BinaryOp {
                                    op: BinaryOperator::And,
                                    lhs: Box::new(Expr::BinaryOp {
                                        op: BinaryOperator::Or,
                                        lhs: Box::new(Expr::BinaryOp {
                                            lhs: Box::new(Expr::Column(
                                                "parent_comments.id".into(),
                                            )),
                                            op: BinaryOperator::Is,
                                            rhs: Box::new(Expr::Literal(Literal::Null)),
                                        }),
                                        rhs: Box::new(Expr::BinaryOp {
                                            lhs: Box::new(Expr::Column("saldo".into())),
                                            op: BinaryOperator::GreaterOrEqual,
                                            rhs: Box::new(Expr::Literal(0_u32.into())),
                                        }),
                                    }),
                                    rhs: Box::new(Expr::BinaryOp {
                                        op: BinaryOperator::Equal,
                                        lhs: Box::new(Expr::Column("read_ribbons.user_id".into())),
                                        rhs: Box::new(Expr::Literal(Literal::Placeholder(
                                            ItemPlaceholder::QuestionMark,
                                        ))),
                                    }),
                                }),
                            }),
                        }),
                    }),
                };
                let (rem, res) = res.unwrap();
                assert_eq!(std::str::from_utf8(rem).unwrap(), "");
                assert_eq!(res, expected);
            }

            #[test]
            fn equality_literals() {
                let cond1 = "foo = 42";
                let cond2 = "foo = 'hello'";

                let res1 = expression(Dialect::PostgreSQL)(cond1.as_bytes());
                assert_eq!(
                    res1.unwrap().1,
                    Expr::BinaryOp {
                        lhs: Box::new(Expr::Column(Column::from("foo"))),
                        op: BinaryOperator::Equal,
                        rhs: Box::new(Expr::Literal(Literal::UnsignedInteger(42)))
                    }
                );

                let res2 = expression(Dialect::PostgreSQL)(cond2.as_bytes());
                assert_eq!(
                    res2.unwrap().1,
                    Expr::BinaryOp {
                        lhs: Box::new(Expr::Column(Column::from("foo"))),
                        op: BinaryOperator::Equal,
                        rhs: Box::new(Expr::Literal(Literal::String(String::from("hello"))))
                    }
                );
            }
        }
    }
}
