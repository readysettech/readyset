use std::fmt::{self, Display};
use std::{iter, mem};

use concrete_iter::concrete_iter;
use derive_more::From;
use itertools::Itertools;
use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case};
use nom::character::complete::char;
use nom::combinator::{complete, map, opt, peek, value};
use nom::multi::{many0, many1, separated_list0, separated_list1};
use nom::sequence::{delimited, pair, preceded, terminated, tuple};
use nom::Parser;
use nom_locate::LocatedSpan;
use pratt::{Affix, Associativity, PrattParser, Precedence};
use proptest::prelude::Arbitrary;
use proptest::strategy::BoxedStrategy;
use readyset_util::fmt::fmt_with;
use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

use crate::common::{column_identifier_no_alias, function_expr, ws_sep_comma};
use crate::literal::literal;
use crate::select::nested_selection;
use crate::set::{variable_scope_prefix, Variable};
use crate::sql_type::{mysql_int_cast_targets, type_identifier};
use crate::whitespace::{whitespace0, whitespace1};
use crate::{
    Column, Dialect, DialectDisplay, Literal, NomSqlResult, SelectStatement, SqlIdentifier, SqlType,
};

/// Function call expressions
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Ord, Hash, Serialize, Deserialize, Arbitrary)]
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

    /// `GROUP_CONCAT` aggregation
    GroupConcat {
        expr: Box<Expr>,
        separator: Option<String>,
    },

    /// The SQL `SUBSTRING`/`SUBSTR` function.
    ///
    /// The supported syntax is one of:
    ///
    /// `SUBSTR[ING](string FROM pos FOR len)`
    /// `SUBSTR[ING](string FROM pos)`
    /// `SUBSTR[ING](string FOR len)`
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
            FunctionExpr::Call { arguments, .. } => concrete_iter!(arguments),
            FunctionExpr::Substring { string, pos, len } => {
                concrete_iter!(iter::once(string.as_ref())
                    .chain(pos.iter().map(|p| p.as_ref()))
                    .chain(len.iter().map(|p| p.as_ref())))
            }
        }
    }
}

impl DialectDisplay for FunctionExpr {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + Copy + '_ {
        fmt_with(move |f| match self {
            FunctionExpr::Avg {
                expr,
                distinct: true,
            } => write!(f, "avg(distinct {})", expr.display(dialect)),
            FunctionExpr::Count {
                expr,
                distinct: true,
            } => write!(f, "count(distinct {})", expr.display(dialect)),
            FunctionExpr::Sum {
                expr,
                distinct: true,
            } => write!(f, "sum(distinct {})", expr.display(dialect)),
            FunctionExpr::Avg { expr, .. } => write!(f, "avg({})", expr.display(dialect)),
            FunctionExpr::Count { expr, .. } => write!(f, "count({})", expr.display(dialect)),
            FunctionExpr::CountStar => write!(f, "count(*)"),
            FunctionExpr::Sum { expr, .. } => write!(f, "sum({})", expr.display(dialect)),
            FunctionExpr::Max(col) => write!(f, "max({})", col.display(dialect)),
            FunctionExpr::Min(col) => write!(f, "min({})", col.display(dialect)),
            FunctionExpr::GroupConcat { expr, separator } => {
                write!(f, "group_concat({}", expr.display(dialect),)?;
                if let Some(separator) = separator {
                    write!(
                        f,
                        " separator '{}'",
                        separator.replace('\'', "''").replace('\\', "\\\\")
                    )?;
                }
                write!(f, ")")
            }
            FunctionExpr::Call { name, arguments } => {
                write!(
                    f,
                    "{}({})",
                    name,
                    arguments.iter().map(|arg| arg.display(dialect)).join(", ")
                )
            }
            FunctionExpr::Substring { string, pos, len } => {
                write!(f, "substring({}", string.display(dialect))?;

                if let Some(pos) = pos {
                    write!(f, " from {}", pos.display(dialect))?;
                }

                if let Some(len) = len {
                    write!(f, " for {}", len.display(dialect))?;
                }

                write!(f, ")")
            }
        })
    }
}

/// Binary infix operators with [`Expr`] on both the left- and right-hand sides
///
/// This type is used as the operator in [`Expr::BinaryOp`].
///
/// Note that because all binary operators have expressions on both sides, SQL `IN` is not a binary
/// operator - since it must have either a subquery or a list of expressions on its right-hand side
#[derive(
    Clone, Copy, Debug, Eq, Hash, PartialEq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
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

    /// `#-`
    ///
    /// Postgres-specific JSONB operator that takes JSONB and returns JSONB with the value at a
    /// key/index path removed.
    HashSubtract,

    /// `*`
    Multiply,
    /// `/`
    Divide,

    /// `?`
    ///
    /// Postgres-specific JSONB operator. Looks for the given string as an object key or an array
    /// element and returns a boolean indicating the presence or absence of that string.
    QuestionMark,

    /// `?|`
    ///
    /// Postgres-specific JSONB operator. Takes an array of strings and checks whether *any* of
    /// those strings appear as object keys or array elements in the provided JSON value.
    QuestionMarkPipe,

    /// `?&`
    ///
    /// Postgres-specific JSONB operator. Takes an array of strings and checks whether *all* of
    /// those strings appear as object keys or array elements in the provided JSON value.
    QuestionMarkAnd,

    /// `||`
    ///
    /// This can represent a few different operators in different contexts. In MySQL it can
    /// represent a boolean OR operation or a string concat operation depending on whether
    /// `PIPES_AS_CONCAT` is enabled in the SQL mode. In Postgres it can either represent string
    /// concatenation or JSON concatenation, depending on the context.
    DoublePipe,

    /// `->`
    ///
    /// This extracts JSON values as JSON:
    /// - MySQL: `json -> jsonpath` to `json` (unimplemented)
    /// - PostgreSQL: `json[b] -> {text,integer}` to `json[b]`
    Arrow1,

    /// `->>`
    ///
    /// This extracts JSON values and applies a transformation:
    /// - MySQL: `json ->> jsonpath` to unquoted `text` (unimplemented)
    /// - PostgreSQL: `json[b] ->> {text,integer}` to `text`
    Arrow2,

    /// PostgreSQL `#>`
    ///
    /// This extracts JSON values as JSON: `json[b] #> text[]` to `json[b]`.
    HashArrow1,

    /// PostgreSQL `#>>`
    ///
    /// This extracts JSON values as JSON: `json[b] #>> text[]` to `text`.
    HashArrow2,

    /// `@>`
    ///
    /// Postgres-specific JSONB operator. Takes two JSONB values and determines whether the
    /// left-side values immediately contain all of the right-side values.
    AtArrowRight,

    /// `<@`
    ///
    /// Postgres-specific JSONB operator. Behaves like [`BinaryOperator::AtArrowRight`] with
    /// switched sides for the operands.
    AtArrowLeft,
}

impl BinaryOperator {
    /// Returns true if this operator represents an ordered comparison
    pub fn is_ordering_comparison(&self) -> bool {
        use BinaryOperator::*;
        matches!(self, Greater | GreaterOrEqual | Less | LessOrEqual)
    }

    /// If this operator is an ordered comparison, invert its meaning. (i.e. Greater becomes
    /// Less)
    pub fn flip_ordering_comparison(self) -> Result<Self, Self> {
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
            Self::And => "AND",
            Self::Or => "OR",
            Self::Like => "LIKE",
            Self::NotLike => "NOT LIKE",
            Self::ILike => "ILIKE",
            Self::NotILike => "NOT ILIKE",
            Self::Equal => "=",
            Self::NotEqual => "!=",
            Self::Greater => ">",
            Self::GreaterOrEqual => ">=",
            Self::Less => "<",
            Self::LessOrEqual => "<=",
            Self::Is => "IS",
            Self::IsNot => "IS NOT",
            Self::Add => "+",
            Self::Subtract => "-",
            Self::HashSubtract => "#-",
            Self::Multiply => "*",
            Self::Divide => "/",
            Self::QuestionMark => "?",
            Self::QuestionMarkPipe => "?|",
            Self::QuestionMarkAnd => "?&",
            Self::DoublePipe => "||",
            Self::Arrow1 => "->",
            Self::Arrow2 => "->>",
            Self::HashArrow1 => "#>",
            Self::HashArrow2 => "#>>",
            Self::AtArrowRight => "@>",
            Self::AtArrowLeft => "<@",
        };
        f.write_str(op)
    }
}

#[derive(
    Clone, Copy, Debug, Eq, Hash, PartialEq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
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
#[derive(
    Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Serialize, Deserialize, From, Arbitrary,
)]
pub enum InValue {
    Subquery(Box<SelectStatement>),
    List(Vec<Expr>),
}

impl DialectDisplay for InValue {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + Copy + '_ {
        fmt_with(move |f| match self {
            InValue::Subquery(stmt) => write!(f, "{}", stmt.display(dialect)),
            InValue::List(exprs) => write!(
                f,
                "{}",
                exprs.iter().map(|expr| expr.display(dialect)).join(", ")
            ),
        })
    }
}

/// A single branch of a `CASE WHEN` statement
#[derive(
    Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Serialize, Deserialize, From, Arbitrary,
)]
pub struct CaseWhenBranch {
    pub condition: Expr,
    pub body: Expr,
}

impl DialectDisplay for CaseWhenBranch {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + Copy + '_ {
        fmt_with(move |f| {
            write!(
                f,
                "WHEN {} THEN {}",
                self.condition.display(dialect),
                self.body.display(dialect)
            )
        })
    }
}

/// SQL Expression AST
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Serialize, Deserialize, From)]
pub enum Expr {
    /// Function call expressions
    ///
    /// TODO(aspen): Eventually, the members of FunctionExpr should be inlined here
    Call(FunctionExpr),

    /// Literal values
    Literal(Literal),

    /// Binary operator
    BinaryOp {
        lhs: Box<Expr>,
        op: BinaryOperator,
        rhs: Box<Expr>,
    },

    /// `<expr> <op> ANY (<expr>)` ([PostgreSQL docs][pg-docs])
    ///
    /// [pg-docs]: https://www.postgresql.org/docs/current/functions-comparisons.html#id-1.5.8.30.16
    #[from(ignore)]
    OpAny {
        lhs: Box<Expr>,
        op: BinaryOperator,
        rhs: Box<Expr>,
    },

    /// `<expr> <op> SOME (<expr>)` ([PostgreSQL docs][pg-docs])
    ///
    /// [pg-docs]: https://www.postgresql.org/docs/current/functions-comparisons.html#id-1.5.8.30.16
    #[from(ignore)]
    OpSome {
        lhs: Box<Expr>,
        op: BinaryOperator,
        rhs: Box<Expr>,
    },

    /// `<expr> <op> ALL (<expr>)` ([PostgreSQL docs][pg-docs])
    ///
    /// [pg-docs]: https://www.postgresql.org/docs/current/functions-comparisons.html#id-1.5.8.30.16
    #[from(ignore)]
    OpAll {
        lhs: Box<Expr>,
        op: BinaryOperator,
        rhs: Box<Expr>,
    },

    /// Unary operator
    UnaryOp { op: UnaryOperator, rhs: Box<Expr> },

    /// CASE (WHEN condition THEN then_expr)... ELSE else_expr
    CaseWhen {
        branches: Vec<CaseWhenBranch>,
        else_expr: Option<Box<Expr>>,
    },

    /// A reference to a column
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

    /// `ARRAY[expr1, expr2, ...]`
    Array(Vec<Expr>),

    /// `ROW` constructor: `ROW(expr1, expr2, ...)` or `(expr1, expr2, ...)`
    Row {
        /// Is the `ROW` keyword explicit?
        explicit: bool,
        exprs: Vec<Expr>,
    },

    /// A variable reference
    Variable(Variable),
}

impl DialectDisplay for Expr {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + Copy + '_ {
        fmt_with(move |f| match self {
            Expr::Call(fe) => write!(f, "{}", fe.display(dialect)),
            Expr::Literal(l) => write!(f, "{}", l.display(dialect)),
            Expr::Column(col) => write!(f, "{}", col.display(dialect)),
            Expr::CaseWhen {
                branches,
                else_expr,
            } => {
                write!(f, "CASE ")?;
                for branch in branches {
                    write!(f, "{} ", branch.display(dialect))?;
                }
                if let Some(else_expr) = else_expr {
                    write!(f, "ELSE {} ", else_expr.display(dialect))?;
                }
                write!(f, "END")
            }
            Expr::BinaryOp { lhs, op, rhs } => write!(
                f,
                "({} {op} {})",
                lhs.display(dialect),
                rhs.display(dialect)
            ),
            Expr::OpAny { lhs, op, rhs } => write!(
                f,
                "{} {op} ANY ({})",
                lhs.display(dialect),
                rhs.display(dialect)
            ),
            Expr::OpSome { lhs, op, rhs } => write!(
                f,
                "{} {op} SOME ({})",
                lhs.display(dialect),
                rhs.display(dialect)
            ),
            Expr::OpAll { lhs, op, rhs } => write!(
                f,
                "{} {op} ALL ({})",
                lhs.display(dialect),
                rhs.display(dialect)
            ),
            Expr::UnaryOp {
                op: UnaryOperator::Neg,
                rhs,
            } => write!(f, "(-{})", rhs.display(dialect)),
            Expr::UnaryOp { op, rhs } => write!(f, "({op} {})", rhs.display(dialect)),
            Expr::Exists(statement) => write!(f, "EXISTS ({})", statement.display(dialect)),

            Expr::Between {
                operand,
                min,
                max,
                negated,
            } => {
                write!(
                    f,
                    "{} {}BETWEEN {} AND {}",
                    operand.display(dialect),
                    if *negated { "NOT " } else { "" },
                    min.display(dialect),
                    max.display(dialect)
                )
            }
            Expr::In { lhs, rhs, negated } => {
                write!(f, "{}", lhs.display(dialect))?;
                if *negated {
                    write!(f, " NOT")?;
                }
                write!(f, " IN ({})", rhs.display(dialect))
            }
            Expr::NestedSelect(q) => write!(f, "({})", q.display(dialect)),
            Expr::Cast {
                expr,
                ty,
                postgres_style,
            } if *postgres_style => {
                write!(f, "({}::{})", expr.display(dialect), ty.display(dialect))
            }
            Expr::Cast { expr, ty, .. } => write!(
                f,
                "CAST({} as {})",
                expr.display(dialect),
                ty.display(dialect)
            ),
            Expr::Array(exprs) => {
                fn write_value(
                    expr: &Expr,
                    dialect: Dialect,
                    f: &mut fmt::Formatter,
                ) -> fmt::Result {
                    match expr {
                        Expr::Array(elems) => {
                            write!(f, "[")?;
                            for (i, elem) in elems.iter().enumerate() {
                                if i != 0 {
                                    write!(f, ",")?;
                                }
                                write_value(elem, dialect, f)?;
                            }
                            write!(f, "]")
                        }
                        _ => write!(f, "{}", expr.display(dialect)),
                    }
                }

                write!(f, "ARRAY[")?;
                for (i, expr) in exprs.iter().enumerate() {
                    if i != 0 {
                        write!(f, ",")?;
                    }
                    write_value(expr, dialect, f)?;
                }
                write!(f, "]")
            }
            Expr::Row { explicit, exprs } => {
                if *explicit {
                    write!(f, "ROW")?;
                }
                write!(f, "(")?;
                for (i, expr) in exprs.iter().enumerate() {
                    if i != 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", expr.display(dialect))?;
                }
                write!(f, ")")
            }
            Expr::Variable(var) => write!(f, "{}", var.display(dialect)),
        })
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

    /// Functions similarly to mem::take, replacing the argument with a meaningless placeholder and
    /// returning the value from the method, thus providing a way to move ownership more easily.
    pub fn take(&mut self) -> Self {
        // If Expr implemented Default we could use mem::take directly for this purpose, but we
        // decided that it felt semantically weird and arbitrary to have a Default implementation
        // for Expr that returned a null literal, since there isn't really such a thing as a
        // "default" expression.
        mem::replace(self, Expr::Literal(Literal::Null))
    }
}

impl Arbitrary for Expr {
    type Parameters = ();

    type Strategy = BoxedStrategy<Expr>;

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        use proptest::option;
        use proptest::prelude::*;

        prop_oneof![
            any::<Literal>().prop_map(Expr::Literal),
            any::<Column>().prop_map(Expr::Column),
            any::<Variable>().prop_map(Expr::Variable),
        ]
        .prop_recursive(4, 8, 4, |element| {
            let box_expr = element.clone().prop_map(Box::new);
            prop_oneof![
                prop_oneof![
                    (box_expr.clone(), any::<bool>())
                        .prop_map(|(expr, distinct)| FunctionExpr::Avg { expr, distinct }),
                    (box_expr.clone(), any::<bool>())
                        .prop_map(|(expr, distinct)| FunctionExpr::Count { expr, distinct }),
                    Just(FunctionExpr::CountStar),
                    (box_expr.clone(), any::<bool>())
                        .prop_map(|(expr, distinct)| FunctionExpr::Sum { expr, distinct }),
                    box_expr.clone().prop_map(FunctionExpr::Max),
                    box_expr.clone().prop_map(FunctionExpr::Min),
                    (box_expr.clone(), any::<Option<String>>()).prop_map(|(expr, separator)| {
                        FunctionExpr::GroupConcat { expr, separator }
                    }),
                    (
                        box_expr.clone(),
                        option::of(box_expr.clone()),
                        option::of(box_expr.clone())
                    )
                        .prop_map(|(string, pos, len)| {
                            FunctionExpr::Substring { string, pos, len }
                        }),
                    (
                        any::<SqlIdentifier>(),
                        proptest::collection::vec(element.clone(), 0..24)
                    )
                        .prop_map(|(name, arguments)| FunctionExpr::Call { name, arguments })
                ]
                .prop_map(Expr::Call),
                (box_expr.clone(), any::<BinaryOperator>(), box_expr.clone(),)
                    .prop_map(|(lhs, op, rhs)| Expr::BinaryOp { lhs, op, rhs },),
                (box_expr.clone(), any::<BinaryOperator>(), box_expr.clone(),)
                    .prop_map(|(lhs, op, rhs)| Expr::OpAny { lhs, op, rhs },),
                (box_expr.clone(), any::<BinaryOperator>(), box_expr.clone(),)
                    .prop_map(|(lhs, op, rhs)| Expr::OpSome { lhs, op, rhs },),
                (box_expr.clone(), any::<BinaryOperator>(), box_expr.clone(),)
                    .prop_map(|(lhs, op, rhs)| Expr::OpAll { lhs, op, rhs },),
                (any::<UnaryOperator>(), box_expr.clone(),)
                    .prop_map(|(op, rhs)| Expr::UnaryOp { op, rhs },),
                (
                    proptest::collection::vec(
                        (element.clone(), element.clone())
                            .prop_map(|(condition, body)| CaseWhenBranch { condition, body }),
                        1..24
                    ),
                    option::of(box_expr.clone())
                )
                    .prop_map(|(branches, else_expr)| Expr::CaseWhen {
                        branches,
                        else_expr
                    }),
                (
                    box_expr.clone(),
                    box_expr.clone(),
                    box_expr.clone(),
                    any::<bool>(),
                )
                    .prop_map(|(operand, min, max, negated)| Expr::Between {
                        operand,
                        min,
                        max,
                        negated
                    }),
                (
                    box_expr.clone(),
                    /* TODO: IN (subquery) */
                    proptest::collection::vec(element.clone(), 1..24).prop_map(InValue::List),
                    any::<bool>(),
                )
                    .prop_map(|(lhs, rhs, negated)| Expr::In { lhs, rhs, negated }),
                (box_expr, any::<SqlType>(), any::<bool>()).prop_map(
                    |(expr, ty, postgres_style)| {
                        Expr::Cast {
                            expr,
                            ty,
                            postgres_style,
                        }
                    }
                ),
                proptest::collection::vec(element, 0..24).prop_map(Expr::Array),
                // TODO: once we have Arbitrary for SelectStatement
                // any::<Box<SelectStatement>>().prop_map(Expr::NestedSelect),
                // any::<Box<SelectStatement>>().prop_map(Expr::Exists),
            ]
        })
        .boxed()
    }
}

/// Suffixes which can be supplied to operators to convert them into predicates on arrays or
/// subqueries.
///
/// Used for support of `<expr> <op> ANY ...`, `<expr> <op> SOME ...`, and `<expr> <op> ALL ...`
#[derive(Debug, Clone, Copy)]
enum OperatorSuffix {
    Any,
    Some,
    All,
}

fn operator_suffix(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], OperatorSuffix> {
    alt((
        value(OperatorSuffix::Any, tag_no_case("any")),
        value(OperatorSuffix::Some, tag_no_case("some")),
        value(OperatorSuffix::All, tag_no_case("all")),
    ))(i)
}

/// A lexed sequence of tokens containing expressions and operators, pre-interpretation of operator
/// precedence but post-interpretation of parentheses
#[derive(Debug, Clone)]
enum TokenTree {
    Infix(BinaryOperator),
    In,
    NotIn,
    Prefix(UnaryOperator),
    Primary(Expr),
    Group(Vec<TokenTree>),
    PgsqlCast(Box<TokenTree>, SqlType),
    OpSuffix(
        Box<TokenTree>,
        BinaryOperator,
        OperatorSuffix,
        Box<TokenTree>,
    ),
}

// no_and_or variants of  `binary_operator`, `infix`, `rest`, and `token_tree` allow parsing (binary
// op) expressions in the right-hand side of a BETWEEN, eg:
//     foo between (1 + 2) and 3 + 5
// should parse the same as:
//     foo between (1 + 2) and (3 + 5)
// , but:
//     foo between (1 + 2) and 8 and bar
// should parse the same as:
//     (foo between (1 + 2) and 8) and bar

fn binary_operator_no_and_or(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], BinaryOperator> {
    alt((
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
        move |i| {
            let (i, _) = tag_no_case("is")(i)?;
            let (i, _) = whitespace1(i)?;
            let (i, _) = tag_no_case("not")(i)?;
            let (i, _) = whitespace1(i)?;

            Ok((i, BinaryOperator::IsNot))
        },
        map(pair(tag_no_case("is"), whitespace1), |_| BinaryOperator::Is),
        // Sigils are separated due to `alt` limit.
        //
        // NOTE: The order here matters or else some of these will be incorrectly partially parsed,
        // such as `?` after `?|`.
        alt((
            map(tag("@>"), |_| BinaryOperator::AtArrowRight),
            map(tag("<@"), |_| BinaryOperator::AtArrowLeft),
            map(char('='), |_| BinaryOperator::Equal),
            map(tag("!="), |_| BinaryOperator::NotEqual),
            map(tag("<>"), |_| BinaryOperator::NotEqual),
            map(tag(">="), |_| BinaryOperator::GreaterOrEqual),
            map(tag("<="), |_| BinaryOperator::LessOrEqual),
            map(char('>'), |_| BinaryOperator::Greater),
            map(char('<'), |_| BinaryOperator::Less),
            map(char('+'), |_| BinaryOperator::Add),
            map(tag("->>"), |_| BinaryOperator::Arrow2),
            map(tag("->"), |_| BinaryOperator::Arrow1),
            map(char('-'), |_| BinaryOperator::Subtract),
            map(char('*'), |_| BinaryOperator::Multiply),
            map(char('/'), |_| BinaryOperator::Divide),
            map(tag("?|"), |_| BinaryOperator::QuestionMarkPipe),
            map(tag("?&"), |_| BinaryOperator::QuestionMarkAnd),
            map(char('?'), |_| BinaryOperator::QuestionMark),
            map(tag("||"), |_| BinaryOperator::DoublePipe),
            map(tag("#>>"), |_| BinaryOperator::HashArrow2),
            map(tag("#>"), |_| BinaryOperator::HashArrow1),
        )),
        map(tag("#-"), |_| BinaryOperator::HashSubtract),
    ))(i)
}

fn in_or_not_in(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], TokenTree> {
    let (i, res) = alt((value(TokenTree::In, tag_no_case("in")), |i| {
        let (i, _) = tag_no_case("not")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, _) = tag_no_case("in")(i)?;
        Ok((i, TokenTree::NotIn))
    }))(i)?;
    let (i, _) = peek(preceded(whitespace0, tag("(")))(i)?;

    Ok((i, res))
}

fn infix_no_and_or(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], TokenTree> {
    alt((
        in_or_not_in,
        map(binary_operator_no_and_or, TokenTree::Infix),
    ))(i)
}

fn binary_operator(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], BinaryOperator> {
    complete(alt((
        map(terminated(tag_no_case("and"), whitespace1), |_| {
            BinaryOperator::And
        }),
        map(terminated(tag_no_case("or"), whitespace1), |_| {
            BinaryOperator::Or
        }),
        binary_operator_no_and_or,
    )))(i)
}

fn infix(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], TokenTree> {
    alt((in_or_not_in, map(binary_operator, TokenTree::Infix)))(i)
}

fn prefix(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], TokenTree> {
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

fn primary_inner(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], TokenTree> {
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

fn primary(dialect: Dialect) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], TokenTree> {
    struct OpSuffix(BinaryOperator, OperatorSuffix, Box<TokenTree>);

    move |i| {
        let (i, lhs) = primary_inner(dialect)(i)?;
        let (i, cast) = opt(move |i| {
            let (i, _) = whitespace0(i)?;
            let (i, _) = tag("::")(i)?;
            let (i, _) = whitespace0(i)?;
            type_identifier(dialect)(i)
        })(i)?;
        let (i, suffix) = opt(move |i| {
            let (i, _): (LocatedSpan<&[u8]>, _) = whitespace0(i)?;
            let (i, op) = binary_operator(i)?;
            let (i, _) = whitespace0(i)?;
            let (i, suffix) = operator_suffix(i)?;
            let (i, _) = whitespace0(i)?;
            let (i, _) = tag("(")(i)?;
            let (i, _) = whitespace0(i)?;
            let (i, rhs) = token_tree(dialect)(i)?;
            let (i, _) = whitespace0(i)?;
            let (i, _) = tag(")")(i)?;

            Ok((i, OpSuffix(op, suffix, Box::new(TokenTree::Group(rhs)))))
        })(i)?;

        let lhs = if let Some(ty) = cast {
            TokenTree::PgsqlCast(Box::new(lhs), ty)
        } else {
            lhs
        };
        Ok((
            i,
            match suffix {
                None => lhs,
                Some(OpSuffix(op, suffix, rhs)) => {
                    TokenTree::OpSuffix(Box::new(lhs), op, suffix, rhs)
                }
            },
        ))
    }
}

fn rest(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Vec<(TokenTree, Vec<TokenTree>, TokenTree)>>
{
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

fn token_tree(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Vec<TokenTree>> {
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
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Vec<(TokenTree, Vec<TokenTree>, TokenTree)>>
{
    move |i| {
        many0(tuple((
            preceded(whitespace0, infix_no_and_or),
            delimited(whitespace0, many0(prefix), whitespace0),
            primary(dialect),
        )))(i)
    }
}

fn token_tree_no_and_or(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Vec<TokenTree>> {
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
        //
        // Though, note that we currently use this same parser for Postgres, so we have to fudge
        // the precedence rules a bit sometimes (particularly with PG-specific operators like `?`).
        // In the future we'll probably want to switch to using a separate parser (or at least a
        // separate precedence/associativity table) per SQL dialect that we support but for now
        // this seems to be good enough.
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
            In | NotIn => Affix::Infix(Precedence(7), Associativity::Right),
            Infix(Add) => Affix::Infix(Precedence(11), Associativity::Right),
            Infix(Subtract) => Affix::Infix(Precedence(11), Associativity::Right),
            Infix(Multiply) => Affix::Infix(Precedence(12), Associativity::Right),
            Infix(Divide) => Affix::Infix(Precedence(12), Associativity::Right),
            Prefix(Not) => Affix::Prefix(Precedence(6)),
            Prefix(Neg) => Affix::Prefix(Precedence(5)),
            Primary(_) => Affix::Nilfix,
            Group(_) => Affix::Nilfix,
            PgsqlCast(..) => Affix::Nilfix,
            OpSuffix(..) => Affix::Nilfix,

            // All JSON operators have the same precedence.
            //
            // Not positive whether this 8 puts all other operators at the correct relative
            // precedence here because these precedences come from MySQL rather than PG, but this
            // seems to produce the correct behavior in all the cases I've come up with:
            Infix(QuestionMark) => Affix::Infix(Precedence(8), Associativity::Left),
            Infix(QuestionMarkPipe) => Affix::Infix(Precedence(8), Associativity::Left),
            Infix(QuestionMarkAnd) => Affix::Infix(Precedence(8), Associativity::Left),
            Infix(DoublePipe) => Affix::Infix(Precedence(8), Associativity::Left),
            Infix(Arrow1) => Affix::Infix(Precedence(8), Associativity::Left),
            Infix(Arrow2) => Affix::Infix(Precedence(8), Associativity::Left),
            Infix(HashArrow1) => Affix::Infix(Precedence(8), Associativity::Left),
            Infix(HashArrow2) => Affix::Infix(Precedence(8), Associativity::Left),
            Infix(AtArrowRight) => Affix::Infix(Precedence(8), Associativity::Left),
            Infix(AtArrowLeft) => Affix::Infix(Precedence(8), Associativity::Left),
            Infix(HashSubtract) => Affix::Infix(Precedence(8), Associativity::Left),
        })
    }

    fn primary(&mut self, input: Self::Input) -> Result<Self::Output, Self::Error> {
        use TokenTree::*;
        Ok(match input {
            Primary(expr) => expr,
            // unwrap: ok because there are no errors possible
            Group(group) => self.parse(&mut group.into_iter()).unwrap(),
            PgsqlCast(box expr, ty) => {
                let tt = self.parse(&mut iter::once(expr)).unwrap();
                Expr::Cast {
                    expr: tt.into(),
                    ty,
                    postgres_style: true,
                }
            }
            OpSuffix(box lhs, op, suffix, box rhs) => {
                let lhs = Box::new(self.parse(&mut iter::once(lhs)).unwrap());
                let rhs = Box::new(self.parse(&mut iter::once(rhs)).unwrap());
                match suffix {
                    OperatorSuffix::Any => Expr::OpAny { lhs, op, rhs },
                    OperatorSuffix::Some => Expr::OpSome { lhs, op, rhs },
                    OperatorSuffix::All => Expr::OpAll { lhs, op, rhs },
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
            TokenTree::In | TokenTree::NotIn => {
                let rhs = match rhs {
                    Expr::NestedSelect(stmt) => InValue::Subquery(stmt),
                    // We checked for an opening paren when parsing IN, so this *must* be `lhs IN
                    // (ROW(...))`
                    expr @ Expr::Row { explicit: true, .. } => InValue::List(vec![expr]),
                    // This is either
                    //   `lhs IN (1, 2, ...)`
                    // or
                    //   `expr IN ((1, 2, ...))`
                    //
                    // The latter case we handle incorrectly right now (TODO) because we don't
                    // support row exprs.
                    Expr::Row {
                        explicit: false,
                        exprs,
                    } => InValue::List(exprs),
                    // We checked for an opening paren when parsing IN, so this *must* be `lhs IN
                    // (expr)`
                    expr => InValue::List(vec![expr]),
                };

                return Ok(Expr::In {
                    lhs: Box::new(lhs),
                    rhs,
                    negated: matches!(op, TokenTree::NotIn),
                });
            }
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

fn between_operand(dialect: Dialect) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Expr> {
    move |i| {
        alt((
            parenthesized_expr(dialect),
            map(function_expr(dialect), Expr::Call),
            map(literal(dialect), Expr::Literal),
            case_when_expr(dialect),
            map(column_identifier_no_alias(dialect), Expr::Column),
        ))(i)
    }
}

fn between_max(dialect: Dialect) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Expr> {
    move |i| {
        alt((
            map(token_tree_no_and_or(dialect), |tt| {
                ExprParser.parse(&mut tt.into_iter()).unwrap()
            }),
            simple_expr(dialect),
        ))(i)
    }
}

fn between_expr(dialect: Dialect) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Expr> {
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

fn exists_expr(dialect: Dialect) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Expr> {
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

fn case_when_branch(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], CaseWhenBranch> {
    move |i| {
        let (i, _) = tag_no_case("when")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, condition) = expression(dialect)(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, _) = tag_no_case("then")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, then) = expression(dialect)(i)?;

        Ok((
            i,
            CaseWhenBranch {
                condition,
                body: then,
            },
        ))
    }
}

fn case_when_else(dialect: Dialect) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Expr> {
    move |i| {
        let (i, _) = tag_no_case("else")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, else_expr) = expression(dialect)(i)?;
        let (i, _) = whitespace1(i)?;
        Ok((i, else_expr))
    }
}

fn case_when_expr(dialect: Dialect) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Expr> {
    move |i| {
        let (i, _) = tag_no_case("case")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, branches) = many1(terminated(case_when_branch(dialect), whitespace1))(i)?;
        let (i, else_expr) = opt(map(case_when_else(dialect), Box::new))(i)?;
        let (i, _) = tag_no_case("end")(i)?;
        Ok((
            i,
            Expr::CaseWhen {
                branches,
                else_expr,
            },
        ))
    }
}

fn cast(dialect: Dialect) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Expr> {
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

fn nested_select(dialect: Dialect) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Expr> {
    move |i| {
        let (i, _) = char('(')(i)?;
        let (i, _) = whitespace0(i)?;
        let (i, statement) = nested_selection(dialect)(i)?;
        let (i, _) = whitespace0(i)?;
        let (i, _) = char(')')(i)?;

        Ok((i, Expr::NestedSelect(Box::new(statement))))
    }
}

fn parenthesized_expr(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Expr> {
    move |i| {
        let (i, _) = char('(')(i)?;
        let (i, _) = whitespace0(i)?;
        let (i, expr) = expression(dialect)(i)?;
        let (i, _) = whitespace0(i)?;
        let (i, _) = char(')')(i)?;

        Ok((i, expr))
    }
}

pub(crate) fn scoped_var(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Variable> {
    move |i| {
        let (i, scope) = variable_scope_prefix(i)?;
        let (i, name) = dialect
            .identifier()
            .map(|ident| ident.to_ascii_lowercase().into())
            .parse(i)?;

        Ok((i, Variable { scope, name }))
    }
}

fn bracketed_expr_list(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Vec<Expr>> {
    move |i| {
        let (i, _) = tag("[")(i)?;
        let (i, exprs) = separated_list0(
            ws_sep_comma,
            alt((
                expression(dialect),
                map(bracketed_expr_list(dialect), Expr::Array),
            )),
        )(i)?;
        let (i, _) = tag("]")(i)?;
        Ok((i, exprs))
    }
}

fn array_expr(dialect: Dialect) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Expr> {
    move |i| {
        let (i, _) = tag_no_case("ARRAY")(i)?;
        let (i, _) = whitespace0(i)?;
        let (i, exprs) = bracketed_expr_list(dialect)(i)?;
        Ok((i, Expr::Array(exprs)))
    }
}

fn row_expr_explicit(dialect: Dialect) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Expr> {
    move |i| {
        let (i, _) = tag_no_case("row")(i)?;
        let (i, _) = whitespace0(i)?;
        let (i, _) = tag("(")(i)?;
        let (i, exprs) = separated_list1(
            ws_sep_comma,
            alt((
                expression(dialect),
                map(bracketed_expr_list(dialect), Expr::Array),
            )),
        )(i)?;
        let (i, _) = tag(")")(i)?;

        Ok((
            i,
            Expr::Row {
                explicit: true,
                exprs,
            },
        ))
    }
}

fn row_expr_implicit(dialect: Dialect) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Expr> {
    move |i| {
        // Implicit row exprs must have two or more elements
        let (i, _) = tag("(")(i)?;
        let (i, first_expr) = expression(dialect)(i)?;
        let (i, _) = ws_sep_comma(i)?;
        let (i, rest_exprs) = separated_list1(
            ws_sep_comma,
            alt((
                expression(dialect),
                map(bracketed_expr_list(dialect), Expr::Array),
            )),
        )(i)?;
        let (i, _) = tag(")")(i)?;

        let mut exprs = Vec::with_capacity(rest_exprs.len() + 1);
        exprs.push(first_expr);
        exprs.extend(rest_exprs);

        Ok((
            i,
            Expr::Row {
                explicit: false,
                exprs,
            },
        ))
    }
}

// Expressions without (binary or unary) operators
pub(crate) fn simple_expr(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Expr> {
    move |i| {
        alt((
            parenthesized_expr(dialect),
            nested_select(dialect),
            exists_expr(dialect),
            between_expr(dialect),
            row_expr_explicit(dialect),
            row_expr_implicit(dialect),
            map(function_expr(dialect), Expr::Call),
            map(literal(dialect), Expr::Literal),
            case_when_expr(dialect),
            array_expr(dialect),
            map(column_identifier_no_alias(dialect), Expr::Column),
            cast(dialect),
            map(scoped_var(dialect), Expr::Variable),
        ))(i)
    }
}

pub(crate) fn expression(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Expr> {
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
    use test_strategy::proptest;

    use super::*;
    use crate::{to_nom_result, Relation};

    #[test]
    fn column_then_column() {
        let (rem, res) =
            to_nom_result(expression(Dialect::MySQL)(LocatedSpan::new(b"x y"))).unwrap();
        assert_eq!(res, Expr::Column("x".into()));
        assert_eq!(rem, b" y");
    }

    #[test]
    fn cast_less_than_all_binary_op() {
        let res = test_parse!(expression(Dialect::PostgreSQL), b"'a'::abc < all('{b,c}')");
        assert_eq!(
            res,
            Expr::OpAll {
                lhs: Box::new(Expr::Cast {
                    expr: Box::new(Expr::Literal(Literal::String("a".to_string()))),
                    ty: SqlType::Other(Relation {
                        schema: None,
                        name: "abc".into(),
                    }),
                    postgres_style: true,
                }),
                op: BinaryOperator::Less,
                rhs: Box::new(Expr::Literal(Literal::String("{b,c}".to_string()))),
            }
        )
    }

    #[proptest]
    fn arbitrary_expr_doesnt_stack_overflow(_expr: Expr) {}

    #[test]
    fn in_lhs_parens() {
        let res = test_parse!(expression(Dialect::MySQL), b"(x + 1) IN (2)");
        assert_eq!(
            res,
            Expr::In {
                lhs: Box::new(Expr::BinaryOp {
                    lhs: Box::new(Expr::Column("x".into())),
                    op: BinaryOperator::Add,
                    rhs: Box::new(Expr::Literal(1.into()))
                }),
                rhs: InValue::List(vec![Expr::Literal(2.into())]),
                negated: false
            }
        );
    }

    pub mod precedence {
        use super::*;

        pub fn parses_same(dialect: Dialect, implicit: &str, explicit: &str) {
            let implicit_res = expression(dialect)(LocatedSpan::new(implicit.as_bytes()))
                .unwrap()
                .1;
            let explicit_res = expression(dialect)(LocatedSpan::new(explicit.as_bytes()))
                .unwrap()
                .1;
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

        #[test]
        fn plus_and_in() {
            parses_same(Dialect::MySQL, "x + 1 in (2)", "((x + 1) in (2))");
        }
    }

    pub mod cast {
        use super::*;

        #[test]
        fn postgres_cast() {
            let res = expression(Dialect::PostgreSQL)(LocatedSpan::new(br#"-128::INTEGER"#));
            assert_eq!(
                res.unwrap().1,
                Expr::UnaryOp {
                    op: UnaryOperator::Neg,
                    rhs: Box::new(Expr::Cast {
                        expr: Box::new(Expr::Literal(Literal::Integer(128))),
                        ty: SqlType::Int(None),
                        postgres_style: true
                    }),
                }
            );

            let res = expression(Dialect::PostgreSQL)(LocatedSpan::new(br#"515*128::TEXT"#));
            assert_eq!(
                res.unwrap().1,
                Expr::BinaryOp {
                    op: BinaryOperator::Multiply,
                    lhs: Box::new(Expr::Literal(Literal::Integer(515))),
                    rhs: Box::new(Expr::Cast {
                        expr: Box::new(Expr::Literal(Literal::Integer(128))),
                        ty: SqlType::Text,
                        postgres_style: true
                    }),
                }
            );

            let res = expression(Dialect::PostgreSQL)(LocatedSpan::new(br#"(515*128)::TEXT"#));
            assert_eq!(
                res.unwrap().1,
                Expr::Cast {
                    expr: Box::new(Expr::BinaryOp {
                        op: BinaryOperator::Multiply,
                        lhs: Box::new(Expr::Literal(Literal::Integer(515))),
                        rhs: Box::new(Expr::Literal(Literal::Integer(128)))
                    }),
                    ty: SqlType::Text,
                    postgres_style: true,
                },
            );

            let res = expression(Dialect::PostgreSQL)(LocatedSpan::new(
                br#"200*postgres.column::DOUBLE PRECISION"#,
            ));
            assert_eq!(
                res.unwrap().1,
                Expr::BinaryOp {
                    op: BinaryOperator::Multiply,
                    lhs: Box::new(Expr::Literal(Literal::Integer(200))),
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
            let res = expression(Dialect::MySQL)(LocatedSpan::new(br#"CAST(-128 AS UNSIGNED)"#));
            assert_eq!(
                res.unwrap().1,
                Expr::Cast {
                    expr: Box::new(Expr::UnaryOp {
                        op: UnaryOperator::Neg,
                        rhs: Box::new(Expr::Literal(Literal::Integer(128))),
                    }),
                    ty: SqlType::UnsignedBigInt(None),
                    postgres_style: false
                }
            );
        }
    }

    mod conditions {
        use super::*;
        use crate::{to_nom_result, FieldDefinitionExpr, ItemPlaceholder, TableExpr};

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
            let res = expression(Dialect::MySQL)(LocatedSpan::new(cond.as_bytes()));
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

            let res = expression(Dialect::MySQL)(LocatedSpan::new(cond.as_bytes()));
            assert_eq!(
                res.unwrap().1,
                x_operator_value(BinaryOperator::Add, 3.into())
            );
        }

        #[test]
        fn simple_arithmetic_expression_with_parenthesis() {
            let cond = "( x - 2 )";

            let res = expression(Dialect::MySQL)(LocatedSpan::new(cond.as_bytes()));
            assert_eq!(
                res.unwrap().1,
                x_operator_value(BinaryOperator::Subtract, 2.into())
            );
        }

        #[test]
        fn parenthetical_arithmetic_expression() {
            let cond = "( x * 5 )";

            let res = expression(Dialect::MySQL)(LocatedSpan::new(cond.as_bytes()));
            assert_eq!(
                res.unwrap().1,
                x_operator_value(BinaryOperator::Multiply, 5.into())
            );
        }

        #[test]
        fn expression_with_arithmetics() {
            let cond = "x * 3 = 21";

            let res = expression(Dialect::MySQL)(LocatedSpan::new(cond.as_bytes()));
            assert_eq!(
                res.unwrap().1,
                Expr::BinaryOp {
                    op: BinaryOperator::Equal,
                    lhs: Box::new(x_operator_value(BinaryOperator::Multiply, 3.into())),
                    rhs: Box::new(Expr::Literal(21.into()))
                }
            );
        }
        #[test]
        fn expression_with_arithmetics_and_parenthesis() {
            let cond = "(x - 7 = 15)";

            let res = expression(Dialect::MySQL)(LocatedSpan::new(cond.as_bytes()));
            assert_eq!(
                res.unwrap().1,
                Expr::BinaryOp {
                    op: BinaryOperator::Equal,
                    lhs: Box::new(x_operator_value(BinaryOperator::Subtract, 7.into())),
                    rhs: Box::new(Expr::Literal(15.into()))
                }
            );
        }

        #[test]
        fn expression_with_arithmetics_in_parenthesis() {
            let cond = "( x + 2) = 15";

            let res = expression(Dialect::MySQL)(LocatedSpan::new(cond.as_bytes()));
            assert_eq!(
                res.unwrap().1,
                Expr::BinaryOp {
                    op: BinaryOperator::Equal,
                    lhs: Box::new(x_operator_value(BinaryOperator::Add, 2.into())),
                    rhs: Box::new(Expr::Literal(15.into()))
                }
            );
        }

        #[test]
        fn expression_with_arithmetics_in_parenthesis_in_both_side() {
            let cond = "( x + 2) =(x*3)";

            let res = expression(Dialect::MySQL)(LocatedSpan::new(cond.as_bytes()));
            assert_eq!(
                res.unwrap().1,
                Expr::BinaryOp {
                    op: BinaryOperator::Equal,
                    lhs: Box::new(x_operator_value(BinaryOperator::Add, 2.into())),
                    rhs: Box::new(x_operator_value(BinaryOperator::Multiply, 3.into()))
                }
            );
        }

        #[test]
        fn inequality_literals() {
            let cond1 = "foo >= 42";
            let cond2 = "foo <= 5";

            let res1 = expression(Dialect::MySQL)(LocatedSpan::new(cond1.as_bytes()));
            assert_eq!(
                res1.unwrap().1,
                Expr::BinaryOp {
                    lhs: Box::new(Expr::Column(Column::from("foo"))),
                    op: BinaryOperator::GreaterOrEqual,
                    rhs: Box::new(Expr::Literal(Literal::Integer(42)))
                }
            );

            let res2 = expression(Dialect::MySQL)(LocatedSpan::new(cond2.as_bytes()));
            assert_eq!(
                res2.unwrap().1,
                Expr::BinaryOp {
                    lhs: Box::new(Expr::Column(Column::from("foo"))),
                    op: BinaryOperator::LessOrEqual,
                    rhs: Box::new(Expr::Literal(Literal::Integer(5)))
                }
            );
        }

        #[test]
        fn empty_string_literal() {
            let cond = "foo = ''";

            let res = expression(Dialect::MySQL)(LocatedSpan::new(cond.as_bytes()));
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
                rhs: Box::new(Expr::Literal(Literal::Integer(12))),
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

            let res = expression(Dialect::MySQL)(LocatedSpan::new(cond.as_bytes()));
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
                rhs: Box::new(Expr::Literal(Literal::Integer(12))),
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

            let res = expression(Dialect::MySQL)(LocatedSpan::new(cond.as_bytes()));
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
                    rhs: Box::new(Expr::Literal(Literal::Integer(12))),
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

            let res = expression(Dialect::MySQL)(LocatedSpan::new(cond.as_bytes()));
            assert_eq!(res.unwrap().1, complete);
        }

        #[test]
        fn nested_select() {
            use crate::select::SelectStatement;
            use crate::table::Relation;

            let cond = "bar in (select col from foo)";

            let res = expression(Dialect::MySQL)(LocatedSpan::new(cond.as_bytes()));

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

            let res = expression(Dialect::MySQL)(LocatedSpan::new(cond.as_bytes()));

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

            let res = expression(Dialect::MySQL)(LocatedSpan::new(cond.as_bytes()));

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

            let res = expression(Dialect::MySQL)(LocatedSpan::new(cond.as_bytes()));

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
                rhs: Box::new(Expr::Literal(0.into())),
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

            let res = expression(Dialect::MySQL)(LocatedSpan::new(cond.as_bytes()));

            let expected = Expr::In {
                lhs: Box::new(Expr::Column("bar".into())),
                rhs: InValue::List(vec![Expr::Literal(0.into()), Expr::Literal(1.into())]),
                negated: false,
            };

            assert_eq!(res.unwrap().1, expected);
        }

        #[test]
        fn is_null() {
            let cond = "bar IS NULL";

            let res = expression(Dialect::MySQL)(LocatedSpan::new(cond.as_bytes()));

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

            let res = expression(Dialect::MySQL)(LocatedSpan::new(cond.as_bytes()));
            let expected = Expr::BinaryOp {
                lhs: Box::new(Expr::Column("bar".into())),
                op: BinaryOperator::IsNot,
                rhs: Box::new(Expr::Literal(Literal::Null)),
            };
            assert_eq!(res.unwrap().1, expected);
        }

        #[test]
        fn between_simple() {
            let qs = b"foo between 1 and 2";
            let expected = Expr::Between {
                operand: Box::new(Expr::Column("foo".into())),
                min: Box::new(Expr::Literal(1.into())),
                max: Box::new(Expr::Literal(2.into())),
                negated: false,
            };
            let (remaining, result) =
                to_nom_result(expression(Dialect::MySQL)(LocatedSpan::new(qs))).unwrap();
            assert_eq!(std::str::from_utf8(remaining).unwrap(), "");
            assert_eq!(result, expected);
        }

        #[test]
        fn not_between() {
            let qs = b"foo not between 1 and 2";
            let expected = Expr::Between {
                operand: Box::new(Expr::Column("foo".into())),
                min: Box::new(Expr::Literal(1.into())),
                max: Box::new(Expr::Literal(2.into())),
                negated: true,
            };
            let (remaining, result) =
                to_nom_result(expression(Dialect::MySQL)(LocatedSpan::new(qs))).unwrap();
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
                min: Box::new(Expr::Literal(1.into())),
                max: Box::new(Expr::Literal(2.into())),
                negated: false,
            };
            let (remaining, result) =
                to_nom_result(expression(Dialect::MySQL)(LocatedSpan::new(qs))).unwrap();
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
                    lhs: Box::new(Expr::Literal(Literal::Integer(1))),
                    rhs: Box::new(Expr::Literal(Literal::Integer(2))),
                }),
                max: Box::new(Expr::BinaryOp {
                    op: BinaryOperator::Add,
                    lhs: Box::new(Expr::Literal(Literal::Integer(3))),
                    rhs: Box::new(Expr::Literal(Literal::Integer(5))),
                }),
                negated: false,
            };
            let res = to_nom_result(expression(Dialect::MySQL)(LocatedSpan::new(qs)));
            let (remaining, result) = res.unwrap();
            assert_eq!(std::str::from_utf8(remaining).unwrap(), "");
            eprintln!("{}", result.display(Dialect::MySQL));
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
            let (remaining, result) =
                to_nom_result(expression(Dialect::MySQL)(LocatedSpan::new(qs))).unwrap();
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
            let (remaining, result) =
                to_nom_result(expression(Dialect::MySQL)(LocatedSpan::new(qs))).unwrap();
            assert_eq!(std::str::from_utf8(remaining).unwrap(), "");
            assert_eq!(result, expected);
        }
    }

    mod negation {
        use super::*;
        use crate::to_nom_result;

        #[test]
        fn neg_integer() {
            let qs = b"-256";
            let expected = Expr::UnaryOp {
                op: UnaryOperator::Neg,
                rhs: Box::new(Expr::Literal(Literal::Integer(256))),
            };
            let (remaining, result) =
                to_nom_result(expression(Dialect::MySQL)(LocatedSpan::new(qs))).unwrap();
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
            let (remaining, result) =
                to_nom_result(expression(Dialect::MySQL)(LocatedSpan::new(qs))).unwrap();
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
            let (remaining, result) =
                to_nom_result(expression(Dialect::MySQL)(LocatedSpan::new(qs))).unwrap();
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
                    rhs: Box::new(Expr::Literal(Literal::Integer(1))),
                }),
            };
            let (remaining, result) =
                to_nom_result(expression(Dialect::MySQL)(LocatedSpan::new(qs))).unwrap();
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
                    rhs: Box::new(Expr::Literal(Literal::Integer(1))),
                }),
            };
            let (remaining, result) =
                to_nom_result(expression(Dialect::MySQL)(LocatedSpan::new(qs))).unwrap();
            assert_eq!(std::str::from_utf8(remaining).unwrap(), "");
            assert_eq!(result, expected);
        }
    }

    mod mysql {
        use super::*;

        #[test]
        fn column_beginning_with_null() {
            let res = test_parse!(expression(Dialect::MySQL), b"nullable");
            assert_eq!(
                res,
                Expr::Column(Column {
                    name: "nullable".into(),
                    table: None
                })
            );
        }

        #[test]
        fn not_in_comparison() {
            let qs1 = b"id not in (1,2)";
            let res1 = expression(Dialect::MySQL)(LocatedSpan::new(qs1));

            let c1 = res1.unwrap().1;
            let expected1 = Expr::In {
                lhs: Box::new(Expr::Column("id".into())),
                rhs: InValue::List(vec![Expr::Literal(1.into()), Expr::Literal(2.into())]),
                negated: true,
            };
            assert_eq!(c1, expected1);

            let expected1 = "`id` NOT IN (1, 2)";
            assert_eq!(c1.display(Dialect::MySQL).to_string(), expected1);
        }

        #[test]
        fn case_display() {
            let c1 = Column {
                name: "foo".into(),
                table: None,
            };

            let exp = Expr::CaseWhen {
                branches: vec![CaseWhenBranch {
                    condition: Expr::BinaryOp {
                        op: BinaryOperator::Equal,
                        lhs: Box::new(Expr::Column(c1.clone())),
                        rhs: Box::new(Expr::Literal(Literal::Integer(0))),
                    },
                    body: Expr::Column(c1.clone()),
                }],
                else_expr: Some(Box::new(Expr::Literal(Literal::Integer(1)))),
            };

            assert_eq!(
                exp.display(Dialect::MySQL).to_string(),
                "CASE WHEN (`foo` = 0) THEN `foo` ELSE 1 END"
            );

            let exp_no_else = Expr::CaseWhen {
                branches: vec![CaseWhenBranch {
                    condition: Expr::BinaryOp {
                        op: BinaryOperator::Equal,
                        lhs: Box::new(Expr::Column(c1.clone())),
                        rhs: Box::new(Expr::Literal(Literal::Integer(0))),
                    },
                    body: Expr::Column(c1.clone()),
                }],
                else_expr: None,
            };

            assert_eq!(
                exp_no_else.display(Dialect::MySQL).to_string(),
                "CASE WHEN (`foo` = 0) THEN `foo` END"
            );

            let exp_multiple_branches = Expr::CaseWhen {
                branches: vec![
                    CaseWhenBranch {
                        condition: Expr::BinaryOp {
                            op: BinaryOperator::Equal,
                            lhs: Box::new(Expr::Column(c1.clone())),
                            rhs: Box::new(Expr::Literal(Literal::Integer(0))),
                        },
                        body: Expr::Column(c1.clone()),
                    },
                    CaseWhenBranch {
                        condition: Expr::BinaryOp {
                            op: BinaryOperator::Equal,
                            lhs: Box::new(Expr::Column(c1.clone())),
                            rhs: Box::new(Expr::Literal(Literal::Integer(7))),
                        },
                        body: Expr::Column(c1),
                    },
                ],
                else_expr: None,
            };

            assert_eq!(
                exp_multiple_branches.display(Dialect::MySQL).to_string(),
                "CASE WHEN (`foo` = 0) THEN `foo` WHEN (`foo` = 7) THEN `foo` END"
            );
        }

        #[test]
        fn display_group_concat() {
            assert_eq!(
                FunctionExpr::GroupConcat {
                    expr: Box::new(Expr::Column("x".into())),
                    separator: Some("a".into())
                }
                .display(Dialect::MySQL)
                .to_string(),
                "group_concat(`x` separator 'a')"
            );
            assert_eq!(
                FunctionExpr::GroupConcat {
                    expr: Box::new(Expr::Column("x".into())),
                    separator: Some("'".into())
                }
                .display(Dialect::MySQL)
                .to_string(),
                "group_concat(`x` separator '''')"
            );
            assert_eq!(
                FunctionExpr::GroupConcat {
                    expr: Box::new(Expr::Column("x".into())),
                    separator: None
                }
                .display(Dialect::MySQL)
                .to_string(),
                "group_concat(`x`)"
            )
        }

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
            use crate::{to_nom_result, ItemPlaceholder};

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

                let res = to_nom_result(expression(Dialect::MySQL)(LocatedSpan::new(
                    cond.as_bytes(),
                )));
                let expected = Expr::BinaryOp {
                    op: BinaryOperator::And,
                    lhs: Box::new(Expr::BinaryOp {
                        lhs: Box::new(Expr::Column("read_ribbons.is_following".into())),
                        op: BinaryOperator::Equal,
                        rhs: Box::new(Expr::Literal(1.into())),
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
                                rhs: Box::new(Expr::Literal(0.into())),
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
                                            rhs: Box::new(Expr::Literal(0.into())),
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

                let res1 = expression(Dialect::MySQL)(LocatedSpan::new(cond1.as_bytes()));
                assert_eq!(
                    res1.unwrap().1,
                    Expr::BinaryOp {
                        lhs: Box::new(Expr::Column(Column::from("foo"))),
                        op: BinaryOperator::Equal,
                        rhs: Box::new(Expr::Literal(Literal::Integer(42)))
                    }
                );

                let res2 = expression(Dialect::MySQL)(LocatedSpan::new(cond2.as_bytes()));
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

        #[test]
        fn column_beginning_with_null() {
            let res = test_parse!(expression(Dialect::PostgreSQL), b"nullable");
            assert_eq!(
                res,
                Expr::Column(Column {
                    name: "nullable".into(),
                    table: None
                })
            );
        }

        #[test]
        fn not_in_comparison() {
            let qs1 = b"id not in (1,2)";
            let res1 = expression(Dialect::PostgreSQL)(LocatedSpan::new(qs1));

            let c1 = res1.unwrap().1;
            let expected1 = Expr::In {
                lhs: Box::new(Expr::Column("id".into())),
                rhs: InValue::List(vec![Expr::Literal(1.into()), Expr::Literal(2.into())]),
                negated: true,
            };
            assert_eq!(c1, expected1);

            let expected1 = "\"id\" NOT IN (1, 2)";
            assert_eq!(c1.display(Dialect::PostgreSQL).to_string(), expected1);
        }

        #[test]
        fn case_display() {
            let c1 = Column {
                name: "foo".into(),
                table: None,
            };

            let exp = Expr::CaseWhen {
                branches: vec![CaseWhenBranch {
                    condition: Expr::BinaryOp {
                        op: BinaryOperator::Equal,
                        lhs: Box::new(Expr::Column(c1.clone())),
                        rhs: Box::new(Expr::Literal(Literal::Integer(0))),
                    },
                    body: Expr::Column(c1.clone()),
                }],
                else_expr: Some(Box::new(Expr::Literal(Literal::Integer(1)))),
            };

            assert_eq!(
                exp.display(Dialect::PostgreSQL).to_string(),
                "CASE WHEN (\"foo\" = 0) THEN \"foo\" ELSE 1 END"
            );

            let exp_no_else = Expr::CaseWhen {
                branches: vec![CaseWhenBranch {
                    condition: Expr::BinaryOp {
                        op: BinaryOperator::Equal,
                        lhs: Box::new(Expr::Column(c1.clone())),
                        rhs: Box::new(Expr::Literal(Literal::Integer(0))),
                    },
                    body: Expr::Column(c1.clone()),
                }],
                else_expr: None,
            };

            assert_eq!(
                exp_no_else.display(Dialect::PostgreSQL).to_string(),
                "CASE WHEN (\"foo\" = 0) THEN \"foo\" END"
            );

            let exp_multiple_branches = Expr::CaseWhen {
                branches: vec![
                    CaseWhenBranch {
                        condition: Expr::BinaryOp {
                            op: BinaryOperator::Equal,
                            lhs: Box::new(Expr::Column(c1.clone())),
                            rhs: Box::new(Expr::Literal(Literal::Integer(0))),
                        },
                        body: Expr::Column(c1.clone()),
                    },
                    CaseWhenBranch {
                        condition: Expr::BinaryOp {
                            op: BinaryOperator::Equal,
                            lhs: Box::new(Expr::Column(c1.clone())),
                            rhs: Box::new(Expr::Literal(Literal::Integer(7))),
                        },
                        body: Expr::Column(c1),
                    },
                ],
                else_expr: None,
            };

            assert_eq!(
                exp_multiple_branches
                    .display(Dialect::PostgreSQL)
                    .to_string(),
                "CASE WHEN (\"foo\" = 0) THEN \"foo\" WHEN (\"foo\" = 7) THEN \"foo\" END"
            );
        }

        #[test]
        fn equals_any() {
            assert_eq!(
                test_parse!(expression(Dialect::PostgreSQL), b"x = ANY('foo')"),
                Expr::OpAny {
                    lhs: Box::new(Expr::Column("x".into())),
                    op: BinaryOperator::Equal,
                    rhs: Box::new(Expr::Literal("foo".into()))
                }
            );
        }

        #[test]
        fn equals_some() {
            assert_eq!(
                test_parse!(expression(Dialect::PostgreSQL), b"x = SOME ('foo')"),
                Expr::OpSome {
                    lhs: Box::new(Expr::Column("x".into())),
                    op: BinaryOperator::Equal,
                    rhs: Box::new(Expr::Literal("foo".into()))
                }
            );
        }

        #[test]
        fn equals_all() {
            assert_eq!(
                test_parse!(expression(Dialect::PostgreSQL), b"x = aLL ('foo')"),
                Expr::OpAll {
                    lhs: Box::new(Expr::Column("x".into())),
                    op: BinaryOperator::Equal,
                    rhs: Box::new(Expr::Literal("foo".into()))
                }
            );
        }

        #[test]
        fn lte_any() {
            assert_eq!(
                test_parse!(expression(Dialect::PostgreSQL), b"x <= ANY('foo')"),
                Expr::OpAny {
                    lhs: Box::new(Expr::Column("x".into())),
                    op: BinaryOperator::LessOrEqual,
                    rhs: Box::new(Expr::Literal("foo".into()))
                }
            );
        }

        #[test]
        fn op_any_mixed_with_op() {
            assert_eq!(
                test_parse!(expression(Dialect::PostgreSQL), b"1 = ANY('{1,2}') = true"),
                Expr::BinaryOp {
                    lhs: Box::new(Expr::OpAny {
                        lhs: Box::new(Expr::Literal(1.into())),
                        op: BinaryOperator::Equal,
                        rhs: Box::new(Expr::Literal("{1,2}".into()))
                    }),
                    op: BinaryOperator::Equal,
                    rhs: Box::new(Expr::Literal(Literal::Boolean(true)))
                }
            );
        }

        #[test]
        fn op_any_mixed_with_and() {
            assert_eq!(
                test_parse!(
                    expression(Dialect::PostgreSQL),
                    b"1 = ANY('{1,2}') and y = 4"
                ),
                Expr::BinaryOp {
                    lhs: Box::new(Expr::OpAny {
                        lhs: Box::new(Expr::Literal(1.into())),
                        op: BinaryOperator::Equal,
                        rhs: Box::new(Expr::Literal("{1,2}".into()))
                    }),
                    op: BinaryOperator::And,
                    rhs: Box::new(Expr::BinaryOp {
                        lhs: Box::new(Expr::Column("y".into())),
                        op: BinaryOperator::Equal,
                        rhs: Box::new(Expr::Literal(4.into()))
                    })
                }
            );
        }

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

            /// `->` vs `=`.
            #[test]
            fn arrow1_equals() {
                // NOTE: `=` does not work for `json`, but Postgres's error message indicates the
                // precedence.
                parses_same(
                    Dialect::PostgreSQL,
                    "'[1, 2, 3]'::json -> 0 = '[3, 2, 1]'::json -> 2",
                    "('[1, 2, 3]'::json -> 0) = ('[3, 2, 1]'::json -> 2)",
                )
            }

            /// `->` vs `-`.
            #[test]
            fn arrow1_subtract() {
                parses_same(
                    Dialect::PostgreSQL,
                    "'[[1, 2, 3]]'::json -> 1 - 1 -> 1",
                    "'[[1, 2, 3]]'::json -> (1 - 1) -> 1",
                )
            }

            /// `->` vs `*`.
            #[test]
            fn arrow1_multiply() {
                parses_same(
                    Dialect::PostgreSQL,
                    "'[[1, 2, 3]]'::json -> 1 * 0 -> 1",
                    "'[[1, 2, 3]]'::json -> (1 * 0) -> 1",
                )
            }

            /// `->>` vs `like`.
            #[test]
            fn arrow2_like() {
                parses_same(
                    Dialect::PostgreSQL,
                    "'[1, 2, 3]'::json ->> 0 like '[3, 2, 1]'::json ->> 2",
                    "('[1, 2, 3]'::json ->> 0) like ('[3, 2, 1]'::json ->> 2)",
                )
            }

            #[test]
            #[ignore = "TODO once we support ROW exprs in dataflow"]
            fn in_rhs_row_expr() {
                parses_same(
                    Dialect::PostgreSQL,
                    "lhs IN ((rhs1, rhs2))",
                    "lhs IN (ROW(rhs1, rhs2))",
                );
            }
        }

        mod conditions {
            use super::*;
            use crate::{to_nom_result, ItemPlaceholder};

            #[test]
            fn question_mark_operator() {
                let cond = "'{\"abc\": 42}' ? 'abc'";
                let res = to_nom_result(expression(Dialect::PostgreSQL)(LocatedSpan::new(
                    cond.as_bytes(),
                )));
                let expected = Expr::BinaryOp {
                    lhs: Box::new(Expr::Literal("{\"abc\": 42}".into())),
                    op: BinaryOperator::QuestionMark,
                    rhs: Box::new(Expr::Literal("abc".into())),
                };

                let (rem, res) = res.unwrap();
                assert_eq!(std::str::from_utf8(rem).unwrap(), "");
                assert_eq!(res, expected);
            }

            #[test]
            fn question_mark_pipe_operator() {
                let cond = "'{\"abc\": 42}' ?| ARRAY['abc', 'def']";

                let res = to_nom_result(expression(Dialect::PostgreSQL)(LocatedSpan::new(
                    cond.as_bytes(),
                )));
                let expected = Expr::BinaryOp {
                    lhs: Box::new(Expr::Literal("{\"abc\": 42}".into())),
                    op: BinaryOperator::QuestionMarkPipe,
                    rhs: Box::new(Expr::Array(vec![
                        Expr::Literal("abc".into()),
                        Expr::Literal("def".into()),
                    ])),
                };

                let (rem, res) = res.unwrap();
                assert_eq!(std::str::from_utf8(rem).unwrap(), "");
                assert_eq!(res, expected);
            }

            #[test]
            fn question_mark_and_operator() {
                let cond = "'{\"abc\": 42}' ?& ARRAY['abc', 'def']";

                let res = to_nom_result(expression(Dialect::PostgreSQL)(LocatedSpan::new(
                    cond.as_bytes(),
                )));
                let expected = Expr::BinaryOp {
                    lhs: Box::new(Expr::Literal("{\"abc\": 42}".into())),
                    op: BinaryOperator::QuestionMarkAnd,
                    rhs: Box::new(Expr::Array(vec![
                        Expr::Literal("abc".into()),
                        Expr::Literal("def".into()),
                    ])),
                };

                let (rem, res) = res.unwrap();
                assert_eq!(std::str::from_utf8(rem).unwrap(), "");
                assert_eq!(res, expected);
            }

            #[test]
            fn double_pipe_operator() {
                // Note that we currently only support the PG JSONB || operator. At some point this
                // test may need to be extended to also handle the string concat || operator as
                // well as the MySQL boolean || operator.
                let cond = "'[\"a\", \"b\"]'::jsonb || '[\"c\", \"d\"]'";

                let res = to_nom_result(expression(Dialect::PostgreSQL)(LocatedSpan::new(
                    cond.as_bytes(),
                )));
                let expected = Expr::BinaryOp {
                    lhs: Box::new(Expr::Cast {
                        expr: Box::new(Expr::Literal(r#"["a", "b"]"#.into())),
                        ty: SqlType::Jsonb,
                        postgres_style: true,
                    }),
                    op: BinaryOperator::DoublePipe,
                    rhs: Box::new(Expr::Literal(r#"["c", "d"]"#.into())),
                };

                let (rem, res) = res.unwrap();
                assert_eq!(std::str::from_utf8(rem).unwrap(), "");
                assert_eq!(res, expected);
            }

            #[test]
            fn at_arrow_right_operator() {
                let cond = b"'[1, 2, 2]' @> '2'";
                let res = test_parse!(expression(Dialect::PostgreSQL), cond);
                assert_eq!(
                    res,
                    Expr::BinaryOp {
                        lhs: Box::new(Expr::Literal("[1, 2, 2]".into())),
                        op: BinaryOperator::AtArrowRight,
                        rhs: Box::new(Expr::Literal("2".into())),
                    }
                );
            }

            #[test]
            fn at_arrow_left_operator() {
                let cond = b"'2' <@ '[1, 2, 2]'";
                let res = test_parse!(expression(Dialect::PostgreSQL), cond);
                assert_eq!(
                    res,
                    Expr::BinaryOp {
                        lhs: Box::new(Expr::Literal("2".into())),
                        op: BinaryOperator::AtArrowLeft,
                        rhs: Box::new(Expr::Literal("[1, 2, 2]".into())),
                    }
                );
            }

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

                let res = to_nom_result(expression(Dialect::PostgreSQL)(LocatedSpan::new(
                    cond.as_bytes(),
                )));
                let expected = Expr::BinaryOp {
                    op: BinaryOperator::And,
                    lhs: Box::new(Expr::BinaryOp {
                        lhs: Box::new(Expr::Column("read_ribbons.is_following".into())),
                        op: BinaryOperator::Equal,
                        rhs: Box::new(Expr::Literal(1.into())),
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
                                rhs: Box::new(Expr::Literal(0.into())),
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
                                            rhs: Box::new(Expr::Literal(0.into())),
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

                let res1 = expression(Dialect::PostgreSQL)(LocatedSpan::new(cond1.as_bytes()));
                assert_eq!(
                    res1.unwrap().1,
                    Expr::BinaryOp {
                        lhs: Box::new(Expr::Column(Column::from("foo"))),
                        op: BinaryOperator::Equal,
                        rhs: Box::new(Expr::Literal(Literal::Integer(42)))
                    }
                );

                let res2 = expression(Dialect::PostgreSQL)(LocatedSpan::new(cond2.as_bytes()));
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

        mod json {
            use super::*;

            #[test]
            fn arrow1_operator() {
                let expr = b"'[1, 2, 3]' -> 2";
                let res = test_parse!(expression(Dialect::PostgreSQL), expr);
                assert_eq!(
                    res,
                    Expr::BinaryOp {
                        lhs: Box::new(Expr::Literal("[1, 2, 3]".into())),
                        op: BinaryOperator::Arrow1,
                        rhs: Box::new(Expr::Literal(2.into())),
                    }
                );
            }

            #[test]
            fn arrow2_operator() {
                let expr = b"'[1, 2, 3]' ->> 2";
                let res = test_parse!(expression(Dialect::PostgreSQL), expr);
                assert_eq!(
                    res,
                    Expr::BinaryOp {
                        lhs: Box::new(Expr::Literal("[1, 2, 3]".into())),
                        op: BinaryOperator::Arrow2,
                        rhs: Box::new(Expr::Literal(2.into())),
                    }
                );
            }

            #[test]
            fn hash_arrow1_operator() {
                let expr = b"'[1, 2, 3]' #> array['2']";
                let res = test_parse!(expression(Dialect::PostgreSQL), expr);
                assert_eq!(
                    res,
                    Expr::BinaryOp {
                        lhs: Box::new(Expr::Literal("[1, 2, 3]".into())),
                        op: BinaryOperator::HashArrow1,
                        rhs: Box::new(Expr::Array(vec![Expr::Literal("2".into())])),
                    }
                );
            }

            #[test]
            fn hash_arrow2_operator() {
                let expr = b"'[1, 2, 3]' #>> array['2']";
                let res = test_parse!(expression(Dialect::PostgreSQL), expr);
                assert_eq!(
                    res,
                    Expr::BinaryOp {
                        lhs: Box::new(Expr::Literal("[1, 2, 3]".into())),
                        op: BinaryOperator::HashArrow2,
                        rhs: Box::new(Expr::Array(vec![Expr::Literal("2".into())])),
                    }
                );
            }

            #[test]
            fn hash_subtract_operator() {
                let expr = b"'[1, 2, 3]' #- array['2']";
                let res = test_parse!(expression(Dialect::PostgreSQL), expr);
                assert_eq!(
                    res,
                    Expr::BinaryOp {
                        lhs: Box::new(Expr::Literal("[1, 2, 3]".into())),
                        op: BinaryOperator::HashSubtract,
                        rhs: Box::new(Expr::Array(vec![Expr::Literal("2".into())])),
                    }
                );
            }
        }

        #[test]
        fn parse_array_expr() {
            let res = test_parse!(
                expression(Dialect::PostgreSQL),
                b"ARRAY[[1, '2'::int], array[3]]"
            );
            assert_eq!(
                res,
                Expr::Array(vec![
                    Expr::Array(vec![
                        Expr::Literal(1.into()),
                        Expr::Cast {
                            expr: Box::new(Expr::Literal("2".into())),
                            ty: SqlType::Int(None),
                            postgres_style: true,
                        },
                    ]),
                    Expr::Array(vec![Expr::Literal(3.into())])
                ])
            );
        }

        #[test]
        fn format_array_expr() {
            let expr = Expr::Array(vec![Expr::Array(vec![
                Expr::Literal(1.into()),
                Expr::Cast {
                    expr: Box::new(Expr::Literal("2".into())),
                    ty: SqlType::Int(None),
                    postgres_style: true,
                },
            ])]);
            let res = expr.display(Dialect::PostgreSQL).to_string();

            assert_eq!(res, "ARRAY[[1,('2'::INT)]]");
        }

        #[test]
        fn row_expr_explicit() {
            assert_eq!(
                test_parse!(expression(Dialect::PostgreSQL), b"ROW(1,2,3)"),
                Expr::Row {
                    explicit: true,
                    exprs: vec![
                        Expr::Literal(1.into()),
                        Expr::Literal(2.into()),
                        Expr::Literal(3.into())
                    ]
                }
            );
            assert_eq!(
                test_parse!(expression(Dialect::PostgreSQL), b"ROW(7)"),
                Expr::Row {
                    explicit: true,
                    exprs: vec![Expr::Literal(7.into())]
                }
            );
        }

        #[test]
        fn row_expr_implicit() {
            assert_eq!(
                test_parse!(expression(Dialect::PostgreSQL), b"(1,2,3)"),
                Expr::Row {
                    explicit: false,
                    exprs: vec![
                        Expr::Literal(1.into()),
                        Expr::Literal(2.into()),
                        Expr::Literal(3.into())
                    ]
                }
            );

            // Only one expr is not a ROW expr
            assert_eq!(
                test_parse!(expression(Dialect::PostgreSQL), b"(7)"),
                Expr::Literal(7.into())
            );
        }
    }
}
