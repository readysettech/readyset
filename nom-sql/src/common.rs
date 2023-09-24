use std::fmt::{self, Debug, Display};
use std::hash::Hash;
use std::ops::{Range, RangeFrom, RangeTo};
use std::str;
use std::str::FromStr;

use itertools::Itertools;
use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case, take_until};
use nom::character::complete::{digit1, line_ending};
use nom::combinator::{map, map_res, not, opt, peek};
use nom::error::{ErrorKind, ParseError};
use nom::multi::{separated_list0, separated_list1};
use nom::sequence::{delimited, pair, preceded, separated_pair, terminated, tuple};
use nom::{IResult, InputLength, InputTake};
use nom_locate::LocatedSpan;
use readyset_util::fmt::fmt_with;
use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

use crate::column::Column;
use crate::dialect::{Dialect, DialectDisplay};
use crate::expression::expression;
use crate::table::Relation;
use crate::whitespace::{whitespace0, whitespace1};
use crate::{Expr, FunctionExpr, Literal, NomSqlResult, SqlIdentifier};

#[cfg(feature = "debug")]
pub fn debug_print(tag: &str, i: &[u8]) {
    eprintln!("{}: {}", tag, String::from_utf8_lossy(i))
}

#[cfg(not(feature = "debug"))]
pub fn debug_print(_tag: &str, _i: &[u8]) {}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub enum IndexType {
    BTree,
    Hash,
}

impl Display for IndexType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IndexType::BTree => write!(f, "BTREE"),
            IndexType::Hash => write!(f, "HASH"),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub enum ReferentialAction {
    Cascade,
    SetNull,
    Restrict,
    NoAction,
    SetDefault,
}

impl fmt::Display for ReferentialAction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::Cascade => "CASCADE",
                Self::SetNull => "SET NULL",
                Self::Restrict => "RESTRICT",
                Self::NoAction => "NO ACTION",
                Self::SetDefault => "SET DEFAULT",
            }
        )
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub enum TableKey {
    PrimaryKey {
        constraint_name: Option<SqlIdentifier>,
        index_name: Option<SqlIdentifier>,
        columns: Vec<Column>,
    },
    UniqueKey {
        constraint_name: Option<SqlIdentifier>,
        index_name: Option<SqlIdentifier>,
        columns: Vec<Column>,
        index_type: Option<IndexType>,
    },
    FulltextKey {
        index_name: Option<SqlIdentifier>,
        columns: Vec<Column>,
    },
    Key {
        constraint_name: Option<SqlIdentifier>,
        index_name: Option<SqlIdentifier>,
        columns: Vec<Column>,
        index_type: Option<IndexType>,
    },
    ForeignKey {
        constraint_name: Option<SqlIdentifier>,
        index_name: Option<SqlIdentifier>,
        columns: Vec<Column>,
        target_table: Relation,
        target_columns: Vec<Column>,
        on_delete: Option<ReferentialAction>,
        on_update: Option<ReferentialAction>,
    },
    CheckConstraint {
        // NOTE: MySQL dosn't allow the `CONSTRAINT (name)` prefix for a CHECK, but Postgres does
        constraint_name: Option<SqlIdentifier>,
        expr: Expr,
        enforced: Option<bool>,
    },
}

impl TableKey {
    pub fn constraint_name(&self) -> &Option<SqlIdentifier> {
        match self {
            TableKey::PrimaryKey {
                constraint_name, ..
            }
            | TableKey::UniqueKey {
                constraint_name, ..
            }
            | TableKey::Key {
                constraint_name, ..
            }
            | TableKey::ForeignKey {
                constraint_name, ..
            }
            | TableKey::CheckConstraint {
                constraint_name, ..
            } => constraint_name,
            TableKey::FulltextKey { .. } => &None,
        }
    }
}

impl DialectDisplay for TableKey {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + Copy + '_ {
        fmt_with(move |f| {
            if let Some(constraint_name) = self.constraint_name() {
                write!(
                    f,
                    "CONSTRAINT {} ",
                    dialect.quote_identifier(constraint_name)
                )?;
            }

            match self {
                TableKey::PrimaryKey {
                    index_name,
                    columns,
                    ..
                } => {
                    write!(f, "PRIMARY KEY ")?;
                    if let Some(index_name) = index_name {
                        write!(f, "{} ", dialect.quote_identifier(index_name))?;
                    }
                    write!(
                        f,
                        "({})",
                        columns.iter().map(|c| c.display(dialect)).join(", ")
                    )
                }
                TableKey::UniqueKey {
                    index_name,
                    columns,
                    index_type,
                    ..
                } => {
                    write!(f, "UNIQUE KEY ")?;
                    if let Some(index_name) = index_name {
                        write!(f, "{} ", dialect.quote_identifier(index_name))?;
                    }
                    write!(
                        f,
                        "({})",
                        columns.iter().map(|c| c.display(dialect)).join(", ")
                    )?;
                    if let Some(index_type) = index_type {
                        write!(f, " USING {}", index_type)?;
                    }
                    Ok(())
                }
                TableKey::FulltextKey {
                    index_name,
                    columns,
                } => {
                    write!(f, "FULLTEXT KEY ")?;
                    if let Some(ref index_name) = *index_name {
                        write!(f, "{} ", dialect.quote_identifier(index_name))?;
                    }
                    write!(
                        f,
                        "({})",
                        columns.iter().map(|c| c.display(dialect)).join(", ")
                    )
                }
                TableKey::Key {
                    index_name,
                    columns,
                    index_type,
                    ..
                } => {
                    write!(f, "KEY ")?;
                    if let Some(index_name) = index_name {
                        write!(f, "{} ", dialect.quote_identifier(index_name))?;
                    }
                    write!(
                        f,
                        "({})",
                        columns.iter().map(|c| c.display(dialect)).join(", ")
                    )?;
                    if let Some(index_type) = index_type {
                        write!(f, " USING {}", index_type)?;
                    }
                    Ok(())
                }
                TableKey::ForeignKey {
                    index_name,
                    columns,
                    target_table,
                    target_columns,
                    on_delete,
                    on_update,
                    ..
                } => {
                    let index_name = fmt_with(|f| {
                        if let Some(index_name) = index_name {
                            write!(f, "{}", dialect.quote_identifier(index_name))?;
                        }
                        Ok(())
                    });

                    write!(
                        f,
                        "FOREIGN KEY {} ({}) REFERENCES {} ({})",
                        index_name,
                        columns.iter().map(|c| c.display(dialect)).join(", "),
                        target_table.display(dialect),
                        target_columns.iter().map(|c| c.display(dialect)).join(", ")
                    )?;
                    if let Some(on_delete) = on_delete {
                        write!(f, " ON DELETE {}", on_delete)?;
                    }
                    if let Some(on_update) = on_update {
                        write!(f, " ON UPDATE {}", on_update)?;
                    }
                    Ok(())
                }
                TableKey::CheckConstraint { expr, enforced, .. } => {
                    // NOTE: MySQL does not allow an optional 'CONSTRAINT' here and expects only
                    // "ADD CHECK" https://dev.mysql.com/doc/refman/5.7/en/alter-table.html

                    // Postgres is fine with it, but since this is our own display, leave it out.
                    // https://www.postgresql.org/docs/current/sql-altertable.html
                    write!(f, " CHECK {}", expr.display(dialect))?;

                    if let Some(enforced) = enforced {
                        if !enforced {
                            write!(f, " NOT")?;
                        }
                        write!(f, " ENFORCED")?;
                    }

                    Ok(())
                }
            }
        })
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
#[allow(clippy::large_enum_variant)] // NOTE(aspen): do we actually care about this?
#[derive(Default)]
pub enum FieldDefinitionExpr {
    #[default]
    All,
    AllInTable(Relation),
    // TODO: re-enable once Expr round trip is stable
    #[weight(0)]
    Expr {
        expr: Expr,
        alias: Option<SqlIdentifier>,
    },
}

/// Constructs a [`FieldDefinitionExpr::Expr`] without an alias
impl From<Expr> for FieldDefinitionExpr {
    fn from(expr: Expr) -> Self {
        FieldDefinitionExpr::Expr { expr, alias: None }
    }
}

/// Constructs a [`FieldDefinitionExpr::Expr`] based on an [`Expr::Column`] for
/// the column and without an alias
impl From<Column> for FieldDefinitionExpr {
    fn from(col: Column) -> Self {
        FieldDefinitionExpr::Expr {
            expr: Expr::Column(col),
            alias: None,
        }
    }
}

impl From<Literal> for FieldDefinitionExpr {
    fn from(lit: Literal) -> Self {
        FieldDefinitionExpr::from(Expr::Literal(lit))
    }
}

impl DialectDisplay for FieldDefinitionExpr {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + Copy + '_ {
        fmt_with(move |f| match self {
            Self::All => write!(f, "*"),
            Self::AllInTable(table) => {
                write!(f, "{}.*", table.display(dialect))
            }
            Self::Expr { expr, alias } => {
                write!(f, "{}", expr.display(dialect))?;
                if let Some(alias) = alias {
                    write!(f, " AS {}", dialect.quote_identifier(alias))?;
                }
                Ok(())
            }
        })
    }
}

pub enum Sign {
    Unsigned,
    Signed,
}

/// A reference to a field in a query, usable in either the `GROUP BY` or `ORDER BY` clauses of the
/// query
#[derive(Clone, Debug, Eq, Hash, PartialEq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub enum FieldReference {
    /// A reference to a field in the `SELECT` list by its (1-based) index.
    Numeric(u64),
    /// An expression
    Expr(Expr),
}

impl DialectDisplay for FieldReference {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + Copy + '_ {
        fmt_with(move |f| match self {
            Self::Numeric(n) => write!(f, "{}", n),
            Self::Expr(expr) => write!(f, "{}", expr.display(dialect)),
        })
    }
}

pub(crate) fn parse_fallible<'a, F, G, R>(
    mut success: F,
    mut failure: G,
) -> impl FnMut(LocatedSpan<&'a [u8]>) -> NomSqlResult<&'a [u8], Result<R, String>>
where
    F: FnMut(LocatedSpan<&'a [u8]>) -> NomSqlResult<&'a [u8], R>,
    G: FnMut(LocatedSpan<&'a [u8]>) -> NomSqlResult<&'a [u8], &'a [u8]>,
{
    move |i| {
        map(&mut success, Ok)(i)
            .or_else(|_| map(map_res(&mut failure, str::from_utf8), |s| Err(s.to_owned()))(i))
    }
}

pub(crate) fn opt_delimited<I: Clone, O1, O2, O3, E: ParseError<I>, F, G, H>(
    mut first: F,
    mut second: G,
    mut third: H,
) -> impl FnMut(I) -> IResult<I, O2, E>
where
    F: FnMut(I) -> IResult<I, O1, E>,
    G: FnMut(I) -> IResult<I, O2, E>,
    H: FnMut(I) -> IResult<I, O3, E>,
{
    move |input: I| {
        let inp = input.clone();
        match second(input) {
            Ok((i, o)) => Ok((i, o)),
            _ => {
                let first_ = &mut first;
                let second_ = &mut second;
                let third_ = &mut third;
                delimited(first_, second_, third_)(inp)
            }
        }
    }
}

// Parses the arguments for an aggregation function, and also returns whether the distinct flag is
// present.
pub fn agg_function_arguments(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], (Expr, bool)> {
    move |i| {
        let distinct_parser = opt(tuple((tag_no_case("distinct"), whitespace1)));
        let (remaining_input, (distinct, args)) = tuple((distinct_parser, expression(dialect)))(i)?;
        Ok((remaining_input, (args, distinct.is_some())))
    }
}

fn group_concat_fx_helper(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], String> {
    move |i| {
        let ws_sep = delimited(whitespace0, tag_no_case("separator"), whitespace0);
        let (i, sep) = delimited(
            ws_sep,
            opt(map_res(
                move |i| dialect.string_literal()(i),
                String::from_utf8,
            )),
            whitespace0,
        )(i)?;

        Ok((i, sep.unwrap_or_default()))
    }
}

fn group_concat_fx(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], (Expr, Option<String>)> {
    move |i| pair(expression(dialect), opt(group_concat_fx_helper(dialect)))(i)
}

fn agg_fx_args(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], (Expr, bool)> {
    move |i| {
        let (i, _) = whitespace0(i)?;
        delimited(tag("("), agg_function_arguments(dialect), tag(")"))(i)
    }
}

fn delim_fx_args(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Vec<Expr>> {
    move |i| {
        delimited(
            tag("("),
            separated_list0(
                tag(","),
                delimited(whitespace0, expression(dialect), whitespace0),
            ),
            tag(")"),
        )(i)
    }
}

fn function_call_without_parens(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], FunctionExpr> {
    // Some functions can be called without parentheses, in both mysql and postgres
    let (i, name) = map(
        alt((
            tag_no_case("now"),
            tag_no_case("current_date"),
            tag_no_case("current_timestamp"),
            tag_no_case("current_time"),
            tag_no_case("localtimestamp"),
            tag_no_case("localtime"),
        )),
        |n: LocatedSpan<&[u8]>| {
            String::from_utf8(n.to_vec())
                .expect("Only constant string literals")
                .into()
        },
    )(i)?;

    Ok((
        i,
        FunctionExpr::Call {
            name,
            arguments: vec![],
        },
    ))
}

fn substring(dialect: Dialect) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], FunctionExpr> {
    move |i| {
        let (i, _) = alt((tag_no_case("substring"), tag_no_case("substr")))(i)?;
        let (i, _) = whitespace0(i)?;
        let (i, _) = tag("(")(i)?;
        let (i, string) = expression(dialect)(i)?;
        let (i, pos) = opt(preceded(
            tuple((whitespace1, tag_no_case("from"), whitespace1)),
            expression(dialect),
        ))(i)?;
        let (i, len) = opt(preceded(
            tuple((whitespace1, tag_no_case("for"), whitespace1)),
            expression(dialect),
        ))(i)?;
        let (i, _) = whitespace0(i)?;
        let (i, _) = tag(")")(i)?;

        Ok((
            i,
            FunctionExpr::Substring {
                string: Box::new(string),
                pos: pos.map(Box::new),
                len: len.map(Box::new),
            },
        ))
    }
}

fn function_call(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], FunctionExpr> {
    move |i| {
        let (i, name) = dialect.function_identifier()(i)?;
        let (i, _) = whitespace0(i)?;
        let (i, arguments) = delim_fx_args(dialect)(i)?;
        Ok((
            i,
            FunctionExpr::Call {
                name: name.into(),
                arguments,
            },
        ))
    }
}

pub fn function_expr(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], FunctionExpr> {
    move |i| {
        alt((
            map(
                tuple((
                    tag_no_case("count"),
                    whitespace0,
                    tag("("),
                    whitespace0,
                    tag("*"),
                    whitespace0,
                    tag(")"),
                )),
                |_| FunctionExpr::CountStar,
            ),
            map(
                preceded(tag_no_case("count"), agg_fx_args(dialect)),
                |args| FunctionExpr::Count {
                    expr: Box::new(args.0.clone()),
                    distinct: args.1,
                },
            ),
            map(preceded(tag_no_case("sum"), agg_fx_args(dialect)), |args| {
                FunctionExpr::Sum {
                    expr: Box::new(args.0.clone()),
                    distinct: args.1,
                }
            }),
            map(preceded(tag_no_case("avg"), agg_fx_args(dialect)), |args| {
                FunctionExpr::Avg {
                    expr: Box::new(args.0.clone()),
                    distinct: args.1,
                }
            }),
            map(preceded(tag_no_case("max"), agg_fx_args(dialect)), |args| {
                FunctionExpr::Max(Box::new(args.0))
            }),
            map(preceded(tag_no_case("min"), agg_fx_args(dialect)), |args| {
                FunctionExpr::Min(Box::new(args.0))
            }),
            map(
                preceded(
                    tag_no_case("group_concat"),
                    preceded(
                        whitespace0,
                        delimited(
                            terminated(tag("("), whitespace0),
                            group_concat_fx(dialect),
                            preceded(whitespace0, tag(")")),
                        ),
                    ),
                ),
                |(expr, separator)| FunctionExpr::GroupConcat {
                    expr: Box::new(expr),
                    separator,
                },
            ),
            substring(dialect),
            function_call(dialect),
            function_call_without_parens,
        ))(i)
    }
}

// Parses a SQL column identifier in the db/schema.table.column format
pub fn column_identifier_no_alias(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Column> {
    move |i| {
        let (i, id1) = opt(terminated(
            dialect.identifier(),
            delimited(whitespace0, tag("."), whitespace0),
        ))(i)?;
        let (i, id2) = opt(terminated(
            dialect.identifier(),
            delimited(whitespace0, tag("."), whitespace0),
        ))(i)?;
        // Do we have a 'db/schema.table.' or 'table.' qualifier?
        let table = match (id1, id2) {
            (Some(db), Some(t)) => Some(Relation {
                schema: Some(db),
                name: t,
            }),
            // (None, Some(t)) should be unreachable
            (Some(t), None) | (None, Some(t)) => Some(Relation::from(t)),
            (None, None) => None,
        };

        let (i, name) = dialect.identifier()(i)?;

        let i = if name == "row" {
            let (i, _) = not(peek(terminated(whitespace0, tag("("))))(i)?;
            i
        } else {
            i
        };

        Ok((i, Column { name, table }))
    }
}

pub(crate) fn eof<I: Copy + InputLength, E: ParseError<I>>(input: I) -> IResult<I, I, E> {
    if input.input_len() == 0 {
        Ok((input, input))
    } else {
        Err(nom::Err::Error(E::from_error_kind(input, ErrorKind::Eof)))
    }
}

/// Parse the rest of the input up to a statement terminator
pub fn until_statement_terminator(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], &[u8]> {
    let len = match i.last() {
        Some(b';' | b'\n') => i.len() - 1,
        _ => i.len(),
    };
    let (i, res) = i.take_split(len);
    Ok((i, *res))
}

// Parse a terminator that ends a SQL statement.
pub fn statement_terminator(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], ()> {
    let (remaining_input, _) =
        delimited(whitespace0, alt((tag(";"), line_ending, eof)), whitespace0)(i)?;

    Ok((remaining_input, ()))
}

/// Parser combinator that applies the given parser,
/// and then tries to match for a statement terminator.
pub fn terminated_with_statement_terminator<F, O>(
    parser: F,
) -> impl FnOnce(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], O>
where
    F: Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], O>,
{
    move |i| terminated(parser, statement_terminator)(i)
}

// Parse rule for AS-based aliases for SQL entities.
pub fn as_alias(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], SqlIdentifier> {
    move |i| {
        map(
            tuple((
                whitespace1,
                opt(pair(tag_no_case("as"), whitespace1)),
                dialect.identifier(),
            )),
            |a| a.2,
        )(i)
    }
}

fn assignment_expr(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], (Column, Expr)> {
    move |i| {
        separated_pair(
            column_identifier_no_alias(dialect),
            delimited(whitespace0, tag("="), whitespace0),
            expression(dialect),
        )(i)
    }
}

/// Whitespace surrounded optionally on either side by a comma
pub(crate) fn ws_sep_comma(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], &[u8]> {
    map(delimited(whitespace0, tag(","), whitespace0), |i| *i)(i)
}

pub(crate) fn ws_sep_equals<I>(i: LocatedSpan<I>) -> NomSqlResult<I, I>
where
    I: nom::InputTakeAtPosition
        + nom::InputTake
        + nom::Compare<&'static str>
        + nom::FindSubstring<&'static str>
        + nom::Slice<Range<usize>>
        + nom::Slice<RangeTo<usize>>
        + nom::Slice<RangeFrom<usize>>
        + nom::InputIter
        + nom::AsBytes
        + nom::Offset
        + InputLength
        + Default
        + Clone
        + Copy
        + PartialEq,
    &'static str: nom::FindToken<<I as nom::InputTakeAtPosition>::Item>,
    <I as nom::InputIter>::Item: nom::AsChar + Clone,
    // Compare required by tag
    <I as nom::InputTakeAtPosition>::Item: nom::AsChar + Clone,
    // AsChar and Clone required by whitespace0
{
    delimited(
        whitespace0,
        map(tag("="), |i: LocatedSpan<I>| *i),
        whitespace0,
    )(i)
}

pub fn assignment_expr_list(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Vec<(Column, Expr)>> {
    move |i| separated_list1(ws_sep_comma, assignment_expr(dialect))(i)
}

// Parse rule for a comma-separated list of fields without aliases.
pub fn field_list(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Vec<Column>> {
    move |i| separated_list0(ws_sep_comma, column_identifier_no_alias(dialect))(i)
}

fn expression_field(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], FieldDefinitionExpr> {
    move |i| {
        let (i, expr) = expression(dialect)(i)?;
        let (i, alias) = opt(as_alias(dialect))(i)?;
        Ok((i, FieldDefinitionExpr::Expr { expr, alias }))
    }
}

fn all_in_table(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], FieldDefinitionExpr> {
    move |i| {
        let (i, ident1) = terminated(dialect.identifier(), tag("."))(i)?;
        let (i, ident2) = opt(terminated(dialect.identifier(), tag(".")))(i)?;
        let (i, _) = tag("*")(i)?;

        let table = match ident2 {
            Some(name) => Relation {
                schema: Some(ident1),
                name,
            },
            None => Relation {
                schema: None,
                name: ident1,
            },
        };

        Ok((i, FieldDefinitionExpr::AllInTable(table)))
    }
}

// Parse list of column/field definitions.
pub fn field_definition_expr(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Vec<FieldDefinitionExpr>> {
    move |i| {
        terminated(
            separated_list0(
                ws_sep_comma,
                alt((
                    map(tag("*"), |_| FieldDefinitionExpr::All),
                    all_in_table(dialect),
                    expression_field(dialect),
                )),
            ),
            opt(ws_sep_comma),
        )(i)
    }
}

// Parse a list of values (e.g., for INSERT syntax).
pub fn value_list(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Vec<Expr>> {
    move |i| separated_list0(ws_sep_comma, expression(dialect))(i)
}

pub(crate) fn if_not_exists(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], bool> {
    let (i, s) = opt(move |i| {
        let (i, _) = tag_no_case("if")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, _) = tag_no_case("not")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, _) = tag_no_case("exists")(i)?;
        let (i, _) = whitespace1(i)?;

        Ok((i, ()))
    })(i)?;

    Ok((i, s.is_some()))
}

// Parse rule for a comment part.
pub fn parse_comment(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], String> {
    map(
        preceded(
            delimited(whitespace0, tag_no_case("comment"), whitespace1),
            map_res(
                delimited(tag("'"), take_until("'"), tag("'")),
                |i: LocatedSpan<&[u8]>| str::from_utf8(&i),
            ),
        ),
        String::from,
    )(i)
}

pub fn field_reference(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], FieldReference> {
    move |i| {
        match dialect {
            Dialect::PostgreSQL => map(expression(dialect), FieldReference::Expr)(i),
            // Only MySQL supports numeric field references (postgresql considers them integer
            // literals, I'm pretty sure)
            Dialect::MySQL => alt((
                map(
                    map_res(
                        map_res(digit1, |i: LocatedSpan<&[u8]>| str::from_utf8(&i)),
                        u64::from_str,
                    ),
                    FieldReference::Numeric,
                ),
                map(expression(dialect), FieldReference::Expr),
            ))(i),
        }
    }
}

pub fn field_reference_list(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Vec<FieldReference>> {
    move |i| separated_list0(ws_sep_comma, field_reference(dialect))(i)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{to_nom_result, SqlType};

    fn test_opt_delimited_fn_call(i: &str) -> IResult<&[u8], &[u8]> {
        opt_delimited(tag("("), tag("abc"), tag(")"))(i.as_bytes())
    }

    #[test]
    fn qualified_column_with_spaces() {
        let res = test_parse!(column_identifier_no_alias(Dialect::MySQL), b"foo . bar");
        assert_eq!(
            res,
            Column {
                table: Some("foo".into()),
                name: "bar".into(),
            }
        )
    }

    #[test]
    fn opt_delimited_tests() {
        // let ok1 = IResult::Ok(("".as_bytes(), "abc".as_bytes()));
        assert_eq!(
            test_opt_delimited_fn_call("abc"),
            IResult::Ok(("".as_bytes(), "abc".as_bytes()))
        );
        assert_eq!(
            test_opt_delimited_fn_call("(abc)"),
            IResult::Ok(("".as_bytes(), "abc".as_bytes()))
        );
        test_opt_delimited_fn_call("(abc").unwrap_err();
        assert_eq!(
            test_opt_delimited_fn_call("abc)"),
            IResult::Ok((")".as_bytes(), "abc".as_bytes()))
        );
        test_opt_delimited_fn_call("ab").unwrap_err();
    }

    #[test]
    fn group_concat() {
        let qs = b"group_concat(x separator ', ')";
        let expected = FunctionExpr::GroupConcat {
            expr: Box::new(Expr::Column(Column::from("x"))),
            separator: Some(", ".to_owned()),
        };
        let res = to_nom_result(function_expr(Dialect::MySQL)(LocatedSpan::new(qs)));
        assert_eq!(res.unwrap().1, expected);

        assert_eq!(
            test_parse!(function_expr(Dialect::MySQL), b"group_concat('a')"),
            FunctionExpr::GroupConcat {
                expr: Box::new(Expr::Literal("a".into())),
                separator: None
            }
        );
        assert_eq!(
            test_parse!(function_expr(Dialect::MySQL), b"group_concat (a)"),
            FunctionExpr::GroupConcat {
                expr: Box::new(Expr::Column("a".into())),
                separator: None
            }
        );
        assert_eq!(
            test_parse!(function_expr(Dialect::MySQL), b"group_concat ( a )"),
            FunctionExpr::GroupConcat {
                expr: Box::new(Expr::Column("a".into())),
                separator: None
            }
        );
    }

    #[test]
    fn simple_generic_function() {
        let qlist = [
            "coalesce(a,b,c)".as_bytes(),
            "coalesce (a,b,c)".as_bytes(),
            "coalesce(a ,b,c)".as_bytes(),
            "coalesce(a, b,c)".as_bytes(),
        ];
        for q in qlist.iter() {
            let res = to_nom_result(function_expr(Dialect::MySQL)(LocatedSpan::new(q)));
            let expected = FunctionExpr::Call {
                name: "coalesce".into(),
                arguments: vec![
                    Expr::Column(Column::from("a")),
                    Expr::Column(Column::from("b")),
                    Expr::Column(Column::from("c")),
                ],
            };
            assert_eq!(res, Ok((&b""[..], expected)));
        }
    }

    #[test]
    fn nested_function_call() {
        let res = test_parse!(function_expr(Dialect::MySQL), b"max(min(foo))");
        assert_eq!(
            res,
            FunctionExpr::Max(Box::new(Expr::Call(FunctionExpr::Min(Box::new(
                Expr::Column("foo".into())
            )))))
        )
    }

    #[test]
    fn nested_cast() {
        let res = test_parse!(function_expr(Dialect::MySQL), b"max(cast(foo as int))");
        assert_eq!(
            res,
            FunctionExpr::Max(Box::new(Expr::Cast {
                expr: Box::new(Expr::Column("foo".into())),
                ty: SqlType::Int(None),
                postgres_style: false,
            }))
        )
    }

    #[test]
    fn generic_function_with_int_literal() {
        let (_, res) = function_expr(Dialect::MySQL)(LocatedSpan::new(b"ifnull(x, 0)")).unwrap();
        assert_eq!(
            res,
            FunctionExpr::Call {
                name: "ifnull".into(),
                arguments: vec![
                    Expr::Column(Column::from("x")),
                    Expr::Literal(Literal::Integer(0))
                ]
            }
        );
    }

    #[test]
    fn comment_data() {
        let res = parse_comment(LocatedSpan::new(b" COMMENT 'test'"));
        assert_eq!(res.unwrap().1, "test");
    }

    #[test]
    fn terminated_by_semicolon() {
        let res = to_nom_result(statement_terminator(LocatedSpan::new(b"   ;  ")));
        assert_eq!(res, Ok((&b""[..], ())));
    }

    #[test]
    fn parse_until_statement_terminator() {
        let (rem, res) = until_statement_terminator(LocatedSpan::new(b"abcdef;")).unwrap();
        assert_eq!(res, b"abcdef");
        assert_eq!(*rem, b";");

        let (rem, res) = until_statement_terminator(LocatedSpan::new(b"abcdef")).unwrap();
        assert_eq!(res, b"abcdef");
        assert_eq!(*rem, b"");

        let (rem, res) = until_statement_terminator(LocatedSpan::new(b"abc\ndef")).unwrap();
        assert_eq!(res, b"abc\ndef");
        assert_eq!(*rem, b"");
    }

    #[test]
    fn substr_from_for() {
        let res = test_parse!(function_expr(Dialect::MySQL), b"substr(a from 1 for 7)");
        assert_eq!(
            res,
            FunctionExpr::Substring {
                string: Box::new(Expr::Column("a".into())),
                pos: Some(Box::new(Expr::Literal(1.into()))),
                len: Some(Box::new(Expr::Literal(7.into())))
            }
        );
    }

    #[test]
    fn substring_from_for() {
        let res = test_parse!(function_expr(Dialect::MySQL), b"substring(a from 1 for 7)");
        assert_eq!(
            res,
            FunctionExpr::Substring {
                string: Box::new(Expr::Column("a".into())),
                pos: Some(Box::new(Expr::Literal(1.into()))),
                len: Some(Box::new(Expr::Literal(7.into())))
            }
        );
    }

    #[test]
    fn substr_from() {
        let res = test_parse!(function_expr(Dialect::MySQL), b"substr(a from 1)");
        assert_eq!(
            res,
            FunctionExpr::Substring {
                string: Box::new(Expr::Column("a".into())),
                pos: Some(Box::new(Expr::Literal(1.into()))),
                len: None,
            }
        );
    }

    #[test]
    fn substr_for() {
        let res = test_parse!(function_expr(Dialect::MySQL), b"substr(a for 7)");
        assert_eq!(
            res,
            FunctionExpr::Substring {
                string: Box::new(Expr::Column("a".into())),
                pos: None,
                len: Some(Box::new(Expr::Literal(7.into()))),
            }
        );
    }

    #[test]
    fn substring_regular_args() {
        let res = test_parse!(function_expr(Dialect::MySQL), b"substring(a,1,7)");
        assert_eq!(
            res,
            FunctionExpr::Call {
                name: "substring".into(),
                arguments: vec![
                    Expr::Column("a".into()),
                    Expr::Literal(1.into()),
                    Expr::Literal(7.into()),
                ]
            }
        );
    }

    #[test]
    fn count_star() {
        assert_eq!(
            test_parse!(function_expr(Dialect::MySQL), b"count(*)"),
            FunctionExpr::CountStar
        );
        assert_eq!(
            test_parse!(function_expr(Dialect::MySQL), b"count (*)"),
            FunctionExpr::CountStar
        );
        assert_eq!(
            test_parse!(function_expr(Dialect::MySQL), b"count ( * )"),
            FunctionExpr::CountStar
        );
    }

    mod mysql {
        use super::*;

        #[test]
        fn cast() {
            let qs = b"cast(`lp`.`start_ddtm` as date)";
            let expected = Expr::Cast {
                expr: Box::new(Expr::Column(Column {
                    table: Some("lp".into()),
                    name: "start_ddtm".into(),
                })),
                ty: SqlType::Date,
                postgres_style: false,
            };
            let res = expression(Dialect::MySQL)(LocatedSpan::new(qs));
            assert_eq!(res.unwrap().1, expected);
        }

        #[test]
        fn simple_generic_function_with_literal() {
            let qlist = [
                "coalesce(\"a\",b,c)".as_bytes(),
                "coalesce (\"a\",b,c)".as_bytes(),
                "coalesce(\"a\" ,b,c)".as_bytes(),
                "coalesce(\"a\", b,c)".as_bytes(),
            ];
            for q in qlist.iter() {
                let res = to_nom_result(function_expr(Dialect::MySQL)(LocatedSpan::new(q)));
                let expected = FunctionExpr::Call {
                    name: "coalesce".into(),
                    arguments: vec![
                        Expr::Literal(Literal::String("a".to_owned())),
                        Expr::Column(Column::from("b")),
                        Expr::Column(Column::from("c")),
                    ],
                };
                assert_eq!(res, Ok((&b""[..], expected)));
            }
        }

        #[test]
        fn table_qualifier() {
            let res1 = test_parse!(column_identifier_no_alias(Dialect::MySQL), b"`t`.`c`");
            let res2 = test_parse!(column_identifier_no_alias(Dialect::MySQL), b"t.c");
            let expected = Column {
                name: "c".into(),
                table: Some("t".into()),
            };
            assert_eq!(res1, expected);
            assert_eq!(res2, expected);
        }

        #[test]
        fn db_table_qualifier() {
            let res1 = test_parse!(column_identifier_no_alias(Dialect::MySQL), b"`db`.`t`.`c`");
            let res2 = test_parse!(column_identifier_no_alias(Dialect::MySQL), b"db.t.c");
            let expected = Column {
                name: "c".into(),
                table: Some(Relation {
                    schema: Some("db".into()),
                    name: "t".into(),
                }),
            };
            assert_eq!(res1, expected);
            assert_eq!(res2, expected);
        }

        #[test]
        fn call_now_without_parens() {
            let res = test_parse!(function_expr(Dialect::MySQL), b"NOW");
            assert_eq!(
                res,
                FunctionExpr::Call {
                    name: "NOW".into(),
                    arguments: vec![]
                }
            );
        }
    }

    mod postgres {
        use super::*;

        test_format_parse_round_trip!(
            rt_field_def_expr(field_definition_expr, Vec<FieldDefinitionExpr>, Dialect::PostgreSQL);
        );

        #[test]
        fn cast() {
            let qs = b"cast(\"lp\".\"start_ddtm\" as date)";
            let expected = Expr::Cast {
                expr: Box::new(Expr::Column(Column {
                    table: Some("lp".into()),
                    name: "start_ddtm".into(),
                })),
                ty: SqlType::Date,
                postgres_style: false,
            };
            let res = expression(Dialect::PostgreSQL)(LocatedSpan::new(qs));
            assert_eq!(res.unwrap().1, expected);
        }

        #[test]
        fn simple_generic_function_with_literal() {
            let qlist = [
                "coalesce('a',b,c)".as_bytes(),
                "coalesce ('a',b,c)".as_bytes(),
                "coalesce('a' ,b,c)".as_bytes(),
                "coalesce('a', b,c)".as_bytes(),
            ];
            for q in qlist.iter() {
                let res = to_nom_result(function_expr(Dialect::PostgreSQL)(LocatedSpan::new(q)));
                let expected = FunctionExpr::Call {
                    name: "coalesce".into(),
                    arguments: vec![
                        Expr::Literal(Literal::String("a".to_owned())),
                        Expr::Column(Column::from("b")),
                        Expr::Column(Column::from("c")),
                    ],
                };
                assert_eq!(res, Ok((&b""[..], expected)));
            }
        }

        #[test]
        fn table_qualifier() {
            let res1 = test_parse!(
                column_identifier_no_alias(Dialect::PostgreSQL),
                b"\"t\".\"c\""
            );
            let res2 = test_parse!(column_identifier_no_alias(Dialect::PostgreSQL), b"t.c");
            let expected = Column {
                name: "c".into(),
                table: Some("t".into()),
            };
            assert_eq!(res1, expected);
            assert_eq!(res2, expected);
        }

        #[test]
        fn db_table_qualifier() {
            let res1 = test_parse!(
                column_identifier_no_alias(Dialect::PostgreSQL),
                b"\"db\".\"t\".\"c\""
            );
            let res2 = test_parse!(column_identifier_no_alias(Dialect::PostgreSQL), b"db.t.c");
            let expected = Column {
                name: "c".into(),
                table: Some(Relation {
                    schema: Some("db".into()),
                    name: "t".into(),
                }),
            };
            assert_eq!(res1, expected);
            assert_eq!(res2, expected);
        }
    }
}
