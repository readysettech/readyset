use std::fmt::{self, Debug, Display};
use std::hash::Hash;
use std::ops::{Range, RangeFrom, RangeTo};
use std::str;
use std::str::FromStr;

use itertools::Itertools;
use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case, take_until};
use nom::character::complete::{digit1, line_ending};
use nom::combinator::{map, map_res, opt};
use nom::error::{ErrorKind, ParseError};
use nom::multi::{separated_list0, separated_list1};
use nom::sequence::{delimited, pair, preceded, separated_pair, terminated, tuple};
use nom::{IResult, InputLength};
use serde::{Deserialize, Serialize};

use crate::column::Column;
use crate::dialect::Dialect;
use crate::expression::expression;
use crate::table::Relation;
use crate::whitespace::{whitespace0, whitespace1};
use crate::{Expr, FunctionExpr, Literal, SqlIdentifier};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
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

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
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

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum TableKey {
    PrimaryKey {
        name: Option<SqlIdentifier>,
        columns: Vec<Column>,
    },
    UniqueKey {
        name: Option<SqlIdentifier>,
        columns: Vec<Column>,
        index_type: Option<IndexType>,
    },
    FulltextKey {
        name: Option<SqlIdentifier>,
        columns: Vec<Column>,
    },
    Key {
        name: Option<SqlIdentifier>,
        columns: Vec<Column>,
        index_type: Option<IndexType>,
    },
    ForeignKey {
        name: Option<SqlIdentifier>,
        index_name: Option<SqlIdentifier>,
        columns: Vec<Column>,
        target_table: Relation,
        target_columns: Vec<Column>,
        on_delete: Option<ReferentialAction>,
        on_update: Option<ReferentialAction>,
    },
    CheckConstraint {
        name: Option<SqlIdentifier>,
        expr: Expr,
        enforced: Option<bool>,
    },
}

impl fmt::Display for TableKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TableKey::PrimaryKey { name, columns } => {
                write!(f, "PRIMARY KEY ")?;
                if let Some(name) = name {
                    write!(f, "`{}` ", name)?;
                }
                write!(
                    f,
                    "({})",
                    columns
                        .iter()
                        .map(|c| format!("`{}`", c.name))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
            TableKey::UniqueKey {
                name,
                columns,
                index_type,
            } => {
                write!(f, "UNIQUE KEY ")?;
                if let Some(ref name) = *name {
                    write!(f, "`{}` ", name)?;
                }
                write!(
                    f,
                    "({})",
                    columns
                        .iter()
                        .map(|c| format!("`{}`", c.name))
                        .collect::<Vec<_>>()
                        .join(", ")
                )?;
                if let Some(index_type) = index_type {
                    write!(f, " USING {}", index_type)?;
                }
                Ok(())
            }
            TableKey::FulltextKey { name, columns } => {
                write!(f, "FULLTEXT KEY ")?;
                if let Some(ref name) = *name {
                    write!(f, "`{}` ", name)?;
                }
                write!(
                    f,
                    "({})",
                    columns
                        .iter()
                        .map(|c| format!("`{}`", c.name))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
            TableKey::Key {
                name,
                columns,
                index_type,
            } => {
                write!(f, "KEY ")?;
                if let Some(name) = name {
                    write!(f, "`{name}` ")?;
                }
                write!(
                    f,
                    "({})",
                    columns
                        .iter()
                        .map(|c| format!("`{}`", c.name))
                        .collect::<Vec<_>>()
                        .join(", ")
                )?;
                if let Some(index_type) = index_type {
                    write!(f, " USING {}", index_type)?;
                }
                Ok(())
            }
            TableKey::ForeignKey {
                name,
                index_name,
                columns: column,
                target_table,
                target_columns: target_column,
                on_delete,
                on_update,
            } => {
                write!(
                    f,
                    "CONSTRAINT `{}` FOREIGN KEY {}({}) REFERENCES {} ({})",
                    name.as_deref().unwrap_or(""),
                    index_name.as_deref().unwrap_or(""),
                    column.iter().map(|c| format!("`{}`", c.name)).join(", "),
                    target_table,
                    target_column
                        .iter()
                        .map(|c| format!("`{}`", c.name))
                        .join(", ")
                )?;
                if let Some(on_delete) = on_delete {
                    write!(f, " ON DELETE {}", on_delete)?;
                }
                if let Some(on_update) = on_update {
                    write!(f, " ON UPDATE {}", on_update)?;
                }
                Ok(())
            }
            TableKey::CheckConstraint {
                name,
                expr,
                enforced,
            } => {
                write!(f, "CONSTRAINT",)?;
                if let Some(name) = name {
                    write!(f, " `{}`", name)?;
                }

                write!(f, " CHECK {}", expr)?;

                if let Some(enforced) = enforced {
                    if !enforced {
                        write!(f, " NOT")?;
                    }
                    write!(f, " ENFORCED")?;
                }

                Ok(())
            }
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, PartialOrd, Serialize, Deserialize)]
#[allow(clippy::large_enum_variant)] // NOTE(grfn): do we actually care about this?
pub enum FieldDefinitionExpr {
    All,
    AllInTable(Relation),
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

impl Display for FieldDefinitionExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FieldDefinitionExpr::All => write!(f, "*"),
            FieldDefinitionExpr::AllInTable(ref table) => {
                write!(f, "{}.*", table)
            }
            FieldDefinitionExpr::Expr { expr, alias } => {
                write!(f, "{}", expr)?;
                if let Some(alias) = alias {
                    write!(f, " AS `{}`", alias)?;
                }
                Ok(())
            }
        }
    }
}

impl Default for FieldDefinitionExpr {
    fn default() -> FieldDefinitionExpr {
        FieldDefinitionExpr::All
    }
}

pub enum Sign {
    Unsigned,
    Signed,
}

/// A reference to a field in a query, usable in either the `GROUP BY` or `ORDER BY` clauses of the
/// query
#[derive(Clone, Debug, Eq, Hash, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum FieldReference {
    /// A reference to a field in the `SELECT` list by its (1-based) index.
    Numeric(u64),
    /// An expression
    Expr(Expr),
}

impl Display for FieldReference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FieldReference::Numeric(n) => write!(f, "{}", n),
            FieldReference::Expr(expr) => write!(f, "{}", expr),
        }
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
pub fn agg_function_arguments(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], (Expr, bool)> {
    move |i| {
        let distinct_parser = opt(tuple((tag_no_case("distinct"), whitespace1)));
        let (remaining_input, (distinct, args)) = tuple((distinct_parser, expression(dialect)))(i)?;
        Ok((remaining_input, (args, distinct.is_some())))
    }
}

fn group_concat_fx_helper(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], String> {
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

fn group_concat_fx(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], (Column, Option<String>)> {
    move |i| {
        pair(
            column_identifier_no_alias(dialect),
            opt(group_concat_fx_helper(dialect)),
        )(i)
    }
}

fn agg_fx_args(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], (Expr, bool)> {
    move |i| delimited(tag("("), agg_function_arguments(dialect), tag(")"))(i)
}

fn delim_fx_args(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], Vec<Expr>> {
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

fn function_call_without_parens(i: &[u8]) -> IResult<&[u8], FunctionExpr> {
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
        |n: &[u8]| {
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

fn function_call(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], FunctionExpr> {
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

pub fn function_expr(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], FunctionExpr> {
    move |i| {
        alt((
            map(tag_no_case("count(*)"), |_| FunctionExpr::CountStar),
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
                    delimited(tag("("), group_concat_fx(dialect), tag(")")),
                ),
                |spec| {
                    let (ref col, sep) = spec;
                    let separator = match sep {
                        // default separator is a comma, see MySQL manual §5.7
                        None => String::from(","),
                        Some(s) => s,
                    };
                    FunctionExpr::GroupConcat {
                        expr: Box::new(Expr::Column(col.clone())),
                        separator,
                    }
                },
            ),
            function_call(dialect),
            function_call_without_parens,
        ))(i)
    }
}

// Parses a SQL column identifier in the db/schema.table.column format
pub fn column_identifier_no_alias(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], Column> {
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

// Parse a terminator that ends a SQL statement.
pub fn statement_terminator(i: &[u8]) -> IResult<&[u8], ()> {
    let (remaining_input, _) =
        delimited(whitespace0, alt((tag(";"), line_ending, eof)), whitespace0)(i)?;

    Ok((remaining_input, ()))
}

/// Parser combinator that applies the given parser,
/// and then tries to match for a statement terminator.
pub fn terminated_with_statement_terminator<F, O>(
    parser: F,
) -> impl FnOnce(&[u8]) -> IResult<&[u8], O>
where
    F: Fn(&[u8]) -> IResult<&[u8], O>,
{
    move |i| terminated(parser, statement_terminator)(i)
}

// Parse rule for AS-based aliases for SQL entities.
pub fn as_alias(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], SqlIdentifier> {
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

fn assignment_expr(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], (Column, Expr)> {
    move |i| {
        separated_pair(
            column_identifier_no_alias(dialect),
            delimited(whitespace0, tag("="), whitespace0),
            expression(dialect),
        )(i)
    }
}

/// Whitespace surrounded optionally on either side by a comma
pub(crate) fn ws_sep_comma(i: &[u8]) -> IResult<&[u8], &[u8]> {
    delimited(whitespace0, tag(","), whitespace0)(i)
}

pub(crate) fn ws_sep_equals<I, E>(i: I) -> IResult<I, I, E>
where
    E: ParseError<I>,
    I: nom::InputTakeAtPosition
        + nom::InputTake
        + nom::Compare<&'static str>
        + nom::FindSubstring<&'static str>
        + nom::Slice<Range<usize>>
        + nom::Slice<RangeTo<usize>>
        + nom::Slice<RangeFrom<usize>>
        + nom::InputIter
        + InputLength
        + Default
        + Clone
        + PartialEq,
    &'static str: nom::FindToken<<I as nom::InputTakeAtPosition>::Item>,
    <I as nom::InputIter>::Item: nom::AsChar + Clone,
    // Compare required by tag
    <I as nom::InputTakeAtPosition>::Item: nom::AsChar + Clone,
    // AsChar and Clone required by whitespace0
{
    delimited(whitespace0, tag("="), whitespace0)(i)
}

pub fn assignment_expr_list(
    dialect: Dialect,
) -> impl Fn(&[u8]) -> IResult<&[u8], Vec<(Column, Expr)>> {
    move |i| separated_list1(ws_sep_comma, assignment_expr(dialect))(i)
}

// Parse rule for a comma-separated list of fields without aliases.
pub fn field_list(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], Vec<Column>> {
    move |i| separated_list0(ws_sep_comma, column_identifier_no_alias(dialect))(i)
}

fn expression_field(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], FieldDefinitionExpr> {
    move |i| {
        let (i, expr) = expression(dialect)(i)?;
        let (i, alias) = opt(as_alias(dialect))(i)?;
        Ok((i, FieldDefinitionExpr::Expr { expr, alias }))
    }
}

fn all_in_table(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], FieldDefinitionExpr> {
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
) -> impl Fn(&[u8]) -> IResult<&[u8], Vec<FieldDefinitionExpr>> {
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
pub fn value_list(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], Vec<Expr>> {
    move |i| separated_list0(ws_sep_comma, expression(dialect))(i)
}

pub(crate) fn if_not_exists(i: &[u8]) -> IResult<&[u8], bool> {
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
pub fn parse_comment(i: &[u8]) -> IResult<&[u8], String> {
    map(
        preceded(
            delimited(whitespace0, tag_no_case("comment"), whitespace1),
            map_res(
                delimited(tag("'"), take_until("'"), tag("'")),
                str::from_utf8,
            ),
        ),
        String::from,
    )(i)
}

pub fn field_reference(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], FieldReference> {
    move |i| {
        match dialect {
            Dialect::PostgreSQL => map(expression(dialect), FieldReference::Expr)(i),
            // Only MySQL supports numeric field references (postgresql considers them integer
            // literals, I'm pretty sure)
            Dialect::MySQL => alt((
                map(
                    map_res(map_res(digit1, str::from_utf8), u64::from_str),
                    FieldReference::Numeric,
                ),
                map(expression(dialect), FieldReference::Expr),
            ))(i),
        }
    }
}

pub fn field_reference_list(
    dialect: Dialect,
) -> impl Fn(&[u8]) -> IResult<&[u8], Vec<FieldReference>> {
    move |i| separated_list0(ws_sep_comma, field_reference(dialect))(i)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SqlType;

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
            separator: ", ".to_owned(),
        };
        let res = function_expr(Dialect::MySQL)(qs);
        assert_eq!(res.unwrap().1, expected);
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
            let res = function_expr(Dialect::MySQL)(q);
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
        let (_, res) = function_expr(Dialect::MySQL)(b"ifnull(x, 0)").unwrap();
        assert_eq!(
            res,
            FunctionExpr::Call {
                name: "ifnull".into(),
                arguments: vec![
                    Expr::Column(Column::from("x")),
                    Expr::Literal(Literal::UnsignedInteger(0))
                ]
            }
        );
    }

    #[test]
    fn comment_data() {
        let res = parse_comment(b" COMMENT 'test'");
        assert_eq!(res.unwrap().1, "test");
    }

    #[test]
    fn terminated_by_semicolon() {
        let res = statement_terminator(b"   ;  ");
        assert_eq!(res, Ok((&b""[..], ())));
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
            let res = expression(Dialect::MySQL)(qs);
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
                let res = function_expr(Dialect::MySQL)(q);
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
            let res = expression(Dialect::PostgreSQL)(qs);
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
                let res = function_expr(Dialect::PostgreSQL)(q);
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
