use std::ops::{Range, RangeFrom, RangeTo};
use std::str;
use std::str::FromStr;

use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case};
use nom::character::complete::{char, digit1, line_ending};
use nom::combinator::{map, map_res, not, opt, peek};
use nom::error::{ErrorKind, ParseError};
use nom::multi::{separated_list0, separated_list1};
use nom::sequence::{delimited, pair, preceded, separated_pair, terminated, tuple};
use nom::{IResult, InputLength, InputTake};
use nom_locate::LocatedSpan;
use readyset_sql::{ast::*, Dialect};

use crate::dialect::DialectParser;
use crate::expression::expression;
use crate::whitespace::{whitespace0, whitespace1};
use crate::NomSqlResult;

#[cfg(feature = "debug")]
pub fn debug_print(tag: &str, i: &[u8]) {
    eprintln!("{}: {}", tag, String::from_utf8_lossy(i))
}

#[cfg(not(feature = "debug"))]
pub fn debug_print(_tag: &str, _i: &[u8]) {}

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

#[allow(clippy::type_complexity)]
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
        let (i, _) = tag("(")(i)?;
        let (i, _) = whitespace0(i)?;
        let (i, args) = agg_function_arguments(dialect)(i)?;
        let (i, _) = whitespace0(i)?;
        let (i, _) = tag(")")(i)?;
        Ok((i, args))
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

fn function_call_without_parens(
    _dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], FunctionExpr> {
    move |i| {
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
                    .to_lowercase()
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
}

fn timestamp_field() -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], TimestampField> {
    move |i| {
        let alt1 = alt((
            map(tag_no_case("day"), |_| TimestampField::Day),
            map(tag_no_case("dow"), |_| TimestampField::Dow),
            map(tag_no_case("doy"), |_| TimestampField::Doy),
            map(tag_no_case("week"), |_| TimestampField::Week),
            map(tag_no_case("month"), |_| TimestampField::Month),
            map(tag_no_case("year"), |_| TimestampField::Year),
            map(tag_no_case("hour"), |_| TimestampField::Hour),
            map(tag_no_case("minute"), |_| TimestampField::Minute),
            map(tag_no_case("second"), |_| TimestampField::Second),
            map(tag_no_case("milliseconds"), |_| {
                TimestampField::Milliseconds
            }),
            map(tag_no_case("microseconds"), |_| {
                TimestampField::Microseconds
            }),
            map(tag_no_case("quarter"), |_| TimestampField::Quarter),
            map(tag_no_case("century"), |_| TimestampField::Century),
            map(tag_no_case("decade"), |_| TimestampField::Decade),
            map(tag_no_case("epoch"), |_| TimestampField::Epoch),
            map(tag_no_case("timezone_hour"), |_| {
                TimestampField::TimezoneHour
            }),
            map(tag_no_case("timezone_minute"), |_| {
                TimestampField::TimezoneMinute
            }),
            map(tag_no_case("timezone"), |_| TimestampField::Timezone),
            map(tag_no_case("isodow"), |_| TimestampField::Isodow),
            map(tag_no_case("isoyear"), |_| TimestampField::Isoyear),
            map(tag_no_case("julian"), |_| TimestampField::Julian),
        ));

        // `alt` has an upper limit on the number of items it supports in tuples, so we have to
        // split the parsing for these fields into separate invocations
        alt((
            alt1,
            map(tag_no_case("millennium"), |_| TimestampField::Millennium),
        ))(i)
    }
}

fn extract(dialect: Dialect) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], FunctionExpr> {
    move |i| {
        let (i, _) = tag_no_case("EXTRACT")(i)?;
        let (i, _) = whitespace0(i)?;
        let (i, _) = char('(')(i)?;
        let (i, _) = whitespace0(i)?;
        let (i, field) = timestamp_field()(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, _) = tag_no_case("FROM")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, expr) = expression(dialect)(i)?;
        let (i, _) = whitespace0(i)?;
        let (i, _) = char(')')(i)?;

        Ok((
            i,
            FunctionExpr::Extract {
                field,
                expr: Box::new(expr),
            },
        ))
    }
}

fn parse_lower_upper_func_body(
    func_name: String,
    i: LocatedSpan<&[u8]>,
    dialect: Dialect,
) -> NomSqlResult<&[u8], (Expr, Option<CollationName>)> {
    let (i, _) = tag_no_case(func_name.as_str())(i)?;
    let (i, _) = whitespace0(i)?;
    let (i, _) = char('(')(i)?;
    let (i, _) = whitespace0(i)?;
    let (i, expr) = expression(dialect)(i)?;
    let (i, _) = whitespace0(i)?;
    let (i, collation) = if dialect == Dialect::PostgreSQL {
        opt(preceded(
            tuple((tag_no_case("COLLATE"), whitespace0)),
            collation_name(dialect),
        ))(i)?
    } else {
        (i, None)
    };
    let (i, _) = char(')')(i)?;

    Ok((i, (expr, collation)))
}

fn lower(dialect: Dialect) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], FunctionExpr> {
    move |i| {
        let (i, (expr, collation)) = parse_lower_upper_func_body("lower".to_string(), i, dialect)?;
        Ok((
            i,
            FunctionExpr::Lower {
                expr: Box::new(expr),
                collation,
            },
        ))
    }
}

fn upper(dialect: Dialect) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], FunctionExpr> {
    move |i| {
        let (i, (expr, collation)) = parse_lower_upper_func_body("upper".to_string(), i, dialect)?;
        Ok((
            i,
            FunctionExpr::Upper {
                expr: Box::new(expr),
                collation,
            },
        ))
    }
}

fn substring(dialect: Dialect) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], FunctionExpr> {
    move |i| {
        let (i, _) = alt((tag_no_case("substring"), tag_no_case("substr")))(i)?;
        let (i, _) = whitespace0(i)?;
        let (i, _) = tag("(")(i)?;
        let (i, _) = whitespace0(i)?;
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
                name: name.to_lowercase().into(),
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
            map(
                tuple((
                    alt((
                        map(tag_no_case("json_object_agg"), |_| true),   //psql
                        map(tag_no_case("jsonb_object_agg"), |_| false), //psql
                        map(tag_no_case("json_objectagg"), |_| false),   //mysql, jsonb_objectagg
                                                                         //not supported
                    )),
                    preceded(
                        whitespace0,
                        delimited(
                            terminated(tag("("), whitespace0),
                            tuple((
                                expression(dialect),
                                preceded(whitespace0, tag(",")),
                                expression(dialect),
                            )),
                            preceded(whitespace0, tag(")")),
                        ),
                    ),
                )),
                |(is_json, (key, _, value))| FunctionExpr::JsonObjectAgg {
                    key: Box::new(key),
                    value: Box::new(value),
                    allow_duplicate_keys: is_json,
                },
            ),
            extract(dialect),
            lower(dialect),
            upper(dialect),
            substring(dialect),
            function_call(dialect),
            function_call_without_parens(dialect),
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

#[allow(clippy::type_complexity)]
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
        separated_list0(
            ws_sep_comma,
            alt((
                map(tag("*"), |_| FieldDefinitionExpr::All),
                all_in_table(dialect),
                expression_field(dialect),
            )),
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
pub fn parse_comment(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], String> {
    move |i| {
        map(
            preceded(
                delimited(whitespace0, tag_no_case("comment"), whitespace1),
                map_res(move |i| dialect.string_literal()(i), String::from_utf8),
            ),
            String::from,
        )(i)
    }
}

pub fn field_reference(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], FieldReference> {
    move |i| {
        alt((
            map(
                map_res(
                    map_res(digit1, |i: LocatedSpan<&[u8]>| str::from_utf8(&i)),
                    u64::from_str,
                ),
                FieldReference::Numeric,
            ),
            map(expression(dialect), FieldReference::Expr),
        ))(i)
    }
}

pub fn field_reference_list(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Vec<FieldReference>> {
    move |i| separated_list0(ws_sep_comma, field_reference(dialect))(i)
}

pub(crate) fn charset_name(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], CharsetName> {
    move |i| {
        alt((
            map(dialect.identifier(), CharsetName::Unquoted),
            map(map_res(dialect.string_literal(), String::from_utf8), |s| {
                CharsetName::Quoted(SqlIdentifier::from(s))
            }),
        ))(i)
    }
}

pub(crate) fn collation_name(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], CollationName> {
    move |i| {
        alt((
            map(dialect.identifier(), CollationName::Unquoted),
            map(map_res(dialect.string_literal(), String::from_utf8), |s| {
                CollationName::Quoted(SqlIdentifier::from(s))
            }),
        ))(i)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use readyset_sql::DialectDisplay;
    use test_utils::tags;

    use crate::to_nom_result;

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

    #[test]
    fn disallow_trailing_comma_in_column_list() {
        let expected = [
            FieldDefinitionExpr::Expr {
                expr: Expr::Column(Column {
                    name: SqlIdentifier::from("a"),
                    table: None,
                }),
                alias: None,
            },
            FieldDefinitionExpr::Expr {
                expr: Expr::Column(Column {
                    name: SqlIdentifier::from("b"),
                    table: None,
                }),
                alias: None,
            },
            FieldDefinitionExpr::Expr {
                expr: Expr::Column(Column {
                    name: SqlIdentifier::from("c"),
                    table: None,
                }),
                alias: None,
            },
        ];

        assert_eq!(
            test_parse!(field_definition_expr(Dialect::MySQL), b"a, b, c"),
            expected
        );

        let (rem, _) = field_definition_expr(Dialect::MySQL)("a, b, c,".as_bytes().into()).unwrap();
        assert!(!rem.is_empty());
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
                    name: "now".into(),
                    arguments: vec![]
                }
            );
        }

        #[test]
        fn table_column_comment() {
            let res = test_parse!(parse_comment(Dialect::MySQL), b"comment 'foo'");
            assert_eq!(res, "foo");
            let res = test_parse!(parse_comment(Dialect::MySQL), b"comment \"foo\"");
            assert_eq!(res, "foo");
            let res = test_parse!(parse_comment(Dialect::MySQL), b"comment 'f''oo'");
            assert_eq!(res, "f'oo");
            let res = test_parse!(parse_comment(Dialect::MySQL), b"comment 'f\"\"oo'");
            assert_eq!(res, "f\"\"oo");
            let res = test_parse!(parse_comment(Dialect::MySQL), b"comment \"f\"\"oo\"");
            assert_eq!(res, "f\"oo");
            let res = test_parse!(parse_comment(Dialect::MySQL), b"comment \"f''oo\"");
            assert_eq!(res, "f''oo");
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

    mod extract {
        use super::*;

        macro_rules! extract_test {
            ($field:ident, $field_variant:ident, $field_expr:expr) => {
                mod $field {
                    use super::*;

                    #[test]
                    fn parse_extract_expr() {
                        let expr = format!("extract({} FROM \"col\")", $field_expr);
                        assert_eq!(
                            test_parse!(extract(Dialect::PostgreSQL), expr.as_bytes()),
                            FunctionExpr::Extract {
                                field: TimestampField::$field_variant,
                                expr: Box::new(Expr::Column(Column {
                                    name: "col".into(),
                                    table: None,
                                })),
                            },
                        );
                    }

                    #[test]
                    fn format_round_trip() {
                        let expected = format!("extract({} FROM \"col\")", $field_expr);
                        let actual = test_parse!(extract(Dialect::PostgreSQL), expected.as_bytes())
                            .display(Dialect::PostgreSQL)
                            .to_string();

                        assert_eq!(expected, actual);
                    }
                }
            };
        }

        extract_test!(century, Century, "CENTURY");
        extract_test!(decade, Decade, "DECADE");
        extract_test!(dow, Dow, "DOW");
        extract_test!(doy, Doy, "DOY");
        extract_test!(epoch, Epoch, "EPOCH");
        extract_test!(hour, Hour, "HOUR");
        extract_test!(isodow, Isodow, "ISODOW");
        extract_test!(isoyear, Isoyear, "ISOYEAR");
        extract_test!(julian, Julian, "JULIAN");
        extract_test!(microseconds, Microseconds, "MICROSECONDS");
        extract_test!(millennium, Millennium, "MILLENNIUM");
        extract_test!(milliseconds, Milliseconds, "MILLISECONDS");
        extract_test!(minute, Minute, "MINUTE");
        extract_test!(month, Month, "MONTH");
        extract_test!(quarter, Quarter, "QUARTER");
        extract_test!(second, Second, "SECOND");
        extract_test!(timezone_hour, TimezoneHour, "TIMEZONE_HOUR");
        extract_test!(timezone_minute, TimezoneMinute, "TIMEZONE_MINUTE");
        extract_test!(timezone, Timezone, "TIMEZONE");
        extract_test!(week, Week, "WEEK");
        extract_test!(year, Year, "YEAR");
    }

    mod lower_upper {
        use super::*;
        #[test]
        fn test_lower() {
            fn test(dialect: Dialect, func_name: &str, val: &str, collate: Option<&str>) {
                let expected = format!(
                    "{}(\'{}\'{})",
                    func_name,
                    val,
                    if let Some(collation_name) = collate {
                        format!(" COLLATE \"{collation_name}\"")
                    } else {
                        "".to_string()
                    }
                );
                let actual = if func_name.eq_ignore_ascii_case("lower") {
                    test_parse!(lower(dialect), expected.as_bytes())
                } else {
                    test_parse!(upper(dialect), expected.as_bytes())
                }
                .display(dialect)
                .to_string();
                assert_eq!(expected, actual, "dialect: {dialect}");
            }

            test(Dialect::PostgreSQL, "lower", "AbC", Some("es_ES"));
            test(Dialect::PostgreSQL, "lower", "AbC", None);

            test(Dialect::PostgreSQL, "upper", "AbC", Some("es_ES"));
            test(Dialect::PostgreSQL, "upper", "AbC", None);

            test(Dialect::MySQL, "lower", "AbC", None);
            test(Dialect::MySQL, "upper", "AbC", None);
        }
    }
}
