use std::str;
use std::str::FromStr;

use failpoint_macros::set_failpoint;
use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case};
use nom::character::complete::digit1;
#[cfg(feature = "failure_injection")]
use nom::combinator::fail;
use nom::combinator::{map, map_parser, opt, value};
use nom::error::{ErrorKind, ParseError};
use nom::multi::{fold_many0, separated_list0};
use nom::sequence::{delimited, preceded, terminated, tuple};
use nom_locate::LocatedSpan;
use readyset_sql::{ast::*, Dialect};
#[cfg(feature = "failure_injection")]
use readyset_util::failpoints;

use crate::common::ws_sep_comma;
use crate::dialect::DialectParser;
use crate::table::relation;
use crate::whitespace::{whitespace0, whitespace1};
use crate::NomSqlResult;

fn digit_as_u16(len: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], u16> {
    match str::from_utf8(&len) {
        Ok(s) => match u16::from_str(s) {
            Ok(v) => Ok((len, v)),
            Err(_) => Err(nom::Err::Error(ParseError::from_error_kind(
                len,
                ErrorKind::LengthValue,
            ))),
        },
        Err(_) => Err(nom::Err::Error(ParseError::from_error_kind(
            len,
            ErrorKind::LengthValue,
        ))),
    }
}

fn digit_as_u8(len: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], u8> {
    match str::from_utf8(&len) {
        Ok(s) => match u8::from_str(s) {
            Ok(v) => Ok((len, v)),
            Err(_) => Err(nom::Err::Error(ParseError::from_error_kind(
                len,
                ErrorKind::LengthValue,
            ))),
        },
        Err(_) => Err(nom::Err::Error(ParseError::from_error_kind(
            len,
            ErrorKind::LengthValue,
        ))),
    }
}

fn precision_helper(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], (u8, Option<u8>)> {
    let (remaining_input, (m, d)) = tuple((
        digit1,
        opt(preceded(tag(","), preceded(whitespace0, digit1))),
    ))(i)?;

    let m = digit_as_u8(m)?.1;
    // Manual map allowed for nom error propagation.
    #[allow(clippy::manual_map)]
    let d = match d {
        None => None,
        Some(v) => Some(digit_as_u8(v)?.1),
    };

    Ok((remaining_input, (m, d)))
}

pub fn precision(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], (u8, Option<u8>)> {
    delimited(tag("("), precision_helper, tag(")"))(i)
}

pub fn numeric_precision(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], (u16, Option<u8>)> {
    delimited(tag("("), numeric_precision_inner, tag(")"))(i)
}

pub fn numeric_precision_inner(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], (u16, Option<u8>)> {
    let (remaining_input, (m, _, d)) = tuple((
        digit1,
        whitespace0,
        opt(preceded(tag(","), preceded(whitespace0, digit1))),
    ))(i)?;

    let m = digit_as_u16(m)?.1;
    d.map(digit_as_u8)
        .transpose()
        .map(|v| (remaining_input, (m, v.map(|digit| digit.1))))
}

fn opt_signed(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Option<Sign>> {
    opt(alt((
        map(tag_no_case("unsigned"), |_| Sign::Unsigned),
        map(tag_no_case("signed"), |_| Sign::Signed),
    )))(i)
}

fn delim_digit(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], LocatedSpan<&[u8]>> {
    delimited(tag("("), digit1, tag(")"))(i)
}

fn delim_u16(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], u16> {
    map_parser(delim_digit, digit_as_u16)(i)
}

fn int_type<'a, F, G>(
    tag: &str,
    mk_unsigned: F,
    mk_signed: G,
    i: LocatedSpan<&'a [u8]>,
) -> NomSqlResult<&'a [u8], SqlType>
where
    F: Fn(Option<u16>) -> SqlType + 'static,
    G: Fn(Option<u16>) -> SqlType + 'static,
{
    let (remaining_input, (_, len, _, signed)) =
        tuple((tag_no_case(tag), opt(delim_u16), whitespace0, opt_signed))(i)?;

    if let Some(Sign::Unsigned) = signed {
        Ok((remaining_input, mk_unsigned(len)))
    } else {
        Ok((remaining_input, mk_signed(len)))
    }
}

// TODO(malte): not strictly ok to treat DECIMAL and NUMERIC as identical; the
// former has "at least" M precision, the latter "exactly".
// See https://dev.mysql.com/doc/refman/5.7/en/precision-math-decimal-characteristics.html
fn decimal_or_numeric(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], SqlType> {
    let (i, _) = alt((tag_no_case("decimal"), tag_no_case("numeric")))(i)?;
    let (i, precision) = opt(precision)(i)?;
    let (i, _) = whitespace0(i)?;
    // Parse and drop sign
    let (i, _) = opt_signed(i)?;

    match precision {
        None => Ok((i, SqlType::Decimal(32, 0))),
        Some((m, None)) => Ok((i, SqlType::Decimal(m, 0))),
        Some((m, Some(d))) => Ok((i, SqlType::Decimal(m, d))),
    }
}

fn opt_without_time_zone(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], ()> {
    map(
        opt(preceded(
            whitespace1,
            tuple((
                tag_no_case("without"),
                whitespace1,
                tag_no_case("time"),
                whitespace1,
                tag_no_case("zone"),
            )),
        )),
        |_| (),
    )(i)
}

fn enum_type(dialect: Dialect) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], SqlType> {
    move |i| {
        let (i, _) = tag_no_case("enum")(i)?;
        let (i, _) = whitespace0(i)?;
        let (i, _) = tag("(")(i)?;
        let (i, _) = whitespace0(i)?;
        let (i, variants) = separated_list0(ws_sep_comma, dialect.utf8_string_literal())(i)?;
        let (i, _) = whitespace0(i)?;
        let (i, _) = tag(")")(i)?;
        Ok((i, SqlType::from_enum_variants(variants)))
    }
}

fn interval_fields(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], IntervalFields> {
    alt((
        value(
            IntervalFields::YearToMonth,
            tuple((
                tag_no_case("YEAR"),
                whitespace1,
                tag_no_case("TO"),
                whitespace1,
                tag_no_case("MONTH"),
            )),
        ),
        value(
            IntervalFields::DayToHour,
            tuple((
                tag_no_case("DAY"),
                whitespace1,
                tag_no_case("TO"),
                whitespace1,
                tag_no_case("HOUR"),
            )),
        ),
        value(
            IntervalFields::DayToMinute,
            tuple((
                tag_no_case("DAY"),
                whitespace1,
                tag_no_case("TO"),
                whitespace1,
                tag_no_case("MINUTE"),
            )),
        ),
        value(
            IntervalFields::DayToSecond,
            tuple((
                tag_no_case("DAY"),
                whitespace1,
                tag_no_case("TO"),
                whitespace1,
                tag_no_case("SECOND"),
            )),
        ),
        value(
            IntervalFields::HourToMinute,
            tuple((
                tag_no_case("HOUR"),
                whitespace1,
                tag_no_case("TO"),
                whitespace1,
                tag_no_case("MINUTE"),
            )),
        ),
        value(
            IntervalFields::HourToSecond,
            tuple((
                tag_no_case("HOUR"),
                whitespace1,
                tag_no_case("TO"),
                whitespace1,
                tag_no_case("SECOND"),
            )),
        ),
        value(
            IntervalFields::MinuteToSecond,
            tuple((
                tag_no_case("MINUTE"),
                whitespace1,
                tag_no_case("TO"),
                whitespace1,
                tag_no_case("SECOND"),
            )),
        ),
        value(IntervalFields::Year, tag_no_case("YEAR")),
        value(IntervalFields::Month, tag_no_case("MONTH")),
        value(IntervalFields::Day, tag_no_case("DAY")),
        value(IntervalFields::Hour, tag_no_case("HOUR")),
        value(IntervalFields::Minute, tag_no_case("MINUTE")),
        value(IntervalFields::Second, tag_no_case("SECOND")),
    ))(i)
}

fn interval_type(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], SqlType> {
    let (i, _) = tag_no_case("interval")(i)?;
    let (i, fields) = opt(preceded(whitespace1, interval_fields))(i)?;
    let (i, precision) = opt(map(preceded(whitespace0, delim_u16), |x| x as u64))(i)?;

    Ok((i, SqlType::Interval { fields, precision }))
}

// `alt` has an upper limit on the number of items it supports in tuples, so we have to split out
// the parsing for types into 3 separate functions
// (see https://github.com/Geal/nom/pull/1556)
fn type_identifier_part1(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], SqlType> {
    move |i| {
        alt((
            interval_type,
            value(SqlType::Int2, tag_no_case("int2")),
            value(SqlType::Int4, tag_no_case("int4")),
            value(SqlType::Int8, tag_no_case("int8")),
            |i| int_type("tinyint", SqlType::TinyIntUnsigned, SqlType::TinyInt, i),
            |i| int_type("smallint", SqlType::SmallIntUnsigned, SqlType::SmallInt, i),
            cond_fail(dialect == Dialect::MySQL, |i| {
                int_type(
                    "mediumint",
                    SqlType::MediumIntUnsigned,
                    SqlType::MediumInt,
                    i,
                )
            }),
            |i| int_type("integer", SqlType::IntUnsigned, SqlType::Int, i),
            |i| int_type("int", SqlType::IntUnsigned, SqlType::Int, i),
            |i| int_type("bigint", SqlType::BigIntUnsigned, SqlType::BigInt, i),
            map(alt((tag_no_case("boolean"), tag_no_case("bool"))), |_| {
                SqlType::Bool
            }),
            map(
                preceded(tag_no_case("datetime"), opt(delim_u16)),
                SqlType::DateTime,
            ),
            map(tag_no_case("date"), |_| SqlType::Date),
            map(
                tuple((
                    tag_no_case("double"),
                    opt(preceded(whitespace1, tag_no_case("precision"))),
                    whitespace0,
                    // FIXME: postgres does not support this precision field
                    opt(precision),
                    whitespace0,
                    opt_signed,
                )),
                |_| SqlType::Double,
            ),
            map(
                tuple((
                    tag_no_case("numeric"),
                    opt(preceded(whitespace0, numeric_precision)),
                )),
                |t| SqlType::Numeric(t.1),
            ),
            enum_type(dialect),
            map(
                tuple((
                    tag_no_case("float"),
                    whitespace0,
                    opt(precision),
                    whitespace0,
                    opt_signed,
                )),
                |_| SqlType::Float,
            ),
            map(
                tuple((tag_no_case("real"), whitespace0, opt_signed)),
                |_| SqlType::Real,
            ),
            map(tag_no_case("text"), |_| SqlType::Text),
            map(
                tuple((
                    tag_no_case("timestamp"),
                    opt(preceded(whitespace0, delim_digit)),
                    preceded(
                        whitespace1,
                        tuple((
                            tag_no_case("with"),
                            whitespace1,
                            tag_no_case("time"),
                            whitespace1,
                            tag_no_case("zone"),
                        )),
                    ),
                )),
                |_| SqlType::TimestampTz,
            ),
        ))(i)
    }
}

fn type_identifier_part2(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], SqlType> {
    alt((
        map(tag_no_case("timestamptz"), |_| SqlType::TimestampTz),
        map(
            tuple((
                tag_no_case("timestamp"),
                opt(preceded(whitespace0, delim_digit)),
                opt_without_time_zone,
            )),
            |_| SqlType::Timestamp,
        ),
        map(
            tuple((
                alt((
                    // The alt expects the same type to be returned for both entries,
                    // so both have to be tuples with same number of elements
                    tuple((
                        map(tag_no_case("varchar"), |i: LocatedSpan<&[u8]>| *i),
                        map(whitespace0, |_| "".as_bytes()),
                        map(whitespace0, |_| "".as_bytes()),
                    )),
                    tuple((
                        map(tag_no_case("character"), |i: LocatedSpan<&[u8]>| *i),
                        map(whitespace1, |_| "".as_bytes()),
                        map(tag_no_case("varying"), |i: LocatedSpan<&[u8]>| *i),
                    )),
                )),
                opt(delim_u16),
                whitespace0,
                opt(tag_no_case("binary")),
            )),
            |t| SqlType::VarChar(t.1),
        ),
        map(
            tuple((
                alt((tag_no_case("character"), tag_no_case("char"))),
                opt(delim_u16),
                whitespace0,
                opt(tag_no_case("binary")),
            )),
            |t| SqlType::Char(t.1),
        ),
        map(
            terminated(tag_no_case("time"), opt_without_time_zone),
            |_| SqlType::Time,
        ),
        decimal_or_numeric,
        map(
            tuple((tag_no_case("binary"), opt(delim_u16), whitespace0)),
            |t| SqlType::Binary(t.1),
        ),
        map(tag_no_case("blob"), |_| SqlType::Blob),
        map(tag_no_case("longblob"), |_| SqlType::LongBlob),
        map(tag_no_case("mediumblob"), |_| SqlType::MediumBlob),
        map(tag_no_case("mediumtext"), |_| SqlType::MediumText),
        map(tag_no_case("longtext"), |_| SqlType::LongText),
        map(tag_no_case("tinyblob"), |_| SqlType::TinyBlob),
        map(tag_no_case("tinytext"), |_| SqlType::TinyText),
        map(
            tuple((tag_no_case("varbinary"), delim_u16, whitespace0)),
            |t| SqlType::VarBinary(t.1),
        ),
        map(tag_no_case("bytea"), |_| SqlType::ByteArray),
        map(tag_no_case("macaddr"), |_| SqlType::MacAddr),
        map(tag_no_case("inet"), |_| SqlType::Inet),
        map(tag_no_case("uuid"), |_| SqlType::Uuid),
        map(tag_no_case("jsonb"), |_| SqlType::Jsonb),
        map(tag_no_case("json"), |_| SqlType::Json),
    ))(i)
}

fn type_identifier_part3(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], SqlType> {
    move |i| {
        alt((
            map(
                tuple((
                    alt((
                        // The alt expects the same type to be returned for both entries,
                        // so both have to be tuples with same number of elements
                        map(tuple((tag_no_case("varbit"), whitespace0)), |_| ()),
                        map(
                            tuple((
                                tag_no_case("bit"),
                                whitespace1,
                                tag_no_case("varying"),
                                whitespace0,
                            )),
                            |_| (),
                        ),
                    )),
                    opt(delim_u16),
                )),
                |t| SqlType::VarBit(t.1),
            ),
            map(tuple((tag_no_case("bit"), opt(delim_u16))), |t| {
                SqlType::Bit(t.1)
            }),
            map(tag_no_case("serial"), |_| SqlType::Serial),
            map(tag_no_case("bigserial"), |_| SqlType::BigSerial),
            map(tag_no_case("citext"), |_| SqlType::Citext),
            map(tag("\"char\""), |_| SqlType::QuotedChar),
            // if a point is followed by an optional srid attribute, we need to ignore it but
            // we need to parse it out.
            map(
                tuple((
                    tag_no_case("point"),
                    opt(preceded(
                        tuple((whitespace1, tag_no_case("srid"), whitespace1)),
                        digit1,
                    )),
                )),
                |_| SqlType::Point,
            ),
            // look for the postgis syntax of "geometry(point, 4326)"
            map(
                tuple((
                    tag_no_case("geometry"),
                    tuple((
                        whitespace0,
                        tag("("),
                        whitespace0,
                        tag_no_case("point"),
                        opt(tuple((tag(","), whitespace0, digit1))),
                        whitespace0,
                        tag(")"),
                    )),
                )),
                |_| SqlType::PostgisPoint,
            ),
            map(
                tuple((
                    tag_no_case("geometry"),
                    tuple((
                        whitespace0,
                        tag("("),
                        whitespace0,
                        tag_no_case("polygon"),
                        opt(tuple((tag(","), whitespace0, digit1))),
                        whitespace0,
                        tag(")"),
                    )),
                )),
                |_| SqlType::PostgisPolygon,
            ),
            map(tag_no_case("tsvector"), |_| SqlType::Tsvector),
            map(other_type(dialect), SqlType::Other),
        ))(i)
    }
}

fn other_type(dialect: Dialect) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Relation> {
    move |i| match dialect {
        Dialect::PostgreSQL => relation(dialect)(i),
        Dialect::MySQL => Err(nom::Err::Error(ParseError::from_error_kind(
            i,
            ErrorKind::IsNot,
        ))),
    }
}

fn array_suffix(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], ()> {
    let (i, _) = tag("[")(i)?;
    let (i, _) = opt(whitespace0)(i)?;
    let (i, _len) = opt(digit1)(i)?;
    let (i, _) = opt(whitespace0)(i)?;
    let (i, _) = tag("]")(i)?;
    Ok((i, ()))
}

fn type_identifier_no_arrays(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], SqlType> {
    move |i| {
        alt((
            type_identifier_part1(dialect),
            type_identifier_part2,
            type_identifier_part3(dialect),
        ))(i)
    }
}

// A SQL type specifier.
pub fn type_identifier(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], SqlType> {
    move |i| {
        set_failpoint!(failpoints::PARSE_SQL_TYPE, |_| fail(i));
        // need to pull a bit of a trick here to properly recursive-descent arrays since they're a
        // suffix. First, we parse the type, then we parse any number of `[]` or `[<n>]` suffixes,
        // and use those to construct the multiple levels of `SqlType::Array`
        let (i, ty) = type_identifier_no_arrays(dialect)(i)?;
        fold_many0(
            array_suffix,
            move || ty.clone(),
            |t, _| SqlType::Array(Box::new(t)),
        )(i)
    }
}

/// For CASTs to integer types, MySQL does not support its own standard type names, and instead
/// only supports a handful of CAST-specific keywords; this function returns a parser for them.
pub fn mysql_int_cast_targets() -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], SqlType> {
    move |i| {
        alt((
            map(
                tuple((
                    tag_no_case("signed"),
                    whitespace0,
                    opt(tag_no_case("integer")),
                )),
                |(_, _, int)| {
                    if int.is_some() {
                        SqlType::SignedInteger
                    } else {
                        SqlType::Signed
                    }
                },
            ),
            map(
                tuple((
                    tag_no_case("unsigned"),
                    whitespace0,
                    opt(tag_no_case("integer")),
                )),
                |(_, _, int)| {
                    if int.is_some() {
                        SqlType::UnsignedInteger
                    } else {
                        SqlType::Unsigned
                    }
                },
            ),
        ))(i)
    }
}

/// Calls the parser if the condition is met; fails otherwise.
///
/// This mirrors the intent of [`nom::combinator::cond`], but for use within [`nom::branch::alt`]:
/// instead of resolving to [`None`] when the condition is not met, it returns a parser error so
/// that this branch fails and another alternative can be selected.
fn cond_fail<F>(pred: bool, f: F) -> impl FnMut(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], SqlType>
where
    F: Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], SqlType>,
{
    move |i| {
        if pred {
            f(i)
        } else {
            Err(nom::Err::Error(ParseError::from_error_kind(
                i,
                ErrorKind::Fail,
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod arbitrary {
        use proptest::arbitrary::any_with;
        use test_strategy::proptest;
        use test_utils::tags;

        use super::*;

        #[tags(no_retry)]
        #[proptest]
        fn dont_generate_arrays(
            #[strategy(any_with::<SqlType>(SqlTypeArbitraryOptions {
                generate_arrays: false,
                ..Default::default()
            }))]
            ty: SqlType,
        ) {
            assert!(!matches!(ty, SqlType::Array(..)))
        }

        #[tags(no_retry)]
        #[proptest]
        fn dont_generate_other(
            #[strategy(any_with::<SqlType>(SqlTypeArbitraryOptions {
                generate_other: false,
                ..Default::default()
            }))]
            ty: SqlType,
        ) {
            assert!(!matches!(ty, SqlType::Other(..)))
        }
    }

    #[test]
    fn sql_types() {
        let type0 = "bigint(20)";
        let type1 = "varchar(255) binary";
        let type2 = "bigint(20) unsigned";
        let type3 = "bigint(20) signed";

        let res = type_identifier(Dialect::MySQL)(LocatedSpan::new(type0.as_bytes()));
        assert_eq!(res.unwrap().1, SqlType::BigInt(Some(20)));
        let res = type_identifier(Dialect::MySQL)(LocatedSpan::new(type1.as_bytes()));
        assert_eq!(res.unwrap().1, SqlType::VarChar(Some(255)));
        let res = type_identifier(Dialect::MySQL)(LocatedSpan::new(type2.as_bytes()));
        assert_eq!(res.unwrap().1, SqlType::BigIntUnsigned(Some(20)));
        let res = type_identifier(Dialect::MySQL)(LocatedSpan::new(type3.as_bytes()));
        assert_eq!(res.unwrap().1, SqlType::BigInt(Some(20)));
        let res = type_identifier(Dialect::MySQL)(LocatedSpan::new(type2.as_bytes()));
        assert_eq!(res.unwrap().1, SqlType::BigIntUnsigned(Some(20)));

        let ok = [
            "bool",
            "integer(16)",
            "datetime(16)",
            "decimal(6,2) unsigned",
            "numeric(2) unsigned",
            "float unsigned",
        ];

        let res_ok: Vec<_> = ok
            .iter()
            .map(|t| {
                type_identifier(Dialect::MySQL)(LocatedSpan::new(t.as_bytes()))
                    .unwrap()
                    .1
            })
            .collect();

        assert_eq!(
            res_ok,
            vec![
                SqlType::Bool,
                SqlType::Int(Some(16)),
                SqlType::DateTime(Some(16)),
                SqlType::Decimal(6, 2),
                SqlType::Numeric(Some((2, None))),
                SqlType::Float,
            ]
        );
    }

    #[test]
    fn boolean_bool() {
        let res = test_parse!(type_identifier(Dialect::PostgreSQL), b"boolean");
        assert_eq!(res, SqlType::Bool);
    }

    #[test]
    fn json_type() {
        for &dialect in Dialect::ALL {
            let res = type_identifier(dialect)(LocatedSpan::new(b"json"));
            assert!(res.is_ok());
            assert_eq!(res.unwrap().1, SqlType::Json);
        }
    }

    #[test]
    fn innermost_array_type() {
        fn nest_array(mut ty: SqlType, dimen: usize) -> SqlType {
            for _ in 0..dimen {
                ty = SqlType::Array(Box::new(ty));
            }
            ty
        }

        for ty in [SqlType::Text, SqlType::Bool, SqlType::Double] {
            for dimen in 0..=5 {
                let arr = nest_array(ty.clone(), dimen);
                assert_eq!(arr.innermost_array_type(), &ty);
            }
        }
    }

    mod mysql {
        use super::*;

        #[test]
        fn double_with_lens() {
            let qs = b"double(16,12)";
            let res = type_identifier(Dialect::MySQL)(LocatedSpan::new(qs));
            assert!(res.is_ok());
            assert_eq!(res.unwrap().1, SqlType::Double);
        }

        #[test]
        fn mediumint() {
            assert_eq!(
                test_parse!(type_identifier(Dialect::MySQL), b"mediumint(8)"),
                SqlType::MediumInt(Some(8))
            );
            assert_eq!(
                test_parse!(type_identifier(Dialect::MySQL), b"mediumint"),
                SqlType::MediumInt(None)
            );
        }
        #[test]
        fn point_type() {
            let res = test_parse!(type_identifier(Dialect::MySQL), b"point");
            assert_eq!(res, SqlType::Point);
        }

        #[test]
        fn point_type_with_srid() {
            let res = test_parse!(type_identifier(Dialect::MySQL), b"point srid 4326");
            assert_eq!(res, SqlType::Point);
        }
    }

    mod postgres {
        use super::*;

        #[test]
        fn numeric() {
            let qs = b"NUMERIC";
            let res = type_identifier(Dialect::PostgreSQL)(LocatedSpan::new(qs));
            assert!(res.is_ok());
            assert_eq!(res.unwrap().1, SqlType::Numeric(None));
        }

        #[test]
        fn numeric_with_precision() {
            let qs = b"NUMERIC(10)";
            let res = type_identifier(Dialect::PostgreSQL)(LocatedSpan::new(qs));
            assert!(res.is_ok());
            assert_eq!(res.unwrap().1, SqlType::Numeric(Some((10, None))));
        }

        #[test]
        fn numeric_with_precision_and_scale() {
            let qs = b"NUMERIC(10, 20)";
            let res = type_identifier(Dialect::PostgreSQL)(LocatedSpan::new(qs));
            assert!(res.is_ok());
            assert_eq!(res.unwrap().1, SqlType::Numeric(Some((10, Some(20)))));
        }

        #[test]
        fn quoted_char_type() {
            let res = test_parse!(type_identifier(Dialect::PostgreSQL), b"\"char\"");
            assert_eq!(res, SqlType::QuotedChar);
        }

        #[test]
        fn macaddr_type() {
            let res = test_parse!(type_identifier(Dialect::PostgreSQL), b"macaddr");
            assert_eq!(res, SqlType::MacAddr);
        }

        #[test]
        fn inet_type() {
            let res = test_parse!(type_identifier(Dialect::PostgreSQL), b"inet");
            assert_eq!(res, SqlType::Inet);
        }

        #[test]
        fn uuid_type() {
            let res = test_parse!(type_identifier(Dialect::PostgreSQL), b"uuid");
            assert_eq!(res, SqlType::Uuid);
        }

        #[test]
        fn json_type() {
            let res = test_parse!(type_identifier(Dialect::PostgreSQL), b"json");
            assert_eq!(res, SqlType::Json);
        }

        #[test]
        fn jsonb_type() {
            let res = test_parse!(type_identifier(Dialect::PostgreSQL), b"jsonb");
            assert_eq!(res, SqlType::Jsonb);
        }

        #[test]
        fn geometry_point_type() {
            let res = test_parse!(type_identifier(Dialect::PostgreSQL), b"geometry(point)");
            assert_eq!(res, SqlType::PostgisPoint);
        }
        #[test]
        fn geometry_point_type_with_srid() {
            let res = test_parse!(
                type_identifier(Dialect::PostgreSQL),
                b"geometry(point, 4326)"
            );
            assert_eq!(res, SqlType::PostgisPoint);
        }

        #[test]
        fn bit_type() {
            let res = test_parse!(type_identifier(Dialect::PostgreSQL), b"bit");
            assert_eq!(res, SqlType::Bit(None));
        }

        #[test]
        fn bit_with_size_type() {
            let res = test_parse!(type_identifier(Dialect::PostgreSQL), b"bit(10)");
            assert_eq!(res, SqlType::Bit(Some(10)));
        }

        #[test]
        fn bit_varying_type() {
            let res = test_parse!(type_identifier(Dialect::PostgreSQL), b"bit varying");
            assert_eq!(res, SqlType::VarBit(None));
        }

        #[test]
        fn bit_varying_with_size_type() {
            let res = test_parse!(type_identifier(Dialect::PostgreSQL), b"bit varying(10)");
            assert_eq!(res, SqlType::VarBit(Some(10)));
        }

        #[test]
        fn timestamp_type() {
            let res = test_parse!(type_identifier(Dialect::PostgreSQL), b"timestamp");
            assert_eq!(res, SqlType::Timestamp);
        }

        #[test]
        fn timestamp_with_prec_type() {
            let res = test_parse!(type_identifier(Dialect::PostgreSQL), b"timestamp (5)");
            assert_eq!(res, SqlType::Timestamp);
        }

        #[test]
        fn timestamp_without_timezone_type() {
            let res = test_parse!(
                type_identifier(Dialect::PostgreSQL),
                b"timestamp without time zone"
            );
            assert_eq!(res, SqlType::Timestamp);
        }

        #[test]
        fn timestamp_with_prec_without_timezone_type() {
            let res = test_parse!(
                type_identifier(Dialect::PostgreSQL),
                b"timestamp (5)   without time zone"
            );
            assert_eq!(res, SqlType::Timestamp);
        }

        #[test]
        fn timestamp_tz_type() {
            let res = test_parse!(
                type_identifier(Dialect::PostgreSQL),
                b"timestamp with time zone"
            );
            assert_eq!(res, SqlType::TimestampTz);
        }

        #[test]
        fn timestamptz_type() {
            let res = test_parse!(type_identifier(Dialect::PostgreSQL), b"timestamptz");
            assert_eq!(res, SqlType::TimestampTz)
        }

        #[test]
        fn timestamp_tz_with_prec_type() {
            let res = test_parse!(
                type_identifier(Dialect::PostgreSQL),
                b"timestamp (5)    with time zone"
            );
            assert_eq!(res, SqlType::TimestampTz);
        }

        #[test]
        fn serial_type() {
            let res = test_parse!(type_identifier(Dialect::PostgreSQL), b"serial");
            assert_eq!(res, SqlType::Serial);
        }

        #[test]
        fn bigserial_type() {
            let res = test_parse!(type_identifier(Dialect::PostgreSQL), b"bigserial");
            assert_eq!(res, SqlType::BigSerial);
        }

        #[test]
        fn varchar_without_length() {
            let res = test_parse!(type_identifier(Dialect::PostgreSQL), b"varchar");
            assert_eq!(res, SqlType::VarChar(None));
        }

        #[test]
        fn character_varying() {
            let res = test_parse!(type_identifier(Dialect::PostgreSQL), b"character varying");
            assert_eq!(res, SqlType::VarChar(None));
        }

        #[test]
        fn character_varying_with_length() {
            let res = test_parse!(
                type_identifier(Dialect::PostgreSQL),
                b"character varying(20)"
            );
            assert_eq!(res, SqlType::VarChar(Some(20)));
        }

        #[test]
        fn character_with_length() {
            let qs = b"character(16)";
            let res = type_identifier(Dialect::PostgreSQL)(LocatedSpan::new(qs));
            assert!(res.is_ok());
            assert_eq!(res.unwrap().1, SqlType::Char(Some(16)));
        }

        #[test]
        fn time_without_time_zone() {
            let res = test_parse!(
                type_identifier(Dialect::PostgreSQL),
                b"time without time zone"
            );
            assert_eq!(res, SqlType::Time);
        }

        #[test]
        fn unsupported_other() {
            let res = test_parse!(type_identifier(Dialect::PostgreSQL), b"unsupportedtype");
            assert_eq!(res, SqlType::Other("unsupportedtype".into()));
        }

        #[test]
        fn custom_schema_qualified_type() {
            let res = test_parse!(type_identifier(Dialect::PostgreSQL), b"foo.custom");
            assert_eq!(
                res,
                SqlType::Other(Relation {
                    schema: Some("foo".into()),
                    name: "custom".into()
                })
            );
        }

        #[test]
        fn int_array() {
            let res = test_parse!(type_identifier(Dialect::PostgreSQL), b"int[]");
            assert_eq!(res, SqlType::Array(Box::new(SqlType::Int(None))));
        }

        #[test]
        fn double_nested_array() {
            let res = test_parse!(type_identifier(Dialect::PostgreSQL), b"text[][]");
            assert_eq!(
                res,
                SqlType::Array(Box::new(SqlType::Array(Box::new(SqlType::Text))))
            );
        }

        #[test]
        fn arrays_with_length() {
            let res = test_parse!(type_identifier(Dialect::PostgreSQL), b"float[4][5]");
            assert_eq!(
                res,
                SqlType::Array(Box::new(SqlType::Array(Box::new(SqlType::Float))))
            );
        }

        #[test]
        fn citext() {
            let res = test_parse!(type_identifier(Dialect::PostgreSQL), b"citext");
            assert_eq!(res, SqlType::Citext);
        }

        #[test]
        fn int_numeric_aliases() {
            assert_eq!(
                test_parse!(type_identifier(Dialect::PostgreSQL), b"int2"),
                SqlType::Int2
            );
            assert_eq!(
                test_parse!(type_identifier(Dialect::PostgreSQL), b"int4"),
                SqlType::Int4
            );
            assert_eq!(
                test_parse!(type_identifier(Dialect::PostgreSQL), b"int8"),
                SqlType::Int8
            );
        }

        #[test]
        fn interval_type() {
            assert_eq!(
                test_parse!(type_identifier(Dialect::PostgreSQL), b"interval"),
                SqlType::Interval {
                    fields: None,
                    precision: None
                }
            );

            assert_eq!(
                test_parse!(type_identifier(Dialect::PostgreSQL), b"interval dAY"),
                SqlType::Interval {
                    fields: Some(IntervalFields::Day),
                    precision: None
                }
            );

            assert_eq!(
                test_parse!(
                    type_identifier(Dialect::PostgreSQL),
                    b"INTERVAL hour to mINuTe (4)"
                ),
                SqlType::Interval {
                    fields: Some(IntervalFields::HourToMinute),
                    precision: Some(4),
                }
            );
        }

        #[test]
        fn mediumint_not_recognized() {
            assert_eq!(
                test_parse!(type_identifier(Dialect::PostgreSQL), b"mediumint"),
                SqlType::Other(Relation {
                    schema: None,
                    name: "mediumint".into()
                })
            );
        }

        #[test]
        fn tsvector_type() {
            let res = test_parse!(type_identifier(Dialect::PostgreSQL), b"tsvector");
            assert_eq!(res, SqlType::Tsvector);
        }
    }
}
