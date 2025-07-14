use std::str;
use std::str::FromStr;

use bit_vec::BitVec;
use nom::branch::alt;
use nom::bytes::complete::{is_not, tag, tag_no_case, take};
use nom::character::complete::{char, digit0, digit1, satisfy};
use nom::combinator::{map, map_parser, map_res, not, opt, peek, recognize};
use nom::error::ErrorKind;
use nom::multi::fold_many0;
use nom::sequence::{delimited, pair, preceded, terminated, tuple};
use nom_locate::LocatedSpan;
use readyset_sql::{ast::*, Dialect};

use crate::dialect::{is_sql_identifier, DialectParser};
use crate::NomSqlResult;

// Integer literal value
pub fn integer_literal(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Literal> {
    let (i, sign) = opt(tag("-"))(i)?;
    let (i, num) = map_parser(digit1, nom::character::complete::u64)(i)?;

    // If it fits in an i64, default to that
    let res = if let Ok(num) = i64::try_from(num) {
        if sign.is_some() {
            Literal::Integer(-(num))
        } else {
            Literal::Integer(num)
        }
    } else {
        // Special case to check if this is i64::MIN, which doesn't fit in i64 on the positve side
        // of the decimal point, but is still a valid i64.
        if sign.is_some() && num == i64::MAX as u64 + 1 {
            Literal::Integer(i64::MIN)
        } else {
            Literal::UnsignedInteger(num)
        }
    };

    Ok((i, res))
}

#[allow(clippy::type_complexity)]
pub fn float(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], (Option<&[u8]>, &[u8], &[u8], &[u8])> {
    tuple((
        opt(map(tag("-"), |i: LocatedSpan<&[u8]>| *i)),
        map(digit1, |i: LocatedSpan<&[u8]>| *i),
        map(tag("."), |i: LocatedSpan<&[u8]>| *i),
        map(digit0, |i: LocatedSpan<&[u8]>| *i),
    ))(i)
}

// Floating point literal value
#[allow(clippy::type_complexity)]
pub fn float_literal(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Literal> {
    map(
        pair(
            peek(float),
            map_res(
                map_res(recognize(float), |i: LocatedSpan<&[u8]>| str::from_utf8(&i)),
                f64::from_str,
            ),
        ),
        |f| {
            let (_, _, _, frac) = f.0;
            Literal::Double(Double {
                value: f.1,
                precision: frac.len() as _,
            })
        },
    )(i)
}

fn boolean_literal(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Literal> {
    alt((
        map(tag_no_case("true"), |_| Literal::Boolean(true)),
        map(tag_no_case("false"), |_| Literal::Boolean(false)),
    ))(i)
}

/// String literal value
fn raw_string_quoted(
    dialect: Dialect,
    quote: &'static [u8],
    escape_quote: &'static [u8],
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Vec<u8>> {
    move |i| {
        delimited(
            tag(quote),
            fold_many0(
                alt((
                    map(is_not(escape_quote), |i: LocatedSpan<&[u8]>| *i),
                    map(pair(tag(quote), tag(quote)), |_| quote),
                    dialect.escapes(),
                    // default for unhandled escape is to drop the backslash
                    preceded(tag("\\"), map(take(1usize), |i: LocatedSpan<&[u8]>| *i)),
                )),
                Vec::new,
                |mut acc: Vec<u8>, bytes: &[u8]| {
                    acc.extend(bytes);
                    acc
                },
            ),
            tag(quote),
        )(i)
    }
}

/// Unescaped string literal value (single-quotes only as this is Postgres-only)
pub fn raw_string_single_quoted_unescaped(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Vec<u8>> {
    map(
        delimited(tag(b"\'"), opt(is_not(b"\'" as &'static [u8])), tag(b"\'")),
        |i: Option<LocatedSpan<&[u8]>>| i.map(|i| i.to_vec()).unwrap_or_default(),
    )(i)
}

fn raw_string_single_quoted(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Vec<u8>> {
    move |i| raw_string_quoted(dialect, b"'", b"\\'")(i)
}

fn raw_string_double_quoted(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Vec<u8>> {
    move |i| raw_string_quoted(dialect, b"\"", b"\\\"")(i)
}

/// Specification for how string literals may be quoted
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuotingStyle {
    /// String literals are quoted with single quotes (`'`)
    Single,
    /// String literals are quoted with double quotes (`"`)
    Double,
    /// String literals may be quoted with either single quotes (`'`) or double quotes (`"`)
    SingleOrDouble,
}

/// Parse a raw (binary) string literal using the given [`QuotingStyle`]
pub fn raw_string_literal(
    dialect: Dialect,
    quoting_style: QuotingStyle,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Vec<u8>> {
    move |i| match quoting_style {
        QuotingStyle::Single => raw_string_single_quoted(dialect)(i),
        QuotingStyle::Double => raw_string_double_quoted(dialect)(i),
        QuotingStyle::SingleOrDouble => alt((
            raw_string_single_quoted(dialect),
            raw_string_double_quoted(dialect),
        ))(i),
    }
}

/// Parse a utf8 string literal using the given [`QuotingStyle`]
pub fn utf8_string_literal(
    dialect: Dialect,
    quoting_style: QuotingStyle,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], String> {
    move |i| {
        map_res(raw_string_literal(dialect, quoting_style), |bytes| {
            String::from_utf8(bytes)
        })(i)
    }
}

fn bits(input: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], BitVec> {
    fold_many0(
        map(alt((char('0'), char('1'))), |i: char| i == '1'),
        BitVec::new,
        |mut acc: BitVec, bit: bool| {
            acc.push(bit);
            acc
        },
    )(input)
}

fn simple_literal(dialect: Dialect) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Literal> {
    move |i| {
        alt((
            float_literal,
            map(dialect.bytes_literal(), Literal::ByteArray),
            integer_literal,
            boolean_literal,
            map(
                delimited(tag_no_case("b'"), bits, tag("'")),
                Literal::BitVector,
            ),
            map(
                terminated(
                    tag_no_case("null"),
                    // Don't parse `null` if it's a prefix of a larger identifier, to allow eg
                    // columns starting with the word "null"
                    not(peek(satisfy(|c| is_sql_identifier(c as _)))),
                ),
                |_| Literal::Null,
            ),
        ))(i)
    }
}

/// Parser for any literal value, including placeholders
pub fn literal(dialect: Dialect) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Literal> {
    move |i| {
        alt((
            simple_literal(dialect),
            map(dialect.string_literal(), |bytes| {
                match String::from_utf8(bytes) {
                    Ok(s) => Literal::String(s),
                    Err(err) => Literal::Blob(err.into_bytes()),
                }
            }),
            map(tag("?"), |_| {
                Literal::Placeholder(ItemPlaceholder::QuestionMark)
            }),
            map(
                preceded(
                    tag(":"),
                    map_res(
                        map_res(digit1, |i: LocatedSpan<&[u8]>| str::from_utf8(&i)),
                        u32::from_str,
                    ),
                ),
                |num| Literal::Placeholder(ItemPlaceholder::ColonNumber(num)),
            ),
            map(
                preceded(
                    tag("$"),
                    map_res(
                        map_res(digit1, |i: LocatedSpan<&[u8]>| str::from_utf8(&i)),
                        |s| match u32::from_str(s) {
                            Ok(0) => Err(ErrorKind::Digit), // Disallow $0 as a placeholder
                            Ok(i) => Ok(i),
                            Err(_) => Err(ErrorKind::Digit),
                        },
                    ),
                ),
                |num| Literal::Placeholder(ItemPlaceholder::DollarNumber(num)),
            ),
            boolean_literal,
        ))(i)
    }
}

/// Parser for a literal value which may be embedded inside of syntactic constructs within another
/// string literal, such as within the string literal syntax for postgresql arrays, jsonpath
/// expressions, etc.
pub fn embedded_literal(
    dialect: Dialect,
    quoting_style: QuotingStyle,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Literal> {
    move |i| {
        alt((
            simple_literal(dialect),
            map(
                raw_string_literal(dialect, quoting_style),
                |bytes| match String::from_utf8(bytes) {
                    Ok(s) => Literal::String(s),
                    Err(err) => Literal::Blob(err.into_bytes()),
                },
            ),
        ))(i)
    }
}

#[cfg(test)]
mod tests {
    use assert_approx_eq::assert_approx_eq;
    use proptest::prop_assume;
    use readyset_sql::DialectDisplay;
    use readyset_util::hash::hash;
    use test_strategy::proptest;
    use test_utils::tags;

    use super::*;

    #[test]
    fn float_formatting_strips_trailing_zeros() {
        let f = Literal::Double(Double {
            value: 1.5,
            precision: u8::MAX,
        });
        assert_eq!(f.display(Dialect::MySQL).to_string(), "1.5");
    }

    #[test]
    fn float_formatting_leaves_zero_after_dot() {
        let f = Literal::Double(Double {
            value: 0.0,
            precision: u8::MAX,
        });
        assert_eq!(f.display(Dialect::MySQL).to_string(), "0.0");
    }

    #[test]
    fn float_lots_of_zeros() {
        let res = float_literal(LocatedSpan::new(b"1.500000000000000000000000000000"))
            .unwrap()
            .1;
        if let Literal::Double(Double { value, .. }) = res {
            assert_approx_eq!(value, 1.5);
        } else {
            unreachable!()
        }
    }

    #[test]
    fn larger_than_i64_max_parses_to_unsigned() {
        let needs_unsigned = i64::MAX as u64 + 1;
        let formatted = format!("{needs_unsigned}");
        let res = integer_literal(LocatedSpan::new(formatted.as_bytes()))
            .unwrap()
            .1;
        assert_eq!(res, Literal::UnsignedInteger(needs_unsigned));
    }

    #[test]
    fn i64_min_parses_to_signed() {
        let formatted = format!("{}", i64::MIN);
        let res = integer_literal(LocatedSpan::new(formatted.as_bytes()))
            .unwrap()
            .1;
        assert_eq!(res, Literal::Integer(i64::MIN));
    }

    #[tags(no_retry)]
    #[proptest]
    fn real_hash_matches_eq(real1: Double, real2: Double) {
        if real1 == real2 {
            assert_eq!(hash(&real1), hash(&real2));
        }
    }

    #[tags(no_retry)]
    #[proptest]
    fn literal_to_string_parse_round_trip(lit: Literal) {
        prop_assume!(!matches!(
            lit,
            Literal::Double(_) | Literal::Float(_) | Literal::Numeric(_) | Literal::ByteArray(_)
        ));
        match lit {
            Literal::BitVector(_) => {
                let s = lit.display(Dialect::MySQL).to_string();
                assert_eq!(
                    literal(Dialect::PostgreSQL)(LocatedSpan::new(s.as_bytes()))
                        .unwrap()
                        .1,
                    lit
                )
            }
            // Positive integers are parsed as signed if they are in range
            Literal::UnsignedInteger(i) if i <= i64::MAX as u64 => {
                let s = lit.display(Dialect::MySQL).to_string();
                assert_eq!(
                    literal(Dialect::PostgreSQL)(LocatedSpan::new(s.as_bytes()))
                        .unwrap()
                        .1,
                    Literal::Integer(i as i64)
                )
            }
            // Can't test cross-dialect round-tripping for strings due to Postgres escaping
            Literal::String(_) => {
                for &dialect in Dialect::ALL {
                    let s = lit.display(dialect).to_string();
                    assert_eq!(
                        literal(dialect)(LocatedSpan::new(s.as_bytes())).unwrap().1,
                        lit
                    )
                }
            }
            _ => {
                for &dialect in Dialect::ALL {
                    let s = lit.display(Dialect::MySQL).to_string();
                    assert_eq!(
                        literal(dialect)(LocatedSpan::new(s.as_bytes())).unwrap().1,
                        lit
                    )
                }
                for &dialect in Dialect::ALL {
                    let s = lit.display(Dialect::PostgreSQL).to_string();
                    assert_eq!(
                        literal(dialect)(LocatedSpan::new(s.as_bytes())).unwrap().1,
                        lit
                    )
                }
            }
        }
    }

    #[test]
    fn boolean_literals() {
        for &dialect in Dialect::ALL {
            assert_eq!(
                test_parse!(literal(dialect), b"true"),
                Literal::Boolean(true)
            );
            assert_eq!(
                test_parse!(literal(dialect), b"True"),
                Literal::Boolean(true)
            );
            assert_eq!(
                test_parse!(literal(dialect), b"TruE"),
                Literal::Boolean(true)
            );
            assert_eq!(
                test_parse!(literal(dialect), b"TRUE"),
                Literal::Boolean(true)
            );
            assert_eq!(
                test_parse!(literal(dialect), b"false"),
                Literal::Boolean(false)
            );
            assert_eq!(
                test_parse!(literal(dialect), b"False"),
                Literal::Boolean(false)
            );
            assert_eq!(
                test_parse!(literal(dialect), b"FalsE"),
                Literal::Boolean(false)
            );
            assert_eq!(
                test_parse!(literal(dialect), b"FALSE"),
                Literal::Boolean(false)
            );
        }
    }

    mod postgres {
        use super::*;

        test_format_parse_round_trip!(
            rt_literal(literal, Literal, Dialect::PostgreSQL);
        );
    }

    mod mysql {
        use super::*;

        #[test]
        fn mysql_hex_literal_round_trip() {
            let input = "X'01aF'";
            let parsed = test_parse!(literal(Dialect::MySQL), input.as_bytes());
            let rt = parsed.display(Dialect::MySQL).to_string();
            eprintln!("rt: {rt}");
            let parsed_again = test_parse!(literal(Dialect::MySQL), rt.as_bytes());
            assert_eq!(parsed, parsed_again);
        }
    }
}
