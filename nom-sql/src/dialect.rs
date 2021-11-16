use bit_vec::BitVec;
use std::borrow::Cow;
use std::str::{self, FromStr};

use nom::branch::alt;
use nom::bytes::complete::{is_not, tag, tag_no_case, take, take_while1};
use nom::character::complete::char;
use nom::character::is_alphanumeric;
use nom::combinator::{map, map_res, not, opt, peek};
use nom::error::{ErrorKind, ParseError};
use nom::multi::fold_many0;
use nom::sequence::{delimited, preceded};
use nom::IResult;
use thiserror::Error;

use crate::keywords::{sql_keyword, sql_keyword_or_builtin_function, POSTGRES_NOT_RESERVED};

#[inline]
fn is_sql_identifier(chr: u8) -> bool {
    is_alphanumeric(chr) || chr == b'_'
}

/// Byte array literal value (PostgreSQL)
fn raw_hex_bytes_psql(input: &[u8]) -> IResult<&[u8], Vec<u8>> {
    delimited(tag("E'\\\\x"), hex_bytes, tag("'::bytea"))(input)
}

/// Blob literal value (MySQL)
fn raw_hex_bytes_mysql(input: &[u8]) -> IResult<&[u8], Vec<u8>> {
    delimited(tag("X'"), hex_bytes, tag("'"))(input)
}

fn hex_bytes(input: &[u8]) -> IResult<&[u8], Vec<u8>> {
    fold_many0(
        map_res(take(2_usize), hex::decode),
        Vec::new(),
        |mut acc: Vec<u8>, bytes: Vec<u8>| {
            acc.extend(bytes);
            acc
        },
    )(input)
}

/// Bit vector literal value (PostgreSQL)
fn raw_bit_vector_psql(input: &[u8]) -> IResult<&[u8], BitVec> {
    delimited(tag_no_case("b'"), bits, tag("'"))(input)
}

fn bits(input: &[u8]) -> IResult<&[u8], BitVec> {
    fold_many0(
        map(alt((char('0'), char('1'))), |i: char| i == '1'),
        BitVec::new(),
        |mut acc: BitVec, bit: bool| {
            acc.push(bit);
            acc
        },
    )(input)
}

/// String literal value
fn raw_string_quoted(input: &[u8], is_single_quote: bool) -> IResult<&[u8], Vec<u8>> {
    // TODO: clean up these assignments. lifetimes and temporary values made it difficult
    let quote_slice: &[u8] = if is_single_quote { b"\'" } else { b"\"" };
    let double_quote_slice: &[u8] = if is_single_quote { b"\'\'" } else { b"\"\"" };
    let backslash_quote: &[u8] = if is_single_quote { b"\\\'" } else { b"\\\"" };
    delimited(
        tag(quote_slice),
        fold_many0(
            alt((
                is_not(backslash_quote),
                map(tag(double_quote_slice), |_| -> &[u8] {
                    if is_single_quote {
                        b"\'"
                    } else {
                        b"\""
                    }
                }),
                map(tag("\\\\"), |_| &b"\\"[..]),
                map(tag("\\b"), |_| &b"\x7f"[..]),
                map(tag("\\r"), |_| &b"\r"[..]),
                map(tag("\\n"), |_| &b"\n"[..]),
                map(tag("\\t"), |_| &b"\t"[..]),
                map(tag("\\0"), |_| &b"\0"[..]),
                map(tag("\\Z"), |_| &b"\x1A"[..]),
                preceded(tag("\\"), take(1usize)),
            )),
            Vec::new(),
            |mut acc: Vec<u8>, bytes: &[u8]| {
                acc.extend(bytes);
                acc
            },
        ),
        tag(quote_slice),
    )(input)
}

fn raw_string_single_quoted(i: &[u8]) -> IResult<&[u8], Vec<u8>> {
    raw_string_quoted(i, true)
}

fn raw_string_double_quoted(i: &[u8]) -> IResult<&[u8], Vec<u8>> {
    raw_string_quoted(i, false)
}

/// Specification for a SQL dialect to use when parsing
///
/// Currently, Dialect controls the escape characters used for identifiers, and the quotes used to
/// surround string literals, but may be extended to cover more dialect differences in the future
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Dialect {
    /// The SQL dialect used by PostgreSQL.
    ///
    /// Identifiers are escaped with double quotes (`"`) and strings use only single quotes (`'`)
    PostgreSQL,

    /// The SQL dialect used by MySQL.
    ///
    /// Identifiers are escaped with backticks (`\``) or square brackets (`[` and `]`) and strings
    /// use either single quotes (`'`) or double quotes (`"`)
    MySQL,
}

#[derive(Debug, PartialEq, Eq, Clone, Error)]
#[error("Unknown dialect `{0}`, expected one of mysql or postgresql")]
pub struct UnknownDialect(String);

impl FromStr for Dialect {
    type Err = UnknownDialect;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "mysql" => Ok(Dialect::MySQL),
            "postgresql" => Ok(Dialect::PostgreSQL),
            _ => Err(UnknownDialect(s.to_owned())),
        }
    }
}

impl Dialect {
    /// Parse a SQL identifier using this Dialect
    pub fn identifier(self) -> impl for<'a> Fn(&'a [u8]) -> IResult<&'a [u8], Cow<str>> {
        move |i| match self {
            Dialect::MySQL => map_res(
                alt((
                    preceded(
                        not(peek(sql_keyword_or_builtin_function)),
                        take_while1(is_sql_identifier),
                    ),
                    delimited(tag("`"), take_while1(|c| c != 0 && c != b'`'), tag("`")),
                    delimited(tag("["), take_while1(is_sql_identifier), tag("]")),
                )),
                |v| str::from_utf8(v).map(Cow::Borrowed),
            )(i),
            Dialect::PostgreSQL => alt((
                map_res(
                    preceded(
                        not(map_res(peek(sql_keyword_or_builtin_function), |i| {
                            if POSTGRES_NOT_RESERVED.contains(&i.to_ascii_uppercase()[..]) {
                                Err(nom::Err::Error((i, ErrorKind::IsNot)))
                            } else {
                                Ok(i)
                            }
                        })),
                        take_while1(is_sql_identifier),
                    ),
                    |v| {
                        str::from_utf8(v)
                            .map(str::to_ascii_lowercase)
                            .map(Cow::Owned)
                    },
                ),
                map_res(
                    delimited(tag("\""), take_while1(|c| c != 0 && c != b'"'), tag("\"")),
                    |v| str::from_utf8(v).map(Cow::Borrowed),
                ),
            ))(i),
        }
    }

    /// Parse a SQL function identifier using this Dialect
    pub fn function_identifier(self) -> impl for<'a> Fn(&'a [u8]) -> IResult<&'a [u8], &'a str> {
        move |i| match self {
            Dialect::MySQL => map_res(
                alt((
                    preceded(not(peek(sql_keyword)), take_while1(is_sql_identifier)),
                    delimited(tag("`"), take_while1(is_sql_identifier), tag("`")),
                    delimited(tag("["), take_while1(is_sql_identifier), tag("]")),
                )),
                str::from_utf8,
            )(i),
            Dialect::PostgreSQL => map_res(
                alt((
                    preceded(not(peek(sql_keyword)), take_while1(is_sql_identifier)),
                    delimited(tag("\""), take_while1(is_sql_identifier), tag("\"")),
                )),
                str::from_utf8,
            )(i),
        }
    }

    /// Parse the raw (byte) content of a string literal using this Dialect
    pub fn string_literal(self) -> impl for<'a> Fn(&'a [u8]) -> IResult<&'a [u8], Vec<u8>> {
        move |i| match self {
            Dialect::PostgreSQL => raw_string_single_quoted(i),
            Dialect::MySQL => preceded(
                opt(alt((tag("_utf8mb4"), tag("_utf8"), tag("_binary")))),
                alt((raw_string_single_quoted, raw_string_double_quoted)),
            )(i),
        }
    }

    pub fn utf8_string_literal(self) -> impl for<'a> Fn(&'a [u8]) -> IResult<&'a [u8], String> {
        move |i| {
            let (remaining, bytes) = self.string_literal()(i)?;
            Ok((
                remaining,
                String::from_utf8(bytes).map_err(|_| {
                    nom::Err::Error(ParseError::from_error_kind(i, ErrorKind::Many0))
                })?,
            ))
        }
    }

    /// Parse the raw (byte) content of a bytes literal using this Dialect.
    // TODO(fran): Improve this. This is very naive, and for Postgres specifically, it only
    //  parses the hex-formatted byte array. We need to also add support for the escaped format.
    pub fn bytes_literal(self) -> impl for<'a> Fn(&'a [u8]) -> IResult<&'a [u8], Vec<u8>> {
        move |i| match self {
            Dialect::PostgreSQL => raw_hex_bytes_psql(i),
            Dialect::MySQL => raw_hex_bytes_mysql(i),
        }
    }

    /// Parse the raw (byte) content of a bit vector literal using this Dialect.
    pub fn bitvec_literal(self) -> impl for<'a> Fn(&'a [u8]) -> IResult<&'a [u8], BitVec> {
        move |i| match self {
            Dialect::PostgreSQL => raw_bit_vector_psql(i),
            Dialect::MySQL => Err(nom::Err::Error((i, nom::error::ErrorKind::ParseTo))),
        }
    }
}

/// Create a function from a combination of nom parsers, which takes a [`Dialect`] as an argument.
///
/// This macro behaves like [`nom::named`], except the functions it defines take a [`Dialect`] as an
/// argument, and return a *function* implementing the parse rule. For example, the following
/// invocation:
///
/// ```ignore
/// named_with_dialect!(fn my_ident(dialect) -> Vec<u8>, call!(dialect.identifier()))
/// ```
///
/// will produce a function with the following signature:
///
/// ```ignore
/// fn named_with_dialect(dialect: Dialect) -> impl Fn(&[u8]) -> nom::IResult<&[u8], Vec<u8>>
/// ```
///
/// For functions with an input type other than `&[u8]`, the input type can be specified after the
/// name for the dialect argument:
///
/// ```ignore
/// named_with_dialect!(fn my_ident(dialect, &str) -> Vec<u8>, call!(dialect.identifier()))
/// ```
///
/// will produce a function with the following signature:
///
/// ```ignore
/// fn named_with_dialect(dialect: Dialect) -> impl Fn(&str) -> nom::IResult<&str, Vec<u8>>
/// ```
macro_rules! named_with_dialect {
    ($vis: vis $func_name: ident($dialect: ident) -> $result: ty, $($body: tt)*) => {
        named_with_dialect!($vis $func_name($dialect, &[u8]) -> $result, $($body)*);
    };
	($vis: vis $func_name: ident($dialect: ident, $input_type: ty) -> $result: ty, $submac: ident!($($args: tt)*)) => {
        $vis fn $func_name($dialect: $crate::Dialect) -> impl Fn($input_type) -> nom::IResult<$input_type, $result> {
            move |i: $input_type| {
                $submac!(
                    i,
                    $($args)*
                )
            }
        }

	};
}

#[cfg(test)]
mod tests {
    use super::*;

    mod mysql {
        use super::*;

        #[test]
        fn sql_identifiers() {
            let id1 = b"foo";
            let id2 = b"f_o_o";
            let id3 = b"foo12";
            let id4 = b":fo oo";
            let id5 = b"primary ";
            let id6 = b"`primary`";
            let id7 = b"`state-province`";
            let id8 = b"`state\0province`";

            assert!(Dialect::MySQL.identifier()(id1).is_ok());
            assert!(Dialect::MySQL.identifier()(id2).is_ok());
            assert!(Dialect::MySQL.identifier()(id3).is_ok());
            assert!(Dialect::MySQL.identifier()(id4).is_err());
            assert!(Dialect::MySQL.identifier()(id5).is_err());
            assert!(Dialect::MySQL.identifier()(id6).is_ok());
            assert!(Dialect::MySQL.identifier()(id7).is_ok());
            assert!(Dialect::MySQL.identifier()(id8).is_err());
        }

        #[test]
        fn literal_string_single_backslash_escape() {
            let all_escaped = br#"\0\'\"\b\n\r\t\Z\\\%\_"#;
            for quote in [&b"'"[..], &b"\""[..]].iter() {
                let quoted = &[quote, &all_escaped[..], quote].concat();
                let res = Dialect::MySQL.string_literal()(quoted);
                let expected = "\0\'\"\x7F\n\r\t\x1a\\%_".as_bytes().to_vec();
                assert_eq!(res, Ok((&b""[..], expected)));
            }
        }

        #[test]
        fn literal_string_charset() {
            let res = Dialect::MySQL.string_literal()(b"_utf8mb4'noria'");
            let expected = b"noria".to_vec();
            assert_eq!(res, Ok((&b""[..], expected)));
        }

        #[test]
        fn literal_string_double_quote() {
            let res = Dialect::MySQL.string_literal()(br#""a""b""#);
            let expected = r#"a"b"#.as_bytes().to_vec();
            assert_eq!(res, Ok((&b""[..], expected)));
        }

        #[test]
        fn bytes_parsing() {
            let res = Dialect::MySQL.bytes_literal()(b"X'0008275c6480'");
            let expected = vec![0, 8, 39, 92, 100, 128];
            assert_eq!(res, Ok((&b""[..], expected)));

            // Empty
            let res = Dialect::MySQL.bytes_literal()(b"X''");
            let expected = vec![];
            assert_eq!(res, Ok((&b""[..], expected)));

            // Malformed string
            let res = Dialect::MySQL.bytes_literal()(b"''");
            assert!(res.is_err());
        }
    }

    mod postgres {
        use super::*;

        #[test]
        fn sql_identifiers() {
            let id1 = b"foo";
            let id2 = b"f_o_o";
            let id3 = b"foo12";
            let id4 = b":fo oo";
            let id5 = b"primary ";
            let id6 = b"\"primary\"";
            let id7 = b"\"state-province\"";

            assert!(Dialect::PostgreSQL.identifier()(id1).is_ok());
            assert!(Dialect::PostgreSQL.identifier()(id2).is_ok());
            assert!(Dialect::PostgreSQL.identifier()(id3).is_ok());
            assert!(Dialect::PostgreSQL.identifier()(id4).is_err());
            assert!(Dialect::PostgreSQL.identifier()(id5).is_err());
            assert!(Dialect::PostgreSQL.identifier()(id6).is_ok());
            assert!(Dialect::PostgreSQL.identifier()(id7).is_ok());
        }

        #[test]
        fn sql_identifiers_case() {
            let id1 = b"FoO";
            let id2 = b"foO";
            let id3 = br#""foO""#;

            assert_eq!(Dialect::PostgreSQL.identifier()(id1).unwrap().1, "foo");
            assert_eq!(Dialect::PostgreSQL.identifier()(id2).unwrap().1, "foo");
            assert_eq!(Dialect::PostgreSQL.identifier()(id3).unwrap().1, "foO");
        }

        #[test]
        fn literal_string_single_backslash_escape() {
            let all_escaped = br#"\0\'\"\b\n\r\t\Z\\\%\_"#;
            let quote = &b"'"[..];
            let quoted = &[quote, &all_escaped[..], quote].concat();
            let res = Dialect::PostgreSQL.string_literal()(quoted);
            let expected = "\0\'\"\x7F\n\r\t\x1a\\%_".as_bytes().to_vec();
            assert_eq!(res, Ok((&b""[..], expected)));
        }

        #[test]
        fn bytes_parsing() {
            let res = Dialect::PostgreSQL.bytes_literal()(b"E'\\\\x0008275c6480'::bytea");
            let expected = vec![0, 8, 39, 92, 100, 128];
            assert_eq!(res, Ok((&b""[..], expected)));

            // Empty
            let res = Dialect::PostgreSQL.bytes_literal()(b"E'\\\\x'::bytea");
            let expected = vec![];
            assert_eq!(res, Ok((&b""[..], expected)));

            // Malformed string
            let res = Dialect::PostgreSQL.bytes_literal()(b"E'\\\\'::btea");
            assert!(res.is_err());
        }
    }
}
