use std::str::{self, FromStr};

use bit_vec::BitVec;
use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case, take, take_while1};
use nom::character::complete::char;
use nom::character::is_alphanumeric;
use nom::combinator::{map, map_res, not, opt, peek};
use nom::error::ErrorKind;
use nom::multi::fold_many0;
use nom::sequence::{delimited, preceded};
use nom::IResult;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::keywords::{sql_keyword, sql_keyword_or_builtin_function, POSTGRES_NOT_RESERVED};
use crate::literal::{raw_string_literal, QuotingStyle};
use crate::whitespace::whitespace0;
use crate::{literal, Literal, SqlIdentifier};

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
        Vec::new,
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
        BitVec::new,
        |mut acc: BitVec, bit: bool| {
            acc.push(bit);
            acc
        },
    )(input)
}

/// Specification for a SQL dialect to use when parsing
///
/// Currently, Dialect controls the escape characters used for identifiers, and the quotes used to
/// surround string literals, but may be extended to cover more dialect differences in the future
#[derive(Debug, PartialEq, Eq, Clone, Copy, Serialize, Deserialize)]
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
    pub fn identifier(self) -> impl for<'a> Fn(&'a [u8]) -> IResult<&'a [u8], SqlIdentifier> {
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
                |v| str::from_utf8(v).map(Into::into),
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
                            .map(Into::into)
                    },
                ),
                map_res(
                    delimited(tag("\""), take_while1(|c| c != 0 && c != b'"'), tag("\"")),
                    |v| str::from_utf8(v).map(Into::into),
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

    /// Returns the [`QuotingStyle`] for this dialect
    pub fn quoting_style(self) -> QuotingStyle {
        match self {
            Dialect::PostgreSQL => QuotingStyle::Single,
            Dialect::MySQL => QuotingStyle::SingleOrDouble,
        }
    }

    /// Parse the raw (byte) content of a string literal using this Dialect
    pub fn string_literal(self) -> impl for<'a> Fn(&'a [u8]) -> IResult<&'a [u8], Vec<u8>> {
        move |i| match self {
            // Currently we allow escape sequences in all string constants. If we support postgres'
            // standard_conforming_strings setting, then the below should be changed to check for
            // the presence of a preceding 'E' instead of matching and discarding the match result.
            Dialect::PostgreSQL => preceded(
                opt(tag_no_case("E")),
                raw_string_literal(self.quoting_style()),
            )(i),
            Dialect::MySQL => preceded(
                opt(alt((tag("_utf8mb4"), tag("_utf8"), tag("_binary")))),
                raw_string_literal(self.quoting_style()),
            )(i),
        }
    }

    pub fn utf8_string_literal(self) -> impl Fn(&[u8]) -> IResult<&[u8], String> {
        move |i| {
            map_res(self.string_literal(), |bytes| {
                String::from_utf8(bytes)
                    .map_err(|_| nom::Err::Error(nom::error::Error::new(i, ErrorKind::Fail)))
            })(i)
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
            Dialect::MySQL => Err(nom::Err::Error(nom::error::Error::new(
                i,
                nom::error::ErrorKind::Many0,
            ))),
        }
    }

    /// Parses the MySQL specific `{offset}, {limit}` part in a `LIMIT` clause
    pub fn offset_limit(
        self,
    ) -> impl Fn(&[u8]) -> IResult<&[u8], (Option<Literal>, Option<Literal>)> {
        move |i| {
            if self == Dialect::PostgreSQL {
                return Err(nom::Err::Error(nom::error::Error::new(i, ErrorKind::Fail)));
            }

            let (i, _) = whitespace0(i)?;
            let (i, offset) = literal(self)(i)?;
            let (i, _) = whitespace0(i)?;
            let (i, _) = tag_no_case(",")(i)?;
            let (i, _) = whitespace0(i)?;
            let (i, limit) = literal(self)(i)?;

            Ok((i, (Some(limit), Some(offset))))
        }
    }

    /// Returns the default subsecond digit count for time types.
    ///
    /// This value is also known as fractional second precision (FSP), and can be queried via
    /// `datetime_precision` in [`information_schema.columns`](https://dev.mysql.com/doc/refman/8.0/en/information-schema-columns-table.html).
    #[inline]
    pub fn default_subsecond_digits(self) -> u16 {
        match self {
            // https://dev.mysql.com/doc/refman/8.0/en/date-and-time-type-syntax.html
            // "If omitted, the default precision is 0. (This differs from the standard SQL default
            // of 6, for compatibility with previous MySQL versions.)"
            Self::MySQL => 0,

            // https://www.postgresql.org/docs/current/datatype-datetime.html
            // "By default, there is no explicit bound on precision", so the max value is used.
            Self::PostgreSQL => 6,
        }
    }
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

            Dialect::MySQL.identifier()(id1).unwrap();
            Dialect::MySQL.identifier()(id2).unwrap();
            Dialect::MySQL.identifier()(id3).unwrap();
            Dialect::MySQL.identifier()(id4).unwrap_err();
            Dialect::MySQL.identifier()(id5).unwrap_err();
            Dialect::MySQL.identifier()(id6).unwrap();
            Dialect::MySQL.identifier()(id7).unwrap();
            Dialect::MySQL.identifier()(id8).unwrap_err();
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
            res.unwrap_err();
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

            Dialect::PostgreSQL.identifier()(id1).unwrap();
            Dialect::PostgreSQL.identifier()(id2).unwrap();
            Dialect::PostgreSQL.identifier()(id3).unwrap();
            Dialect::PostgreSQL.identifier()(id4).unwrap_err();
            Dialect::PostgreSQL.identifier()(id5).unwrap_err();
            Dialect::PostgreSQL.identifier()(id6).unwrap();
            Dialect::PostgreSQL.identifier()(id7).unwrap();
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
        fn literal_string_with_escape_character() {
            let lit = b"E'string'";
            assert_eq!(
                Dialect::PostgreSQL.string_literal()(lit).unwrap().1,
                b"string"
            );
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
            res.unwrap_err();
        }
    }
}
