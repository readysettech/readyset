use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case, take_while1};
use nom::character::complete::hex_digit0;
use nom::character::is_alphanumeric;
use nom::combinator::{map, map_res, not, opt, peek, verify};
use nom::error::ErrorKind;
use nom::sequence::{delimited, preceded};
use nom::{InputLength, InputTake};
use nom_locate::LocatedSpan;
use readyset_sql::{ast::*, Dialect};

use crate::keywords::{sql_keyword, sql_keyword_or_builtin_function, POSTGRES_NOT_RESERVED};
use crate::literal::{raw_string_literal, raw_string_single_quoted_unescaped, QuotingStyle};
use crate::whitespace::whitespace0;
use crate::{literal, NomSqlError, NomSqlResult};

macro_rules! failed {
    ($input:expr) => {
        return Err(nom::Err::Error(NomSqlError {
            input: $input,
            kind: ErrorKind::Fail,
        }))
    };
}

#[inline]
pub(crate) fn is_sql_identifier(chr: u8) -> bool {
    is_alphanumeric(chr) || chr == b'_'
}

/// Byte array literal value (PostgreSQL)
fn raw_hex_bytes_psql(input: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Vec<u8>> {
    alt((
        // TODO(mvzink): Delete this in favor of `DialectParser::string_literal`
        delimited(tag("E'\\\\x"), hex_bytes(1), tag("'::bytea")),
        delimited(tag_no_case("x'"), hex_bytes(1), tag("'")),
    ))(input)
}

/// Blob literal value (MySQL)
fn raw_hex_bytes_mysql(input: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Vec<u8>> {
    alt((
        delimited(tag_no_case("x'"), hex_bytes(2), tag("'")),
        preceded(tag("0x"), hex_bytes(2)),
    ))(input)
}

fn hex_bytes(chunk: usize) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Vec<u8>> {
    move |i| {
        map_res(
            verify(hex_digit0, |i: &LocatedSpan<&[u8]>| i.len() % chunk == 0),
            |i: LocatedSpan<&[u8]>| hex::decode(*i),
        )(i)
    }
}

pub(crate) trait DialectParser {
    /// Parse a SQL identifier using this Dialect
    fn identifier(self) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], SqlIdentifier>;

    /// Parse a SQL function identifier using this Dialect
    fn function_identifier(
        self,
    ) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], SqlIdentifier>;

    /// Returns the [`QuotingStyle`] for this dialect
    fn quoting_style(self) -> QuotingStyle;

    /// Parse the raw (byte) content of a string literal using this Dialect
    fn string_literal(self) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Vec<u8>>;

    fn utf8_string_literal(self) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], String>;

    /// Parse the raw (byte) content of a bytes literal using this Dialect.
    // Naturally syntax and types vary between databases:
    // - mysql: 0xFFFF | [Xx]'FFFF' -> varbinary; length must be even, 0X... is invalid!
    // - psql: [Xx]'FFFF' -> bit-string
    fn bytes_literal(self) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Vec<u8>>;

    fn limit_offset_literal(self) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Literal>;

    /// Parse only the `OFFSET <value>` case
    fn offset_only(self) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Literal>;

    /// Parse only the `LIMIT <value>` clause ignoring OFFSET
    fn limit_only(self) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], LimitValue>;

    /// Posgres and MySQL parses LIMIT and OFFSET quite differently:
    /// - Postgres' spec for LIMIT/OFFSET is `[ LIMIT { number | ALL } ] [ OFFSET number ]`
    /// - MySQL's spec is: `[LIMIT {[offset,] row_count | row_count OFFSET offset}]`
    ///
    /// In addition to this spec postgres seems to allow `LIMIT NULL` and `OFFSET NULL` as well
    /// as non-integer values none of which are accepted by MySQL. This difference is handled by
    /// `Self::limit_offset_literal`.
    ///
    /// So with the NULL, ALL and differing datatype cases covered by other dialect functions we
    /// have the remaining differences:
    ///  - Postgres allows `OFFSET` without `LIMIT`
    ///  - MySQL allows for `LIMIT <offset>, <limit>`
    fn limit_offset(self) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], LimitClause>;

    /// Parse and remap escape sequences. The difference between Postgres and MySQL is the handling
    /// of `LIKE` pattern wildcards (% and _). From the [MySQL docs]: "If you use \% or \_ outside
    /// of pattern-matching contexts, they evaluate to the strings \% and \_, not to % and _."
    ///
    /// This effectively means the escape character (backslash) is ignored for MySQL when it
    /// precedes % or _, and it's handled in [`dataflow_expression::like::LikePattern`].
    ///
    /// [MySQL docs]: https://dev.mysql.com/doc/refman/8.4/en/string-literals.html
    fn escapes(self) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], &[u8]>;
}

impl DialectParser for Dialect {
    fn identifier(self) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], SqlIdentifier> {
        move |i| match self {
            Dialect::MySQL => {
                fn quoted_ident_contents(
                    i: LocatedSpan<&[u8]>,
                ) -> NomSqlResult<&[u8], LocatedSpan<&[u8]>> {
                    let mut idx = 0;
                    while idx < i.len() {
                        if i[idx] == b'`' && idx != (i.len() - 1) && i[idx + 1] == b'`' {
                            idx += 2;
                            continue;
                        }

                        if i[idx] == 0 || i[idx] == b'`' {
                            return Ok(i.take_split(idx));
                        }

                        idx += 1;
                    }

                    Ok(i.take_split(i.input_len()))
                }

                map_res(
                    alt((
                        preceded(
                            not(peek(sql_keyword_or_builtin_function)),
                            take_while1(is_sql_identifier),
                        ),
                        delimited(tag("`"), quoted_ident_contents, tag("`")),
                        delimited(tag("["), take_while1(is_sql_identifier), tag("]")),
                    )),
                    |v| std::str::from_utf8(&v).map(|s| s.replace("``", "`").into()),
                )(i)
            }
            Dialect::PostgreSQL => alt((
                map_res(
                    preceded(
                        not(map_res(peek(sql_keyword_or_builtin_function), |i| {
                            if POSTGRES_NOT_RESERVED.contains(&i.to_ascii_uppercase()[..]) {
                                Err(())
                            } else {
                                Ok(i)
                            }
                        })),
                        take_while1(is_sql_identifier),
                    ),
                    |v| {
                        std::str::from_utf8(&v)
                            .map(str::to_ascii_lowercase)
                            .map(Into::into)
                    },
                ),
                map_res(
                    delimited(tag("\""), take_while1(|c| c != 0 && c != b'"'), tag("\"")),
                    |v: LocatedSpan<&[u8]>| std::str::from_utf8(&v).map(Into::into),
                ),
            ))(i),
        }
    }

    fn function_identifier(
        self,
    ) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], SqlIdentifier> {
        move |i| match self {
            Dialect::MySQL => map_res(
                alt((
                    preceded(not(peek(sql_keyword)), take_while1(is_sql_identifier)),
                    delimited(tag("`"), take_while1(is_sql_identifier), tag("`")),
                    delimited(tag("["), take_while1(is_sql_identifier), tag("]")),
                )),
                |v: LocatedSpan<&[u8]>| std::str::from_utf8(&v).map(Into::into),
            )(i),
            Dialect::PostgreSQL => alt((
                map_res(
                    preceded(not(peek(sql_keyword)), take_while1(is_sql_identifier)),
                    |v: LocatedSpan<&[u8]>| {
                        std::str::from_utf8(&v)
                            .map(str::to_lowercase)
                            .map(Into::into)
                    },
                ),
                map_res(
                    delimited(tag("\""), take_while1(is_sql_identifier), tag("\"")),
                    |v: LocatedSpan<&[u8]>| std::str::from_utf8(&v).map(Into::into),
                ),
            ))(i),
        }
    }

    fn quoting_style(self) -> QuotingStyle {
        match self {
            Dialect::PostgreSQL => QuotingStyle::Single,
            Dialect::MySQL => QuotingStyle::SingleOrDouble,
        }
    }

    fn string_literal(self) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Vec<u8>> {
        move |i| match self {
            Dialect::PostgreSQL => {
                let (i, escape) = opt(tag_no_case("E"))(i)?;
                if escape.is_some() {
                    raw_string_literal(self, self.quoting_style())(i)
                } else {
                    raw_string_single_quoted_unescaped(i)
                }
            }
            Dialect::MySQL => preceded(
                opt(alt((tag("_utf8mb4"), tag("_utf8"), tag("_binary")))),
                raw_string_literal(self, self.quoting_style()),
            )(i),
        }
    }

    fn utf8_string_literal(self) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], String> {
        move |i| map_res(self.string_literal(), String::from_utf8)(i)
    }

    fn bytes_literal(self) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Vec<u8>> {
        move |i| match self {
            Dialect::PostgreSQL => raw_hex_bytes_psql(i),
            Dialect::MySQL => raw_hex_bytes_mysql(i),
        }
    }

    fn limit_offset_literal(self) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Literal> {
        move |i| {
            let (i, literal) = literal(self)(i)?;
            let literal = match &literal {
                Literal::Placeholder(_) => literal,
                Literal::UnsignedInteger(_) => literal,
                Literal::Integer(int) => {
                    if *int >= 0 {
                        literal
                    } else {
                        failed!(i);
                    }
                }
                Literal::Null => match self {
                    Dialect::PostgreSQL => literal,
                    Dialect::MySQL => {
                        failed!(i);
                    }
                },
                Literal::Numeric(..) | Literal::Float(_) | Literal::Double(_) => match self {
                    Dialect::PostgreSQL => literal,
                    Dialect::MySQL => {
                        failed!(i);
                    }
                },
                _ => {
                    failed!(i);
                }
            };

            Ok((i, literal))
        }
    }

    fn offset_only(self) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], Literal> {
        move |i| {
            let (i, _) = whitespace0(i)?;
            let (i, _) = tag_no_case("offset")(i)?;
            let (i, _) = whitespace0(i)?;
            let (i, offset) = self.limit_offset_literal()(i)?;
            Ok((i, offset))
        }
    }

    fn limit_only(self) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], LimitValue> {
        move |i| {
            let (i, _) = whitespace0(i)?;
            let (i, _) = tag_no_case("limit")(i)?;
            let (i, _) = whitespace0(i)?;
            match self {
                Dialect::PostgreSQL => alt((
                    map(self.limit_offset_literal(), LimitValue::Literal),
                    map(tag_no_case("all"), |_| LimitValue::All),
                ))(i),
                Dialect::MySQL => map(self.limit_offset_literal(), LimitValue::Literal)(i),
            }
        }
    }

    fn limit_offset(self) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], LimitClause> {
        move |i| {
            let (i, _) = whitespace0(i)?;
            match self {
                Dialect::PostgreSQL => {
                    let (i, limit) = opt(self.limit_only())(i)?;
                    let (i, offset) = opt(self.offset_only())(i)?;

                    Ok((i, LimitClause::LimitOffset { limit, offset }))
                }
                Dialect::MySQL => alt((
                    move |i| {
                        let (i, _) = whitespace0(i)?;
                        let (i, _) = tag_no_case("limit")(i)?;
                        let (i, _) = whitespace0(i)?;
                        let (i, offset) = self.limit_offset_literal()(i)?;
                        let (i, _) = whitespace0(i)?;
                        let (i, _) = tag_no_case(",")(i)?;
                        let (i, _) = whitespace0(i)?;
                        let (i, limit) = self.limit_offset_literal()(i)?;

                        Ok((
                            i,
                            LimitClause::OffsetCommaLimit {
                                offset,
                                limit: LimitValue::Literal(limit),
                            },
                        ))
                    },
                    move |i| {
                        let (i, limit) = self.limit_only()(i)?;
                        let (i, offset) = opt(self.offset_only())(i)?;

                        Ok((
                            i,
                            LimitClause::LimitOffset {
                                limit: Some(limit),
                                offset,
                            },
                        ))
                    },
                ))(i),
            }
        }
    }

    fn escapes(self) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], &[u8]> {
        move |i| {
            let common_escapes = move |i| {
                alt((
                    map(tag("\\\\"), |_| &b"\\"[..]),
                    map(tag("\\b"), |_| &b"\x08"[..]),
                    map(tag("\\r"), |_| &b"\r"[..]),
                    map(tag("\\n"), |_| &b"\n"[..]),
                    map(tag("\\t"), |_| &b"\t"[..]),
                    map(tag("\\0"), |_| &b"\0"[..]),
                    map(tag("\\Z"), |_| &b"\x1A"[..]),
                ))(i)
            };
            match self {
                Self::PostgreSQL => common_escapes(i),
                Self::MySQL => alt((
                    common_escapes,
                    map(tag("\\%"), |_| &b"\\%"[..]),
                    map(tag("\\_"), |_| &b"\\_"[..]),
                ))(i),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use nom::IResult;

    use super::*;
    use crate::to_nom_result;

    fn parse_dialect_bytes(dialect: Dialect, b: &[u8]) -> IResult<&[u8], Vec<u8>> {
        to_nom_result(dialect.bytes_literal()(LocatedSpan::new(b)))
    }

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

            Dialect::MySQL.identifier()(LocatedSpan::new(id1)).unwrap();
            Dialect::MySQL.identifier()(LocatedSpan::new(id2)).unwrap();
            Dialect::MySQL.identifier()(LocatedSpan::new(id3)).unwrap();
            Dialect::MySQL.identifier()(LocatedSpan::new(id4)).unwrap_err();
            Dialect::MySQL.identifier()(LocatedSpan::new(id5)).unwrap_err();
            Dialect::MySQL.identifier()(LocatedSpan::new(id6)).unwrap();
            Dialect::MySQL.identifier()(LocatedSpan::new(id7)).unwrap();
            Dialect::MySQL.identifier()(LocatedSpan::new(id8)).unwrap_err();
        }

        #[test]
        fn literal_string_single_backslash_escape() {
            let all_escaped = br#"\0\'\"\b\n\r\t\Z\\\%\_"#;
            for quote in [&b"'"[..], &b"\""[..]].iter() {
                let quoted = &[quote, &all_escaped[..], quote].concat();
                let res = to_nom_result(Dialect::MySQL.string_literal()(LocatedSpan::new(quoted)));
                let expected = "\0\'\"\x08\n\r\t\x1a\\\\%\\_".as_bytes().to_vec();
                assert_eq!(res, Ok((&b""[..], expected)));
            }
        }

        #[test]
        fn literal_string_charset() {
            let res = to_nom_result(Dialect::MySQL.string_literal()(LocatedSpan::new(
                b"_utf8mb4'noria'",
            )));
            let expected = b"noria".to_vec();
            assert_eq!(res, Ok((&b""[..], expected)));
        }

        #[test]
        fn literal_string_double_quote() {
            let res = to_nom_result(Dialect::MySQL.string_literal()(LocatedSpan::new(
                br#""a""b""#,
            )));
            let expected = r#"a"b"#.as_bytes().to_vec();
            assert_eq!(res, Ok((&b""[..], expected)));
        }

        #[test]
        fn bytes_parsing() {
            let res = parse_dialect_bytes(Dialect::MySQL, b"X'0008275c6480'");
            let expected = Ok((&b""[..], vec![0, 8, 39, 92, 100, 128]));
            assert_eq!(res, expected);
            let res = parse_dialect_bytes(Dialect::MySQL, b"x'0008275c6480'");
            assert_eq!(res, expected);
            let res = parse_dialect_bytes(Dialect::MySQL, b"0x0008275c6480");
            assert_eq!(res, expected);

            let res = parse_dialect_bytes(Dialect::MySQL, b"0x6D617263656C6F");
            let expected = Ok((&b""[..], vec![109, 97, 114, 99, 101, 108, 111]));
            assert_eq!(res, expected);

            // Empty
            let res = parse_dialect_bytes(Dialect::MySQL, b"X''");
            let expected = vec![];
            assert_eq!(res, Ok((&b""[..], expected)));

            // Malformed strings
            Dialect::MySQL.bytes_literal()(LocatedSpan::new(b"''")).unwrap_err();
            Dialect::MySQL.bytes_literal()(LocatedSpan::new(b"0x123")).unwrap_err();
        }

        #[test]
        fn ident_with_backtick() {
            let res = test_parse!(Dialect::MySQL.identifier(), b"````");
            assert_eq!(res, SqlIdentifier::from("`"));
            let rt = Dialect::MySQL.quote_identifier(&res).to_string();
            let res2 = test_parse!(Dialect::MySQL.identifier(), rt.as_bytes());
            assert_eq!(res2, res);
        }

        #[test]
        fn ident_with_backtick_and_other_chars() {
            let res = test_parse!(Dialect::MySQL.identifier(), b"```i`");
            assert_eq!(res, SqlIdentifier::from("`i"));
            let rt = Dialect::MySQL.quote_identifier(&res).to_string();
            let res2 = test_parse!(Dialect::MySQL.identifier(), rt.as_bytes());
            assert_eq!(res2, res);
        }
    }

    mod postgres {
        use super::*;
        use crate::to_nom_result;

        #[test]
        fn sql_identifiers() {
            let id1 = b"foo";
            let id2 = b"f_o_o";
            let id3 = b"foo12";
            let id4 = b":fo oo";
            let id5 = b"primary ";
            let id6 = b"\"primary\"";
            let id7 = b"\"state-province\"";

            Dialect::PostgreSQL.identifier()(LocatedSpan::new(id1)).unwrap();
            Dialect::PostgreSQL.identifier()(LocatedSpan::new(id2)).unwrap();
            Dialect::PostgreSQL.identifier()(LocatedSpan::new(id3)).unwrap();
            Dialect::PostgreSQL.identifier()(LocatedSpan::new(id4)).unwrap_err();
            Dialect::PostgreSQL.identifier()(LocatedSpan::new(id5)).unwrap_err();
            Dialect::PostgreSQL.identifier()(LocatedSpan::new(id6)).unwrap();
            Dialect::PostgreSQL.identifier()(LocatedSpan::new(id7)).unwrap();

            Dialect::PostgreSQL.identifier()(LocatedSpan::new(b"groups")).unwrap();
        }

        #[test]
        fn sql_identifiers_case() {
            let id1 = b"FoO";
            let id2 = b"foO";
            let id3 = br#""foO""#;

            assert_eq!(
                Dialect::PostgreSQL.identifier()(LocatedSpan::new(id1))
                    .unwrap()
                    .1,
                "foo"
            );
            assert_eq!(
                Dialect::PostgreSQL.identifier()(LocatedSpan::new(id2))
                    .unwrap()
                    .1,
                "foo"
            );
            assert_eq!(
                Dialect::PostgreSQL.identifier()(LocatedSpan::new(id3))
                    .unwrap()
                    .1,
                "foO"
            );
        }

        #[test]
        fn literal_string_single_backslash_no_escape() {
            let all_escaped = br#"\0\"\b\n\r\t\Z\\\%\_"#;
            let quote = &b"'"[..];
            let quoted = &[quote, &all_escaped[..], quote].concat();
            let res = to_nom_result(Dialect::PostgreSQL.string_literal()(LocatedSpan::new(
                quoted,
            )));
            let expected = all_escaped.to_vec();
            assert_eq!(res, Ok((&b""[..], expected)));
        }

        #[test]
        fn literal_string_single_backslash_escape() {
            let all_escaped = br#"\0\'\"\b\n\r\t\Z\\\%\_"#;
            let start_quote = &b"E'"[..];
            let end_quote = &b"'"[..];
            let quoted = &[start_quote, &all_escaped[..], end_quote].concat();
            let res = to_nom_result(Dialect::PostgreSQL.string_literal()(LocatedSpan::new(
                quoted,
            )));
            let expected = "\0\'\"\x08\n\r\t\x1a\\%_".as_bytes().to_vec();
            assert_eq!(res, Ok((&b""[..], expected)));
        }

        #[test]
        fn literal_string_with_escape_character() {
            let lit = b"E'string'";
            assert_eq!(
                Dialect::PostgreSQL.string_literal()(LocatedSpan::new(lit))
                    .unwrap()
                    .1,
                b"string"
            );
        }

        #[test]
        fn bytes_parsing() {
            let res = to_nom_result(Dialect::PostgreSQL.bytes_literal()(LocatedSpan::new(
                b"E'\\\\x0008275c6480'::bytea",
            )));
            let expected = vec![0, 8, 39, 92, 100, 128];
            assert_eq!(res, Ok((&b""[..], expected)));

            let res = parse_dialect_bytes(Dialect::PostgreSQL, b"X'0008275c6480'");
            let expected = Ok((&b""[..], vec![0, 8, 39, 92, 100, 128]));
            assert_eq!(res, expected);
            let res = parse_dialect_bytes(Dialect::PostgreSQL, b"x'0008275c6480'");
            assert_eq!(res, expected);

            // Empty
            let res = to_nom_result(Dialect::PostgreSQL.bytes_literal()(LocatedSpan::new(
                b"E'\\\\x'::bytea",
            )));
            let expected = vec![];
            assert_eq!(res, Ok((&b""[..], expected)));

            // Malformed string
            let res = Dialect::PostgreSQL.bytes_literal()(LocatedSpan::new(b"E'\\\\'::btea"));
            res.unwrap_err();
        }

        #[test]
        #[ignore = "REA-4485"]
        fn bytes_parsing_odd_length() {
            // odd length is okay in postgres
            let res = parse_dialect_bytes(Dialect::PostgreSQL, b"X'D617263656C6F'");
            let expected = Ok((&b""[..], vec![13, 97, 114, 99, 101, 108, 111]));
            assert_eq!(res, expected);
        }
    }
}
