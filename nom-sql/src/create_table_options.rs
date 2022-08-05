use std::fmt;
use std::ops::{Range, RangeFrom, RangeTo};
use std::str::FromStr;

use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case};
use nom::character::complete::{alphanumeric1, digit1};
use nom::combinator::{map, map_res, opt};
use nom::error::ParseError;
use nom::multi::separated_list0;
use nom::sequence::{separated_pair, tuple};
use nom::IResult;
use serde::{Deserialize, Serialize};

use crate::common::{ws_sep_comma, ws_sep_equals};
use crate::literal::integer_literal;
use crate::whitespace::{whitespace0, whitespace1};
use crate::{Dialect, Literal, NomSqlResult, Span, SqlIdentifier};

#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub enum CharsetName {
    Quoted(SqlIdentifier),
    Unquoted(SqlIdentifier),
}

impl fmt::Display for CharsetName {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CharsetName::Quoted(i) | CharsetName::Unquoted(i) => write!(f, "{i}"),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub enum CollationName {
    Quoted(SqlIdentifier),
    Unquoted(SqlIdentifier),
}

impl fmt::Display for CollationName {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CollationName::Quoted(i) | CollationName::Unquoted(i) => write!(f, "{i}"),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub enum CreateTableOption {
    AutoIncrement(u64),
    Engine(Option<String>),
    Charset(CharsetName),
    Collate(CollationName),
    Comment(String),
    /// Any currently uncotegorized option falls here
    /// TODO: implement other options
    Other,
}

impl fmt::Display for CreateTableOption {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CreateTableOption::AutoIncrement(v) => write!(f, "AUTO_INCREMENT={}", v),
            CreateTableOption::Engine(e) => {
                write!(f, "ENGINE={}", e.as_deref().unwrap_or(""))
            }
            CreateTableOption::Charset(c) => write!(f, "DEFAULT CHARSET={}", c),
            CreateTableOption::Collate(c) => write!(f, "COLLATE={}", c),
            CreateTableOption::Comment(c) => write!(f, "COMMENT='{}'", c),
            CreateTableOption::Other => Ok(()),
        }
    }
}

pub fn table_options(dialect: Dialect) -> impl Fn(Span) -> NomSqlResult<Vec<CreateTableOption>> {
    move |i| { separated_list0(table_options_separator, create_option(dialect)) }(i)
}

fn table_options_separator(i: Span) -> NomSqlResult<()> {
    map(
        alt((map(whitespace1, |_| "".as_bytes()), ws_sep_comma)),
        |_| (),
    )(i)
}

fn create_option(dialect: Dialect) -> impl Fn(Span) -> NomSqlResult<CreateTableOption> {
    move |i| {
        alt((
            map(create_option_type, |_| CreateTableOption::Other),
            map(create_option_pack_keys, |_| CreateTableOption::Other),
            create_option_engine,
            create_option_auto_increment,
            create_option_default_charset(dialect),
            create_option_collate(dialect),
            create_option_comment(dialect),
            map(create_option_max_rows, |_| CreateTableOption::Other),
            map(create_option_avg_row_length, |_| CreateTableOption::Other),
            map(create_option_row_format, |_| CreateTableOption::Other),
            map(create_option_key_block_size, |_| CreateTableOption::Other),
        ))(i)
    }
}

/// Helper to parse equals-separated create option pairs.
/// Throws away the create option and value
pub fn create_option_equals_pair<F, G, O1, O2>(
    mut first: F,
    mut second: G,
) -> impl FnMut(Span) -> NomSqlResult<O2>
where
    F: FnMut(Span) -> NomSqlResult<O1>,
    G: FnMut(Span) -> NomSqlResult<O2>,
{
    move |i| {
        let (i, _o1) = first(i)?;
        let (i, _) = ws_sep_equals(i)?;
        second(i)
    }
}

/// Helper to parse space-separated create option pairs.
/// Throws away the create option and value
pub fn create_option_spaced_pair<F, G, O1, O2>(
    mut first: F,
    mut second: G,
) -> impl FnMut(Span) -> NomSqlResult<O2>
where
    F: FnMut(Span) -> NomSqlResult<O1>,
    G: FnMut(Span) -> NomSqlResult<O2>,
{
    move |i| {
        let (i, _o1) = first(i)?;
        let (i, _) = whitespace1(i)?;
        second(i)
    }
}

fn create_option_type(i: Span) -> NomSqlResult<&[u8]> {
    create_option_equals_pair(tag_no_case("type"), alphanumeric1)(i)
}

fn create_option_pack_keys(i: Span) -> NomSqlResult<&[u8]> {
    create_option_equals_pair(
        tag_no_case("pack_keys"),
        alt((tag("0"), tag("1"), tag("default"))),
    )(i)
}

fn create_option_engine(i: Span) -> NomSqlResult<CreateTableOption> {
    map(
        create_option_equals_pair(
            tag_no_case("engine"),
            opt(map_res(
                map(alphanumeric1, |x: Span| *x),
                std::str::from_utf8,
            )),
        ),
        |l| CreateTableOption::Engine(l.map(str::to_string)),
    )(i)
}

fn create_option_auto_increment(i: Span) -> NomSqlResult<CreateTableOption> {
    map(
        create_option_equals_pair(
            tag_no_case("auto_increment"),
            map_res(
                map_res(map(digit1, |x: Span| *x), std::str::from_utf8),
                u64::from_str,
            ),
        ),
        CreateTableOption::AutoIncrement,
    )(i)
}

fn charset_name(dialect: Dialect) -> impl Fn(Span) -> NomSqlResult<CharsetName> {
    move |i| {
        alt((
            map(dialect.identifier(), CharsetName::Unquoted),
            map(map_res(dialect.string_literal(), String::from_utf8), |s| {
                CharsetName::Quoted(SqlIdentifier::from(s))
            }),
        ))(i)
    }
}

fn charset_prefix(i: Span) -> NomSqlResult<&[u8]> {
    let (i, _) = whitespace0(i)?;
    let (i, _) = tag_no_case("default")(i)?;
    let (i, _) = whitespace0(i)?;
    alt((
        tag_no_case("charset"),
        map(
            separated_pair(tag_no_case("character"), whitespace1, tag_no_case("set")),
            |_| "".as_bytes(),
        ),
    ))(i)
    // remaining whitespace is stripped in create_options_spaced_pair
}

fn create_option_default_charset(
    dialect: Dialect,
) -> impl Fn(Span) -> NomSqlResult<CreateTableOption> {
    move |i| {
        map(
            alt((
                create_option_equals_pair(charset_prefix, charset_name(dialect)),
                create_option_spaced_pair(charset_prefix, charset_name(dialect)),
            )),
            CreateTableOption::Charset,
        )(i)
    }
}

fn collation_name(dialect: Dialect) -> impl Fn(Span) -> NomSqlResult<CollationName> {
    move |i| {
        alt((
            map(dialect.identifier(), CollationName::Unquoted),
            map(map_res(dialect.string_literal(), String::from_utf8), |s| {
                CollationName::Quoted(SqlIdentifier::from(s))
            }),
        ))(i)
    }
}

fn create_option_collate(dialect: Dialect) -> impl Fn(Span) -> NomSqlResult<CreateTableOption> {
    move |i| {
        alt((
            map(
                create_option_equals_pair(tag_no_case("collate"), collation_name(dialect)),
                CreateTableOption::Collate,
            ),
            map(
                create_option_spaced_pair(tag_no_case("collate"), collation_name(dialect)),
                CreateTableOption::Collate,
            ),
        ))(i)
    }
}

fn create_option_comment(dialect: Dialect) -> impl Fn(Span) -> NomSqlResult<CreateTableOption> {
    move |i| {
        map(
            map_res(
                create_option_equals_pair(tag_no_case("comment"), dialect.string_literal()),
                String::from_utf8,
            ),
            CreateTableOption::Comment,
        )(i)
    }
}

fn create_option_max_rows(i: Span) -> NomSqlResult<Literal> {
    create_option_equals_pair(tag_no_case("max_rows"), integer_literal)(i)
}

fn create_option_avg_row_length(i: Span) -> NomSqlResult<Literal> {
    create_option_equals_pair(tag_no_case("avg_row_length"), integer_literal)(i)
}

fn create_option_row_format(i: Span) -> NomSqlResult<&[u8]> {
    tuple((
        tag_no_case("row_format"),
        whitespace0,
        opt(tag("=")),
        whitespace0,
        alt((
            tag_no_case("DEFAULT"),
            tag_no_case("DYNAMIC"),
            tag_no_case("FIXED"),
            tag_no_case("COMPRESSED"),
            tag_no_case("REDUNDANT"),
            tag_no_case("COMPACT"),
        )),
    ))(i)
    .map(|(i, t)| (i, t.4))
}

fn create_option_key_block_size(i: Span) -> NomSqlResult<Literal> {
    tuple((
        tag_no_case("key_block_size"),
        whitespace0,
        opt(tag("=")),
        whitespace0,
        integer_literal,
    ))(i)
    .map(|(i, t)| (i, t.4))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn should_parse_all(qstring: &str, cmp: Vec<CreateTableOption>) {
        assert_eq!(
            Ok((&b""[..], cmp)),
            table_options(Dialect::MySQL)(qstring.as_bytes())
        )
    }

    #[test]
    fn create_table_option_list_empty() {
        should_parse_all("", vec![]);
    }

    #[test]
    fn create_table_charset_collate_spaced_quoted_quoted() {
        should_parse_all(
            "DEFAULT CHARSET 'utf8mb4' COLLATE 'utf8mb4_unicode_520_ci'",
            vec![
                CreateTableOption::Charset(CharsetName::Quoted("utf8mb4".into())),
                CreateTableOption::Collate(CollationName::Quoted("utf8mb4_unicode_520_ci".into())),
            ],
        );
    }

    #[test]
    fn create_table_charset_collate_spaced_quoted_unquoted() {
        should_parse_all(
            "DEFAULT CHARSET 'utf8mb4' COLLATE utf8mb4_unicode_520_ci",
            vec![
                CreateTableOption::Charset(CharsetName::Quoted("utf8mb4".into())),
                CreateTableOption::Collate(CollationName::Unquoted(
                    "utf8mb4_unicode_520_ci".into(),
                )),
            ],
        );
    }

    #[test]
    fn create_table_charset_collate_spaced_unquoted_quoted() {
        should_parse_all(
            "DEFAULT CHARSET utf8mb4 COLLATE 'utf8mb4_unicode_520_ci'",
            vec![
                CreateTableOption::Charset(CharsetName::Unquoted("utf8mb4".into())),
                CreateTableOption::Collate(CollationName::Quoted("utf8mb4_unicode_520_ci".into())),
            ],
        );
    }

    #[test]
    fn create_table_charset_collate_spaced_unquoted_unquoted() {
        should_parse_all(
            "DEFAULT  CHARSET  utf8mb4  COLLATE  utf8mb4_unicode_520_ci",
            vec![
                CreateTableOption::Charset(CharsetName::Unquoted("utf8mb4".into())),
                CreateTableOption::Collate(CollationName::Unquoted(
                    "utf8mb4_unicode_520_ci".into(),
                )),
            ],
        );
    }

    #[test]
    fn create_table_option_list() {
        should_parse_all(
            "ENGINE=InnoDB AUTO_INCREMENT=44782967 \
             DEFAULT CHARSET=binary ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8",
            vec![
                CreateTableOption::Engine(Some("InnoDB".to_string())),
                CreateTableOption::AutoIncrement(44782967),
                CreateTableOption::Charset(CharsetName::Unquoted("binary".into())),
                CreateTableOption::Other,
                CreateTableOption::Other,
            ],
        );
    }

    #[test]
    fn create_table_charset_extra_spacing() {
        should_parse_all(
            "DEFAULT CHARSET  utf8mb4",
            vec![CreateTableOption::Charset(CharsetName::Unquoted(
                "utf8mb4".into(),
            ))],
        );
    }

    #[test]
    fn create_table_character_set_extra_spacing() {
        should_parse_all(
            "DEFAULT CHARACTER   SET  utf8mb4",
            vec![CreateTableOption::Charset(CharsetName::Unquoted(
                "utf8mb4".into(),
            ))],
        );
    }

    #[test]
    fn create_table_option_list_commaseparated() {
        should_parse_all(
            "AUTO_INCREMENT=1,ENGINE=,KEY_BLOCK_SIZE=8",
            vec![
                CreateTableOption::AutoIncrement(1),
                CreateTableOption::Engine(None),
                CreateTableOption::Other,
            ],
        );
    }
}
