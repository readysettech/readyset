use crate::common::{integer_literal, ws_sep_comma, ws_sep_equals, Literal};
use crate::Dialect;
use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case};
use nom::character::complete::{alphanumeric1, digit1, multispace0, multispace1};
use nom::combinator::{map, map_res, opt};
use nom::multi::separated_list;
use nom::sequence::tuple;
use nom::IResult;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub enum CreateTableOption {
    AutoIncrement(u64),
    Engine(Option<String>),
    Charset(String),
    Collate(String),
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

pub fn table_options(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], Vec<CreateTableOption>> {
    move |i| { separated_list(table_options_separator, create_option(dialect)) }(i)
}

fn table_options_separator(i: &[u8]) -> IResult<&[u8], ()> {
    map(alt((multispace1, ws_sep_comma)), |_| ())(i)
}

fn create_option(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], CreateTableOption> {
    move |i| {
        alt((
            map(create_option_type, |_| CreateTableOption::Other),
            map(create_option_pack_keys, |_| CreateTableOption::Other),
            create_option_engine,
            create_option_auto_increment,
            create_option_default_charset,
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
pub fn create_option_equals_pair<'a, I, O1, O2, F, G>(
    first: F,
    second: G,
) -> impl Fn(I) -> IResult<I, O2>
where
    F: Fn(I) -> IResult<I, O1>,
    G: Fn(I) -> IResult<I, O2>,
    I: nom::InputTakeAtPosition + nom::InputTake + nom::Compare<&'a str>,
    <I as nom::InputTakeAtPosition>::Item: nom::AsChar + Clone,
{
    move |i: I| {
        let (i, _o1) = first(i)?;
        let (i, _) = ws_sep_equals(i)?;
        second(i)
    }
}

/// Helper to parse space-separated create option pairs.
/// Throws away the create option and value
pub fn create_option_spaced_pair<'a, I, O1, O2, F, G>(
    first: F,
    second: G,
) -> impl Fn(I) -> IResult<I, O2>
where
    F: Fn(I) -> IResult<I, O1>,
    G: Fn(I) -> IResult<I, O2>,
    I: nom::InputTakeAtPosition + nom::InputTake + nom::Compare<&'a str>,
    <I as nom::InputTakeAtPosition>::Item: nom::AsChar + Clone,
{
    move |i: I| {
        let (i, _o1) = first(i)?;
        let (i, _) = multispace1(i)?;
        second(i)
    }
}

fn create_option_type(i: &[u8]) -> IResult<&[u8], &[u8]> {
    create_option_equals_pair(tag_no_case("type"), alphanumeric1)(i)
}

fn create_option_pack_keys(i: &[u8]) -> IResult<&[u8], &[u8]> {
    create_option_equals_pair(
        tag_no_case("pack_keys"),
        alt((tag("0"), tag("1"), tag("default"))),
    )(i)
}

fn create_option_engine(i: &[u8]) -> IResult<&[u8], CreateTableOption> {
    map(
        create_option_equals_pair(
            tag_no_case("engine"),
            opt(map_res(alphanumeric1, std::str::from_utf8)),
        ),
        |l| CreateTableOption::Engine(l.map(str::to_string)),
    )(i)
}

fn create_option_auto_increment(i: &[u8]) -> IResult<&[u8], CreateTableOption> {
    map(
        create_option_equals_pair(
            tag_no_case("auto_increment"),
            map_res(map_res(digit1, std::str::from_utf8), u64::from_str),
        ),
        CreateTableOption::AutoIncrement,
    )(i)
}

fn create_option_default_charset(i: &[u8]) -> IResult<&[u8], CreateTableOption> {
    // TODO:  Deduplicate the branch contents
    map(
        map(
            map_res(
                alt((
                    create_option_equals_pair(
                        alt((
                            tag_no_case("default charset"),
                            tag_no_case("default character set"),
                        )),
                        alt((
                            tag("utf8mb4"),
                            tag("utf8mb3"),
                            tag("utf8"),
                            tag("binary"),
                            tag("big5"),
                            tag("ucs2"),
                            tag("latin1"),
                        )),
                    ),
                    create_option_spaced_pair(
                        alt((
                            tag_no_case("default charset"),
                            tag_no_case("default character set"),
                        )),
                        alt((
                            tag("utf8mb4"),
                            tag("utf8mb3"),
                            tag("utf8"),
                            tag("binary"),
                            tag("big5"),
                            tag("ucs2"),
                            tag("latin1"),
                        )),
                    ),
                )),
                std::str::from_utf8,
            ),
            str::to_string,
        ),
        CreateTableOption::Charset,
    )(i)
}

fn create_option_collate(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], CreateTableOption> {
    move |i| {
        alt((
            map(
                create_option_equals_pair(
                    tag_no_case("collate"),
                    // TODO(malte): imprecise hack, should not accept everything
                    dialect.identifier(),
                ),
                |v| CreateTableOption::Collate(v.to_string()),
            ),
            map(
                map_res(
                    create_option_spaced_pair(
                        tag_no_case("collate"),
                        // TODO(malte): imprecise hack, should not accept everything
                        dialect.string_literal(),
                    ),
                    String::from_utf8,
                ),
                CreateTableOption::Collate,
            ),
        ))(i)
    }
}

fn create_option_comment(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], CreateTableOption> {
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

fn create_option_max_rows(i: &[u8]) -> IResult<&[u8], Literal> {
    create_option_equals_pair(tag_no_case("max_rows"), integer_literal)(i)
}

fn create_option_avg_row_length(i: &[u8]) -> IResult<&[u8], Literal> {
    create_option_equals_pair(tag_no_case("avg_row_length"), integer_literal)(i)
}

fn create_option_row_format(i: &[u8]) -> IResult<&[u8], &[u8]> {
    tuple((
        tag_no_case("row_format"),
        multispace0,
        opt(tag("=")),
        multispace0,
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

fn create_option_key_block_size(i: &[u8]) -> IResult<&[u8], Literal> {
    tuple((
        tag_no_case("key_block_size"),
        multispace0,
        opt(tag("=")),
        multispace0,
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
    fn create_table_option_list() {
        should_parse_all(
            "ENGINE=InnoDB AUTO_INCREMENT=44782967 \
             DEFAULT CHARSET=binary ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8",
            vec![
                CreateTableOption::Engine(Some("InnoDB".to_string())),
                CreateTableOption::AutoIncrement(44782967),
                CreateTableOption::Charset("binary".to_string()),
                CreateTableOption::Other,
                CreateTableOption::Other,
            ],
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
