use nom::character::complete::{alphanumeric1, multispace0, multispace1};

use common::{integer_literal, sql_identifier, string_literal, ws_sep_comma};
use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case};
use nom::combinator::{map, opt};
use nom::multi::separated_list;
use nom::sequence::{preceded, tuple};
use nom::IResult;

pub fn table_options(i: &[u8]) -> IResult<&[u8], ()> {
    map(
        separated_list(table_options_separator, create_option),
        |_| (),
    )(i)
}

fn table_options_separator(i: &[u8]) -> IResult<&[u8], ()> {
    map(
        alt((multispace1, ws_sep_comma)),
        |_| (),
    )(i)
}

fn create_option(i: &[u8]) -> IResult<&[u8], ()> {
    alt((
        create_option_type,
        create_option_pack_keys,
        create_option_engine,
        create_option_auto_increment,
        create_option_default_charset,
        create_option_collate,
        create_option_comment,
        create_option_max_rows,
        create_option_avg_row_length,
        create_option_row_format,
        create_option_key_block_size,
    ))(i)
}

fn create_option_type(i: &[u8]) -> IResult<&[u8], ()> {
    map(
        preceded(
            tag_no_case("type"),
            preceded(
                multispace0,
                preceded(tag("="), preceded(multispace0, alphanumeric1)),
            ),
        ),
        |_| (),
    )(i)
}

fn create_option_pack_keys(i: &[u8]) -> IResult<&[u8], ()> {
    let (remaining_input, (_, _, _, _, _)) = tuple((
        tag_no_case("pack_keys"),
        multispace0,
        tag("="),
        multispace0,
        alt((tag("0"), tag("1"))),
    ))(i)?;
    Ok((remaining_input, ()))
}

fn create_option_engine(i: &[u8]) -> IResult<&[u8], ()> {
    let (remaining_input, (_, _, _, _, _)) = tuple((
        tag_no_case("engine"),
        multispace0,
        tag("="),
        multispace0,
        opt(alphanumeric1),
    ))(i)?;
    Ok((remaining_input, ()))
}

fn create_option_auto_increment(i: &[u8]) -> IResult<&[u8], ()> {
    let (remaining_input, (_, _, _, _, _)) = tuple((
        tag_no_case("auto_increment"),
        multispace0,
        tag("="),
        multispace0,
        integer_literal,
    ))(i)?;
    Ok((remaining_input, ()))
}

fn create_option_default_charset(i: &[u8]) -> IResult<&[u8], ()> {
    let (remaining_input, (_, _, _, _, _)) = tuple((
        tag_no_case("default charset"),
        multispace0,
        tag("="),
        multispace0,
        alt((
            tag("utf8mb4"),
            tag("utf8"),
            tag("binary"),
            tag("big5"),
            tag("ucs2"),
            tag("latin1"),
        )),
    ))(i)?;
    Ok((remaining_input, ()))
}

fn create_option_collate(i: &[u8]) -> IResult<&[u8], ()> {
    let (remaining_input, (_, _, _, _, _)) = tuple((
        tag_no_case("collate"),
        multispace0,
        tag("="),
        multispace0,
        // TODO(malte): imprecise hack, should not accept everything
        sql_identifier,
    ))(i)?;
    Ok((remaining_input, ()))
}

fn create_option_comment(i: &[u8]) -> IResult<&[u8], ()> {
    let (remaining_input, (_, _, _, _, _)) = tuple((
        tag_no_case("comment"),
        multispace0,
        tag("="),
        multispace0,
        string_literal,
    ))(i)?;
    Ok((remaining_input, ()))
}

fn create_option_max_rows(i: &[u8]) -> IResult<&[u8], ()> {
    let (remaining_input, (_, _, _, _, _)) = tuple((
        tag_no_case("max_rows"),
        multispace0,
        opt(tag("=")),
        multispace0,
        integer_literal,
    ))(i)?;
    Ok((remaining_input, ()))
}

fn create_option_avg_row_length(i: &[u8]) -> IResult<&[u8], ()> {
    let (remaining_input, (_, _, _, _, _)) = tuple((
        tag_no_case("avg_row_length"),
        multispace0,
        opt(tag("=")),
        multispace0,
        integer_literal,
    ))(i)?;
    Ok((remaining_input, ()))
}

fn create_option_row_format(i: &[u8]) -> IResult<&[u8], ()> {
    let (remaining_input, (_, _, _, _, _)) = tuple((
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
    ))(i)?;
    Ok((remaining_input, ()))
}

fn create_option_key_block_size(i: &[u8]) -> IResult<&[u8], ()> {
    let (remaining_input, (_, _, _, _, _)) = tuple((
        tag_no_case("key_block_size"),
        multispace0,
        opt(tag("=")),
        multispace0,
        integer_literal,
    ))(i)?;
    Ok((remaining_input, ()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn should_parse_all(qstring: &str) {
        assert_eq!(Ok((&b""[..], ())), table_options(qstring.as_bytes()))
    }

    #[test]
    fn create_table_option_list_empty() {
        should_parse_all("");
    }

    #[test]
    fn create_table_option_list() {
        should_parse_all(
            "ENGINE=InnoDB AUTO_INCREMENT=44782967 \
             DEFAULT CHARSET=binary ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8",
        );
    }

    #[test]
    fn create_table_option_list_commaseparated() {
        should_parse_all("AUTO_INCREMENT=1,ENGINE=,KEY_BLOCK_SIZE=8");
    }
}
