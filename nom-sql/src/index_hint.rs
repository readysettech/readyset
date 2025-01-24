use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case};
use nom::combinator::{map, opt, value};
use nom::multi::separated_list1;
use nom::sequence::preceded;
use nom_locate::LocatedSpan;
use readyset_sql::{ast::*, Dialect};

use crate::common::ws_sep_comma;
use crate::dialect::DialectParser;
use crate::whitespace::{whitespace0, whitespace1};
use crate::NomSqlResult;

/// Parse an identifier or the PRIMARY keyword.
/// This is used to parse the index list.
/// PRIMARY is a reserved KEYWORD, and identifier() does not accept reserved keywords.
fn identifier_or_primary(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], SqlIdentifier> {
    move |i| {
        let (i, _) = whitespace0(i)?;
        let (i, identifier) = alt((
            map(dialect.identifier(), |ident| ident),
            value(SqlIdentifier::from("PRIMARY"), tag_no_case("PRIMARY")),
        ))(i)?;
        let (i, _) = whitespace0(i)?;

        Ok((i, identifier))
    }
}

/// Parse an index hint.
pub fn index_hint_list(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], IndexHint> {
    move |i| {
        let (i, _) = whitespace1(i)?;
        let (i, hint_type) = alt((
            value(IndexHintType::Use, tag_no_case("USE")),
            value(IndexHintType::Ignore, tag_no_case("IGNORE")),
            value(IndexHintType::Force, tag_no_case("FORCE")),
        ))(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, index_or_key) = alt((
            value(IndexOrKeyType::Index, tag_no_case("INDEX")),
            value(IndexOrKeyType::Key, tag_no_case("KEY")),
        ))(i)?;
        let (i, index_usage_type) = opt(preceded(
            whitespace1,
            alt((
                value(IndexUsageType::Join, tag_no_case("FOR JOIN")),
                value(IndexUsageType::OrderBy, tag_no_case("FOR ORDER BY")),
                value(IndexUsageType::GroupBy, tag_no_case("FOR GROUP BY")),
            )),
        ))(i)?;
        let (i, _) = whitespace0(i)?;
        let (i, _) = tag("(")(i)?;
        let (i, _) = whitespace0(i)?;
        let (i, index_list) = separated_list1(ws_sep_comma, identifier_or_primary(dialect))(i)?;

        let (i, _) = whitespace0(i)?;
        let (i, _) = tag(")")(i)?;

        Ok((
            i,
            IndexHint {
                hint_type,
                index_or_key,
                index_usage_type,
                index_list,
            },
        ))
    }
}
