use std::fmt::Display;

use itertools::Itertools;
use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case};
use nom::combinator::{map, opt, value};
use nom::multi::separated_list1;
use nom::sequence::preceded;
use nom_locate::LocatedSpan;
use readyset_util::fmt::fmt_with;
use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

use crate::common::ws_sep_comma;
use crate::whitespace::{whitespace0, whitespace1};
use crate::{Dialect, DialectDisplay, NomSqlResult, SqlIdentifier};

/// Type of index hint.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Arbitrary)]
pub enum IndexHintType {
    /// Use index hint.
    Use,

    /// Ignore index hint.
    Ignore,

    /// Force index hint.
    Force,
}

impl From<&str> for IndexHintType {
    fn from(s: &str) -> Self {
        match s {
            "USE" => IndexHintType::Use,
            "IGNORE" => IndexHintType::Ignore,
            "FORCE" => IndexHintType::Force,
            _ => panic!("Invalid index hint type: {}", s),
        }
    }
}

impl From<&&str> for IndexHintType {
    fn from(s: &&str) -> Self {
        IndexHintType::from(*s)
    }
}

impl From<IndexHintType> for &str {
    fn from(t: IndexHintType) -> &'static str {
        match t {
            IndexHintType::Use => "USE",
            IndexHintType::Ignore => "IGNORE",
            IndexHintType::Force => "FORCE",
        }
    }
}

impl<'a> From<LocatedSpan<&'a [u8]>> for IndexHintType {
    fn from(span: LocatedSpan<&'a [u8]>) -> Self {
        let s = span.fragment();
        let str_slice = std::str::from_utf8(s).expect("Invalid UTF-8 string");
        IndexHintType::from(str_slice)
    }
}

/// Type of index or key.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Arbitrary)]
pub enum IndexOrKeyType {
    /// Index.
    Index,

    /// Key.
    Key,
}

impl From<&str> for IndexOrKeyType {
    fn from(s: &str) -> Self {
        match s {
            "INDEX" => IndexOrKeyType::Index,
            "KEY" => IndexOrKeyType::Key,
            _ => panic!("Invalid index or key type: {}", s),
        }
    }
}

impl From<&&str> for IndexOrKeyType {
    fn from(s: &&str) -> Self {
        IndexOrKeyType::from(*s)
    }
}

impl From<IndexOrKeyType> for &str {
    fn from(t: IndexOrKeyType) -> &'static str {
        match t {
            IndexOrKeyType::Index => "INDEX",
            IndexOrKeyType::Key => "KEY",
        }
    }
}

/// Index usage type.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Arbitrary)]
pub enum IndexUsageType {
    /// FOR JOIN
    Join,

    /// FOR ORDER BY
    OrderBy,

    /// FOR GROUP BY
    GroupBy,
}

impl From<&str> for IndexUsageType {
    fn from(s: &str) -> Self {
        match s {
            "FOR JOIN" => IndexUsageType::Join,
            "FOR ORDER BY" => IndexUsageType::OrderBy,
            "FOR GROUP BY" => IndexUsageType::GroupBy,
            _ => panic!("Invalid index usage type: {}", s),
        }
    }
}

impl From<&&str> for IndexUsageType {
    fn from(s: &&str) -> Self {
        IndexUsageType::from(*s)
    }
}

impl From<IndexUsageType> for &str {
    fn from(t: IndexUsageType) -> &'static str {
        match t {
            IndexUsageType::Join => " FOR JOIN",
            IndexUsageType::OrderBy => " FOR ORDER BY",
            IndexUsageType::GroupBy => " FOR GROUP BY",
        }
    }
}
/// Index hints for a query.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Arbitrary)]
pub struct IndexHint {
    /// Type of index hint.
    pub hint_type: IndexHintType,

    /// Type of index or key.
    pub index_or_key: IndexOrKeyType,

    /// Index usage type.
    pub index_usage_type: Option<IndexUsageType>,

    /// List of indexes.
    pub index_list: Vec<SqlIdentifier>,
}

impl IndexHint {
    /// Create a new index hint.
    pub fn default() -> Self {
        Self {
            hint_type: IndexHintType::Use,
            index_or_key: IndexOrKeyType::Index,
            index_usage_type: None,
            index_list: vec![],
        }
    }
}

impl DialectDisplay for IndexHint {
    fn display(&self, _dialect: Dialect) -> impl Display + '_ {
        fmt_with(move |f| {
            let hint_type: &str = self.hint_type.clone().into();
            let index_or_key: &str = self.index_or_key.clone().into();
            let index_usage: &str = self
                .index_usage_type
                .as_ref()
                .map(|t| t.clone().into())
                .unwrap_or("");
            let index_list = self.index_list.iter().map(|t| t.to_owned()).join(", ");
            write!(
                f,
                "{} {}{} ({})",
                hint_type, index_or_key, index_usage, index_list
            )
        })
    }
}

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
