use std::{fmt, str::FromStr};

use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Specification for a SQL dialect to use when parsing
///
/// Currently, Dialect controls the escape characters used for identifiers, and the quotes used to
/// surround string literals, but may be extended to cover more dialect differences in the future
#[derive(Debug, PartialEq, Eq, Clone, Copy, Serialize, Deserialize, ValueEnum)]
#[value(rename_all = "lower")]
pub enum Dialect {
    /// The SQL dialect used by PostgreSQL.
    ///
    /// Identifiers are escaped with double quotes (`"`) and strings use only single quotes (`'`)
    #[value(alias("postgres"))]
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
    /// All SQL dialects.
    pub const ALL: &'static [Self] = &[Self::MySQL, Self::PostgreSQL];

    /// Returns the table/column identifier quoting character for this dialect.
    pub fn quote_identifier_char(self) -> char {
        match self {
            Self::PostgreSQL => '"',
            Self::MySQL => '`',
        }
    }

    /// Quotes the table/column identifier appropriately for this dialect.
    pub fn quote_identifier(self, ident: impl fmt::Display) -> impl fmt::Display {
        let quote = self.quote_identifier_char();
        readyset_util::fmt_args!(
            "{quote}{}{quote}",
            ident.to_string().replace(quote, &format!("{quote}{quote}"))
        )
    }
}
