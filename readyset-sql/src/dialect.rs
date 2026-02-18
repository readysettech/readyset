use std::{fmt, str::FromStr};

use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Specification for a SQL dialect to use when parsing
///
/// Currently, Dialect controls the escape characters used for identifiers, and the quotes used to
/// surround string literals, but may be extended to cover more dialect differences in the future
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, Serialize, Deserialize, ValueEnum)]
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

impl fmt::Display for Dialect {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PostgreSQL => f.write_str("PostgreSQL"),
            Self::MySQL => f.write_str("MySQL"),
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

    /// Returns true if the identifier string requires quoting to be used as an SQL identifier.
    ///
    /// An identifier needs quoting if it contains characters outside `[A-Za-z0-9_]` or starts
    /// with a digit.
    pub fn identifier_needs_quoting(ident: &str) -> bool {
        let mut chars = ident.chars();
        match chars.next() {
            None => true, // empty string always needs quoting
            Some(first) => {
                if !first.is_ascii_alphabetic() && first != '_' {
                    return true;
                }
                chars.any(|c| !c.is_ascii_alphanumeric() && c != '_')
            }
        }
    }

    /// Quotes the identifier only if it contains characters that require quoting; otherwise
    /// returns it as-is. Use this for function names in display output where unconditional quoting
    /// would change the SQL semantics.
    pub fn maybe_quote_identifier(self, ident: impl fmt::Display) -> String {
        let s = ident.to_string();
        if Self::identifier_needs_quoting(&s) {
            let quote = self.quote_identifier_char();
            format!(
                "{quote}{}{quote}",
                s.replace(quote, &format!("{quote}{quote}"))
            )
        } else {
            s
        }
    }
}
