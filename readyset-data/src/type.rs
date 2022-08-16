use std::convert::TryFrom;
use std::fmt;

use nom_sql::SqlType;
use readyset_errors::{internal, ReadySetError};
use serde::{Deserialize, Serialize};

/// noria representation of SqlType.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub enum DfType {
    /// One of `nom_sql::SqlType`
    Sql(SqlType),
    /// The type cannot be inferred. (e.g., for the `Literal` NULL in 'SELECT NULL')
    Unknown,
}

impl From<SqlType> for DfType {
    fn from(ty: SqlType) -> Self {
        Self::Sql(ty)
    }
}

impl From<Option<SqlType>> for DfType {
    fn from(ty: Option<SqlType>) -> Self {
        ty.map(Self::Sql).unwrap_or(Self::Unknown)
    }
}

impl From<DfType> for Option<SqlType> {
    fn from(val: DfType) -> Self {
        match val {
            DfType::Sql(ty) => Some(ty),
            DfType::Unknown => None,
        }
    }
}

impl TryFrom<DfType> for SqlType {
    type Error = ReadySetError;

    fn try_from(val: DfType) -> Result<Self, Self::Error> {
        match val {
            DfType::Sql(ty) => Ok(ty),
            DfType::Unknown => internal!("Attempted to coerce an Unknown type to a SqlType"),
        }
    }
}

impl Default for DfType {
    fn default() -> Self {
        Self::Unknown
    }
}

impl fmt::Display for DfType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DfType::Unknown => write!(f, "Unknown"),
            DfType::Sql(ty) => write!(f, "SqlType({})", ty),
        }
    }
}
