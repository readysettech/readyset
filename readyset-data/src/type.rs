use std::convert::TryFrom;
use std::fmt;

use nom_sql::SqlType;
use readyset_errors::{internal, ReadySetError};
use serde::{Deserialize, Serialize};

/// noria representation of SqlType.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub enum DataflowType {
    /// One of `nom_sql::SqlType`
    Sql(SqlType),
    /// The type cannot be inferred. (e.g., for the `Literal` NULL in 'SELECT NULL')
    Unknown,
}

impl From<SqlType> for DataflowType {
    fn from(ty: SqlType) -> Self {
        Self::Sql(ty)
    }
}

impl From<Option<SqlType>> for DataflowType {
    fn from(ty: Option<SqlType>) -> Self {
        ty.map(Self::Sql).unwrap_or(Self::Unknown)
    }
}

impl From<DataflowType> for Option<SqlType> {
    fn from(val: DataflowType) -> Self {
        match val {
            DataflowType::Sql(ty) => Some(ty),
            DataflowType::Unknown => None,
        }
    }
}

impl TryFrom<DataflowType> for SqlType {
    type Error = ReadySetError;

    fn try_from(val: DataflowType) -> Result<Self, Self::Error> {
        match val {
            DataflowType::Sql(ty) => Ok(ty),
            DataflowType::Unknown => internal!("Attempted to coerce an Unknown type to a SqlType"),
        }
    }
}

impl Default for DataflowType {
    fn default() -> Self {
        Self::Unknown
    }
}

impl fmt::Display for DataflowType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DataflowType::Unknown => write!(f, "Unknown"),
            DataflowType::Sql(ty) => write!(f, "SqlType({})", ty),
        }
    }
}
