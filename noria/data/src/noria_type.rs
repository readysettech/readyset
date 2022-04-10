use std::convert::TryFrom;
use std::fmt;

use nom_sql::SqlType;
use noria_errors::{internal, ReadySetError};
use serde::{Deserialize, Serialize};

/// noria representation of SqlType.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub enum Type {
    /// One of `nom_sql::SqlType`
    Sql(SqlType),
    /// The type cannot be inferred. (e.g., for the `Literal` NULL in 'SELECT NULL')
    Unknown,
}

impl From<SqlType> for Type {
    fn from(ty: SqlType) -> Self {
        Self::Sql(ty)
    }
}

impl From<Option<SqlType>> for Type {
    fn from(ty: Option<SqlType>) -> Self {
        ty.map(Self::Sql).unwrap_or(Self::Unknown)
    }
}

impl From<Type> for Option<SqlType> {
    fn from(val: Type) -> Self {
        match val {
            Type::Sql(ty) => Some(ty),
            Type::Unknown => None,
        }
    }
}

impl TryFrom<Type> for SqlType {
    type Error = ReadySetError;

    fn try_from(val: Type) -> Result<Self, Self::Error> {
        match val {
            Type::Sql(ty) => Ok(ty),
            Type::Unknown => internal!("Attempted to coerce an Unknown type to a SqlType"),
        }
    }
}

impl Default for Type {
    fn default() -> Self {
        Self::Unknown
    }
}

impl fmt::Display for Type {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Type::Unknown => write!(f, "Unknown"),
            Type::Sql(ty) => write!(f, "SqlType({})", ty),
        }
    }
}
