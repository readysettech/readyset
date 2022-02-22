use nom_sql::SqlType;
use serde::{Deserialize, Serialize};

/// noria representation of SqlType.
#[derive(Clone, Serialize, Deserialize, Debug)]
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

impl Default for Type {
    fn default() -> Self {
        Self::Unknown
    }
}
