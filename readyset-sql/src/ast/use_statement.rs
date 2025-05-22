use std::fmt;

use crate::ast::SqlIdentifier;
use crate::{AstConversionError, Dialect, IntoDialect, TryFromDialect};
use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct UseStatement {
    pub database: SqlIdentifier,
}

impl UseStatement {
    pub fn from_database(database: SqlIdentifier) -> Self {
        Self { database }
    }
}

impl TryFromDialect<sqlparser::ast::Use> for UseStatement {
    fn try_from_dialect(
        value: sqlparser::ast::Use,
        dialect: Dialect,
    ) -> Result<Self, AstConversionError> {
        match value {
            sqlparser::ast::Use::Object(mut object) if object.0.len() == 1 => Ok(Self {
                database: object.0.pop().unwrap().into_dialect(dialect),
            }),
            // can also be skipped!, but not much of a difference between them
            _ => unsupported!("unsupported use statement {value:?}"),
        }
    }
}

impl fmt::Display for UseStatement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "USE {}", self.database)
    }
}
