use std::fmt;

use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

use crate::{ast::*, Dialect, FromDialect, IntoDialect};

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct UseStatement {
    pub database: SqlIdentifier,
}

impl UseStatement {
    pub fn from_database(database: SqlIdentifier) -> Self {
        Self { database }
    }
}

impl FromDialect<sqlparser::ast::Use> for UseStatement {
    fn from_dialect(value: sqlparser::ast::Use, dialect: Dialect) -> Self {
        match value {
            sqlparser::ast::Use::Object(mut object) if object.0.len() == 1 => Self {
                database: object.0.pop().unwrap().into_dialect(dialect),
            },
            _ => unimplemented!("unsupported use statement {value:?}"),
        }
    }
}

impl fmt::Display for UseStatement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "USE {}", self.database)
    }
}
