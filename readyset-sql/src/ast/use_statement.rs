use std::fmt;

use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

use crate::ast::*;

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct UseStatement {
    pub database: SqlIdentifier,
}

impl UseStatement {
    pub fn from_database(database: SqlIdentifier) -> Self {
        Self { database }
    }
}

impl fmt::Display for UseStatement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "USE {}", self.database)
    }
}
