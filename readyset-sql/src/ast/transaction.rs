use std::fmt;

use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

// TODO(peter): Handle dialect differences.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub enum StartTransactionStatement {
    // TODO(ENG-2992): Implement optional fields for START TRANSACTION
    Start,
    Begin,
}

impl fmt::Display for StartTransactionStatement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            StartTransactionStatement::Start => write!(f, "START TRANSACTION"),
            StartTransactionStatement::Begin => write!(f, "BEGIN"),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct CommitStatement;

impl fmt::Display for CommitStatement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "COMMIT")
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct RollbackStatement;

impl fmt::Display for RollbackStatement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ROLLBACK")
    }
}
