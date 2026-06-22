use std::fmt;

use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

use crate::AstConversionError;

/// The object class targeted by a Postgres `DISCARD` statement.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub enum DiscardObject {
    All,
    Plans,
    Sequences,
    Temporary,
}

impl TryFrom<sqlparser::ast::DiscardObject> for DiscardObject {
    type Error = AstConversionError;

    fn try_from(value: sqlparser::ast::DiscardObject) -> Result<Self, Self::Error> {
        match value {
            sqlparser::ast::DiscardObject::ALL => Ok(DiscardObject::All),
            sqlparser::ast::DiscardObject::PLANS => Ok(DiscardObject::Plans),
            sqlparser::ast::DiscardObject::SEQUENCES => Ok(DiscardObject::Sequences),
            sqlparser::ast::DiscardObject::TEMP => Ok(DiscardObject::Temporary),
        }
    }
}

impl fmt::Display for DiscardObject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DiscardObject::All => write!(f, "ALL"),
            DiscardObject::Plans => write!(f, "PLANS"),
            DiscardObject::Sequences => write!(f, "SEQUENCES"),
            DiscardObject::Temporary => write!(f, "TEMPORARY"),
        }
    }
}

/// A Postgres `DISCARD { ALL | PLANS | SEQUENCES | TEMPORARY }` statement.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct DiscardStatement {
    pub object_type: DiscardObject,
}

impl fmt::Display for DiscardStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DISCARD {}", self.object_type)
    }
}
