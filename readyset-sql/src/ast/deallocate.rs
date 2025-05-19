use std::fmt;

use readyset_util::fmt::fmt_with;
use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

use crate::{Dialect, DialectDisplay};

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct DeallocateStatement {
    pub identifier: StatementIdentifier,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub enum StatementIdentifier {
    SingleStatement(String),
    AllStatements,
}

impl DialectDisplay for DeallocateStatement {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| {
            write!(f, "DEALLOCATE ")?;

            // PREPARE is required for MySQL, but optional in PG (although
            // hardly used, so ignore it by convention)
            if dialect == Dialect::MySQL {
                write!(f, "PREPARE ")?;
            }

            match &self.identifier {
                StatementIdentifier::AllStatements => write!(f, "ALL"),
                StatementIdentifier::SingleStatement(id) => write!(f, "{id}"),
            }
        })
    }
}
