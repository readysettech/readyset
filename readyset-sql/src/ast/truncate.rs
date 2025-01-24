use std::fmt;

use itertools::Itertools;
use readyset_util::fmt::fmt_with;
use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

use crate::{ast::*, Dialect, DialectDisplay};

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]

pub struct TruncateTable {
    pub relation: Relation,
    pub only: bool,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct TruncateStatement {
    pub tables: Vec<TruncateTable>,
    pub restart_identity: bool,
    pub cascade: bool,
}

impl DialectDisplay for TruncateStatement {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| {
            write!(f, "TRUNCATE ")?;

            write!(
                f,
                "{}",
                self.tables
                    .iter()
                    .map(|t| format!(
                        "{}{}",
                        if t.only { "ONLY " } else { "" },
                        t.relation.display(dialect)
                    ))
                    .join(", ")
            )?;

            if self.restart_identity {
                write!(f, " RESTART IDENTITY")?;
            }

            if self.cascade {
                write!(f, " CASCADE")?;
            }

            Ok(())
        })
    }
}
