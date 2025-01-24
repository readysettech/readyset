use std::fmt;

use itertools::Itertools;
use readyset_util::fmt::fmt_with;
use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

use crate::{ast::*, Dialect, DialectDisplay};

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct RenameTableStatement {
    pub ops: Vec<RenameTableOperation>,
}

impl DialectDisplay for RenameTableStatement {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| {
            write!(
                f,
                "RENAME TABLE {}",
                self.ops.iter().map(|op| op.display(dialect)).join(", ")
            )
        })
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct RenameTableOperation {
    pub from: Relation,
    pub to: Relation,
}

impl DialectDisplay for RenameTableOperation {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| {
            write!(
                f,
                "{} TO {}",
                self.from.display(dialect),
                self.to.display(dialect)
            )
        })
    }
}
