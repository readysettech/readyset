use std::fmt;

use itertools::Itertools;
use readyset_util::fmt::fmt_with;
use serde::{Deserialize, Serialize};
use sqlparser::ast::RenameTable;
use test_strategy::Arbitrary;

use crate::{AstConversionError, Dialect, DialectDisplay, IntoDialect, TryFromDialect, ast::*};

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

impl TryFromDialect<sqlparser::ast::RenameTable> for RenameTableOperation {
    fn try_from_dialect(
        value: sqlparser::ast::RenameTable,
        dialect: Dialect,
    ) -> Result<Self, AstConversionError> {
        let RenameTable { old_name, new_name } = value;
        Ok(Self {
            from: old_name.into_dialect(dialect),
            to: new_name.into_dialect(dialect),
        })
    }
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
