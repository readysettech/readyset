use std::fmt;

use itertools::Itertools;
use readyset_util::fmt::fmt_with;
use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

use crate::{
    AstConversionError, Dialect, DialectDisplay, FromDialect, IntoDialect, TryFromDialect, ast::*,
};

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

impl FromDialect<sqlparser::ast::TruncateTableTarget> for TruncateTable {
    fn from_dialect(value: sqlparser::ast::TruncateTableTarget, dialect: Dialect) -> Self {
        Self {
            relation: value.name.into_dialect(dialect),
            only: value.only,
        }
    }
}

impl TryFromDialect<sqlparser::ast::Statement> for TruncateStatement {
    fn try_from_dialect(
        value: sqlparser::ast::Statement,
        dialect: Dialect,
    ) -> Result<Self, AstConversionError> {
        if let sqlparser::ast::Statement::Truncate {
            table_names,
            partitions: _,
            table: _,
            identity,
            cascade,
            on_cluster: _,
        } = value
        {
            let tables = table_names
                .into_iter()
                .map(|tn| tn.into_dialect(dialect))
                .collect();
            Ok(Self {
                tables,
                restart_identity: identity == Some(sqlparser::ast::TruncateIdentityOption::Restart),
                cascade: cascade == Some(sqlparser::ast::CascadeOption::Cascade),
            })
        } else {
            failed!("Should only be called on TRUNCATE statement, got {value:?}")
        }
    }
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
