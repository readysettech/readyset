use std::fmt;

use itertools::Itertools;
use readyset_util::fmt::fmt_with;
use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

use crate::{ast::*, AstConversionError, Dialect, DialectDisplay, IntoDialect, TryFromDialect};

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

impl TryFromDialect<sqlparser::ast::Statement> for TruncateStatement {
    fn try_from_dialect(
        value: sqlparser::ast::Statement,
        dialect: Dialect,
    ) -> Result<Self, AstConversionError> {
        if let sqlparser::ast::Statement::Truncate {
            table_names,
            partitions: _,
            table: _,
            // TODO(mvzink): I believe sqlparser is incorrect here; ONLY should be on each table
            // target, not the whole statement. We interpret it as applying to all tables, but
            // should probably upstream a fix (or verify that sqlparser is correct).
            // TODO(mohamed): upstream a fix REA-5744
            only,
            identity,
            cascade,
            on_cluster: _,
        } = value
        {
            let tables = table_names
                .into_iter()
                .map(|table| TruncateTable {
                    relation: table.name.into_dialect(dialect),
                    only,
                })
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
