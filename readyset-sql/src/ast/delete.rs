use std::fmt;

use readyset_util::fmt::fmt_with;
use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

use crate::{ast::*, Dialect, DialectDisplay};

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct DeleteStatement {
    pub table: Relation,
    pub where_clause: Option<Expr>,
}

impl DialectDisplay for DeleteStatement {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| {
            write!(f, "DELETE FROM {}", self.table.display(dialect))?;

            if let Some(ref where_clause) = self.where_clause {
                write!(f, " WHERE {}", where_clause.display(dialect))?;
            }

            Ok(())
        })
    }
}
