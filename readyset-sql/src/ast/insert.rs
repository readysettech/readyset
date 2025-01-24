use std::fmt;

use itertools::Itertools;
use readyset_util::fmt::fmt_with;
use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

use crate::{ast::*, Dialect, DialectDisplay};

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct InsertStatement {
    pub table: Relation,
    pub fields: Option<Vec<Column>>,
    pub data: Vec<Vec<Expr>>,
    pub ignore: bool,
    pub on_duplicate: Option<Vec<(Column, Expr)>>,
}

impl DialectDisplay for InsertStatement {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| {
            write!(f, "INSERT INTO {}", self.table.display(dialect))?;

            if let Some(ref fields) = self.fields {
                write!(
                    f,
                    " ({})",
                    fields
                        .iter()
                        .map(|col| dialect.quote_identifier(&col.name))
                        .join(", ")
                )?;
            }

            write!(
                f,
                " VALUES {}",
                self.data
                    .iter()
                    .map(|data| format!("({})", data.iter().map(|l| l.display(dialect)).join(", ")))
                    .join(", ")
            )
        })
    }
}
