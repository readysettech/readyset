use std::fmt;

use itertools::Itertools;
use readyset_util::fmt::fmt_with;
use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

use crate::{ast::*, Dialect, DialectDisplay};

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct UpdateStatement {
    pub table: Relation,
    pub fields: Vec<(Column, Expr)>,
    pub where_clause: Option<Expr>,
}

impl DialectDisplay for UpdateStatement {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| {
            write!(f, "UPDATE {} ", self.table.display(dialect))?;

            // TODO: Consider using `Vec1`.
            assert!(!self.fields.is_empty());
            write!(
                f,
                "SET {}",
                self.fields
                    .iter()
                    .map(|(col, literal)| format!(
                        "{} = {}",
                        col.display(dialect),
                        literal.display(dialect)
                    ))
                    .join(", ")
            )?;

            if let Some(ref where_clause) = self.where_clause {
                write!(f, " WHERE ")?;
                write!(f, "{}", where_clause.display(dialect))?;
            }

            Ok(())
        })
    }
}
