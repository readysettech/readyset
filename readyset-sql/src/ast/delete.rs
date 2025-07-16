use std::fmt;

use readyset_util::fmt::fmt_with;
use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

use crate::{AstConversionError, Dialect, DialectDisplay, TryFromDialect, TryIntoDialect, ast::*};

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct DeleteStatement {
    pub table: Relation,
    pub where_clause: Option<Expr>,
}

impl TryFromDialect<sqlparser::ast::Delete> for DeleteStatement {
    fn try_from_dialect(
        value: sqlparser::ast::Delete,
        dialect: Dialect,
    ) -> Result<Self, AstConversionError> {
        Ok(Self {
            // TODO: Support multiple tables (in `value.tables`, or possibly in `FromTable`)
            table: value.from.try_into_dialect(dialect)?,
            where_clause: value.selection.try_into_dialect(dialect)?,
            // TODO: Support order_by and limit
        })
    }
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
