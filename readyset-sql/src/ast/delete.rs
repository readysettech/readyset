use std::fmt;

use readyset_util::fmt::fmt_with;
use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

use crate::{ast::*, AstConversionError, Dialect, DialectDisplay};

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct DeleteStatement {
    pub table: Relation,
    pub where_clause: Option<Expr>,
}

impl TryFrom<sqlparser::ast::Delete> for DeleteStatement {
    type Error = AstConversionError;

    fn try_from(value: sqlparser::ast::Delete) -> Result<Self, Self::Error> {
        Ok(Self {
            // TODO: Support multiple tables (in `value.tables`, or possibly in `FromTable`)
            table: value.from.into(),
            where_clause: value.selection.map(TryInto::try_into).transpose()?,
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
