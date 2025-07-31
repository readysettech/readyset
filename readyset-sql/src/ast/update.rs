use std::fmt;

use itertools::Itertools;
use proptest::sample::size_range;
use readyset_util::fmt::fmt_with;
use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

use crate::{AstConversionError, Dialect, DialectDisplay, TryFromDialect, TryIntoDialect, ast::*};

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct UpdateStatement {
    pub table: Relation,
    #[any(size_range(1..16).lift())]
    pub fields: Vec<(Column, Expr)>,
    pub where_clause: Option<Expr>,
}

impl TryFromDialect<sqlparser::ast::Statement> for UpdateStatement {
    fn try_from_dialect(
        value: sqlparser::ast::Statement,
        dialect: Dialect,
    ) -> Result<Self, AstConversionError> {
        match value {
            sqlparser::ast::Statement::Update {
                table,
                assignments,
                from: _,
                selection,
                returning: _,
                or: _,
                limit: _,
            } => {
                let table = table.try_into_dialect(dialect)?;
                let fields: Vec<(Column, Expr)> = assignments.try_into_dialect(dialect)?;
                let where_clause = selection.try_into_dialect(dialect)?;
                Ok(Self {
                    table,
                    fields,
                    where_clause,
                })
            }
            _ => failed!("Should only be called with an update statement: {value:?}"),
        }
    }
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
