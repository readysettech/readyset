use std::fmt;

use itertools::Itertools;
use readyset_util::fmt::fmt_with;
use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

use crate::{ast::*, AstConversionError, Dialect, DialectDisplay};

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct UpdateStatement {
    pub table: Relation,
    pub fields: Vec<(Column, Expr)>,
    pub where_clause: Option<Expr>,
}

impl TryFrom<sqlparser::ast::Statement> for UpdateStatement {
    type Error = AstConversionError;

    fn try_from(value: sqlparser::ast::Statement) -> Result<Self, Self::Error> {
        match value {
            sqlparser::ast::Statement::Update {
                table,
                assignments,
                from: _,
                selection,
                returning: _,
                or: _,
            } => {
                let table = table.into();
                let fields = assignments
                    .into_iter()
                    .map(|sqlparser::ast::Assignment { target, value }| {
                        Ok((target.into(), value.try_into()?))
                    })
                    .try_collect()?;
                let where_clause = selection.map(TryInto::try_into).transpose()?;
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
