use std::fmt;

use itertools::Itertools;
use readyset_util::fmt::fmt_with;
use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

use crate::{
    ast::*, AstConversionError, Dialect, DialectDisplay, IntoDialect, TryFromDialect,
    TryIntoDialect,
};

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct InsertStatement {
    pub table: Relation,
    pub fields: Option<Vec<Column>>,
    pub data: Vec<Vec<Expr>>,
    pub ignore: bool,
    pub on_duplicate: Option<Vec<(Column, Expr)>>,
}

impl TryFromDialect<sqlparser::ast::Insert> for InsertStatement {
    fn try_from_dialect(
        value: sqlparser::ast::Insert,
        dialect: Dialect,
    ) -> Result<Self, AstConversionError> {
        if let sqlparser::ast::Insert {
            table: sqlparser::ast::TableObject::TableName(name),
            columns,
            source,
            ignore,
            ..
        } = value
        {
            Ok(Self {
                table: name.into_dialect(dialect),
                fields: Some(columns.into_dialect(dialect)),
                data: if let Some(query) = source {
                    match *query.body {
                        sqlparser::ast::SetExpr::Values(values) => {
                            values.rows.try_into_dialect(dialect)?
                        }
                        body => return unsupported!("Unsupported source type for INSERT: {body}"),
                    }
                } else {
                    Vec::new()
                },
                ignore,
                on_duplicate: match value.on {
                    Some(sqlparser::ast::OnInsert::DuplicateKeyUpdate(assignments)) => Some(
                        assignments
                            .into_iter()
                            .map(|assignment| assignment_into_expr(assignment, dialect))
                            .try_collect()?,
                    ),
                    _ => None, // TODO: We could support Postgres' ON CONFLICT too
                },
            })
        } else {
            failed!("Should only be called on an INSERT statement")
        }
    }
}

fn assignment_into_expr(
    assignment: sqlparser::ast::Assignment,
    dialect: Dialect,
) -> Result<(crate::ast::Column, crate::ast::Expr), AstConversionError> {
    Ok((
        match assignment.target {
            sqlparser::ast::AssignmentTarget::ColumnName(object_name) => {
                object_name.into_dialect(dialect)
            }
            sqlparser::ast::AssignmentTarget::Tuple(_vec) => {
                return unsupported!("Currently don't support tuple assignment: (a,b) = (1,2)");
            }
        },
        assignment.value.try_into_dialect(dialect)?,
    ))
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
