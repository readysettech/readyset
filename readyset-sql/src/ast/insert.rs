use std::fmt;

use itertools::Itertools;
use readyset_util::fmt::fmt_with;
use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

use crate::{ast::*, AstConversionError, Dialect, DialectDisplay};

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct InsertStatement {
    pub table: Relation,
    pub fields: Option<Vec<Column>>,
    pub data: Vec<Vec<Expr>>,
    pub ignore: bool,
    pub on_duplicate: Option<Vec<(Column, Expr)>>,
}

impl TryFrom<sqlparser::ast::Insert> for InsertStatement {
    type Error = AstConversionError;

    fn try_from(value: sqlparser::ast::Insert) -> Result<Self, Self::Error> {
        if let sqlparser::ast::Insert {
            table: sqlparser::ast::TableObject::TableName(name),
            columns,
            source,
            ignore,
            ..
        } = value
        {
            Ok(Self {
                table: name.into(),
                fields: if columns.is_empty() {
                    None
                } else {
                    Some(columns.into_iter().map(Into::into).collect())
                },
                data: if let Some(query) = source {
                    match *query.body {
                        sqlparser::ast::SetExpr::Values(values) => values
                            .rows
                            .into_iter()
                            .map(|row| row.into_iter().map(TryInto::try_into).try_collect())
                            .try_collect()?,
                        _ => unimplemented!(), // Our AST currently doesn't support anything else
                    }
                } else {
                    Vec::new()
                },
                ignore,
                on_duplicate: match value.on {
                    Some(sqlparser::ast::OnInsert::DuplicateKeyUpdate(assignments)) => Some(
                        assignments
                            .into_iter()
                            .map(assignment_into_expr)
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
) -> Result<(crate::ast::Column, crate::ast::Expr), AstConversionError> {
    Ok((
        match assignment.target {
            sqlparser::ast::AssignmentTarget::ColumnName(object_name) => object_name.into(),
            sqlparser::ast::AssignmentTarget::Tuple(_vec) => {
                return unsupported!("Currently don't support tuple assignment: (a,b) = (1,2)");
            }
        },
        assignment.value.try_into()?,
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
