use std::fmt;

use readyset_util::fmt::fmt_with;
use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

use crate::{ast::*, Dialect, DialectDisplay};

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub enum CommentStatement {
    Column {
        column_name: SqlIdentifier,
        table_name: SqlIdentifier,
        comment: String,
    },
    Table {
        table_name: SqlIdentifier,
        comment: String,
    },
}

impl DialectDisplay for CommentStatement {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| match self {
            Self::Column {
                column_name,
                table_name,
                comment,
            } => {
                write!(
                    f,
                    "COMMENT ON COLUMN {}.{} IS ",
                    dialect.quote_identifier(&table_name),
                    dialect.quote_identifier(&column_name),
                )?;
                literal::display_string_literal(f, comment)
            }
            Self::Table {
                table_name,
                comment,
            } => {
                write!(
                    f,
                    "COMMENT ON TABLE {} IS ",
                    dialect.quote_identifier(&table_name),
                )?;
                literal::display_string_literal(f, comment)
            }
        })
    }
}
