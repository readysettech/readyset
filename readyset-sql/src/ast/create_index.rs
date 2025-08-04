use std::fmt;

use readyset_util::fmt::fmt_with;
use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

use crate::{
    AstConversionError, Dialect, DialectDisplay, IntoDialect, TryFromDialect, TryIntoDialect,
    ast::*, dialect_display::CommaSeparatedList,
};

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct CreateIndexStatement {
    /// Index name
    pub name: Option<SqlIdentifier>,
    /// Table name
    pub table_name: Relation,
    /// Index type (BTREE, HASH, etc.)
    pub using: Option<IndexType>,
    /// Columns to index
    pub columns: Vec<Column>,
    /// Whether this is a UNIQUE index
    pub unique: bool,
    /// Whether this is a FULLTEXT index
    pub fulltext: bool,
    /// Whether this is a SPATIAL index
    pub spatial: bool,
    /// IF NOT EXISTS clause
    pub if_not_exists: bool,
    /// WITH clause options
    pub with_options: Vec<SqlOption>,
    /// WHERE clause predicate
    pub predicate: Option<Expr>,
}

impl DialectDisplay for CreateIndexStatement {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| {
            write!(f, "CREATE ")?;

            if self.unique {
                write!(f, "UNIQUE ")?;
            } else if self.fulltext {
                write!(f, "FULLTEXT ")?;
            } else if self.spatial {
                write!(f, "SPATIAL ")?;
            }

            write!(f, "INDEX ")?;

            if self.if_not_exists {
                write!(f, "IF NOT EXISTS ")?;
            }

            if let Some(name) = &self.name {
                write!(f, "{name} ")?;
            }

            write!(f, "ON {} ", self.table_name.display(dialect))?;

            if let Some(using) = &self.using {
                write!(f, "USING {using} ")?;
            }

            write!(f, "(")?;
            for (i, column) in self.columns.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{}", column.display(dialect))?;
            }
            write!(f, ")")?;

            if !self.with_options.is_empty() {
                write!(
                    f,
                    " WITH ({})",
                    CommaSeparatedList::from(&self.with_options).display(dialect)
                )?;
            }

            if let Some(predicate) = &self.predicate {
                write!(f, " WHERE {}", predicate.display(dialect))?;
            }

            Ok(())
        })
    }
}

impl TryFromDialect<sqlparser::ast::CreateIndex> for CreateIndexStatement {
    fn try_from_dialect(
        value: sqlparser::ast::CreateIndex,
        dialect: Dialect,
    ) -> Result<Self, AstConversionError> {
        let sqlparser::ast::CreateIndex {
            name,
            table_name,
            using,
            columns,
            unique,
            concurrently: _, // Ignore concurrently for now
            if_not_exists,
            include: _,        // Ignore include for now
            nulls_distinct: _, // Ignore nulls_distinct for now
            with,
            predicate,
            index_options,
            alter_options: _,
        } = value;

        // Convert index columns to our Column type
        let columns = columns
            .into_iter()
            .map(|col| col.try_into_dialect(dialect))
            .collect::<Result<Vec<_>, _>>()?;

        // Convert WITH options
        let with_options = with
            .into_iter()
            .map(|expr| match expr {
                sqlparser::ast::Expr::Identifier(ident) => Ok(SqlOption {
                    name: ident.into_dialect(dialect),
                    value: None,
                }),
                sqlparser::ast::Expr::Function(func) => {
                    let is_empty = match &func.args {
                        sqlparser::ast::FunctionArguments::None => true,
                        sqlparser::ast::FunctionArguments::Subquery(_) => false,
                        sqlparser::ast::FunctionArguments::List(args) => args.args.is_empty(),
                    };
                    if is_empty {
                        Ok(SqlOption {
                            name: func.name.to_string().into(),
                            value: None,
                        })
                    } else {
                        not_yet_implemented!("WITH options with values: {func:?}")
                    }
                }
                _ => not_yet_implemented!("unsupported WITH option: {expr:?}"),
            })
            .collect::<Result<Vec<_>, _>>()?;

        // See note about `USING` in [`TableKey::Index`] conversion code.
        let using = index_options
            .into_iter()
            .find_map(|opt| match opt {
                sqlparser::ast::IndexOption::Using(using) => Some(using),
                _ => None,
            })
            .or(using);

        Ok(CreateIndexStatement {
            name: name
                .map(|n| {
                    // For multi-part names, just take the last part for now
                    n.0.into_iter().last().unwrap().try_into_dialect(dialect)
                })
                .transpose()?,
            table_name: table_name.try_into_dialect(dialect)?,
            using: using.map(|u| u.try_into_dialect(dialect)).transpose()?,
            columns,
            unique,
            fulltext: false, // Not yet supported by sqlparser
            spatial: false,  // Not yet supported by sqlparser
            if_not_exists,
            with_options,
            predicate: predicate.map(|p| p.try_into_dialect(dialect)).transpose()?,
        })
    }
}

impl TryFromDialect<sqlparser::ast::IndexType> for IndexType {
    fn try_from_dialect(
        value: sqlparser::ast::IndexType,
        _dialect: Dialect,
    ) -> Result<Self, AstConversionError> {
        use sqlparser::ast::IndexType as SqlIndexType;
        match value {
            SqlIndexType::BTree => Ok(IndexType::BTree),
            SqlIndexType::Hash => Ok(IndexType::Hash),
            SqlIndexType::GIN => Ok(IndexType::GIN),
            SqlIndexType::GiST => Ok(IndexType::GiST),
            SqlIndexType::SPGiST => Ok(IndexType::SPGiST),
            SqlIndexType::BRIN => Ok(IndexType::BRIN),
            SqlIndexType::Bloom => Ok(IndexType::Bloom),
            SqlIndexType::Custom(name) => Ok(IndexType::Custom(name.into_dialect(_dialect))),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct SqlOption {
    pub name: SqlIdentifier,
    pub value: Option<String>,
}

impl DialectDisplay for SqlOption {
    fn display(&self, _dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| {
            if let Some(value) = &self.value {
                write!(f, "{} = {}", self.name, value)
            } else {
                write!(f, "{}", self.name)
            }
        })
    }
}
