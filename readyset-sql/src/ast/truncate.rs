use std::fmt;

use itertools::Itertools;
use proptest::{
    prelude::{Just, Strategy as _, any, any_with},
    sample::size_range,
};
use readyset_util::fmt::fmt_with;
use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

use crate::{
    AstConversionError, Dialect, DialectDisplay, TryFromDialect, TryIntoDialect as _, ast::*,
};

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
#[arbitrary(args = Option<Dialect>)]
pub struct TruncateTable {
    pub relation: Relation,
    #[strategy(if args == &Some(Dialect::PostgreSQL) { any::<bool>().boxed() } else { Just(false).boxed() })]
    pub only: bool,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
#[arbitrary(args = Option<Dialect>)]
pub struct TruncateStatement {
    #[strategy(any_with::<Vec<TruncateTable>>((size_range(1..=(if args == &Some(Dialect::PostgreSQL) { 16 } else { 1 })), *args)))]
    pub tables: Vec<TruncateTable>,
    #[strategy(if args == &Some(Dialect::PostgreSQL) { any::<bool>().boxed() } else { Just(false).boxed() })]
    pub restart_identity: bool,
    #[strategy(if args == &Some(Dialect::PostgreSQL) { any::<bool>().boxed() } else { Just(false).boxed() })]
    pub cascade: bool,
}

impl TryFromDialect<sqlparser::ast::TruncateTableTarget> for TruncateTable {
    fn try_from_dialect(
        value: sqlparser::ast::TruncateTableTarget,
        dialect: Dialect,
    ) -> Result<Self, AstConversionError> {
        Ok(Self {
            relation: value.name.try_into_dialect(dialect)?,
            only: value.only,
        })
    }
}

impl TryFromDialect<sqlparser::ast::Statement> for TruncateStatement {
    fn try_from_dialect(
        value: sqlparser::ast::Statement,
        dialect: Dialect,
    ) -> Result<Self, AstConversionError> {
        if let sqlparser::ast::Statement::Truncate {
            table_names,
            partitions: _,
            table: _,
            identity,
            cascade,
            on_cluster: _,
        } = value
        {
            let tables = table_names
                .into_iter()
                .map(|tn| tn.try_into_dialect(dialect))
                .try_collect()?;
            Ok(Self {
                tables,
                restart_identity: identity == Some(sqlparser::ast::TruncateIdentityOption::Restart),
                cascade: cascade == Some(sqlparser::ast::CascadeOption::Cascade),
            })
        } else {
            failed!("Should only be called on TRUNCATE statement, got {value:?}")
        }
    }
}

impl DialectDisplay for TruncateStatement {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| {
            write!(f, "TRUNCATE ")?;

            write!(
                f,
                "{}",
                self.tables
                    .iter()
                    .map(|t| format!(
                        "{}{}",
                        if t.only { "ONLY " } else { "" },
                        t.relation.display(dialect)
                    ))
                    .join(", ")
            )?;

            if self.restart_identity {
                write!(f, " RESTART IDENTITY")?;
            }

            if self.cascade {
                write!(f, " CASCADE")?;
            }

            Ok(())
        })
    }
}
