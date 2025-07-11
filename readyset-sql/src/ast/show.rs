use std::fmt;

use proptest::option;
use readyset_util::fmt::fmt_with;
use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

use crate::{ast::*, AstConversionError, Dialect, DialectDisplay, TryFromDialect, TryIntoDialect};

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Arbitrary)]
pub struct ReadySetTablesOptions {
    pub all: bool,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub enum ShowStatement {
    Events,
    Tables(Tables),
    CachedQueries(Option<String>),
    ProxiedQueries(ProxiedQueriesOptions),
    ReadySetStatus,
    ReadySetStatusAdapter,
    ReadySetMigrationStatus(u64),
    ReadySetVersion,
    ReadySetTables(ReadySetTablesOptions),
    Connections,
    Databases,
    Rls(Option<Relation>),
}

impl DialectDisplay for ShowStatement {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| {
            write!(f, "SHOW ")?;
            match self {
                Self::Events => write!(f, "EVENTS"),
                Self::Tables(tables) => write!(f, "{}", tables.display(dialect)),
                Self::CachedQueries(maybe_query_id) => {
                    if let Some(query_id) = maybe_query_id {
                        write!(f, "CACHES WHERE query_id = {query_id}")
                    } else {
                        write!(f, "CACHES")
                    }
                }
                Self::ProxiedQueries(options) => {
                    write!(f, "PROXIED ")?;
                    if options.only_supported {
                        write!(f, "SUPPORTED ")?;
                    }
                    write!(f, "QUERIES")?;
                    if let Some(query_id) = &options.query_id {
                        write!(f, " WHERE query_id = '{query_id}'")?;
                    }
                    if let Some(limit) = &options.limit {
                        write!(f, " LIMIT {limit}")?;
                    }
                    Ok(())
                }
                Self::ReadySetStatus => write!(f, "READYSET STATUS"),
                Self::ReadySetStatusAdapter => write!(f, "READYSET STATUS ADAPTER"),
                Self::ReadySetMigrationStatus(id) => write!(f, "READYSET MIGRATION STATUS {id}"),
                Self::ReadySetVersion => write!(f, "READYSET VERSION"),
                Self::ReadySetTables(options) => {
                    if options.all {
                        write!(f, "READYSET ALL TABLES")
                    } else {
                        write!(f, "READYSET TABLES")
                    }
                }
                Self::Connections => write!(f, "CONNECTIONS"),
                Self::Databases => write!(f, "DATABASES"),
                Self::Rls(maybe_table) => {
                    if let Some(table) = maybe_table {
                        write!(f, "RLS ON = {}", table.display(dialect))
                    } else {
                        write!(f, "ALL RLS")
                    }
                }
            }
        })
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct ProxiedQueriesOptions {
    #[strategy(option::of("q_{a-z}{16}"))]
    pub query_id: Option<String>,
    pub only_supported: bool,
    pub limit: Option<u64>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct Tables {
    pub full: bool,
    pub from_db: Option<String>,
    pub filter: Option<FilterPredicate>,
}

impl DialectDisplay for Tables {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| {
            if self.full {
                write!(f, "FULL ")?;
            }
            write!(f, "TABLES")?;
            if let Some(from_db) = self.from_db.as_ref() {
                write!(f, " FROM {from_db}")?;
            }
            if let Some(filter) = self.filter.as_ref() {
                write!(f, " {}", filter.display(dialect))?;
            }
            Ok(())
        })
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub enum FilterPredicate {
    Like(String),
    Where(Expr),
}

impl TryFromDialect<sqlparser::ast::ShowStatementFilter> for FilterPredicate {
    fn try_from_dialect(
        value: sqlparser::ast::ShowStatementFilter,
        dialect: Dialect,
    ) -> Result<Self, AstConversionError> {
        use sqlparser::ast::ShowStatementFilter::*;
        match value {
            Like(like) => Ok(Self::Like(like)),
            Where(expr) => Ok(Self::Where(expr.try_into_dialect(dialect)?)),
            _ => unsupported!(
                "show statement filter {value:?} is not supported for MySQL or Postgres"
            ),
        }
    }
}

impl DialectDisplay for FilterPredicate {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| match self {
            Self::Like(like) => write!(f, "LIKE '{like}'"),
            Self::Where(expr) => write!(f, "WHERE {}", expr.display(dialect)),
        })
    }
}
