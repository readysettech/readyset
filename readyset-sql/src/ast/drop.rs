use std::fmt;

use itertools::Itertools;
use readyset_util::fmt::fmt_with;
use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

use crate::{Dialect, DialectDisplay, ast::*};

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct DropTableStatement {
    pub tables: Vec<Relation>,
    pub if_exists: bool,
}

impl DialectDisplay for DropTableStatement {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| {
            write!(f, "DROP TABLE ")?;

            if self.if_exists {
                write!(f, "IF EXISTS ")?;
            }

            write!(
                f,
                "{}",
                self.tables
                    .iter()
                    .map(|t| dialect.quote_identifier(&t.name))
                    .join(", ")
            )
        })
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct DropCacheStatement {
    pub name: Relation,
}

impl DialectDisplay for DropCacheStatement {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| write!(f, "DROP CACHE {}", self.name.display(dialect)))
    }
}

impl DropCacheStatement {
    pub fn display_unquoted(&self) -> impl fmt::Display + Copy + '_ {
        fmt_with(move |f| write!(f, "DROP CACHE {}", self.name.display_unquoted()))
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct DropViewStatement {
    pub views: Vec<Relation>,
    pub if_exists: bool,
}

impl DialectDisplay for DropViewStatement {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| {
            write!(f, "DROP VIEW ")?;
            if self.if_exists {
                write!(f, "IF EXISTS ")?;
            }
            write!(
                f,
                "{}",
                self.views
                    .iter()
                    .map(|view| view.display(dialect))
                    .join(", ")
            )
        })
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct DropAllCachesStatement {
    pub cache_type: Option<CacheType>,
}

impl fmt::Display for DropAllCachesStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.cache_type {
            Some(CacheType::Deep) => write!(f, "DROP ALL DEEP CACHES"),
            Some(CacheType::Shallow) => write!(f, "DROP ALL SHALLOW CACHES"),
            None => write!(f, "DROP ALL CACHES"),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct DropAllProxiedQueriesStatement;

impl DialectDisplay for DropAllProxiedQueriesStatement {
    fn display(&self, _dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| write!(f, "DROP ALL PROXIED QUERIES"))
    }
}
