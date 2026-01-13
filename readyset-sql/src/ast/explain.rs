use std::{fmt, hash::Hash};

use readyset_util::fmt::fmt_with;
use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

use crate::{Dialect, DialectDisplay, ast::*};

/// EXPLAIN statements
///
/// This is a non-standard ReadySet-specific extension to SQL
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
pub enum ExplainStatement {
    /// Print a graphviz representation of the current query graph to stdout
    Graphviz {
        /// Print a *simplified* graphviz representation, smaller but with less information
        simplified: bool,
        /// Limit the graph to only a single cache
        for_cache: Option<Relation>,
    },
    /// Provides metadata about the last statement that was executed.
    LastStatement,
    /// List domain shard replicas and what worker they're running on
    Domains,
    /// List all CREATE CACHE statements that have been executed, for the
    /// purpose of exporting them
    Caches,
    /// List and give information about all materializations in the graph
    Materializations,
    /// For the given query, report whether it is supported by ReadySet, its rewritten form, and
    /// its ID
    CreateCache {
        /// The result of parsing the inner statement or query ID for the `EXPLAIN CREATE CACHE`
        /// statement.
        ///
        /// If parsing succeeded, then this will contain an Ok result with the definition of the
        /// statement.  If the statement failed to parse, this will contain an Err result with the
        /// remainder of the string that could not be parsed.
        inner: CacheInner,
        cache_type: Option<CacheType>,
    },
}

impl DialectDisplay for ExplainStatement {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| {
            write!(f, "EXPLAIN ")?;
            match self {
                ExplainStatement::Graphviz {
                    simplified,
                    for_cache,
                } => {
                    if *simplified {
                        write!(f, "SIMPLIFIED ")?;
                    }
                    write!(f, "GRAPHVIZ")?;
                    if let Some(cache) = for_cache {
                        write!(f, " FOR CACHE {}", cache.display(dialect))?;
                    }
                    write!(f, ";")
                }
                ExplainStatement::LastStatement => write!(f, "LAST STATEMENT;"),
                ExplainStatement::Domains => write!(f, "DOMAINS;"),
                ExplainStatement::Caches => write!(f, "CACHES;"),
                ExplainStatement::Materializations => write!(f, "MATERIALIZATIONS;"),
                ExplainStatement::CreateCache {
                    inner, cache_type, ..
                } => {
                    write!(f, "CREATE")?;
                    if let Some(cache_type) = cache_type {
                        write!(f, " {}", cache_type.display(dialect))?;
                    }
                    write!(f, " CACHE FROM ")?;
                    write!(f, "{}", inner.display(dialect))
                }
            }
        })
    }
}
