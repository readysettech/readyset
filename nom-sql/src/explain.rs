use std::fmt;

use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::combinator::{opt, value};
use nom::sequence::{terminated, tuple};
use nom_locate::LocatedSpan;
use readyset_util::fmt::fmt_with;
use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

use crate::common::statement_terminator;
use crate::table::relation;
use crate::whitespace::whitespace1;
use crate::{Dialect, DialectDisplay, NomSqlResult, Relation};

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
}

impl ExplainStatement {
    pub fn display(&self, dialect: Dialect) -> impl fmt::Display + Copy + '_ {
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
            }
        })
    }
}

fn explain_graphviz(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], ExplainStatement> {
    move |i| {
        let (i, simplified) = opt(terminated(tag_no_case("simplified"), whitespace1))(i)?;
        let (i, _) = tag_no_case("graphviz")(i)?;
        let (i, for_cache) = opt(move |i| {
            let (i, _) = whitespace1(i)?;
            let (i, _) = tag_no_case("for")(i)?;
            let (i, _) = whitespace1(i)?;
            let (i, _) = tag_no_case("cache")(i)?;
            let (i, _) = whitespace1(i)?;
            relation(dialect)(i)
        })(i)?;

        Ok((
            i,
            ExplainStatement::Graphviz {
                simplified: simplified.is_some(),
                for_cache,
            },
        ))
    }
}

pub(crate) fn explain_statement(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], ExplainStatement> {
    move |i| {
        let (i, _) = tag_no_case("explain")(i)?;
        let (i, _) = whitespace1(i)?;
        let (i, stmt) = alt((
            explain_graphviz(dialect),
            value(
                ExplainStatement::LastStatement,
                tuple((tag_no_case("last"), whitespace1, tag_no_case("statement"))),
            ),
            value(ExplainStatement::Domains, tag_no_case("domains")),
            value(ExplainStatement::Caches, tag_no_case("caches")),
            value(
                ExplainStatement::Materializations,
                tag_no_case("materializations"),
            ),
        ))(i)?;
        let (i, _) = statement_terminator(i)?;
        Ok((i, stmt))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explain_graphviz() {
        assert_eq!(
            explain_statement(Dialect::MySQL)(LocatedSpan::new(b"explain graphviz;"))
                .unwrap()
                .1,
            ExplainStatement::Graphviz {
                simplified: false,
                for_cache: None
            }
        );
    }

    #[test]
    fn explain_graphviz_for_cache() {
        assert_eq!(
            test_parse!(
                explain_statement(Dialect::MySQL),
                b"explain graphviz for cache q"
            ),
            ExplainStatement::Graphviz {
                simplified: false,
                for_cache: Some(Relation {
                    schema: None,
                    name: "q".into()
                })
            }
        );
    }

    #[test]
    fn explain_last_statement() {
        assert_eq!(
            explain_statement(Dialect::MySQL)(LocatedSpan::new(b"explain last statement;"))
                .unwrap()
                .1,
            ExplainStatement::LastStatement
        );
    }

    #[test]
    fn explain_domains() {
        assert_eq!(
            test_parse!(explain_statement(Dialect::MySQL), b"explain domains;"),
            ExplainStatement::Domains
        );
    }

    #[test]
    fn explain_caches() {
        assert_eq!(
            test_parse!(explain_statement(Dialect::MySQL), b"explain caches;"),
            ExplainStatement::Caches
        )
    }

    #[test]
    fn explain_materializations() {
        assert_eq!(
            test_parse!(
                explain_statement(Dialect::MySQL),
                b"explain   mAtERIaLIZAtIOns"
            ),
            ExplainStatement::Materializations
        );
    }
}
