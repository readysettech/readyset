use nom::bytes::complete::tag_no_case;
use nom::combinator::opt;
use nom::sequence::terminated;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

use nom::character::complete::multispace1;
use nom::IResult;

use crate::common::statement_terminator;

/// EXPLAIN statements
///
/// This is a non-standard ReadySet-specific extension to SQL
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum ExplainStatement {
    /// Print a (maybe simplified) graphviz representation of the current query graph to stdout
    Graphviz { simplified: bool },
}

impl Display for ExplainStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "EXPLAIN ")?;
        match self {
            ExplainStatement::Graphviz { simplified } => {
                if *simplified {
                    write!(f, "SIMPLIFIED ")?;
                }
                write!(f, "GRAPHVIZ;")
            }
        }
    }
}

fn explain_graphviz(i: &[u8]) -> IResult<&[u8], ExplainStatement> {
    let (i, simplified) = opt(terminated(tag_no_case("simplified"), multispace1))(i)?;
    let (i, _) = tag_no_case("graphviz")(i)?;
    Ok((
        i,
        ExplainStatement::Graphviz {
            simplified: simplified.is_some(),
        },
    ))
}

pub(crate) fn explain_statement(i: &[u8]) -> IResult<&[u8], ExplainStatement> {
    let (i, _) = tag_no_case("explain")(i)?;
    let (i, _) = multispace1(i)?;
    let (i, stmt) = /* alt(... */ explain_graphviz(i)?;
    let (i, _) = statement_terminator(i)?;
    Ok((i, stmt))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explain_graphviz() {
        assert_eq!(
            explain_statement(b"explain graphviz;").unwrap().1,
            ExplainStatement::Graphviz { simplified: false }
        );
    }
}
