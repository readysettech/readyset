use std::fmt;

use readyset_util::fmt::fmt_with;
use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

use crate::{ast::*, Dialect, DialectDisplay};

#[derive(Clone, Debug, Eq, Hash, PartialEq, Deserialize, Serialize, Arbitrary)]
pub enum CompoundSelectOperator {
    Union,
    DistinctUnion,
    Intersect,
    Except,
}

impl fmt::Display for CompoundSelectOperator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            CompoundSelectOperator::Union => write!(f, "UNION"),
            CompoundSelectOperator::DistinctUnion => write!(f, "UNION DISTINCT"),
            CompoundSelectOperator::Intersect => write!(f, "INTERSECT"),
            CompoundSelectOperator::Except => write!(f, "EXCEPT"),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Deserialize, Serialize, Arbitrary)]
pub struct CompoundSelectStatement {
    pub selects: Vec<(Option<CompoundSelectOperator>, SelectStatement)>,
    pub order: Option<OrderClause>,
    pub limit_clause: LimitClause,
}

impl DialectDisplay for CompoundSelectStatement {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| {
            for (op, sel) in &self.selects {
                if let Some(o) = op {
                    write!(f, " {}", o)?;
                }
                write!(f, " {}", sel.display(dialect))?;
            }

            if let Some(ord) = &self.order {
                write!(f, " {}", ord.display(dialect))?;
            }

            if self.limit_clause.is_empty() {
                write!(f, " {}", self.limit_clause.display(dialect))?;
            }

            Ok(())
        })
    }
}
