use std::{fmt, iter, str};

use itertools::{Either, Itertools};
use readyset_util::fmt::fmt_with;
use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

use crate::{
    ast::*, AstConversionError, Dialect, DialectDisplay, IntoDialect, TryFromDialect,
    TryIntoDialect,
};

#[derive(Clone, Debug, Eq, Hash, PartialEq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub enum JoinRightSide {
    /// A single table expression.
    Table(TableExpr),
    /// A comma-separated (and implicitly joined) sequence of tables.
    #[strategy(size_range(1..12))]
    Tables(Vec<TableExpr>),
}

impl JoinRightSide {
    /// Returns an iterator over the [`TableExpr`]s mentioned in this JoinRightSide
    pub fn table_exprs(&self) -> impl Iterator<Item = &TableExpr> + '_ {
        match self {
            JoinRightSide::Table(t) => Either::Left(iter::once(t)),
            JoinRightSide::Tables(ts) => Either::Right(ts.iter()),
        }
    }
}

impl DialectDisplay for JoinRightSide {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| match self {
            Self::Table(t) => write!(f, "{}", t.display(dialect)),
            Self::Tables(ts) => write!(f, "({})", ts.iter().map(|t| t.display(dialect)).join(", ")),
        })
    }
}

#[derive(
    Clone, Copy, Debug, Eq, Hash, PartialEq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary,
)]
pub enum JoinOperator {
    Join,
    LeftJoin,
    #[weight(0)]
    LeftOuterJoin,
    #[weight(0)]
    RightJoin,
    #[weight(0)]
    RightOuterJoin,
    InnerJoin,
    #[weight(0)]
    CrossJoin,
    #[weight(0)]
    StraightJoin,
}

impl TryFrom<&sqlparser::ast::JoinOperator> for JoinOperator {
    type Error = AstConversionError;
    fn try_from(value: &sqlparser::ast::JoinOperator) -> Result<Self, Self::Error> {
        use sqlparser::ast::JoinOperator as JoinOp;
        match value {
            JoinOp::Join(..) => Ok(Self::Join),
            JoinOp::Inner(..) => Ok(Self::InnerJoin),
            JoinOp::Left(..) => Ok(Self::LeftJoin),
            JoinOp::LeftOuter(..) => Ok(Self::LeftOuterJoin),
            JoinOp::Right(..) => Ok(Self::RightJoin),
            JoinOp::RightOuter(..) => Ok(Self::RightOuterJoin),
            JoinOp::StraightJoin(..) => Ok(Self::StraightJoin),
            JoinOp::CrossJoin => Ok(Self::CrossJoin),
            JoinOp::FullOuter(..)
            | JoinOp::Semi(..)
            | JoinOp::LeftSemi(..)
            | JoinOp::RightSemi(..)
            | JoinOp::Anti(..)
            | JoinOp::LeftAnti(..)
            | JoinOp::RightAnti(..)
            | JoinOp::CrossApply
            | JoinOp::OuterApply
            | JoinOp::AsOf { .. } => unsupported!("Unsupported join operator: {value:?}"),
        }
    }
}

impl JoinOperator {
    pub fn is_inner_join(&self) -> bool {
        matches!(self, JoinOperator::Join | JoinOperator::InnerJoin)
    }
}

impl fmt::Display for JoinOperator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            JoinOperator::Join => write!(f, "JOIN")?,
            JoinOperator::LeftJoin => write!(f, "LEFT JOIN")?,
            JoinOperator::LeftOuterJoin => write!(f, "LEFT OUTER JOIN")?,
            JoinOperator::RightJoin => write!(f, "RIGHT JOIN")?,
            JoinOperator::RightOuterJoin => write!(f, "RIGHT OUTER JOIN")?,
            JoinOperator::InnerJoin => write!(f, "INNER JOIN")?,
            JoinOperator::CrossJoin => write!(f, "CROSS JOIN")?,
            JoinOperator::StraightJoin => write!(f, "STRAIGHT JOIN")?,
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub enum JoinConstraint {
    On(Expr),
    Using(Vec<Column>),
    Empty,
}

impl TryFromDialect<sqlparser::ast::JoinOperator> for JoinConstraint {
    fn try_from_dialect(
        value: sqlparser::ast::JoinOperator,
        dialect: Dialect,
    ) -> Result<Self, AstConversionError> {
        use sqlparser::ast::JoinOperator as JoinOp;
        match value {
            JoinOp::Join(constraint)
            | JoinOp::Inner(constraint)
            | JoinOp::Left(constraint)
            | JoinOp::LeftOuter(constraint)
            | JoinOp::Right(constraint)
            | JoinOp::RightOuter(constraint)
            | JoinOp::FullOuter(constraint)
            | JoinOp::LeftSemi(constraint)
            | JoinOp::RightSemi(constraint)
            | JoinOp::LeftAnti(constraint)
            | JoinOp::RightAnti(constraint)
            | JoinOp::Semi(constraint)
            | JoinOp::Anti(constraint)
            | JoinOp::StraightJoin(constraint)
            | JoinOp::AsOf { constraint, .. } => constraint.try_into_dialect(dialect),
            JoinOp::CrossJoin | JoinOp::CrossApply | JoinOp::OuterApply => Ok(Self::Empty),
        }
    }
}

impl TryFromDialect<sqlparser::ast::JoinConstraint> for JoinConstraint {
    fn try_from_dialect(
        value: sqlparser::ast::JoinConstraint,
        dialect: Dialect,
    ) -> Result<Self, AstConversionError> {
        use sqlparser::ast::JoinConstraint::*;
        match value {
            On(expr) => Ok(Self::On(expr.try_into_dialect(dialect)?)),
            Using(idents) => Ok(Self::Using(idents.into_dialect(dialect))),
            None => Ok(Self::Empty),
            Natural => unsupported!("NATURAL join"),
        }
    }
}

impl DialectDisplay for JoinConstraint {
    fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| match self {
            Self::On(ce) => write!(f, "ON {}", ce.display(dialect)),
            Self::Using(columns) => write!(
                f,
                "USING ({})",
                columns.iter().map(|c| c.display(dialect)).join(", ")
            ),
            Self::Empty => Ok(()),
        })
    }
}
