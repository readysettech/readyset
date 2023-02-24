use std::{fmt, iter, str};

use itertools::{Either, Itertools};
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::combinator::map;
use nom_locate::LocatedSpan;
use readyset_util::fmt::fmt_with;
use serde::{Deserialize, Serialize};
use test_strategy::Arbitrary;

use crate::column::Column;
use crate::{Dialect, Expr, NomSqlResult, TableExpr};

#[derive(Clone, Debug, Eq, Hash, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum JoinRightSide {
    /// A single table expression.
    Table(TableExpr),
    /// A comma-separated (and implicitly joined) sequence of tables.
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

impl JoinRightSide {
    pub fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
        fmt_with(move |f| match self {
            Self::Table(t) => write!(f, "{}", t.display(dialect)),
            Self::Tables(ts) => write!(f, "({})", ts.iter().map(|t| t.display(dialect)).join(", ")),
        })
    }
}

#[derive(
    Clone, Copy, Debug, Eq, Hash, PartialEq, PartialOrd, Serialize, Deserialize, Arbitrary,
)]
pub enum JoinOperator {
    Join,
    LeftJoin,
    #[weight(0)]
    LeftOuterJoin,
    #[weight(0)]
    RightJoin,
    InnerJoin,
    #[weight(0)]
    CrossJoin,
    #[weight(0)]
    StraightJoin,
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
            JoinOperator::InnerJoin => write!(f, "INNER JOIN")?,
            JoinOperator::CrossJoin => write!(f, "CROSS JOIN")?,
            JoinOperator::StraightJoin => write!(f, "STRAIGHT JOIN")?,
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum JoinConstraint {
    On(Expr),
    Using(Vec<Column>),
    Empty,
}

impl JoinConstraint {
    pub fn display(&self, dialect: Dialect) -> impl fmt::Display + '_ {
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

// Parse binary comparison operators
pub fn join_operator(i: LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], JoinOperator> {
    alt((
        map(tag_no_case("join"), |_| JoinOperator::Join),
        map(tag_no_case("left join"), |_| JoinOperator::LeftJoin),
        map(tag_no_case("left outer join"), |_| {
            JoinOperator::LeftOuterJoin
        }),
        map(tag_no_case("right join"), |_| JoinOperator::RightJoin),
        map(tag_no_case("inner join"), |_| JoinOperator::InnerJoin),
        map(tag_no_case("cross join"), |_| JoinOperator::CrossJoin),
        map(tag_no_case("straight_join"), |_| JoinOperator::StraightJoin),
    ))(i)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::FieldDefinitionExpr;
    use crate::select::{selection, JoinClause, SelectStatement};
    use crate::{BinaryOperator, Dialect, Relation};

    #[test]
    fn inner_join() {
        let qstring = "SELECT tags.* FROM tags \
                       INNER JOIN taggings ON (tags.id = taggings.tag_id)";
        let expected = "SELECT `tags`.* FROM `tags` \
                       INNER JOIN `taggings` ON (`tags`.`id` = `taggings`.`tag_id`)";

        let res = selection(Dialect::MySQL)(LocatedSpan::new(qstring.as_bytes()));

        let join_cond = Expr::BinaryOp {
            lhs: Box::new(Expr::Column(Column::from("tags.id"))),
            op: BinaryOperator::Equal,
            rhs: Box::new(Expr::Column(Column::from("taggings.tag_id"))),
        };
        let expected_stmt = SelectStatement {
            tables: vec![TableExpr::from(Relation::from("tags"))],
            fields: vec![FieldDefinitionExpr::AllInTable("tags".into())],
            join: vec![JoinClause {
                operator: JoinOperator::InnerJoin,
                right: JoinRightSide::Table(TableExpr::from(Relation::from("taggings"))),
                constraint: JoinConstraint::On(join_cond),
            }],
            ..Default::default()
        };

        let q = res.unwrap().1;
        assert_eq!(q, expected_stmt);
        assert_eq!(expected, q.display(Dialect::MySQL).to_string());
    }
}
