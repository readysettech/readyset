use nom::{Err, ErrorKind, IResult, Needed};
use std::str;

use column::Column;
use condition::ConditionExpression;
use select::{JoinClause, SelectStatement};
use table::Table;

#[derive(Clone, Debug, Hash, PartialEq, Serialize, Deserialize)]
pub enum JoinRightSide {
    /// A single table.
    Table(Table),
    /// A comma-separated (and implicitly joined) sequence of tables.
    Tables(Vec<Table>),
    /// A nested selection, represented as (query, alias).
    NestedSelect(Box<SelectStatement>, Option<String>),
    /// A nested join clause.
    NestedJoin(Box<JoinClause>),
}

#[derive(Clone, Debug, Hash, PartialEq, Serialize, Deserialize)]
pub enum JoinOperator {
    Join,
    LeftJoin,
    LeftOuterJoin,
    InnerJoin,
    CrossJoin,
    StraightJoin,
}

#[derive(Clone, Debug, Hash, PartialEq, Serialize, Deserialize)]
pub enum JoinConstraint {
    On(ConditionExpression),
    Using(Vec<Column>),
}

/// Parse binary comparison operators
named!(pub join_operator<&[u8], JoinOperator>,
        alt_complete!(
              map!(caseless_tag!("join"), |_| JoinOperator::Join)
            | map!(caseless_tag!("left join"), |_| JoinOperator::LeftJoin)
            | map!(caseless_tag!("left outer join"), |_| JoinOperator::LeftOuterJoin)
            | map!(caseless_tag!("inner join"), |_| JoinOperator::InnerJoin)
            | map!(caseless_tag!("cross join"), |_| JoinOperator::CrossJoin)
            | map!(caseless_tag!("straight_join"), |_| JoinOperator::StraightJoin)
        )
);
