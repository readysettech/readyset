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
              map!(tag_no_case!("join"), |_| JoinOperator::Join)
            | map!(tag_no_case!("left join"), |_| JoinOperator::LeftJoin)
            | map!(tag_no_case!("left outer join"), |_| JoinOperator::LeftOuterJoin)
            | map!(tag_no_case!("inner join"), |_| JoinOperator::InnerJoin)
            | map!(tag_no_case!("cross join"), |_| JoinOperator::CrossJoin)
            | map!(tag_no_case!("straight_join"), |_| JoinOperator::StraightJoin)
        )
);
