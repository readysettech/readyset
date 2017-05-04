use nom::{IResult, Err, ErrorKind, Needed};
use std::str;

use column::Column;
use condition::ConditionExpression;
use select::{JoinClause, SelectStatement};
use table::Table;

#[derive(Clone, Debug, Hash, PartialEq, Serialize, Deserialize)]
pub enum JoinRightSide {
    Table(Table),
    Tables(Vec<Table>),
    NestedSelect(Box<SelectStatement>),
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
