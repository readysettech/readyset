use nom::types::CompleteByteSlice;
use std::fmt;
use std::str;

use column::Column;
use condition::ConditionExpression;
use select::{JoinClause, SelectStatement};
use table::Table;

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
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

impl fmt::Display for JoinRightSide {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            JoinRightSide::Table(ref t) => write!(f, "{}", t)?,
            JoinRightSide::NestedSelect(ref q, ref a) => {
                write!(f, "({})", q)?;
                if a.is_some() {
                    write!(f, " AS {}", a.as_ref().unwrap())?;
                }
            }
            JoinRightSide::NestedJoin(ref jc) => write!(f, "({})", jc)?,
            _ => unimplemented!(),
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum JoinOperator {
    Join,
    LeftJoin,
    LeftOuterJoin,
    InnerJoin,
    CrossJoin,
    StraightJoin,
}

impl fmt::Display for JoinOperator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            JoinOperator::Join => write!(f, "JOIN")?,
            JoinOperator::LeftJoin => write!(f, "LEFT JOIN")?,
            JoinOperator::LeftOuterJoin => write!(f, "LEFT OUTER JOIN")?,
            JoinOperator::InnerJoin => write!(f, "INNER JOIN")?,
            JoinOperator::CrossJoin => write!(f, "CROSS JOIN")?,
            JoinOperator::StraightJoin => write!(f, "STRAIGHT JOIN")?,
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum JoinConstraint {
    On(ConditionExpression),
    Using(Vec<Column>),
}

impl fmt::Display for JoinConstraint {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            JoinConstraint::On(ref ce) => write!(f, "ON {}", ce)?,
            JoinConstraint::Using(ref columns) => write!(
                f,
                "USING ({})",
                columns
                    .iter()
                    .map(|c| format!("{}", c))
                    .collect::<Vec<_>>()
                    .join(", ")
            )?,
        }
        Ok(())
    }
}

// Parse binary comparison operators
named!(pub join_operator<CompleteByteSlice, JoinOperator>,
        alt!(
              map!(tag_no_case!("join"), |_| JoinOperator::Join)
            | map!(tag_no_case!("left join"), |_| JoinOperator::LeftJoin)
            | map!(tag_no_case!("left outer join"), |_| JoinOperator::LeftOuterJoin)
            | map!(tag_no_case!("inner join"), |_| JoinOperator::InnerJoin)
            | map!(tag_no_case!("cross join"), |_| JoinOperator::CrossJoin)
            | map!(tag_no_case!("straight_join"), |_| JoinOperator::StraightJoin)
        )
);

#[cfg(test)]
mod tests {
    use super::*;
    use common::{FieldDefinitionExpression, Operator};
    use condition::ConditionBase::*;
    use condition::ConditionExpression::{self, *};
    use condition::ConditionTree;
    use select::{selection, JoinClause, SelectStatement};

    #[test]
    fn inner_join() {
        let qstring = "SELECT tags.* FROM tags \
                       INNER JOIN taggings ON tags.id = taggings.tag_id";

        let res = selection(CompleteByteSlice(qstring.as_bytes()));

        let ct = ConditionTree {
            left: Box::new(Base(Field(Column::from("tags.id")))),
            right: Box::new(Base(Field(Column::from("taggings.tag_id")))),
            operator: Operator::Equal,
        };
        let join_cond = ConditionExpression::ComparisonOp(ct);
        let expected_stmt = SelectStatement {
            tables: vec![Table::from("tags")],
            fields: vec![FieldDefinitionExpression::AllInTable("tags".into())],
            join: vec![JoinClause {
                operator: JoinOperator::InnerJoin,
                right: JoinRightSide::Table(Table::from("taggings")),
                constraint: JoinConstraint::On(join_cond),
            }],
            ..Default::default()
        };

        let q = res.unwrap().1;
        assert_eq!(q, expected_stmt);
        assert_eq!(qstring, format!("{}", q));
    }
}
