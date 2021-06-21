use std::fmt;
use std::str;
use test_strategy::Arbitrary;

use crate::column::Column;
use crate::select::{JoinClause, SelectStatement};
use crate::table::Table;
use crate::Expression;
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::combinator::map;
use nom::IResult;

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

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Arbitrary)]
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

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum JoinConstraint {
    On(Expression),
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
pub fn join_operator(i: &[u8]) -> IResult<&[u8], JoinOperator> {
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
    use crate::common::FieldDefinitionExpression;
    use crate::select::{selection, JoinClause, SelectStatement};
    use crate::BinaryOperator;

    #[test]
    fn inner_join() {
        let qstring = "SELECT tags.* FROM tags \
                       INNER JOIN taggings ON (tags.id = taggings.tag_id)";

        let res = selection(qstring.as_bytes());

        let join_cond = Expression::BinaryOp {
            lhs: Box::new(Expression::Column(Column::from("tags.id"))),
            op: BinaryOperator::Equal,
            rhs: Box::new(Expression::Column(Column::from("taggings.tag_id"))),
        };
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
