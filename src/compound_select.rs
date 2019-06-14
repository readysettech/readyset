use nom::multispace;
use nom::types::CompleteByteSlice;
use std::fmt;
use std::str;

use common::{opt_multispace, statement_terminator};
use order::{order_clause, OrderClause};
use select::{limit_clause, nested_selection, LimitClause, SelectStatement};

#[derive(Clone, Debug, Eq, Hash, PartialEq, Deserialize, Serialize)]
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

#[derive(Clone, Debug, Eq, Hash, PartialEq, Deserialize, Serialize)]
pub struct CompoundSelectStatement {
    pub selects: Vec<(Option<CompoundSelectOperator>, SelectStatement)>,
    pub order: Option<OrderClause>,
    pub limit: Option<LimitClause>,
}

impl fmt::Display for CompoundSelectStatement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for (ref op, ref sel) in &self.selects {
            if op.is_some() {
                write!(f, " {}", op.as_ref().unwrap())?;
            }
            write!(f, " {}", sel)?;
        }
        if self.order.is_some() {
            write!(f, " {}", self.order.as_ref().unwrap())?;
        }
        if self.limit.is_some() {
            write!(f, " {}", self.order.as_ref().unwrap())?;
        }
        Ok(())
    }
}

// Parse compound operator
named!(compound_op<CompleteByteSlice, CompoundSelectOperator>,
    alt!(
          do_parse!(
              tag_no_case!("union") >>
              distinct: opt!(
                  preceded!(multispace,
                            alt!(  map!(tag_no_case!("all"), |_| { false })
                                          | map!(tag_no_case!("distinct"), |_| { true }))
                            )) >>
              (match distinct {
                  // DISTINCT is the default in both MySQL and SQLite
                  None => CompoundSelectOperator::DistinctUnion,
                  Some(d) => {
                      if d {
                          CompoundSelectOperator::DistinctUnion
                      } else {
                          CompoundSelectOperator::Union
                      }
                  },
              })
          )
        | map!(tag_no_case!("intersect"), |_| CompoundSelectOperator::Intersect)
        | map!(tag_no_case!("except"), |_| CompoundSelectOperator::Except)
    )
);

// Parse compound selection
named!(pub compound_selection<CompleteByteSlice, CompoundSelectStatement>,
    do_parse!(
        first_select: delimited!(opt!(tag!("(")), nested_selection, opt!(tag!(")"))) >>
        other_selects: many1!(
            do_parse!(opt_multispace >>
                    op: compound_op >>
                    multispace >>
                    opt!(tag!("(")) >>
                    opt_multispace >>
                    select: nested_selection >>
                    opt_multispace >>
                    opt!(tag!(")")) >>
                    (Some(op), select)
            )
        ) >>
        opt_multispace >>
        order: opt!(order_clause) >>
        limit: opt!(limit_clause) >>
        statement_terminator >>
        ({
            let mut v = vec![(None, first_select)];
            v.extend(other_selects);

            CompoundSelectStatement {
                selects: v,
                order: order,
                limit: limit,
            }
        })
    )
);

#[cfg(test)]
mod tests {
    use super::*;
    use column::Column;
    use common::{FieldDefinitionExpression, FieldValueExpression, Literal};
    use table::Table;

    #[test]
    fn union() {
        let qstr = "SELECT id, 1 FROM Vote UNION SELECT id, stars from Rating;";
        let qstr2 = "(SELECT id, 1 FROM Vote) UNION (SELECT id, stars from Rating);";
        let res = compound_selection(CompleteByteSlice(qstr.as_bytes()));
        let res2 = compound_selection(CompleteByteSlice(qstr2.as_bytes()));

        let first_select = SelectStatement {
            tables: vec![Table::from("Vote")],
            fields: vec![
                FieldDefinitionExpression::Col(Column::from("id")),
                FieldDefinitionExpression::Value(FieldValueExpression::Literal(
                    Literal::Integer(1).into(),
                )),
            ],
            ..Default::default()
        };
        let second_select = SelectStatement {
            tables: vec![Table::from("Rating")],
            fields: vec![
                FieldDefinitionExpression::Col(Column::from("id")),
                FieldDefinitionExpression::Col(Column::from("stars")),
            ],
            ..Default::default()
        };
        let expected = CompoundSelectStatement {
            selects: vec![
                (None, first_select),
                (Some(CompoundSelectOperator::DistinctUnion), second_select),
            ],
            order: None,
            limit: None,
        };

        assert_eq!(res.unwrap().1, expected);
        assert_eq!(res2.unwrap().1, expected);
    }

    #[test]
    fn multi_union() {
        let qstr = "SELECT id, 1 FROM Vote \
                    UNION SELECT id, stars from Rating \
                    UNION DISTINCT SELECT 42, 5 FROM Vote;";
        let res = compound_selection(CompleteByteSlice(qstr.as_bytes()));

        let first_select = SelectStatement {
            tables: vec![Table::from("Vote")],
            fields: vec![
                FieldDefinitionExpression::Col(Column::from("id")),
                FieldDefinitionExpression::Value(FieldValueExpression::Literal(
                    Literal::Integer(1).into(),
                )),
            ],
            ..Default::default()
        };
        let second_select = SelectStatement {
            tables: vec![Table::from("Rating")],
            fields: vec![
                FieldDefinitionExpression::Col(Column::from("id")),
                FieldDefinitionExpression::Col(Column::from("stars")),
            ],
            ..Default::default()
        };
        let third_select = SelectStatement {
            tables: vec![Table::from("Vote")],
            fields: vec![
                FieldDefinitionExpression::Value(FieldValueExpression::Literal(
                    Literal::Integer(42).into(),
                )),
                FieldDefinitionExpression::Value(FieldValueExpression::Literal(
                    Literal::Integer(5).into(),
                )),
            ],
            ..Default::default()
        };

        let expected = CompoundSelectStatement {
            selects: vec![
                (None, first_select),
                (Some(CompoundSelectOperator::DistinctUnion), second_select),
                (Some(CompoundSelectOperator::DistinctUnion), third_select),
            ],
            order: None,
            limit: None,
        };

        assert_eq!(res.unwrap().1, expected);
    }

    #[test]
    fn union_all() {
        let qstr = "SELECT id, 1 FROM Vote UNION ALL SELECT id, stars from Rating;";
        let res = compound_selection(CompleteByteSlice(qstr.as_bytes()));

        let first_select = SelectStatement {
            tables: vec![Table::from("Vote")],
            fields: vec![
                FieldDefinitionExpression::Col(Column::from("id")),
                FieldDefinitionExpression::Value(FieldValueExpression::Literal(
                    Literal::Integer(1).into(),
                )),
            ],
            ..Default::default()
        };
        let second_select = SelectStatement {
            tables: vec![Table::from("Rating")],
            fields: vec![
                FieldDefinitionExpression::Col(Column::from("id")),
                FieldDefinitionExpression::Col(Column::from("stars")),
            ],
            ..Default::default()
        };
        let expected = CompoundSelectStatement {
            selects: vec![
                (None, first_select),
                (Some(CompoundSelectOperator::Union), second_select),
            ],
            order: None,
            limit: None,
        };

        assert_eq!(res.unwrap().1, expected);
    }
}
