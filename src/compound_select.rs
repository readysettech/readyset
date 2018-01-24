use nom::multispace;
use std::str;

use select::{limit_clause, nested_selection, order_clause, LimitClause, OrderClause,
             SelectStatement};

#[derive(Clone, Debug, Hash, PartialEq, Deserialize, Serialize)]
pub enum CompoundSelectOperator {
    Union,
    DistinctUnion,
    Intersect,
    Except,
}

#[derive(Clone, Debug, Hash, PartialEq, Deserialize, Serialize)]
pub struct CompoundSelectStatement {
    pub selects: Vec<(Option<CompoundSelectOperator>, SelectStatement)>,
    pub order: Option<OrderClause>,
    pub limit: Option<LimitClause>,
}

/// Parse compound operator
named!(compound_op<&[u8], CompoundSelectOperator>,
    alt_complete!(
          do_parse!(
              tag_no_case!("union") >>
              distinct: opt!(
                  preceded!(multispace,
                            alt_complete!(  map!(tag_no_case!("all"), |_| { false })
                                          | map!(tag_no_case!("distinct"), |_| { true }))
                            )),
              || {
                  match distinct {
                      // DISTINCT is the default in both MySQL and SQLite
                      None => CompoundSelectOperator::DistinctUnion,
                      Some(d) => {
                          if d {
                              CompoundSelectOperator::DistinctUnion
                          } else {
                              CompoundSelectOperator::Union
                          }
                      },
                  }
              }
          )
        | map!(tag_no_case!("intersect"), |_| CompoundSelectOperator::Intersect)
        | map!(tag_no_case!("except"), |_| CompoundSelectOperator::Except)
    )
);

/// Parse compound selection
named!(pub compound_selection<&[u8], CompoundSelectStatement>,
    complete!(do_parse!(
        first_select: delimited!(opt!(tag!("(")), nested_selection, opt!(tag!(")"))) >>
        other_selects: many1!(
            complete!(
                do_parse!(opt!(multispace) >>
                       op: compound_op >>
                       multispace >>
                       opt!(tag!("(")) >>
                       opt!(multispace) >>
                       select: nested_selection >>
                       opt!(multispace) >>
                       opt!(")") >>
                       (Some(op), select)
                )
            )
        ) >>
        opt!(multispace) >>
        order: opt!(order_clause) >>
        limit: opt!(limit_clause) >>
        ({
            let mut v = vec![(None, first_select)];
            v.extend(other_selects);

            CompoundSelectStatement {
                selects: v,
                order: order,
                limit: limit,
            }
        })
    ))
);

#[cfg(test)]
mod tests {
    use super::*;
    use column::Column;
    use common::FieldExpression;
    use table::Table;

    #[test]
    fn union() {
        let qstr = "SELECT id, 1 FROM Vote UNION SELECT id, stars from Rating;";
        let qstr2 = "(SELECT id, 1 FROM Vote) UNION (SELECT id, stars from Rating);";
        let res = compound_selection(qstr.as_bytes());
        let res2 = compound_selection(qstr2.as_bytes());

        let first_select = SelectStatement {
            tables: vec![Table::from("Vote")],
            fields: vec![
                FieldExpression::Col(Column::from("id")),
                FieldExpression::Literal(1.into()),
            ],
            ..Default::default()
        };
        let second_select = SelectStatement {
            tables: vec![Table::from("Rating")],
            fields: vec![
                FieldExpression::Col(Column::from("id")),
                FieldExpression::Col(Column::from("stars")),
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
        let res = compound_selection(qstr.as_bytes());

        let first_select = SelectStatement {
            tables: vec![Table::from("Vote")],
            fields: vec![
                FieldExpression::Col(Column::from("id")),
                FieldExpression::Literal(1.into()),
            ],
            ..Default::default()
        };
        let second_select = SelectStatement {
            tables: vec![Table::from("Rating")],
            fields: vec![
                FieldExpression::Col(Column::from("id")),
                FieldExpression::Col(Column::from("stars")),
            ],
            ..Default::default()
        };
        let third_select = SelectStatement {
            tables: vec![Table::from("Vote")],
            fields: vec![
                FieldExpression::Literal(42.into()),
                FieldExpression::Literal(5.into()),
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
        let res = compound_selection(qstr.as_bytes());

        let first_select = SelectStatement {
            tables: vec![Table::from("Vote")],
            fields: vec![
                FieldExpression::Col(Column::from("id")),
                FieldExpression::Literal(1.into()),
            ],
            ..Default::default()
        };
        let second_select = SelectStatement {
            tables: vec![Table::from("Rating")],
            fields: vec![
                FieldExpression::Col(Column::from("id")),
                FieldExpression::Col(Column::from("stars")),
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
