use nom::multispace;
use nom::{Err, ErrorKind, IResult, Needed};
use std::{fmt, str};

use common::{field_value_list, table_reference, Literal};
use condition::ConditionExpression;
use table::Table;
use column::Column;
use select::where_clause;


#[derive(Clone, Debug, Default, Hash, PartialEq, Serialize, Deserialize)]
pub struct UpdateStatement {
    pub table: Table,
    pub fields: Vec<(Column, Literal)>,
    pub where_clause: Option<ConditionExpression>,
}

impl fmt::Display for UpdateStatement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "UPDATE {} ", self.table)?;
        assert!(self.fields.len() > 0);
        write!(
            f,
            "SET {}",
            self.fields
                .iter()
                .map(|&(ref col, ref literal)| {
                    format!("{} = {}", col, literal.to_string())
                })
                .collect::<Vec<_>>()
                .join(", ")
        )?;
        if let Some(ref where_clause) = self.where_clause {
            write!(f, " WHERE ")?;
            write!(f, "{}", where_clause)?;
        }
        Ok(())
    }
}


named!(pub updating<&[u8], UpdateStatement>,
    chain!(
        caseless_tag!("update") ~
        multispace ~
        table: table_reference ~
        multispace ~
        caseless_tag!("set") ~
        multispace ~
        fields: field_value_list ~
        cond: opt!(where_clause) ~
        || {
            UpdateStatement {
                table: table,
                fields: fields,
                where_clause: cond,
            }
        }
    )
);

#[cfg(test)]
mod tests {
    use super::*;
    use column::Column;
    use table::Table;
    use common::{Literal, Operator};
    use condition::ConditionBase::*;
    use condition::ConditionExpression::*;
    use condition::ConditionTree;

    #[test]
    fn simple_update() {
        let qstring = "UPDATE users SET id = 42, name = 'test'";

        let res = updating(qstring.as_bytes());
        assert_eq!(
            res.unwrap().1,
            UpdateStatement {
                table: Table::from("users"),
                fields: vec![
                    (Column::from("id"), 42.into()),
                    (Column::from("name"), "test".into()),
                ],
                ..Default::default()
            }
        );
    }
    #[test]
    fn update_with_where_clause() {
        let qstring = "UPDATE users SET id = 42, name = 'test' WHERE id = 1";

        let res = updating(qstring.as_bytes());
        let expected_left = Base(Field(Column::from("id")));
        let expected_where_cond = Some(ComparisonOp(ConditionTree {
            left: Box::new(expected_left),
            right: Box::new(Base(Literal(Literal::Integer(1)))),
            operator: Operator::Equal,
        }));
        assert_eq!(
            res.unwrap().1,
            UpdateStatement {
                table: Table::from("users"),
                fields: vec![
                    (Column::from("id"), 42.into()),
                    (Column::from("name"), "test".into()),
                ],
                where_clause: expected_where_cond,
                ..Default::default()
            }
        );
    }

    #[test]
    fn format_update_with_where_clause() {
        let qstring = "UPDATE users SET id = 42, name = 'test' WHERE id = 1";
        let expected = "UPDATE users SET id = 42, name = 'test' WHERE id = 1";
        let res = updating(qstring.as_bytes());
        assert_eq!(format!("{}", res.unwrap().1), expected);
    }

}
