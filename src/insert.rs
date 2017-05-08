use nom::multispace;
use nom::{IResult, Err, ErrorKind, Needed};
use std::str;

use common::{field_list, statement_terminator, table_reference, value_list, Literal};
use column::Column;
use table::Table;

#[derive(Clone, Debug, Default, Hash, PartialEq, Serialize, Deserialize)]
pub struct InsertStatement {
    pub table: Table,
    pub fields: Vec<(Column, Literal)>,
}

/// Parse rule for a SQL insert query.
/// TODO(malte): support REPLACE, multiple parens expr, nested selection, DEFAULT VALUES
named!(pub insertion<&[u8], InsertStatement>,
    complete!(chain!(
        caseless_tag!("insert") ~
        multispace ~
        caseless_tag!("into") ~
        multispace ~
        table: table_reference ~
        multispace ~
        fields: opt!(chain!(
                tag!("(") ~
                multispace? ~
                fields: field_list ~
                multispace? ~
                tag!(")") ~
                multispace,
                || { fields }
                )
            ) ~
        caseless_tag!("values") ~
        multispace ~
        tag!("(") ~
        values: value_list ~
        tag!(")") ~
        statement_terminator,
        || {
            // "table AS alias" isn't legal in INSERT statements
            assert!(table.alias.is_none());
            InsertStatement {
                table: table,
                fields: match fields {
                    Some(ref f) =>
                        f.iter()
                         .cloned()
                         .zip(values.into_iter())
                         .collect(),
                    None =>
                        values.into_iter()
                              .enumerate()
                              .map(|(i, v)| {
                                  (Column::from(format!("{}", i).as_str()), v)
                              })
                              .collect(),
                },
            }
        }
    ))
);

#[cfg(test)]
mod tests {
    use super::*;
    use column::Column;
    use table::Table;

    #[test]
    fn simple_insert() {
        let qstring = "INSERT INTO users VALUES (42, \"test\");";

        let res = insertion(qstring.as_bytes());
        assert_eq!(res.unwrap().1,
                   InsertStatement {
                       table: Table::from("users"),
                       fields: vec![(Column::from("0"), 42.into()),
                                    (Column::from("1"), "test".into())],
                       ..Default::default()
                   });
    }

    #[test]
    fn complex_insert() {
        let qstring = "INSERT INTO users VALUES (42, 'test', \"test\", CURRENT_TIMESTAMP);";

        let res = insertion(qstring.as_bytes());
        assert_eq!(res.unwrap().1,
                   InsertStatement {
                       table: Table::from("users"),
                       fields: vec![(Column::from("0"), 42.into()),
                                    (Column::from("1"), "test".into()),
                                    (Column::from("2"), "test".into()),
                                    (Column::from("3"), Literal::CurrentTimestamp)],
                       ..Default::default()
                   });
    }

    #[test]
    fn insert_with_field_names() {
        let qstring = "INSERT INTO users (id, name) VALUES (42, \"test\");";

        let res = insertion(qstring.as_bytes());
        assert_eq!(res.unwrap().1,
                   InsertStatement {
                       table: Table::from("users"),
                       fields: vec![(Column::from("id"), 42.into()),
                                    (Column::from("name"), "test".into())],
                       ..Default::default()
                   });
    }
}
