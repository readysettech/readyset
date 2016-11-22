use nom::multispace;
use nom::{IResult, Err, ErrorKind, Needed};
use std::str;

use common::{field_list, statement_terminator, table_reference, value_list};
use parser::Column;

#[derive(Clone, Debug, Default, PartialEq)]
pub struct InsertStatement {
    pub table: String,
    pub fields: Vec<(Column, String)>,
}

/// Parse rule for a SQL insert query.
/// TODO(malte): support REPLACE, multiple parens expr, nested selection, DEFAULT VALUES
named!(pub insertion<&[u8], InsertStatement>,
    chain!(
        caseless_tag!("insert") ~
        multispace ~
        caseless_tag!("into") ~
        multispace ~
        table: table_reference ~
        multispace ~
        fields: opt!(chain!(
                tag!("(") ~
                fields: field_list ~
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
            InsertStatement {
                table: String::from(table),
                fields: match fields {
                    Some(ref f) =>
                        f.iter()
                         .cloned()
                         .zip(values.into_iter()
                                    .map(|s| String::from(s))
                                    )
                         .collect(),
                    None =>
                        values.into_iter()
                              .enumerate()
                              .map(|(i, v)| (Column::from(format!("{}", i).as_str()), String::from(v)))
                              .collect(),
                },
            }
        }
    )
);

#[cfg(test)]
mod tests {
    use super::*;
    use parser::Column;

    #[test]
    fn simple_insert() {
        let qstring = "INSERT INTO users VALUES (42, test);";

        let res = insertion(qstring.as_bytes());
        assert_eq!(res.unwrap().1,
                   InsertStatement {
                       table: String::from("users"),
                       fields: vec![(Column::from("0"), "42".into()), (Column::from("1"), "test".into())],
                       ..Default::default()
                   });
    }

    #[test]
    fn placeholder_insert() {
        let qstring = "INSERT INTO users VALUES (?, ?);";

        let res = insertion(qstring.as_bytes());
        assert_eq!(res.unwrap().1,
                   InsertStatement {
                       table: String::from("users"),
                       fields: vec![(Column::from("0"), "?".into()), (Column::from("1"), "?".into())],
                       ..Default::default()
                   });
    }

    #[test]
    fn insert_with_field_names() {
        let qstring = "INSERT INTO users (id, name) VALUES (42, test);";

        let res = insertion(qstring.as_bytes());
        assert_eq!(res.unwrap().1,
                   InsertStatement {
                       table: String::from("users"),
                       fields: vec![(Column::from("id"), "42".into()), (Column::from("name"), "test".into())],
                       ..Default::default()
                   });
    }
}
