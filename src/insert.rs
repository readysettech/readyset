use nom::multispace;
use nom::{IResult, Err, ErrorKind, Needed};
use std::str;

use common::{fieldlist, statement_terminator, table_reference};

#[derive(Clone, Debug, Default, PartialEq)]
pub struct InsertStatement {
    pub table: String,
    pub fields: Vec<String>,
}

/// Parse rule for a SQL insert query.
/// TODO(malte): support REPLACE, multiple parens expr, nested selection, DEFAULT VALUES
named!(pub insertion<&[u8], InsertStatement>,
    dbg_dmp!(chain!(
        caseless_tag!("insert") ~
        multispace ~
        caseless_tag!("into") ~
        multispace ~
        table: table_reference ~
        multispace ~
        caseless_tag!("values") ~
        multispace ~
        tag!("(") ~
        fields: fieldlist ~
        tag!(")") ~
        statement_terminator,
        || {
            InsertStatement {
                table: String::from(table),
                fields: fields.iter().map(|s| String::from(*s)).collect(),
            }
        }
    ))
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_insert() {
        let qstring = "INSERT INTO users VALUES (42, test);";

        let res = insertion(qstring.as_bytes());
        assert_eq!(res.unwrap().1,
                   InsertStatement {
                       table: String::from("users"),
                       fields: vec!["42".into(), "test".into()],
                       ..Default::default()
                   });
    }
}
