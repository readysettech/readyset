use nom::multispace;
use std::str;
use std::fmt;

use common::{field_list, opt_multispace, statement_terminator, table_reference, value_list,
             Literal};
use column::Column;
use keywords::escape_if_keyword;
use table::Table;

#[derive(Clone, Debug, Default, Hash, PartialEq, Serialize, Deserialize)]
pub struct InsertStatement {
    pub table: Table,
    pub fields: Vec<Column>,
    pub data: Vec<Vec<Literal>>,
    pub ignore: bool,
}

impl fmt::Display for InsertStatement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "INSERT INTO {}", escape_if_keyword(&self.table.name))?;
        write!(
            f,
            " ({})",
            self.fields
                .iter()
                .map(|ref col| col.name.to_owned())
                .collect::<Vec<_>>()
                .join(", ")
        )?;
        write!(
            f,
            " VALUES {}",
            self.data
                .iter()
                .map(|fields| format!(
                    "({})",
                    fields
                        .into_iter()
                        .map(|l| l.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                ))
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

/// Parse rule for a SQL insert query.
/// TODO(malte): support REPLACE, nested selection, DEFAULT VALUES
named!(pub insertion<&[u8], InsertStatement>,
    complete!(do_parse!(
        tag_no_case!("insert") >>
        ignore: opt!(preceded!(multispace, tag_no_case!("ignore"))) >>
        multispace >>
        tag_no_case!("into") >>
        multispace >>
        table: table_reference >>
        opt_multispace >>
        fields: opt!(do_parse!(
                tag!("(") >>
                opt_multispace >>
                fields: field_list >>
                opt_multispace >>
                tag!(")") >>
                multispace >>
                (fields)
                )
            ) >>
        tag_no_case!("values") >>
        opt_multispace >>
        data: many1!(
            do_parse!(
                tag!("(") >>
                values: value_list >>
                tag!(")") >>
                opt!(
                    complete!(do_parse!(
                            opt_multispace >>
                            tag!(",") >>
                            opt_multispace >>
                            ()
                    ))
                ) >>
                (values)
            )
        ) >>
        statement_terminator >>
        ({
            // "table AS alias" isn't legal in INSERT statements
            assert!(table.alias.is_none());
            InsertStatement {
                table: table,
                fields: match fields {
                    Some(ref f) =>
                        f.iter()
                         .cloned()
                         .collect(),
                    None =>
                        data[0].iter()
                              .enumerate()
                              .map(|(i, _)| {
                                  Column::from(format!("{}", i).as_str())
                              })
                              .collect(),
                },
                data: data,
                ignore: ignore.is_some(),
            }
        })
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
        assert_eq!(
            res.unwrap().1,
            InsertStatement {
                table: Table::from("users"),
                fields: vec![Column::from("0"), Column::from("1")],
                data: vec![vec![42.into(), "test".into()]],
                ..Default::default()
            }
        );
    }

    #[test]
    fn complex_insert() {
        let qstring = "INSERT INTO users VALUES (42, 'test', \"test\", CURRENT_TIMESTAMP);";

        let res = insertion(qstring.as_bytes());
        assert_eq!(
            res.unwrap().1,
            InsertStatement {
                table: Table::from("users"),
                fields: vec![
                    Column::from("0"),
                    Column::from("1"),
                    Column::from("2"),
                    Column::from("3"),
                ],
                data: vec![
                    vec![
                        42.into(),
                        "test".into(),
                        "test".into(),
                        Literal::CurrentTimestamp,
                    ],
                ],
                ..Default::default()
            }
        );
    }

    #[test]
    fn insert_with_field_names() {
        let qstring = "INSERT INTO users (id, name) VALUES (42, \"test\");";

        let res = insertion(qstring.as_bytes());
        assert_eq!(
            res.unwrap().1,
            InsertStatement {
                table: Table::from("users"),
                fields: vec![Column::from("id"), Column::from("name")],
                data: vec![vec![42.into(), "test".into()]],
                ..Default::default()
            }
        );
    }

    // Issue #3
    #[test]
    fn insert_without_spaces() {
        let qstring = "INSERT INTO users(id, name) VALUES(42, \"test\");";

        let res = insertion(qstring.as_bytes());
        assert_eq!(
            res.unwrap().1,
            InsertStatement {
                table: Table::from("users"),
                fields: vec![Column::from("id"), Column::from("name")],
                data: vec![vec![42.into(), "test".into()]],
                ..Default::default()
            }
        );
    }

    #[test]
    fn multi_insert() {
        let qstring = "INSERT INTO users (id, name) VALUES (42, \"test\"),(21, \"test2\");";

        let res = insertion(qstring.as_bytes());
        assert_eq!(
            res.unwrap().1,
            InsertStatement {
                table: Table::from("users"),
                fields: vec![Column::from("id"), Column::from("name")],
                data: vec![
                    vec![42.into(), "test".into()],
                    vec![21.into(), "test2".into()],
                ],
                ..Default::default()
            }
        );
    }

    #[test]
    fn insert_with_parameters() {
        let qstring = "INSERT INTO users (id, name) VALUES (?, ?);";

        let res = insertion(qstring.as_bytes());
        assert_eq!(
            res.unwrap().1,
            InsertStatement {
                table: Table::from("users"),
                fields: vec![Column::from("id"), Column::from("name")],
                data: vec![vec![Literal::Placeholder, Literal::Placeholder]],
                ..Default::default()
            }
        );
    }
}
