use nom::multispace;
use nom::types::CompleteByteSlice;
use std::fmt;
use std::str;

use column::Column;
use common::{
    assignment_expr_list, field_list, opt_multispace, statement_terminator, table_reference,
    value_list, FieldValueExpression, Literal,
};
use keywords::escape_if_keyword;
use table::Table;

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct InsertStatement {
    pub table: Table,
    pub fields: Option<Vec<Column>>,
    pub data: Vec<Vec<Literal>>,
    pub ignore: bool,
    pub on_duplicate: Option<Vec<(Column, FieldValueExpression)>>,
}

impl fmt::Display for InsertStatement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "INSERT INTO {}", escape_if_keyword(&self.table.name))?;
        if let Some(ref fields) = self.fields {
            write!(
                f,
                " ({})",
                fields
                    .iter()
                    .map(|ref col| col.name.to_owned())
                    .collect::<Vec<_>>()
                    .join(", ")
            )?;
        }
        write!(
            f,
            " VALUES {}",
            self.data
                .iter()
                .map(|datas| format!(
                    "({})",
                    datas
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

// Parse rule for a SQL insert query.
// TODO(malte): support REPLACE, nested selection, DEFAULT VALUES
named!(pub insertion<CompleteByteSlice, InsertStatement>,
    do_parse!(
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
                    do_parse!(
                            opt_multispace >>
                            tag!(",") >>
                            opt_multispace >>
                            ()
                    )
                ) >>
                (values)
            )
        ) >>
        upd_if_dup: opt!(do_parse!(
                opt_multispace >>
                tag_no_case!("on duplicate key update") >>
                multispace >>
                assigns: assignment_expr_list >>
                (assigns)
        )) >>
        statement_terminator >>
        ({
            // "table AS alias" isn't legal in INSERT statements
            assert!(table.alias.is_none());
            InsertStatement {
                table: table,
                fields: fields,
                data: data,
                ignore: ignore.is_some(),
                on_duplicate: upd_if_dup,
            }
        })
    )
);

#[cfg(test)]
mod tests {
    use super::*;
    use arithmetic::{ArithmeticBase, ArithmeticExpression, ArithmeticOperator};
    use column::Column;
    use table::Table;

    #[test]
    fn simple_insert() {
        let qstring = "INSERT INTO users VALUES (42, \"test\");";

        let res = insertion(CompleteByteSlice(qstring.as_bytes()));
        assert_eq!(
            res.unwrap().1,
            InsertStatement {
                table: Table::from("users"),
                fields: None,
                data: vec![vec![42.into(), "test".into()]],
                ..Default::default()
            }
        );
    }

    #[test]
    fn complex_insert() {
        let qstring = "INSERT INTO users VALUES (42, 'test', \"test\", CURRENT_TIMESTAMP);";

        let res = insertion(CompleteByteSlice(qstring.as_bytes()));
        assert_eq!(
            res.unwrap().1,
            InsertStatement {
                table: Table::from("users"),
                fields: None,
                data: vec![vec![
                    42.into(),
                    "test".into(),
                    "test".into(),
                    Literal::CurrentTimestamp,
                ],],
                ..Default::default()
            }
        );
    }

    #[test]
    fn insert_with_field_names() {
        let qstring = "INSERT INTO users (id, name) VALUES (42, \"test\");";

        let res = insertion(CompleteByteSlice(qstring.as_bytes()));
        assert_eq!(
            res.unwrap().1,
            InsertStatement {
                table: Table::from("users"),
                fields: Some(vec![Column::from("id"), Column::from("name")]),
                data: vec![vec![42.into(), "test".into()]],
                ..Default::default()
            }
        );
    }

    // Issue #3
    #[test]
    fn insert_without_spaces() {
        let qstring = "INSERT INTO users(id, name) VALUES(42, \"test\");";

        let res = insertion(CompleteByteSlice(qstring.as_bytes()));
        assert_eq!(
            res.unwrap().1,
            InsertStatement {
                table: Table::from("users"),
                fields: Some(vec![Column::from("id"), Column::from("name")]),
                data: vec![vec![42.into(), "test".into()]],
                ..Default::default()
            }
        );
    }

    #[test]
    fn multi_insert() {
        let qstring = "INSERT INTO users (id, name) VALUES (42, \"test\"),(21, \"test2\");";

        let res = insertion(CompleteByteSlice(qstring.as_bytes()));
        assert_eq!(
            res.unwrap().1,
            InsertStatement {
                table: Table::from("users"),
                fields: Some(vec![Column::from("id"), Column::from("name")]),
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

        let res = insertion(CompleteByteSlice(qstring.as_bytes()));
        assert_eq!(
            res.unwrap().1,
            InsertStatement {
                table: Table::from("users"),
                fields: Some(vec![Column::from("id"), Column::from("name")]),
                data: vec![vec![Literal::Placeholder, Literal::Placeholder]],
                ..Default::default()
            }
        );
    }

    #[test]
    fn insert_with_on_dup_update() {
        let qstring = "INSERT INTO keystores (`key`, `value`) VALUES (?, ?) \
                       ON DUPLICATE KEY UPDATE `value` = `value` + 1";

        let res = insertion(CompleteByteSlice(qstring.as_bytes()));
        let expected_ae = ArithmeticExpression {
            op: ArithmeticOperator::Add,
            left: ArithmeticBase::Column(Column::from("value")),
            right: ArithmeticBase::Scalar(1.into()),
            alias: None,
        };
        assert_eq!(
            res.unwrap().1,
            InsertStatement {
                table: Table::from("keystores"),
                fields: Some(vec![Column::from("key"), Column::from("value")]),
                data: vec![vec![Literal::Placeholder, Literal::Placeholder]],
                on_duplicate: Some(vec![(
                    Column::from("value"),
                    FieldValueExpression::Arithmetic(expected_ae),
                ),]),
                ..Default::default()
            }
        );
    }

    #[test]
    fn insert_with_leading_value_whitespace() {
        let qstring = "INSERT INTO users (id, name) VALUES ( 42, \"test\");";

        let res = insertion(CompleteByteSlice(qstring.as_bytes()));
        assert_eq!(
            res.unwrap().1,
            InsertStatement {
                table: Table::from("users"),
                fields: Some(vec![Column::from("id"), Column::from("name")]),
                data: vec![vec![42.into(), "test".into()]],
                ..Default::default()
            }
        );
    }

}
