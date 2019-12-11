use std::{fmt, str};
use nom::character::complete::multispace0;

use common::{statement_terminator, table_list};
use keywords::escape_if_keyword;
use table::Table;

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct DropTableStatement {
    pub tables: Vec<Table>,
    pub if_exists: bool,
}

impl fmt::Display for DropTableStatement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DROP TABLE ")?;
        if self.if_exists {
            write!(f, "IF EXISTS ")?;
        }
        let ts = self
            .tables
            .iter()
            .map(|t| escape_if_keyword(&t.name))
            .collect::<Vec<_>>()
            .join(", ");
        write!(f, "{}", ts)?;
        Ok(())
    }
}

named!(pub drop_table<&[u8], DropTableStatement>,
    do_parse!(
        tag_no_case!("drop table") >>
        if_exists: opt!(delimited!(multispace0, tag_no_case!("if exists"), multispace0)) >>
        multispace0 >>
        tables: table_list >>
        multispace0 >>
        // MySQL 5.7 reference manual, §13.1.29:
        // The RESTRICT and CASCADE keywords do nothing. They are permitted to make porting easier from
        // other database systems.
        opt!(delimited!(multispace0, tag_no_case!("restricted"), multispace0)) >>
        opt!(delimited!(multispace0, tag_no_case!("cascade"), multispace0)) >>
        statement_terminator >>
        ({
            DropTableStatement {
                tables: tables,
                if_exists: if_exists.is_some(),
            }
        })
    )
);

#[cfg(test)]
mod tests {
    use super::*;
    use table::Table;

    #[test]
    fn simple_drop_table() {
        let qstring = "DROP TABLE users;";
        let res = drop_table(qstring.as_bytes());
        assert_eq!(
            res.unwrap().1,
            DropTableStatement {
                tables: vec![Table::from("users")],
                if_exists: false,
            }
        );
    }

    #[test]
    fn format_drop_table() {
        let qstring = "DROP TABLE IF EXISTS users,posts;";
        let expected = "DROP TABLE IF EXISTS users, posts";
        let res = drop_table(qstring.as_bytes());
        assert_eq!(format!("{}", res.unwrap().1), expected);
    }
}
