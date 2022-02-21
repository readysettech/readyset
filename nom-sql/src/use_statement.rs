use std::fmt;

use nom::bytes::complete::tag_no_case;
use nom::character::complete::multispace1;
use nom::IResult;
use serde::{Deserialize, Serialize};

use crate::{Dialect, SqlIdentifier};

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct UseStatement {
    pub database: SqlIdentifier,
}

impl UseStatement {
    fn from_database(database: SqlIdentifier) -> Self {
        Self { database }
    }
}

impl fmt::Display for UseStatement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "USE {}", self.database)
    }
}

pub fn use_statement(dialect: Dialect) -> impl Fn(&[u8]) -> IResult<&[u8], UseStatement> {
    move |i| {
        let (i, _) = tag_no_case("use")(i)?;
        let (i, _) = multispace1(i)?;
        let (i, database) = dialect.identifier()(i)?;
        Ok((i, UseStatement::from_database(database)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn use_db() {
        let qstring1 = "USE db1";
        let qstring2 = "use `db2`";
        let qstring3 = "USE `test`";
        let qstring4 = "use noria";
        let res1 = use_statement(Dialect::MySQL)(qstring1.as_bytes())
            .unwrap()
            .1;
        let res2 = use_statement(Dialect::MySQL)(qstring2.as_bytes())
            .unwrap()
            .1;
        let res3 = use_statement(Dialect::MySQL)(qstring3.as_bytes())
            .unwrap()
            .1;
        let res4 = use_statement(Dialect::MySQL)(qstring4.as_bytes())
            .unwrap()
            .1;
        assert_eq!(res1, UseStatement::from_database("db1".into()));
        assert_eq!(res2, UseStatement::from_database("db2".into()));
        assert_eq!(res3, UseStatement::from_database("test".into()));
        assert_eq!(res4, UseStatement::from_database("noria".into()));
    }
}
