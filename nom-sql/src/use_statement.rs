use nom::bytes::complete::tag_no_case;
use nom_locate::LocatedSpan;
use readyset_sql::{ast::*, Dialect};

use crate::dialect::DialectParser;
use crate::whitespace::whitespace1;
use crate::NomSqlResult;

pub fn use_statement(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], UseStatement> {
    move |i| {
        let (i, _) = tag_no_case("use")(i)?;
        let (i, _) = whitespace1(i)?;
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
        let res1 = use_statement(Dialect::MySQL)(LocatedSpan::new(qstring1.as_bytes()))
            .unwrap()
            .1;
        let res2 = use_statement(Dialect::MySQL)(LocatedSpan::new(qstring2.as_bytes()))
            .unwrap()
            .1;
        let res3 = use_statement(Dialect::MySQL)(LocatedSpan::new(qstring3.as_bytes()))
            .unwrap()
            .1;
        let res4 = use_statement(Dialect::MySQL)(LocatedSpan::new(qstring4.as_bytes()))
            .unwrap()
            .1;
        assert_eq!(res1, UseStatement::from_database("db1".into()));
        assert_eq!(res2, UseStatement::from_database("db2".into()));
        assert_eq!(res3, UseStatement::from_database("test".into()));
        assert_eq!(res4, UseStatement::from_database("noria".into()));
    }
}
