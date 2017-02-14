use nom::IResult;
use std::str;

pub use common::{FieldExpression, Operator};
use create::*;
use insert::*;
use select::*;

#[derive(Clone, Debug, Hash, PartialEq)]
pub enum SqlQuery {
    CreateTable(CreateTableStatement),
    Insert(InsertStatement),
    Select(SelectStatement),
}

/// Parse sequence of SQL statements, divided by semicolons or newlines
// named!(pub query_list<&[u8], Vec<SqlQuery> >,
//    many1!(map_res!(selection, |s| { SqlQuery::Select(s) }))
// );

pub fn parse_query(input: &str) -> Result<SqlQuery, &str> {
    // we process all queries in lowercase to avoid having to deal with capitalization in the
    // parser.
    let q_bytes = String::from(input.trim()).into_bytes();

    // TODO(malte): appropriately pass through errors from nom
    match creation(&q_bytes) {
        IResult::Done(_, o) => return Ok(SqlQuery::CreateTable(o)),
        _ => (),
    };

    match insertion(&q_bytes) {

        IResult::Done(_, o) => return Ok(SqlQuery::Insert(o)),
        _ => (),
    };

    match selection(&q_bytes) {
        IResult::Done(_, o) => return Ok(SqlQuery::Select(o)),
        _ => (),
    };

    Err("failed to parse query")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::hash::{Hash, Hasher};
    use std::collections::hash_map::DefaultHasher;

    use column::Column;
    use table::Table;

    #[test]
    fn hash_query() {
        let qstring = "INSERT INTO users VALUES (42, test);";
        let res = parse_query(qstring);
        assert!(res.is_ok());

        let expected = SqlQuery::Insert(InsertStatement {
            table: Table::from("users"),
            fields: vec![(Column::from("0"), "42".into()), (Column::from("1"), "test".into())],
            ..Default::default()
        });
        let mut h0 = DefaultHasher::new();
        let mut h1 = DefaultHasher::new();
        res.unwrap().hash(&mut h0);
        expected.hash(&mut h1);
        assert_eq!(h0.finish(), h1.finish());
    }

    #[test]
    fn trim_query() {
        let qstring = "   INSERT INTO users VALUES (42, test);     ";
        let res = parse_query(qstring);
        assert!(res.is_ok());
    }
}
