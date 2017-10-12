use nom::IResult;
use std::str;
use std::fmt;

use create::*;
use insert::*;
use select::*;
use delete::*;
use update::*;

#[derive(Clone, Debug, Hash, PartialEq, Serialize, Deserialize)]
pub enum SqlQuery {
    CreateTable(CreateTableStatement),
    Insert(InsertStatement),
    Select(SelectStatement),
    Delete(DeleteStatement),
    Update(UpdateStatement),
}

impl fmt::Display for SqlQuery {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            SqlQuery::Select(ref select) => write!(f, "{}", select),
            SqlQuery::Insert(ref insert) => write!(f, "{}", insert),
            SqlQuery::CreateTable(ref create) => write!(f, "{}", create),
            SqlQuery::Delete(ref delete) => write!(f, "{}", delete),
            SqlQuery::Update(ref update) => write!(f, "{}", update),
        }
    }
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

    match deletion(&q_bytes) {
        IResult::Done(_, o) => return Ok(SqlQuery::Delete(o)),
        _ => (),
    };
    match updating(&q_bytes) {
        IResult::Done(_, o) => return Ok(SqlQuery::Update(o)),
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
        let qstring = "INSERT INTO users VALUES (42, \"test\");";
        let res = parse_query(qstring);
        assert!(res.is_ok());

        let expected = SqlQuery::Insert(InsertStatement {
            table: Table::from("users"),
            fields: vec![
                (Column::from("0"), 42.into()),
                (Column::from("1"), "test".into()),
            ],
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
        let qstring = "   INSERT INTO users VALUES (42, \"test\");     ";
        let res = parse_query(qstring);
        assert!(res.is_ok());
    }

    #[test]
    fn display_select_query() {
        let qstring0 = "SELECT * FROM users";
        let qstring1 = "SELECT * FROM users AS u";
        let qstring2 = "SELECT name, password FROM users AS u";
        let qstring3 = "SELECT name, password FROM users AS u WHERE user_id = '1'";
        let qstring4 = "SELECT name, password FROM users AS u WHERE user = 'aaa' AND password = 'xxx'";

        let res0 = parse_query(qstring0);
        let res1 = parse_query(qstring1);
        let res2 = parse_query(qstring2);
        let res3 = parse_query(qstring3);
        let res4 = parse_query(qstring4);

        assert!(res0.is_ok());
        assert!(res1.is_ok());
        assert!(res2.is_ok());
        assert!(res3.is_ok());
        assert!(res4.is_ok());

        assert_eq!(qstring0, format!("{}", res0.unwrap()));
        assert_eq!(qstring1, format!("{}", res1.unwrap()));
        assert_eq!(qstring2, format!("{}", res2.unwrap()));
        assert_eq!(qstring3, format!("{}", res3.unwrap()));
        assert_eq!(qstring4, format!("{}", res4.unwrap()));
    }

    #[test]
    fn format_select_query() {
        let qstring1 = "select * from users u";
        let qstring2 = "select name,password from users u;";
        let qstring3 = "select name,password from users u WHERE user_id='1'";

        let expected1 = "SELECT * FROM users AS u";
        let expected2 = "SELECT name, password FROM users AS u";
        let expected3 = "SELECT name, password FROM users AS u WHERE user_id = '1'";

        let res1 = parse_query(qstring1);
        let res2 = parse_query(qstring2);
        let res3 = parse_query(qstring3);

        assert!(res1.is_ok());
        assert!(res2.is_ok());
        assert!(res3.is_ok());

        assert_eq!(expected1, format!("{}", res1.unwrap()));
        assert_eq!(expected2, format!("{}", res2.unwrap()));
        assert_eq!(expected3, format!("{}", res3.unwrap()));
    }

    #[test]
    fn format_select_query_with_where_clause() {
        let qstring0 = "select name, password from users as u where user='aaa' and password= 'xxx'";
        let qstring1 = "select name, password from users as u where user=? and password =?";

        let expected0 = "SELECT name, password FROM users AS u WHERE user = 'aaa' AND password = 'xxx'";
        let expected1 = "SELECT name, password FROM users AS u WHERE user = ? AND password = ?";

        let res0 = parse_query(qstring0);
        let res1 = parse_query(qstring1);
        assert!(res0.is_ok());
        assert!(res1.is_ok());
        assert_eq!(expected0, format!("{}", res0.unwrap()));
        assert_eq!(expected1, format!("{}", res1.unwrap()));
    }

    #[test]
    fn format_select_query_with_function() {
        let qstring1 = "select count(*) from users";
        let expected1 = "SELECT count(all) FROM users";

        let res1 = parse_query(qstring1);
        assert!(res1.is_ok());
        assert_eq!(expected1, format!("{}", res1.unwrap()));
    }

    #[test]
    fn display_insert_query() {
        let qstring = "INSERT INTO users (name, password) VALUES ('aaa', 'xxx')";
        let res = parse_query(qstring);
        assert!(res.is_ok());
        assert_eq!(qstring, format!("{}", res.unwrap()));
    }

    #[test]
    fn display_insert_query_no_columns() {
        let qstring = "INSERT INTO users VALUES ('aaa', 'xxx')";
        let expected = "INSERT INTO users (0, 1) VALUES ('aaa', 'xxx')";
        let res = parse_query(qstring);
        assert!(res.is_ok());
        assert_eq!(expected, format!("{}", res.unwrap()));
    }

    #[test]
    fn format_insert_query() {
        let qstring = "insert into users (name, password) values ('aaa', 'xxx')";
        let expected = "INSERT INTO users (name, password) VALUES ('aaa', 'xxx')";
        let res = parse_query(qstring);
        assert!(res.is_ok());
        assert_eq!(expected, format!("{}", res.unwrap()));
    }

    #[test]
    fn format_update_query() {
        let qstring = "update users set name=42, password='xxx' where id=1";
        let expected = "UPDATE users SET name = 42, password = 'xxx' WHERE id = 1";
        let res = parse_query(qstring);
        println!("res: {:#?}", res);
        assert!(res.is_ok());
        assert_eq!(expected, format!("{}", res.unwrap()));
    }

    #[test]
    fn format_delete_query_with_where_clause() {
        let qstring0 = "delete from users where user='aaa' and password= 'xxx'";
        let qstring1 = "delete from users where user=? and password =?";

        let expected0 = "DELETE FROM users WHERE user = 'aaa' AND password = 'xxx'";
        let expected1 = "DELETE FROM users WHERE user = ? AND password = ?";

        let res0 = parse_query(qstring0);
        let res1 = parse_query(qstring1);
        assert!(res0.is_ok());
        assert!(res1.is_ok());
        assert_eq!(expected0, format!("{}", res0.unwrap()));
        assert_eq!(expected1, format!("{}", res1.unwrap()));
    }

}
