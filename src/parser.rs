use nom::types::CompleteByteSlice;
use std::fmt;
use std::str;

use compound_select::{compound_selection, CompoundSelectStatement};
use create::{creation, view_creation, CreateTableStatement, CreateViewStatement};
use delete::{deletion, DeleteStatement};
use drop::{drop_table, DropTableStatement};
use insert::{insertion, InsertStatement};
use select::{selection, SelectStatement};
use set::{set, SetStatement};
use update::{updating, UpdateStatement};

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum SqlQuery {
    CreateTable(CreateTableStatement),
    CreateView(CreateViewStatement),
    Insert(InsertStatement),
    CompoundSelect(CompoundSelectStatement),
    Select(SelectStatement),
    Delete(DeleteStatement),
    DropTable(DropTableStatement),
    Update(UpdateStatement),
    Set(SetStatement),
}

impl fmt::Display for SqlQuery {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            SqlQuery::Select(ref select) => write!(f, "{}", select),
            SqlQuery::Insert(ref insert) => write!(f, "{}", insert),
            SqlQuery::CreateTable(ref create) => write!(f, "{}", create),
            SqlQuery::CreateView(ref create) => write!(f, "{}", create),
            SqlQuery::Delete(ref delete) => write!(f, "{}", delete),
            SqlQuery::DropTable(ref drop) => write!(f, "{}", drop),
            SqlQuery::Update(ref update) => write!(f, "{}", update),
            SqlQuery::Set(ref set) => write!(f, "{}", set),
            _ => unimplemented!(),
        }
    }
}

named!(pub sql_query<CompleteByteSlice, SqlQuery>,
    alt!(
          do_parse!(c: creation >> (SqlQuery::CreateTable(c)))
        | do_parse!(i: insertion >> (SqlQuery::Insert(i)))
        | do_parse!(c: compound_selection >> (SqlQuery::CompoundSelect(c)))
        | do_parse!(s: selection >> (SqlQuery::Select(s)))
        | do_parse!(d: deletion >> (SqlQuery::Delete(d)))
        | do_parse!(dt: drop_table >> (SqlQuery::DropTable(dt)))
        | do_parse!(u: updating >> (SqlQuery::Update(u)))
        | do_parse!(s: set >> (SqlQuery::Set(s)))
        | do_parse!(c: view_creation >> (SqlQuery::CreateView(c)))
    )
);

pub fn parse_query_bytes<T>(input: T) -> Result<SqlQuery, &'static str>
    where T: AsRef<[u8]> {
    match sql_query(CompleteByteSlice(input.as_ref())) {
        Ok((_, o)) => Ok(o),
        Err(_) => Err("failed to parse query"),
    }
}

pub fn parse_query<T>(input: T) -> Result<SqlQuery, &'static str>
    where T: AsRef<str> {
    parse_query_bytes(input.as_ref().trim().as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    use table::Table;

    #[test]
    fn hash_query() {
        let qstring = "INSERT INTO users VALUES (42, \"test\");";
        let res = parse_query(qstring);
        assert!(res.is_ok());

        let expected = SqlQuery::Insert(InsertStatement {
            table: Table::from("users"),
            fields: None,
            data: vec![vec![42.into(), "test".into()]],
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
    fn parse_byte_slice() {
        let qstring: &[u8] = b"INSERT INTO users VALUES (42, \"test\");";
        let res = parse_query_bytes(qstring);
        assert!(res.is_ok());
    }

    #[test]
    fn parse_byte_vector() {
        let qstring: Vec<u8> = b"INSERT INTO users VALUES (42, \"test\");".to_vec();
        let res = parse_query_bytes(&qstring);
        assert!(res.is_ok());
    }

    #[test]
    fn display_select_query() {
        let qstring0 = "SELECT * FROM users";
        let qstring1 = "SELECT * FROM users AS u";
        let qstring2 = "SELECT name, password FROM users AS u";
        let qstring3 = "SELECT name, password FROM users AS u WHERE user_id = '1'";
        let qstring4 =
            "SELECT name, password FROM users AS u WHERE user = 'aaa' AND password = 'xxx'";
        let qstring5 = "SELECT name * 2 AS double_name FROM users";

        let res0 = parse_query(qstring0);
        let res1 = parse_query(qstring1);
        let res2 = parse_query(qstring2);
        let res3 = parse_query(qstring3);
        let res4 = parse_query(qstring4);
        let res5 = parse_query(qstring5);

        assert!(res0.is_ok());
        assert!(res1.is_ok());
        assert!(res2.is_ok());
        assert!(res3.is_ok());
        assert!(res4.is_ok());
        assert!(res5.is_ok());

        assert_eq!(qstring0, format!("{}", res0.unwrap()));
        assert_eq!(qstring1, format!("{}", res1.unwrap()));
        assert_eq!(qstring2, format!("{}", res2.unwrap()));
        assert_eq!(qstring3, format!("{}", res3.unwrap()));
        assert_eq!(qstring4, format!("{}", res4.unwrap()));
        assert_eq!(qstring5, format!("{}", res5.unwrap()));
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

        let expected0 =
            "SELECT name, password FROM users AS u WHERE user = 'aaa' AND password = 'xxx'";
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
        let expected1 = "SELECT count(*) FROM users";

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
        let expected = "INSERT INTO users VALUES ('aaa', 'xxx')";
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

    #[test]
    fn format_query_with_escaped_keyword() {
        let qstring0 = "delete from articles where `key`='aaa'";
        let qstring1 = "delete from `where` where user=?";

        let expected0 = "DELETE FROM articles WHERE `key` = 'aaa'";
        let expected1 = "DELETE FROM `where` WHERE user = ?";

        let res0 = parse_query(qstring0);
        let res1 = parse_query(qstring1);
        assert!(res0.is_ok());
        assert!(res1.is_ok());
        assert_eq!(expected0, format!("{}", res0.unwrap()));
        assert_eq!(expected1, format!("{}", res1.unwrap()));
    }
}
