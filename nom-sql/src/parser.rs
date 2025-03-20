use std::str;

use nom::branch::alt;
use nom::combinator::{map, opt};
use nom_locate::LocatedSpan;
use readyset_sql::{ast::*, Dialect};
use readyset_util::redacted::Sensitive;

use crate::alter::{alter_readyset_statement, alter_table_statement};
use crate::comment::comment;
use crate::common::statement_terminator;
use crate::compound_select::simple_or_compound_selection;
use crate::create::{
    create_cached_query, create_database, create_table, key_specification, view_creation,
};
use crate::deallocate::deallocate;
use crate::delete::deletion;
use crate::drop::{
    drop_all_caches, drop_all_proxied_queries, drop_cached_query, drop_table, drop_view,
};
use crate::explain::explain_statement;
use crate::expression::expression;
use crate::insert::insertion;
use crate::rename::rename_table;
use crate::rls::{create_rls, drop_rls};
use crate::select::selection;
use crate::set::set;
use crate::show::show;
use crate::sql_type::type_identifier;
use crate::transaction::{commit, rollback, start_transaction};
use crate::truncate::truncate;
use crate::update::updating;
use crate::use_statement::use_statement;
use crate::whitespace::whitespace0;
use crate::NomSqlResult;

pub fn sql_query(dialect: Dialect) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], SqlQuery> {
    move |i| {
        // Ignore preceding whitespace or comments
        let (i, _) = whitespace0(i)?;

        // `alt` supports a maximum of 21 parsers, so we split the parser up to handle more
        let (i, o) = alt((sql_query_part1(dialect), sql_query_part2(dialect)))(i)?;
        let (i, _) = opt(statement_terminator)(i)?;
        Ok((i, o))
    }
}

fn sql_query_part1(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], SqlQuery> {
    move |i| {
        alt((
            // ordered with the most-performance sensitive at the top (that is,
            // put the stuff we'll see on the hot path up front).
            map(simple_or_compound_selection(dialect), SqlQuery::from),
            map(insertion(dialect), SqlQuery::Insert),
            map(updating(dialect), SqlQuery::Update),
            map(deletion(dialect), SqlQuery::Delete),
            map(start_transaction(dialect), SqlQuery::StartTransaction),
            map(commit(dialect), SqlQuery::Commit),
            map(rollback(dialect), SqlQuery::Rollback),
            map(set(dialect), SqlQuery::Set),
            map(deallocate(dialect), SqlQuery::Deallocate),
            map(create_table(dialect), SqlQuery::CreateTable),
            map(drop_table(dialect), SqlQuery::DropTable),
            map(drop_view(dialect), SqlQuery::DropView),
            map(view_creation(dialect), SqlQuery::CreateView),
            map(drop_cached_query(dialect), SqlQuery::DropCache),
            map(drop_all_caches, SqlQuery::DropAllCaches),
            map(drop_all_proxied_queries(), SqlQuery::DropAllProxiedQueries),
            map(alter_table_statement(dialect), SqlQuery::AlterTable),
            map(rename_table(dialect), SqlQuery::RenameTable),
            map(use_statement(dialect), SqlQuery::Use),
            map(show(dialect), SqlQuery::Show),
            alt((
                map(explain_statement(dialect), SqlQuery::Explain),
                map(create_database(dialect), SqlQuery::CreateDatabase),
                map(create_rls(dialect), SqlQuery::CreateRls),
                map(drop_rls(dialect), SqlQuery::DropRls),
            )),
        ))(i)
    }
}
fn sql_query_part2(
    dialect: Dialect,
) -> impl Fn(LocatedSpan<&[u8]>) -> NomSqlResult<&[u8], SqlQuery> {
    move |i| {
        alt((
            map(truncate(dialect), SqlQuery::Truncate),
            map(alter_readyset_statement(dialect), SqlQuery::AlterReadySet),
            // This does a more expensive clone of `i`, so process it last.
            map(create_cached_query(dialect), SqlQuery::CreateCache),
            map(comment(dialect), SqlQuery::Comment),
        ))(i)
    }
}

macro_rules! export_parser {
    ($parser: ident -> $ret:ty, $parse_bytes: ident, $parse: ident) => {
        pub fn $parse_bytes<T>(dialect: Dialect, input: T) -> Result<$ret, String>
        where
            T: AsRef<[u8]>,
        {
            match $parser(dialect)(LocatedSpan::new(input.as_ref())) {
                Ok((i, o)) if i.is_empty() => Ok(o),
                Ok((i, _)) => Err(format!(
                    "failed to parse query, expected end of input but got: '{}'",
                    String::from_utf8_lossy(&i)
                        .chars()
                        .take(16)
                        .collect::<String>()
                )),
                Err(e) => Err(format!(
                    "failed to parse query: {}",
                    Sensitive(&e.to_string())
                )),
            }
        }

        // TODO(fran): Make this function return a ReadySetResult.
        pub fn $parse<T>(dialect: Dialect, input: T) -> Result<$ret, String>
        where
            T: AsRef<str>,
        {
            $parse_bytes(dialect, input.as_ref().trim().as_bytes())
        }
    };
}

export_parser!(sql_query -> SqlQuery, parse_query_bytes, parse_query);
export_parser!(selection -> SelectStatement, parse_select_statement_bytes, parse_select_statement);
export_parser!(expression -> Expr, parse_expr_bytes, parse_expr);
export_parser!(create_table -> CreateTableStatement, parse_create_table_bytes, parse_create_table);
export_parser!(view_creation -> CreateViewStatement, parse_create_view_bytes, parse_create_view);
export_parser!(
    create_cached_query -> CreateCacheStatement,
    parse_create_cache_bytes,
    parse_create_cache
);
export_parser!(
    alter_table_statement -> AlterTableStatement,
    parse_alter_table_bytes,
    parse_alter_table
);
export_parser!(
    key_specification -> TableKey,
    parse_key_specification_bytes,
    parse_key_specification_string
);
export_parser!(
    type_identifier -> SqlType,
    parse_sql_type_bytes,
    parse_sql_type
);

#[cfg(test)]
mod tests {
    use readyset_sql::DialectDisplay;

    use super::*;

    #[test]
    fn drop_all_caches() {
        let res = parse_query(Dialect::MySQL, "drOP ALL    caCHEs").unwrap();
        assert_eq!(res, SqlQuery::DropAllCaches(DropAllCachesStatement {}));
    }

    mod mysql {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        use super::*;

        #[test]
        fn trim_query() {
            let qstring = "   INSERT INTO users VALUES (42, \"test\");     ";
            let res = parse_query(Dialect::MySQL, qstring);
            res.unwrap();
        }

        #[test]
        fn parse_byte_slice() {
            let qstring: &[u8] = b"INSERT INTO users VALUES (42, \"test\");";
            let res = parse_query_bytes(Dialect::MySQL, qstring);
            res.unwrap();
        }

        #[test]
        fn parse_byte_vector() {
            let qstring: Vec<u8> = b"INSERT INTO users VALUES (42, \"test\");".to_vec();
            let res = parse_query_bytes(Dialect::MySQL, qstring);
            res.unwrap();
        }

        #[test]
        fn hash_query() {
            let qstring = "INSERT INTO users VALUES (42, \"test\");";
            let res = parse_query(Dialect::MySQL, qstring);
            assert!(res.is_ok());

            let expected = SqlQuery::Insert(InsertStatement {
                table: Relation::from("users"),
                fields: None,
                data: vec![vec![Expr::Literal(42.into()), Expr::Literal("test".into())]],
                ignore: false,
                on_duplicate: None,
            });
            let mut h0 = DefaultHasher::new();
            let mut h1 = DefaultHasher::new();
            res.unwrap().hash(&mut h0);
            expected.hash(&mut h1);
            assert_eq!(h0.finish(), h1.finish());
        }

        #[test]
        fn format_query_with_escaped_keyword() {
            let qstring0 = "delete from articles where `key`='aaa'";
            let qstring1 = "delete from `where` where user=?";

            let expected0 = "DELETE FROM `articles` WHERE (`key` = 'aaa')";
            let expected1 = "DELETE FROM `where` WHERE (`user` = ?)";

            let res0 = parse_query(Dialect::MySQL, qstring0);
            let res1 = parse_query(Dialect::MySQL, qstring1);
            assert!(res0.is_ok());
            assert!(res1.is_ok());
            assert_eq!(expected0, res0.unwrap().display(Dialect::MySQL).to_string());
            assert_eq!(expected1, res1.unwrap().display(Dialect::MySQL).to_string());
        }

        #[test]
        fn display_select_query() {
            let qstring0 = "SELECT * FROM `users`";
            let qstring1 = "SELECT * FROM `users` AS `u`";
            let qstring2 = "SELECT `name`, `password` FROM `users` AS `u`";
            let qstring3 = "SELECT `name`, `password` FROM `users` AS `u` WHERE (`user_id` = '1')";
            let qstring4 =
            "SELECT `name`, `password` FROM `users` AS `u` WHERE ((`user` = 'aaa') AND (`password` = 'xxx'))";
            let qstring5 = "SELECT (`name` * 2) AS `double_name` FROM `users`";

            let res0 = parse_query(Dialect::MySQL, qstring0);
            let res1 = parse_query(Dialect::MySQL, qstring1);
            let res2 = parse_query(Dialect::MySQL, qstring2);
            let res3 = parse_query(Dialect::MySQL, qstring3);
            let res4 = parse_query(Dialect::MySQL, qstring4);
            let res5 = parse_query(Dialect::MySQL, qstring5);

            assert!(res0.is_ok());
            assert!(res1.is_ok());
            assert!(res2.is_ok());
            assert!(res3.is_ok());
            assert!(res4.is_ok());
            assert!(res5.is_ok());

            assert_eq!(qstring0, res0.unwrap().display(Dialect::MySQL).to_string());
            assert_eq!(qstring1, res1.unwrap().display(Dialect::MySQL).to_string());
            assert_eq!(qstring2, res2.unwrap().display(Dialect::MySQL).to_string());
            assert_eq!(qstring3, res3.unwrap().display(Dialect::MySQL).to_string());
            assert_eq!(qstring4, res4.unwrap().display(Dialect::MySQL).to_string());
            assert_eq!(qstring5, res5.unwrap().display(Dialect::MySQL).to_string());
        }

        #[test]
        fn format_select_query() {
            let qstring1 = "select * from users u";
            let qstring2 = "select name,password from users u;";
            let qstring3 = "select name,password from users u WHERE user_id='1'";

            let expected1 = "SELECT * FROM `users` AS `u`";
            let expected2 = "SELECT `name`, `password` FROM `users` AS `u`";
            let expected3 = "SELECT `name`, `password` FROM `users` AS `u` WHERE (`user_id` = '1')";

            let res1 = parse_query(Dialect::MySQL, qstring1);
            let res2 = parse_query(Dialect::MySQL, qstring2);
            let res3 = parse_query(Dialect::MySQL, qstring3);

            assert!(res1.is_ok());
            assert!(res2.is_ok());
            assert!(res3.is_ok());

            assert_eq!(expected1, res1.unwrap().display(Dialect::MySQL).to_string());
            assert_eq!(expected2, res2.unwrap().display(Dialect::MySQL).to_string());
            assert_eq!(expected3, res3.unwrap().display(Dialect::MySQL).to_string());
        }

        #[test]
        fn format_select_query_with_where_clause() {
            let qstring0 =
                "select name, password from users as u where user='aaa' and password= 'xxx'";
            let qstring1 = "select name, password from users as u where user=? and password =?";

            let expected0 =
            "SELECT `name`, `password` FROM `users` AS `u` WHERE ((`user` = 'aaa') AND (`password` = 'xxx'))";
            let expected1 =
            "SELECT `name`, `password` FROM `users` AS `u` WHERE ((`user` = ?) AND (`password` = ?))";

            let res0 = parse_query(Dialect::MySQL, qstring0);
            let res1 = parse_query(Dialect::MySQL, qstring1);
            assert!(res0.is_ok());
            assert!(res1.is_ok());
            assert_eq!(expected0, res0.unwrap().display(Dialect::MySQL).to_string());
            assert_eq!(expected1, res1.unwrap().display(Dialect::MySQL).to_string());
        }

        #[test]
        fn format_select_query_with_function() {
            let qstring1 = "select count(*) from users";
            let expected1 = "SELECT count(*) FROM `users`";

            let res1 = parse_query(Dialect::MySQL, qstring1);
            assert!(res1.is_ok());
            assert_eq!(expected1, res1.unwrap().display(Dialect::MySQL).to_string());
        }

        #[test]
        fn display_insert_query() {
            let qstring = "INSERT INTO users (name, password) VALUES ('aaa', 'xxx')";
            let expected = "INSERT INTO `users` (`name`, `password`) VALUES ('aaa', 'xxx')";
            let res = parse_query(Dialect::MySQL, qstring);
            assert!(res.is_ok());
            assert_eq!(expected, res.unwrap().display(Dialect::MySQL).to_string());
        }

        #[test]
        fn display_insert_query_no_columns() {
            let qstring = "INSERT INTO users VALUES ('aaa', 'xxx')";
            let expected = "INSERT INTO `users` VALUES ('aaa', 'xxx')";
            let res = parse_query(Dialect::MySQL, qstring);
            assert!(res.is_ok());
            assert_eq!(expected, res.unwrap().display(Dialect::MySQL).to_string());
        }

        #[test]
        fn format_insert_query() {
            let qstring = "insert into users (name, password) values ('aaa', 'xxx')";
            let expected = "INSERT INTO `users` (`name`, `password`) VALUES ('aaa', 'xxx')";
            let res = parse_query(Dialect::MySQL, qstring);
            assert!(res.is_ok());
            assert_eq!(expected, res.unwrap().display(Dialect::MySQL).to_string());
        }

        #[test]
        fn format_update_query() {
            let qstring = "update users set name=42, password='xxx' where id=1";
            let expected = "UPDATE `users` SET `name` = 42, `password` = 'xxx' WHERE (`id` = 1)";
            let res = parse_query(Dialect::MySQL, qstring);
            assert!(res.is_ok());
            assert_eq!(expected, res.unwrap().display(Dialect::MySQL).to_string());
        }

        #[test]
        fn format_delete_query_with_where_clause() {
            let qstring0 = "delete from users where user='aaa' and password= 'xxx'";
            let qstring1 = "delete from users where user=? and password =?";

            let expected0 = "DELETE FROM `users` WHERE ((`user` = 'aaa') AND (`password` = 'xxx'))";
            let expected1 = "DELETE FROM `users` WHERE ((`user` = ?) AND (`password` = ?))";

            let res0 = parse_query(Dialect::MySQL, qstring0);
            let res1 = parse_query(Dialect::MySQL, qstring1);
            assert!(res0.is_ok());
            assert!(res1.is_ok());
            assert_eq!(expected0, res0.unwrap().display(Dialect::MySQL).to_string());
            assert_eq!(expected1, res1.unwrap().display(Dialect::MySQL).to_string());
        }

        #[test]
        fn select_index_hint() {
            let base_query = "SELECT * FROM `users`";
            let index_hint_type_list: Vec<&str> = vec!["USE", "IGNORE", "FORCE"];
            let index_or_key_list: Vec<&str> = vec!["INDEX", "KEY"];
            let index_for_list: Vec<&str> = vec!["", " FOR JOIN", " FOR ORDER BY", " FOR GROUP BY"];
            let index_name_list: Vec<&str> = vec!["index_name", "primary", "index_name1"];
            for hint_type in index_hint_type_list {
                for index_or_key in index_or_key_list.iter() {
                    for index_for in index_for_list.iter() {
                        let mut formatted_index_list_str = String::new();
                        let mut index_list: Vec<SqlIdentifier> = vec![];
                        for n in index_name_list.iter() {
                            index_list.push(SqlIdentifier::from(*n));
                            if formatted_index_list_str.is_empty() {
                                formatted_index_list_str = n.to_string();
                            } else {
                                formatted_index_list_str =
                                    format!("{}, {}", formatted_index_list_str, n);
                            }
                            let index_hint_str = format!(
                                "{} {}{} ({})",
                                hint_type, index_or_key, index_for, formatted_index_list_str
                            );
                            let qstring = format!("SELECT * FROM `users` {}", index_hint_str);
                            let res = parse_query(Dialect::MySQL, &qstring);
                            assert!(res.is_ok());
                            assert_eq!(
                                base_query,
                                res.unwrap().display(Dialect::MySQL).to_string()
                            );

                            let index_hit = IndexHint {
                                hint_type: hint_type.into(),
                                index_or_key: index_or_key.into(),
                                index_usage_type: match index_for {
                                    &"" => None,
                                    _ => Some(index_for.trim().into()),
                                },
                                index_list: index_list.clone(),
                            };
                            assert_eq!(
                                index_hint_str,
                                index_hit.display(Dialect::MySQL).to_string()
                            );
                        }
                    }
                }
            }
        }
    }

    mod tests_postgres {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        use super::*;

        #[test]
        fn trim_query() {
            let qstring = "   INSERT INTO users VALUES (42, 'test');     ";
            let res = parse_query(Dialect::PostgreSQL, qstring);
            res.unwrap();
        }

        #[test]
        fn trim_comment() {
            let qstring = "-- comment \n INSERT INTO users VALUES (42, 'test');  ";
            parse_query(Dialect::PostgreSQL, qstring).unwrap();
        }

        #[test]
        fn parse_byte_slice() {
            let qstring: &[u8] = b"INSERT INTO users VALUES (42, 'test');";
            let res = parse_query_bytes(Dialect::PostgreSQL, qstring);
            res.unwrap();
        }

        #[test]
        fn parse_byte_vector() {
            let qstring: Vec<u8> = b"INSERT INTO users VALUES (42, 'test');".to_vec();
            let res = parse_query_bytes(Dialect::PostgreSQL, qstring);
            res.unwrap();
        }

        #[test]
        fn hash_query() {
            let qstring = "INSERT INTO users VALUES (42, 'test');";
            let res = parse_query(Dialect::PostgreSQL, qstring);
            assert!(res.is_ok());

            let expected = SqlQuery::Insert(InsertStatement {
                table: Relation::from("users"),
                fields: None,
                data: vec![vec![Expr::Literal(42.into()), Expr::Literal("test".into())]],
                ignore: false,
                on_duplicate: None,
            });
            let mut h0 = DefaultHasher::new();
            let mut h1 = DefaultHasher::new();
            res.unwrap().hash(&mut h0);
            expected.hash(&mut h1);
            assert_eq!(h0.finish(), h1.finish());
        }

        #[test]
        fn format_query_with_escaped_keyword() {
            let qstring0 = "delete from articles where \"key\"='aaa'";
            let qstring1 = "delete from \"where\" where user=?";

            let expected0 = "DELETE FROM \"articles\" WHERE (\"key\" = 'aaa')";
            let expected1 = "DELETE FROM \"where\" WHERE (\"user\" = ?)";

            let res0 = parse_query(Dialect::PostgreSQL, qstring0);
            let res1 = parse_query(Dialect::PostgreSQL, qstring1);
            assert!(res0.is_ok());
            assert!(res1.is_ok());
            assert_eq!(
                expected0,
                res0.unwrap().display(Dialect::PostgreSQL).to_string()
            );
            assert_eq!(
                expected1,
                res1.unwrap().display(Dialect::PostgreSQL).to_string()
            );
        }

        #[test]
        fn display_select_query() {
            let qstring0 = "SELECT * FROM \"users\"";
            let qstring1 = "SELECT * FROM \"users\" AS \"u\"";
            let qstring2 = "SELECT \"name\", \"password\" FROM \"users\" AS \"u\"";
            let qstring3 =
                "SELECT \"name\", \"password\" FROM \"users\" AS \"u\" WHERE (\"user_id\" = '1')";
            let qstring4 =
            "SELECT \"name\", \"password\" FROM \"users\" AS \"u\" WHERE ((\"user\" = 'aaa') AND (\"password\" = 'xxx'))";
            let qstring5 = "SELECT (\"name\" * 2) AS \"double_name\" FROM \"users\"";

            let res0 = parse_query(Dialect::PostgreSQL, qstring0);
            let res1 = parse_query(Dialect::PostgreSQL, qstring1);
            let res2 = parse_query(Dialect::PostgreSQL, qstring2);
            let res3 = parse_query(Dialect::PostgreSQL, qstring3);
            let res4 = parse_query(Dialect::PostgreSQL, qstring4);
            let res5 = parse_query(Dialect::PostgreSQL, qstring5);

            assert!(res0.is_ok());
            assert!(res1.is_ok());
            assert!(res2.is_ok());
            assert!(res3.is_ok());
            assert!(res4.is_ok());
            assert!(res5.is_ok());

            assert_eq!(
                qstring0,
                res0.unwrap().display(Dialect::PostgreSQL).to_string()
            );
            assert_eq!(
                qstring1,
                res1.unwrap().display(Dialect::PostgreSQL).to_string()
            );
            assert_eq!(
                qstring2,
                res2.unwrap().display(Dialect::PostgreSQL).to_string()
            );
            assert_eq!(
                qstring3,
                res3.unwrap().display(Dialect::PostgreSQL).to_string()
            );
            assert_eq!(
                qstring4,
                res4.unwrap().display(Dialect::PostgreSQL).to_string()
            );
            assert_eq!(
                qstring5,
                res5.unwrap().display(Dialect::PostgreSQL).to_string()
            );
        }

        #[test]
        fn format_select_query() {
            let qstring1 = "select * from users u";
            let qstring2 = "select name,password from users u;";
            let qstring3 = "select name,password from users u WHERE user_id='1'";

            let expected1 = "SELECT * FROM \"users\" AS \"u\"";
            let expected2 = "SELECT \"name\", \"password\" FROM \"users\" AS \"u\"";
            let expected3 =
                "SELECT \"name\", \"password\" FROM \"users\" AS \"u\" WHERE (\"user_id\" = '1')";

            let res1 = parse_query(Dialect::PostgreSQL, qstring1);
            let res2 = parse_query(Dialect::PostgreSQL, qstring2);
            let res3 = parse_query(Dialect::PostgreSQL, qstring3);

            assert!(res1.is_ok());
            assert!(res2.is_ok());
            assert!(res3.is_ok());

            assert_eq!(
                expected1,
                res1.unwrap().display(Dialect::PostgreSQL).to_string()
            );
            assert_eq!(
                expected2,
                res2.unwrap().display(Dialect::PostgreSQL).to_string()
            );
            assert_eq!(
                expected3,
                res3.unwrap().display(Dialect::PostgreSQL).to_string()
            );
        }

        #[test]
        fn format_select_query_with_where_clause() {
            let qstring0 =
                "select name, password from users as u where user='aaa' and password= 'xxx'";
            let qstring1 = "select name, password from users as u where user=? and password =?";

            let expected0 =
            "SELECT \"name\", \"password\" FROM \"users\" AS \"u\" WHERE ((\"user\" = 'aaa') AND (\"password\" = 'xxx'))";
            let expected1 =
            "SELECT \"name\", \"password\" FROM \"users\" AS \"u\" WHERE ((\"user\" = ?) AND (\"password\" = ?))";

            let res0 = parse_query(Dialect::PostgreSQL, qstring0);
            let res1 = parse_query(Dialect::PostgreSQL, qstring1);
            assert!(res0.is_ok());
            assert!(res1.is_ok());
            assert_eq!(
                expected0,
                res0.unwrap().display(Dialect::PostgreSQL).to_string()
            );
            assert_eq!(
                expected1,
                res1.unwrap().display(Dialect::PostgreSQL).to_string()
            );
        }

        #[test]
        fn format_select_query_with_function() {
            let qstring1 = "select count(*) from users";
            let expected1 = "SELECT count(*) FROM \"users\"";

            let res1 = parse_query(Dialect::PostgreSQL, qstring1);
            assert!(res1.is_ok());
            assert_eq!(
                expected1,
                res1.unwrap().display(Dialect::PostgreSQL).to_string()
            );
        }

        #[test]
        fn display_insert_query() {
            let qstring = "INSERT INTO users (name, password) VALUES ('aaa', 'xxx')";
            let expected = "INSERT INTO \"users\" (\"name\", \"password\") VALUES ('aaa', 'xxx')";
            let res = parse_query(Dialect::PostgreSQL, qstring);
            assert!(res.is_ok());
            assert_eq!(
                expected,
                res.unwrap().display(Dialect::PostgreSQL).to_string()
            );
        }

        #[test]
        fn display_insert_query_no_columns() {
            let qstring = "INSERT INTO users VALUES ('aaa', 'xxx')";
            let expected = "INSERT INTO \"users\" VALUES ('aaa', 'xxx')";
            let res = parse_query(Dialect::PostgreSQL, qstring);
            assert!(res.is_ok());
            assert_eq!(
                expected,
                res.unwrap().display(Dialect::PostgreSQL).to_string()
            );
        }

        #[test]
        fn format_insert_query() {
            let qstring = "insert into users (name, password) values ('aaa', 'xxx')";
            let expected = "INSERT INTO \"users\" (\"name\", \"password\") VALUES ('aaa', 'xxx')";
            let res = parse_query(Dialect::PostgreSQL, qstring);
            assert!(res.is_ok());
            assert_eq!(
                expected,
                res.unwrap().display(Dialect::PostgreSQL).to_string()
            );
        }

        #[test]
        fn format_update_query() {
            let qstring = "update users set name=42, password='xxx' where id=1";
            let expected =
                "UPDATE \"users\" SET \"name\" = 42, \"password\" = 'xxx' WHERE (\"id\" = 1)";
            let res = parse_query(Dialect::PostgreSQL, qstring);
            assert!(res.is_ok());
            assert_eq!(
                expected,
                res.unwrap().display(Dialect::PostgreSQL).to_string()
            );
        }

        #[test]
        fn format_delete_query_with_where_clause() {
            let qstring0 = "delete from users where user='aaa' and password= 'xxx'";
            let qstring1 = "delete from users where user=? and password =?";

            let expected0 =
                "DELETE FROM \"users\" WHERE ((\"user\" = 'aaa') AND (\"password\" = 'xxx'))";
            let expected1 = "DELETE FROM \"users\" WHERE ((\"user\" = ?) AND (\"password\" = ?))";

            let res0 = parse_query(Dialect::PostgreSQL, qstring0);
            let res1 = parse_query(Dialect::PostgreSQL, qstring1);
            assert!(res0.is_ok());
            assert!(res1.is_ok());
            assert_eq!(
                expected0,
                res0.unwrap().display(Dialect::PostgreSQL).to_string()
            );
            assert_eq!(
                expected1,
                res1.unwrap().display(Dialect::PostgreSQL).to_string()
            );
        }

        #[test]
        fn cast_to_interval() {
            assert_eq!(
                parse_query(Dialect::PostgreSQL, "SELECT '23'::interval as foo from t1").unwrap(),
                SqlQuery::Select(SelectStatement {
                    fields: vec![FieldDefinitionExpr::Expr {
                        expr: Expr::Cast {
                            expr: Box::new(Expr::Literal("23".into())),
                            ty: SqlType::Interval {
                                fields: None,
                                precision: None
                            },
                            postgres_style: true
                        },
                        alias: Some("foo".into())
                    }],
                    tables: vec![TableExpr {
                        inner: TableExprInner::Table("t1".into()),
                        alias: None,
                        index_hint: None,
                    }],
                    ..Default::default()
                })
            )
        }
    }
}
