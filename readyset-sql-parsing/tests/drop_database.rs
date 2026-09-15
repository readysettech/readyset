//! `DROP DATABASE` and `DROP SCHEMA` parse only through sqlparser.

use readyset_sql::ast::{DropDatabaseStatement, SqlQuery};
use readyset_sql::{Dialect, DialectDisplay};
use readyset_sql_parsing::{ParsingPreset, parse_query_with_config};

fn parse(dialect: Dialect, sql: &str) -> SqlQuery {
    parse_query_with_config(ParsingPreset::for_prod(), dialect, sql)
        .unwrap_or_else(|e| panic!("failed to parse {sql:?}: {e}"))
}

#[test]
fn drop_database() {
    let q = parse(Dialect::MySQL, "DROP DATABASE foo");
    assert_eq!(
        q,
        SqlQuery::DropDatabase(DropDatabaseStatement {
            is_schema: false,
            if_exists: false,
            names: vec!["foo".into()],
        })
    );
    assert_eq!(q.query_type(), "DROP DATABASE");
    assert!(q.is_write());
    assert_eq!(q.display(Dialect::MySQL).to_string(), "DROP DATABASE `foo`");
}

#[test]
fn drop_schema_if_exists() {
    let q = parse(Dialect::MySQL, "DROP SCHEMA IF EXISTS `Mixed-Case`");
    assert_eq!(
        q,
        SqlQuery::DropDatabase(DropDatabaseStatement {
            is_schema: true,
            if_exists: true,
            names: vec!["Mixed-Case".into()],
        })
    );
    assert_eq!(q.query_type(), "DROP SCHEMA");
    assert_eq!(
        q.display(Dialect::MySQL).to_string(),
        "DROP SCHEMA IF EXISTS `Mixed-Case`"
    );
}

/// Postgres `DROP SCHEMA` takes a list; `CASCADE` parses but is absent from the AST, as for
/// `DROP TABLE`.
#[test]
fn drop_schema_multiple_names_cascade() {
    let q = parse(Dialect::PostgreSQL, "DROP SCHEMA a, b CASCADE");
    assert_eq!(q, parse(Dialect::PostgreSQL, "DROP SCHEMA a, b"));
    assert_eq!(
        q.display(Dialect::PostgreSQL).to_string(),
        r#"DROP SCHEMA "a", "b""#
    );
}
