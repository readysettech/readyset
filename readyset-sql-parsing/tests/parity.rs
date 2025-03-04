//! This is not meant to provide complete test coverage that the parse is correct, but rather by
//! running these through both nom-sql and sqlparser-rs, to verify we are getting the same results
//! for some queries for which we don't otherwise have coverage. Likely, these should be replaced by
//! tests of actual relevant cache functionality.

use readyset_sql::{
    ast::{
        PostgresParameterScope, SetPostgresParameter, SetStatement, SetVariables, SqlQuery,
        Variable, VariableScope,
    },
    Dialect,
};
use readyset_sql_parsing::parse_query;

fn check_parse_mysql(sql: &str) -> SqlQuery {
    parse_query(Dialect::MySQL, sql).expect("Failed to parse as MySQL")
}

fn check_parse_postgres(sql: &str) -> SqlQuery {
    parse_query(Dialect::PostgreSQL, sql).expect("Failed to parse as Postgres")
}

fn check_parse_both(sql: &str) {
    check_parse_mysql(sql);
    check_parse_postgres(sql);
}

#[test]
fn test_select_query_parsing() {
    let sql = "SELECT * FROM users WHERE age > 18;";
    check_parse_both(sql);
}

#[test]
fn test_insert_query_parsing() {
    let sql = "INSERT INTO users (name, email) VALUES ('John', 'john@example.com');";
    check_parse_both(sql);
}

#[test]
fn test_update_query_parsing() {
    let sql = "UPDATE users SET status = 'active' WHERE id = 123;";
    check_parse_both(sql);
}

#[test]
fn test_delete_query_parsing() {
    let sql = "DELETE FROM users WHERE last_login < '2023-01-01';";
    check_parse_both(sql);
}

#[test]
fn test_create_table_parsing() {
    let sql = "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL);";
    check_parse_both(sql);
}

#[test]
fn test_complex_join_parsing() {
    let sql = "
            SELECT u.name, o.order_date
            FROM users u
            INNER JOIN orders o ON u.id = o.user_id
            WHERE o.status = 'completed'
            ORDER BY o.order_date DESC;
        ";
    check_parse_both(sql);
}

#[test]
fn test_cast_with_mysql_integer_types() {
    check_parse_mysql("SELECT CAST(123 AS SIGNED);");
    check_parse_mysql("SELECT CAST(123 AS SIGNED INTEGER);");
    check_parse_mysql("SELECT CAST(123 AS UNSIGNED);");
    check_parse_mysql("SELECT CAST(123 AS UNSIGNED INTEGER);");
}

#[test]
fn test_cast_with_float_types() {
    let sql = "SELECT CAST(123.45 AS FLOAT);";
    check_parse_both(sql);
}

#[test]
fn test_cast_with_double_types() {
    let sql = "SELECT CAST(123.45 AS DOUBLE);";
    check_parse_both(sql);
}

#[test]
#[ignore = "Fix UNION conversion"]
fn test_union() {
    check_parse_both("SELECT a FROM b UNION SELECT c FROM d;");
}

fn check_mysql_variable_scope(sql: &str, expected_scope: VariableScope) {
    match parse_query(Dialect::MySQL, sql).expect("Failed to parse query") {
        SqlQuery::Set(SetStatement::Variable(SetVariables { variables })) => {
            let (variable, _expr) = variables
                .first()
                .expect("Expected at least one variable assignment");
            let Variable { scope, .. } = variable;
            assert_eq!(*scope, expected_scope, "input: {sql:?}");
        }
        query => panic!("Expected SET statement, got {query:?}"),
    }
}

#[test]
#[ignore = "Fix sqlparser variable scopes"]
fn test_mysql_variable_scope() {
    check_mysql_variable_scope("SET var = 1;", VariableScope::Local);
    check_mysql_variable_scope("SET @var = 1;", VariableScope::User);
    check_mysql_variable_scope("SET @@var = 1;", VariableScope::Session);
    check_mysql_variable_scope("SET @@session.var = 1;", VariableScope::Session);
    check_mysql_variable_scope("SET @@local.var = 1;", VariableScope::Local);
    check_mysql_variable_scope("SET @@global.var = 1;", VariableScope::Global);
    check_mysql_variable_scope("SET LOCAL var = 1;", VariableScope::Local);
    check_mysql_variable_scope("SET SESSION var = 1;", VariableScope::Session);
    check_mysql_variable_scope("SET GLOBAL var = 1;", VariableScope::Global);
}

fn check_postgres_variable_scope(sql: &str, expected_scope: Option<PostgresParameterScope>) {
    match parse_query(Dialect::PostgreSQL, sql).expect("Failed to parse query") {
        SqlQuery::Set(SetStatement::PostgresParameter(SetPostgresParameter { scope, .. })) => {
            assert_eq!(scope, expected_scope, "input: {sql:?}");
        }
        query => panic!("Expected SET statement, got {query:?}"),
    }
}

#[test]
#[ignore = "Fix sqlparser variable scopes"]
fn test_postgres_variable_scope() {
    check_postgres_variable_scope("SET var = 1;", None);
    check_postgres_variable_scope("SET LOCAL var = 1;", Some(PostgresParameterScope::Local));
    check_postgres_variable_scope(
        "SET SESSION var = 1;",
        Some(PostgresParameterScope::Session),
    );
}
