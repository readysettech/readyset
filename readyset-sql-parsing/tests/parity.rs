//! This is not meant to provide complete test coverage that the parse is correct, but rather by
//! running these through both nom-sql and sqlparser-rs, to verify we are getting the same results
//! for some queries for which we don't otherwise have coverage. Likely, these should be replaced by
//! tests of actual relevant cache functionality.

use readyset_sql::{
    ast::{
        AlterTableDefinition, AlterTableStatement, PostgresParameterScope, SetPostgresParameter,
        SetStatement, SetVariables, SqlIdentifier, SqlQuery, Variable, VariableScope,
    },
    Dialect,
};
use readyset_sql_parsing::parse_query;

macro_rules! check_parse_mysql {
    ($sql:expr) => {
        parse_query(Dialect::MySQL, $sql).expect(&format!("Failed to parse as MySQL: {:?}", $sql))
    };
}

macro_rules! check_parse_postgres {
    ($sql:expr) => {
        parse_query(Dialect::PostgreSQL, $sql)
            .expect(&format!("Failed to parse as Postgres: {:?}", $sql))
    };
}

macro_rules! check_parse_both {
    ($sql:expr) => {
        check_parse_mysql!($sql);
        check_parse_postgres!($sql);
    };
}

#[test]
fn test_select_query_parsing() {
    let sql = "SELECT * FROM users WHERE age > 18;";
    check_parse_both!(sql);
}

#[test]
fn test_insert_query_parsing() {
    let sql = "INSERT INTO users (name, email) VALUES ('John', 'john@example.com');";
    check_parse_both!(sql);
}

#[test]
fn test_update_query_parsing() {
    let sql = "UPDATE users SET status = 'active' WHERE id = 123;";
    check_parse_both!(sql);
}

#[test]
fn test_delete_query_parsing() {
    let sql = "DELETE FROM users WHERE last_login < '2023-01-01';";
    check_parse_both!(sql);
}

#[test]
fn test_create_table_parsing() {
    let sql = "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL);";
    check_parse_both!(sql);
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
    check_parse_both!(sql);
}

#[test]
fn test_cast_with_mysql_integer_types() {
    check_parse_mysql!("SELECT CAST(123 AS SIGNED);");
    check_parse_mysql!("SELECT CAST(123 AS SIGNED INTEGER);");
    check_parse_mysql!("SELECT CAST(123 AS UNSIGNED);");
    check_parse_mysql!("SELECT CAST(123 AS UNSIGNED INTEGER);");
}

#[test]
fn test_cast_with_float_types() {
    let sql = "SELECT CAST(123.45 AS FLOAT);";
    check_parse_both!(sql);
}

#[test]
fn test_cast_with_double_types() {
    let sql = "SELECT CAST(123.45 AS DOUBLE);";
    check_parse_both!(sql);
}

#[test]
fn test_union() {
    check_parse_both!("SELECT a FROM b UNION SELECT c FROM d;");
    check_parse_both!("SELECT a FROM b UNION DISTINCT SELECT c FROM d;");
    check_parse_both!("SELECT a FROM b UNION ALL SELECT c FROM d;");
    check_parse_both!("SELECT a FROM b EXCEPT SELECT c FROM d;");
    check_parse_both!("SELECT a FROM b INTERSECT SELECT c FROM d;");
    check_parse_both!("SELECT a FROM b UNION (SELECT c FROM d LIMIT 1 OFFSET 1);");
}

#[test]
#[ignore = "nom-sql is incorrect"]
fn test_union_with_limit_offset_precedence() {
    check_parse_both!("SELECT a FROM b UNION SELECT c FROM d LIMIT 1 OFFSET 1;");
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

#[test]
fn test_mysql_multiple_variables() {
    check_parse_mysql!("SET @var1 = 1, @var2 = 2;");
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

#[test]
fn test_on_update_current_timestamp() {
    check_parse_mysql!(
        r#"CREATE TABLE users (
            id INTEGER PRIMARY KEY AUTO_INCREMENT,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP(5)
        )"#
    );
}

#[test]
fn test_deallocate() {
    check_parse_mysql!("DEALLOCATE pdo_stmt_00000001;");
}

#[test]
fn test_function_casing() {
    check_parse_both!("SELECT setval('users_uid_seq', GREATEST(MAX(uid), '1')) FROM users");
}

#[test]
fn test_select_limit_without_table() {
    check_parse_mysql!("select @@version_comment limit 1");
    check_parse_both!("select now() order by 1 limit 1");
}

#[test]
fn test_alter_drop_foreign_key() {
    assert_eq!(
        SqlQuery::AlterTable(AlterTableStatement {
            table: readyset_sql::ast::Relation {
                schema: None,
                name: "orderitem".into()
            },
            definitions: Ok(vec![AlterTableDefinition::DropForeignKey {
                name: SqlIdentifier::from("orderitem_ibfk_1"),
            }]),
            algorithm: None,
            lock: None,
            only: false,
        }),
        check_parse_mysql!("ALTER TABLE `orderitem` DROP FOREIGN KEY `orderitem_ibfk_1`")
    );
}

#[test]
fn test_set_names() {
    check_parse_mysql!("SET NAMES 'utf8mb4' COLLATE 'utf8_general_ci'");
    check_parse_postgres!("SET NAMES 'UTF8'");
}

#[test]
#[ignore = "nom-sql doesn't support SET NAMES DEFAULT"]
fn test_set_names_default() {
    check_parse_both!("SET NAMES DEFAULT");
}

#[test]
#[ignore = "nom-sql doesn't support unquoted SET NAMES"]
fn test_set_names_unquoted() {
    check_parse_mysql!("SET NAMES utf8mb4 COLLATE utf8_general_ci");
}

#[test]
fn test_limit_offset() {
    check_parse_both!("select * from users limit 10");
    check_parse_both!("select * from users limit 10 offset 10");
    check_parse_mysql!("select * from users limit 5, 10");
    check_parse_postgres!("select * from users offset 10");
}

#[test]
#[ignore = "sqlparser-rs ignores explicit LIMIT ALL"]
fn test_postgres_limit_all() {
    check_parse_postgres!("select * from users limit all");
    check_parse_postgres!("select * from users limit all offset 10");
    check_parse_postgres!("select * from users limit all");
}

#[test]
fn test_limit_offset_placeholders() {
    check_parse_mysql!("select * from users limit 10 offset ?");
    check_parse_mysql!("select * from users limit 5, ?");
    check_parse_postgres!("select * from users offset $1");
    // Would fail upstream, but nom-sql and sqlparser-rs are permissive enough to allow this:
    check_parse_mysql!("select * from users limit 5, $1");
    // Ditto, except we don't support the non-numeric $ placeholder:
    // check_parse_postgres!("select * from users offset $f");
}
