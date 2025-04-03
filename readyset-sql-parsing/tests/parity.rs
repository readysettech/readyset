//! This is not meant to provide complete test coverage that the parse is correct, but rather by
//! running these through both nom-sql and sqlparser-rs, to verify we are getting the same results
//! for some queries for which we don't otherwise have coverage. Likely, these should be replaced by
//! tests of actual relevant cache functionality.

use readyset_sql::{
    ast::{
        AlterTableDefinition, AlterTableStatement, PostgresParameterScope, SetPostgresParameter,
        SetStatement, SetVariables, SqlIdentifier, SqlQuery, VariableScope,
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

macro_rules! check_parse_fails {
    ($dialect:expr, $sql:expr, $expected_error:expr) => {
        let result = parse_query($dialect, $sql)
            .expect_err(&format!("Expected failure for MySQL: {:?}", $sql));
        assert!(
            result.to_string().contains($expected_error),
            "Expected error '{}' not found: got {}",
            $expected_error,
            result
        );
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
fn test_union_with_limit_offset_precedence() {
    check_parse_both!("SELECT a FROM b UNION SELECT c FROM d LIMIT 1 OFFSET 1;");
    check_parse_both!("SELECT a FROM b UNION (SELECT c FROM d LIMIT 1 OFFSET 1);");
}

fn check_mysql_variable_scope(sql: &str, expected_scopes: &[VariableScope]) {
    match check_parse_mysql!(sql) {
        SqlQuery::Set(SetStatement::Variable(SetVariables { variables })) => {
            let scopes = variables
                .iter()
                .map(|(variable, _expr)| variable.scope)
                .collect::<Vec<_>>();
            assert_eq!(&scopes, expected_scopes, "input: {sql:?}");
        }
        query => panic!("Expected SET statement, got {query:?}"),
    }
}

#[test]
fn test_mysql_variable_scope() {
    check_mysql_variable_scope("SET var = 1;", &[VariableScope::Local]);
    check_mysql_variable_scope("SET @var = 1;", &[VariableScope::User]);
    check_mysql_variable_scope("SET @@var = 1;", &[VariableScope::Session]);
    check_mysql_variable_scope("SET @@session.var = 1;", &[VariableScope::Session]);
    check_mysql_variable_scope("SET @@local.var = 1;", &[VariableScope::Local]);
    check_mysql_variable_scope("SET @@global.var = 1;", &[VariableScope::Global]);
    check_mysql_variable_scope("SET LOCAL var = 1;", &[VariableScope::Local]);
    check_mysql_variable_scope("SET SESSION var = 1;", &[VariableScope::Session]);
    check_mysql_variable_scope("SET GLOBAL var = 1;", &[VariableScope::Global]);
}

#[test]
fn test_mysql_multiple_variables() {
    let sql = "SET @var1 = 1, @@global.var2 = 2, @@session.var3 = 3, @@local.var4 = 4, LOCAL var5 = 5, SESSION var6 = 6, GLOBAL var7 = 7;";
    let expected_scopes = vec![
        VariableScope::User,
        VariableScope::Global,
        VariableScope::Session,
        VariableScope::Local,
        VariableScope::Local,
        VariableScope::Session,
        VariableScope::Global,
    ];
    check_mysql_variable_scope(sql, &expected_scopes);
}

fn check_postgres_variable_scope(sql: &str, expected_scope: Option<PostgresParameterScope>) {
    match check_parse_postgres!(sql) {
        SqlQuery::Set(SetStatement::PostgresParameter(SetPostgresParameter { scope, .. })) => {
            assert_eq!(scope, expected_scope, "input: {sql:?}");
        }
        query => panic!("Expected SET statement, got {query:?}"),
    }
}

#[test]
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
    check_parse_postgres!("select * from users offset :1");
}

#[test]
fn test_limit_offset_placeholders_should_fail() {
    // Should fail, but both nom-sql and sqlparser-rs are apparently pretty permissive and allow
    // dollar and colon placeholders in both MySQL and PostgreSQL:
    check_parse_mysql!("select * from users limit 5, $1");
    check_parse_mysql!("select * from users limit 5, :1");

    // Actually fails:
    check_parse_fails!(
        Dialect::PostgreSQL,
        "select * from users offset $f",
        "NomSqlError"
    );
}

#[test]
#[cfg(feature = "sqlparser")]
#[should_panic(expected = "Expected: an expression, found: ?")]
fn test_mysql_placeholders_fail_in_postgres() {
    // nom-sql, again being very permissive, supports MySQL-style placeholders in PostgreSQL, but
    // sqlparser-rs does not:
    check_parse_postgres!("select * from users offset ?");
}

#[test]
fn test_not_like_expressions() {
    check_parse_both!("SELECT * FROM t WHERE a NOT LIKE 'foo';");
    check_parse_both!("SELECT * FROM t WHERE a NOT ILIKE 'foo';");
}

#[test]
fn test_comment_on_table() {
    check_parse_postgres!(
        r#"COMMENT ON TABLE "config" IS 'The base table for configuration data.'"#
    );
}

#[test]
fn test_comment_on_column() {
    check_parse_postgres!(
        r#"COMMENT ON COLUMN "config"."id" IS 'The unique identifier for the configuration.'"#
    );
}

#[test]
fn test_convert_mysql_style() {
    check_parse_mysql!("SELECT CONVERT('foo', CHAR)");
}

#[test]
fn test_convert_with_using() {
    check_parse_fails!(
        Dialect::MySQL,
        "SELECT CONVERT('foo' USING latin1)",
        "NomSqlError"
    );
}

#[test]
fn test_column_default_without_parens() {
    check_parse_both!(
        "CREATE TABLE IF NOT EXISTS m (version VARCHAR(50) PRIMARY KEY NOT NULL, run_on TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP);"
    );
}

#[test]
fn test_op_any_all() {
    check_parse_postgres!("SELECT * FROM t WHERE 'abc' NOT ILIKE ANY('{\"aBC\"}')");
    check_parse_postgres!("SELECT * FROM t WHERE 'abc' ILIKE all('{\"aBC\"}')");

    check_parse_fails!(
        Dialect::PostgreSQL,
        "SELECT * FROM t WHERE a >= ANY(SELECT b FROM t2)",
        "NomSqlError"
    );
    check_parse_fails!(
        Dialect::PostgreSQL,
        "SELECT * FROM t WHERE a = ALL(SELECT b FROM t2)",
        "NomSqlError"
    );
}

#[test]
fn test_substring() {
    check_parse_both!("SELECT SUBSTRING('foo', 1, 2) FROM t");
    check_parse_both!("SELECT SUBSTR('foo', 1, 2) FROM t");
    check_parse_both!("SELECT SUBSTRING('foo' FROM 1 FOR 2) FROM t");
    check_parse_both!("SELECT SUBSTR('foo' FROM 1 FOR 2) FROM t");
}

#[test]
fn test_empty_insert_fields() {
    // psql doesn't allow `VALUES ()`
    check_parse_mysql!("INSERT INTO t VALUES ()");
    // psql doesn't allow empty cols list
    check_parse_mysql!("INSERT INTO t () VALUES ()");

    // normal inserts
    check_parse_both!("INSERT INTO t VALUES (1,2)");
    check_parse_both!("INSERT INTO t (a,b) VALUES (1,2)");
}

#[test]
fn test_alter_table_rename_col() {
    check_parse_both!("ALTER TABLE t RENAME COLUMN a TO b");

    // COLUMN keyword is optional in psql, but required in mysql
    check_parse_postgres!("ALTER TABLE t RENAME a TO b");
}

#[test]
fn test_unsupported_op() {
    // Even though the operator is not supported, we store the err
    // in the CreateCacheStatement. We error when creating the cache/lowering.
    //
    // This operator is psql-specific
    check_parse_postgres!("CREATE CACHE FROM SELECT * FROM t WHERE a ~* 'f.*'");
    check_parse_postgres!("create cache from selecT * from t where a !~* 'f.*' and a ~ 'g.*'");
}

#[test]
#[cfg(feature = "sqlparser")]
#[should_panic(expected = "sqlparser error")]
/// Invalid postgres syntax, parsed by nom but not by sqlparser
fn test_empty_insert_fails_in_postgres() {
    // Invalid because of empty values list
    check_parse_postgres!("INSERT INTO t VALUES ()");
}

#[test]
#[cfg(feature = "sqlparser")]
#[should_panic(expected = "sqlparser error")]
/// Invalid postgres syntax, parsed by nom but not by sqlparser
fn test_empty_insert_fields_fails_in_postgres() {
    // Invalid because of empty cols list, accepted by mysql though
    check_parse_postgres!("INSERT INTO t () VALUES ()");
}
