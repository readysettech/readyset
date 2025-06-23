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
use readyset_sql_parsing::{parse_query_with_config, ParsingPreset};

macro_rules! check_parse_mysql {
    ($sql:expr) => {
        parse_query_with_config(ParsingPreset::BothErrorOnMismatch, Dialect::MySQL, $sql).unwrap()
    };
}

macro_rules! check_parse_postgres {
    ($sql:expr) => {
        parse_query_with_config(
            ParsingPreset::BothErrorOnMismatch,
            Dialect::PostgreSQL,
            $sql,
        )
        .unwrap()
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
        let result = parse_query_with_config(ParsingPreset::BothErrorOnMismatch, $dialect, $sql)
            .expect_err(&format!("Expected failure for {:?}: {:?}", $dialect, $sql));
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
fn test_set_names_default() {
    check_parse_both!("SET NAMES DEFAULT");
}

#[test]
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
#[should_panic(expected = "Expected: an expression, found: ?")]
fn test_mysql_placeholders_fail_in_postgres() {
    // nom-sql, again being very permissive, supports MySQL-style placeholders in PostgreSQL, but
    // sqlparser-rs does not:
    check_parse_postgres!("select * from users offset ?");
}

#[test]
#[ignore = "REA-5766: sqlparser simply removes `LIMIT ALL`"]
fn test_limit_all_with_placeholder_offset() {
    check_parse_postgres!("SELECT * FROM t WHERE x = $2 OFFSET $1");
    check_parse_postgres!("SELECT * FROM t WHERE x = $2 LIMIT ALL OFFSET $1");
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
fn test_empty_insert() {
    // MySQL parses `insert into t () VALUES ...` into `insert into t VALUES ...`
    check_parse_mysql!("INSERT INTO t VALUES ()");
}

#[test]
fn test_empty_insert_fails_in_postgres() {
    // Invalid postgres syntax, parsed by nom but not by sqlparser
    // Invalid because of empty values list
    check_parse_fails!(
        Dialect::PostgreSQL,
        "INSERT INTO t VALUES ()",
        "sqlparser error"
    );
    // Invalid because of empty cols list, accepted by mysql thought
    check_parse_fails!(
        Dialect::PostgreSQL,
        "INSERT INTO t () VALUES ()",
        "sqlparser error"
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
}

#[test]
fn test_op_any_subquery_unsupported() {
    check_parse_fails!(
        Dialect::PostgreSQL,
        "SELECT * FROM t WHERE a >= ANY(SELECT b FROM t2)",
        "nom-sql error"
    );
}

#[test]
fn test_op_all_subquery_unsupported() {
    check_parse_fails!(
        Dialect::PostgreSQL,
        "SELECT * FROM t WHERE a = ALL(SELECT b FROM t2)",
        "nom-sql error"
    );
}

#[test]
fn test_substring() {
    check_parse_both!("SELECT substring('foo', 1, 2) FROM t");
    check_parse_both!("SELECT substr('foo', 1, 2) FROM t");
    check_parse_both!("SELECT substring('foo' FROM 1 FOR 2) FROM t");
    check_parse_both!("SELECT substr('foo' FROM 1 FOR 2) FROM t");
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
/// Invalid postgres syntax, parsed by nom but not by sqlparser
fn test_empty_insert_fields_fails_in_postgres() {
    // Invalid because of empty cols list, accepted by mysql though
    check_parse_fails!(
        Dialect::PostgreSQL,
        "INSERT INTO t () VALUES ()",
        "sqlparser error"
    );
}

#[test]
fn test_point_columns() {
    check_parse_mysql!("CREATE TABLE t (p POINT)");
}

#[test]
fn test_point_columns_srid() {
    check_parse_mysql!("CREATE TABLE t (p POINT SRID 4326)");
}

#[test]
fn test_column_spec_charset_collation_quotation() {
    check_parse_mysql!(
        "CREATE TABLE t (a VARCHAR(10) CHARACTER SET 'utf8mb4' COLLATE 'utf8mb4_unicode_ci')"
    );
}

#[test]
fn test_psql_lower_upper_collations() {
    check_parse_postgres!(r#"SELECT lower('A' COLLATE "en_US.utf8");"#);
    check_parse_postgres!(r#"SELECT upper('a' COLLATE "en_US.utf8");"#);
}

#[test]
fn test_wildcard_various_positions() {
    check_parse_mysql!("SELECT * FROM t WHERE a = ?;");
    check_parse_mysql!("SELECT t.* FROM t WHERE t.id = ?;");
    check_parse_mysql!("SELECT *, id FROM users;");
    check_parse_mysql!("SELECT * FROM (SELECT * FROM users) AS sub;");
    check_parse_mysql!("SELECT COUNT(*) FROM users;");
}

#[test]
fn test_charset_introducers() {
    check_parse_mysql!("SELECT _utf8'hello';");
    check_parse_mysql!("SELECT _utf8mb4'hello' AS greeting;");
    check_parse_mysql!("SELECT _binary'123';");
}

#[test]
fn test_psql_escape_literal() {
    check_parse_postgres!("SELECT E'\\n';");
}

#[test]
fn test_compound_select_cases() {
    check_parse_both!(
        "WITH cte1 AS (SELECT id FROM users), cte2 AS (SELECT id FROM admins)
        SELECT * FROM cte1 UNION SELECT * FROM cte2;"
    );
    check_parse_both!("SELECT id FROM users UNION SELECT id FROM admins;");
    check_parse_both!("SELECT name FROM employees UNION ALL SELECT name FROM contractors ORDER BY name LIMIT 5 OFFSET 2;");
    check_parse_both!("SELECT 1 EXCEPT SELECT 2 LIMIT 1;");
    check_parse_both!(
        "WITH cte AS (SELECT id FROM users) SELECT id FROM cte UNION SELECT id FROM admins;"
    );
    check_parse_fails!(
        Dialect::PostgreSQL,
        "(SELECT a FROM t1 INTERSECT SELECT a FROM t2) ORDER BY a;",
        "NomSqlError"
    );
}

#[test]
fn test_fk_index_name() {
    check_parse_mysql!(
        r#"CREATE TABLE child_table (
            id INT PRIMARY KEY,
            parent_id INT,
            CONSTRAINT fk_child_parent
                FOREIGN KEY idx_parent_id (parent_id)
                REFERENCES parent_table(id)
        );"#
    );
}

#[test]
fn test_tablekey_key_variant() {
    check_parse_fails!(
        Dialect::MySQL,
        "ALTER TABLE t ADD CONSTRAINT c KEY key_name (t1.c1, t2.c2) USING BTREE",
        "sqlparser error"
    );
    check_parse_fails!(
        Dialect::PostgreSQL,
        "ALTER TABLE t ADD CONSTRAINT c KEY key_name (t1.c1, t2.c2) USING BTREE",
        "sqlparser error"
    );
    // why does nom only parse this in psql? it seems closer to mysql syntax
    check_parse_fails!(
        Dialect::MySQL,
        r#"CREATE TABLE "comments" (
            "id" int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY,
            "hat_id" int,
            fulltext INDEX "index_comments_on_comment" ("comment"),
            INDEX "confidence_idx" ("confidence"),
            UNIQUE ("short_id"),
            INDEX "story_id_short_id" ("story_id", "short_id"),
            INDEX "thread_id" ("thread_id"),
            INDEX "index_comments_on_user_id" ("user_id")
        )
        ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"#,
        "sqlparser error"
    );
}

#[test]
fn test_create_table_with_data_directory_option() {
    check_parse_mysql!(
        r#"CREATE TABLE comments (
            id INT PRIMARY KEY,
            comment TEXT
        )
        DATA DIRECTORY = '/var/lib/mysql-files'
        ENGINE=InnoDB
        DEFAULT CHARSET=utf8mb4;"#
    );
}

#[test]
fn test_row() {
    check_parse_both!(r#"SELECT ROW(1, 2, 3);"#);
}

#[test]
fn test_explain_materialization() {
    check_parse_both!("EXPLAIN MATERIALIZATIONS");
}

#[test]
fn point_types() {
    check_parse_mysql!("CREATE TABLE t (p POINT)");
    check_parse_postgres!("CREATE TABLE t (p GEOMETRY(POINT))");
}

#[test]
fn test_trailing_semicolons() {
    check_parse_both!(r#"SELECT 1;;"#);
}

#[test]
fn test_multiple_statements() {
    check_parse_fails!(Dialect::MySQL, r#"SELECT 1; SELECT 2;"#, "EOF");
    check_parse_fails!(Dialect::PostgreSQL, r#"SELECT 1; SELECT 2;"#, "EOF");
}

#[test]
fn test_key_part_prefix() {
    check_parse_mysql!(
        r#"CREATE TABLE comments (
            id INT,
            comment TEXT,
            FULLTEXT INDEX index_comments_on_comment_prefix (comment(50))
        );"#
    );
    check_parse_mysql!(
        r#"ALTER TABLE comments ADD FULLTEXT INDEX index_comments_on_comment_prefix (comment(50));"#
    ); // nom-sql doesn't parse `CREATE INDEX`
    check_parse_fails!(
        Dialect::PostgreSQL,
        r#"CREATE INDEX idx_jsonblob_id ON blobs (CAST(jsonblob->>'id' AS INTEGER));"#,
        "nom-sql: failed to parse query"
    );
}

#[test]
fn test_key_part_expression() {
    check_parse_fails!(
        Dialect::MySQL,
        r#"CREATE TABLE blobs (
            jsonblob JSON,
            INDEX index_blobs_on_id ((CAST(jsonblob->>'$.id' AS UNSIGNED)))
        );"#,
        "non-column expression"
    );
    // We should throw away the unsupported part and succeed overall
    match check_parse_mysql!(
        r#"ALTER TABLE blobs ADD INDEX index_blobs_on_id ((CAST(jsonblob->>"$.id" AS UNSIGNED)));"#
    ) {
        SqlQuery::AlterTable(AlterTableStatement {
            table, definitions, ..
        }) => {
            assert_eq!(table.name, "blobs");
            assert!(
                definitions.is_err(),
                "Expected unsupported definitions: {definitions:?}"
            )
        }
        stmt => panic!("expected AlterTableStatement, got {stmt:?}"),
    };
    // nom-sql doesn't parse `CREATE INDEX`
    check_parse_fails!(
        Dialect::PostgreSQL,
        r#"CREATE INDEX idx_jsonblob_id ON blobs (CAST(jsonblob->>'id' AS INTEGER));"#,
        "nom-sql: failed to parse query"
    );
}
