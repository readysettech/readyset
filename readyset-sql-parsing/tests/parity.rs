//! This is not meant to provide complete test coverage that the parse is correct, but rather by
//! running these through both nom-sql and sqlparser-rs, to verify we are getting the same results
//! for some queries for which we don't otherwise have coverage. Likely, these should be replaced by
//! tests of actual relevant cache functionality.

use readyset_sql::{
    Dialect, DialectDisplay,
    ast::{
        AlterTableDefinition, AlterTableStatement, Expr, FieldDefinitionExpr, IndexKeyPart,
        Literal, MySQLColumnPosition, PostgresParameterScope, SelectStatement,
        SetPostgresParameter, SetStatement, SetVariables, SqlIdentifier, SqlQuery, VariableScope,
    },
};
use readyset_sql_parsing::{ParsingPreset, parse_query_with_config};

#[test]
fn select_query_parsing() {
    let sql = "SELECT * FROM users WHERE age > 18;";
    check_parse_both!(sql);
}

#[test]
fn insert_query_parsing() {
    let sql = "INSERT INTO users (name, email) VALUES ('John', 'john@example.com');";
    check_parse_both!(sql);
}

#[test]
fn update_query_parsing() {
    let sql = "UPDATE users SET status = 'active' WHERE id = 123;";
    check_parse_both!(sql);
}

#[test]
fn delete_query_parsing() {
    let sql = "DELETE FROM users WHERE last_login < '2023-01-01';";
    check_parse_both!(sql);
}

#[test]
fn create_table_parsing() {
    let sql = "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL);";
    check_parse_both!(sql);
}

#[test]
fn complex_join_parsing() {
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
fn lateral_join_parsing() {
    check_parse_both!("
            SELECT u.name, o.order_date
            FROM users u, LATERAL (SELECT o.order_date, o.status from orders o WHERE o.user_id = u.id) o
            WHERE o.status = 'completed'
            ORDER BY o.order_date DESC;
        ");

    check_parse_both!(
        "
            SELECT u.name, o.order_date
            FROM users u,
            LATERAL (SELECT o.order_date, o.status from orders o join
                        lateral (select u.name from users u where u.id = o.user_id) u1 on o.status = u.name
                     WHERE o.user_id = u.id) o join
            LATERAL (SELECT o.status from orders o,
                        lateral (select u.id from users u where u.id = o.user_id) u1
                     WHERE o.user_id = u1.id) o1 on o.status = o1.status
            WHERE o.status = 'completed'
            ORDER BY o.order_date DESC;
        "
    );
}

#[test]
fn cross_join() {
    check_parse_both!("CREATE CACHE FROM SELECT qa.j.jn FROM qa.j CROSS JOIN qa.spj ORDER BY 1;");
    check_parse_both!(
        "CREATE CACHE FROM
        SELECT Test_INTEGER, Test_INTEGER2
        FROM qa.DataTypes TDT
        CROSS JOIN LATERAL (
            SELECT * FROM qa.DataTypes3 DT WHERE TDT.Test_INTEGER = DT.Test_INTEGER2
        ) T1 ORDER BY 1, 2;"
    );
}

#[test]
fn cast_with_mysql_integer_types() {
    check_parse_mysql!("SELECT CAST(123 AS SIGNED);");
    check_parse_mysql!("SELECT CAST(123 AS SIGNED INTEGER);");
    check_parse_mysql!("SELECT CAST(123 AS UNSIGNED);");
    check_parse_mysql!("SELECT CAST(123 AS UNSIGNED INTEGER);");
}

#[test]
fn cast_with_float_types() {
    let sql = "SELECT CAST(123.45 AS FLOAT);";
    check_parse_both!(sql);
}

#[test]
fn cast_with_double_types() {
    let sql = "SELECT CAST(123.45 AS DOUBLE);";
    check_parse_both!(sql);
}

/// MySQL CAST with ARRAY syntax should parse successfully (via sqlparser only, nom-sql doesn't
/// support this syntax). The actual unsupported error is raised during dataflow lowering.
#[test]
fn mysql_cast_with_array_parses() {
    // This syntax is MySQL-specific and only parsed by sqlparser, not nom-sql
    let sql = "SELECT CAST(x AS SIGNED ARRAY) FROM t";
    let result = parse_query_with_config(ParsingPreset::OnlySqlparser, Dialect::MySQL, sql);
    assert!(
        result.is_ok(),
        "CAST with ARRAY should parse: {:?}",
        result.err()
    );

    // Verify the AST has array=true
    let query = result.unwrap();
    if let SqlQuery::Select(SelectStatement { fields, .. }) = query {
        if let FieldDefinitionExpr::Expr { expr, .. } = &fields[0] {
            if let Expr::Cast { array, .. } = expr {
                assert!(*array, "Cast should have array=true");
            } else {
                panic!("Expected Cast expression");
            }
        } else {
            panic!("Expected Expr field");
        }
    } else {
        panic!("Expected Select query");
    }
}

/// Verify that CAST with ARRAY round-trips through display correctly.
#[test]
fn mysql_cast_with_array_roundtrip() {
    let sql = "SELECT CAST(x AS SIGNED ARRAY) FROM t";
    let query = parse_query_with_config(ParsingPreset::OnlySqlparser, Dialect::MySQL, sql)
        .expect("CAST with ARRAY should parse");

    let displayed = query.display(Dialect::MySQL).to_string();
    assert!(
        displayed.contains("ARRAY"),
        "Round-trip should preserve ARRAY syntax: {}",
        displayed
    );
}

/// MySQL CREATE TABLE with functional index using CAST...ARRAY should parse the CAST...ARRAY
/// syntax successfully. The functional index expression is preserved in the AST as an
/// `IndexKeyPart::Expr` variant.
#[test]
fn mysql_create_table_with_cast_array_functional_index() {
    // Functional index columns (like CAST...ARRAY) should be preserved as IndexKeyPart::Expr
    let sql = "CREATE TABLE t (j JSON, INDEX idx1 ((CAST(j->'$.id' AS UNSIGNED ARRAY))))";
    let result = parse_query_with_config(ParsingPreset::OnlySqlparser, Dialect::MySQL, sql);
    let query = result.expect("Functional index should parse successfully");

    // Verify the functional index is preserved
    match query {
        SqlQuery::CreateTable(stmt) => {
            let body = stmt.body.expect("Should have parsed body");
            let keys = body.keys.expect("Should have keys with functional index");
            assert_eq!(keys.len(), 1, "Should have one key");
            let key_parts = keys[0].get_key_parts().expect("Should have key parts");
            assert_eq!(key_parts.len(), 1, "Should have one key part");
            assert!(
                matches!(key_parts[0], IndexKeyPart::Expr(_)),
                "Key part should be an expression: {:?}",
                key_parts[0]
            );
        }
        _ => panic!("Expected CreateTable, got {:?}", query),
    }
}

#[test]
fn mysql_create_table_with_mixed_functional_index() {
    // When an index has both regular and functional columns, both are preserved
    let sql =
        "CREATE TABLE t (id INT, j JSON, INDEX idx1 (id, (CAST(j->'$.id' AS UNSIGNED ARRAY))))";
    let result = parse_query_with_config(ParsingPreset::OnlySqlparser, Dialect::MySQL, sql);
    let query = result.expect("Mixed functional index should parse successfully");

    match query {
        SqlQuery::CreateTable(stmt) => {
            let body = stmt.body.expect("Should have parsed body");
            let keys = body.keys.expect("Should have keys");
            assert_eq!(keys.len(), 1, "Should have one key");
            let key_parts = keys[0].get_key_parts().expect("Should have key parts");
            assert_eq!(key_parts.len(), 2, "Should have two key parts");
            assert!(
                matches!(&key_parts[0], IndexKeyPart::Column(c) if c.name.as_str() == "id"),
                "First key part should be column 'id': {:?}",
                key_parts[0]
            );
            assert!(
                matches!(&key_parts[1], IndexKeyPart::Expr(_)),
                "Second key part should be an expression: {:?}",
                key_parts[1]
            );
            // get_columns() should return only simple columns for backward compatibility
            let key_cols = keys[0].get_columns();
            assert_eq!(
                key_cols.len(),
                1,
                "get_columns() should return only simple columns"
            );
            assert_eq!(key_cols[0].name.as_str(), "id");
        }
        _ => panic!("Expected CreateTable, got {:?}", query),
    }
}

#[test]
fn union() {
    check_parse_both!("SELECT a FROM b UNION SELECT c FROM d;");
    check_parse_both!("SELECT a FROM b UNION DISTINCT SELECT c FROM d;");
    check_parse_both!("SELECT a FROM b UNION ALL SELECT c FROM d;");
    check_parse_both!("SELECT a FROM b EXCEPT SELECT c FROM d;");
    check_parse_both!("SELECT a FROM b INTERSECT SELECT c FROM d;");
    check_parse_both!("SELECT a FROM b UNION (SELECT c FROM d LIMIT 1 OFFSET 1);");
}

#[test]
fn union_with_limit_offset_precedence() {
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
fn mysql_variable_scope() {
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
fn mysql_multiple_variables() {
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
fn postgres_variable_scope() {
    check_postgres_variable_scope("SET var = 1;", None);
    check_postgres_variable_scope("SET LOCAL var = 1;", Some(PostgresParameterScope::Local));
    check_postgres_variable_scope(
        "SET SESSION var = 1;",
        Some(PostgresParameterScope::Session),
    );
}

#[test]
fn on_update_current_timestamp() {
    check_parse_mysql!(
        r#"CREATE TABLE users (
            id INTEGER PRIMARY KEY AUTO_INCREMENT,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP(5)
        )"#
    );
}

#[test]
fn deallocate() {
    check_parse_both!("DEALLOCATE pdo_stmt_00000001;");
    check_parse_mysql!("DEALLOCATE `pdo_stmt_00000001`;");
    check_parse_mysql!("DEALLOCATE PREPARE pdo_stmt_00000001;");
    check_parse_mysql!("DEALLOCATE PREPARE `pdo_stmt_00000001`;");
    check_parse_postgres!(r#"DEALLOCATE "pdo_stmt_00000001";"#);
    check_parse_postgres!(r"DEALLOCATE ALL;");
}

#[test]
fn function_casing() {
    check_parse_both!("SELECT setval('users_uid_seq', GREATEST(MAX(uid), '1')) FROM users");
}

#[test]
fn select_limit_without_table() {
    check_parse_mysql!("select @@version_comment limit 1");
    check_parse_both!("select now() order by 1 limit 1");
}

#[test]
fn alter_drop_foreign_key() {
    assert_eq!(
        SqlQuery::AlterTable(AlterTableStatement {
            table: readyset_sql::ast::Relation {
                schema: None,
                name: "orderitem".into()
            },
            definitions: Ok(vec![AlterTableDefinition::DropForeignKey {
                name: SqlIdentifier::from("orderitem_ibfk_1"),
            }]),
            only: false,
        }),
        check_parse_mysql!("ALTER TABLE `orderitem` DROP FOREIGN KEY `orderitem_ibfk_1`")
    );
}

#[test]
fn set_names() {
    check_parse_mysql!("SET NAMES 'utf8mb4' COLLATE 'utf8_general_ci'");
    check_parse_postgres!("SET NAMES 'UTF8'");
}

#[test]
fn set_names_default() {
    check_parse_both!("SET NAMES DEFAULT");
}

#[test]
fn set_names_unquoted() {
    check_parse_mysql!("SET NAMES utf8mb4 COLLATE utf8_general_ci");
}

#[test]
fn limit_offset() {
    check_parse_both!("select * from users limit 10");
    check_parse_both!("select * from users limit 10 offset 10");
    check_parse_mysql!("select * from users limit 5, 10");
    check_parse_postgres!("select * from users offset 10");
}

#[test]
#[ignore = "sqlparser-rs ignores explicit LIMIT ALL"]
fn postgres_limit_all() {
    check_parse_postgres!("select * from users limit all");
    check_parse_postgres!("select * from users limit all offset 10");
    check_parse_postgres!("select * from users limit all");
}

#[test]
fn limit_offset_placeholders() {
    check_parse_mysql!("select * from users limit 10 offset ?");
    check_parse_mysql!("select * from users limit 5, ?");
    check_parse_postgres!("select * from users offset $1");
    check_parse_postgres!("select * from users offset :1");
}

#[test]
fn limit_placeholders() {
    check_parse_mysql!("select * from users limit ?");
    check_parse_mysql!("select * from users limit $1");
    check_parse_postgres!("select * from users limit $1");
    check_parse_postgres!("select * from users limit :1");
}

#[test]
fn limit_placeholders_create_cache() {
    check_parse_mysql!("create cache from select * from users limit ?");
    check_parse_mysql!("create cache from select * from users limit $1");
    check_parse_postgres!("create cache from select * from users limit $1");
    check_parse_postgres!("create cache from select * from users limit :1");
}

#[test]
fn limit_offset_placeholders_should_fail() {
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
fn mysql_placeholders_fail_in_postgres() {
    // nom-sql, again being very permissive, supports MySQL-style placeholders in PostgreSQL, but
    // sqlparser-rs does not:
    check_parse_postgres!("select * from users offset ?");
}

#[test]
#[ignore = "REA-5766: sqlparser simply removes `LIMIT ALL`"]
fn limit_all_with_placeholder_offset() {
    check_parse_postgres!("SELECT * FROM t WHERE x = $2 OFFSET $1");
    check_parse_postgres!("SELECT * FROM t WHERE x = $2 LIMIT ALL OFFSET $1");
}

#[test]
fn not_like_expressions() {
    check_parse_both!("SELECT * FROM t WHERE a NOT LIKE 'foo';");
    check_parse_both!("SELECT * FROM t WHERE a NOT ILIKE 'foo';");
}

#[test]
fn comment_on_table() {
    check_parse_postgres!(
        r#"COMMENT ON TABLE "config" IS 'The base table for configuration data.'"#
    );
}

#[test]
fn comment_on_column() {
    check_parse_postgres!(
        r#"COMMENT ON COLUMN "config"."id" IS 'The unique identifier for the configuration.'"#
    );
}

#[test]
fn collation_name() {
    // nom-sql doesn't parse `COLLATE` expressions
    check_parse_fails!(
        Dialect::MySQL,
        "SELECT * FROM t WHERE x COLLATE latin1_swedish_ci = 'foo'",
        "NomSqlError"
    );
    check_parse_mysql!(
        "CREATE TABLE t (x TEXT COLLATE latin1_swedish_ci) COLLATE latin1_swedish_ci"
    );
    check_parse_mysql!(
        "CREATE TABLE t (x TEXT COLLATE `latin1_swedish_ci`) COLLATE `latin1_swedish_ci`"
    );
    check_parse_both!(r#"CREATE TABLE t (x TEXT COLLATE "en-US-x-icu") COLLATE "en-US-x-icu""#);
    check_parse_both!("CREATE TABLE t (x TEXT COLLATE 'en-US-x-icu') COLLATE 'en-US-x-icu'");
}

#[test]
fn charset_name() {
    check_parse_mysql!("CREATE TABLE t (x TEXT) CHARSET latin1");
    check_parse_mysql!("CREATE TABLE t (x TEXT) CHARSET `latin1`");
    check_parse_both!(r#"CREATE TABLE t (x TEXT) CHARSET "UTF8""#);
    check_parse_both!("CREATE TABLE t (x TEXT) CHARSET 'UTF8'");
}

#[test]
fn convert_type() {
    check_parse_mysql!("SELECT CONVERT('foo', CHAR)");
}

#[test]
fn convert_charset() {
    check_parse_mysql!("SELECT CONVERT('foo' USING latin1)");
    check_parse_mysql!("SELECT CONVERT('foo' USING 'latin1')");
}

#[test]
fn empty_insert() {
    // MySQL parses `insert into t () VALUES ...` into `insert into t VALUES ...`
    check_parse_mysql!("INSERT INTO t VALUES ()");
}

#[test]
fn empty_insert_fails_in_postgres() {
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
fn column_default_without_parens() {
    check_parse_both!(
        "CREATE TABLE IF NOT EXISTS m (version VARCHAR(50) PRIMARY KEY NOT NULL, run_on TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP);"
    );
}

#[test]
fn op_any_all() {
    check_parse_postgres!("SELECT * FROM t WHERE 'abc' NOT ILIKE ANY('{\"aBC\"}')");
    check_parse_postgres!("SELECT * FROM t WHERE 'abc' ILIKE all('{\"aBC\"}')");
}

#[test]
fn op_any_subquery_unsupported() {
    check_parse_fails!(
        Dialect::PostgreSQL,
        "SELECT * FROM t WHERE a >= ANY(SELECT b FROM t2)",
        "nom-sql error"
    );
}

#[test]
fn op_all_subquery_unsupported() {
    check_parse_fails!(
        Dialect::PostgreSQL,
        "SELECT * FROM t WHERE a = ALL(SELECT b FROM t2)",
        "nom-sql error"
    );
}

#[test]
fn substring() {
    check_parse_both!("SELECT substring('foo', 1, 2) FROM t");
    check_parse_both!("SELECT substr('foo', 1, 2) FROM t");
    check_parse_both!("SELECT substring('foo' FROM 1 FOR 2) FROM t");
    check_parse_both!("SELECT substr('foo' FROM 1 FOR 2) FROM t");
}

#[test]
fn empty_insert_fields() {
    // psql doesn't allow `VALUES ()`
    check_parse_mysql!("INSERT INTO t VALUES ()");
    // psql doesn't allow empty cols list
    check_parse_mysql!("INSERT INTO t () VALUES ()");

    // normal inserts
    check_parse_both!("INSERT INTO t VALUES (1,2)");
    check_parse_both!("INSERT INTO t (a,b) VALUES (1,2)");
}

#[test]
fn alter_table_rename_col() {
    check_parse_both!("ALTER TABLE t RENAME COLUMN a TO b");

    // COLUMN keyword is optional in psql, but required in mysql
    check_parse_postgres!("ALTER TABLE t RENAME a TO b");
}

#[test]
fn unsupported_op() {
    // Even though the operator is not supported, we store the err
    // in the CreateCacheStatement. We error when creating the cache/lowering.
    //
    // This operator is psql-specific
    check_parse_postgres!("CREATE CACHE FROM SELECT * FROM t WHERE a ~* 'f.*'");
    check_parse_postgres!("create cache from selecT * from t where a !~* 'f.*' and a ~ 'g.*'");
}

#[test]
/// Invalid postgres syntax, parsed by nom but not by sqlparser
fn empty_insert_fields_fails_in_postgres() {
    // Invalid because of empty cols list, accepted by mysql though
    check_parse_fails!(
        Dialect::PostgreSQL,
        "INSERT INTO t () VALUES ()",
        "sqlparser error"
    );
}

#[test]
fn point_columns() {
    check_parse_mysql!("CREATE TABLE t (p POINT)");
}

#[test]
fn point_columns_srid() {
    check_parse_mysql!("CREATE TABLE t (p POINT SRID 4326)");
}

#[test]
fn column_spec_charset_collation_quotation() {
    check_parse_mysql!(
        "CREATE TABLE t (a VARCHAR(10) CHARACTER SET 'utf8mb4' COLLATE 'utf8mb4_unicode_ci')"
    );
}

#[test]
fn psql_lower_upper_collations() {
    check_parse_postgres!(r#"SELECT lower('A' COLLATE "en_US.utf8");"#);
    check_parse_postgres!(r#"SELECT upper('a' COLLATE "en_US.utf8");"#);
}

#[test]
fn wildcard_various_positions() {
    check_parse_mysql!("SELECT * FROM t WHERE a = ?;");
    check_parse_mysql!("SELECT t.* FROM t WHERE t.id = ?;");
    check_parse_mysql!("SELECT *, id FROM users;");
    check_parse_mysql!("SELECT * FROM (SELECT * FROM users) AS sub;");
    check_parse_mysql!("SELECT COUNT(*) FROM users;");
}

#[test]
fn charset_introducers() {
    check_parse_mysql!("SELECT _utf8'hello';");
    check_parse_mysql!("SELECT _utf8mb4'hello' AS greeting;");
    check_parse_mysql!("SELECT _binary'123';");
}

#[test]
fn psql_escape_literal() {
    check_parse_postgres!("SELECT E'\\n';");
}

#[test]
fn compound_select_cases() {
    check_parse_both!(
        "WITH cte1 AS (SELECT id FROM users), cte2 AS (SELECT id FROM admins)
        SELECT * FROM cte1 UNION SELECT * FROM cte2;"
    );
    check_parse_both!("SELECT id FROM users UNION SELECT id FROM admins;");
    check_parse_both!(
        "SELECT name FROM employees UNION ALL SELECT name FROM contractors ORDER BY name LIMIT 5 OFFSET 2;"
    );
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
fn fk_index_name() {
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
fn tablekey_key_variant() {
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
fn create_table_with_data_directory_option() {
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
fn create_table_with_charset_option() {
    check_parse_mysql!(
        r#"CREATE TABLE t (
            id INT PRIMARY KEY
        )
        CHARSET=utf8mb4;"#
    );
    check_parse_mysql!(
        r#"CREATE TABLE t (
            id INT PRIMARY KEY
        )
        DEFAULT CHARSET=utf8mb4;"#
    );
    check_parse_mysql!(
        r#"CREATE TABLE t (
            id INT PRIMARY KEY
        )
        CHARACTER SET=utf8mb4;"#
    );
    check_parse_mysql!(
        r#"CREATE TABLE t (
            id INT PRIMARY KEY
        )
        DEFAULT CHARACTER SET=utf8mb4;"#
    );
    check_parse_mysql!(
        r#"CREATE TABLE t (
            id INT PRIMARY KEY
        )
        DEFAULT CHARACTER SET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"#
    );
    check_parse_mysql!(
        r#"CREATE TABLE t (
            id INT PRIMARY KEY
        )
        DEFAULT CHARACTER SET=utf8mb4 DEFAULT COLLATE=utf8mb4_unicode_ci;"#
    );
}

#[test]
fn create_table_with_collate_option() {
    check_parse_mysql!(
        r#"CREATE TABLE t (
            id INT PRIMARY KEY
        )
        COLLATE=utf8mb4_unicode_ci;"#
    );
    check_parse_mysql!(
        r#"CREATE TABLE t (
            id INT PRIMARY KEY
        )
        DEFAULT COLLATE=utf8mb4_unicode_ci;"#
    );
}

#[test]
fn column_default_and_collate() {
    check_parse_mysql!("create table t (x varchar(255) default 'foo' collate utf8mb4_bin);");
    check_parse_postgres!(r#"create table t (x text default 'foo' collate "en-US");"#);
}

#[test]
fn row() {
    check_parse_both!(r#"SELECT ROW(1, 2, 3);"#);
}

#[test]
fn explain_materialization() {
    check_parse_both!("EXPLAIN MATERIALIZATIONS");
}

#[test]
fn point_types() {
    check_parse_mysql!("CREATE TABLE t (p POINT)");
    check_parse_postgres!("CREATE TABLE t (p GEOMETRY(POINT))");
}

#[test]
fn trailing_semicolons() {
    check_parse_both!(r#"SELECT 1;;"#);
}

#[test]
fn multiple_statements() {
    check_parse_fails!(Dialect::MySQL, r#"SELECT 1; SELECT 2;"#, "EOF");
    check_parse_fails!(Dialect::PostgreSQL, r#"SELECT 1; SELECT 2;"#, "EOF");
}

#[test]
fn key_part_prefix() {
    check_parse_mysql!(
        r#"CREATE TABLE comments (
            id INT,
            comment TEXT,
            FULLTEXT INDEX index_comments_on_comment_prefix (comment(50))
        );"#
    );
    check_parse_mysql!(
        r#"ALTER TABLE comments ADD FULLTEXT INDEX index_comments_on_comment_prefix (comment(50));"#
    );
    // functional key parts (expressions) not supported
    check_parse_fails!(
        Dialect::PostgreSQL,
        r#"CREATE INDEX idx_jsonblob_id ON blobs (CAST(jsonblob->>'id' AS INTEGER));"#,
        "nom-sql: failed to parse query"
    );
}

#[test]
fn key_part_expression() {
    // Functional index expressions should be preserved as IndexKeyPart::Expr variants
    match parse_query_with_config(
        ParsingPreset::OnlySqlparser,
        Dialect::MySQL,
        r#"CREATE TABLE blobs (
            jsonblob JSON,
            INDEX index_blobs_on_id ((CAST(jsonblob->>'$.id' AS UNSIGNED)))
        );"#,
    )
    .expect("Functional index should parse successfully")
    {
        SqlQuery::CreateTable(stmt) => {
            let body = stmt.body.expect("Should have parsed body");
            let keys = body.keys.expect("Should have keys with functional index");
            assert_eq!(keys.len(), 1, "Should have one key");
            let key_parts = keys[0].get_key_parts().expect("Should have key parts");
            assert_eq!(key_parts.len(), 1, "Should have one key part");
            assert!(
                matches!(key_parts[0], IndexKeyPart::Expr(_)),
                "Key part should be a functional expression: {:?}",
                key_parts[0]
            );
        }
        stmt => panic!("expected CreateTableStatement, got {stmt:?}"),
    }
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

#[test]
fn create_cache() {
    check_parse_mysql!("CREATE CACHE FROM SELECT * FROM users WHERE id = ?");
    check_parse_both!("CREATE CACHE FROM SELECT * FROM users WHERE id = $1");
    check_parse_both!("CREATE CACHE ALWAYS FROM SELECT * FROM users WHERE id = $1");
    check_parse_both!("CREATE CACHE CONCURRENTLY ALWAYS FROM SELECT * FROM users WHERE id = $1");
    check_parse_both!("CREATE CACHE ALWAYS CONCURRENTLY FROM SELECT * FROM users WHERE id = $1");
    check_parse_both!("CREATE CACHE CONCURRENTLY foobar FROM SELECT * FROM users WHERE id = $1");
    check_parse_both!("CREATE CACHE FROM q_29697d90bc73217f");
}

#[test]
fn create_deep_shallow_cache() {
    check_parse_both!("CREATE DEEP CACHE FROM SELECT * FROM users WHERE id = $1");
    check_parse_both!("CREATE SHALLOW CACHE FROM SELECT * FROM users WHERE id = $1");
    check_parse_both!("CREATE CACHE FROM SELECT * FROM users WHERE id = $1");
}

#[test]
fn create_deep_cache_policy_fails() {
    check_parse_fails!(
        Dialect::MySQL,
        "CREATE DEEP CACHE POLICY TTL 10 SECONDS FROM SELECT * FROM arst",
        "only shallow caches support caching policies"
    );
    check_parse_fails!(
        Dialect::PostgreSQL,
        "CREATE DEEP CACHE POLICY TTL 10 SECONDS FROM SELECT * FROM arst",
        "only shallow caches support caching policies"
    );
}

#[test]
fn drop_all_caches() {
    check_parse_both!("DROP ALL CACHES");
    check_parse_both!("DROP ALL DEEP CACHES");
    check_parse_both!("DROP ALL SHALLOW CACHES");
}

#[test]
fn explain_create_cache() {
    check_parse_mysql!("EXPLAIN CREATE CACHE FROM SELECT * FROM users WHERE id = ?");
    check_parse_mysql!("EXPLAIN\nCREATE CACHE FROM SELECT * FROM users WHERE id = ?");
    check_parse_both!("EXPLAIN CREATE CACHE FROM SELECT * FROM users WHERE id = $1");
    check_parse_both!("EXPLAIN\nCREATE CACHE FROM SELECT * FROM users WHERE id = $1");
    check_parse_both!("EXPLAIN CREATE CACHE FROM q_29697d90bc73217f");
    check_parse_both!("EXPLAIN\nCREATE CACHE FROM q_29697d90bc73217f");
}

#[test]
fn explain_create_shallow_cache() {
    check_parse_mysql!("EXPLAIN CREATE SHALLOW CACHE FROM SELECT * FROM foo");
    check_parse_mysql!("EXPLAIN CREATE SHALLOW CACHE POLICY TTL 10 SECONDS FROM SELECT * FROM foo");
    check_parse_mysql!("EXPLAIN CREATE SHALLOW CACHE c1 FROM SELECT * FROM foo");
    check_parse_mysql!(
        "EXPLAIN CREATE SHALLOW CACHE POLICY TTL 10 SECONDS c1 FROM SELECT * FROM foo"
    );
}

#[test]
fn explain_create_cache_options() {
    check_parse_both!("EXPLAIN CREATE CACHE ALWAYS FROM SELECT * FROM users WHERE id = $1");
    check_parse_both!(
        "EXPLAIN CREATE CACHE CONCURRENTLY ALWAYS FROM SELECT * FROM users WHERE id = $1"
    );
    check_parse_both!(
        "EXPLAIN CREATE CACHE ALWAYS CONCURRENTLY FROM SELECT * FROM users WHERE id = $1"
    );
    check_parse_both!(
        "EXPLAIN CREATE CACHE CONCURRENTLY foobar FROM SELECT * FROM users WHERE id = $1"
    );
}

#[test]
fn is_or_is_not_true_or_false() {
    check_parse_both!("SELECT * FROM users WHERE active IS FALSE");
    check_parse_both!("SELECT * FROM users WHERE active IS NOT FALSE");
    check_parse_both!("SELECT * FROM users WHERE active IS TRUE");
    check_parse_both!("SELECT * FROM users WHERE active IS NOT TRUE");
}

#[test]
fn show_proxied_queries() {
    check_parse_both!("SHOW PROXIED QUERIES");
    check_parse_both!("SHOW PROXIED QUERIES LIMIT 10");
    check_parse_both!("SHOW PROXIED SUPPORTED QUERIES");
    check_parse_both!("SHOW PROXIED SUPPORTED QUERIES LIMIT 10");
    check_parse_both!("SHOW PROXIED SUPPORTED QUERIES WHERE query_id = 'q_29697d90bc73217f'");
    check_parse_both!("SHOW PROXIED QUERIES WHERE query_id = 'q_29697d90bc7317f' LIMIT 1000");
    check_parse_both!("SHOW PROXIED SHALLOW QUERIES");
    check_parse_both!("SHOW PROXIED DEEP QUERIES");
    check_parse_both!("SHOW PROXIED SUPPORTED SHALLOW QUERIES");
    check_parse_both!("SHOW PROXIED SUPPORTED DEEP QUERIES");
}

#[test]
fn show_caches() {
    check_parse_both!("SHOW CACHES");
    check_parse_both!("SHOW DEEP CACHES");
    check_parse_both!("SHOW SHALLOW CACHES");
    check_parse_both!("SHOW CACHES WHERE query_id = 'q_29697d90bc73217f'");
    check_parse_both!("SHOW DEEP CACHES WHERE query_id = 'q_29697d90bc73217f'");
}

#[test]
fn hex_string_literal() {
    check_parse_both!("SELECT * FROM users WHERE id = X'abcd1234'");
}

#[test]
fn regression_large_numeric_rt() {
    let literal = {
        "918282487647446915437259234329978868297024096732378832539647003364358012940393742320900080267948613617679675404376962705102449632104125033441369810265121826950804308153317933398857940076948698051584602712424818484803528707857164190643512538078710611101654127931780075409599835813705622148301961479782168897843968906540992361311498717310520852403091596531231313705074965623627080530270849185950392286653862589530638287680487979416584379841744919802394727728211003499107562060530396641475781064021099184553221049173851144118372734890635883781783664406520153969708270883650033305397514735052099149050301757410121944236003586086639135690164929694276310708867550780002150700955837372471031546489485396567959510769506735328747424981099585743090417371233013196985240112250222716032395800913601322557137387603493173587624967364274497164788969531545210399675320528589586528239292509724078663464788125224371631677483940366334015233493950455599834390222266297890109045901728072259094646069377506310289377461939579269849720942511176404903449055763208335233553003546259123684416819339853228183654429305159075099564253971561420491018666050107975284789355259417239994636277049890658599456229255741199507510601800963260901884130373984276460053028553138417177031797690395920767758966877167382465738514606398262854273354300024581850941209923056455030852517694396038048448830013068779972828623476057893705450480165786055479502294220895267718733758185933115410726841034204242738938202609509015912267375284967399138931833135596367916301365952406443371245000807296542795302128165316813403113571848776150494389139816523766695844796282377415918303834043541391689484090373589603295226642621918379577281833798232428985585040256181167903246499408719531355201930457332360808635892018921890122092097151031571599061457856424129486565835616838071090543714645607958835536479903425350217089908137023168656715814466243768695017097986324113398450073361355694269659048232399918576202111172585476193064349649460096366825206301783031869575962989401350686314717140542340384509678651172865662238628172029118941197983514168588708735880141956653462349855780281296535902364646314467957752279116841927089594197443777746039559464122944548483081162204564361835119659145554546121535678494345362321064515770341346161105333147727593196055764605397383219357509843633887155472406294054640405085037372258233356621058177714506935535688104167348655061615537499594486617118834305794905766617700198590807383895464129463800562603204609317438573019106086418048311607306657944267171797449114002190363301635174560941480663020793492746271528177112209777888242291406126403649747690042362952366442802162370409854374683843254020749631659780371467807513400671619137898638859653803125041287804002817264252324977311696929104171847846313642698505497232362317059224795919110167423427846677293346885260084901322309440311248345002240405533823299538959908104937819220755601925150993766201129990939505570097104898243393475721839381460417227514080855859378098271339789745758223716527534124319048157807484437970400439788697603841640217585736647961644835328111287878008490875121857508394686770893134362779265098082498847198772322167174785828832689067783778464533358640633821030640001878351919532644731876751769592849971698296378250503934091352663097212376509599892607695067857451124849095716833430049323692805330088881981040122551818493827410641321544169268469447811913284434659506236508569328457163062723850070124764946569606981588809830643169859958198346253865038803837516639123616515003136373548662272740615125043305730722408476932585494057976875342551608347584776135117576219492299555143489980735739985159782192819379798438157825676454528382608116907997477950399439700653055381954252553035626101059020146498003017076413395172593253310626772409670637654699472025320350192038721901249847081184874151772723106149100020925709239584214186239632281542178788109592386870412643955387845392553249027908482836689795694014335140670218536250427215803512436516923815236915806155077647299336637719767006203723766372736048277610458614095982119347801306007575568838545640343760357299777080427513812068465131880584094164828142189156328274236168145558232224049788155030188440747444614670555902747463701061662470326738630507684792732538018853074699092886856654414692054083320561470238975161400966664959291941472275849541154960962683605451928641294074193998577858607836257003391700666580872247659796091070946153564407689998911324345207546104184696502484847107292151440992532708706378805864703285537013791295974217951766293661275232863797719017171386049002773999592537528377041787363705828424478735869389398354031031497451483804597759835014689562262563407545889627643858345147835229047544862772957716799239757738644561170332782175161050934943039473433643582622659513164100615286062530245544586673092862229637398593510439237123803075693015731904901510045573597195344868921819196422696482621721388742249885544863353606789962840285472611378013064087095773713692046489034595575009195411181700325203495769046761937970232833052157279548102611087350727209743406386398374923568783166060843112809203141195492312275517599739293240622373355647018808930123084639024282036849558389793559424773043769766178027829105311587212768380889702273413180246924875440014253628839622531769584850070204801549746288945224888039481616260626992668886198878800511015310887625072213871976069981461007815099292627430546709847097159150736752403176133874235155440333737279506791255821229841996074675066957012488545860337192974369388823212967598000012958846807117639358400941701727256760012946922082271566799997267861809986004184432206558105421811870644266502952364809143192767288049177273179265422864867717336568922486548425694948193566487743191335282282171320207436924163236421798793587571068893270722069211038682430545301131136237560653805181087662467809341652949407058970461837644452047064052056124547807305691677653965042013575701809917949474999996144363878219736172239070474686258601010590107712594832583811534690055838786415623360963788488767234380051966750713478120435355176605158213212393023656256427961166219845599017818702970108791507441764420908073540388494672942689036712326384261328156214178676367216840052222939302230519192891752664833737657282156750969439315606676940719404539693189508333850810176336154477148778580246889894120336859048333210910753322052248387340987368399892237871597326295386734155487595985966185444649410893878710816478085379304689824355242809776280315758246465192918250667203963914513829576049332474041578179872493106133663096344117440839804313731257110007274439108995685667154782552098285701873163395020730757386726172074471645106084445551258876626937518241729657847720894728942001687802331644045564591458220358291252285389427264768770730455608051570851115676343817524525349997418639723412979301039291952757726147470257478304662175017517952559489654509877779562065841696730265045869285473796580490548804504471520354006021503105792237766845301049364849953610593263255836206247282582223620745777382354266933377289159220110567532282287775990763995030019310165213835178687453849887361817555705424386621634173532922844369108285827226973731881289613308403095197304552602752802890738233985966397922341140216187856177183083884879994080508719009062766546744868261988613516534226561697173298500580807292372015299947887413985513464272487621985931015583885166342809723136830057990222715102945541616837856688421067912819769386041808612893239654306714799144678031915044678662310129120833123739291437353920124738462075180761495081910299837004673246199549689815970068596719501828968762805316796990014400791283877882830135066543370066878999042851466911018628917291746238893511070929224096422314486100540725675959220394429737749088948320069674519580422710796514962282641106683575235977082385328012746882580842534238540468959280856443909850495068828095251036559174907562416077237422571969878209330930109922808285965831562725655672502762477183593241459871536929991398638393482357248843507716242099499810843422646985980339391933695479043131387033934994944320244293363871520835572094283237439616860311316379663799561467373032024790310149009338994414281139360014947678233894779723837605495537080398432478750313660058004573500358999834951682190245230945400067022247791905727555590411273321518794577142786972861248101179553077427351238286858868882484101735394900971702814356735031935503897198416341979625653925450509545731579174111928100368910386907001778382401787012685220250711493083328891157791983302081143068205498759258363486159325905319549439856958011415771383830935239513743872353801204087195223838576185499679360977096277467805388960936497548428715085378164982063104749124017577767211843602270880737935901295966456697504793201391740130797144534555028631209085330679193544012491732641047722023599797426319760743361142730252723321800255649380140218902831290048649936588857841115355015645749525014151749118299686121191434104398135512250470781540444939625871225952763944833089696367656650549566805935318463442375374266470133129630108276537081345638787991017151474998717878094052402938930486911315194311218873818592142245283180309912187483994700954976878968759648050893939693074581143754877799222277100678243750939301199997171039163548209596632364256870001688744511060554700017755619885609705581294439318939453815898329194237735224302963452219370021455151448794883869815261009906758177112851557908886340418006248410724399461528854817093542483819903965919242035304804501763985275509554547520334383566709256698884239470977092151480456347924939562529922342126184679642954678938148943007401649534334901159249545331853577765791446029941100497091153181910984194692212551653773555871254282737585390420540174488305875835255649377627560814692290982522978663556518356919412816643154210692073880241047439738667543431051366188256223615928902766560106937883893530455582011544810237877463971225023864093198596942773029834089718691194761826769799442798641799569764781167253423542798688504074351963850985264724310522457048706157802736715676845192132843195763673211999670621556664986019409658302109614285697034022264863564314725964465804385562751782435430286405333873627779287562021897956716774537782793787185053886698630533887816769809366762261676659445984720789645651986216173077308568454482230210649352544661815113678704829095607492510493525916731955075334034885642944265675525645356640860645529496757935645124903461940318430685482053750371912108500703312960047174760867968540898670855660694272300917595763777935107903877530255473616671906563847049922748278840795346278209653537464679952788149616540726572444243092345525195085123613096166394278376080191195975084862702396866230671251170475533946684905007115087403091233550203300237591512710316379005927562253937414004984975383344850228406762221450289666952503321465770230397761404765786497290395433783178886645678208490440129165477305871287781340877022204471775308596184308054688313305432477373666176737337099625607270022026533099476446473986899662343440901328798105887064511064692040258806618228578880727413199081934906116989874006245814103348985315689248636984556138987406322475391597451039678268901977729035703668462866615991711925557625866132364494279510846602699866299338927586667254670581374209356897316445160559754611719806860068219010195689763630370287763273595098769761530924696869230197803899799286965333522180383649486213465557610930998202087614918706878715104250226317395479634842179099537645359105168465260355000286906489565678114138925162167184957183640527936858259740225093108809426809600290891679761586038289739940898935835361017906984440082490382986407381122402222577503512062529984346846802088844521628831859477047885364140722936634625143432292700063026132224319116085579433637524864650542143592525959821901162144495626217387484797262682626415154969737758738693154559351249681318549846282249179772070526242899888409221147691599714129628550966507278824445062654893183520593066009099651136486199186383385707980137110451379918061681538566026202260335638638093980692110267351531115164746951270803937798017220903093146770515871395045159628235241179442737486128706879201415942618486498661792567640147510693965324110361993347187713696170870667154354719547403486349930264067893766088071395240730266660983741753059173527915340305130778698616777033148681423474110608949479259236506718401351248254353046050864572477719489761207964380193650742726541430582435641449242052291268399496989486997298791639045726586476673230239951725764876992835333031224165114121454752514002343384823102502237746323503809433212566580350142272786430809624654697537996441899566012280572074363892535844501909828365622985840580025438050623302434214395232642619408921287535276234614321882270758410685764697128623451290643510126132599507505277628966619518560182793969513182254601158193444036779357807674564032421763871215534683608300590043740817134338852950499566758519640462859842789058322238768436916627633300379948040631897995141768430722761216268814398369781574344813823708333644769476252775907562022191570219128063535256042563278830719825360545901463724177371537677480336820051841295117572088494559890827527148807242145202516996530536425066341553660006421063408517736284932349277265147900701942086101259678319836618551313918507530697240369683340889457631621962597405373901178940560745703572329575537228966693639539154952744756071321592075490694787562646834852013005377419423820050948750984210399727137869127794796420774140163660926687366297998130445747392497239783574169108722491278595438730420715982786947516436824519836995645911793364568270073427834568689003263462484321330392104667307321649889788081276382899338969357035941568017462744214015737207508016586925917583217859968480521697216386072113628808846875918778123772574852063337453197566884858987227904454068651540318123383809550200329366545218039728330671059161849135823728933679201325111933745529076010863414310097451413845086492890274068216266788095065515059476237944234744578242925375621946611369401022292475625708544507245300088293136338299861511966183227181350901323715451482380160947388385820765802925989415491524895983519764499436888712400054714719433485848760243938403808842629012295056332947424559047486692992437091230855138388979890564026405789630446917921634528857923936195943365408753518427232325937397691541327364253131767492629639918920369643255573548714314881183543285504676207579282211581419125898189039025306621382764189183805729753237679407489468959756174097516726777092702448273502004068992057800365947712922285428784677981607688459309362087262130622092693308694928732987903853689457129516307038387071027298860900484395016601993184963275946209562432518301415534283714550020856858514834868307723670873216244305797566144779876178493035360040139542415581070176866820137007662123051738738634913933971093875049175049652428887805344839991601880508866206467044578800368209481643581095657570365780738984019423447342620959206176250820542662019018699642368350409150811558420604576247433076131494742756565106401931483031510442119370031146093111196171001727220674731054854449586468637687625884551030562167259541370408231262710859662827278988941584139110513908251309971056610081884593120793621617613294680567199218404579227657481741439164117820897433923216984087358987134607246712175400409602790017189690004872511723428416611256593015933217049820073334341020738292976710238372863488445816247249845893779265892030134313554707517443920808747714789695263610228310292963822010232593507643429723075657144928542017804446105158772804920803872874673204219347157880392271219389010890636075326264856226175624509070499810759471470151991107877525520558657381577558887342559030043472150807510590700665820002996278251607062596082379425845954354964974601922737002804396403746508874587393361012476287715583399616826350166681459697730551937006684023555446622988603794690436116292866582296085706490421157660595583099984754097970131866175756322588596091571611415030137245989072020292492727038398818790136661534917551061675987845710374210128195675838343411275691697700671748556724617093331845486388923894037691731567485937763229332851299353149984389764414312270566971204748755838140465192532833097749945364261866252605420779026128670379720902801548121528414056467990070517836441388800790370862244791483031495356119441502506157833872956153710572659286480502490536307576011610007615713841323047052069117583013134798123164289481939332634655219669616636572275860293757362006586070850702107890575595515186422286488790069064027330273535838570433795617255814129180453494348650348415173533376138682819157710065487123646737079790476487196292462218497025050696112132237528395160261996050358239655137745215361076012808090708401258009940795240734508688254740061577512546871550303460225371742186090411573600538806766325997866195218827080649028868924870873872376899420937044185720050610269174423340877590075126212425614827110564604840977287331837974538324898437425615355932106912200995741299781585904974212152963689644454613117225358264334286943682657852571044417151160793203299436103327664280344556895439175091692184827742325111424974650976146091789820298847232288806683269878186681277263053401685724233719056701290900349424624128320608183388411052482150334659000793359483341048822894472104655876872801576483636456399446915370164534133115270816467940030976614103794274861092633127696643323555830179172058893804988282823258605368050336001141325530613792215320784087567188832179500482531042187972138945506662668718969757580984932357399375628252197227915740905969105449207232089485863064343706620371381768518658492894069574858626872143452463416892515098337568626108503859166066346983965348852704814499030172886584492876833080772462295062612790866079602504699879589768952850394777774534661908607719695813019395449573274227499827540918372359164289675274317554704479667979559043079798145606721490170357774834980455965891908585246692274323583851395200502274960762105094071451497110408112718498252869811126068214926156598432196285059548714229697606681832365034611443585158952327542386935665839761122933641984290968968799748179936100783276037541264354808635202362066448989961926228135929851912625907734768624742566873100925144455425526820946495899660716126542500007978741240144216253550423033221077544167783831781278153866517889116403101158287015060742509104965015920532320937555601294423590194361094979036786044520050607043837238125507059176683897534038927059280598015862865563363375623988794721939891783336502185140486174871665247459830349949367128250935603539077407894629720453823417573446873483760184357801185685950634031118567370262456441362158808971005214489932516914635761541445754723102980730519550847866215867185079920225237207784956486268319381968282109727304043106296241164848812806263773936478803784127225742615259252369259633099961591954818124105065937521421818557755590884888562241286719916122280992988262653709395446812211490519821693691149440813413821199834793600117523123381495516427740096043809138079098716008386848495219911122418307807667535484760421556611803725755624955897976764844007025999420405655862376579200245199513381061897440666089574299935897445384590023954602523574847192512578796778795443074242371468930863891586559848723284484997013902275727847724827955098206974120136511027315142533367691992197196021211895238946895756070990066526703385658667739730524017947117767303249248383637966479849115628830868444309195914992187009055886166484876073179608646947975930539937831277837485974982901073008669586584552700308476082971796077662840023374136025410766156368089342969453039889247759601194000645147102220507699758472746227999155428872594627092464042290200670088941276205563993505104873430291590266721400810216558724334683014535274790827356651424226139112313473967400013497490848031297194929580911941165183528163801165215546869105443265752644755148433097533196590950500692311148411103970377429261620966242179238068005314099440635049283944115042793992282598032451159494177240257371320840392224723046001120693407142503900515583768224642320533267263302410432882295949482047295204858483323284602165354338579209779508390140755203170122288331446874910221867217446267147368553494293488419575587951540534231728667821274178201448390406388544436190099515500438829196855201509223191676813000128789357860366477513077597239200232105071736608192188042702457234427753648933077992895518134012839264881385900140989781940067863828748537204863480045715789236522852435265552279508418579679388563457245368777431804095874936052921052075780608031015022581208182237848310444278446258530806651210854298877657830882624923716611285721732342504976718391994201753319564038240920304772854696667541262720890531774175930734109558382984018601234583818438976500609808416714547424227985563320830352528300573360899659841732862910421037943688258697584364338075224312968980057189872276322963805332567967917336472834328905290649662594424637186607128134354363180933741364732248460804643855609117076885107260311552524021301632829050146521847420874009737374844596469556967631905570450339455075917973643436980920013628480264216254401627410744460718194456658165499922133818144835877511287166596086014047582295386378766298116428097396763035176277999896095003358638654140436619731830770889420951277903723959665448845849667851780252312854060060526237465523985488871909415248052895349056550738981973965365843614595824018575708471262573471124543028918845084515520459157075336951771420085091774256656835792078448693090685366656525847353511333479718916902032305949654295689754784524434175038103920411878470460727493170119400062999556305282228196785300435806669583072511493336736993225098666540478715788270810142150171482912284515191715212430299344590180512868819111638012418485377724329734028063828796384903609667125779939731658974634200715488173082527814211729021122787445491271439024216241635697667882976207037931161393923866918249068688443260215482325369555112534153941226413078215261222759382478511530181413077990111131053882591131071552074355963631088288048442346664780539907970583654523130165973918791572156248832445054696218844633827994914678733920074015580491533776737406569242123587001443019451977497476678442102322790537955140706329816188642112375332886932464289246849161950590108569763397198064846099056460606705946144825106748281747814278226704992611903621302659406825896918858702771770105724664122764818603363093056353793942198189756040407809487893483347038713963306699848554043430541638569503989968449579929031491889004672209867848358655947453437820805126990824476343110577985880069869961183158193376956350150834282719938645629447631153349535026888825645346284430880508726337623175370136386794299319397573887393159555826801290374744820194121985735266026300798832363062915223758504885420134459434540034927869112171321387373612890797730960688720008733698318194624637223567788436489197756510896265207034413054675013229930428957010851137131776951470813445099776955756117961987834741214644265557741477986255526360212734386878765387319157086970365924592047693229608944528978700264754476425442668897718098420230748200052496711670259103848759554659416861741174820916893851513765570360548395539413960213997828440880793287047578640743130226043992848720819059378642655274528903490896328777978736406278608494600099236870540989494588880623870249207441004992740047476723342900871436955984468798361835094079437759974454699654820942505744564238742096413302126615121721736347807145309707970757754524280733580291357999903376741821086151419240340013849712808667532661481790551032914907630011013792220719469349639536151559407361393270336553356039850245953732406040820116272636081309020166346557909264100415580179717874045120246528902718755272371372586370407081331465757093644535625837590244537861026645095775548425137884179721899808722015838324318538917451294947845330406751307782272647792017211969745951773565940886346133799864774183534313579559837889608300878812856552487299706602935375033594912607675554715227298454210895849015875481370451631787237247406261319291553526232661407687607436329793029409023821940768839613366876657646507803829922640493145654650572848947578159485999174833060479874886522355023641921572146969442000176933940732310214601690056292540694320114794551249890249951566113587673970617636484905377528375117458102026664337071903858266294520587878173991064244264757081750552038035711631793126287571354872157481999086913656264372013115341925456969362319427237260833750565088282170597226125473594401464365057655542506926806286514373659770810580955914256022976876811828959740810034448224773685040187673519500073915194229020450708201759758831051299861005219036826917564023664824827645234462431282422533794277111024204929743816409467683442526014238412143500457847257026008584849863007857924537401616691021974368410604181333969800773848178035783564401112615135594343864922379869346001395668236226402880594063775321977378726991225864403951987305194490160114315855013142048111066506209975464405553805417737353211074920976930814547666603173632859314698036915461961500139034947370847321486238967049056419161997715861471575057660962725614130499401900679632096807015314464396642586260269347803178434152341877221794846868617266231011682852697735615701409027440377226076356461497576189252465010100567913885860602237738698269879499014172104741418738556872159166386759491925850784998809606053879157762375837726099959524990055469736744043658530894794444077673200781507832570319174970500296564385554227862449290642554232171713960721807949397886407392233875593731343514476435838672171755534308925412100182721896079673043309863859817231891428566189377884722497722328044195251419105317337537273041296240886814215111555989848981348116904326046101513178973774627612297152213564396166389543854132551703833695080530013572482541686775413369501188338871417731175480343855777719750599841718577558905297585078625306673210756586328861426897257357458401532307507570741267387812974600652549039857108172815962575837699458734893232314498183305956962374912047298140969189595809710685188099940260384410062993342402235521278248730371834677603246333823196249344847863804921240106432596615038012304995462101965362297362512275380109890092199168850158823123377385281833226431934820855149330399397456939449192942686995743785544628697084461175036643254450733041605800767936182055771567063126374211003288487615670873726120082140044380371148097771275529547921300175236928847531366166302333462001468313838710548869175332509631702598639994013814559584593098043840333810851695306003405545539615854383812502396976696119845565813498431668259390403754444270687758438727060988307394357804844048615979227160646140939571650466487880092560459289236825990983432337631616934770309466993514565270625674996002496964274061267027532322757848371048162425881047382916206589212577042351920107839337207372451605707512505414336749366815223131069808317894105343600741918755331912818863324200384872376682010316197647353795997721620642068460557246660792644717250420236001955709259339447160164512404191780077907375974985284718944363140470807051927589954947001887844990971375127630295082401600691887446398523675333960983179604337391921411132219254808565173611565010006903809489817084208689319048440466260553721866656477961820253829774841504690078976760618310893788379213334868435571623412504579253289011123572551565509089510086777994394651077399562175004550355955635425561338438872628648899067463668098003060494126158787854745300860898987612791903364037840539889998864123157573832573477350384976902352587375269254288727006543413875754286077734952300233647568959321368778719548359583753560140424806771541060059028615544171871936950358558978159383415268061380191824183892353758954980440212509731955648849874555471935088968485360103973487918718446251407521479981968845634216897270409113829780127829434595128990657364504824582616620653003140482120852860485968581799762653530608265005128139260572401206817801141519926153031928640775382984549673254415099800960244872652158780548875637501502993515351821200017845244865386338826374907091564552150282390613970815424850806953473285987353388836387771144962787093012887605496911540561640975129271955223372675224626985184050768857665500196549264222573263888889976207002265162779871740229048430375199076993601227907359932025731949696602751677655051558809440420464228619932843646273776449115845131767099981173411318359956665446906451712578721513285093099521522615465780747906520647127303649479919105423874592534136084447899022241910070777501378868175800079195538175122453670910260674433318883687496839647880700750556962550410792506410442090955727804706749408689023529501315713570647473129662143514380348355319177538023310112932423837925651331261402811839606887332362868166840825170141321279971704353441971561317377142080419995820481577935827602064098260899069527816295196517587791487504613900293174629048275697856906022814352141360027313544432404554646081076161932806906154557031840132196776958444474283590375423495927089468331597347879897112842728557024937244234643081540392132158764246554000288600964708658296253233856285873453720639100204013982372637412709977020270790096918317259355230839546234089083085289818972832675546956213599920112123809915194917379417781733088886207984790831121260123346792537362605727873840555805644697914463321894603122638071670435753871641727408440741937073055006528139122798326462110441837282605553342675824120266297668432088094696398713765136422791292267495163819297771305089461350429600528199116150561440575213938405451432988957793104595272992524993273128722716686790273190900570667434583011527342377041600005192417693317250685423737652537539498410208378887597112072272790326367792254872535274934387569144430553238860728835058064947533197489057379881245726490103616546415840748242238028924158445331159937091040856327382186518955059909800824132276705679091496877556031125266975461431634322877586986278246652927632532721396758936931310426708015740308253464107986491643108963173209399174982123664542148422662880931350375784731183040105653828268900089517973804895605672323307648100752427725780019924904494346924144418433704598187264692237454898232866815175821338885188247136040217870833909877837245788238956187075384853934763557175583248437342329406731588077131354572881563097835656711319805424443662904254691023643303689441024361945880657522126929630299795448166613813674229405328888914957681787431904216259624161383895701450421624770612590941971259129507564141160423812481795800489361637339309354541359715062808952894703022858453599659729459614151332992874313593777932360748044243537832345582464292512672354667764005013714025386106929435189683213871292196471139100688568188613010374848710179032574604274434011173728926331573348900180104869087985309297762563622445614389496498751650169653203871586888694615716500019635409913446501845898514882346059919796352456261880919582122326145136104680445900281104139398108635477720494410074521878162068132881144835554442962661478410681163990621157569396319509194829783273520635695858305697931262381749704583153987193083141679721276218782309298827123180214546269755449470599523124906305969036030442101013110126997662225991533637246708358369283115081087604360254623324561954899173044333955469900607442602327726632440184733457803815941661868019968296417433236619911022439058303009608404147228615705552836985587981981306488494389209754803536201829402088650186521059153742104076511763246271075383348154152156964498235800542962306366825092429756085579188454937225288009520957134868002798474374210503197841064502650274360298906251873374653063741810229414754050184843650098202424587854735904238115260217265310626364993560622279544174956940190318447712929890538389274346861868987763965977653556128400511750492684494734643336101942448990390743593161179942766485614678576694689737424623294181763560669674644075676562609716279538578261870200632306512030554646140794088805449408443071511098206194810750748405548102534467282122147332973524891164969559784320779910037686035139514130024337822570372116780954441750454984049831046615581097208810236951200006196767986324550452759017395259152782471229967586722406295258531982147384041971348938135609112937820318338594841427544554079657317623100116777215318343204301234447092378547462805791435230381348429481687814899933558000598229578400743307468534691791615647995482791521291267720629442236098039934983389316332527526980723461127031142031977105152767650381026800899060543499505969807090852521478068770671637028215700461526625284402632105329339765857362276636639036109765909103646848120490266393692805305456781872052736373710756051410214324169487216805947216691641770472377248207573444446347503318759397910582011982520634710637820989568309442298068007688331116065538764660949838203670435410466641443892614698960997372966537474929574402734355977266813902294234055788737528072401058171644132461996434600149293149052797293877547100756954443421233292515001485878518863106009041158338558123498314416785764679291996475458845551016321502085184530804123641927253118790247855012906743454541405824734642796190515041779138065686028492656734139815921462811423691350128400368807365065872726119641820992890529197744523097597473497010750621183632013297558314047634670016522610506292711448484124358347335769160292921545748014745720402001742164801887863674569723456622877220306618154390508887977802218005221060089674601141466582992423465557919739214320545511849784397990365454175669708877122686766354216138928115420246421248856946336371977758727290241926721457091355846248468460315478944457295309528009677149843407925436983751519220024915333554608786847190062739867371168780195317653048243110320118548870223485633087609445028973898640413520003063449438527561895078260055251651741526281236592455689403482601868797225741898817279762401863254213114194141822158729581583796471934987372460306730375495232948787321029317176115463103243223215456288675538261145221353984314010989854910586403359626258274144791874599131924542523236385721457134330419273528212944307075904077555539051082872430594027173788103106359157139339229822391400946482086398435659566304736541252037493530253995872081296375973625971075488422760575717240877698823128455159449714815851170749315400917431220283603064520912589369170138424083271110476970752850097236777923300137789031976186035132321101554423053869461121821576426271073852183890156068272745861299451268206926404135638723700650050335778177448272638682661754110304896418029970164039694994599976478606228870763633234111723378063737230540992085737701445573364366757939044482767190173436804946972286162541251595359216531320236026170213421691941510095426034402927303301468826150543657021402306871978504056738993941345635773153078418723750573078967775202554298350862878920950846353437610625846816315663632381945763167062102346994900983025826916094251484369602625238210731974947619447050100705429351311252870753646586684646564887038767242580777830872889480946325659614201677317702196926710586947103294713786539217529327316046407574320122047244252529138530648373129511664031179049371005332454021804468215697429418358691633286224814919994414833779808677133371190884375719050902858550247265597204407237488662204205871458876959793064861350437527405864104209695996066624826235250207934960621887916471057305549049876444673334911254537801384624697121312906438396271668366384019749518757450146253956185330717062282065364412117034259698421644101952226525593601855721789708532837553394284354692452032478991397450689086728528848187582770639725962999550299270342584224280118428888772703670899570871305504510751143965318735919630938594629934074704961890631842701171563272573945175758991664485114109987699045831304982164281548489450364473659799903070181648775668392006603214446976639764167293469497926892490752500357248570641945298853292870018314785418153464985633699235891796937726580454077026424628017548237396187905783611341566440983475589750485974001995836470121922074993895404146492609712873457240868644277967798953313277763498989539113913880945525832611085905958014404557696543947682591060607708724503600397135426070564167705175580393301022919746155222149240016865220002422323567095120764439425107321708709776269843370107235962215340905307986000931656330765117674541439036669821981675846877946754458449320853642096761928607925724307608460028907702931686720821279966701875890315302099020293170257490721821476917274191724671306558047255570989055700663684803133868139964680880248819285715364278170938877723460770683658088426690972970256207846406916541083407627616528500348387374012458618542478749210342462946898447849651885725887244383610973127669557526867571093465456562360496182123894782431489855156453152724704643967877680762239835958182021551825341608283589509051993958342931266816713444113500224855759088702113906819803415223904463403767516233761889820361043179349308440801789414760587035371336794531536780537961664625467556337230902274338380900728667005295559567938954891495874020590568596020690527292576456739209260920316506730148049226400100166341294720501578307442013070692252691559094322474749676468591854973352094929681268425938222951565715527258534564529498463257999123599462705038568302769663741583309069546498292599286521501317104333915241446618442835572721217916555674632757560047340810160869696014382776864847356306554001311118582648571527383739777424308854880857030521320753624034936992467384812135359347846674521188055295112990414060104488272805665538327024322864753392360254960476763196476119455807743741931055067883275359465289537653352605871382914889192616896202054682081197352178118882323179295427008878950839758353219706429095454956396671874299664464746894338345987723346103689159129004948805891090366916504722035364786592040349103720267540838297234794805963117742592045911532190836524518199505489724788907580783502680657872619450871916935284546384061298944727458359279736720722241836531099657181668996755505301892749702932348797983071856745334487725746799085831965899780518138490169823026107053143199883302383167924848941139718798914771054760106027546393788162550161982639149931931818746751464910159915533104995062045100811717900590431305560421170209108192147577193807389988723874813596773783295334711537468969386841930582398164594348804680971555022320287754499835554647284191583316124357640754628929894713515508399177078256551372079504005156065130618473975897191943064885450022003735693700523273219566506232288422108485382036445603408194421771345332145957024566116953788836738024589101959902116152337690117709634260308347292521392639195912614395471196758308236839414636516724695434683886656915724789339374272803905438226612037555758091021830309912730437202285146440501686366239805633380585980178374483215484371291008853833720239382991297711872903223274305285699518274713854444100540664642273681634935048379822796845114645493189049335588918511704923579489883248095229345570038378974904472061845549972725240896367732771338068000748144135911839232644982854157797968176169585631714595256298461684074474712010893855124589717276127253004550404113363976502932383487150136392079504484442293717863254694836950927405164457176505110563531450472661186370615077541139820201308139761490309927852996397566793118525199815076765638284493632833047951493536882508280349107591769623005957960805633730723189911249204721438147275375025634876971089659460970208169844393488835486675522349089441357788525763849571347982918902621904201638735908899468966509864481485122689951268959351100667483654688203361077138920622514310785212593830293259278927276820083462654474256076956178837686990107908309919746608477348814029007768544536368872134221262340695505096056440488040403725935976983264691282082479592418687542262645295415692124698066276320949936401216800882200812948957421000411105122454477940906870143257700621087352419075237633804440620840984040803284833562081628848449667705897492866898093731430888344322535824202613469827296466345774743093497473175802967394216889284175928129571278748408509161956132546395418153301969438696830457206628396082202180562518181170735668252637670954560302229789195227188436084259746850762454108633554922296506857796758612614227157049431010215796597033984923195472940755495324520387619082166704794091747475947390200201971510327863016673910152096514262906967005966530399264776334450678215361411325196946667439991642776579899038311634809931592870345053746387935324787859517677485102953070773484205757450442047666640703247389106630137840415042146049853244595483415287190896760996083667341638610651395832492515447378566346780495375088238007070310022595585457519698922784753038510057003980972717286805927245079858418911202098806272954619909801273639081426069321790026693270573393612753934940997453231350728384867799997262939474202192243245425573805145429792355693442076158079417599446514547150879051840112857814837566365551179645375565941912210677205014191480180882528422417383584979246944928724196786660461330837629999998192401476252575714223600857671214435692107339216784435682107947531958965583953914078592502986112501615373154655335570100408963365743496870301808069944955399404235587466308064729498160705132611103020537052532123768669077358697307907490527279526211798476618562100912160706646909500548830765341303918293944205508778719268892880694186088643521916604262652374349395911669627847159947675536931480517994932909422418974061623235614952349169732360987295928973966257276920633922083936895890817787930986654839337823892009599528097494703356574365449115163045642784088007625620278140389927916272948228366302624753758466658034732816390896384937362311171073527993004015094113869029903731480225610487609117170189381406433310930865250963393262205728565959660320159893572747677577018467210725943868253379361238403148714403333455054108973262814523028446886796912083469656229724547900728425316548872328268083105655437688220072338687094249081439323859810080645952015135496475919564923136874878528281530745701665602648327865475741831000258821291597662826689760974596180883217219578568309815804337344312069831436773818932005664375051077152589390948209729005579456557626504721470671629722124412817736653826135612931627719124911971530145502795552745745920911417420928994871282881513105889199316044933135941725065649245783169247004110349945120983367423395357460133550360391031924083360763355186871761742787991611247299951765817025266185982258716635365272607146836200339951766120334308517368703101087479013989761589270362827247874641990640917507774078068369709510670987639942249052934460152973792221578267934716657044282160649233327606535960587549673551952537732832591737709093715180706304259167785644781963377375885430651854366119280719544544406663302143682104945189298103092377834773332136227595470523850808825830573975237285825318030594070980029767963678559399078280666125332850862186026097276596176691860597589409671885464108194626644814776509140655644698752371549543542951654173903739144584124147002425900731015087066353077280855475538339288646320349006675329570277383044652245854865306950981755743051098789073031656204813948843921085095474590721191302325330069150819556181212733122257859483045839407050411324194033278025290476885566010129984558672749708311997433383975635199870610411759192165116237777717918196031381455973921270772818112783888535470558238540749977003987645053709069333201934835139942579585912194903095944099894838231373350531371912581715738513807774965669509242351164199585736264899233594052569640153440098762487085262830873234976535673739010987352900497398015923479521598238453036049466378294695485845739328019974423422391291365909973714007071998029514590217833011222875612666953361618230684815669388589307047983661778236030259278783005348436850410814807014909896345805115029414208869176430813784439504296175537757940626443601686297182560199944878889220392208011928496423612265692170101443125794555601734592142937921441307716162663425326423449976018497447441695812588843884011898291003557404799553277985522092996122567776861887676227491214370854529021016482041514121348372617630433755055380970834434638708917566248946791954555138714770642250941626631726224451195791305000588586353667977916676584403998557954995060479139883455367431522998839905948336540189079255277709324364647302325813826253151905137429654940379348578750575648839443130931520009086529363734995014036010477161966798924195543754436452020589327137287697178194152802164231815464585209305510301686974055524412702326798295295125200590165172587840830174169517347256772261812473960063363724796206374025256067378004928402883305411249787855689775698824560374646968538509739923918711267863185550676673591235133427589918275611076800936363231669366095640378922596505743592193667124033930723349482057253135744190587863029883850330487421063761067369958081369868029543627758626957459402386941531773235433160012293751759544057199728102458195068682517088300435002071920048549985439506127099399198230911479300340929334854023482390535287279162691110510736293132190962871984551125828868524893793112112527740634239177350066187408315100830039471401209077526240653619150270385638248716195716878814648405391582404767672416365231836125653753894074868035596288967427521362257684424973362632387000818747733310131533332449138354511784511178039378738367336959401106570109383758326355513726127407256472736600808141260466337374375533278955951326098696154549627330720200482025324594845871110175695074493057354516864725200968790409248513542202234002589829639582226154131973539366797414865172299935759934036535099509223055490793199499066908728402226544021892209414821207575530091310784838711346351517011631555934826903126369925087769042536600455218391912363404593998326037212370147261345971465282234211056923733491588239672276452239209945212548345257304253243739176158668583357666018832650665718115639422539366239913501196551386244706800192595388102014321804206353091528517993321661000796241760932368139317198239360832243598192032369088618026278099366191229832455248448182119441438184778006025393455023665639691692196122364755599513913150915174128522578859933395764858647736179316237616100814201616104462418556291310268524189623127016666520552006844825995821661403104450724413555243734141431092122838349876421036634105685569663564382882747758019240717297400995834263124439707319960892861490928125561496961541877557135935016291446843169315567546211260612287138311669588114290137895902965771463763436387617659619459105943165514852862996951512529840669265690641148598018177535316176819126725458540194500071230761009329339166096927220735705994389660676607805294764286825527951476199121023708057655448914930326060616270476574159447875336970365652305219662304288600398798484602074342069657026146359065748458530730165658091681093402095153525105508578175897964945131210881429404729779081871828924619679723892960092907946432642854803336744283633996899783637459299429747361760316302749717784518242763453570497848118392169796302415331580000940694386561725819368607011613095803338049965238058533661377211311340852793939097444213921804636727032481858101813770370369349142013435347651618379547380123277605344406208314840199425582727794782444445221905519616735517384084050115873149380981941011916817432454264061388653080899928631748539206215572999620165800525763991191449482529172180312070111264664927664023838194584755632252789284078757595985322695887450265888773758931958551602781430674708920847295798254557119193172801745326973753148957584382513854608406466936854837355723995386707011977605246270907330146928665171655321391279552706631542629748108195181411649531794559555909215559844981290596194395911225071420416010270849945797578154298720327773989407690344356721269828516272213367239980779384207019470555893296428724633992532840505300263432088205509235129674978594790417524106906560342888659998374570560462416692236653598576452421113180135949919680915784543581032981425971054580526461441259400109892594416507355960002828965105249592768880874752824827133212659750795723102594716623983196683367347905515733688284470575260285607479138889426432544076226362903467603160849959673647218964494272676937222321449584733338200790773490359837733803816274716613671464778621493633256370567012434980134665083050017508419747702686878714190737146024424988188626974578906067047774433855069614743430891595542623421855120938525514919678726934391836541908928998695856436400472520136158223377963977816793326884548433820864089572649741686354902764420428318109860519627142022301371551954233223153778224120420528048401505529604101446385708844037742687733107100691037582130547737528434981513068527756333155799765960058187195265527250962750306071766246414423605787189414443770071483094095493860343876759092961378516343585240928961077257108372119705482001609474176974807146100728126564395734824239084697505661660370419708265454456431912209653574006676743831819010157809888550108081106695357072389515529566022995364866580452365007150550938254447498896237218067987420604410830630665162326746305940542303356679792826837607500364508648368525704629912452536836716319745244876914987690006800522336350333111969895413134740713282707324878305406069363040603631334033444709457570255649171392383046687114519854306270810430627480753272541926921151260438503815952543326856496133656364355634945981892701704174482200791479399092589854816209493718178995731693310223510566024317633704211429818353097378346705198565743162095612866218971800625139751561018107502245502036823148620637304010219355194864158003012328885658412852265632913362173418099156208192706161806134928699009783349485396494739332952366694500935472121056472452280265181241103558379781544290473852857111725672702132220646371685649955791674524150836057135109600891773840828650598699475082461230717089478983118847416468158934154215824671993484679174562166307468804041706717235413516640210933961853911975816048376898530359412734156189094384790280041121461930607366970410330450078636039660762360852535987589369778597779957707468155600883795174622865140548313317906192288903836063844224190717929034815875166788936187491166020825738286660894301909951687900034563563864563722940351570083982846110215458779347729539003464844598318309791182272713601277689567383125833617447923609891111323454461740894288201950365810827856503696545466126040524375893993950539546798590309796928718267486357721978038487455002884949835776448586775332760595029623334498933274753785685012395188590835677065071651643306078610493617570765476958196396758160347886608943599634070990356172886704934268084728601961766158995832262779901901874053545867895029943841882126777821768067214124366347319933314928277983512447354913192175029739548653544424920189661385509141841534206246433005620865882180270028723227012707182581926242959635898288236763223754665843781934371018882088145306607760501504741293113473912815653359884341783794447425662042184760267129824851684414649528184308826716398083070304524262519379932908684714880032851773881432117549152529574789422540826083729139218052827322230410421049112318069661063094787036908804430843594356233524582203280397861637355373122257066340368162980675979452952970117405560684361749326506065687218258930400783076787977112991527555144663642348713378367939054320636238268796274476248470701635958057605261956715543040539820368436971322794104881232649091750807998778939882590406797419320585063656335804011780666585372254976218410401861747358158535348632969126737678517537632985691081189701268189441633437586179868528214834783405544062350578703280809324295612767714831510611378421606990865770058461110078328812615223743939325115420135446499885741043036584640229085172496550931231231884845360535026717822868161638500696652810525990087282651806228052006532571709048756622518774310820344017825998711106945078119153849955394248358811095621551249909424660882201443772145778606304067127161698187384432302163875421033786275273271868299100072988203147085300409718480443793265885468419382424514668440341652075479678385889657068322991359464512989714337828307115904033655378180584607928087192910002598888104657532242720135106093766384588343951403233301383143940825484709182260543306652472088178944475515900789387003946773625541296055492018215002301286096522514167424618082114149723053668502970686995794866382465326954252664730638600422308234552907148065783886569229406539595149340512498371028979985078733170973105363497861831990858357904221003710824753965868212941342757362564491516601629147699664139663174064096406900493212786177026491836370374345197726418995491166754637649647936295024967170909023339256684145959413804394018100243409272305406935951164510391773900856415627925863204445938947969208235031533023699340142403273571033189275012159056748148672370925279902433482458994467270115729577375065044489819377999848768122873951623049303975316574221815955207758676103124218080279355882626327199296332791924234712735429571665390058820155771691579755323906866364274662867905619205036351202011957229968018211117127107199632457940208679094378939445791390076317423455138698512070428523992973149795685608679369367989004702615274274771005486922394688142939047433934178311444757143008876120398977936483680871206126235790168968394100266697401693386328174436036014644945290721001994559819231411012583980693085959118253207059548679690307328490636839758250039567538706177843900806538215196651572868475699024793180024569137445203317521713005377741509210448292153860491678535437161360781484487026555828935238740038455969445742565489105888068363740925919034628662375674670476913637282061530030579443917983663508609334512452143560011875002851725868487650431646892893383681621909670420193723214852049230276599683697743669098287062849912493295677286950361801745905899352472023039873545846566436911745120049114457852835966983926042630526234656651743673018211079875229224457303535160034180087302660602712026912319632840091923060085471831266753329919997588213706372914350997114413163206789267941283286841670639716609612021116545095394761536203818733472487433452130452980352876443406066422371361155668895113368100232714932065017142257275859839615607430266869420391608921786786370482252125243350596040412766472168490458567968532826502169214548924636210825524626301489200553434905798698586540260999429653035860526477140427505522056415286319013284213338618416811065591030697960521218356760963175557246156994287596324978259546112657111205977438283222658484252359350658499090482970773031901404963596116526681946335939906066781807411473861821690157711351974818186702260923699939675255229515757247770934628128207574829534735989988570974861522950370197524397921083296382509635768855008598515123487127581719575282073282873882697988778824217170948333413395284194305367062981073011819105903762196391614000433329039293017912649708362500728360632117147165718985995106640266291328970156606702254430137591531058804155139019960309005301639679517151763341132800840416493990907755450826309003058495228898912813982691649124217476296076842745119437924936388293423932707419187107250658321731094178688787334843830720201238769819353808831209424787870191234142317908406478850299646638521737457243259748943349779805374313779454081531966250437268102940100898176383248034483875393620163257666095505454561499231259039898363959102343842666278766762720791735187281257153975400695718926622376759093250904893864739558340954400103000304050739766931804473194602499823918757594175111325313704721990062284636587219938619509085569788718924515164203644369406817260161567235844900123020088686211490063633544858245767590560379786126612936682696617837276938078354807180624941083740825752849398158931602701680622550277521687173539878342359260973154561714977531518794739797769456123369156482339999734880242304715894209522612725725388742657549741700766830080462261339227498126835070123975279207808623922123906459796510386648903945178046483708485998117604375432087088225944002306225348612202774927374869007362350680563726246666045681551958933124324339173767651045416215527618811619274972678220458427673974208868753320032175821206088267307876273220541959336224811163726133635191771096322020555441294620620051396909241848089723092768253921102208386831377763928835758805310352.232205547611837787221050406167403739"
    };
    let input = format!("SELECT {literal}");
    let result = check_parse_both!(&input);
    if let SqlQuery::Select(SelectStatement { ref fields, .. }) = result {
        if let FieldDefinitionExpr::Expr { ref expr, .. } = fields[0] {
            assert_eq!(expr, &Expr::Literal(Literal::Number(literal.to_string())));
        } else {
            panic!("Expected expr in field list, got {fields:?}");
        }
    } else {
        panic!("Expected SELECT, got {result:?}");
    }
    assert_eq!(result.display(Dialect::PostgreSQL).to_string(), input);
}

#[test]
fn regression_numeric_with_no_leading_digit() {
    check_parse_both!("SELECT -.5");
    check_parse_both!("SELECT -5");
    check_parse_both!("SELECT -0");
    check_parse_both!("SELECT -.0");
}

#[test]
fn negating_large_numeric_literal() {
    check_parse_both!(format!("SELECT -{}", u64::MAX));
    check_parse_both!(format!("SELECT 1-{}", u64::MAX));
    // nom-sql can't handle numbers outside the u64 range, so it parses it as a column name
    check_parse_fails!(
        Dialect::MySQL,
        format!("SELECT -{}", u64::MAX as u128 + 1),
        "nom-sql AST differs from sqlparser-rs AST"
    );
}

#[test]
fn show_connections() {
    check_parse_both!("SHOW CONNECTIONS");
}

#[test]
fn show_events() {
    check_parse_both!("SHOW EVENTS");
}

#[test]
fn explain_domains() {
    check_parse_both!("EXPLAIN DOMAINS");
}

#[test]
fn explain_caches() {
    check_parse_both!("EXPLAIN CACHES");
}

#[test]
fn where_not_in_not_between() {
    check_parse_fails!(
        Dialect::PostgreSQL,
        "SELECT * FROM users WHERE status NOT IN (1, 2, 3) NOT BETWEEN true AND true;",
        "nom-sql error"
    );
}

#[test]
fn rename_table() {
    check_parse_both!("RENAME TABLE tb1 TO tb2");
    check_parse_fails!(
        Dialect::PostgreSQL,
        "ALTER TABLE tb1 RENAME TO tb2",
        "nom-sql AST differs"
    );
    check_parse_fails!(
        Dialect::MySQL,
        "ALTER TABLE tb1 RENAME TO tb2",
        "nom-sql AST differs"
    );
    check_parse_fails!(
        Dialect::MySQL,
        "ALTER TABLE tb1 RENAME AS tb2",
        "nom-sql AST differs"
    );
}

#[test]
#[ignore = "REA-5840"]
fn create_with_index_type() {
    check_parse_mysql!("CREATE TABLE foo (x int, UNIQUE KEY USING HASH (x))");
    check_parse_mysql!("CREATE TABLE foo (x int, UNIQUE KEY USING BTREE (x))");
}

#[test]
#[ignore = "REA-5841"]
fn create_with_unique_key_on_column() {
    check_parse_mysql!("CREATE TABLE foo (x int UNIQUE)");
    check_parse_mysql!("CREATE TABLE foo (x int UNIQUE KEY)");
}

#[test]
#[ignore = "REA-5842"]
fn create_with_unique_key_using_suffix() {
    check_parse_mysql!("CREATE TABLE users (id int, UNIQUE KEY id_k (id) USING HASH);");
    check_parse_mysql!("CREATE TABLE users (id int, UNIQUE KEY id_k (id) USING BTREE);");
}

#[test]
fn key_with_using_prefix() {
    check_parse_mysql!("CREATE TABLE t (x INT, INDEX idx_on_x USING BTREE (x))");
    check_parse_mysql!("CREATE TABLE t (x INT, KEY idx_on_x USING BTREE (x))");
    check_parse_mysql!("CREATE TABLE t (x INT, INDEX idx_on_x USING HASH (x))");
    check_parse_mysql!("CREATE TABLE t (x INT, KEY idx_on_x USING HASH (x))");
    check_parse_mysql!("CREATE TABLE t (x INT, UNIQUE INDEX idx_on_x USING BTREE (x))");
    check_parse_mysql!("CREATE TABLE t (x INT, UNIQUE KEY idx_on_x USING BTREE (x))");
    check_parse_mysql!("CREATE TABLE t (x INT, UNIQUE INDEX idx_on_x USING HASH (x))");
    check_parse_mysql!("CREATE TABLE t (x INT, UNIQUE KEY idx_on_x USING HASH (x))");
}

#[test]
fn create_table_options_comma_separated() {
    check_parse_mysql!("CREATE TABLE t (x int) AUTO_INCREMENT=1 COLLATE=utf8mb4_general_ci");
    check_parse_mysql!("CREATE TABLE t (x int) AUTO_INCREMENT=1, COLLATE=utf8mb4_general_ci");
    check_parse_fails!(
        Dialect::MySQL,
        "CREATE TABLE t (x int) AUTO_INCREMENT 1, COLLATE utf8mb4_general_ci",
        "nom-sql error"
    );
}

#[test]
fn create_table_options_without_equals() {
    check_parse_fails!(
        Dialect::MySQL,
        "CREATE TABLE t (x int) AUTO_INCREMENT 1 COLLATE utf8mb4_general_ci",
        "nom-sql error"
    );
}

#[test]
#[ignore = "REA-5844"]
fn column_varying_binary() {
    check_parse_mysql!("CREATE TABLE t (c varchar(255) binary)");
    check_parse_mysql!("CREATE TABLE t (c varchar(255) binary NOT NULL default '')");
}

#[test]
fn check_not_enforced_table_modifier() {
    check_parse_mysql!("CREATE TABLE t (x INT, CHECK (x > 1))");
    check_parse_mysql!("CREATE TABLE t (x INT, CHECK (x > 1) ENFORCED)");
    check_parse_mysql!("CREATE TABLE t (x INT, CHECK (x > 1) NOT ENFORCED)");
}

#[test]
#[ignore = "REA-5845"]
fn check_not_enforced_column_modifier() {
    check_parse_mysql!("CREATE TABLE t (x INT CHECK (x > 1))");
    check_parse_mysql!("CREATE TABLE t (x INT CHECK (x > 1) ENFORCED)");
    check_parse_mysql!("CREATE TABLE t (x INT CHECK (x > 1) NOT ENFORCED)");
}
#[test]
#[ignore = "REA-5846"]
fn check_not_enforced_anonymous_table_modifier() {
    check_parse_mysql!("CREATE TABLE t (x INT, CONSTRAINT CHECK (x > 1))");
    check_parse_mysql!("CREATE TABLE t (x INT, CONSTRAINT CHECK (x > 1) ENFORCED)");
    check_parse_mysql!("CREATE TABLE t (x INT, CONSTRAINT CHECK (x > 1) NOT ENFORCED)");
}

#[test]
#[ignore = "REA-5848"]
fn drop_prepare() {
    check_parse_mysql!("DROP PREPARE a42");
}

#[test]
#[ignore = "REA-5850"]
fn postgres_octal_escape() {
    check_parse_postgres!("SELECT E'\\0'");
    check_parse_postgres!("SELECT E'\\1'");
}

#[test]
fn postgres_bytea_casts() {
    // nom-sql turns $1::bytea into a byte array literal, while sqlparser leaves it as a cast. The
    // sqlparser behavior is desirable as it will make implementing REA-2574 easier.
    for sql in [
        "SELECT E'\\\\x0008275c6480'::bytea",
        "SELECT E'\\\\x'::bytea",
    ] {
        check_parse_fails!(
            Dialect::PostgreSQL,
            sql,
            "nom-sql AST differs from sqlparser-rs AST"
        );
    }
    // For some reason nom-sql doesn't do this conversion if the string is not hex
    check_parse_postgres!("SELECT E'\\\\'::bytea");
}

#[test]
fn postgres_hex_bytes_odd_digits() {
    check_parse_postgres!("SELECT X'0008275c6480';");
    check_parse_postgres!("SELECT X'0008275c6480';");
    check_parse_fails!(
        Dialect::PostgreSQL,
        format!("SELECT X'D617263656C6F'"),
        "Odd number of digits"
    );
}

#[test]
fn interval_type() {
    check_parse_type_both!("interval");
    check_parse_type_postgres!("interval day");
    check_parse_type_postgres!("INTERVAL HOUR TO MINUTE (4)");
}

#[test]
#[ignore = "REA-5858"]
fn truncate_restart() {
    check_parse_postgres!("truncate t1 *");
    check_parse_postgres!("truncate t1 *, t2*, t3");
}

#[test]
fn alter_table_lock_algorithm() {
    check_parse_mysql!("ALTER TABLE t1 LOCK=NONE");
    check_parse_mysql!("ALTER TABLE t1 LOCK= SHARED, ALGORITHM INPLACE");
    check_parse_mysql!("ALTER TABLE t1 ALGORITHM DEFAULT, LOCK=EXCLUSIVE");
    check_parse_mysql!("ALTER TABLE t1 ALGORITHM DEFAULT, DROP COLUMN foo, LOCK EXCLUSIVE");
}

#[test]
fn create_view() {
    check_parse_postgres!(
        r#"CREATE VIEW "v1" AS SELECT t1.i FROM (t1 JOIN t2 ON ((t1.i = t2.j)))"#
    );
}

#[test]
fn constraint_deferrable_timing() {
    // Table-level PRIMARY KEY constraints with DEFERRABLE
    check_parse_postgres!("CREATE TABLE test (id INT, CONSTRAINT pk PRIMARY KEY (id))");
    check_parse_postgres!("CREATE TABLE test (id INT, CONSTRAINT pk PRIMARY KEY (id) DEFERRABLE)");
    check_parse_postgres!(
        "CREATE TABLE test (id INT, CONSTRAINT pk PRIMARY KEY (id) DEFERRABLE INITIALLY DEFERRED)"
    );
    check_parse_postgres!(
        "CREATE TABLE test (id INT, CONSTRAINT pk PRIMARY KEY (id) DEFERRABLE INITIALLY IMMEDIATE)"
    );
    check_parse_postgres!(
        "CREATE TABLE test (id INT, CONSTRAINT pk PRIMARY KEY (id) NOT DEFERRABLE)"
    );
    check_parse_postgres!(
        "CREATE TABLE test (id INT, CONSTRAINT pk PRIMARY KEY (id) NOT DEFERRABLE INITIALLY IMMEDIATE)"
    );

    // Table-level UNIQUE constraints with DEFERRABLE
    check_parse_postgres!("CREATE TABLE test (id INT, CONSTRAINT uk UNIQUE (id))");
    check_parse_postgres!("CREATE TABLE test (id INT, CONSTRAINT uk UNIQUE (id) DEFERRABLE)");
    check_parse_postgres!(
        "CREATE TABLE test (id INT, CONSTRAINT uk UNIQUE (id) DEFERRABLE INITIALLY DEFERRED)"
    );
    check_parse_postgres!(
        "CREATE TABLE test (id INT, CONSTRAINT uk UNIQUE (id) DEFERRABLE INITIALLY IMMEDIATE)"
    );
    check_parse_postgres!("CREATE TABLE test (id INT, CONSTRAINT uk UNIQUE (id) NOT DEFERRABLE)");

    // Table-level FOREIGN KEY constraints with DEFERRABLE
    check_parse_postgres!(
        "CREATE TABLE test (id INT, CONSTRAINT fk FOREIGN KEY (id) REFERENCES other(id))"
    );
    check_parse_postgres!(
        "CREATE TABLE test (id INT, CONSTRAINT fk FOREIGN KEY (id) REFERENCES other(id) DEFERRABLE)"
    );
    check_parse_postgres!(
        "CREATE TABLE test (id INT, CONSTRAINT fk FOREIGN KEY (id) REFERENCES other(id) DEFERRABLE INITIALLY DEFERRED)"
    );
    check_parse_postgres!(
        "CREATE TABLE test (id INT, CONSTRAINT fk FOREIGN KEY (id) REFERENCES other(id) DEFERRABLE INITIALLY IMMEDIATE)"
    );
    check_parse_postgres!(
        "CREATE TABLE test (id INT, CONSTRAINT fk FOREIGN KEY (id) REFERENCES other(id) NOT DEFERRABLE)"
    );
    check_parse_postgres!(
        "CREATE TABLE test (id INT, CONSTRAINT fk FOREIGN KEY (id) REFERENCES other(id) NOT DEFERRABLE INITIALLY IMMEDIATE)"
    );

    // Multiple constraints with mixed timing
    check_parse_postgres!(
        "CREATE TABLE test (id INT, fid INT, CONSTRAINT pk PRIMARY KEY (id) DEFERRABLE INITIALLY DEFERRED,
        CONSTRAINT uk UNIQUE (fid) NOT DEFERRABLE, CONSTRAINT fk FOREIGN KEY (fid) REFERENCES other(id) DEFERRABLE INITIALLY IMMEDIATE)"
    );

    // Test INITIALLY without DEFERRABLE
    check_parse_postgres!(
        "CREATE TABLE test (id INT, CONSTRAINT pk PRIMARY KEY (id) INITIALLY IMMEDIATE)"
    );
    check_parse_postgres!(
        "CREATE TABLE test (id INT, CONSTRAINT pk PRIMARY KEY (id) INITIALLY DEFERRED)"
    );
}

#[test]
fn constraint_deferrable_timing_column_level() {
    // PRIMARY KEY constraints with DEFERRABLE
    check_parse_postgres!("CREATE TABLE test (id INT PRIMARY KEY)");
    check_parse_postgres!("CREATE TABLE test (id INT PRIMARY KEY DEFERRABLE)");
    check_parse_postgres!("CREATE TABLE test (id INT PRIMARY KEY DEFERRABLE INITIALLY DEFERRED)");
    check_parse_postgres!("CREATE TABLE test (id INT PRIMARY KEY DEFERRABLE INITIALLY IMMEDIATE)");
    check_parse_postgres!("CREATE TABLE test (id INT PRIMARY KEY NOT DEFERRABLE)");
    check_parse_postgres!(
        "CREATE TABLE test (id INT PRIMARY KEY NOT DEFERRABLE INITIALLY IMMEDIATE)"
    );

    // UNIQUE constraints with DEFERRABLE
    check_parse_postgres!("CREATE TABLE test (id INT UNIQUE)");
    check_parse_postgres!("CREATE TABLE test (id INT UNIQUE DEFERRABLE)");
    check_parse_postgres!("CREATE TABLE test (id INT UNIQUE DEFERRABLE INITIALLY DEFERRED)");
    check_parse_postgres!("CREATE TABLE test (id INT UNIQUE DEFERRABLE INITIALLY IMMEDIATE)");
    check_parse_postgres!("CREATE TABLE test (id INT UNIQUE NOT DEFERRABLE)");

    // INITIALLY without DEFERRABLE
    check_parse_postgres!("CREATE TABLE test (id INT PRIMARY KEY INITIALLY IMMEDIATE)");
    check_parse_postgres!("CREATE TABLE test (id INT PRIMARY KEY INITIALLY DEFERRED)");
    check_parse_postgres!("CREATE TABLE test (id INT UNIQUE INITIALLY IMMEDIATE)");
    check_parse_postgres!("CREATE TABLE test (id INT UNIQUE INITIALLY DEFERRED)");
}

#[test]
fn create_table_like() {
    check_parse_mysql!("CREATE TABLE a LIKE b");
    check_parse_fails!(
        Dialect::MySQL,
        "CREATE TEMPORARY TABLE a LIKE b",
        "NomSqlError { input: \"TEMPORARY TABLE"
    );
    check_parse_fails!(
        Dialect::MySQL,
        "CREATE TEMPORARY TABLE IF NOT EXISTS a LIKE b",
        "NomSqlError { input: \"TEMPORARY TABLE"
    );
    check_parse_mysql!("CREATE TABLE IF NOT EXISTS a LIKE b");
}

// TODO: Fix sqlparser upstream REA-6164
#[test]
#[should_panic = "nom-sql AST differs from sqlparser-rs AST"]
fn create_table_like_parenthesized() {
    check_parse_mysql!("CREATE TABLE a(LIKE b)");
    check_parse_mysql!("CREATE TABLE a (LIKE b)");
    check_parse_mysql!("CREATE TABLE IF NOT EXISTS a (LIKE b)");
}

#[test]
fn create_table_start_transaction() {
    check_parse_mysql!("CREATE TABLE a (x int) START TRANSACTION");
}

#[test]
fn drop_temporary_table_if_exists() {
    check_parse_mysql!("DROP TEMPORARY TABLE IF EXISTS users /* generated by server */;");
    check_parse_mysql!("DROP TABLE IF EXISTS users /* generated by server */;");
}

/// Tests for MySQL column position (FIRST, AFTER) in ALTER TABLE ADD COLUMN
#[test]
fn alter_table_add_column_position_first() {
    let sql = "ALTER TABLE t ADD COLUMN c INT FIRST";
    let ast = parse_query_with_config(ParsingPreset::OnlySqlparser, Dialect::MySQL, sql).unwrap();

    if let SqlQuery::AlterTable(alter) = ast {
        let defs = alter.definitions.unwrap();
        assert_eq!(defs.len(), 1);
        match &defs[0] {
            AlterTableDefinition::AddColumn { position, .. } => {
                assert_eq!(*position, Some(MySQLColumnPosition::First));
            }
            _ => panic!("Expected AddColumn"),
        }
    } else {
        panic!("Expected AlterTable");
    }
}

#[test]
fn alter_table_add_column_position_after() {
    let sql = "ALTER TABLE t ADD COLUMN c INT AFTER other_col";
    let ast = parse_query_with_config(ParsingPreset::OnlySqlparser, Dialect::MySQL, sql).unwrap();

    if let SqlQuery::AlterTable(alter) = ast {
        let defs = alter.definitions.unwrap();
        assert_eq!(defs.len(), 1);
        match &defs[0] {
            AlterTableDefinition::AddColumn { position, .. } => {
                assert_eq!(
                    *position,
                    Some(MySQLColumnPosition::After("other_col".into()))
                );
            }
            _ => panic!("Expected AddColumn"),
        }
    } else {
        panic!("Expected AlterTable");
    }
}

#[test]
fn alter_table_add_column_no_position() {
    let sql = "ALTER TABLE t ADD COLUMN c INT";
    let ast = parse_query_with_config(ParsingPreset::OnlySqlparser, Dialect::MySQL, sql).unwrap();

    if let SqlQuery::AlterTable(alter) = ast {
        let defs = alter.definitions.unwrap();
        assert_eq!(defs.len(), 1);
        match &defs[0] {
            AlterTableDefinition::AddColumn { position, .. } => {
                assert_eq!(*position, None);
            }
            _ => panic!("Expected AddColumn"),
        }
    } else {
        panic!("Expected AlterTable");
    }
}

/// Tests for MySQL column position (FIRST, AFTER) in ALTER TABLE CHANGE COLUMN
#[test]
fn alter_table_change_column_position_first() {
    let sql = "ALTER TABLE t CHANGE COLUMN old_col new_col INT FIRST";
    let ast = parse_query_with_config(ParsingPreset::OnlySqlparser, Dialect::MySQL, sql).unwrap();

    if let SqlQuery::AlterTable(alter) = ast {
        let defs = alter.definitions.unwrap();
        assert_eq!(defs.len(), 1);
        match &defs[0] {
            AlterTableDefinition::ChangeColumn { position, .. } => {
                assert_eq!(*position, Some(MySQLColumnPosition::First));
            }
            _ => panic!("Expected ChangeColumn"),
        }
    } else {
        panic!("Expected AlterTable");
    }
}

#[test]
fn alter_table_change_column_position_after() {
    let sql = "ALTER TABLE t CHANGE COLUMN old_col new_col INT AFTER other_col";
    let ast = parse_query_with_config(ParsingPreset::OnlySqlparser, Dialect::MySQL, sql).unwrap();

    if let SqlQuery::AlterTable(alter) = ast {
        let defs = alter.definitions.unwrap();
        assert_eq!(defs.len(), 1);
        match &defs[0] {
            AlterTableDefinition::ChangeColumn { position, .. } => {
                assert_eq!(
                    *position,
                    Some(MySQLColumnPosition::After("other_col".into()))
                );
            }
            _ => panic!("Expected ChangeColumn"),
        }
    } else {
        panic!("Expected AlterTable");
    }
}

#[test]
fn alter_table_add_column_position_after_reserved_word() {
    // Test that reserved words are correctly preserved in the AST
    let sql = "ALTER TABLE t ADD COLUMN c INT AFTER `select`";
    let ast = parse_query_with_config(ParsingPreset::OnlySqlparser, Dialect::MySQL, sql).unwrap();

    if let SqlQuery::AlterTable(alter) = ast {
        let defs = alter.definitions.unwrap();
        assert_eq!(defs.len(), 1);
        match &defs[0] {
            AlterTableDefinition::AddColumn { position, .. } => {
                assert_eq!(*position, Some(MySQLColumnPosition::After("select".into())));
            }
            _ => panic!("Expected AddColumn"),
        }
    } else {
        panic!("Expected AlterTable");
    }
}

#[test]
fn alter_table_add_column_position_after_special_chars() {
    // Test that identifiers with special characters are correctly preserved
    let sql = "ALTER TABLE t ADD COLUMN c INT AFTER `my-column`";
    let ast = parse_query_with_config(ParsingPreset::OnlySqlparser, Dialect::MySQL, sql).unwrap();

    if let SqlQuery::AlterTable(alter) = ast {
        let defs = alter.definitions.unwrap();
        assert_eq!(defs.len(), 1);
        match &defs[0] {
            AlterTableDefinition::AddColumn { position, .. } => {
                assert_eq!(
                    *position,
                    Some(MySQLColumnPosition::After("my-column".into()))
                );
            }
            _ => panic!("Expected AddColumn"),
        }
    } else {
        panic!("Expected AlterTable");
    }
}
