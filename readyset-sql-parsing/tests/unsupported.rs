//! Tests for queries that should be rejected as unsupported.

use readyset_sql::Dialect;
use readyset_sql_parsing::{ParsingPreset, parse_query_with_config};

mod utils;

#[test]
fn postgres_like_escape_char() {
    for query in &[
        "SELECT * FROM users WHERE name LIKE 'John#_Doe' ESCAPE '#'",
        "SELECT * FROM users WHERE name ILIKE 'John!%' ESCAPE '!'",
        "SELECT * FROM products WHERE sku NOT LIKE '!%OFF' ESCAPE '!'",
        "SELECT * FROM products WHERE name NOT ILIKE 'test#%' ESCAPE '#'",
        "SELECT * FROM paths WHERE path LIKE 'C:\\Users\\%' ESCAPE ''",
        "SELECT * FROM users WHERE name LIKE ANY(ARRAY['test#%', 'demo#_']) ESCAPE '#'",
        "SELECT * FROM users WHERE name ILIKE ANY(ARRAY['test!%']) ESCAPE '!'",
        // Various escape characters
        "SELECT * FROM t WHERE col LIKE 'pattern' ESCAPE '!'",
        "SELECT * FROM t WHERE col LIKE 'pattern' ESCAPE '#'",
        "SELECT * FROM t WHERE col LIKE 'pattern' ESCAPE '@'",
        "SELECT * FROM t WHERE col LIKE 'pattern' ESCAPE '$'",
        "SELECT * FROM t WHERE col LIKE 'pattern' ESCAPE '^'",
        "SELECT * FROM t WHERE col LIKE 'pattern' ESCAPE '&'",
        "SELECT * FROM t WHERE col LIKE 'pattern' ESCAPE '*'",
        "SELECT * FROM t WHERE col LIKE 'pattern' ESCAPE '~'",
        "SELECT * FROM t WHERE col LIKE 'pattern' ESCAPE '|'",
    ] {
        check_parse_fails!(
            Dialect::PostgreSQL,
            query,
            "LIKE/ILIKE with custom ESCAPE character"
        );
    }
}

#[test]
fn mysql_like_escape_char() {
    for query in &[
        "SELECT * FROM users WHERE name LIKE 'John#_Doe' ESCAPE '#'",
        "SELECT * FROM products WHERE sku NOT LIKE '!%OFF' ESCAPE '!'",
        "SELECT * FROM paths WHERE path LIKE 'C:\\Users\\%' ESCAPE ''",
        // Various escape characters
        "SELECT * FROM t WHERE col LIKE 'pattern' ESCAPE '!'",
        "SELECT * FROM t WHERE col LIKE 'pattern' ESCAPE '#'",
        "SELECT * FROM t WHERE col LIKE 'pattern' ESCAPE '@'",
        "SELECT * FROM t WHERE col LIKE 'pattern' ESCAPE '$'",
    ] {
        check_parse_fails!(
            Dialect::MySQL,
            query,
            "LIKE/ILIKE with custom ESCAPE character"
        );
    }
}

#[test]
fn cast_format_unsupported() {
    for query in &[
        "SELECT CAST(col AS STRING FORMAT 'YYYY-MM-DD') FROM t",
        "SELECT TRY_CAST(col AS TIMESTAMP FORMAT 'MM/DD/YY') FROM t",
        "SELECT SAFE_CAST(col AS DATE FORMAT 'YYYY') FROM t",
        "SELECT CAST(ts AS STRING FORMAT 'TZH' AT TIME ZONE 'UTC') FROM t",
    ] {
        check_parse_fails!(Dialect::PostgreSQL, query, "CAST with FORMAT clause");
    }
}

#[test]
fn function_advanced_features_unsupported() {
    // Test that advanced function features are rejected
    for query in &[
        "SELECT COUNT(*) FILTER (WHERE x > 0) FROM t",
        "SELECT SUM(x) WITHIN GROUP (ORDER BY y) FROM t",
        "SELECT FIRST_VALUE(x) IGNORE NULLS OVER (ORDER BY y)",
    ] {
        check_parse_fails!(Dialect::PostgreSQL, query, "Function with");
    }
}

#[test]
fn character_length_octets_unsupported() {
    // Test that character length with OCTETS is rejected
    for query in &[
        "CREATE TABLE t (col VARCHAR(100 OCTETS))",
        "CREATE TABLE t (col CHAR(50 OCTETS))",
        "CREATE TABLE t (col CHARACTER(25 OCTETS))",
    ] {
        check_parse_fails!(Dialect::PostgreSQL, query, "OCTETS");
    }
}

#[test]
fn time_with_timezone_unsupported() {
    // Test that TIME WITH TIME ZONE is rejected
    check_parse_fails!(
        Dialect::PostgreSQL,
        "SELECT CAST('12:00:00' AS TIME WITH TIME ZONE)",
        "TIME WITH TIME ZONE"
    );
}

#[test]
fn table_alias_column_renaming_unsupported() {
    // Test that table alias with column renaming is rejected
    for query in &[
        "SELECT * FROM users AS u(user_id, user_name)",
        "SELECT * FROM (SELECT id, name FROM users) AS u(user_id, user_name)",
    ] {
        check_parse_fails!(Dialect::PostgreSQL, query, "column renaming");
    }
}

#[test]
fn create_table_unsupported_clauses() {
    // Test that unsupported CREATE TABLE clauses are rejected
    for query in &[
        "CREATE TEMPORARY TABLE tmp (id INT)",
        "CREATE GLOBAL TEMPORARY TABLE tmp (id INT)",
        "CREATE EXTERNAL TABLE ext (id INT)",
        "CREATE TABLE new_t AS SELECT 1",
        "CREATE TABLE ordered (id INT) ORDER BY (id)",
        "CREATE TABLE part (id INT) PARTITION BY (id)",
    ] {
        check_parse_fails!(Dialect::PostgreSQL, query, "not supported");
    }
}
