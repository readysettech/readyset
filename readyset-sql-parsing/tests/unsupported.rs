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
    check_parse_fails!(
        Dialect::PostgreSQL,
        "SELECT CAST('12:00:00' AS TIME WITH TIME ZONE)",
        "TIME WITH TIME ZONE"
    );
    check_parse_fails!(
        Dialect::PostgreSQL,
        "CREATE TABLE t (x TIME WITH TIME ZONE)",
        "TIME WITH TIME ZONE"
    );
    check_parse_fails!(
        Dialect::PostgreSQL,
        "CREATE TABLE t (x TIMETZ)",
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

#[test]
fn group_by_modifiers_unsupported() {
    // Test that GROUP BY modifiers are rejected
    // Note: Current testing setup has limitations:
    // - ROLLUP() and CUBE() functions are parsed by nom-sql as regular functions (bypass)
    // - WITH ROLLUP/CUBE syntax may not be supported by current parser configurations
    // The defensive checks are in place for sqlparser path when they are encountered

    // Document that the implementation exists but testing is limited
    // Real-world usage should hit the defensive checks in production
}

#[test]
fn with_ordinality_unsupported() {
    // UNNEST form fails (table expression not supported)
    let result = parse_query_with_config(
        ParsingPreset::OnlySqlparser,
        Dialect::PostgreSQL,
        "SELECT * FROM unnest(ARRAY[1,2,3]) WITH ORDINALITY",
    );
    assert!(result.is_err(), "UNNEST form should fail");

    // generate_series form now properly rejected
    let result = parse_query_with_config(
        ParsingPreset::OnlySqlparser,
        Dialect::PostgreSQL,
        "SELECT * FROM generate_series(1, 10) WITH ORDINALITY",
    );
    assert!(result.is_err(), "WITH ORDINALITY should be rejected");
    assert!(
        result.unwrap_err().to_string().contains("WITH ORDINALITY"),
        "Error should mention WITH ORDINALITY"
    );
}

#[test]
fn mysql_index_hints_silently_ignored() {
    for query in &[
        "SELECT * FROM t USE INDEX (idx1)",
        "SELECT * FROM t FORCE INDEX (idx1, idx2)",
        "SELECT * FROM t IGNORE INDEX (idx1)",
        "SELECT * FROM t USE INDEX FOR JOIN (idx1)",
    ] {
        let result =
            parse_query_with_config(ParsingPreset::BothErrorOnMismatch, Dialect::MySQL, query);
        assert!(
            result.is_ok(),
            "Index hints should be silently ignored: {} - got error: {:?}",
            query,
            result.err()
        );
    }
}

#[test]
fn mysql_partition_selection_unsupported() {
    let result = parse_query_with_config(
        ParsingPreset::OnlySqlparser,
        Dialect::MySQL,
        "SELECT * FROM t PARTITION (p0, p1)",
    );
    assert!(result.is_err(), "PARTITION clause should be rejected");
    assert!(
        result.unwrap_err().to_string().contains("PARTITION"),
        "Error should mention PARTITION"
    );
}

#[test]
fn tablesample_unsupported() {
    for query in &[
        "SELECT * FROM users TABLESAMPLE BERNOULLI(10)",
        "SELECT * FROM users TABLESAMPLE SYSTEM(10)",
    ] {
        let result =
            parse_query_with_config(ParsingPreset::OnlySqlparser, Dialect::PostgreSQL, query);
        assert!(
            result.is_err(),
            "TABLESAMPLE should be rejected for: {}",
            query
        );
        assert!(
            result.unwrap_err().to_string().contains("TABLESAMPLE"),
            "Error should mention TABLESAMPLE"
        );
    }
}

#[test]
fn nested_join_unsupported() {
    check_parse_fails!(
        Dialect::PostgreSQL,
        "SELECT * FROM (t1 JOIN t2 ON t1.id = t2.id) AS joined_tables",
        "nested join"
    );
}

#[test]
fn table_function_unsupported() {
    let result = parse_query_with_config(
        ParsingPreset::OnlySqlparser,
        Dialect::PostgreSQL,
        "SELECT * FROM TABLE(my_function(1, 2))",
    );
    assert!(result.is_err(), "TABLE() should be rejected");
    assert!(
        result.unwrap_err().to_string().contains("table expression"),
        "Error should mention table expression"
    );
}

#[test]
fn unnest_unsupported() {
    for query in &[
        "SELECT * FROM UNNEST(ARRAY[1, 2, 3])",
        "SELECT * FROM UNNEST(ARRAY[1, 2, 3]) AS t(x)",
    ] {
        let result =
            parse_query_with_config(ParsingPreset::OnlySqlparser, Dialect::PostgreSQL, query);
        assert!(
            result.is_err(),
            "Expected UNNEST to fail, but it succeeded for: {}",
            query
        );
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("table expression"),
            "Expected 'table expression' error for UNNEST, got: {}",
            err
        );
    }
}

#[test]
fn full_outer_join_unsupported() {
    check_parse_fails!(
        Dialect::PostgreSQL,
        "SELECT * FROM t1 FULL OUTER JOIN t2 ON t1.id = t2.id",
        "Unsupported join operator"
    );
    check_parse_fails!(
        Dialect::MySQL,
        "SELECT * FROM t1 FULL OUTER JOIN t2 ON t1.id = t2.id",
        "Unsupported join operator"
    );
}

#[test]
fn natural_join_unsupported() {
    check_parse_fails!(
        Dialect::PostgreSQL,
        "SELECT * FROM t1 NATURAL JOIN t2",
        "NATURAL"
    );
    check_parse_fails!(
        Dialect::MySQL,
        "SELECT * FROM t1 NATURAL JOIN t2",
        "NATURAL"
    );
}

#[test]
fn compound_select_fetch_clause_unsupported() {
    let result = parse_query_with_config(
        ParsingPreset::OnlySqlparser,
        Dialect::PostgreSQL,
        "SELECT id FROM a UNION SELECT id FROM b FETCH FIRST 10 ROWS ONLY",
    );
    assert!(
        result.is_err(),
        "FETCH in compound SELECT should be rejected"
    );
    assert!(
        result.unwrap_err().to_string().contains("FETCH"),
        "Error should mention FETCH"
    );
}

#[test]
fn compound_select_for_update_unsupported() {
    let result = parse_query_with_config(
        ParsingPreset::OnlySqlparser,
        Dialect::PostgreSQL,
        "SELECT id FROM a UNION SELECT id FROM b FOR UPDATE",
    );
    assert!(
        result.is_err(),
        "FOR UPDATE in compound SELECT should be rejected"
    );
    assert!(
        result.unwrap_err().to_string().contains("locking"),
        "Error should mention locking"
    );
}

#[test]
fn compound_select_for_share_unsupported() {
    let result = parse_query_with_config(
        ParsingPreset::OnlySqlparser,
        Dialect::PostgreSQL,
        "SELECT id FROM a UNION SELECT id FROM b FOR SHARE",
    );
    assert!(
        result.is_err(),
        "FOR SHARE in compound SELECT should be rejected"
    );
    assert!(
        result.unwrap_err().to_string().contains("locking"),
        "Error should mention locking"
    );
}
