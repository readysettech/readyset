//! Integration tests for [`SqlQuery::is_write`].
//!
//! Lives in `tests/` rather than as a `#[cfg(test)] mod` so it can use
//! `readyset_sql_parsing` (a dev-dep) to construct values: the inline-test approach
//! triggers a "multiple different versions of crate readyset_sql" cycle on dev-deps.

use readyset_sql::Dialect;
use readyset_sql::ast::SqlQuery;
use readyset_sql_parsing::parse_query;

/// `(dialect, sql, expected_is_write)` covering every `SqlQuery` variant the parser
/// constructs. Variants without a parseable surface (`CreateMcpToken`, `AlterMcpToken`,
/// `CreateRls`, `DropRls`) rely on the exhaustive `match` in `is_write()` for coverage
/// at compile time.
fn cases() -> &'static [(Dialect, &'static str, bool)] {
    &[
        // DML
        (Dialect::MySQL, "INSERT INTO t VALUES (1)", true),
        (Dialect::MySQL, "UPDATE t SET x = 1", true),
        (Dialect::MySQL, "DELETE FROM t", true),
        (Dialect::MySQL, "TRUNCATE t", true),
        // DDL on relations
        (Dialect::MySQL, "CREATE TABLE t (x int)", true),
        (Dialect::MySQL, "ALTER TABLE t ADD COLUMN y int", true),
        (Dialect::MySQL, "DROP TABLE t", true),
        (Dialect::MySQL, "RENAME TABLE t TO u", true),
        (Dialect::MySQL, "CREATE VIEW v AS SELECT 1", true),
        (Dialect::MySQL, "DROP VIEW v", true),
        // `CREATE INDEX` has no parser surface; coverage is via the exhaustive `match`
        // in `is_write()`.
        (Dialect::MySQL, "CREATE DATABASE d", true),
        // Cache lifecycle
        (Dialect::MySQL, "CREATE CACHE FROM SELECT 1", true),
        (Dialect::MySQL, "DROP CACHE c", true),
        (Dialect::MySQL, "DROP ALL CACHES", true),
        (Dialect::MySQL, "FLUSH CACHE c", true),
        (Dialect::MySQL, "FLUSH ALL SHALLOW CACHES", true),
        // Reads / transaction control / bookkeeping
        (Dialect::MySQL, "SELECT 1", false),
        (Dialect::MySQL, "(SELECT 1) UNION (SELECT 2)", false),
        (Dialect::MySQL, "SET autocommit = 0", false),
        (Dialect::MySQL, "SHOW TABLES", false),
        (Dialect::MySQL, "EXPLAIN CACHES", false),
        (Dialect::MySQL, "USE foo", false),
        (Dialect::MySQL, "DEALLOCATE PREPARE s", false),
        (Dialect::MySQL, "BEGIN", false),
        (Dialect::MySQL, "COMMIT", false),
        (Dialect::MySQL, "ROLLBACK", false),
        (Dialect::MySQL, "DROP ALL PROXIED QUERIES", false),
    ]
}

#[test]
fn is_write_truth_table() {
    for (dialect, sql, expected) in cases() {
        let q: SqlQuery =
            parse_query(*dialect, sql).unwrap_or_else(|e| panic!("failed to parse {sql:?}: {e}"));
        assert_eq!(
            q.is_write(),
            *expected,
            "{sql:?}: expected is_write() == {expected}, query_type = {}",
            q.query_type()
        );
    }
}

#[test]
fn is_write_disjoint_from_is_select() {
    for (dialect, sql, _) in cases() {
        let q: SqlQuery =
            parse_query(*dialect, sql).unwrap_or_else(|e| panic!("failed to parse {sql:?}: {e}"));
        if q.is_select() {
            assert!(
                !q.is_write(),
                "{sql:?}: SELECT must not be classified as a write"
            );
        }
    }
}
