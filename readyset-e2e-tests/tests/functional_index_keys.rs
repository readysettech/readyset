//! Tests for functional index expressions in unique keys and regular indexes.
//!
//! MySQL 8.0+ supports functional indexes (e.g., `KEY idx ((expr))`) but with restrictions:
//! - PRIMARY KEY cannot use functional expressions (MySQL error 3756)
//! - UNIQUE KEY CAN use functional expressions
//! - Regular KEY (non-unique index) CAN use functional expressions
//!
//! When a unique key consists entirely of functional expressions and there's no usable
//! fallback key, the table should be marked as non-replicated by ReadySet.
//!
//! Tables with functional KEY indexes (non-unique, non-primary) like
//! `KEY idx ((CAST(col AS UNSIGNED ARRAY)))` can be snapshotted and replicated correctly.

use mysql_async::prelude::Queryable;
use readyset_client_test_helpers::mysql_helpers::{self, MySQLAdapter};
use readyset_client_test_helpers::{TestBuilder, sleep};
use readyset_sql::ast::{Relation, SqlIdentifier};
use readyset_sql_parsing::ParsingPreset;
use readyset_util::eventually;
use test_utils::{tags, upstream};

/// Test that a table with no primary key but a unique key consisting only of functional
/// expressions is marked as non-replicated.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(mysql80, mysql84)]
async fn functional_uk_only_rejects_table() {
    readyset_tracing::init_test_logging();
    let db_name = "functional_uk_only_test";
    mysql_helpers::recreate_database(db_name).await;

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(db_name));
    let mut upstream_conn = mysql_async::Conn::new(upstream_opts).await.unwrap();

    // Create a table with no primary key and a functional-only unique key.
    upstream_conn
        .query_drop(
            "CREATE TABLE t_func_uk (
                id INT NOT NULL,
                col INT NOT NULL,
                UNIQUE KEY uk1 ((col * 2))
            )",
        )
        .await
        .unwrap();

    let (_rs_opts, mut handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .replicate_db(db_name.to_string())
        .parsing_preset(ParsingPreset::OnlySqlparser)
        .build::<MySQLAdapter>()
        .await;

    // Wait for replication to attempt processing the table
    sleep().await;
    sleep().await;

    // The table should be marked as non-replicated
    eventually!(run_test: {
        handle.non_replicated_relations().await.unwrap()
    }, then_assert: |result| {
        let relation = Relation {
            schema: Some(SqlIdentifier::from(db_name)),
            name: SqlIdentifier::from("t_func_uk"),
        };
        let has_table = result.iter().any(|r| r.name == relation);
        assert!(
            has_table,
            "Table t_func_uk should be non-replicated. Non-replicated tables: {:?}",
            result
        );
    });

    shutdown_tx.shutdown().await;

    upstream_conn
        .query_drop(format!("DROP DATABASE {db_name}"))
        .await
        .unwrap();
}

// NOTE: The test functional_pk_with_usable_uk_succeeds was removed because MySQL 8 does not
// support functional expressions in PRIMARY KEY definitions (error 3756: "The primary key
// cannot be a functional index"). The test concept (falling back to a usable UK when PK is
// unusable) cannot be tested with functional PKs in MySQL.

/// Test that a table with no PK, first UK is functional-only, but second UK is usable
/// IS replicated (uses the second unique key for snapshotting).
#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(mysql80, mysql84)]
async fn no_pk_functional_first_uk_usable_second_uk_succeeds() {
    readyset_tracing::init_test_logging();
    let db_name = "no_pk_fallback_uk_test";
    mysql_helpers::recreate_database(db_name).await;

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(db_name));
    let mut upstream_conn = mysql_async::Conn::new(upstream_opts).await.unwrap();

    // Create a table with no PK, first UK is functional-only, second UK is usable.
    upstream_conn
        .query_drop(
            "CREATE TABLE t_uk_fallback (
                id INT NOT NULL,
                col INT NOT NULL,
                UNIQUE KEY uk_func ((col * 2)),
                UNIQUE KEY uk_id (id)
            )",
        )
        .await
        .unwrap();

    // Insert some test data
    upstream_conn
        .query_drop("INSERT INTO t_uk_fallback (id, col) VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .unwrap();

    let (rs_opts, mut handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .replicate_db(db_name.to_string())
        .parsing_preset(ParsingPreset::OnlySqlparser)
        .build::<MySQLAdapter>()
        .await;

    let mut rs_conn = mysql_async::Conn::new(rs_opts).await.unwrap();

    // Wait for replication
    sleep().await;
    sleep().await;

    // The table should NOT be in non-replicated tables
    eventually!(run_test: {
        handle.non_replicated_relations().await.unwrap()
    }, then_assert: |result| {
        let relation = Relation {
            schema: Some(SqlIdentifier::from(db_name)),
            name: SqlIdentifier::from("t_uk_fallback"),
        };
        let has_table = result.iter().any(|r| r.name == relation);
        assert!(
            !has_table,
            "Table t_uk_fallback should be replicated (not in non-replicated list). Non-replicated tables: {:?}",
            result
        );
    });

    // Verify the data was snapshotted correctly
    eventually!(run_test: {
        let rows: Vec<(i32, i32)> = rs_conn
            .query("SELECT id, col FROM t_uk_fallback ORDER BY id")
            .await
            .unwrap();
        rows
    }, then_assert: |result| {
        assert_eq!(result.len(), 3, "Should have 3 rows");
        assert_eq!(result[0], (1, 10));
        assert_eq!(result[1], (2, 20));
        assert_eq!(result[2], (3, 30));
    });

    shutdown_tx.shutdown().await;

    upstream_conn
        .query_drop(format!("DROP DATABASE {db_name}"))
        .await
        .unwrap();
}

/// Tests that tables with functional index columns (like CAST...ARRAY in MySQL 8) can be
/// snapshotted. The functional index columns should be skipped for regular KEY indexes,
/// allowing the table to be processed successfully.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(mysql80, mysql84)]
async fn test_functional_index_snapshot() {
    readyset_tracing::init_test_logging();
    let db_name = "func_index_snapshot_test";
    mysql_helpers::recreate_database(db_name).await;

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(db_name));
    let mut upstream_conn = mysql_async::Conn::new(upstream_opts).await.unwrap();

    // Create table with a functional index using CAST...ARRAY (MySQL 8 feature)
    upstream_conn
        .query_drop(
            "CREATE TABLE functional_index_test (
                id INT PRIMARY KEY,
                data JSON,
                KEY idx_func ((CAST(data->'$.tags' AS UNSIGNED ARRAY)))
            )",
        )
        .await
        .unwrap();

    // Insert test data
    upstream_conn
        .query_drop(
            r#"INSERT INTO functional_index_test (id, data) VALUES
                (1, '{"tags": [1, 2, 3]}'),
                (2, '{"tags": [4, 5]}'),
                (3, '{"tags": [1, 6]}')"#,
        )
        .await
        .unwrap();

    // Setup ReadySet connection after table creation (triggers snapshot)
    let (rs_opts, _rs_handle, tx) = TestBuilder::default()
        .recreate_database(false)
        .replicate_db(db_name.to_string())
        .parsing_preset(ParsingPreset::OnlySqlparser)
        .build::<MySQLAdapter>()
        .await;
    let mut rs_conn = mysql_async::Conn::new(rs_opts).await.unwrap();
    sleep().await;

    // Create a cache and verify data is accessible
    rs_conn
        .query_drop("CREATE CACHE FROM SELECT id, data FROM functional_index_test WHERE id = ?")
        .await
        .unwrap();
    sleep().await;

    // Verify we can read the snapshotted data
    let rows: Vec<(i32, String)> = rs_conn
        .exec(
            "SELECT id, data FROM functional_index_test WHERE id = ?",
            (1,),
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, 1);

    tx.shutdown().await;

    upstream_conn
        .query_drop(format!("DROP DATABASE {db_name}"))
        .await
        .unwrap();
}

/// Tests that tables with functional index columns can be created during streaming replication.
/// The CREATE TABLE statement comes through the binlog and should be parsed successfully.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(mysql80, mysql84)]
async fn test_functional_index_replication() {
    readyset_tracing::init_test_logging();
    let db_name = "func_index_repl_test";
    mysql_helpers::recreate_database(db_name).await;

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(db_name));
    let mut upstream_conn = mysql_async::Conn::new(upstream_opts).await.unwrap();

    // Setup ReadySet connection BEFORE table creation (so CREATE TABLE comes via replication)
    let (rs_opts, _rs_handle, tx) = TestBuilder::default()
        .recreate_database(false)
        .replicate_db(db_name.to_string())
        .parsing_preset(ParsingPreset::OnlySqlparser)
        .build::<MySQLAdapter>()
        .await;
    let mut rs_conn = mysql_async::Conn::new(rs_opts).await.unwrap();

    // Now create the table - this will be replicated to ReadySet
    upstream_conn
        .query_drop(
            "CREATE TABLE functional_index_repl_test (
                id INT PRIMARY KEY,
                data JSON,
                KEY idx_func ((CAST(data->'$.tags' AS UNSIGNED ARRAY)))
            )",
        )
        .await
        .unwrap();
    sleep().await;

    // Insert test data via replication
    upstream_conn
        .query_drop(
            r#"INSERT INTO functional_index_repl_test (id, data) VALUES
                (1, '{"tags": [1, 2, 3]}'),
                (2, '{"tags": [4, 5]}')"#,
        )
        .await
        .unwrap();
    sleep().await;

    // Create a cache and verify data is accessible
    rs_conn
        .query_drop(
            "CREATE CACHE FROM SELECT id, data FROM functional_index_repl_test WHERE id = ?",
        )
        .await
        .unwrap();
    sleep().await;

    // Verify we can read the replicated data
    let rows: Vec<(i32, String)> = rs_conn
        .exec(
            "SELECT id, data FROM functional_index_repl_test WHERE id = ?",
            (1,),
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, 1);

    tx.shutdown().await;

    upstream_conn
        .query_drop(format!("DROP DATABASE {db_name}"))
        .await
        .unwrap();
}

/// Tests that tables with mixed functional and regular index columns work correctly
/// during snapshotting. The functional columns should be filtered out, keeping only
/// the regular column references.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(mysql80, mysql84)]
async fn test_mixed_functional_index_snapshot() {
    readyset_tracing::init_test_logging();
    let db_name = "mixed_index_snapshot_test";
    mysql_helpers::recreate_database(db_name).await;

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(db_name));
    let mut upstream_conn = mysql_async::Conn::new(upstream_opts).await.unwrap();

    // Create table with a mixed index (regular column + functional expression)
    upstream_conn
        .query_drop(
            "CREATE TABLE mixed_index_test (
                id INT PRIMARY KEY,
                store_uuid VARCHAR(36),
                data JSON,
                has_photo TINYINT,
                KEY idx_mixed (store_uuid, (CAST(data->'$.tags' AS UNSIGNED ARRAY)), has_photo)
            )",
        )
        .await
        .unwrap();

    // Insert test data
    upstream_conn
        .query_drop(
            r#"INSERT INTO mixed_index_test (id, store_uuid, data, has_photo) VALUES
                (1, 'abc-123', '{"tags": [1, 2]}', 1),
                (2, 'def-456', '{"tags": [3]}', 0)"#,
        )
        .await
        .unwrap();

    // Setup ReadySet connection after table creation (triggers snapshot)
    let (rs_opts, _rs_handle, tx) = TestBuilder::default()
        .recreate_database(false)
        .replicate_db(db_name.to_string())
        .parsing_preset(ParsingPreset::OnlySqlparser)
        .build::<MySQLAdapter>()
        .await;
    let mut rs_conn = mysql_async::Conn::new(rs_opts).await.unwrap();
    sleep().await;

    // Create a cache and verify data is accessible
    rs_conn
        .query_drop("CREATE CACHE FROM SELECT * FROM mixed_index_test WHERE id = ?")
        .await
        .unwrap();
    sleep().await;

    // Verify we can read the snapshotted data
    let rows: Vec<(i32, String, String, i32)> = rs_conn
        .exec("SELECT * FROM mixed_index_test WHERE id = ?", (1,))
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, 1);
    assert_eq!(rows[0].1, "abc-123");
    assert_eq!(rows[0].3, 1);

    tx.shutdown().await;

    upstream_conn
        .query_drop(format!("DROP DATABASE {db_name}"))
        .await
        .unwrap();
}

/// Tests that tables with mixed functional and regular index columns work correctly
/// during streaming replication. The CREATE TABLE comes through the binlog.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(mysql80, mysql84)]
async fn test_mixed_functional_index_replication() {
    readyset_tracing::init_test_logging();
    let db_name = "mixed_index_repl_test";
    mysql_helpers::recreate_database(db_name).await;

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(db_name));
    let mut upstream_conn = mysql_async::Conn::new(upstream_opts).await.unwrap();

    // Setup ReadySet connection BEFORE table creation (so CREATE TABLE comes via replication)
    let (rs_opts, _rs_handle, tx) = TestBuilder::default()
        .recreate_database(false)
        .replicate_db(db_name.to_string())
        .parsing_preset(ParsingPreset::OnlySqlparser)
        .build::<MySQLAdapter>()
        .await;
    let mut rs_conn = mysql_async::Conn::new(rs_opts).await.unwrap();

    // Now create the table - this will be replicated to ReadySet
    upstream_conn
        .query_drop(
            "CREATE TABLE mixed_index_repl_test (
                id INT PRIMARY KEY,
                store_uuid VARCHAR(36),
                data JSON,
                has_photo TINYINT,
                KEY idx_mixed (store_uuid, (CAST(data->'$.tags' AS UNSIGNED ARRAY)), has_photo)
            )",
        )
        .await
        .unwrap();
    sleep().await;

    // Insert test data via replication
    upstream_conn
        .query_drop(
            r#"INSERT INTO mixed_index_repl_test (id, store_uuid, data, has_photo) VALUES
                (1, 'abc-123', '{"tags": [1, 2]}', 1),
                (2, 'def-456', '{"tags": [3]}', 0)"#,
        )
        .await
        .unwrap();
    sleep().await;

    // Create a cache and verify data is accessible
    rs_conn
        .query_drop("CREATE CACHE FROM SELECT * FROM mixed_index_repl_test WHERE id = ?")
        .await
        .unwrap();
    sleep().await;

    // Verify we can read the replicated data
    let rows: Vec<(i32, String, String, i32)> = rs_conn
        .exec("SELECT * FROM mixed_index_repl_test WHERE id = ?", (1,))
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, 1);
    assert_eq!(rows[0].1, "abc-123");
    assert_eq!(rows[0].3, 1);

    tx.shutdown().await;

    upstream_conn
        .query_drop(format!("DROP DATABASE {db_name}"))
        .await
        .unwrap();
}

/// Tests that UPDATE and DELETE operations work correctly on tables with mixed functional
/// and regular index columns. This verifies that row identity is correctly tracked using
/// the primary key even when the table has functional index expressions.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(mysql80, mysql84)]
async fn test_mixed_functional_index_update_delete() {
    readyset_tracing::init_test_logging();
    let db_name = "mixed_index_update_delete_test";
    mysql_helpers::recreate_database(db_name).await;

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(db_name));
    let mut upstream_conn = mysql_async::Conn::new(upstream_opts).await.unwrap();

    // Create table with PK and a mixed functional index
    upstream_conn
        .query_drop(
            "CREATE TABLE t_update_delete (
                id INT PRIMARY KEY,
                name VARCHAR(50),
                data JSON,
                KEY idx_mixed (name, (CAST(data->'$.values' AS UNSIGNED ARRAY)))
            )",
        )
        .await
        .unwrap();

    // Insert initial data
    upstream_conn
        .query_drop(
            r#"INSERT INTO t_update_delete (id, name, data) VALUES
                (1, 'alice', '{"values": [10, 20]}'),
                (2, 'bob', '{"values": [30]}'),
                (3, 'charlie', '{"values": [40, 50, 60]}')"#,
        )
        .await
        .unwrap();

    // Setup ReadySet after data is inserted (triggers snapshot)
    let (rs_opts, _rs_handle, tx) = TestBuilder::default()
        .recreate_database(false)
        .replicate_db(db_name.to_string())
        .parsing_preset(ParsingPreset::OnlySqlparser)
        .build::<MySQLAdapter>()
        .await;
    let mut rs_conn = mysql_async::Conn::new(rs_opts).await.unwrap();
    sleep().await;

    // Create a cache
    rs_conn
        .query_drop("CREATE CACHE FROM SELECT id, name, data FROM t_update_delete WHERE id = ?")
        .await
        .unwrap();
    sleep().await;

    // Verify initial data
    eventually!(run_test: {
        let rows: Vec<(i32, String, String)> = rs_conn
            .exec("SELECT id, name, data FROM t_update_delete WHERE id = ?", (1,))
            .await
            .unwrap();
        rows
    }, then_assert: |result| {
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].1, "alice");
    });

    // UPDATE a row upstream
    upstream_conn
        .query_drop("UPDATE t_update_delete SET name = 'alice_updated' WHERE id = 1")
        .await
        .unwrap();
    sleep().await;

    // Verify the update is reflected in ReadySet
    eventually!(run_test: {
        let rows: Vec<(i32, String, String)> = rs_conn
            .exec("SELECT id, name, data FROM t_update_delete WHERE id = ?", (1,))
            .await
            .unwrap();
        rows
    }, then_assert: |result| {
        assert_eq!(result.len(), 1, "Should still have row with id=1");
        assert_eq!(result[0].1, "alice_updated", "Name should be updated");
    });

    // DELETE a row upstream
    upstream_conn
        .query_drop("DELETE FROM t_update_delete WHERE id = 2")
        .await
        .unwrap();
    sleep().await;

    // Verify the delete is reflected in ReadySet
    eventually!(run_test: {
        let rows: Vec<(i32, String, String)> = rs_conn
            .exec("SELECT id, name, data FROM t_update_delete WHERE id = ?", (2,))
            .await
            .unwrap();
        rows
    }, then_assert: |result| {
        assert_eq!(result.len(), 0, "Row with id=2 should be deleted");
    });

    // Verify other rows are unaffected
    eventually!(run_test: {
        let rows: Vec<(i32, String, String)> = rs_conn
            .exec("SELECT id, name, data FROM t_update_delete WHERE id = ?", (3,))
            .await
            .unwrap();
        rows
    }, then_assert: |result| {
        assert_eq!(result.len(), 1, "Row with id=3 should still exist");
        assert_eq!(result[0].1, "charlie");
    });

    tx.shutdown().await;

    upstream_conn
        .query_drop(format!("DROP DATABASE {db_name}"))
        .await
        .unwrap();
}
