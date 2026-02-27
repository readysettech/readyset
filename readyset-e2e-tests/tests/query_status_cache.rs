use mysql_async::prelude::Queryable;
use readyset_adapter::backend::MigrationMode;
use readyset_adapter::query_status_cache::MigrationStyle;
use readyset_client_test_helpers::mysql_helpers::{self, MySQLAdapter};
use readyset_client_test_helpers::TestBuilder;
use readyset_util::eventually;
use test_utils::{tags, upstream};

/// Helper: returns a list of (query_text, status) pairs from SHOW PROXIED QUERIES.
async fn proxied_queries(conn: &mut mysql_async::Conn) -> Vec<(String, String)> {
    let rows: Vec<(String, String, String, String)> =
        conn.query("SHOW PROXIED QUERIES").await.unwrap();
    rows.into_iter()
        .map(|(_, query_text, status, _)| (query_text, status))
        .collect()
}

/// REA-5115: After `ALTER READYSET ADD TABLES` makes a non-replicated table
/// replicated, queries against that table should become cacheable.
///
/// Starts Readyset with a replication filter allowing only t1 (so t2 is
/// non-replicated). Verifies t2 is in non_replicated_relations, then uses
/// ALTER READYSET ADD TABLES to add t2. After resnapshot completes, verifies
/// t2 is no longer non-replicated and that CREATE CACHE succeeds for a
/// query against t2.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(mysql80, mysql84)]
async fn add_table_makes_query_cacheable() {
    readyset_tracing::init_test_logging();
    let db_name = "add_table_cacheable_test";
    mysql_helpers::recreate_database(db_name).await;

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(db_name));
    let mut upstream_conn = mysql_async::Conn::new(upstream_opts).await.unwrap();

    // Create two tables upstream before starting Readyset.
    upstream_conn
        .query_drop("CREATE TABLE t1 (id INT PRIMARY KEY, val INT)")
        .await
        .unwrap();
    upstream_conn
        .query_drop("CREATE TABLE t2 (id INT PRIMARY KEY, val INT)")
        .await
        .unwrap();
    upstream_conn
        .query_drop("INSERT INTO t1 VALUES (1, 10)")
        .await
        .unwrap();
    upstream_conn
        .query_drop("INSERT INTO t2 VALUES (1, 20)")
        .await
        .unwrap();

    // Start Readyset with only t1 replicated. t2 will be non-replicated.
    let (rs_opts, mut handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .replicate_db(db_name.to_string())
        .replication_tables(format!("{db_name}.t1"))
        .fallback(true)
        .migration_mode(MigrationMode::OutOfBand)
        .migration_style(MigrationStyle::Explicit)
        .build::<MySQLAdapter>()
        .await;

    let mut rs_conn = mysql_async::Conn::new(rs_opts).await.unwrap();

    // Verify t2 is non-replicated.
    eventually!(run_test: {
        handle.non_replicated_relations().await.unwrap()
    }, then_assert: |nrrs| {
        assert!(
            nrrs.iter().any(|r| {
                r.name.name.as_str() == "t2"
                    && r.name.schema.as_ref().map(|s| s.as_str()) == Some(db_name)
            }),
            "Expected {db_name}.t2 to be non-replicated, got: {nrrs:?}"
        );
    });

    // CREATE CACHE should fail for t2 since it's not replicated.
    let err = rs_conn
        .query_drop("CREATE CACHE FROM SELECT val FROM t2 WHERE id = ?")
        .await;
    assert!(err.is_err(), "CREATE CACHE should fail for non-replicated t2");

    // Now make t2 replicated via ALTER READYSET ADD TABLES.
    rs_conn
        .query_drop(format!("ALTER READYSET ADD TABLES {db_name}.t2"))
        .await
        .unwrap();

    // Wait for t2 to leave non_replicated_relations (resnapshot completed).
    eventually!(attempts: 120, sleep: std::time::Duration::from_millis(500),
        run_test: {
            handle.non_replicated_relations().await.unwrap()
        }, then_assert: |nrrs| {
            assert!(
                !nrrs.iter().any(|r| {
                    r.name.name.as_str() == "t2"
                        && r.name.schema.as_ref().map(|s| s.as_str()) == Some(db_name)
                }),
                "Expected {db_name}.t2 to no longer be non-replicated after ADD TABLES, \
                 but still found: {nrrs:?}"
            );
        }
    );

    // Wait for the resnapshot to fully complete before issuing CREATE CACHE.
    handle.backend_ready().await;

    // CREATE CACHE should now succeed since t2 is replicated.
    rs_conn
        .query_drop("CREATE CACHE FROM SELECT val FROM t2 WHERE id = ?")
        .await
        .unwrap();

    // Verify the cached query returns correct data.
    let rows: Vec<(i32,)> = rs_conn
        .exec("SELECT val FROM t2 WHERE id = ?", (1,))
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, 20);

    shutdown_tx.shutdown().await;
}

/// REA-5970: Schema-qualified invalidation matching. When the schema catalog
/// reports a schema-qualified relation change (e.g., `db.t1`), queries in
/// the QSC that reference `t1` without qualification should still be
/// invalidated via name-only fallback.
///
/// Queries t1 with an unqualified name, then ALTER TABLE on the upstream
/// changes its schema. The catalog diff reports the change with a
/// schema-qualified relation name. The QSC entry should still be
/// invalidated despite the qualification mismatch.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(mysql80, mysql84)]
async fn alter_table_invalidates_proxied_query() {
    readyset_tracing::init_test_logging();
    let db_name = "alter_table_invalidation_test";
    mysql_helpers::recreate_database(db_name).await;

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(db_name));
    let mut upstream_conn = mysql_async::Conn::new(upstream_opts).await.unwrap();

    // Create t1 upstream before starting Readyset.
    upstream_conn
        .query_drop("CREATE TABLE t1 (id INT PRIMARY KEY, val INT)")
        .await
        .unwrap();
    upstream_conn
        .query_drop("INSERT INTO t1 VALUES (1, 42)")
        .await
        .unwrap();

    let (rs_opts, mut handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .replicate_db(db_name.to_string())
        .fallback(true)
        .migration_mode(MigrationMode::OutOfBand)
        .migration_style(MigrationStyle::Explicit)
        .build::<MySQLAdapter>()
        .await;

    let mut rs_conn = mysql_async::Conn::new(rs_opts).await.unwrap();

    // Run an unqualified query against t1 — enters the QSC.
    rs_conn
        .query_drop("SELECT val FROM t1 WHERE id = 1")
        .await
        .unwrap();

    // Verify the t1 query appears in SHOW PROXIED QUERIES.
    eventually!(run_test: {
        proxied_queries(&mut rs_conn).await
    }, then_assert: |queries| {
        assert!(
            queries.iter().any(|(q, _)| q.contains("`t1`")),
            "Expected a proxied query referencing t1, got: {queries:?}"
        );
    });

    // Record the current schema generation so we can wait for it to advance.
    let gen_before = handle.schema_catalog().await.unwrap().generation;

    // ALTER TABLE t1 on the upstream — this triggers a schema catalog change.
    upstream_conn
        .query_drop("ALTER TABLE t1 ADD COLUMN extra INT DEFAULT 0")
        .await
        .unwrap();

    // Wait for the schema generation to advance (proves the catalog sync happened).
    readyset_client_test_helpers::wait_for_schema_generation_change(&mut handle, gen_before).await;

    // The adapter's SchemaCatalogSynchronizer runs asynchronously and may not have
    // processed the diff yet, so poll until the QSC reflects the invalidation.
    eventually!(attempts: 40, sleep: std::time::Duration::from_millis(500),
        run_test: {
            proxied_queries(&mut rs_conn).await
        }, then_assert: |queries| {
            assert!(
                !queries.iter().any(|(q, _)| q.contains("`t1`")),
                "Expected t1 query to be invalidated after ALTER TABLE, \
                 but still found: {queries:?}"
            );
        }
    );

    // Re-run the query — it should re-enter as pending.
    rs_conn
        .query_drop("SELECT val FROM t1 WHERE id = 1")
        .await
        .unwrap();

    eventually!(run_test: {
        proxied_queries(&mut rs_conn).await
    }, then_assert: |queries| {
        let entry = queries.iter().find(|(q, _)| q.contains("`t1`"));
        assert!(
            entry.is_some(),
            "Expected re-run t1 query to re-appear in SHOW PROXIED QUERIES, got: {queries:?}"
        );
        assert_eq!(
            entry.unwrap().1, "pending",
            "Expected re-entered query to have status 'pending'"
        );
    });

    shutdown_tx.shutdown().await;
}
