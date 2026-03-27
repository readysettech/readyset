use std::panic::AssertUnwindSafe;
use std::time::Duration;

use readyset_client_test_helpers::psql_helpers::{self, PostgreSQLAdapter};
use readyset_client_test_helpers::{Adapter, TestBuilder};
use readyset_util::eventually;
use test_utils::{tags, upstream};
use tokio_postgres::SimpleQueryMessage;

fn first_row_col(rows: &[SimpleQueryMessage], col: usize) -> String {
    let first = rows
        .first()
        .expect("expected at least one message in result set");
    match first {
        SimpleQueryMessage::Row(row) => row.get(col).expect("column should exist").to_string(),
        other => panic!("expected Row, got {other:?}"),
    }
}

fn row_count(rows: &[SimpleQueryMessage]) -> usize {
    rows.iter()
        .filter(|m| matches!(m, SimpleQueryMessage::Row(_)))
        .count()
}

/// Test that tables with REPLICA IDENTITY USING INDEX set to an index different
/// from the PRIMARY KEY correctly replicate DELETE and UPDATE operations.
///
/// Regression test for REA-6194: when the replica identity columns differ from
/// the primary key columns, Readyset would fail with "wrong number of key
/// columns" and drop the table from replication.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(postgres)]
async fn replica_identity_different_from_pk() {
    readyset_tracing::init_test_logging();

    let db_name = "replica_identity_different_from_pk";
    PostgreSQLAdapter::recreate_database(db_name).await;

    // Connect to upstream and set up the schema
    let mut upstream_config = psql_helpers::upstream_config();
    upstream_config.dbname(db_name);
    let upstream_conn = psql_helpers::connect(upstream_config).await;

    upstream_conn
        .simple_query(
            "CREATE TABLE users (
                id integer PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                email VARCHAR(255) NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )",
        )
        .await
        .expect("create table");

    upstream_conn
        .simple_query(
            "CREATE UNIQUE INDEX users_id_email_unique ON users USING btree (id, email)",
        )
        .await
        .expect("create unique index");

    upstream_conn
        .simple_query("ALTER TABLE ONLY users REPLICA IDENTITY USING INDEX users_id_email_unique")
        .await
        .expect("set replica identity");

    upstream_conn
        .simple_query(
            "INSERT INTO users (id, name, email) VALUES
                (1, 'John Doe', 'john@example.com'),
                (2, 'Jane Doe', 'jane@example.com'),
                (3, 'Bob Smith', 'bob@example.com')",
        )
        .await
        .expect("insert initial data");

    // Start Readyset with replication
    let (rs_opts, _rs_handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .replicate_db(db_name)
        .fallback(false)
        .build::<PostgreSQLAdapter>()
        .await;

    let mut rs_config = rs_opts.clone();
    rs_config.dbname(db_name);
    let rs_conn = psql_helpers::connect(rs_config).await;

    // Wait for snapshot to complete and create a cache (retrying until the table is available)
    eventually!(
        attempts: 40,
        sleep: Duration::from_millis(500),
        run_test: {
            let result = rs_conn
                .simple_query(
                    "CREATE CACHE FROM SELECT id, name, email FROM users WHERE id = $1",
                )
                .await;
            AssertUnwindSafe(move || result)
        },
        then_assert: |result| {
            result().expect("create cache");
        }
    );

    // Verify initial data is visible
    eventually!(
        attempts: 40,
        sleep: Duration::from_millis(500),
        run_test: {
            let rows = rs_conn
                .simple_query("SELECT id, name, email FROM users WHERE id = 1")
                .await;
            AssertUnwindSafe(move || rows)
        },
        then_assert: |result| {
            let rows = result().unwrap();
            assert_eq!(row_count(&rows), 1);
            assert_eq!(first_row_col(&rows, 1), "John Doe");
        }
    );

    // DELETE a row upstream
    upstream_conn
        .simple_query("DELETE FROM users WHERE id = 2")
        .await
        .expect("delete row");

    // Verify the delete replicates: querying the deleted row should return 0 rows
    eventually!(
        attempts: 40,
        sleep: Duration::from_millis(500),
        run_test: {
            let rows = rs_conn
                .simple_query("SELECT id, name, email FROM users WHERE id = 2")
                .await;
            AssertUnwindSafe(move || rows)
        },
        then_assert: |result| {
            let rows = result().unwrap();
            assert_eq!(row_count(&rows), 0);
        }
    );

    // UPDATE a row upstream
    upstream_conn
        .simple_query("UPDATE users SET name = 'Robert Smith' WHERE id = 3")
        .await
        .expect("update row");

    // Verify the update replicates
    eventually!(
        attempts: 40,
        sleep: Duration::from_millis(500),
        run_test: {
            let rows = rs_conn
                .simple_query("SELECT id, name, email FROM users WHERE id = 3")
                .await;
            AssertUnwindSafe(move || rows)
        },
        then_assert: |result| {
            let rows = result().unwrap();
            assert_eq!(row_count(&rows), 1);
            assert_eq!(first_row_col(&rows, 1), "Robert Smith");
        }
    );

    shutdown_tx.shutdown().await;
}
