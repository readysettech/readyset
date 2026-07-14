//! MySQL counterpart to the Postgres REA-6740 reproduction. Postgres memoizes
//! unnamed prepared statements by query text with no `search_path` dimension;
//! MySQL prepared statements are always named (one id per COM_STMT_PREPARE) and
//! are not tracked in that text-keyed map, so switching the default schema with
//! `USE` and re-preparing the same text must re-resolve the table.
//!
//! The client statement cache is disabled so every `exec` issues a fresh
//! COM_STMT_PREPARE, mirroring how the Postgres client re-Parses the unnamed
//! statement on every query.

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder};
use readyset_adapter::backend::{BackendBuilder, MigrationMode};
use readyset_client_metrics::QueryDestination;
use readyset_client_test_helpers::mysql_helpers::{self, MySQLAdapter, last_query_info};
use readyset_client_test_helpers::{TestBuilder, derive_test_name};
use readyset_tracing::init_test_logging;
use test_utils::{tags, upstream};
use tokio::test;

#[test]
#[tags(serial, slow)]
#[upstream(mysql)]
async fn prepared_reresolves_after_use_db() {
    init_test_logging();

    let test_name = derive_test_name();
    let db_a = format!("{test_name}_a");
    let db_b = format!("{test_name}_b");
    mysql_helpers::recreate_database(&db_a).await;
    mysql_helpers::recreate_database(&db_b).await;

    let upstream_opts = mysql_helpers::upstream_config();
    let mut upstream = Conn::new(upstream_opts).await.unwrap();
    for stmt in [
        format!("CREATE TABLE {db_a}.items (id BIGINT AUTO_INCREMENT PRIMARY KEY, label TEXT NOT NULL)"),
        format!("CREATE TABLE {db_b}.items (id BIGINT AUTO_INCREMENT PRIMARY KEY, label TEXT NOT NULL)"),
        format!("INSERT INTO {db_a}.items (label) VALUES ('a-one'), ('a-two')"),
        format!("INSERT INTO {db_b}.items (label) VALUES ('b-one'), ('b-two'), ('b-three')"),
    ] {
        upstream.query_drop(&stmt).await.unwrap();
    }

    let (rs_opts, _handle, shutdown_tx) = TestBuilder::new(
        BackendBuilder::default().require_authentication(false),
    )
    .recreate_database(false)
    .replicate_db(&db_a)
    .fallback(true)
    .migration_mode(MigrationMode::OutOfBand)
    .build::<MySQLAdapter>()
    .await;

    // Disable the client statement cache so each `exec` re-prepares.
    let opts = OptsBuilder::from_opts(rs_opts).stmt_cache_size(0);
    let mut conn = Conn::new(opts).await.unwrap();

    let query = "SELECT label FROM items WHERE 1 = ? ORDER BY id";

    // Register the shallow cache while USE resolves `items` to db_a.
    conn.query_drop(format!("USE {db_a}")).await.unwrap();
    conn.query_drop(
        "CREATE SHALLOW CACHE POLICY TTL 60 SECONDS \
         FROM SELECT label FROM items WHERE 1 = ? ORDER BY id",
    )
    .await
    .unwrap();

    let rows_a: Vec<String> = conn.exec(query, (1,)).await.unwrap();
    assert_eq!(rows_a, vec!["a-one", "a-two"], "db_a rows");
    assert_eq!(
        last_query_info(&mut conn).await.destination,
        QueryDestination::Upstream,
    );

    let rows_a: Vec<String> = conn.exec(query, (1,)).await.unwrap();
    assert_eq!(rows_a, vec!["a-one", "a-two"], "db_a rows (cached)");
    assert_eq!(
        last_query_info(&mut conn).await.destination,
        QueryDestination::ReadysetShallow,
    );

    conn.query_drop(format!("USE {db_b}")).await.unwrap();
    let rows_b: Vec<String> = conn.exec(query, (1,)).await.unwrap();
    assert_eq!(
        rows_b,
        vec!["b-one", "b-two", "b-three"],
        "re-preparing the same text after USE must resolve against db_b, not \
         return db_a's memoized rows",
    );

    shutdown_tx.shutdown().await;
}
