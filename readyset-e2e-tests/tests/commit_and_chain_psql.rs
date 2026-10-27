//! Regression tests for REA-6786: COMMIT AND CHAIN and ROLLBACK AND CHAIN start a new
//! transaction, upstream and in Readyset.

use std::assert_matches;

use readyset_adapter::BackendBuilder;
use readyset_client_metrics::QueryDestination;
use readyset_client_test_helpers::psql_helpers::{self, PostgreSQLAdapter};
use readyset_client_test_helpers::{TestBuilder, TestShutdownSender, derive_test_name};
use readyset_server::{CacheMode, Handle};
use readyset_sql_parsing::ParsingPreset;
use readyset_util::eventually;
use test_utils::{tags, upstream};
use tokio_postgres::{Client, SimpleQueryMessage};

const QUERY: &str = "SELECT v FROM t WHERE id = 1";

/// Start a shallow cache adapter for `db` with table `t` on the upstream. Return a connection to
/// the adapter, a direct connection to the upstream, and the handles, which the caller must keep
/// for the test's duration.
async fn setup(db: &str) -> (Client, Client, Handle, TestShutdownSender<PostgreSQLAdapter>) {
    let backend = BackendBuilder::new().require_authentication(false);
    let (rs_opts, handle, shutdown_tx) = TestBuilder::new(backend)
        .cache_mode(CacheMode::Shallow)
        // nom-sql rejects AND CHAIN, so the test presets proxy the statement as unparsed text.
        .parsing_preset(ParsingPreset::for_prod())
        .replicate(false)
        .fallback_without_replication(db)
        .build::<PostgreSQLAdapter>()
        .await;

    let mut config = psql_helpers::upstream_config();
    config.dbname(db);
    let upstream = psql_helpers::connect(config).await;
    upstream
        .simple_query("CREATE TABLE t (id INT PRIMARY KEY, v INT); INSERT INTO t VALUES (1, 0)")
        .await
        .unwrap();

    let conn = psql_helpers::connect(rs_opts).await;
    (conn, upstream, handle, shutdown_tx)
}

/// The ids in `t` greater than 1, read directly from the upstream.
async fn inserted_ids(upstream: &Client) -> Vec<String> {
    upstream
        .simple_query("SELECT id FROM t WHERE id > 1 ORDER BY id")
        .await
        .unwrap()
        .into_iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(row) => Some(row.get(0).unwrap().to_owned()),
            _ => None,
        })
        .collect()
}

/// Run `query` through Readyset and return where it ran.
async fn destination(conn: &Client, query: &str) -> QueryDestination {
    conn.simple_query(query).await.unwrap();
    psql_helpers::last_query_info(conn).await.destination
}

/// A write after COMMIT AND CHAIN or ROLLBACK AND CHAIN is part of the new transaction, so a
/// ROLLBACK undoes it.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(postgres)]
async fn chain_starts_upstream_transaction() {
    readyset_tracing::init_test_logging();
    let db = derive_test_name();
    let (conn, upstream, _handle, shutdown_tx) = setup(&db).await;

    conn.simple_query("BEGIN").await.unwrap();
    conn.simple_query("INSERT INTO t VALUES (2, 0)").await.unwrap();
    conn.simple_query("COMMIT AND CHAIN").await.unwrap();
    conn.simple_query("INSERT INTO t VALUES (3, 0)").await.unwrap();
    conn.simple_query("ROLLBACK").await.unwrap();
    assert_eq!(inserted_ids(&upstream).await, ["2"]);

    conn.simple_query("BEGIN").await.unwrap();
    conn.simple_query("INSERT INTO t VALUES (4, 0)").await.unwrap();
    conn.simple_query("ROLLBACK AND CHAIN").await.unwrap();
    conn.simple_query("INSERT INTO t VALUES (5, 0)").await.unwrap();
    conn.simple_query("ROLLBACK").await.unwrap();
    assert_eq!(inserted_ids(&upstream).await, ["2"]);

    shutdown_tx.shutdown().await;
}

/// Reads in the transaction that COMMIT AND CHAIN or ROLLBACK AND CHAIN starts go upstream, as
/// they do in any transaction, whether the client sends the statement as a simple query or a
/// prepared one.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(postgres)]
async fn chain_reads_go_upstream() {
    readyset_tracing::init_test_logging();
    let db = derive_test_name();
    let (conn, _upstream, _handle, shutdown_tx) = setup(&db).await;

    conn.simple_query(&format!("CREATE SHALLOW CACHE POLICY TTL 600 SECONDS FROM {QUERY}"))
        .await
        .unwrap();
    eventually! {
        matches!(destination(&conn, QUERY).await, QueryDestination::ReadysetShallow(_))
    }

    for chain in ["COMMIT AND CHAIN", "ROLLBACK AND CHAIN"] {
        for prepared in [false, true] {
            conn.simple_query("BEGIN").await.unwrap();
            assert_eq!(destination(&conn, QUERY).await, QueryDestination::Upstream);
            if prepared {
                conn.execute(chain, &[]).await.unwrap();
            } else {
                conn.simple_query(chain).await.unwrap();
            }
            assert_eq!(
                destination(&conn, QUERY).await,
                QueryDestination::Upstream,
                "{chain}, prepared: {prepared}"
            );
            conn.simple_query("COMMIT").await.unwrap();
            assert_matches!(
                destination(&conn, QUERY).await,
                QueryDestination::ReadysetShallow(_)
            );
        }
    }

    shutdown_tx.shutdown().await;
}

/// In a failed transaction, COMMIT AND CHAIN rolls the transaction back and starts a new one.
#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(postgres)]
async fn chain_after_failed_transaction() {
    readyset_tracing::init_test_logging();
    let db = derive_test_name();
    let (conn, upstream, _handle, shutdown_tx) = setup(&db).await;

    conn.simple_query(&format!("CREATE SHALLOW CACHE POLICY TTL 600 SECONDS FROM {QUERY}"))
        .await
        .unwrap();
    eventually! {
        matches!(destination(&conn, QUERY).await, QueryDestination::ReadysetShallow(_))
    }

    for prepared in [false, true] {
        conn.simple_query("BEGIN").await.unwrap();
        conn.simple_query("INSERT INTO t VALUES (2, 0)").await.unwrap();
        conn.simple_query("SELECT 1 / 0").await.unwrap_err();
        if prepared {
            conn.execute("COMMIT AND CHAIN", &[]).await.unwrap();
        } else {
            conn.simple_query("COMMIT AND CHAIN").await.unwrap();
        }
        assert_eq!(
            destination(&conn, QUERY).await,
            QueryDestination::Upstream,
            "prepared: {prepared}"
        );
        conn.simple_query("INSERT INTO t VALUES (3, 0)").await.unwrap();
        conn.simple_query("ROLLBACK").await.unwrap();
        assert!(inserted_ids(&upstream).await.is_empty(), "prepared: {prepared}");
        assert_matches!(
            destination(&conn, QUERY).await,
            QueryDestination::ReadysetShallow(_)
        );
    }

    shutdown_tx.shutdown().await;
}
