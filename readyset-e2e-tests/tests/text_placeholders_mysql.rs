use std::assert_matches;

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Error, Row, ServerError};
use readyset_client_metrics::QueryDestination;
use readyset_client_test_helpers::mysql_helpers::{self, MySQLAdapter, last_query_info};
use readyset_client_test_helpers::{TestBuilder, derive_test_name};
use readyset_util::eventually;
use test_utils::{tags, upstream};

async fn assert_upstream_rejects(rs: &mut Conn, read: &str) {
    let err = rs.query_drop(read).await.unwrap_err();
    assert_matches!(err, Error::Server(ServerError { code: 1064, .. }), "{read}");
    assert_eq!(
        last_query_info(rs).await.destination,
        QueryDestination::Upstream,
        "{read}"
    );
}

/// MySQL rejects `?` in a text query, so upstream answers it even when a cache holds its twin.
#[tokio::test]
#[tags(serial)]
#[upstream(mysql)]
async fn text_placeholders_go_upstream() {
    readyset_tracing::init_test_logging();
    let db = derive_test_name();
    mysql_helpers::recreate_database(&db).await;
    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(&db));
    let mut upstream = mysql_async::Conn::new(upstream_opts).await.unwrap();
    upstream
        .query_drop(
            "CREATE TABLE t (a INT, c INT, d INT, s TEXT); INSERT INTO t VALUES (1, 1, 5, '?')",
        )
        .await
        .unwrap();

    let (opts, _handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .fallback(true)
        .replicate_db(&db)
        .build::<MySQLAdapter>()
        .await;
    let mut rs = mysql_async::Conn::new(opts).await.unwrap();

    // The twin spells its `?` inside a literal, which binds nothing and reaches the cache.
    for (kind, twin, read) in [
        (
            "DEEP",
            "SELECT a FROM t WHERE d = 5 AND c IN (1, 2, 3) AND s = '?'",
            "SELECT a FROM t WHERE d = 5 AND c IN (?, ?, ?) AND s = '?'",
        ),
        (
            "SHALLOW",
            "SELECT a FROM t WHERE c IN (1, 2, 3, 4) AND d = 5 AND s = '?'",
            "SELECT a FROM t WHERE c IN (?, ?, ?, ?) AND d = 5 AND s = '?'",
        ),
    ] {
        rs.query_drop(format!("CREATE {kind} CACHE FROM {twin}"))
            .await
            .unwrap();
        eventually!({
            rs.query_drop(twin).await.unwrap();
            !matches!(
                last_query_info(&mut rs).await.destination,
                QueryDestination::Upstream | QueryDestination::ReadysetThenUpstream(_)
            )
        });
        assert_upstream_rejects(&mut rs, read).await;
    }

    // Nor does one leave a cache or a proxied query behind.
    rs.query_drop("DROP ALL PROXIED QUERIES").await.unwrap();
    for read in [
        "SELECT /*rs+ CREATE SHALLOW CACHE */ a FROM t WHERE c = ?",
        "SELECT CURRENT_TIMESTAMP(?) FROM t",
    ] {
        assert_upstream_rejects(&mut rs, read).await;
    }
    let proxied: Vec<Row> = rs.query("SHOW PROXIED QUERIES").await.unwrap();
    assert!(proxied.is_empty(), "{proxied:?}");
    let caches: Vec<Row> = rs.query("SHOW CACHES").await.unwrap();
    assert_eq!(caches.len(), 2);

    shutdown_tx.shutdown().await;
}
