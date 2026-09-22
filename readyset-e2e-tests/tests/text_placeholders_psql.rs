use readyset_client_metrics::QueryDestination;
use readyset_client_test_helpers::psql_helpers::{self, PostgreSQLAdapter, last_query_info};
use readyset_client_test_helpers::{Adapter, TestBuilder};
use readyset_util::eventually;
use test_utils::{tags, upstream};
use tokio_postgres::error::SqlState;
use tokio_postgres::{Client, SimpleQueryMessage};

/// The first column of every row a simple-protocol query returns.
async fn first_column(client: &Client, sql: &str) -> Vec<String> {
    client
        .simple_query(sql)
        .await
        .unwrap()
        .iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => r.get(0).map(str::to_owned),
            _ => None,
        })
        .collect()
}

/// Postgres spells jsonb operators with `?`, and a literal may hold any placeholder spelling.
/// Only a bind placeholder keeps a text query away from its cache.
#[tokio::test]
#[tags(serial, slow)]
#[upstream(postgres)]
async fn question_mark_operators_are_not_placeholders() {
    readyset_tracing::init_test_logging();
    let db = "text_placeholders_psql";
    PostgreSQLAdapter::recreate_database(db).await;
    let mut cfg = psql_helpers::upstream_config();
    cfg.dbname(db);
    let upstream = psql_helpers::connect(cfg).await;
    upstream
        .simple_query(
            "CREATE TABLE t (a INT, j JSONB, s TEXT); \
             INSERT INTO t VALUES (1, '{\"k\": 1}', '?'), (2, '{\"x\": 1}', '$1')",
        )
        .await
        .unwrap();

    let (opts, _handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .fallback(true)
        .replicate_db(db)
        .build::<PostgreSQLAdapter>()
        .await;
    let rs = psql_helpers::connect(opts).await;

    for (kind, read, expected) in [
        ("DEEP", "SELECT a FROM t WHERE j ? 'k'", "1"),
        ("DEEP", "SELECT a FROM t WHERE s = '$1'", "2"),
        (
            "SHALLOW",
            "SELECT a FROM t WHERE j ?& ARRAY['k'] AND j ?| ARRAY['k', 'z'] AND s = '?'",
            "1",
        ),
    ] {
        rs.simple_query(&format!("CREATE {kind} CACHE FROM {read}"))
            .await
            .unwrap();
        eventually!({
            assert_eq!(first_column(&rs, read).await, [expected], "{read}");
            !matches!(
                last_query_info(&rs).await.destination,
                QueryDestination::Upstream | QueryDestination::ReadysetThenUpstream(_)
            )
        });
    }

    // A bind placeholder beside an operator still goes upstream, which rejects it.
    rs.simple_query("DROP ALL PROXIED QUERIES").await.unwrap();
    let read = "SELECT a FROM t WHERE j ? 'k' AND a = $1";
    let err = rs.simple_query(read).await.unwrap_err();
    assert_eq!(err.code(), Some(&SqlState::UNDEFINED_PARAMETER), "{err}");
    assert_eq!(last_query_info(&rs).await.destination, QueryDestination::Upstream);
    assert!(first_column(&rs, "SHOW PROXIED QUERIES").await.is_empty());
    assert_eq!(first_column(&rs, "SHOW CACHES").await.len(), 3);

    shutdown_tx.shutdown().await;
}
