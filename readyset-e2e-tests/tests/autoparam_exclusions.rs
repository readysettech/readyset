use mysql_async::Conn;
use mysql_async::prelude::Queryable;
use readyset_adapter::backend::{MigrationMode, QueryInfo};
use readyset_client_metrics::QueryDestination;
use readyset_client_test_helpers::mysql_helpers::{self, MySQLAdapter};
use readyset_client_test_helpers::{TestBuilder, sleep};
use test_utils::{tags, upstream};

/// Helper that fires an `EXPLAIN LAST STATEMENT` and asserts that the last
/// target/QueryDestination was `expected`.
async fn assert_last_target_was(rs_conn: &mut Conn, expected: QueryDestination) {
    let destination: QueryInfo = rs_conn
        .query_first("EXPLAIN LAST STATEMENT")
        .await
        .unwrap()
        .unwrap();
    let msg = destination.reason;
    assert_eq!(destination.destination, expected, "{msg}");
}

/// What `CREATE CACHE WITH (AUTOPARAM (EXCLUDE_EXISTS))` is meant to do: the literal inside the
/// EXISTS stays inline while everything else autoparameterizes, so the cache serves any `id` but
/// only that status.
///
/// Disabled, and known to fail. `CREATE CACHE` no longer honors the option: a cache built from
/// marked literals takes a form no read produces on its own, and a marked literal holds no
/// parameter position, so nothing about a read can say it belongs to such a cache. See
/// `readyset_sql_passes::adapter_rewrites::autoparam_exclusions`. The test is kept as the
/// specification for re-enabling it.
#[ignore = "AUTOPARAM (EXCLUDE_*) is not honored; reads cannot be routed to a scoped cache"]
#[tokio::test]
#[tags(serial)]
#[upstream(mysql)]
async fn autoparam_exclude_exists_end_to_end() {
    readyset_tracing::init_test_logging();
    let db_name = "autoparam_exclude_exists";
    mysql_helpers::recreate_database(db_name).await;

    let (rs_opts, _handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .migration_mode(MigrationMode::OutOfBand)
        .fallback(true)
        .replicate_db(db_name)
        .build::<MySQLAdapter>()
        .await;

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(db_name));
    let mut upstream_conn = mysql_async::Conn::new(upstream_opts).await.unwrap();

    let mut rs_conn = mysql_async::Conn::new(rs_opts).await.unwrap();

    upstream_conn
        .query_drop(
            "CREATE TABLE t (id int, v int); \
             CREATE TABLE u (t_id int, status varchar(16)); \
             INSERT INTO t (id, v) VALUES (1, 10), (2, 30); \
             INSERT INTO u (t_id, status) VALUES (1, 'active'), (2, 'archived');",
        )
        .await
        .unwrap();

    sleep().await;

    // `u.status = 'active'` originates inside the EXISTS, so EXCLUDE_EXISTS keeps it inline
    // even though the rewrite pipeline hoists it into the top-level WHERE. `t.id` is left a
    // literal so autoparameterization (which is not excluded) parameterizes it; writing it as a
    // placeholder would suppress subquery unnesting at creation and never match the incoming
    // ad-hoc query, which does unnest.
    rs_conn
        .query_drop(
            "CREATE CACHE excl WITH (AUTOPARAM (EXCLUDE_EXISTS)) FROM \
             SELECT v FROM t WHERE t.id = 5 AND EXISTS \
             (SELECT 1 FROM u WHERE u.t_id = t.id AND u.status = 'active')",
        )
        .await
        .unwrap();

    sleep().await;

    // The literal the cache kept inline: served by that cache.
    let result: Vec<i32> = rs_conn
        .query(
            "SELECT v FROM t WHERE t.id = 1 AND EXISTS \
             (SELECT 1 FROM u WHERE u.t_id = t.id AND u.status = 'active')",
        )
        .await
        .unwrap();
    assert_eq!(result, vec![10]);
    assert_last_target_was(
        &mut rs_conn,
        QueryDestination::Readyset(Some("excl".into())),
    )
    .await;

    // A different status literal misses the cache and falls back upstream, still correct.
    let result: Vec<i32> = rs_conn
        .query(
            "SELECT v FROM t WHERE t.id = 2 AND EXISTS \
             (SELECT 1 FROM u WHERE u.t_id = t.id AND u.status = 'archived')",
        )
        .await
        .unwrap();
    assert_eq!(result, vec![30]);
    assert_last_target_was(&mut rs_conn, QueryDestination::Upstream).await;

    shutdown_tx.shutdown().await;
}
