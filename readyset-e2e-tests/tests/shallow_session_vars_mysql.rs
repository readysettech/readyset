use std::assert_matches;
use std::panic::AssertUnwindSafe;
use std::time::Duration;

use futures::FutureExt;
use mysql_async::prelude::Queryable;

use readyset_client_metrics::QueryDestination;
use readyset_client_test_helpers::mysql_helpers::{self, MySQLAdapter, last_query_info};
use readyset_client_test_helpers::{TestBuilder, TestShutdownSender};
use readyset_server::Handle;
use readyset_sql_parsing::ParsingPreset;
use readyset_util::eventually;
use test_utils::{tags, upstream};

type Setup = (
    mysql_async::Conn,
    mysql_async::Conn,
    mysql_async::Opts,
    Handle,
    TestShutdownSender<MySQLAdapter>,
);

/// Build an adapter with fallback, a three row table `t`, and a shallow cache on `query`.
async fn setup(query: &str, options: &str) -> Setup {
    setup_with(TestBuilder::default(), query, options).await
}

/// Like [`setup`], with the health monitor reading upstream global variables every second.
async fn setup_with_health_checks(query: &str) -> Setup {
    let builder = TestBuilder::default().upstream_health_interval(Duration::from_secs(1));
    setup_with(builder, query, "").await
}

async fn setup_with(builder: TestBuilder, query: &str, options: &str) -> Setup {
    readyset_tracing::init_test_logging();
    let (opts, handle, shutdown_tx) = builder
        .fallback(true)
        .parsing_preset(ParsingPreset::OnlySqlparser)
        .build::<MySQLAdapter>()
        .await;
    let mut conn = mysql_async::Conn::new(opts.clone()).await.unwrap();
    conn.query_drop("CREATE TABLE t (id INT PRIMARY KEY, s VARCHAR(8))")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO t (id, s) VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await
        .unwrap();
    conn.query_drop(format!("CREATE SHALLOW CACHE {options} FROM {query}"))
        .await
        .unwrap();
    let upstream_opts = mysql_helpers::upstream_config().db_name(opts.db_name());
    let upstream_conn = mysql_async::Conn::new(upstream_opts).await.unwrap();
    (conn, upstream_conn, opts, handle, shutdown_tx)
}

/// Run `test` with the upstream global variable `name` set to `value`, then restore the original
/// value, even if `test` panics.
async fn with_global<F: Future<Output = ()>>(name: &str, value: &str, test: F) {
    let mut upstream = mysql_async::Conn::new(mysql_helpers::upstream_config())
        .await
        .unwrap();
    // The binary protocol returns the value with its type, so it can be bound to restore it.
    let original: mysql_async::Value = upstream
        .exec_first(format!("SELECT @@GLOBAL.{name}"), ())
        .await
        .unwrap()
        .unwrap();
    upstream
        .query_drop(format!("SET GLOBAL {name} = {value}"))
        .await
        .unwrap();
    let result = AssertUnwindSafe(test).catch_unwind().await;
    upstream
        .exec_drop(format!("SET GLOBAL {name} = ?"), (original,))
        .await
        .unwrap();
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}

async fn assert_filled(conn: &mut mysql_async::Conn) {
    assert_matches!(
        last_query_info(conn).await.destination,
        QueryDestination::ReadysetThenUpstream(..)
    );
}

async fn assert_hit(conn: &mut mysql_async::Conn) {
    assert_matches!(
        last_query_info(conn).await.destination,
        QueryDestination::ReadysetShallow(..)
    );
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(mysql)]
async fn sql_select_limit_partitions_entries() {
    const QUERY: &str = "SELECT id FROM t ORDER BY id";
    let (mut a, _upstream, opts, _handle, shutdown_tx) = setup(QUERY, "").await;

    let ids: Vec<i64> = a.query(QUERY).await.unwrap();
    assert_eq!(ids, vec![1, 2, 3]);
    assert_filled(&mut a).await;
    let ids: Vec<i64> = a.query(QUERY).await.unwrap();
    assert_eq!(ids, vec![1, 2, 3]);
    assert_hit(&mut a).await;

    let mut b = mysql_async::Conn::new(opts).await.unwrap();
    b.query_drop("SET sql_select_limit = 1").await.unwrap();
    let ids: Vec<i64> = b.query(QUERY).await.unwrap();
    assert_eq!(ids, vec![1]);
    assert_filled(&mut b).await;
    let ids: Vec<i64> = b.query(QUERY).await.unwrap();
    assert_eq!(ids, vec![1]);
    assert_hit(&mut b).await;

    let ids: Vec<i64> = a.query(QUERY).await.unwrap();
    assert_eq!(ids, vec![1, 2, 3]);
    assert_hit(&mut a).await;

    // Binary protocol entries are keyed the same way.
    let ids: Vec<i64> = b.exec(QUERY, ()).await.unwrap();
    assert_eq!(ids, vec![1]);
    assert_filled(&mut b).await;
    let ids: Vec<i64> = a.exec(QUERY, ()).await.unwrap();
    assert_eq!(ids, vec![1, 2, 3]);
    assert_filled(&mut a).await;

    b.query_drop("SET sql_select_limit = DEFAULT").await.unwrap();
    let ids: Vec<i64> = b.query(QUERY).await.unwrap();
    assert_eq!(ids, vec![1, 2, 3]);
    assert_hit(&mut b).await;

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(mysql)]
async fn group_concat_max_len_partitions_entries() {
    const QUERY: &str = "SELECT GROUP_CONCAT(LENGTH(s)) FROM t";
    let (mut a, _upstream, opts, _handle, shutdown_tx) = setup(QUERY, "").await;

    let joined: String = a.query_first(QUERY).await.unwrap().unwrap();
    assert_eq!(joined, "1,1,1");
    assert_filled(&mut a).await;

    let mut b = mysql_async::Conn::new(opts).await.unwrap();
    b.query_drop("SET group_concat_max_len = 4").await.unwrap();
    let joined: String = b.query_first(QUERY).await.unwrap().unwrap();
    assert_eq!(joined, "1,1,");
    assert_filled(&mut b).await;
    let joined: String = b.query_first(QUERY).await.unwrap().unwrap();
    assert_eq!(joined, "1,1,");
    assert_hit(&mut b).await;

    let joined: String = a.query_first(QUERY).await.unwrap().unwrap();
    assert_eq!(joined, "1,1,1");
    assert_hit(&mut a).await;

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(mysql)]
async fn group_concat_max_len_expression_shares_entry() {
    const QUERY: &str = "SELECT GROUP_CONCAT(LENGTH(s)) FROM t";
    let (mut a, _upstream, opts, _handle, shutdown_tx) = setup(QUERY, "").await;

    a.query_drop("SET group_concat_max_len = 4").await.unwrap();
    let joined: String = a.query_first(QUERY).await.unwrap().unwrap();
    assert_eq!(joined, "1,1,");
    assert_filled(&mut a).await;

    let mut b = mysql_async::Conn::new(opts).await.unwrap();
    b.query_drop("SET group_concat_max_len = 2 * 2").await.unwrap();
    let joined: String = b.query_first(QUERY).await.unwrap().unwrap();
    assert_eq!(joined, "1,1,");
    assert_hit(&mut b).await;

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(mysql)]
async fn lc_time_names_partitions_entries() {
    const QUERY: &str = "SELECT DATE_FORMAT('2000-01-01', '%W') FROM t WHERE id = 1";
    let (mut a, _upstream, opts, _handle, shutdown_tx) = setup(QUERY, "").await;

    let day: String = a.query_first(QUERY).await.unwrap().unwrap();
    assert_eq!(day, "Saturday");
    assert_filled(&mut a).await;

    let mut b = mysql_async::Conn::new(opts.clone()).await.unwrap();
    b.query_drop("SET lc_time_names = fr_FR").await.unwrap();
    let day: String = b.query_first(QUERY).await.unwrap().unwrap();
    assert_eq!(day, "samedi");
    assert_filled(&mut b).await;
    let day: String = b.query_first(QUERY).await.unwrap().unwrap();
    assert_eq!(day, "samedi");
    assert_hit(&mut b).await;

    let mut c = mysql_async::Conn::new(opts).await.unwrap();
    c.query_drop("SET lc_time_names = 'Fr_fr'").await.unwrap();
    let day: String = c.query_first(QUERY).await.unwrap().unwrap();
    assert_eq!(day, "samedi");
    assert_hit(&mut c).await;

    let day: String = a.query_first(QUERY).await.unwrap().unwrap();
    assert_eq!(day, "Saturday");
    assert_hit(&mut a).await;

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(mysql)]
async fn div_precision_increment_partitions_entries() {
    const QUERY: &str = "SELECT 1 / 3 FROM t WHERE id = 1";
    let (mut a, _upstream, opts, _handle, shutdown_tx) = setup(QUERY, "").await;

    let quotient: String = a.query_first(QUERY).await.unwrap().unwrap();
    assert_eq!(quotient, "0.3333");
    assert_filled(&mut a).await;

    let mut b = mysql_async::Conn::new(opts).await.unwrap();
    b.query_drop("SET div_precision_increment = 1").await.unwrap();
    let quotient: String = b.query_first(QUERY).await.unwrap().unwrap();
    assert_eq!(quotient, "0.3");
    assert_filled(&mut b).await;
    let quotient: String = b.query_first(QUERY).await.unwrap().unwrap();
    assert_eq!(quotient, "0.3");
    assert_hit(&mut b).await;

    let quotient: String = a.query_first(QUERY).await.unwrap().unwrap();
    assert_eq!(quotient, "0.3333");
    assert_hit(&mut a).await;

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(mysql)]
async fn default_week_format_partitions_entries() {
    const QUERY: &str = "SELECT WEEK('2000-01-01') FROM t WHERE id = 1";
    let (mut a, _upstream, opts, _handle, shutdown_tx) = setup(QUERY, "").await;

    let week: i64 = a.query_first(QUERY).await.unwrap().unwrap();
    assert_eq!(week, 0);
    assert_filled(&mut a).await;

    let mut b = mysql_async::Conn::new(opts).await.unwrap();
    b.query_drop("SET default_week_format = 3").await.unwrap();
    let week: i64 = b.query_first(QUERY).await.unwrap().unwrap();
    assert_eq!(week, 52);
    assert_filled(&mut b).await;
    let week: i64 = b.query_first(QUERY).await.unwrap().unwrap();
    assert_eq!(week, 52);
    assert_hit(&mut b).await;

    let week: i64 = a.query_first(QUERY).await.unwrap().unwrap();
    assert_eq!(week, 0);
    assert_hit(&mut a).await;

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(mysql)]
async fn block_encryption_mode_partitions_entries() {
    const QUERY: &str = "SELECT HEX(AES_ENCRYPT('a', 'k', '0123456789abcdef')) FROM t WHERE id = 1";
    let (mut a, _upstream, opts, _handle, shutdown_tx) = setup(QUERY, "").await;

    let ciphertext: String = a.query_first(QUERY).await.unwrap().unwrap();
    assert_eq!(ciphertext, "53E61674DBCC997DD27BD802A2EB9F22");
    assert_filled(&mut a).await;

    let mut b = mysql_async::Conn::new(opts.clone()).await.unwrap();
    b.query_drop("SET block_encryption_mode = 'aes-256-cbc'").await.unwrap();
    let ciphertext: String = b.query_first(QUERY).await.unwrap().unwrap();
    assert_eq!(ciphertext, "ED8EC86B20144BB08F1DD23B3BABAA40");
    assert_filled(&mut b).await;
    let ciphertext: String = b.query_first(QUERY).await.unwrap().unwrap();
    assert_eq!(ciphertext, "ED8EC86B20144BB08F1DD23B3BABAA40");
    assert_hit(&mut b).await;

    let mut c = mysql_async::Conn::new(opts).await.unwrap();
    c.query_drop("SET block_encryption_mode = 'AES-256-CBC'").await.unwrap();
    let ciphertext: String = c.query_first(QUERY).await.unwrap().unwrap();
    assert_eq!(ciphertext, "ED8EC86B20144BB08F1DD23B3BABAA40");
    assert_hit(&mut c).await;

    let ciphertext: String = a.query_first(QUERY).await.unwrap().unwrap();
    assert_eq!(ciphertext, "53E61674DBCC997DD27BD802A2EB9F22");
    assert_hit(&mut a).await;

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(mysql)]
async fn windowing_use_high_precision_partitions_entries() {
    // With high precision off, MySQL removes the departing 1e20 from a running sum, which loses
    // the 1 that was absorbed when the 1e20 was added.
    const QUERY: &str = "SELECT SUM(CASE id WHEN 1 THEN 1e20 ELSE 1e0 END) \
        OVER (ORDER BY id ROWS 1 PRECEDING) FROM t ORDER BY id";
    let (mut a, _upstream, opts, _handle, shutdown_tx) = setup(QUERY, "").await;

    let sums: Vec<f64> = a.query(QUERY).await.unwrap();
    assert_eq!(sums, vec![1e20, 1e20, 2.0]);
    assert_filled(&mut a).await;

    let mut b = mysql_async::Conn::new(opts.clone()).await.unwrap();
    b.query_drop("SET windowing_use_high_precision = OFF").await.unwrap();
    let sums: Vec<f64> = b.query(QUERY).await.unwrap();
    assert_eq!(sums, vec![1e20, 1e20, 1.0]);
    assert_filled(&mut b).await;
    let sums: Vec<f64> = b.query(QUERY).await.unwrap();
    assert_eq!(sums, vec![1e20, 1e20, 1.0]);
    assert_hit(&mut b).await;

    let mut c = mysql_async::Conn::new(opts).await.unwrap();
    c.query_drop("SET windowing_use_high_precision = 0").await.unwrap();
    let sums: Vec<f64> = c.query(QUERY).await.unwrap();
    assert_eq!(sums, vec![1e20, 1e20, 1.0]);
    assert_hit(&mut c).await;

    let sums: Vec<f64> = a.query(QUERY).await.unwrap();
    assert_eq!(sums, vec![1e20, 1e20, 2.0]);
    assert_hit(&mut a).await;

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial, slow)]
#[upstream(mysql)]
async fn sql_select_limit_survives_refresh() {
    const QUERY: &str = "SELECT id FROM t ORDER BY id DESC";
    let (mut a, mut upstream, opts, _handle, shutdown_tx) =
        setup(QUERY, "POLICY TTL 60 SECONDS REFRESH EVERY 1 SECONDS").await;

    let mut b = mysql_async::Conn::new(opts).await.unwrap();
    b.query_drop("SET sql_select_limit = 1").await.unwrap();
    let ids: Vec<i64> = b.query(QUERY).await.unwrap();
    assert_eq!(ids, vec![3]);
    assert_filled(&mut b).await;

    upstream
        .query_drop("INSERT INTO t (id, s) VALUES (4, 'd')")
        .await
        .unwrap();

    eventually!(run_test: {
        let ids: Vec<i64> = b.query(QUERY).await.unwrap();
        let info = last_query_info(&mut b).await;
        AssertUnwindSafe(move || (info, ids))
    }, then_assert: |result| {
        let (info, ids) = result();
        assert_matches!(info.destination, QueryDestination::ReadysetShallow(..));
        assert_eq!(ids, vec![4]);
    });

    let ids: Vec<i64> = a.query(QUERY).await.unwrap();
    assert_eq!(ids, vec![4, 3, 2, 1]);

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(mysql)]
async fn global_sql_select_limit_partitions_entries() {
    const QUERY: &str = "SELECT id FROM t ORDER BY id";
    let (mut a, _upstream, opts, _handle, shutdown_tx) = setup_with_health_checks(QUERY).await;

    let ids: Vec<i64> = a.query(QUERY).await.unwrap();
    assert_eq!(ids, vec![1, 2, 3]);

    with_global("sql_select_limit", "1", async {
        // Sessions that connect before the next health check start with the old value.
        eventually!(run_test: {
            let mut b = mysql_async::Conn::new(opts.clone()).await.unwrap();
            let first: Vec<i64> = b.query(QUERY).await.unwrap();
            let second: Vec<i64> = b.query(QUERY).await.unwrap();
            let info = last_query_info(&mut b).await;
            AssertUnwindSafe(move || (first, second, info))
        }, then_assert: |result| {
            let (first, second, info) = result();
            assert_eq!(first, vec![1]);
            assert_eq!(second, vec![1]);
            assert_matches!(info.destination, QueryDestination::ReadysetShallow(..));
        });

        let ids: Vec<i64> = a.query(QUERY).await.unwrap();
        assert_eq!(ids, vec![1, 2, 3]);
        assert_hit(&mut a).await;
    })
    .await;

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(mysql)]
async fn global_sql_auto_is_null_proxies_new_sessions() {
    const QUERY: &str = "SELECT id FROM t ORDER BY id";
    let (_a, _upstream, opts, _handle, shutdown_tx) = setup_with_health_checks(QUERY).await;

    with_global("sql_auto_is_null", "1", async {
        eventually!(run_test: {
            let mut b = mysql_async::Conn::new(opts.clone()).await.unwrap();
            let ids: Vec<i64> = b.query(QUERY).await.unwrap();
            let info = last_query_info(&mut b).await;
            AssertUnwindSafe(move || (ids, info))
        }, then_assert: |result| {
            let (ids, info) = result();
            assert_eq!(ids, vec![1, 2, 3]);
            assert_eq!(info.destination, QueryDestination::Upstream);
        });
    })
    .await;

    shutdown_tx.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[tags(serial)]
#[upstream(mysql)]
async fn reset_connection_restores_default_entries() {
    const QUERY: &str = "SELECT id FROM t ORDER BY id";
    let (_a, _upstream, opts, _handle, shutdown_tx) = setup_with_health_checks(QUERY).await;

    // A session that connects before the first health check has no defaults to restore.
    eventually!(run_test: {
        let mut b = mysql_async::Conn::new(opts.clone()).await.unwrap();
        b.query_drop("SET sql_select_limit = 1").await.unwrap();
        let limited: Vec<i64> = b.query(QUERY).await.unwrap();
        b.reset().await.unwrap();
        let ids: Vec<i64> = b.query(QUERY).await.unwrap();
        AssertUnwindSafe(move || (limited, ids))
    }, then_assert: |result| {
        let (limited, ids) = result();
        assert_eq!(limited, vec![1]);
        assert_eq!(ids, vec![1, 2, 3]);
    });

    shutdown_tx.shutdown().await;
}
