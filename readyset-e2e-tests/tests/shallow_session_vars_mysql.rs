use std::assert_matches;
use std::panic::AssertUnwindSafe;

use mysql_async::prelude::Queryable;

use readyset_client_metrics::QueryDestination;
use readyset_client_test_helpers::mysql_helpers::{self, MySQLAdapter, last_query_info};
use readyset_client_test_helpers::{TestBuilder, TestShutdownSender};
use readyset_server::Handle;
use readyset_sql_parsing::ParsingPreset;
use readyset_util::eventually;
use test_utils::{tags, upstream};

/// Build an adapter with fallback, a three row table `t`, and a shallow cache on `query`.
async fn setup(
    query: &str,
    options: &str,
) -> (
    mysql_async::Conn,
    mysql_async::Conn,
    mysql_async::Opts,
    Handle,
    TestShutdownSender<MySQLAdapter>,
) {
    readyset_tracing::init_test_logging();
    let (opts, handle, shutdown_tx) = TestBuilder::default()
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
