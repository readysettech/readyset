use mysql_async::prelude::Queryable;
use readyset_client_metrics::QueryDestination;
use readyset_client_test_helpers::{
    TestBuilder, TestShutdownSender,
    mysql_helpers::{self, MySQLAdapter, last_query_info},
};
use readyset_server::Handle;
use readyset_tracing::init_test_logging;
use readyset_util::eventually;
use test_utils::{tags, upstream};

/// Sets up a database holding two tables and returns a connection to each of the upstream and the
/// adapter.
///
/// The server handle is returned rather than dropped here: dropping it takes the server down with
/// it, and every read below needs it alive.
async fn setup(
    db_name: &str,
) -> (
    mysql_async::Conn,
    mysql_async::Conn,
    Handle,
    TestShutdownSender<MySQLAdapter>,
) {
    init_test_logging();
    mysql_helpers::recreate_database(db_name).await;

    let upstream_opts = mysql_helpers::upstream_config().db_name(Some(db_name));
    let mut upstream_conn = mysql_async::Conn::new(upstream_opts).await.unwrap();
    for statement in [
        "CREATE TABLE customers (id INT PRIMARY KEY, region TEXT)",
        "CREATE TABLE orders (id INT PRIMARY KEY, cust INT, amount INT)",
        "INSERT INTO customers VALUES (1, 'east'), (2, 'west')",
        "INSERT INTO orders VALUES (1, 1, 10), (2, 1, 20), (3, 2, 30)",
    ] {
        upstream_conn.query_drop(statement).await.unwrap();
    }

    let (rs_opts, handle, shutdown_tx) = TestBuilder::default()
        .recreate_database(false)
        .replicate_db(db_name)
        // A statement the pass declines reaches the upstream instead, which is what a deployment
        // sees; without this such a read would error rather than answer.
        .fallback(true)
        .build::<MySQLAdapter>()
        .await;
    let rs_conn = mysql_async::Conn::new(rs_opts).await.unwrap();
    (upstream_conn, rs_conn, handle, shutdown_tx)
}

/// Reads what the cache was built from, so a test can tell an inlined plan from a kept one.
///
/// Kept, an entry becomes a view named `__<query>__<entry>` that the plan joins; inlined, its
/// body is absorbed and no such name appears.  Both plans cache and both answer correctly, so
/// this is what distinguishes them.
async fn cached_statements(conn: &mut mysql_async::Conn) -> String {
    let rows: Vec<mysql_async::Row> = conn.query("SHOW CACHES").await.unwrap();
    rows.iter()
        .map(|row| {
            (0..row.columns_ref().len())
                .filter_map(|i| row.get::<String, _>(i))
                .collect::<Vec<_>>()
                .join(" ")
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// An entry read once is substituted at the reference that reads it, and the derived table it
/// leaves behind is served from the cache.
///
/// A CTE query caches either way -- kept, it becomes a view -- so the destination alone cannot
/// tell the two apart.  What distinguishes them is the plan the cache was built from: inlining
/// leaves no `WITH` clause in it.
#[tokio::test]
#[tags(serial)]
#[upstream(mysql)]
async fn a_singly_read_entry_is_inlined_into_its_cache() {
    let (mut upstream_conn, mut rs_conn, _handle, shutdown_tx) = setup("cte_inlining_served").await;
    let query = "SELECT orders.amount FROM orders \
                 JOIN (SELECT id FROM customers WHERE region = 'east') east ON orders.cust = east.id \
                 ORDER BY orders.amount";
    let with_cte = "WITH east AS (SELECT id FROM customers WHERE region = 'east') \
                    SELECT orders.amount FROM orders JOIN east ON orders.cust = east.id \
                    ORDER BY orders.amount";

    let expected: Vec<i32> = upstream_conn.query(query).await.unwrap();
    rs_conn
        .query_drop(format!("CREATE CACHE FROM {with_cte}"))
        .await
        .expect("a singly read entry should cache");

    eventually!(run_test: {
        let rows: Vec<i32> = rs_conn.query(with_cte).await.unwrap();
        let destination = last_query_info(&mut rs_conn).await.destination;
        (rows, destination)
    }, then_assert: |result| {
        let (rows, destination) = result;
        assert_eq!(rows, expected);
        assert!(
            matches!(destination, QueryDestination::Readyset(_)),
            "the read should be served from the cache, got {destination:?}"
        );
    });

    let cached = cached_statements(&mut rs_conn).await;
    assert!(
        cached.contains("orders"),
        "the plan should have been read back at all: {cached:?}"
    );
    assert!(
        !cached.contains("__east"),
        "the entry should be absorbed into the plan, not left as a view: {cached}"
    );

    shutdown_tx.shutdown().await;
}

/// The cache tracks the tables behind the entry.  A read before the write and a read after it
/// are what distinguish a maintained cache from one that answered once and went stale.
#[tokio::test]
#[tags(serial)]
#[upstream(mysql)]
async fn a_cached_entry_tracks_a_write() {
    let (mut upstream_conn, mut rs_conn, _handle, shutdown_tx) = setup("cte_inlining_write").await;
    let with_cte = "WITH east AS (SELECT id FROM customers WHERE region = 'east') \
                    SELECT orders.amount FROM orders JOIN east ON orders.cust = east.id \
                    ORDER BY orders.amount";

    rs_conn
        .query_drop(format!("CREATE CACHE FROM {with_cte}"))
        .await
        .expect("a singly read entry should cache");

    eventually!(run_test: {
        let rows: Vec<i32> = rs_conn.query(with_cte).await.unwrap();
        let destination = last_query_info(&mut rs_conn).await.destination;
        (rows, destination)
    }, then_assert: |result| {
        let (rows, destination) = result;
        assert_eq!(rows, vec![10, 20]);
        assert!(
            matches!(destination, QueryDestination::Readyset(_)),
            "the read should be served from the cache, got {destination:?}"
        );
    });

    upstream_conn
        .query_drop("INSERT INTO orders VALUES (4, 1, 40)")
        .await
        .unwrap();

    eventually!(run_test: {
        let rows: Vec<i32> = rs_conn.query(with_cte).await.unwrap();
        let destination = last_query_info(&mut rs_conn).await.destination;
        (rows, destination)
    }, then_assert: |result| {
        let (rows, destination) = result;
        assert_eq!(rows, vec![10, 20, 40], "the cache should track the write");
        assert!(
            matches!(destination, QueryDestination::Readyset(_)),
            "the read should be served from the cache, got {destination:?}"
        );
    });

    let cached = cached_statements(&mut rs_conn).await;
    assert!(
        cached.contains("orders"),
        "the plan should have been read back at all: {cached:?}"
    );
    assert!(
        !cached.contains("__east"),
        "the cache tracking the write should be the inlined plan, not a view: {cached}"
    );

    shutdown_tx.shutdown().await;
}

/// Inlining copies a body into the statement once per read, so an entry read twice is declined
/// rather than copied.
#[tokio::test]
#[tags(serial)]
#[upstream(mysql)]
async fn an_entry_read_twice_is_declined() {
    let (_upstream_conn, mut rs_conn, _handle, shutdown_tx) = setup("cte_inlining_twice").await;
    let with_cte = "WITH both AS (SELECT id FROM customers) \
                    SELECT count(*) AS n FROM both AS a JOIN both AS b ON a.id = b.id";

    let error = rs_conn
        .query_drop(format!("CREATE CACHE FROM {with_cte}"))
        .await
        .expect_err("an entry read twice should be declined");
    assert!(
        error.to_string().contains("read more than once"),
        "got: {error}"
    );

    shutdown_tx.shutdown().await;
}
